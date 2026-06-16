// ClipboardDispatcher：剪贴板监控 + 分发器

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::clipboard::io::ClipboardIO;
use crate::clipboard::types::{
    ClipChangeType, ClipContent, ClipData, compute_signature,
};
use crate::PublishSubject;

type Signature = (i64, String, i64, Vec<String>);

#[derive(Clone)]
struct DispatcherInner {
    last_signature: Option<Signature>,
    self_signatures: std::collections::VecDeque<Signature>,
    self_filter_cap: usize,
    dispatch_count: u64,
    self_filtered_count: u64,
    duplicate_count: u64,
    sequence: u64,
}

#[pyclass(name = "ClipboardDispatcher")]
pub struct ClipboardDispatcher {
    backend_name: String,
    subject: Py<PublishSubject>,
    inner: Mutex<DispatcherInner>,
    running: Arc<AtomicBool>,
    started: Arc<AtomicBool>,
    interval_ms: u64,
    on_change_data_cb: Mutex<Option<PyObject>>,
    filter_self: bool,
    default_tags: Vec<String>,
    self_source: String,
}

impl ClipboardDispatcher {
    pub fn new(
        py: Python<'_>,
        interval: f64,
        backend: String,
        _change_types: Option<PyObject>,
        tags: Option<Vec<String>>,
        filter_self: bool,
        _self_filter: Option<PyObject>,
        self_source: Option<String>,
        self_signature_cap: usize,
        on_change_data: Option<PyObject>,
    ) -> PyResult<Self> {
        let interval_ms = (interval.max(0.02) * 1000.0) as u64;
        let backend_name_lower = backend.to_lowercase();
        let backend_name: String = {
            if cfg!(windows) {
                if backend_name_lower == "win32" || backend_name_lower == "auto" {
                    "win32".into()
                } else {
                    "polling".into()
                }
            } else {
                "polling".into()
            }
        };

        Ok(ClipboardDispatcher {
            backend_name,
            subject: Py::new(py, PublishSubject::new())?,
            inner: Mutex::new(DispatcherInner {
                last_signature: None,
                self_signatures: std::collections::VecDeque::with_capacity(
                    self_signature_cap.max(1),
                ),
                self_filter_cap: self_signature_cap.max(1),
                dispatch_count: 0,
                self_filtered_count: 0,
                duplicate_count: 0,
                sequence: 0,
            }),
            running: Arc::new(AtomicBool::new(false)),
            started: Arc::new(AtomicBool::new(false)),
            interval_ms,
            on_change_data_cb: Mutex::new(on_change_data),
            filter_self,
            default_tags: tags.unwrap_or_default(),
            self_source: self_source.unwrap_or_else(|| "rx-rust:rust".into()),
        })
    }
}

impl ClipboardDispatcher {
    // 公共 start 方法 - 可从 Rust 调用
    pub fn start_me(&self, py: Python<'_>) -> PyResult<()> {
        if self.started.load(Ordering::SeqCst) {
            return Ok(());
        }
        self.started.store(true, Ordering::SeqCst);
        self.running.store(true, Ordering::SeqCst);

        let running = self.running.clone();
        let interval_ms = self.interval_ms;
        let filter_self = self.filter_self;
        let default_tags = self.default_tags.clone();
        let self_source = self.self_source.clone();
        let subject = self.subject.clone_ref(py);
        let inner = Arc::new(parking_lot::Mutex::new(self.inner.lock().clone()));
        // 注意：不存储 on_change_data，因为 PyObject 不能跨线程克隆

        std::thread::spawn(move || {
            loop {
                if !running.load(Ordering::SeqCst) {
                    break;
                }
                Python::with_gil(|py| {
                    // 直接在这里执行调度逻辑
                    if let Ok((ct, content, files, _)) = ClipboardIO::read(py) {
                        let ct_val: u8 = ct.borrow(py).value;
                        let sig = compute_signature(ct_val, &content, &files);
                        
                        // 检查过滤
                        let mut inner_guard = inner.lock();
                        if filter_self && inner_guard.self_signatures.contains(&sig) {
                            inner_guard.self_filtered_count += 1;
                            return;
                        }
                        if let Some(last) = &inner_guard.last_signature {
                            if *last == sig {
                                inner_guard.duplicate_count += 1;
                                return;
                            }
                        }
                        inner_guard.last_signature = Some(sig);
                        inner_guard.dispatch_count += 1;
                        drop(inner_guard);

                        // 构造 ClipData
                        let ts = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_secs_f64())
                            .unwrap_or(0.0);
                        let seq = {
                            let mut inner_guard = inner.lock();
                            inner_guard.sequence += 1;
                            inner_guard.sequence
                        };
                        
                        let meta = PyDict::new_bound(py);
                        meta.set_item("_source", self_source.clone()).ok();
                        let clip = ClipData {
                            content: content.clone(),
                            files: files.clone(),
                            change_type: ct.clone_ref(py),
                            tags: default_tags.clone(),
                            metadata: meta.unbind(),
                            timestamp: ts,
                            sequence: seq,
                        };

                        match Py::new(py, clip) {
                            Ok(c) => {
                                subject.borrow(py).on_next(c.into_any());
                            }
                            Err(_) => return,
                        }
                    }
                });
                std::thread::sleep(Duration::from_millis(interval_ms));
            }
        });

        Ok(())
    }

    // 公共 stop 方法 - 可从 Rust 调用
    pub fn stop_me(&self) {
        self.started.store(false, Ordering::SeqCst);
        self.running.store(false, Ordering::SeqCst);
    }
}

#[pymethods]
impl ClipboardDispatcher {

    // --- 属性 ---
    #[getter]
    fn subject(&self, py: Python<'_>) -> Py<PublishSubject> {
        self.subject.clone_ref(py)
    }

    #[getter]
    fn backend_name(&self) -> String {
        self.backend_name.clone()
    }

    #[getter]
    fn dispatch_count(&self) -> u64 {
        self.inner.lock().dispatch_count
    }

    #[getter]
    fn self_filtered_count(&self) -> u64 {
        self.inner.lock().self_filtered_count
    }

    #[getter]
    fn duplicate_count(&self) -> u64 {
        self.inner.lock().duplicate_count
    }

    #[getter]
    fn is_running(&self) -> bool {
        self.started.load(Ordering::SeqCst)
    }

    // --- 生命周期 ---
    fn start(slf: Py<Self>) -> PyResult<()> {
        Python::with_gil(|py| {
            slf.borrow(py).start_me(py)
        })
    }

    fn stop(slf: Py<Self>) {
        Python::with_gil(|py| {
            slf.borrow(py).stop_me()
        })
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        {
            let self_clone = Python::with_gil(|py| slf.clone_ref(py));
            let _ = Self::start(self_clone);
        }
        slf
    }

    fn __exit__(slf: Py<Self>, _exc_type: PyObject, _exc_val: PyObject, _exc_tb: PyObject) -> bool {
        let self_clone = Python::with_gil(|py| slf.clone_ref(py));
        Self::stop(self_clone);
        false
    }

    // 尝试分发一次剪贴板内容
    fn try_dispatch_once(slf: Py<Self>) -> PyResult<()> {
        Self::try_dispatch_once_impl(slf)
    }

    // --- 写入剪贴板 & 分发 ---
    #[pyo3(signature = (content=None, files=None, change_type=None, *, source=None, tags=None, metadata=None))]
    fn set_clipboard(
        slf: Py<Self>,
        content: Option<PyObject>,
        files: Option<Vec<String>>,
        change_type: Option<Py<ClipChangeType>>,
        source: Option<String>,
        tags: Option<Vec<String>>,
        metadata: Option<Py<PyDict>>,
    ) -> PyResult<Py<ClipData>> {
        Python::with_gil(|py| {
            // 写入系统剪贴板
            let clip_content = match content {
                Some(ref c) => {
                    if let Ok(s) = c.extract::<String>(py) {
                        ClipboardIO::write_text(&s)?;
                        ClipContent::Text(s)
                    } else if let Ok(b) = c.extract::<Vec<u8>>(py) {
                        ClipboardIO::write_bytes(&b)?;
                        ClipContent::Bytes(b)
                    } else if c.is_none(py) {
                        ClipContent::None
                    } else {
                        ClipContent::None
                    }
                }
                None => ClipContent::None,
            };

            let files_clone = files.clone().unwrap_or_default();

            let ct_val: u8 = match change_type {
                Some(ref c) => c.borrow(py).value,
                None => match clip_content {
                    ClipContent::Text(_) => 0,
                    ClipContent::Bytes(_) => 2,
                    ClipContent::None => 5,
                },
            };
            let ct = Py::new(py, ClipChangeType { value: ct_val })?;

            // 注册签名用于自过滤
            let sig = compute_signature(ct_val, &clip_content, &files_clone);
            let self_source_clone = {
                let slf_ref = slf.borrow(py);
                let mut inner = slf_ref.inner.lock();
                inner.self_signatures.push_back(sig);
                if inner.self_signatures.len() > inner.self_filter_cap {
                    inner.self_signatures.pop_front();
                }
                inner.dispatch_count += 1;
                slf_ref.self_source.clone()
            };
            let source = source.unwrap_or(self_source_clone);
            let merged_tags = {
                let slf_ref = slf.borrow(py);
                let mut t = tags.unwrap_or_default();
                t.extend(slf_ref.default_tags.clone());
                t
            };

            let meta_obj = match metadata {
                Some(m) => m,
                None => PyDict::new_bound(py).unbind(),
            };
            let meta_bound = meta_obj.bind(py);
            meta_bound.set_item("_source", source)?;
            let meta_out = meta_bound.clone().unbind();

            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs_f64())
                .unwrap_or(0.0);
            let seq = {
                let slf_ref = slf.borrow(py);
                let mut inner = slf_ref.inner.lock();
                inner.sequence += 1;
                inner.sequence
            };

            let clip = ClipData {
                content: clip_content,
                files: files_clone,
                change_type: ct,
                tags: merged_tags,
                metadata: meta_out,
                timestamp: ts,
                sequence: seq,
            };
            let clip_py = Py::new(py, clip)?;

            // 分发到 subject
            let subject = slf.borrow(py).subject.clone_ref(py);
            subject.borrow(py).on_next(clip_py.clone_ref(py).into_any());

            Ok(clip_py)
        })
    }
}

impl ClipboardDispatcher {
    // 内部：Rust 线程中调用，尝试读取剪贴板并分发到 subject
    fn try_dispatch_once_impl(slf: Py<Self>) -> PyResult<()> {
        Python::with_gil(|py| {
            let (ct, content, files, _meta) = ClipboardIO::read(py)?;
            let ct_val: u8 = ct.borrow(py).value;
            let sig = compute_signature(ct_val, &content, &files);

            // 去重 + 自过滤
            let (filter_self, default_tags, self_source) = {
                let slf_ref = slf.borrow(py);
                let filter_self = slf_ref.filter_self;
                let default_tags = slf_ref.default_tags.clone();
                let self_source = slf_ref.self_source.clone();
                
                let mut inner = slf_ref.inner.lock();
                if filter_self && inner.self_signatures.contains(&sig) {
                    inner.self_filtered_count += 1;
                    return Ok(());
                }
                if let Some(last) = &inner.last_signature {
                    if *last == sig {
                        inner.duplicate_count += 1;
                        return Ok(());
                    }
                }
                inner.last_signature = Some(sig);
                inner.dispatch_count += 1;
                (filter_self, default_tags, self_source)
            };

            // 构造 ClipData
            let tags = default_tags;
            let source = self_source;
            let cb_opt = slf.borrow(py).on_change_data_cb.lock().as_ref().map(|cb| cb.clone_ref(py));

            // 获取 sequence
            let seq = {
                let slf_ref = slf.borrow(py);
                let mut inner = slf_ref.inner.lock();
                inner.sequence += 1;
                inner.sequence
            };
            
            let ct_clone = ct.clone_ref(py);
            
            let clip_obj: PyObject = if let Some(cb) = cb_opt {
                match cb.call0(py) {
                    Ok(val) => val,
                    Err(_) => {
                        let ts = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_secs_f64())
                            .unwrap_or(0.0);
                        let meta = PyDict::new_bound(py);
                        meta.set_item("_source", source)?;
                        let clip = ClipData {
                            content: content.clone(),
                            files: files.clone(),
                            change_type: ct_clone,
                            tags: tags.clone(),
                            metadata: meta.unbind(),
                            timestamp: ts,
                            sequence: seq,
                        };
                        Py::new(py, clip)?.into_any()
                    }
                }
            } else {
                let ts = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs_f64())
                    .unwrap_or(0.0);
                let meta = PyDict::new_bound(py);
                meta.set_item("_source", source)?;
                let clip = ClipData {
                    content: content.clone(),
                    files: files.clone(),
                    change_type: ct_clone,
                    tags: tags.clone(),
                    metadata: meta.unbind(),
                    timestamp: ts,
                    sequence: seq,
                };
                Py::new(py, clip)?.into_any()
            };

            let subject = slf.borrow(py).subject.clone_ref(py);
            subject.borrow(py).on_next(clip_obj);

            Ok(())
        })
    }
}
