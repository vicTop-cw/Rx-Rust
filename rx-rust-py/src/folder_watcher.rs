// ============================================================================
// rx-rust-py folder_watcher - 目录系统监控（Rust 实现，钩子式 API）
//
// 与 vools.reactive.folder_watcher 语义对齐，底层用 notify crate:
//   - Windows: ReadDirectoryChangesW
//   - Linux:   inotify
//   - macOS:   FSEvents
//   - 其他:    Polling 回退
//
// 类型设计：
//   FolderChangeType (0..6): CREATED, DELETED, RENAMED, MOVED_IN, MOVED_OUT, ATTRIB, CONTENT
//   FolderData: path, old_path, change_type, file_count, child_folder_count,
//               timestamp, sequence, tags, metadata
//   FolderDispatcher: 监控内核，事件分发
//   FolderSubject: 自含 Dispatcher + Subject-like 语义
//   FolderObserver: 钩子式观察者（按事件类型路由回调）
// ============================================================================

use notify::{
    event::{Event as NotifyEvent, EventKind, ModifyKind, RenameMode},
    RecommendedWatcher, RecursiveMode, Watcher,
};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;

// ============================================================================
// FolderChangeType - 目录变更类型枚举
// ============================================================================

#[pyclass]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FolderChangeType(pub u32);

impl FolderChangeType {
    pub const CREATED: u32 = 0;
    pub const DELETED: u32 = 1;
    pub const RENAMED: u32 = 2;
    pub const MOVED_IN: u32 = 3;
    pub const MOVED_OUT: u32 = 4;
    pub const ATTRIB: u32 = 5;
    pub const CONTENT: u32 = 6;

    pub fn to_name(ct: u32) -> &'static str {
        match ct {
            0 => "CREATED",
            1 => "DELETED",
            2 => "RENAMED",
            3 => "MOVED_IN",
            4 => "MOVED_OUT",
            5 => "ATTRIB",
            6 => "CONTENT",
            _ => "UNKNOWN",
        }
    }
}

#[pymethods]
impl FolderChangeType {
    #[classattr]
    const CREATED: u32 = 0;
    #[classattr]
    const DELETED: u32 = 1;
    #[classattr]
    const RENAMED: u32 = 2;
    #[classattr]
    const MOVED_IN: u32 = 3;
    #[classattr]
    const MOVED_OUT: u32 = 4;
    #[classattr]
    const ATTRIB: u32 = 5;
    #[classattr]
    const CONTENT: u32 = 6;

    #[new]
    fn new(value: u32) -> Self {
        Self(value)
    }

    fn __int__(&self) -> u32 {
        self.0
    }

    fn __str__(&self) -> &'static str {
        Self::to_name(self.0)
    }

    fn __repr__(&self) -> String {
        format!("FolderChangeType.{}({})", Self::to_name(self.0), self.0)
    }

    fn __eq__(&self, other: PyObject, py: Python<'_>) -> PyObject {
        if let Ok(val) = other.extract::<u32>(py) {
            return (self.0 == val).into_pyobject(py).unwrap().unbind().into_any();
        }
        if let Ok(ft) = other.extract::<PyRef<FolderChangeType>>(py) {
            return (self.0 == ft.0).into_pyobject(py).unwrap().unbind().into_any();
        }
        false.into_pyobject(py).unwrap().unbind().into_any()
    }
}

// ============================================================================
// FolderData - 结构化目录事件数据
// ============================================================================

static FOLDER_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[pyclass]
pub struct FolderData {
    #[pyo3(get)]
    pub path: String,
    #[pyo3(get)]
    pub old_path: Option<String>,
    #[pyo3(get)]
    pub change_type: u32,
    #[pyo3(get)]
    pub file_count: Option<u64>,
    #[pyo3(get)]
    pub child_folder_count: Option<u64>,
    #[pyo3(get)]
    pub timestamp: f64,
    #[pyo3(get)]
    pub sequence: u64,
    #[pyo3(get)]
    pub tags: Vec<String>,
    #[pyo3(get)]
    pub metadata: std::collections::HashMap<String, String>,
}

#[pymethods]
impl FolderData {
    #[new]
    #[pyo3(signature = (path, old_path=None, change_type=FolderChangeType::CONTENT, file_count=None, child_folder_count=None, timestamp=None, sequence=None, tags=None, metadata=None))]
    fn new(
        path: String,
        old_path: Option<String>,
        change_type: u32,
        file_count: Option<u64>,
        child_folder_count: Option<u64>,
        timestamp: Option<f64>,
        sequence: Option<u64>,
        tags: Option<Vec<String>>,
        metadata: Option<std::collections::HashMap<String, String>>,
    ) -> Self {
        let ts = timestamp.unwrap_or_else(|| {
            let now = std::time::SystemTime::now();
            now.duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs_f64()
        });
        Self {
            path,
            old_path,
            change_type,
            file_count,
            child_folder_count,
            timestamp: ts,
            sequence: sequence.unwrap_or_else(|| FOLDER_SEQUENCE.fetch_add(1, Ordering::SeqCst)),
            tags: tags.unwrap_or_default(),
            metadata: metadata.unwrap_or_default(),
        }
    }

    #[staticmethod]
    #[pyo3(signature = (path, old_path=None, change_type=FolderChangeType::CONTENT, file_count=None, child_folder_count=None, tags=None, metadata=None))]
    fn now(
        path: String,
        old_path: Option<String>,
        change_type: u32,
        file_count: Option<u64>,
        child_folder_count: Option<u64>,
        tags: Option<Vec<String>>,
        metadata: Option<std::collections::HashMap<String, String>>,
    ) -> Self {
        let now = std::time::SystemTime::now();
        let ts = now
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();
        Self {
            path,
            old_path,
            change_type,
            file_count,
            child_folder_count,
            timestamp: ts,
            sequence: FOLDER_SEQUENCE.fetch_add(1, Ordering::SeqCst),
            tags: tags.unwrap_or_default(),
            metadata: metadata.unwrap_or_default(),
        }
    }

    fn to_dict(&self, py: Python<'_>) -> Py<PyDict> {
        let d = PyDict::new_bound(py);
        let _ = d.set_item("path", &self.path);
        let _ = d.set_item("old_path", self.old_path.clone());
        let _ = d.set_item("change_type", self.change_type);
        let _ = d.set_item("change_type_name", FolderChangeType::to_name(self.change_type));
        let _ = d.set_item("file_count", self.file_count);
        let _ = d.set_item("child_folder_count", self.child_folder_count);
        let _ = d.set_item("timestamp", self.timestamp);
        let _ = d.set_item("sequence", self.sequence);
        let tags_list = PyList::new_bound(py, self.tags.iter());
        let _ = d.set_item("tags", tags_list);
        let meta = PyDict::new_bound(py);
        for (k, v) in &self.metadata {
            let _ = meta.set_item(k, v);
        }
        let _ = d.set_item("metadata", meta);
        d.unbind()
    }

    #[staticmethod]
    fn from_dict(dict: PyObject, py: Python<'_>) -> PyResult<Self> {
        let d = dict.extract::<Py<PyDict>>(py)?;
        let d_borrowed = d.bind(py);
        let path: String = d_borrowed
            .get_item("path")?
            .map_or_else(|| Ok(String::new()), |v| v.extract())?;
        let old_path: Option<String> = d_borrowed
            .get_item("old_path")?
            .map_or_else(|| Ok(None), |v| v.extract())?;
        let change_type: u32 = d_borrowed.get_item("change_type")?.map_or_else(
            || Ok(FolderChangeType::CONTENT),
            |v| v.extract(),
        )?;
        let file_count: Option<u64> = d_borrowed
            .get_item("file_count")?
            .map_or_else(|| Ok(None), |v| v.extract())?;
        let child_folder_count: Option<u64> = d_borrowed
            .get_item("child_folder_count")?
            .map_or_else(|| Ok(None), |v| v.extract())?;
        let timestamp: f64 =
            d_borrowed.get_item("timestamp")?.map_or_else(
                || {
                    let now = std::time::SystemTime::now();
                    Ok::<f64, PyErr>(now.duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs_f64())
                },
                |v| v.extract(),
            )?;
        let sequence: u64 = d_borrowed.get_item("sequence")?.map_or_else(
            || Ok(FOLDER_SEQUENCE.fetch_add(1, Ordering::SeqCst)),
            |v| v.extract(),
        )?;
        let tags: Vec<String> = d_borrowed
            .get_item("tags")?
            .map_or_else(|| Ok(Vec::new()), |v| v.extract())?;
        let meta_dict: std::collections::HashMap<String, String> = d_borrowed
            .get_item("metadata")?
            .map_or_else(|| Ok(std::collections::HashMap::new()), |v| v.extract())?;

        Ok(Self {
            path,
            old_path,
            change_type,
            file_count,
            child_folder_count,
            timestamp,
            sequence,
            tags,
            metadata: meta_dict,
        })
    }

    fn to_json(&self, py: Python<'_>) -> PyResult<String> {
        let json_module = py.import_bound("json")?;
        let d = self.to_dict(py);
        let result = json_module.call_method1("dumps", (d,))?;
        result.extract::<String>()
    }

    #[staticmethod]
    fn from_json(s: &str, py: Python<'_>) -> PyResult<Self> {
        let json_module = py.import_bound("json")?;
        let obj = json_module.call_method1("loads", (s,))?;
        Self::from_dict(obj.unbind(), py)
    }

    fn __str__(&self) -> String {
        format!(
            "FolderData(path={:?}, change_type={}, seq={})",
            self.path,
            FolderChangeType::to_name(self.change_type),
            self.sequence
        )
    }

    fn __repr__(&self) -> String {
        self.__str__()
    }
}

// ============================================================================
// FolderDispatcher - 目录监控与事件分发核心
// ============================================================================

type FolderObserverCallback = Arc<dyn Fn(Py<FolderData>) + Send + Sync>;

struct FolderDispatcherState {
    watcher: Option<RecommendedWatcher>,
    observers: Vec<FolderObserverCallback>,
    paths: Vec<PathBuf>,
    change_types: Option<std::collections::HashSet<u32>>,
    dispatch_count: u64,
    error_count: u64,
    running: bool,
    pending_rename_from: Option<PathBuf>,
    pending_rename_time: Option<Instant>,
}

#[pyclass]
pub struct FolderDispatcher {
    state: Arc<Mutex<FolderDispatcherState>>,
    backend_name: String,
    interval_ms: u64,
}

fn map_folder_event_kind_to_change_type(kind: EventKind) -> u32 {
    match kind {
        EventKind::Create(_) => FolderChangeType::CREATED,
        EventKind::Remove(_) => FolderChangeType::DELETED,
        EventKind::Modify(ModifyKind::Data(_)) => FolderChangeType::CONTENT,
        EventKind::Modify(ModifyKind::Metadata(_)) => FolderChangeType::ATTRIB,
        EventKind::Modify(_) => FolderChangeType::CONTENT,
        EventKind::Any => FolderChangeType::CONTENT,
        EventKind::Other => FolderChangeType::CONTENT,
    }
}

#[pymethods]
impl FolderDispatcher {
    #[new]
    #[pyo3(signature = (paths=None, backend="auto", change_types=None, interval=0.5))]
    fn new(
        paths: Option<Vec<String>>,
        backend: &str,
        change_types: Option<Vec<u32>>,
        interval: f64,
    ) -> Self {
        let paths_pb = match paths {
            Some(ps) => ps.into_iter().map(PathBuf::from).collect(),
            None => Vec::new(),
        };

        let ct_set = change_types.map(|v| v.into_iter().collect::<std::collections::HashSet<_>>());

        let interval_ms = (interval.max(0.01) * 1000.0) as u64;

        // 选择后端
        let backend_name = if backend == "polling" {
            String::from("polling")
        } else {
            // 先尝试 RecommendedWatcher，失败则回退 polling
            match notify::RecommendedWatcher::new(
                |_res: Result<NotifyEvent, notify::Error>| {
                    // 空回调，后续在 start 里注册真正的回调
                },
                notify::Config::default(),
            ) {
                Ok(_) => String::from("native"),
                Err(_) => String::from("polling"),
            }
        };

        let state = Arc::new(Mutex::new(FolderDispatcherState {
            watcher: None,
            observers: Vec::new(),
            paths: paths_pb,
            change_types: ct_set,
            dispatch_count: 0,
            error_count: 0,
            running: false,
            pending_rename_from: None,
            pending_rename_time: None,
        }));

        Self {
            state,
            backend_name,
            interval_ms,
        }
    }

    #[getter]
    fn backend_name(&self) -> String {
        self.backend_name.clone()
    }

    #[getter]
    fn dispatch_count(&self) -> u64 {
        self.state.lock().unwrap().dispatch_count
    }

    #[getter]
    fn error_count(&self) -> u64 {
        self.state.lock().unwrap().error_count
    }

    #[getter]
    fn is_running(&self) -> bool {
        self.state.lock().unwrap().running
    }

    fn start(&self, py: Python<'_>) -> PyResult<()> {
        let mut st = self.state.lock().unwrap();
        if st.running {
            return Ok(());
        }

        let paths_to_watch: Vec<PathBuf> = st.paths.clone();
        let interval_ms = self.interval_ms;
        let backend_name = self.backend_name.clone();

        // 创建事件 channel
        let (tx, rx) = std::sync::mpsc::channel::<(PathBuf, Option<PathBuf>, u32)>();

        // ========== 1) 初始化 notify watcher（或 polling 回退） ==========
        let use_polling = backend_name == "polling";

        if !use_polling {
            // 使用 notify::RecommendedWatcher
            let tx_for_notify = tx.clone();
            let state_for_notify = self.state.clone();

            match notify::RecommendedWatcher::new(
                move |res: Result<NotifyEvent, notify::Error>| {
                    if let Ok(ev) = res {
                        // RENAME 事件的 cookie 配对处理
                        let is_rename_from = matches!(
                            ev.kind,
                            EventKind::Modify(ModifyKind::Name(RenameMode::From))
                        );
                        let is_rename_to = matches!(
                            ev.kind,
                            EventKind::Modify(ModifyKind::Name(RenameMode::To))
                        );
                        let is_rename_both = matches!(
                            ev.kind,
                            EventKind::Modify(ModifyKind::Name(RenameMode::Both))
                        );

                        if is_rename_from {
                            if let Some(p) = ev.paths.first().cloned() {
                                if p.is_dir() {
                                    let mut guard = state_for_notify.lock().unwrap();
                                    guard.pending_rename_from = Some(p);
                                    guard.pending_rename_time = Some(Instant::now());
                                } else {
                                    // 非目录 - 忽略（我们只监控目录事件）
                                    let mut guard = state_for_notify.lock().unwrap();
                                    guard.pending_rename_from = Some(p);
                                    guard.pending_rename_time = Some(Instant::now());
                                }
                            }
                        } else if is_rename_to {
                            let (old_opt, new_opt) = {
                                let mut guard = state_for_notify.lock().unwrap();
                                let old = guard.pending_rename_from.take();
                                guard.pending_rename_time = None;
                                let new = ev.paths.first().cloned();
                                (old, new)
                            };
                            if let Some(new) = new_opt {
                                // 只派发目录事件
                                if new.is_dir() || old_opt.as_ref().map(|p| p.is_dir()).unwrap_or(false) {
                                    let _ = tx_for_notify.send((new, old_opt, FolderChangeType::RENAMED));
                                }
                            }
                        } else if is_rename_both {
                            // Both 模式: paths[0] old, paths[1] new
                            if ev.paths.len() >= 2 {
                                let old = ev.paths[0].clone();
                                let new = ev.paths[1].clone();
                                if new.is_dir() || old.is_dir() {
                                    let _ = tx_for_notify.send((new, Some(old), FolderChangeType::RENAMED));
                                }
                            } else if let Some(p) = ev.paths.first().cloned() {
                                if p.is_dir() {
                                    let _ = tx_for_notify.send((p, None, FolderChangeType::RENAMED));
                                }
                            }
                        } else {
                            let ct = map_folder_event_kind_to_change_type(ev.kind);
                            for p in &ev.paths {
                                // 只派发目录事件
                                if p.is_dir() {
                                    let _ = tx_for_notify.send((p.clone(), None, ct));
                                }
                            }
                        }
                    }
                },
                notify::Config::default(),
            ) {
                Ok(mut watcher) => {
                    for p in &paths_to_watch {
                        if p.exists() {
                            let _ = watcher.watch(p, RecursiveMode::Recursive);
                        }
                    }
                    st.watcher = Some(watcher);
                }
                Err(_e) => {
                    // notify 创建失败，回退 polling
                }
            }
        }

        // 如果没有创建 notify watcher，则用 polling 回退
        let mut polling_needed = {
            let guard = self.state.lock().unwrap();
            guard.watcher.is_none() && !paths_to_watch.is_empty()
        };
        if use_polling && !paths_to_watch.is_empty() {
            polling_needed = true;
        }

        st.running = true;
        drop(st);

        // ========== 2) Polling 回退线程 ==========
        if polling_needed {
            let paths_poll = paths_to_watch.clone();
            let state_clone = self.state.clone();
            let interval = std::time::Duration::from_millis(interval_ms);
            let tx_poll = tx.clone();

            thread::spawn(move || {
                let mut snapshot: std::collections::HashMap<PathBuf, std::time::SystemTime> =
                    std::collections::HashMap::new();

                fn scan_folder_dirs(
                    base: &PathBuf,
                    snap: &mut std::collections::HashMap<PathBuf, std::time::SystemTime>,
                ) {
                    if let Ok(entries) = std::fs::read_dir(base) {
                        for entry in entries.flatten() {
                            if let Ok(ft) = entry.file_type() {
                                if ft.is_dir() {
                                    let p = entry.path();
                                    if let Ok(meta) = std::fs::metadata(&p) {
                                        if let Ok(mtime) = meta.modified() {
                                            snap.insert(p, mtime);
                                        }
                                    }
                                    // 递归
                                    scan_folder_dirs(&entry.path(), snap);
                                }
                            }
                        }
                    }
                }

                // 初始快照
                for p in &paths_poll {
                    if p.is_dir() {
                        if let Ok(meta) = std::fs::metadata(p) {
                            if let Ok(mtime) = meta.modified() {
                                snapshot.insert(p.clone(), mtime);
                            }
                        }
                        scan_folder_dirs(p, &mut snapshot);
                    }
                }

                loop {
                    {
                        let guard = state_clone.lock().unwrap();
                        if !guard.running {
                            break;
                        }
                    }
                    thread::sleep(interval);

                    let mut new_snap: std::collections::HashMap<PathBuf, std::time::SystemTime> =
                        std::collections::HashMap::new();
                    for p in &paths_poll {
                        if p.is_dir() {
                            if let Ok(meta) = std::fs::metadata(p) {
                                if let Ok(mtime) = meta.modified() {
                                    new_snap.insert(p.clone(), mtime);
                                }
                            }
                            scan_folder_dirs(p, &mut new_snap);
                        }
                    }

                    // CREATED / CONTENT
                    for (path, &mtime) in &new_snap {
                        match snapshot.get(path) {
                            None => {
                                let _ = tx_poll.send((path.clone(), None, FolderChangeType::CREATED));
                            }
                            Some(&old_mtime) => {
                                if mtime != old_mtime {
                                    let _ = tx_poll.send((path.clone(), None, FolderChangeType::CONTENT));
                                }
                            }
                        }
                    }
                    // DELETED
                    for path in snapshot.keys() {
                        if !new_snap.contains_key(path) {
                            let _ = tx_poll.send((path.clone(), None, FolderChangeType::DELETED));
                        }
                    }

                    snapshot = new_snap;
                }
            });
        }

        // ========== 3) 事件分发线程（把 channel 中的事件转成 FolderData） ==========
        let state_clone_dispatch = self.state.clone();
        thread::spawn(move || {
            loop {
                // 检查 running
                {
                    let guard = state_clone_dispatch.lock().unwrap();
                    if !guard.running {
                        break;
                    }
                }

                // 检查超时的 pending_rename_from（>500ms 未配对 => MOVED_OUT）
                {
                    let mut guard = state_clone_dispatch.lock().unwrap();
                    if let Some(t) = guard.pending_rename_time {
                        if t.elapsed().as_millis() > 500 {
                            if let Some(old) = guard.pending_rename_from.take() {
                                if old.is_dir() {
                                    // 作为 MOVED_OUT 派发
                                    let obs_snapshot: Vec<FolderObserverCallback> = guard.observers.clone();
                                    let ct_filter = guard.change_types.clone();
                                    drop(guard);

                                    let should = ct_filter.as_ref().map_or(true, |f| f.contains(&FolderChangeType::MOVED_OUT));
                                    if should {
                                        let fd_res = Python::with_gil(|py| {
                                            let fd = Py::new(
                                                py,
                                                FolderData::now(
                                                    old.to_string_lossy().into_owned(),
                                                    None,
                                                    FolderChangeType::MOVED_OUT,
                                                    None,
                                                    None,
                                                    None,
                                                    None,
                                                ),
                                            )?;
                                            Ok::<Py<FolderData>, PyErr>(fd)
                                        });
                                        match fd_res {
                                            Ok(fd) => {
                                                for cb in &obs_snapshot {
                                                    cb(fd.clone());
                                                }
                                                let mut g = state_clone_dispatch.lock().unwrap();
                                                g.dispatch_count += 1;
                                            }
                                            Err(_) => {
                                                let mut g = state_clone_dispatch.lock().unwrap();
                                                g.error_count += 1;
                                            }
                                        }
                                    }
                                    continue;
                                }
                            }
                        }
                    }
                }

                match rx.recv_timeout(std::time::Duration::from_millis(200)) {
                    Ok((path, old_path, change_type)) => {
                        // 获取观察者快照 + change_types 过滤
                        let (observers, ct_filter) = {
                            let guard = state_clone_dispatch.lock().unwrap();
                            let filter = guard.change_types.clone();
                            let obs: Vec<FolderObserverCallback> = guard.observers.clone();
                            (obs, filter)
                        };

                        let should_dispatch = ct_filter.as_ref().map_or(true, |f| f.contains(&change_type));
                        if !should_dispatch {
                            continue;
                        }

                        let fd_res = Python::with_gil(|py| {
                            let fd = Py::new(
                                py,
                                FolderData::now(
                                    path.to_string_lossy().into_owned(),
                                    old_path.map(|p| p.to_string_lossy().into_owned()),
                                    change_type,
                                    None,
                                    None,
                                    None,
                                    None,
                                ),
                            )?;
                            Ok::<Py<FolderData>, PyErr>(fd)
                        });

                        match fd_res {
                            Ok(fd) => {
                                for cb in &observers {
                                    cb(fd.clone());
                                }
                                let mut guard = state_clone_dispatch.lock().unwrap();
                                guard.dispatch_count += 1;
                            }
                            Err(_e) => {
                                let mut guard = state_clone_dispatch.lock().unwrap();
                                guard.error_count += 1;
                            }
                        }
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                }
            }
        });

        let _ = py;
        Ok(())
    }

    fn stop(&self) {
        let mut st = self.state.lock().unwrap();
        st.running = false;
        st.pending_rename_from = None;
        st.pending_rename_time = None;
        if let Some(mut w) = st.watcher.take() {
            for p in &st.paths {
                let _ = w.unwatch(p);
            }
        }
    }

    fn subscribe(&self, on_next: PyObject, py: Python<'_>) -> PyResult<PyObject> {
        let cb = Py::new(py, on_next)?;
        let observer_cb: FolderObserverCallback = Arc::new(move |fd: Py<FolderData>| {
            Python::with_gil(|py| {
                let _ = cb.call1(py, (fd,));
            });
        });

        let mut st = self.state.lock().unwrap();
        st.observers.push(observer_cb);
        let observer_index = st.observers.len();

        let sub = FolderSubscriptionHandle {
            index: observer_index,
            state: self.state.clone(),
            disposed: Arc::new(Mutex::new(false)),
        };
        let py_sub = Py::new(py, sub)?;
        Ok(py_sub.into_any())
    }

    fn add_path(&self, path: String) -> PyResult<()> {
        let p = PathBuf::from(&path);
        let mut st = self.state.lock().unwrap();
        st.paths.push(p.clone());
        if let Some(w) = &mut st.watcher {
            if p.exists() {
                let _ = w.watch(&p, RecursiveMode::Recursive);
            }
        }
        Ok(())
    }

    fn remove_path(&self, path: String) -> PyResult<()> {
        let p = PathBuf::from(&path);
        let mut st = self.state.lock().unwrap();
        if let Some(pos) = st.paths.iter().position(|x| x == &p) {
            st.paths.remove(pos);
        }
        if let Some(w) = &mut st.watcher {
            let _ = w.unwatch(&p);
        }
        Ok(())
    }

    fn __enter__<'py>(slf: PyRef<'py, Self>) -> PyRef<'py, Self> {
        Python::with_gil(|py| {
            let _ = slf.start(py);
        });
        slf
    }

    fn __exit__<'py>(slf: PyRef<'py, Self>, _exc_type: PyObject, _exc_val: PyObject, _tb: PyObject) -> bool {
        slf.stop();
        false
    }
}

// ============================================================================
// FolderSubscriptionHandle - 订阅句柄
// ============================================================================

#[pyclass]
pub struct FolderSubscriptionHandle {
    index: usize,
    state: Arc<Mutex<FolderDispatcherState>>,
    disposed: Arc<Mutex<bool>>,
}

#[pymethods]
impl FolderSubscriptionHandle {
    fn dispose(&self) {
        *self.disposed.lock().unwrap() = true;
        let mut st = self.state.lock().unwrap();
        // 惰性移除（下次事件时不会调用 disposed 的观察者）
        if self.index <= st.observers.len() && self.index > 0 {
            // 在列表中直接移除
            // 但注意：多线程并发下我们只设一个 flag 更安全
            // 这里我们执行即时移除
            let idx = self.index - 1;
            if idx < st.observers.len() {
                st.observers.remove(idx);
            }
        }
    }

    fn is_disposed(&self) -> bool {
        *self.disposed.lock().unwrap()
    }
}

// ============================================================================
// FolderSubject - 自含 Dispatcher 的 Subject（支持 pipe/subscribe）
// ============================================================================

#[pyclass]
pub struct FolderSubject {
    dispatcher: Arc<Mutex<Option<Py<FolderDispatcher>>>>,
    subscribed: Arc<Mutex<bool>>,
}

#[pymethods]
impl FolderSubject {
    #[new]
    #[pyo3(signature = (paths=None, backend="auto", change_types=None, _tags=None, interval=0.5, auto_start=true))]
    fn new(
        paths: Option<Vec<String>>,
        backend: &str,
        change_types: Option<Vec<u32>>,
        _tags: Option<Vec<String>>,
        interval: f64,
        auto_start: bool,
        py: Python<'_>,
    ) -> PyResult<Self> {
        let dispatcher_rs = FolderDispatcher::new(paths, backend, change_types, interval);
        let dispatcher_py = Py::new(py, dispatcher_rs)?;

        if auto_start {
            let disp_ref = dispatcher_py.bind(py);
            let _ = disp_ref.call_method0("start");
        }

        Ok(Self {
            dispatcher: Arc::new(Mutex::new(Some(dispatcher_py))),
            subscribed: Arc::new(Mutex::new(false)),
        })
    }

    #[getter]
    fn backend_name(&self, py: Python<'_>) -> String {
        let guard = self.dispatcher.lock().unwrap();
        if let Some(d) = &*guard {
            d.bind(py)
                .getattr("backend_name")
                .and_then(|v| v.extract::<String>())
                .unwrap_or_default()
        } else {
            String::new()
        }
    }

    #[getter]
    fn dispatch_count(&self, py: Python<'_>) -> u64 {
        let guard = self.dispatcher.lock().unwrap();
        if let Some(d) = &*guard {
            d.bind(py)
                .getattr("dispatch_count")
                .and_then(|v| v.extract::<u64>())
                .unwrap_or(0)
        } else {
            0
        }
    }

    #[getter]
    fn error_count(&self, py: Python<'_>) -> u64 {
        let guard = self.dispatcher.lock().unwrap();
        if let Some(d) = &*guard {
            d.bind(py)
                .getattr("error_count")
                .and_then(|v| v.extract::<u64>())
                .unwrap_or(0)
        } else {
            0
        }
    }

    #[getter]
    fn is_running(&self, py: Python<'_>) -> bool {
        let guard = self.dispatcher.lock().unwrap();
        if let Some(d) = &*guard {
            d.bind(py)
                .getattr("is_running")
                .and_then(|v| v.extract::<bool>())
                .unwrap_or(false)
        } else {
            false
        }
    }

    fn subscribe(&self, on_next: PyObject, py: Python<'_>) -> PyResult<PyObject> {
        let guard = self.dispatcher.lock().unwrap();
        if let Some(d) = &*guard {
            let result = d.bind(py).call_method1("subscribe", (on_next,))?;
            *self.subscribed.lock().unwrap() = true;
            Ok(result)
        } else {
            Err(pyo3::exceptions::PyRuntimeError::new_err("FolderSubject dispatcher is not available"))
        }
    }

    fn start(&self, py: Python<'_>) -> PyResult<()> {
        let guard = self.dispatcher.lock().unwrap();
        if let Some(d) = &*guard {
            d.bind(py).call_method0("start")?;
        }
        Ok(())
    }

    fn stop(&self, py: Python<'_>) -> PyResult<()> {
        let guard = self.dispatcher.lock().unwrap();
        if let Some(d) = &*guard {
            d.bind(py).call_method0("stop")?;
        }
        Ok(())
    }

    fn add_path(&self, path: String, py: Python<'_>) -> PyResult<()> {
        let guard = self.dispatcher.lock().unwrap();
        if let Some(d) = &*guard {
            d.bind(py).call_method1("add_path", (path,))?;
        }
        Ok(())
    }

    fn remove_path(&self, path: String, py: Python<'_>) -> PyResult<()> {
        let guard = self.dispatcher.lock().unwrap();
        if let Some(d) = &*guard {
            d.bind(py).call_method1("remove_path", (path,))?;
        }
        Ok(())
    }

    // pipe: 接收一个或多个操作符（Python callable），它们接受 (self, observable) -> observable
    fn pipe(&self, py: Python<'_>, operators: Bound<'_, PyTuple>) -> PyResult<PyObject> {
        // 我们把 FolderSubject 自身作为 "observable" 来链式调用
        // 在 Python 端，pipe 的语义是依次对 self 调用每个 operator
        // 简化：将每个 operator 作用于当前 subject-like 对象，返回最后一个结果

        // 先包装成 Python 可调用的 "FolderSubject-like" 对象
        // 由于 FolderSubject 自身已经实现 subscribe，它就是 observable-like 的
        let self_obj: PyObject = Py::new(py, Self {
            dispatcher: self.dispatcher.clone(),
            subscribed: self.subscribed.clone(),
        })?
        .into_any();

        let mut current = self_obj;

        for op in operators.iter() {
            // 每个 operator 是 callable，接受 (source) -> new_observable
            let result = op.call1((current,))?;
            current = result;
        }

        Ok(current)
    }

    fn __enter__<'py>(slf: PyRef<'py, Self>) -> PyRef<'py, Self> {
        Python::with_gil(|py| {
            let _ = slf.start(py);
        });
        slf
    }

    fn __exit__<'py>(slf: PyRef<'py, Self>, _exc_type: PyObject, _exc_val: PyObject, _tb: PyObject) -> bool {
        Python::with_gil(|py| {
            let _ = slf.stop(py);
        });
        false
    }
}

// ============================================================================
// FolderObserver - 钩子式观察者（按事件类型路由回调）
// ============================================================================

struct FolderObserverHooks {
    on_created: Option<PyObject>,
    on_deleted: Option<PyObject>,
    on_renamed: Option<PyObject>,
    on_moved_in: Option<PyObject>,
    on_moved_out: Option<PyObject>,
    on_attrib: Option<PyObject>,
    on_content: Option<PyObject>,
    on_any: Option<PyObject>,
    on_error: Option<PyObject>,
    // 动态钩子（可 add/remove）
    dynamic_hooks: std::collections::HashMap<u32, Vec<PyObject>>,
    dynamic_any: Vec<PyObject>,
}

#[pyclass]
pub struct FolderObserver {
    hooks: Arc<Mutex<FolderObserverHooks>>,
    subscribed: Arc<Mutex<bool>>,
    subscription_refs: Arc<Mutex<Vec<PyObject>>>,
}

#[pymethods]
impl FolderObserver {
    #[new]
    #[pyo3(signature = (on_created=None, on_deleted=None, on_renamed=None, on_moved_in=None, on_moved_out=None, on_attrib=None, on_content=None, on_any=None, on_error=None))]
    fn new(
        on_created: Option<PyObject>,
        on_deleted: Option<PyObject>,
        on_renamed: Option<PyObject>,
        on_moved_in: Option<PyObject>,
        on_moved_out: Option<PyObject>,
        on_attrib: Option<PyObject>,
        on_content: Option<PyObject>,
        on_any: Option<PyObject>,
        on_error: Option<PyObject>,
    ) -> Self {
        Self {
            hooks: Arc::new(Mutex::new(FolderObserverHooks {
                on_created,
                on_deleted,
                on_renamed,
                on_moved_in,
                on_moved_out,
                on_attrib,
                on_content,
                on_any,
                on_error,
                dynamic_hooks: std::collections::HashMap::new(),
                dynamic_any: Vec::new(),
            })),
            subscribed: Arc::new(Mutex::new(false)),
            subscription_refs: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn __call__(&self, fd: PyObject, py: Python<'_>) -> PyResult<()> {
        // 从 fd 对象取 change_type
        let ct: u32 = match fd.getattr(py, "change_type") {
            Ok(v) => v.extract(py).unwrap_or(FolderChangeType::CONTENT),
            Err(_) => FolderChangeType::CONTENT,
        };

        // 收集所有要调用的钩子
        let (any_hooks, specific_hooks, error_cb) = {
            let h = self.hooks.lock().unwrap();
            let mut any = Vec::new();
            if let Some(cb) = &h.on_any {
                any.push(cb.clone_ref(py));
            }
            for cb in &h.dynamic_any {
                any.push(cb.clone_ref(py));
            }

            let mut specific = Vec::new();
            let named_cb = match ct {
                0 => &h.on_created,
                1 => &h.on_deleted,
                2 => &h.on_renamed,
                3 => &h.on_moved_in,
                4 => &h.on_moved_out,
                5 => &h.on_attrib,
                6 => &h.on_content,
                _ => &None,
            };
            if let Some(cb) = named_cb {
                specific.push(cb.clone_ref(py));
            }
            if let Some(dyn_list) = h.dynamic_hooks.get(&ct) {
                for cb in dyn_list {
                    specific.push(cb.clone_ref(py));
                }
            }

            let err_cb = h.on_error.clone();
            (any, specific, err_cb)
        };

        // 先 on_any
        for cb in &any_hooks {
            if let Err(_e) = cb.call1(py, (fd.clone_ref(py),)) {
                if let Some(err_cb) = &error_cb {
                    let _ = err_cb.call1(py, (_e.clone_ref(py),));
                }
            }
        }
        // 再类型特定钩子
        for cb in &specific_hooks {
            if let Err(_e) = cb.call1(py, (fd.clone_ref(py),)) {
                if let Some(err_cb) = &error_cb {
                    let _ = err_cb.call1(py, (_e.clone_ref(py),));
                }
            }
        }
        Ok(())
    }

    fn subscribe(&self, observable: PyObject, py: Python<'_>) -> PyResult<PyObject> {
        // 将 self 的 __call__ 包装成 Python callable，传入 observable.subscribe
        let wrapper = FolderObserverWrapper {
            hooks: self.hooks.clone(),
        };
        let wrapper_py = Py::new(py, wrapper)?.into_any();
        let result = observable.call_method1(py, "subscribe", (wrapper_py,))?;
        *self.subscribed.lock().unwrap() = true;
        self.subscription_refs.lock().unwrap().push(result.clone_ref(py));
        Ok(result)
    }

    fn attach(&self, subject: PyObject, py: Python<'_>) -> PyResult<PyObject> {
        let sub = self.subscribe(subject, py)?;
        // 返回 self 的 Python 对象
        let self_obj = Py::new(
            py,
            FolderObserver {
                hooks: self.hooks.clone(),
                subscribed: self.subscribed.clone(),
                subscription_refs: self.subscription_refs.clone(),
            },
        )?
        .into_any();
        // 保留订阅句柄
        let _ = sub;
        Ok(self_obj)
    }

    fn unsubscribe(&self) {
        // 通知 Python 端订阅对象 dispose（如果有）
        let refs = self.subscription_refs.lock().unwrap().clone();
        for r in refs {
            Python::with_gil(|py| {
                if let Ok(dispose) = r.getattr(py, "dispose") {
                    let _ = dispose.call0(py);
                }
            });
        }
        *self.subscribed.lock().unwrap() = false;
        self.subscription_refs.lock().unwrap().clear();
    }

    #[getter]
    fn is_subscribed(&self) -> bool {
        *self.subscribed.lock().unwrap()
    }

    // ---- 动态钩子 ----
    #[pyo3(signature = (change_type_or_name, hook))]
    fn add_hook(&self, change_type_or_name: PyObject, hook: PyObject, py: Python<'_>) -> PyResult<()> {
        let ct = parse_change_type_arg(change_type_or_name, py)?;
        let mut h = self.hooks.lock().unwrap();
        h.dynamic_hooks.entry(ct).or_insert_with(Vec::new).push(hook);
        Ok(())
    }

    #[pyo3(signature = (change_type_or_name, hook))]
    fn remove_hook(&self, change_type_or_name: PyObject, hook: PyObject, py: Python<'_>) -> PyResult<bool> {
        let ct = parse_change_type_arg(change_type_or_name, py)?;
        let mut h = self.hooks.lock().unwrap();
        let removed = if let Some(vec) = h.dynamic_hooks.get_mut(&ct) {
            let before = vec.len();
            // 通过 repr 比较匹配（PyObject 无法直接 Eq 比较）
            vec.retain(|existing| !py_objs_equal(py, existing, &hook));
            vec.len() < before
        } else {
            false
        };
        Ok(removed)
    }

    fn clear_hooks(&self) {
        let mut h = self.hooks.lock().unwrap();
        h.dynamic_hooks.clear();
        h.dynamic_any.clear();
    }

    fn __enter__<'py>(slf: PyRef<'py, Self>) -> PyRef<'py, Self> {
        slf
    }

    fn __exit__<'py>(slf: PyRef<'py, Self>, _exc_type: PyObject, _exc_val: PyObject, _tb: PyObject) -> bool {
        slf.unsubscribe();
        false
    }
}

// 辅助：解析 change_type 参数（int 或 str）
fn parse_change_type_arg(obj: PyObject, py: Python<'_>) -> PyResult<u32> {
    if let Ok(ct) = obj.extract::<u32>(py) {
        return Ok(ct);
    }
    if let Ok(name) = obj.extract::<String>(py) {
        let upper = name.to_uppercase();
        match upper.as_str() {
            "CREATED" | "FOLDER_CREATED" | "0" => Ok(FolderChangeType::CREATED),
            "DELETED" | "FOLDER_DELETED" | "1" => Ok(FolderChangeType::DELETED),
            "RENAMED" | "FOLDER_RENAMED" | "2" => Ok(FolderChangeType::RENAMED),
            "MOVED_IN" | "FOLDER_MOVED_IN" | "3" => Ok(FolderChangeType::MOVED_IN),
            "MOVED_OUT" | "FOLDER_MOVED_OUT" | "4" => Ok(FolderChangeType::MOVED_OUT),
            "ATTRIB" | "FOLDER_ATTRIB" | "5" => Ok(FolderChangeType::ATTRIB),
            "CONTENT" | "FOLDER_CONTENT" | "6" => Ok(FolderChangeType::CONTENT),
            _ => Err(pyo3::exceptions::PyValueError::new_err(format!(
                "Unknown change_type: {}",
                name
            ))),
        }
    } else {
        Err(pyo3::exceptions::PyValueError::new_err(
            "change_type_or_name must be int or str",
        ))
    }
}

// 辅助：比较两个 PyObject 钩子是否"相等"（通过 identity + repr 的启发式）
fn py_objs_equal(py: Python<'_>, a: &PyObject, b: &PyObject) -> bool {
    // 先用 id
    let id_a: Result<usize, _> = a.getattr(py, "__hash__").and_then(|f| f.call0(py)?.extract(py));
    let id_b: Result<usize, _> = b.getattr(py, "__hash__").and_then(|f| f.call0(py)?.extract(py));
    if let (Ok(a), Ok(b)) = (id_a, id_b) {
        if a == b {
            return true;
        }
    }
    // 退而求其次：用 is 比较
    a.bind(py).is(b.bind(py))
}

// ============================================================================
// FolderObserverWrapper - 用于 subscribe 的 Python callable 桥
// ============================================================================

#[pyclass]
struct FolderObserverWrapper {
    hooks: Arc<Mutex<FolderObserverHooks>>,
}

#[pymethods]
impl FolderObserverWrapper {
    fn __call__(&self, fd: PyObject, py: Python<'_>) -> PyResult<()> {
        let ct: u32 = fd.getattr(py, "change_type")?.extract(py).unwrap_or(FolderChangeType::CONTENT);

        let (any_hooks, specific_hooks, error_cb) = {
            let h = self.hooks.lock().unwrap();
            let mut any = Vec::new();
            if let Some(cb) = &h.on_any {
                any.push(cb.clone_ref(py));
            }
            for cb in &h.dynamic_any {
                any.push(cb.clone_ref(py));
            }
            let mut specific = Vec::new();
            let named_cb = match ct {
                0 => &h.on_created,
                1 => &h.on_deleted,
                2 => &h.on_renamed,
                3 => &h.on_moved_in,
                4 => &h.on_moved_out,
                5 => &h.on_attrib,
                6 => &h.on_content,
                _ => &None,
            };
            if let Some(cb) = named_cb {
                specific.push(cb.clone_ref(py));
            }
            if let Some(dyn_list) = h.dynamic_hooks.get(&ct) {
                for cb in dyn_list {
                    specific.push(cb.clone_ref(py));
                }
            }
            let err_cb = h.on_error.clone();
            (any, specific, err_cb)
        };

        for cb in &any_hooks {
            if let Err(_e) = cb.call1(py, (fd.clone_ref(py),)) {
                if let Some(err_cb) = &error_cb {
                    let _ = err_cb.call1(py, (_e.clone_ref(py),));
                }
            }
        }
        for cb in &specific_hooks {
            if let Err(_e) = cb.call1(py, (fd.clone_ref(py),)) {
                if let Some(err_cb) = &error_cb {
                    let _ = err_cb.call1(py, (_e.clone_ref(py),));
                }
            }
        }
        Ok(())
    }
}

// ============================================================================
// 顶层工厂函数 & 操作符
// ============================================================================

#[pyfunction]
#[pyo3(signature = (paths=None, backend="auto", change_types=None, tags=None, interval=0.5, auto_start=true))]
fn from_foldersystem(
    paths: Option<Vec<String>>,
    backend: &str,
    change_types: Option<Vec<u32>>,
    tags: Option<Vec<String>>,
    interval: f64,
    auto_start: bool,
    py: Python<'_>,
) -> PyResult<(PyObject, PyObject)> {
    // 创建 FolderSubject，同时返回 subject 和 dispatcher
    let subject = FolderSubject::new(paths, backend, change_types, tags, interval, auto_start, py)?;
    let subject_py = Py::new(py, subject)?.into_any();

    // 获取 dispatcher 引用（通过属性访问）
    let dispatcher_py = subject_py.clone_ref(py);

    Ok((subject_py, dispatcher_py))
}

#[pyfunction]
#[pyo3(signature = (dispatcher=None, mode="create"))]
fn write_to_foldersystem(
    dispatcher: Option<PyObject>,
    mode: &str,
    py: Python<'_>,
) -> PyResult<PyObject> {
    // 返回一个 Python 可调用对象：接受一个 observable，返回一个新 observable
    let op = WriteToFolderOperator {
        mode: mode.to_string(),
        _dispatcher_ref: dispatcher,
    };
    Ok(Py::new(py, op)?.into_any())
}

#[pyclass]
struct WriteToFolderOperator {
    mode: String,
    _dispatcher_ref: Option<PyObject>,
}

#[pymethods]
impl WriteToFolderOperator {
    fn __call__(&self, source: PyObject, py: Python<'_>) -> PyResult<PyObject> {
        let obs = WriteToFolderObservable {
            source: Py::new(py, source.extract::<PyObject>(py)?)?,
            mode: self.mode.clone(),
        };
        Ok(Py::new(py, obs)?.into_any())
    }
}

#[pyclass]
struct WriteToFolderObservable {
    source: Py<PyAny>,
    mode: String,
}

#[pymethods]
impl WriteToFolderObservable {
    fn subscribe(&self, on_next: PyObject, py: Python<'_>) -> PyResult<PyObject> {
        let handler = WriteFolderHandler {
            downstream: Py::new(py, on_next)?,
            mode: self.mode.clone(),
        };
        let handler_py = Py::new(py, handler)?.into_any();
        let source_ref = self.source.bind(py);
        source_ref.call_method1("subscribe", (handler_py,))
    }
}

#[pyclass]
struct WriteFolderHandler {
    downstream: Py<PyAny>,
    mode: String,
}

#[pymethods]
impl WriteFolderHandler {
    fn __call__(&self, item: PyObject, py: Python<'_>) -> PyResult<()> {
        // 支持多种上游: FolderData / dict / str
        let mut path: String = String::new();
        let mut emit_ct: u32 = FolderChangeType::CREATED;

        // FolderData
        if let Ok(fd_ref) = item.extract::<PyRef<FolderData>>(py) {
            path = fd_ref.path.clone();
            emit_ct = fd_ref.change_type;
        } else if let Ok(dict_ref) = item.extract::<&PyDict>(py) {
            // dict: {"path": "...", "mode": "..."}
            if let Ok(p) = dict_ref.get_item("path") {
                if let Ok(s) = p {
                    if let Ok(p_str) = s.extract::<String>() {
                        path = p_str;
                    }
                }
            }
            if let Ok(m) = dict_ref.get_item("change_type") {
                if let Ok(mode_val) = m {
                    if let Ok(ct) = mode_val.extract::<u32>() {
                        emit_ct = ct;
                    }
                }
            }
        } else if let Ok(p_str) = item.extract::<String>(py) {
            path = p_str;
        }

        if path.is_empty() {
            return Ok(());
        }

        let success = match self.mode.as_str() {
            "delete" | "remove" => std::fs::remove_dir(&path).is_ok()
                || std::fs::remove_dir_all(&path).is_ok(),
            "create" | _ => {
                // 创建目录
                std::fs::create_dir_all(&path).is_ok()
            }
        };

        if success {
            let ct = match self.mode.as_str() {
                "delete" | "remove" => FolderChangeType::DELETED,
                _ => FolderChangeType::CREATED,
            };
            let actual_ct = if emit_ct != FolderChangeType::CREATED && emit_ct != FolderChangeType::DELETED {
                ct
            } else {
                emit_ct
            };

            let new_fd = Py::new(
                py,
                FolderData::now(
                    path.clone(),
                    None,
                    actual_ct,
                    None,
                    None,
                    None,
                    None,
                ),
            )?;
            let _ = self.downstream.call1(py, (new_fd,));
        }
        Ok(())
    }
}

// ============================================================================
// 模块注册辅助
// ============================================================================

pub fn add_folder_watcher_to_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<FolderChangeType>()?;
    m.add_class::<FolderData>()?;
    m.add_class::<FolderDispatcher>()?;
    m.add_class::<FolderSubscriptionHandle>()?;
    m.add_class::<FolderSubject>()?;
    m.add_class::<FolderObserver>()?;
    m.add_function(wrap_pyfunction!(from_foldersystem, m)?)?;
    m.add_function(wrap_pyfunction!(write_to_foldersystem, m)?)?;
    Ok(())
}

#[pyclass]
pub struct FolderOpsModule {}

#[pymethods]
impl FolderOpsModule {
    #[staticmethod]
    #[pyo3(signature = (paths=None, backend="auto", change_types=None, tags=None, interval=0.5, auto_start=true))]
    fn from_foldersystem(
        paths: Option<Vec<String>>,
        backend: &str,
        change_types: Option<Vec<u32>>,
        tags: Option<Vec<String>>,
        interval: f64,
        auto_start: bool,
        py: Python<'_>,
    ) -> PyResult<(PyObject, PyObject)> {
        from_foldersystem(paths, backend, change_types, tags, interval, auto_start, py)
    }

    #[staticmethod]
    #[pyo3(signature = (dispatcher=None, mode="create"))]
    fn write_to_foldersystem(dispatcher: Option<PyObject>, mode: &str, py: Python<'_>) -> PyResult<PyObject> {
        write_to_foldersystem(dispatcher, mode, py)
    }
}

// ============================================================================
// Python 子模块（让 Python 端可以通过 rx_rust.folder_watcher 访问）
// ============================================================================

#[pymodule]
fn rx_rust_folder_watcher(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    add_folder_watcher_to_module(m)?;
    Ok(())
}
