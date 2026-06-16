// ============================================================================
// rx-rust-py file_watcher - 文件系统监控（Rust 实现）
//
// 使用 notify crate（跨平台）：
//   - Windows: ReadDirectoryChangesW
//   - Linux:   inotify
//   - macOS:   FSEvents
//   - 其他:    Polling 回退
// ============================================================================

use notify::{
    event::{Event as NotifyEvent, EventKind, ModifyKind, RenameMode},
    RecommendedWatcher, RecursiveMode, Watcher,
};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;

// ============================================================================
// FileChangeType - 文件变更类型枚举
// ============================================================================

#[pyclass]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileChangeType(pub u32);

impl FileChangeType {
    pub fn to_name(&self) -> &'static str {
        match self.0 {
            0 => "CREATED",
            1 => "MODIFIED",
            2 => "DELETED",
            3 => "RENAMED",
            4 => "MOVED_IN",
            5 => "MOVED_OUT",
            6 => "ACCESS",
            7 => "ATTRIB",
            _ => "UNKNOWN",
        }
    }
}

#[pymethods]
impl FileChangeType {
    #[classattr]
    const CREATED: u32 = 0;
    #[classattr]
    const MODIFIED: u32 = 1;
    #[classattr]
    const DELETED: u32 = 2;
    #[classattr]
    const RENAMED: u32 = 3;
    #[classattr]
    const MOVED_IN: u32 = 4;
    #[classattr]
    const MOVED_OUT: u32 = 5;
    #[classattr]
    const ACCESS: u32 = 6;
    #[classattr]
    const ATTRIB: u32 = 7;

    #[new]
    fn new(value: u32) -> Self {
        Self(value)
    }

    fn __int__(&self) -> u32 {
        self.0
    }

    fn __str__(&self) -> &'static str {
        self.to_name()
    }

    fn __repr__(&self) -> String {
        format!("FileChangeType.{}({})", self.to_name(), self.0)
    }

    fn __eq__(&self, other: PyObject, py: Python<'_>) -> PyObject {
        if let Ok(val) = other.extract::<u32>(py) {
            return (self.0 == val).to_object(py).into_any();
        }
        if let Ok(ft) = other.extract::<PyRef<FileChangeType>>(py) {
            return (self.0 == ft.0).to_object(py).into_any();
        }
        false.to_object(py).into_any()
    }
}

// ============================================================================
// FileData - 结构化文件事件数据
// ============================================================================

static SEQUENCE_COUNTER: AtomicU64 = AtomicU64::new(1);

#[pyclass]
pub struct FileData {
    #[pyo3(get)]
    pub path: String,
    #[pyo3(get)]
    pub old_path: Option<String>,
    #[pyo3(get)]
    pub change_type: u32,
    #[pyo3(get)]
    pub is_directory: bool,
    #[pyo3(get)]
    pub size: Option<u64>,
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
impl FileData {
    #[new]
    #[pyo3(signature = (path, old_path=None, change_type=FileChangeType::MODIFIED, is_directory=false, size=None, timestamp=None, sequence=None, tags=None, metadata=None))]
    fn new(
        path: String,
        old_path: Option<String>,
        change_type: u32,
        is_directory: bool,
        size: Option<u64>,
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
            is_directory,
            size,
            timestamp: ts,
            sequence: sequence.unwrap_or_else(|| SEQUENCE_COUNTER.fetch_add(1, Ordering::SeqCst)),
            tags: tags.unwrap_or_default(),
            metadata: metadata.unwrap_or_default(),
        }
    }

    #[staticmethod]
    #[pyo3(signature = (path, old_path=None, change_type=FileChangeType::MODIFIED, is_directory=false, size=None, tags=None, metadata=None))]
    fn now(
        path: String,
        old_path: Option<String>,
        change_type: u32,
        is_directory: bool,
        size: Option<u64>,
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
            is_directory,
            size,
            timestamp: ts,
            sequence: SEQUENCE_COUNTER.fetch_add(1, Ordering::SeqCst),
            tags: tags.unwrap_or_default(),
            metadata: metadata.unwrap_or_default(),
        }
    }

    fn to_dict(&self, py: Python<'_>) -> Py<PyDict> {
        let d = PyDict::new_bound(py);
        let _ = d.set_item("path", &self.path);
        let _ = d.set_item("old_path", self.old_path.clone());
        let _ = d.set_item("change_type", self.change_type);
        let _ = d.set_item("change_type_name", FileChangeType(self.change_type).to_name());
        let _ = d.set_item("is_directory", self.is_directory);
        let _ = d.set_item("size", self.size);
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
        let path: String = d_borrowed.get_item("path")?.map_or_else(|| Ok(String::new()), |v| v.extract())?;
        let old_path: Option<String> = d_borrowed.get_item("old_path")?.map_or_else(|| Ok(None), |v| v.extract())?;
        let change_type: u32 = d_borrowed.get_item("change_type")?.map_or_else(|| Ok(FileChangeType::MODIFIED), |v| v.extract())?;
        let is_directory: bool = d_borrowed.get_item("is_directory")?.map_or_else(|| Ok(false), |v| v.extract())?;
        let size: Option<u64> = d_borrowed.get_item("size")?.map_or_else(|| Ok(None), |v| v.extract())?;
        let timestamp: f64 = d_borrowed.get_item("timestamp")?.map_or_else(|| {
            let now = std::time::SystemTime::now();
            Ok::<f64, PyErr>(now.duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs_f64())
        }, |v| v.extract())?;
        let sequence: u64 = d_borrowed.get_item("sequence")?.map_or_else(|| Ok(SEQUENCE_COUNTER.fetch_add(1, Ordering::SeqCst)), |v| v.extract())?;
        let tags: Vec<String> = d_borrowed.get_item("tags")?.map_or_else(|| Ok(Vec::new()), |v| v.extract())?;
        let meta_dict: std::collections::HashMap<String, String> = d_borrowed
            .get_item("metadata")?
            .map_or_else(|| Ok(std::collections::HashMap::new()), |v| v.extract())?;

        Ok(Self {
            path,
            old_path,
            change_type,
            is_directory,
            size,
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
            "FileData(path={:?}, change_type={}, is_directory={}, size={:?}, seq={})",
            self.path,
            FileChangeType(self.change_type).to_name(),
            self.is_directory,
            self.size,
            self.sequence
        )
    }

    fn __repr__(&self) -> String {
        self.__str__()
    }
}

// ============================================================================
// FileDispatcher - 文件监控与事件分发核心
// ============================================================================

type ObserverCallback = Arc<dyn Fn(Py<FileData>) + Send + Sync>;

struct DispatcherState {
    watcher: Option<RecommendedWatcher>,
    observers: Vec<ObserverCallback>,
    paths: Vec<PathBuf>,
    change_types: Option<std::collections::HashSet<u32>>,
    dispatch_count: u64,
    error_count: u64,
    running: bool,
    pending_rename_from: Option<PathBuf>,
}

#[pyclass]
pub struct FileDispatcher {
    state: Arc<Mutex<DispatcherState>>,
    backend_name: String,
    interval_ms: u64,
}

fn map_event_kind_to_change_type(kind: EventKind) -> u32 {
    match kind {
        EventKind::Access(_) => FileChangeType::ACCESS,
        EventKind::Create(_) => FileChangeType::CREATED,
        EventKind::Modify(ModifyKind::Data(_)) => FileChangeType::MODIFIED,
        EventKind::Modify(ModifyKind::Metadata(_)) => FileChangeType::ATTRIB,
        EventKind::Modify(_) => FileChangeType::MODIFIED,
        EventKind::Remove(_) => FileChangeType::DELETED,
        EventKind::Any => FileChangeType::MODIFIED,
        EventKind::Other => FileChangeType::MODIFIED,
    }
}

#[pymethods]
impl FileDispatcher {
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
        let (watcher_opt, bname) = match notify::RecommendedWatcher::new(
            |res: Result<NotifyEvent, notify::Error>| {
                if let Ok(ev) = res {
                    // 处理事件
                    let paths = ev.paths.clone();
                    let kind = ev.kind;

                    // 简单处理：对每个路径派发一个事件
                    for p in &paths {
                        let _ = (p, kind); // just to suppress unused warnings for now
                    }
                }
            },
            notify::Config::default(),
        ) {
            Ok(w) => (Some(w), String::from("native")),
            Err(_) => (None, String::from("polling")),
        };

        let backend_name = if backend == "polling" {
            String::from("polling")
        } else {
            bname
        };

        let state = Arc::new(Mutex::new(DispatcherState {
            watcher: watcher_opt,
            observers: Vec::new(),
            paths: paths_pb,
            change_types: ct_set,
            dispatch_count: 0,
            error_count: 0,
            running: false,
            pending_rename_from: None,
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
        let ct_set = st.change_types.clone();
        let dispatch_state = self.state.clone();
        let backend_name = self.backend_name.clone();
        let interval_ms = self.interval_ms;

        // 创建观察者回调链 - 使用闭包
        // notify 回调在后台线程，回调中需调用 Python 对象
        // 我们不直接调用 Python，而是将事件放入队列，用另一个线程分发
        let (tx, rx) = std::sync::mpsc::channel::<(PathBuf, Option<PathBuf>, u32, bool, Option<u64>)>();
        // rx 用于后台线程
        let rx_state = dispatch_state.clone();
        // 需要持有 GIL 来调用 Python 回调

        // 注意：notify 的回调和 Python 回调需要分开
        // - notify 回调运行在 notify 后台线程（无 GIL）
        // - 我们建立一个分发线程，持有 GIL 分发事件

        // 用 notify 的标准方式注册回调
        let tx_clone = tx.clone();
        let tx_for_error = tx.clone();
        let _ = tx_for_error;

        let watcher_result = notify::RecommendedWatcher::new(
            move |res: Result<NotifyEvent, notify::Error>| {
                if let Ok(ev) = res {
                    let ct = map_event_kind_to_change_type(ev.kind);

                    // 处理 RENAME 特殊逻辑
                    let is_rename_from = matches!(
                        ev.kind,
                        EventKind::Modify(ModifyKind::Name(RenameMode::From))
                    );
                    let is_rename_to = matches!(
                        ev.kind,
                        EventKind::Modify(ModifyKind::Name(RenameMode::To))
                    );

                    if is_rename_from {
                        // 记录 from，等待后续 to 事件
                        if let Some(p) = ev.paths.first() {
                            // 用一个局部静态暂存
                            // 但因为这里是独立的闭包，我们用线程局部变量
                            // 简单策略：立即派发 DELETED，下游再做合并；或
                            // 我们用一个简单变量：通过 dispatch_state.lock().pending_rename_from 处理
                            let mut guard = rx_state.lock().unwrap();
                            guard.pending_rename_from = Some(p.clone());
                        }
                    } else if is_rename_to {
                        // 查找 pending 的 from
                        let (old_path_opt, new_path_opt) = {
                            let mut guard = rx_state.lock().unwrap();
                            let old = guard.pending_rename_from.take();
                            let new = ev.paths.first().cloned();
                            (old, new)
                        };
                        if let (Some(old), Some(new)) = (old_path_opt, new_path_opt) {
                            let is_dir = std::path::Path::new(&new).is_dir();
                            let size = if is_dir {
                                None
                            } else {
                                std::fs::metadata(&new).ok().map(|m| m.len())
                            };
                            let _ = tx_clone.send((new, Some(old), FileChangeType::RENAMED, is_dir, size));
                        } else if let Some(new) = ev.paths.first().cloned() {
                            let is_dir = std::path::Path::new(&new).is_dir();
                            let size = if is_dir {
                                None
                            } else {
                                std::fs::metadata(&new).ok().map(|m| m.len())
                            };
                            let _ = tx_clone.send((new, None, FileChangeType::CREATED, is_dir, size));
                        }
                    } else {
                        for p in &ev.paths {
                            let is_dir = std::path::Path::new(p).is_dir();
                            let size = if ct == FileChangeType::DELETED || ct == FileChangeType::MOVED_OUT || is_dir {
                                None
                            } else {
                                std::fs::metadata(p).ok().map(|m| m.len())
                            };
                            let _ = tx_clone.send((p.clone(), None, ct, is_dir, size));
                        }
                    }
                }
            },
            notify::Config::default(),
        );

        match watcher_result {
            Ok(mut watcher) => {
                // 注册要监控的路径
                for p in &paths_to_watch {
                    if p.exists() {
                        let _ = watcher.watch(p, RecursiveMode::Recursive);
                    }
                }
                st.watcher = Some(watcher);
            }
            Err(e) => {
                println!("[file_watcher] 创建 watcher 失败: {:?}", e);
                // 用 polling 回退模式
            }
        }

        st.running = true;
        // 释放锁，避免死锁

        // 启动后台线程处理事件队列
        let rx_state_for_thread = self.state.clone();
        let _ct_filter = ct_set.clone();

        // 对于 polling 后端：如果 watcher 创建失败，则启用 polling 线程
        let polling_needed = {
            let st_guard = self.state.lock().unwrap();
            st_guard.watcher.is_none() && !paths_to_watch.is_empty()
        };

        if polling_needed {
            let paths_poll = paths_to_watch.clone();
            let state_clone = self.state.clone();
            let interval = std::time::Duration::from_millis(interval_ms);
            let tx_poll = tx.clone();

            // polling 线程
            thread::spawn(move || {
                // 初始快照
                let mut snapshot: std::collections::HashMap<PathBuf, (std::time::SystemTime, u64)> =
                    std::collections::HashMap::new();

                fn scan_directory(
                    base: &PathBuf,
                    snap: &mut std::collections::HashMap<PathBuf, (std::time::SystemTime, u64)>,
                ) {
                    if let Ok(entries) = std::fs::read_dir(base) {
                        for entry in entries.flatten() {
                            let path = entry.path();
                            if let Ok(meta) = std::fs::metadata(&path) {
                                let mtime = meta.modified().unwrap_or(std::time::SystemTime::UNIX_EPOCH);
                                let size = meta.len();
                                let path_clone = path.clone();
                                snap.insert(path, (mtime, size));
                                if meta.is_dir() {
                                    scan_directory(&path_clone, snap);
                                }
                            }
                        }
                    }
                }

                // 建立初始快照
                for p in &paths_poll {
                    scan_directory(p, &mut snapshot);
                }

                loop {
                    {
                        let st_guard = state_clone.lock().unwrap();
                        if !st_guard.running {
                            break;
                        }
                    }

                    thread::sleep(interval);

                    let mut new_snap: std::collections::HashMap<PathBuf, (std::time::SystemTime, u64)> =
                        std::collections::HashMap::new();
                    for p in &paths_poll {
                        scan_directory(p, &mut new_snap);
                    }

                    // 对比
                    let mut events: Vec<(PathBuf, Option<PathBuf>, u32, bool, Option<u64>)> = Vec::new();
                    for (path, &(mtime, size)) in &new_snap {
                        let is_dir = path.is_dir();
                        let size_opt = if is_dir { None } else { Some(size) };
                        match snapshot.get(path) {
                            None => events.push((path.clone(), None, FileChangeType::CREATED, is_dir, size_opt)),
                            Some(&(old_mtime, _)) => {
                                if mtime != old_mtime {
                                    events.push((path.clone(), None, FileChangeType::MODIFIED, is_dir, size_opt));
                                }
                            }
                        }
                    }
                    for path in snapshot.keys() {
                        if !new_snap.contains_key(path) {
                            let is_dir = path.is_dir();
                            events.push((path.clone(), None, FileChangeType::DELETED, is_dir, None));
                        }
                    }

                    for ev in events {
                        let _ = tx_poll.send(ev);
                    }

                    snapshot = new_snap;
                }
            });
        }

        // 启动事件分发线程：把 channel 中的事件变成 FileData 并分发给 Python 回调
        let state_clone_dispatch = self.state.clone();
        thread::spawn(move || {
            let start_time = Instant::now();
            loop {
                // 检查 running 状态
                {
                    let st_guard = state_clone_dispatch.lock().unwrap();
                    if !st_guard.running {
                        break;
                    }
                }

                // 带超时的接收，使我们能定期检查 running 状态
                match rx.recv_timeout(std::time::Duration::from_millis(200)) {
                    Ok((path, old_path, change_type, is_directory, size)) => {
                        // 获取观察者快照 & change_types 过滤
                        let (observers, ct_filter, should_dispatch, tags_clone) = {
                            let st_guard = state_clone_dispatch.lock().unwrap();
                            let filter = st_guard.change_types.clone();
                            let should = filter.as_ref().map_or(true, |f| f.contains(&change_type));
                            let obs: Vec<ObserverCallback> = st_guard.observers.clone();
                            let tags: Vec<String> = vec![];
                            (obs, filter, should, tags)
                        };

                        if !should_dispatch {
                            continue;
                        }

                        // 创建 FileData 并分发
                        Python::with_gil(|py| {
                            if let Ok(fd) = Py::new(
                                py,
                                FileData::now(
                                    path.to_string_lossy().into_owned(),
                                    old_path.map(|p| p.to_string_lossy().into_owned()),
                                    change_type,
                                    is_directory,
                                    size,
                                    Some(tags_clone),
                                    None,
                                ),
                            ) {
                                // 分发到每个观察者
                                for obs_cb in &observers {
                                    obs_cb(fd.clone_ref(py));
                                }
                                let mut st_guard = state_clone_dispatch.lock().unwrap();
                                st_guard.dispatch_count += 1;
                            } else {
                                let mut st_guard = state_clone_dispatch.lock().unwrap();
                                st_guard.error_count += 1;
                            }
                        });
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                }

                // 防止线程无事件时无限跑
                if start_time.elapsed().as_secs() > 24 * 60 * 60 {
                    // 24小时上限
                    break;
                }
            }
        });

        Ok(())
    }

    fn stop(&self) {
        let mut st = self.state.lock().unwrap();
        st.running = false;
        if let Some(mut w) = st.watcher.take() {
            for p in &st.paths {
                let _ = w.unwatch(p);
            }
        }
    }

    fn subscribe(&self, on_next: PyObject, py: Python<'_>) -> PyResult<PyObject> {
        // 创建订阅句柄
        let disposed = Arc::new(Mutex::new(false));
        let disposed_clone = disposed.clone();

        // 保存回调 - 我们需要一个能在后台线程调用的闭包
        // 使用 Py<PyObject> + Python::with_gil
        let cb = on_next.clone_ref(py);

        let observer_cb: ObserverCallback = Arc::new(move |fd: Py<FileData>| {
            Python::with_gil(|py| {
                let _ = cb.call1(py, (fd,));
            });
        });

        // 注册
        let mut st = self.state.lock().unwrap();
        st.observers.push(observer_cb);
        // 观察者 id
        let observer_index = st.observers.len();

        // 订阅对象 - Python 端可见
        let sub = SubscriptionHandle {
            index: observer_index,
            state: self.state.clone(),
            disposed: disposed_clone,
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
// SubscriptionHandle - 订阅句柄（简化版，与现有 Observable 兼容）
// ============================================================================

#[pyclass]
pub struct SubscriptionHandle {
    index: usize,
    state: Arc<Mutex<DispatcherState>>,
    disposed: Arc<Mutex<bool>>,
}

#[pymethods]
impl SubscriptionHandle {
    fn dispose(&self) {
        *self.disposed.lock().unwrap() = true;
        // 从观察者列表中移除
        let mut st = self.state.lock().unwrap();
        if self.index <= st.observers.len() && self.index > 0 {
            st.observers.remove(self.index - 1);
        }
    }

    fn is_disposed(&self) -> bool {
        *self.disposed.lock().unwrap()
    }
}

// ============================================================================
// FileSubject - 自含 Dispatcher 的 Subject
// ============================================================================

#[pyclass]
pub struct FileSubject {
    dispatcher: Py<FileDispatcher>,
}

#[pymethods]
impl FileSubject {
    #[new]
    #[pyo3(signature = (paths=None, backend="auto", change_types=None, interval=0.5))]
    fn new(
        paths: Option<Vec<String>>,
        backend: &str,
        change_types: Option<Vec<u32>>,
        interval: f64,
        py: Python<'_>,
    ) -> PyResult<Self> {
        let dispatcher = Py::new(py, FileDispatcher::new(paths, backend, change_types, interval))?;
        let subject = Self { dispatcher };
        // 手动启动
        let disp_borrow = subject.dispatcher.bind(py);
        let _ = disp_borrow.call_method0("start")?;
        Ok(subject)
    }

    #[getter]
    fn dispatcher<'py>(&self, py: Python<'py>) -> Py<FileDispatcher> {
        self.dispatcher.clone_ref(py)
    }

    #[getter]
    fn backend_name(&self, py: Python<'_>) -> String {
        let d = self.dispatcher.bind(py);
        d.getattr("backend_name").unwrap().extract().unwrap_or_default()
    }

    #[getter]
    fn dispatch_count(&self, py: Python<'_>) -> u64 {
        let d = self.dispatcher.bind(py);
        d.getattr("dispatch_count").unwrap().extract().unwrap_or(0)
    }

    #[getter]
    fn is_running(&self, py: Python<'_>) -> bool {
        let d = self.dispatcher.bind(py);
        d.getattr("is_running").unwrap().extract().unwrap_or(false)
    }

    fn subscribe(&self, on_next: PyObject, py: Python<'_>) -> PyResult<PyObject> {
        let d = self.dispatcher.bind(py);
        Ok(d.call_method1("subscribe", (on_next,))?.unbind().into_any())
    }

    fn start(&self, py: Python<'_>) -> PyResult<()> {
        let d = self.dispatcher.bind(py);
        d.call_method0("start")?;
        Ok(())
    }

    fn stop(&self, py: Python<'_>) -> PyResult<()> {
        let d = self.dispatcher.bind(py);
        d.call_method0("stop")?;
        Ok(())
    }

    fn add_path(&self, path: String, py: Python<'_>) -> PyResult<()> {
        let d = self.dispatcher.bind(py);
        d.call_method1("add_path", (path,))?;
        Ok(())
    }

    fn remove_path(&self, path: String, py: Python<'_>) -> PyResult<()> {
        let d = self.dispatcher.bind(py);
        d.call_method1("remove_path", (path,))?;
        Ok(())
    }

    fn __enter__<'py>(slf: PyRef<'py, Self>) -> PyRef<'py, Self> {
        let py = slf.py();
        let _ = slf.start(py);
        // slf 已经返回
        std::mem::drop(slf);
        // 重新获取 - 简化：返回原始的引用，不做任何事
        // 由于 Rust 的生命周期，我们通过 Python 端调用 start
        Python::with_gil(|py| {
            let d = slf_placeholder_get(py);
            let _ = d;
        });
        // 简化处理：只返回 slf
        unimplemented!()
    }

    fn __exit__(&self, py: Python<'_>, _exc_type: PyObject, _exc_val: PyObject, _tb: PyObject) -> bool {
        let _ = self.stop(py);
        false
    }
}

fn slf_placeholder_get(py: Python<'_>) -> usize {
    let _ = py;
    0
}

// ============================================================================
// FileObserver - 按 FileChangeType 路由回调的便捷观察者
// ============================================================================

#[pyclass]
pub struct FileObserver {
    callbacks: Arc<Mutex<FileObserverCallbacks>>,
    subscribed: Arc<Mutex<bool>>,
}

struct FileObserverCallbacks {
    on_created: Option<PyObject>,
    on_modified: Option<PyObject>,
    on_deleted: Option<PyObject>,
    on_renamed: Option<PyObject>,
    on_moved_in: Option<PyObject>,
    on_moved_out: Option<PyObject>,
    on_access: Option<PyObject>,
    on_attrib: Option<PyObject>,
    on_any: Option<PyObject>,
    on_error: Option<PyObject>,
}

#[pymethods]
impl FileObserver {
    #[new]
    #[pyo3(signature = (on_created=None, on_modified=None, on_deleted=None, on_renamed=None, on_moved_in=None, on_moved_out=None, on_access=None, on_attrib=None, on_any=None, on_error=None))]
    fn new(
        on_created: Option<PyObject>,
        on_modified: Option<PyObject>,
        on_deleted: Option<PyObject>,
        on_renamed: Option<PyObject>,
        on_moved_in: Option<PyObject>,
        on_moved_out: Option<PyObject>,
        on_access: Option<PyObject>,
        on_attrib: Option<PyObject>,
        on_any: Option<PyObject>,
        on_error: Option<PyObject>,
    ) -> Self {
        Self {
            callbacks: Arc::new(Mutex::new(FileObserverCallbacks {
                on_created,
                on_modified,
                on_deleted,
                on_renamed,
                on_moved_in,
                on_moved_out,
                on_access,
                on_attrib,
                on_any,
                on_error,
            })),
            subscribed: Arc::new(Mutex::new(false)),
        }
    }

    fn __call__(&self, fd: Py<FileData>, py: Python<'_>) -> PyResult<()> {
        let cb = self.callbacks.lock().unwrap();
        let ct: u32 = fd.bind(py).getattr("change_type")?.extract()?;

        // on_any
        if let Some(any_cb) = &cb.on_any {
            let _ = any_cb.call1(py, (fd.clone_ref(py),));
        }

        let specific_cb = match ct {
            0 => &cb.on_created,
            1 => &cb.on_modified,
            2 => &cb.on_deleted,
            3 => &cb.on_renamed,
            4 => &cb.on_moved_in,
            5 => &cb.on_moved_out,
            6 => &cb.on_access,
            7 => &cb.on_attrib,
            _ => &None,
        };

        if let Some(handler) = specific_cb {
            let _ = handler.call1(py, (fd,));
        }
        Ok(())
    }

    fn subscribe(&self, observable: PyObject, py: Python<'_>) -> PyResult<PyObject> {
        // observable 应该是 FileSubject 或 FileDispatcher
        let cb_ptr = self.callbacks.clone();
        let sub_ref = self.subscribed.clone();

        // 我们需要一个能被 observable.subscribe 接受的可调用对象
        // 用 Python 中的 lambda 来桥接 - 或者直接传入 FileObserver 作为回调
        // 简化：在 Python 中传入一个包装 lambda
        let result = observable.call_method1(py, "subscribe", (self_2_py_callback(&cb_ptr, py),))?;
        *sub_ref.lock().unwrap() = true;
        Ok(result)
    }

    fn attach(&self, subject: PyObject, py: Python<'_>) -> Py<Self> {
        let _ = self.subscribe(subject, py);
        Py::new(py, self.clone_py(py)).unwrap()
    }

    fn unsubscribe(&self) {
        *self.subscribed.lock().unwrap() = false;
    }

    #[getter]
    fn is_subscribed(&self) -> bool {
        *self.subscribed.lock().unwrap()
    }

    fn __enter__<'py>(slf: PyRef<'py, Self>) -> PyRef<'py, Self> {
        slf
    }

    fn __exit__<'py>(&self, _exc_type: PyObject, _exc_val: PyObject, _tb: PyObject) -> bool {
        self.unsubscribe();
        false
    }
}

impl FileObserver {
    fn clone_py(&self, _py: Python<'_>) -> Self {
        // 简化：浅拷贝引用（回调中的 PyObject 需要在 Python GIL 下获取）
        // 我们不深拷贝 PyObject，而是共享引用；不过 Arc<Mutex<...>> 会共享
        // 简化方案：返回一个新的 FileObserver，共享 callbacks
        // 但 callbacks 内含 PyObject，它们需要克隆。我们在 Python 层面做：
        // 这是一个简单的设计，只需在 Python 端保存引用即可
        Self {
            callbacks: self.callbacks.clone(),
            subscribed: self.subscribed.clone(),
        }
    }
}

// 辅助：把 FileObserver 包装成 Python 可调用对象
fn self_2_py_callback(
    callbacks: &Arc<Mutex<FileObserverCallbacks>>,
    py: Python<'_>,
) -> PyObject {
    let cb = callbacks.clone();
    // 生成一个 Python 可调用包装对象
    let wrapper = FileObserverWrapper {
        callbacks: cb,
    };
    Py::new(py, wrapper).unwrap().into_any()
}

#[pyclass]
struct FileObserverWrapper {
    callbacks: Arc<Mutex<FileObserverCallbacks>>,
}

#[pymethods]
impl FileObserverWrapper {
    fn __call__(&self, fd: PyObject, py: Python<'_>) -> PyResult<()> {
        let cb = self.callbacks.lock().unwrap();
        let ct: u32 = fd.getattr(py, "change_type")?.extract(py)?;

        if let Some(any_cb) = &cb.on_any {
            let _ = any_cb.call1(py, (fd.clone_ref(py),));
        }

        let specific_cb = match ct {
            0 => &cb.on_created,
            1 => &cb.on_modified,
            2 => &cb.on_deleted,
            3 => &cb.on_renamed,
            4 => &cb.on_moved_in,
            5 => &cb.on_moved_out,
            6 => &cb.on_access,
            7 => &cb.on_attrib,
            _ => &None,
        };

        if let Some(handler) = specific_cb {
            let _ = handler.call1(py, (fd,));
        }
        Ok(())
    }
}

// ============================================================================
// 顶层工厂函数 & 操作符
// ============================================================================

#[pyfunction]
#[pyo3(signature = (paths=None, backend="auto", change_types=None, interval=0.5))]
fn from_filesystem(
    paths: Option<Vec<String>>,
    backend: &str,
    change_types: Option<Vec<u32>>,
    interval: f64,
    py: Python<'_>,
) -> PyResult<(PyObject, PyObject)> {
    // 返回 (Observable-like object, Dispatcher)
    // 简化：返回 FileSubject 本身 + Dispatcher 引用
    let dispatcher = Py::new(py, FileDispatcher::new(paths, backend, change_types, interval))?;
    // 启动
    let _ = dispatcher.bind(py).call_method0("start")?;
    Ok((dispatcher.clone_ref(py).into_any(), dispatcher.into_any()))
}

#[pyfunction]
#[pyo3(signature = (dispatcher, mode="create"))]
fn write_to_filesystem(dispatcher: PyObject, mode: &str, py: Python<'_>) -> PyResult<PyObject> {
    // 返回一个 Python 可调用对象，接受一个 Observable 并返回 Observable
    // 简化：返回一个包装类 WriteToFsOperator

    let op = WriteToFsOperator {
        mode: mode.to_string(),
        _dispatcher_ref: dispatcher.extract::<PyObject>(py)?,
    };

    Ok(Py::new(py, op)?.into_any())
}

#[pyclass]
struct WriteToFsOperator {
    mode: String,
    _dispatcher_ref: PyObject,
}

#[pymethods]
impl WriteToFsOperator {
    fn __call__(&self, source: PyObject, py: Python<'_>) -> PyResult<PyObject> {
        // 创建一个新的 "Observable-like" 对象，它包装 source 的订阅
        // 简化：返回一个 WriteToFsObservable 包装
        let obs = WriteToFsObservable {
            source: source.extract::<PyObject>(py)?,
            mode: self.mode.clone(),
        };
        Ok(Py::new(py, obs)?.into_any())
    }
}

#[pyclass]
struct WriteToFsObservable {
    source: Py<PyAny>,
    mode: String,
}

#[pymethods]
impl WriteToFsObservable {
    fn subscribe(&self, on_next: PyObject, py: Python<'_>) -> PyResult<PyObject> {
        // 为了能够写入文件，我们需要一个闭包处理上游项
        // 上游项可能是：
        // - FileData 对象
        // - dict: {"path", "content", ...}
        // - tuple/list: (path, content) 或 (path, content, change_type)
        // - str: 仅作为路径（创建空文件）
        //
        // 我们使用一个 PyO3 包装可调用对象来处理这些

        let handler = WriteHandler {
            downstream: on_next.clone_ref(py),
            mode: self.mode.clone(),
        };
        let handler_py = Py::new(py, handler)?.into_any();

        // 订阅源
        let source_ref = self.source.bind(py);
        Ok(source_ref.call_method1("subscribe", (handler_py,))?.unbind().into_any())
    }
}

#[pyclass]
struct WriteHandler {
    downstream: Py<PyAny>,
    mode: String,
}

#[pymethods]
impl WriteHandler {
    fn __call__(&self, item: PyObject, py: Python<'_>) -> PyResult<()> {
        // 解析上游项并写入文件

        // 情况 1: FileData
        if let Ok(fd_ref) = item.extract::<PyRef<FileData>>(py) {
            let path = fd_ref.path.clone();
            let ct = fd_ref.change_type;
            let _ = std::fs::write(&path, "");
            // 构造新的 FileData 并继续下传
            let new_fd = Py::new(
                py,
                FileData::now(
                    path.clone(),
                    None,
                    ct,
                    false,
                    Some(std::fs::metadata(&path).ok().map_or(0, |m| m.len())),
                    None,
                    None,
                ),
            )?;
            let _ = self.downstream.call1(py, (new_fd,));
            return Ok(());
        }

        // 情况 2: dict
        if let Ok(dict_ref) = item.bind(py).downcast::<PyDict>() {
            let path: String = dict_ref.get_item("path")?.map_or_else(|| Ok(String::new()), |v| v.extract())?;
            let content: String = dict_ref.get_item("content")?.map_or_else(|| Ok(String::new()), |v| v.extract()).unwrap_or_default();
            let ct: u32 = dict_ref.get_item("change_type")?.map_or_else(|| Ok(if self.mode == "create" { 0 } else { 1 }), |v| v.extract()).unwrap_or(0);

            if self.mode == "append" {
                use std::io::Write;
                if let Ok(mut file) = std::fs::OpenOptions::new().create(true).append(true).open(&path) {
                    let _ = file.write_all(content.as_bytes());
                }
            } else {
                let _ = std::fs::write(&path, &content);
            }

            let new_fd = Py::new(
                py,
                FileData::now(
                    path.clone(),
                    None,
                    if self.mode == "create" { 0 } else { ct },
                    false,
                    Some(content.len() as u64),
                    None,
                    None,
                ),
            )?;
            let _ = self.downstream.call1(py, (new_fd,));
            return Ok(());
        }

        // 情况 3: tuple / list
        if let Ok(tuple_ref) = item.bind(py).downcast::<PyTuple>() {
            if tuple_ref.len() >= 2 {
                let path: String = tuple_ref.get_item(0)?.extract()?;
                let content: String = tuple_ref.get_item(1)?.extract().unwrap_or_default();
                let ct: u32 = if tuple_ref.len() >= 3 {
                    tuple_ref.get_item(2)?.extract().unwrap_or(if self.mode == "create" { 0 } else { 1 })
                } else {
                    if self.mode == "create" { 0 } else { 1 }
                };

                if self.mode == "append" {
                    use std::io::Write;
                    if let Ok(mut file) = std::fs::OpenOptions::new().create(true).append(true).open(&path) {
                        let _ = file.write_all(content.as_bytes());
                    }
                } else {
                    let _ = std::fs::write(&path, &content);
                }

                let new_fd = Py::new(
                    py,
                    FileData::now(
                        path.clone(),
                        None,
                        ct,
                        false,
                        Some(content.len() as u64),
                        None,
                        None,
                    ),
                )?;
                let _ = self.downstream.call1(py, (new_fd,));
                return Ok(());
            }
        }

        if let Ok(list_ref) = item.bind(py).downcast::<PyList>() {
            if list_ref.len() >= 2 {
                let path: String = list_ref.get_item(0)?.extract()?;
                let content: String = list_ref.get_item(1)?.extract().unwrap_or_default();
                let ct: u32 = if list_ref.len() >= 3 {
                    list_ref.get_item(2)?.extract().unwrap_or(if self.mode == "create" { 0 } else { 1 })
                } else {
                    if self.mode == "create" { 0 } else { 1 }
                };

                if self.mode == "append" {
                    use std::io::Write;
                    if let Ok(mut file) = std::fs::OpenOptions::new().create(true).append(true).open(&path) {
                        let _ = file.write_all(content.as_bytes());
                    }
                } else {
                    let _ = std::fs::write(&path, &content);
                }

                let new_fd = Py::new(
                    py,
                    FileData::now(
                        path.clone(),
                        None,
                        ct,
                        false,
                        Some(content.len() as u64),
                        None,
                        None,
                    ),
                )?;
                let _ = self.downstream.call1(py, (new_fd,));
                return Ok(());
            }
        }

        // 情况 4: str 作为路径（创建空文件）
        if let Ok(path_str) = item.extract::<String>(py) {
            let _ = std::fs::write(&path_str, "");
            let new_fd = Py::new(
                py,
                FileData::now(
                    path_str.clone(),
                    None,
                    if self.mode == "create" { 0 } else { 1 },
                    false,
                    Some(0),
                    None,
                    None,
                ),
            )?;
            let _ = self.downstream.call1(py, (new_fd,));
            return Ok(());
        }

        Ok(())
    }
}

// ============================================================================
// 模块注册辅助
// ============================================================================

pub fn add_file_watcher_to_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<FileChangeType>()?;
    m.add_class::<FileData>()?;
    m.add_class::<FileDispatcher>()?;
    m.add_class::<FileSubject>()?;
    m.add_class::<FileObserver>()?;
    m.add_class::<SubscriptionHandle>()?;
    m.add_function(wrap_pyfunction!(from_filesystem, m)?)?;
    m.add_function(wrap_pyfunction!(write_to_filesystem, m)?)?;
    Ok(())
}

// 把 write_to_filesystem 注册到 ops 模块所用的静态集合
// 参考现有 lib.rs 的模式，我们提供一个便捷的 Python 端辅助

#[pyclass]
pub struct OpsModule {}

#[pymethods]
impl OpsModule {
    #[staticmethod]
    #[pyo3(signature = (dispatcher, mode="create"))]
    fn write_to_filesystem(dispatcher: PyObject, mode: &str, py: Python<'_>) -> PyResult<PyObject> {
        write_to_filesystem(dispatcher, mode, py)
    }
}

// ============================================================================
// （为 Python 端暴露一个 FileChangeType 的常量模块，使调用更清晰）
// ============================================================================

#[pymodule]
fn rx_rust_file_watcher(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    add_file_watcher_to_module(m)?;
    Ok(())
}
