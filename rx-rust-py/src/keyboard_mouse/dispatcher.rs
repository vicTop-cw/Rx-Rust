// 键鼠事件分发器模块：KeyboardDispatcher / MouseDispatcher

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use parking_lot::Mutex;
use pyo3::prelude::*;

use crate::keyboard_mouse::backends::{
    KeyboardBackend, MouseBackend, PollingKeyboardBackend, PollingMouseBackend, RawKeyEvent,
    RawMouseEvent,
};
use crate::keyboard_mouse::io::{KeyboardIO, MouseIO};
use crate::keyboard_mouse::types::{key_code_to_name, name_to_key_code, KeyData, MouseData};
use crate::PublishSubject;
use crate::Subscription;

// =====================================================================
// MouseIO 扩展方法（用于 drag 操作需要的 left_down/left_up）
// =====================================================================

#[cfg(windows)]
mod mouse_io_ext {
    use super::*;
    use windows_sys::Win32::UI::Input::KeyboardAndMouse::{
        INPUT, INPUT_0, INPUT_MOUSE, MOUSEEVENTF_LEFTDOWN, MOUSEEVENTF_LEFTUP, MOUSEINPUT, SendInput,
    };

    impl MouseIO {
        pub fn left_down() -> PyResult<()> {
            println!("[keyboard_mouse] left_down");
            Self::send_mouse_input(0, 0, 0, MOUSEEVENTF_LEFTDOWN)
        }

        pub fn left_up() -> PyResult<()> {
            println!("[keyboard_mouse] left_up");
            Self::send_mouse_input(0, 0, 0, MOUSEEVENTF_LEFTUP)
        }

        fn send_mouse_input(dx: i32, dy: i32, mouse_data: u32, flags: u32) -> PyResult<()> {
            let input = INPUT {
                r#type: INPUT_MOUSE,
                Anonymous: INPUT_0 {
                    mi: MOUSEINPUT {
                        dx,
                        dy,
                        mouseData: mouse_data,
                        dwFlags: flags,
                        time: 0,
                        dwExtraInfo: 0,
                    },
                },
            };
            let inputs = [input];
            let result = unsafe { SendInput(inputs.len() as u32, inputs.as_ptr(), std::mem::size_of::<INPUT>() as i32) };
            if result == 0 {
                println!("[keyboard_mouse] SendInput (mouse) failed");
            }
            Ok(())
        }
    }
}

#[cfg(not(windows))]
mod mouse_io_ext {
    use super::*;

    impl MouseIO {
        pub fn left_down() -> PyResult<()> {
            println!("[keyboard_mouse] left_down: not supported on this platform");
            Ok(())
        }

        pub fn left_up() -> PyResult<()> {
            println!("[keyboard_mouse] left_up: not supported on this platform");
            Ok(())
        }
    }
}

// =====================================================================
// KeyboardDispatcher - 键盘事件监控 + 分发器
// =====================================================================

/// 自我过滤事件记录
struct SelfEvent {
    key_code: u32,
    direction: u8, // 0=down, 1=up
    timestamp: f64,
}

struct KeyboardDispatcherInner {
    dispatch_count: u64,
    error_count: u64,
    self_filtered_count: u64,
    last_key_state: std::collections::HashMap<u32, bool>,
}

#[pyclass(name = "KeyboardDispatcher")]
pub struct KeyboardDispatcher {
    backend_name: String,
    backend: Arc<Mutex<Option<Arc<dyn KeyboardBackend + Send + Sync>>>>,
    subject: Py<PublishSubject>,
    inner: Mutex<KeyboardDispatcherInner>,
    running: Arc<AtomicBool>,
    started: Arc<AtomicBool>,
    interval_ms: u64,
    // 自我过滤
    self_filter: bool,
    self_events: Mutex<VecDeque<SelfEvent>>,
    self_filter_cap: usize,
    // 自定义过滤函数
    custom_self_filter: Option<PyObject>,
    // I/O 用于模拟
    io: KeyboardIO,
}

#[pymethods]
impl KeyboardDispatcher {
    #[new]
    #[pyo3(signature = (backend="auto".to_string(), interval=0.05, filter_self=true, self_filter=None, self_filter_cap=32))]
    fn new(
        py: Python<'_>,
        backend: String,
        interval: f64,
        filter_self: bool,
        self_filter: Option<PyObject>,
        self_filter_cap: usize,
    ) -> PyResult<Self> {
        let interval_ms = (interval.max(0.01) * 1000.0) as u64;
        let backend_name_lower = backend.to_lowercase();

        // 选择后端
        let backend_name: String = if cfg!(windows) {
            if backend_name_lower == "win32" {
                "win32".into()
            } else if backend_name_lower == "auto" || backend_name_lower == "polling" {
                "polling".into()
            } else {
                "polling".into()
            }
        } else {
            "polling".into()
        };

        Ok(KeyboardDispatcher {
            backend_name,
            backend: Arc::new(Mutex::new(None)),
            subject: Py::new(py, PublishSubject::new())?,
            inner: Mutex::new(KeyboardDispatcherInner {
                dispatch_count: 0,
                error_count: 0,
                self_filtered_count: 0,
                last_key_state: std::collections::HashMap::new(),
            }),
            running: Arc::new(AtomicBool::new(false)),
            started: Arc::new(AtomicBool::new(false)),
            interval_ms,
            self_filter: filter_self,
            self_events: Mutex::new(VecDeque::with_capacity(self_filter_cap.max(1))),
            self_filter_cap: self_filter_cap.max(1),
            custom_self_filter: self_filter,
            io: KeyboardIO,
        })
    }

    // --- 属性 ---
    #[getter]
    fn subject(&self, py: Python<'_>) -> PyResult<PyObject> {
        Ok(self.subject.clone_ref(py).into_any())
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
    fn error_count(&self) -> u64 {
        self.inner.lock().error_count
    }

    #[getter]
    fn self_filtered_count(&self) -> u64 {
        self.inner.lock().self_filtered_count
    }

    #[getter]
    fn is_running(&self) -> bool {
        self.started.load(Ordering::SeqCst)
    }

    // --- 生命周期 ---
    fn start(slf: Py<Self>) -> PyResult<()> {
        Python::with_gil(|py| {
            if slf.borrow(py).started.load(Ordering::SeqCst) {
                return Ok(());
            }
            slf.borrow(py).started.store(true, Ordering::SeqCst);
            slf.borrow(py).running.store(true, Ordering::SeqCst);

            let running = slf.borrow(py).running.clone();
            let backend_name = slf.borrow(py).backend_name.clone();
            let interval_ms = slf.borrow(py).interval_ms;
            let self_arc = slf.clone_ref(py);

            // 创建 channel
            let (tx, rx) = mpsc::channel::<RawKeyEvent>();

            // 创建并启动后端
            let backend: Arc<dyn KeyboardBackend + Send + Sync> = if backend_name == "win32" {
                #[cfg(windows)]
                {
                    Arc::new(crate::keyboard_mouse::backends::win32::Win32KeyboardBackend::new())
                }
                #[cfg(not(windows))]
                {
                    slf.borrow(py).inner.lock().error_count += 1;
                    return Err(pyo3::exceptions::PyRuntimeError::new_err(
                        "Win32 backend not available on this platform",
                    ));
                }
            } else {
                Arc::new(PollingKeyboardBackend::new(interval_ms))
            };

            // 尝试启动后端
            if let Err(e) = backend.start(tx) {
                eprintln!(
                    "[KeyboardDispatcher] backend.start() failed: {}, falling back to polling",
                    e
                );
                slf.borrow(py).inner.lock().error_count += 1;
                // 回退到 polling
                let polling = Arc::new(PollingKeyboardBackend::new(interval_ms));
                let (tx2, rx2) = mpsc::channel();
                if let Err(e2) = polling.start(tx2) {
                    slf.borrow(py).inner.lock().error_count += 1;
                    return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "Failed to start polling backend: {}",
                        e2
                    )));
                }
                *slf.borrow(py).backend.lock() = Some(polling);
                // 启动消费线程，使用 rx2
                Self::start_consumer_thread(py, self_arc, rx2, running);
            } else {
                *slf.borrow(py).backend.lock() = Some(backend);
                // 启动消费线程
                Self::start_consumer_thread(py, self_arc, rx, running);
            }

            Ok(())
        })
    }

    fn stop(slf: Py<Self>) {
        Python::with_gil(|py| {
            slf.borrow(py).started.store(false, Ordering::SeqCst);
            slf.borrow(py).running.store(false, Ordering::SeqCst);
            // 停止后端
            if let Some(backend) = slf.borrow(py).backend.lock().take() {
                backend.stop();
            }
        });
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        let self_clone = Python::with_gil(|py| slf.clone_ref(py));
        let _ = Self::start(self_clone);
        slf
    }

    fn __exit__(slf: Py<Self>, _exc_type: PyObject, _exc_val: PyObject, _exc_tb: PyObject) -> bool {
        let self_clone = Python::with_gil(|py| slf.clone_ref(py));
        Self::stop(self_clone);
        false
    }

    // --- 订阅 ---
    fn subscribe(&self, on_next: PyObject, py: Python<'_>) -> PyResult<Py<Subscription>> {
        Ok(self.subject.call_method1(py, "subscribe", (on_next,))?.extract(py)?)
    }

    // --- 模拟操作（触发自我过滤） ---
    fn press(&self, key: &str, py: Python<'_>) -> PyResult<()> {
        // 解析 key → key_code
        let key_code = self.parse_key(key).ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(format!("Unknown key: {}", key))
        })?;

        // 自我过滤登记
        self.register_self_event(key_code, 0);

        // 调用 io.press_key
        KeyboardIO::press_key(key_code)
    }

    fn release(&self, key: &str, py: Python<'_>) -> PyResult<()> {
        let key_code = self.parse_key(key).ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(format!("Unknown key: {}", key))
        })?;

        self.register_self_event(key_code, 1);
        KeyboardIO::release_key(key_code)
    }

    fn type_text(&self, text: &str, _py: Python<'_>) -> PyResult<()> {
        // 遍历每个字符，登记 + 模拟
        for ch in text.chars() {
            if ch.is_ascii() {
                if let Some(key_code) = name_to_key_code(&ch.to_uppercase().to_string()) {
                    self.register_self_event(key_code, 0);
                    let _ = KeyboardIO::press_key(key_code);
                    let _ = KeyboardIO::release_key(key_code);
                }
            } else {
                // 非 ASCII 使用 type_text
                self.register_self_event(0, 0);
                let _ = KeyboardIO::type_text(&ch.to_string());
            }
        }
        Ok(())
    }

    fn hotkey(&self, keys: Vec<String>, _py: Python<'_>) -> PyResult<()> {
        // 解析所有 key → key_codes
        let mut key_codes: Vec<u32> = Vec::new();
        for key in &keys {
            if let Some(code) = self.parse_key(key) {
                key_codes.push(code);
            }
        }

        // 登记所有按下事件
        for &code in &key_codes {
            self.register_self_event(code, 0);
        }

        // 调用 io.hotkey
        KeyboardIO::hotkey(&key_codes)
    }

    fn tap(&self, key: &str, py: Python<'_>) -> PyResult<()> {
        self.press(key, py)?;
        std::thread::sleep(std::time::Duration::from_millis(20));
        self.release(key, py)?;
        Ok(())
    }

    // --- 辅助 ---
    fn parse_key(&self, key: &str) -> Option<u32> {
        name_to_key_code(key)
    }
}

// =====================================================================
// KeyboardDispatcher 内部方法
// =====================================================================

impl KeyboardDispatcher {
    /// 启动消费线程：从 rx 接收 RawKeyEvent → 构造 KeyData → 发送到 subject
    fn start_consumer_thread(
        py: Python<'_>,
        slf: Py<Self>,
        rx: mpsc::Receiver<RawKeyEvent>,
        running: Arc<AtomicBool>,
    ) {
        let subject_clone = slf.borrow(py).subject.clone_ref(py);
        let filter_self_flag = slf.borrow(py).self_filter;
        let self_filter_cap = slf.borrow(py).self_filter_cap;
        let inner_data = {
            let slf_ref = slf.borrow(py);
            let inner_guard = slf_ref.inner.lock();
            let dispatch_count = inner_guard.dispatch_count;
            let error_count = inner_guard.error_count;
            let self_filtered_count = inner_guard.self_filtered_count;
            let last_key_state = inner_guard.last_key_state.clone();
            (dispatch_count, error_count, self_filtered_count, last_key_state)
        };
        let self_events: Arc<Mutex<VecDeque<SelfEvent>>> = Arc::new(Mutex::new(
            VecDeque::with_capacity(self_filter_cap),
        ));
        let inner = Arc::new(Mutex::new(KeyboardDispatcherInner {
            dispatch_count: inner_data.0,
            error_count: inner_data.1,
            self_filtered_count: inner_data.2,
            last_key_state: inner_data.3,
        }));

        let self_events_clone = self_events.clone();

        thread::spawn(move || {
            loop {
                // 检查 running
                if !running.load(Ordering::Relaxed) {
                    break;
                }

                // 带超时的 recv
                match rx.recv_timeout(Duration::from_millis(200)) {
                    Ok(raw_event) => {
                        // 检查自我过滤
                        let should_filter = if filter_self_flag {
                            let now = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_secs_f64();
                            let mut events = self_events_clone.lock();
                            // 清理超过 5s 的旧事件
                            events.retain(|e| now - e.timestamp < 5.0);

                            let (key_code, direction) = match raw_event {
                                RawKeyEvent::KeyDown(code) => (code, 0u8),
                                RawKeyEvent::KeyUp(code) => (code, 1u8),
                            };

                            let is_self = events.iter().any(|e| {
                                e.key_code == key_code
                                    && e.direction == direction
                                    && now - e.timestamp < 0.5
                            });

                            if is_self {
                                // 消费掉匹配的事件
                                events.retain(|e| {
                                    !(e.key_code == key_code && e.direction == direction)
                                });
                            }

                            is_self
                        } else {
                            false
                        };

                        if should_filter {
                            continue;
                        }

                        // 构造 KeyData
                        let fd_option = Python::with_gil(|py| {
                            let (key_code, is_press) = match raw_event {
                                RawKeyEvent::KeyDown(code) => (code, true),
                                RawKeyEvent::KeyUp(code) => (code, false),
                            };
                            Py::new(
                                py,
                                KeyData::now(
                                    key_code,
                                    is_press,
                                    Some(0),
                                    None,
                                ),
                            )
                            .ok()
                        });

                        if let Some(fd) = fd_option {
                            Python::with_gil(|py| {
                                let _ = subject_clone.call_method1(py, "on_next", (fd,));
                            });
                            inner.lock().dispatch_count += 1;
                        }
                    }
                    Err(RecvTimeoutError::Timeout) => continue,
                    Err(RecvTimeoutError::Disconnected) => break,
                }
            }
        });
    }

    /// 登记自我事件
    fn register_self_event(&self, key_code: u32, direction: u8) {
        if !self.self_filter {
            return;
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        let mut events = self.self_events.lock();
        if events.len() >= self.self_filter_cap {
            events.pop_front();
        }
        events.push_back(SelfEvent {
            key_code,
            direction,
            timestamp: now,
        });
    }
}

// =====================================================================
// MouseDispatcher - 鼠标事件监控 + 分发器
// =====================================================================

struct MouseDispatcherInner {
    dispatch_count: u64,
    error_count: u64,
    self_filtered_count: u64,
}

#[pyclass(name = "MouseDispatcher")]
pub struct MouseDispatcher {
    backend_name: String,
    backend: Arc<Mutex<Option<Arc<dyn MouseBackend + Send + Sync>>>>,
    subject: Py<PublishSubject>,
    inner: Mutex<MouseDispatcherInner>,
    running: Arc<AtomicBool>,
    started: Arc<AtomicBool>,
    interval_ms: u64,
    // 自我过滤
    self_filter: bool,
    self_events: Mutex<VecDeque<SelfEvent>>,
    self_filter_cap: usize,
    // 自定义过滤函数
    custom_self_filter: Option<PyObject>,
    // I/O
    io: MouseIO,
}

#[pymethods]
impl MouseDispatcher {
    #[new]
    #[pyo3(signature = (backend="auto".to_string(), interval=0.05, filter_self=true, self_filter=None, self_filter_cap=32))]
    fn new(
        py: Python<'_>,
        backend: String,
        interval: f64,
        filter_self: bool,
        self_filter: Option<PyObject>,
        self_filter_cap: usize,
    ) -> PyResult<Self> {
        let interval_ms = (interval.max(0.01) * 1000.0) as u64;
        let backend_name_lower = backend.to_lowercase();

        let backend_name: String = if cfg!(windows) {
            if backend_name_lower == "win32" {
                "win32".into()
            } else if backend_name_lower == "auto" || backend_name_lower == "polling" {
                "polling".into()
            } else {
                "polling".into()
            }
        } else {
            "polling".into()
        };

        Ok(MouseDispatcher {
            backend_name,
            backend: Arc::new(Mutex::new(None)),
            subject: Py::new(py, PublishSubject::new())?,
            inner: Mutex::new(MouseDispatcherInner {
                dispatch_count: 0,
                error_count: 0,
                self_filtered_count: 0,
            }),
            running: Arc::new(AtomicBool::new(false)),
            started: Arc::new(AtomicBool::new(false)),
            interval_ms,
            self_filter: filter_self,
            self_events: Mutex::new(VecDeque::with_capacity(self_filter_cap.max(1))),
            self_filter_cap: self_filter_cap.max(1),
            custom_self_filter: self_filter,
            io: MouseIO,
        })
    }

    // --- 属性 ---
    #[getter]
    fn subject(&self, py: Python<'_>) -> PyResult<PyObject> {
        Ok(self.subject.clone_ref(py).into_any())
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
    fn error_count(&self) -> u64 {
        self.inner.lock().error_count
    }

    #[getter]
    fn self_filtered_count(&self) -> u64 {
        self.inner.lock().self_filtered_count
    }

    #[getter]
    fn is_running(&self) -> bool {
        self.started.load(Ordering::SeqCst)
    }

    // --- 生命周期 ---
    fn start(slf: Py<Self>) -> PyResult<()> {
        Python::with_gil(|py| {
            if slf.borrow(py).started.load(Ordering::SeqCst) {
                return Ok(());
            }
            slf.borrow(py).started.store(true, Ordering::SeqCst);
            slf.borrow(py).running.store(true, Ordering::SeqCst);

            let running = slf.borrow(py).running.clone();
            let backend_name = slf.borrow(py).backend_name.clone();
            let interval_ms = slf.borrow(py).interval_ms;
            let self_arc = slf.clone_ref(py);

            // 创建 channel
            let (tx, rx) = mpsc::channel::<RawMouseEvent>();

            // 创建并启动后端
            let backend: Arc<dyn MouseBackend + Send + Sync> = if backend_name == "win32" {
                #[cfg(windows)]
                {
                    Arc::new(crate::keyboard_mouse::backends::win32::Win32MouseBackend::new())
                }
                #[cfg(not(windows))]
                {
                    slf.borrow(py).inner.lock().error_count += 1;
                    return Err(pyo3::exceptions::PyRuntimeError::new_err(
                        "Win32 backend not available on this platform",
                    ));
                }
            } else {
                Arc::new(PollingMouseBackend::new(interval_ms))
            };

            if let Err(e) = backend.start(tx) {
                eprintln!(
                    "[MouseDispatcher] backend.start() failed: {}, falling back to polling",
                    e
                );
                slf.borrow(py).inner.lock().error_count += 1;
                let polling = Arc::new(PollingMouseBackend::new(interval_ms));
                let (tx2, rx2) = mpsc::channel();
                if let Err(e2) = polling.start(tx2) {
                    slf.borrow(py).inner.lock().error_count += 1;
                    return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "Failed to start polling backend: {}",
                        e2
                    )));
                }
                *slf.borrow(py).backend.lock() = Some(polling);
                Self::start_consumer_thread(py, self_arc, rx2, running);
            } else {
                *slf.borrow(py).backend.lock() = Some(backend);
                Self::start_consumer_thread(py, self_arc, rx, running);
            }

            Ok(())
        })
    }

    fn stop(slf: Py<Self>) {
        Python::with_gil(|py| {
            slf.borrow(py).started.store(false, Ordering::SeqCst);
            slf.borrow(py).running.store(false, Ordering::SeqCst);
            if let Some(backend) = slf.borrow(py).backend.lock().take() {
                backend.stop();
            }
        });
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        let self_clone = Python::with_gil(|py| slf.clone_ref(py));
        let _ = Self::start(self_clone);
        slf
    }

    fn __exit__(slf: Py<Self>, _exc_type: PyObject, _exc_val: PyObject, _exc_tb: PyObject) -> bool {
        let self_clone = Python::with_gil(|py| slf.clone_ref(py));
        Self::stop(self_clone);
        false
    }

    // --- 订阅 ---
    fn subscribe(&self, on_next: PyObject, py: Python<'_>) -> PyResult<Py<Subscription>> {
        Ok(self.subject.call_method1(py, "subscribe", (on_next,))?.extract(py)?)
    }

    // --- 模拟操作 ---
    fn move_to(&self, x: i32, y: i32, _py: Python<'_>) -> PyResult<()> {
        self.register_self_event(0, 0);
        MouseIO::move_to(x, y)
    }

    fn click(&self, button: &str, _py: Python<'_>) -> PyResult<()> {
        self.register_self_event(0, 0);
        MouseIO::click(button)
    }

    fn scroll(&self, delta: i32, _py: Python<'_>) -> PyResult<()> {
        self.register_self_event(0, 0);
        MouseIO::scroll(delta)
    }

    fn drag(
        &self,
        from_x: i32,
        from_y: i32,
        to_x: i32,
        to_y: i32,
        _py: Python<'_>,
    ) -> PyResult<()> {
        self.register_self_event(0, 0);
        MouseIO::move_to(from_x, from_y)?;
        self.register_self_event(0, 0);
        MouseIO::left_down()?;
        self.register_self_event(0, 0);
        MouseIO::move_to(to_x, to_y)?;
        self.register_self_event(0, 0);
        MouseIO::left_up()?;
        Ok(())
    }

    fn double_click(&self, button: &str, _py: Python<'_>) -> PyResult<()> {
        self.register_self_event(0, 0);
        MouseIO::click(button)?;
        self.register_self_event(0, 0);
        MouseIO::click(button)
    }

    fn move_relative(&self, dx: i32, dy: i32, _py: Python<'_>) -> PyResult<()> {
        self.register_self_event(0, 0);
        MouseIO::move_relative(dx, dy)
    }
}

// =====================================================================
// MouseDispatcher 内部方法
// =====================================================================

impl MouseDispatcher {
    /// 启动消费线程
    fn start_consumer_thread(
        py: Python<'_>,
        slf: Py<Self>,
        rx: mpsc::Receiver<RawMouseEvent>,
        running: Arc<AtomicBool>,
    ) {
        let subject_clone = slf.borrow(py).subject.clone_ref(py);
        let filter_self_flag = slf.borrow(py).self_filter;
        let self_filter_cap = slf.borrow(py).self_filter_cap;
        let self_events: Arc<Mutex<VecDeque<SelfEvent>>> = Arc::new(Mutex::new(
            VecDeque::with_capacity(self_filter_cap),
        ));
        let inner = {
            let slf_ref = slf.borrow(py);
            let inner_guard = slf_ref.inner.lock();
            let dispatch_count = inner_guard.dispatch_count;
            let error_count = inner_guard.error_count;
            let self_filtered_count = inner_guard.self_filtered_count;
            drop(inner_guard);
            drop(slf_ref);
            Arc::new(Mutex::new(MouseDispatcherInner {
                dispatch_count,
                error_count,
                self_filtered_count,
            }))
        };

        let self_events_clone = self_events.clone();

        thread::spawn(move || {
            loop {
                if !running.load(Ordering::Relaxed) {
                    break;
                }

                match rx.recv_timeout(Duration::from_millis(200)) {
                    Ok(raw_event) => {
                        // 检查自我过滤（鼠标事件简化处理）
                        let should_filter = if filter_self_flag {
                            let now = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_secs_f64();
                            let mut events = self_events_clone.lock();
                            events.retain(|e| now - e.timestamp < 5.0);

                            // 鼠标事件简化：检查是否有近期标记
                            let is_self = events
                                .iter()
                                .any(|e| e.key_code == 0 && now - e.timestamp < 0.5);

                            if is_self {
                                events.retain(|e| !(e.key_code == 0 && now - e.timestamp >= 0.5));
                            }

                            is_self
                        } else {
                            false
                        };

                        if should_filter {
                            continue;
                        }

                        // 构造 MouseData
                        let fd_option = Python::with_gil(|py| {
                            let (x, y, event_type, button, delta) = match raw_event {
                                RawMouseEvent::Move(x, y) => (x, y, 0u8, Some("none".to_string()), 0),
                                RawMouseEvent::LeftDown(x, y) => (x, y, 1u8, Some("left".to_string()), 0),
                                RawMouseEvent::LeftUp(x, y) => (x, y, 2u8, Some("left".to_string()), 0),
                                RawMouseEvent::RightDown(x, y) => {
                                    (x, y, 3u8, Some("right".to_string()), 0)
                                }
                                RawMouseEvent::RightUp(x, y) => (x, y, 4u8, Some("right".to_string()), 0),
                                RawMouseEvent::MiddleDown(x, y) => {
                                    (x, y, 5u8, Some("middle".to_string()), 0)
                                }
                                RawMouseEvent::MiddleUp(x, y) => {
                                    (x, y, 6u8, Some("middle".to_string()), 0)
                                }
                                RawMouseEvent::Scroll(x, y, delta) => {
                                    (x, y, 7u8, Some("none".to_string()), delta)
                                }
                                RawMouseEvent::Wheel(x, y, delta) => {
                                    (x, y, 7u8, Some("none".to_string()), delta)
                                }
                            };
                            Py::new(py, MouseData::now(x, y, event_type, button, delta)).ok()
                        });

                        if let Some(fd) = fd_option {
                            Python::with_gil(|py| {
                                let _ = subject_clone.call_method1(py, "on_next", (fd,));
                            });
                            inner.lock().dispatch_count += 1;
                        }
                    }
                    Err(RecvTimeoutError::Timeout) => continue,
                    Err(RecvTimeoutError::Disconnected) => break,
                }
            }
        });
    }

    /// 登记自我事件
    fn register_self_event(&self, key_code: u32, direction: u8) {
        if !self.self_filter {
            return;
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        let mut events = self.self_events.lock();
        if events.len() >= self.self_filter_cap {
            events.pop_front();
        }
        events.push_back(SelfEvent {
            key_code,
            direction,
            timestamp: now,
        });
    }
}
