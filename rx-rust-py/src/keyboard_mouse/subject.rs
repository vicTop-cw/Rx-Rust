// 键盘鼠标 Subject 模块：KeySubject / MouseSubject
// 封装 Dispatcher，提供 Python 友好的高层接口

use std::sync::atomic::{AtomicBool, Ordering};

use pyo3::prelude::*;

use crate::keyboard_mouse::dispatcher::{KeyboardDispatcher, MouseDispatcher};
use crate::PublishSubject;

/// KeySubject：键盘事件主题，封装 KeyboardDispatcher
#[pyclass(name = "KeySubject")]
pub struct KeySubject {
    dispatcher: Py<KeyboardDispatcher>,
    started: AtomicBool,
}

/// MouseSubject：鼠标事件主题，封装 MouseDispatcher
#[pyclass(name = "MouseSubject")]
pub struct MouseSubject {
    dispatcher: Py<MouseDispatcher>,
    started: AtomicBool,
}

#[pymethods]
impl KeySubject {
    #[new]
    #[pyo3(signature = (backend="auto".to_string(), interval=0.05, filter_self=true))]
    fn new(py: Python<'_>, backend: String, interval: f64, filter_self: bool) -> PyResult<Self> {
        let dispatcher = Py::new(
            py,
            KeyboardDispatcher::new(py, backend, interval, filter_self, None, 32)?,
        )?;

        let subject = Self {
            dispatcher,
            started: AtomicBool::new(false),
        };

        // 构造后自动启动
        subject.start(py)?;
        Ok(subject)
    }

    #[getter]
    fn dispatcher<'py>(&self, py: Python<'py>) -> Py<KeyboardDispatcher> {
        self.dispatcher.clone_ref(py)
    }

    #[getter]
    fn subject(&self, py: Python<'_>) -> Py<PublishSubject> {
        self.dispatcher.borrow(py).subject.clone_ref(py)
    }

    #[getter]
    fn backend_name(&self) -> String {
        self.dispatcher.borrow().backend_name.clone()
    }

    #[getter]
    fn dispatch_count(&self) -> u64 {
        self.dispatcher.borrow().dispatch_count()
    }

    #[getter]
    fn self_filtered_count(&self) -> u64 {
        self.dispatcher.borrow().self_filtered_count()
    }

    #[getter]
    fn is_running(&self) -> bool {
        self.started.load(Ordering::SeqCst)
    }

    fn start(&self, py: Python<'_>) -> PyResult<()> {
        if self.started.load(Ordering::SeqCst) {
            return Ok(());
        }
        KeyboardDispatcher::start(self.dispatcher.clone_ref(py))?;
        self.started.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn stop(&self, py: Python<'_>) {
        self.started.store(false, Ordering::SeqCst);
        KeyboardDispatcher::stop(self.dispatcher.clone_ref(py));
    }

    fn subscribe(&self, py: Python<'_>, observer: PyObject) -> PyObject {
        let subject = self.dispatcher.borrow(py).subject.clone_ref(py);
        let res = subject.borrow(py).subscribe(observer);
        res.into_any().clone_ref(py)
    }

    // 模拟操作透传
    fn press(&self, key: &str, py: Python<'_>) -> PyResult<()> {
        self.dispatcher.borrow(py).press(key, py)
    }

    fn release(&self, key: &str, py: Python<'_>) -> PyResult<()> {
        self.dispatcher.borrow(py).release(key, py)
    }

    fn type_text(&self, text: &str, py: Python<'_>) -> PyResult<()> {
        self.dispatcher.borrow(py).type_text(text, py)
    }

    fn hotkey(&self, keys: Vec<&str>, py: Python<'_>) -> PyResult<()> {
        self.dispatcher.borrow(py).hotkey(keys, py)
    }

    fn tap(&self, key: &str, py: Python<'_>) -> PyResult<()> {
        self.dispatcher.borrow(py).tap(key, py)
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        let py = Python::acquire_gil().python();
        let slf_ref = slf.clone_ref(py);
        if let Err(e) = Self::start(&slf_ref, py) {
            eprintln!("KeySubject start error: {:?}", e);
        }
        slf
    }

    fn __exit__(
        &self,
        py: Python<'_>,
        _exc_type: PyObject,
        _exc_val: PyObject,
        _exc_tb: PyObject,
    ) -> bool {
        self.stop(py);
        false
    }

    fn __del__(&mut self) {
        Python::with_gil(|py| {
            self.stop(py).ok();
        });
    }
}

#[pymethods]
impl MouseSubject {
    #[new]
    #[pyo3(signature = (backend="auto".to_string(), interval=0.05, filter_self=true))]
    fn new(py: Python<'_>, backend: String, interval: f64, filter_self: bool) -> PyResult<Self> {
        let dispatcher = Py::new(
            py,
            MouseDispatcher::new(py, backend, interval, filter_self, None, 32)?,
        )?;

        let subject = Self {
            dispatcher,
            started: AtomicBool::new(false),
        };

        // 构造后自动启动
        subject.start(py)?;
        Ok(subject)
    }

    #[getter]
    fn dispatcher<'py>(&self, py: Python<'py>) -> Py<MouseDispatcher> {
        self.dispatcher.clone_ref(py)
    }

    #[getter]
    fn subject(&self, py: Python<'_>) -> Py<PublishSubject> {
        self.dispatcher.borrow(py).subject.clone_ref(py)
    }

    #[getter]
    fn backend_name(&self) -> String {
        self.dispatcher.borrow().backend_name.clone()
    }

    #[getter]
    fn dispatch_count(&self) -> u64 {
        self.dispatcher.borrow().dispatch_count()
    }

    #[getter]
    fn self_filtered_count(&self) -> u64 {
        self.dispatcher.borrow().self_filtered_count()
    }

    #[getter]
    fn is_running(&self) -> bool {
        self.started.load(Ordering::SeqCst)
    }

    fn start(&self, py: Python<'_>) -> PyResult<()> {
        if self.started.load(Ordering::SeqCst) {
            return Ok(());
        }
        MouseDispatcher::start(self.dispatcher.clone_ref(py))?;
        self.started.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn stop(&self, py: Python<'_>) {
        self.started.store(false, Ordering::SeqCst);
        MouseDispatcher::stop(self.dispatcher.clone_ref(py));
    }

    fn subscribe(&self, py: Python<'_>, observer: PyObject) -> PyObject {
        let subject = self.dispatcher.borrow(py).subject.clone_ref(py);
        let res = subject.borrow(py).subscribe(observer);
        res.into_any().clone_ref(py)
    }

    // 鼠标特有操作透传
    fn move_to(&self, x: i32, y: i32, py: Python<'_>) -> PyResult<()> {
        self.dispatcher.borrow(py).move_to(x, y, py)
    }

    fn click(&self, button: &str, py: Python<'_>) -> PyResult<()> {
        self.dispatcher.borrow(py).click(button, py)
    }

    fn scroll(&self, delta: i32, py: Python<'_>) -> PyResult<()> {
        self.dispatcher.borrow(py).scroll(delta, py)
    }

    fn drag(&self, from_x: i32, from_y: i32, to_x: i32, to_y: i32, py: Python<'_>) -> PyResult<()> {
        self.dispatcher
            .borrow(py)
            .drag(from_x, from_y, to_x, to_y, py)
    }

    fn double_click(&self, button: &str, py: Python<'_>) -> PyResult<()> {
        self.dispatcher.borrow(py).double_click(button, py)
    }

    fn move_relative(&self, dx: i32, dy: i32, py: Python<'_>) -> PyResult<()> {
        self.dispatcher.borrow(py).move_relative(dx, dy, py)
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        let py = Python::acquire_gil().python();
        let slf_ref = slf.clone_ref(py);
        if let Err(e) = Self::start(&slf_ref, py) {
            eprintln!("MouseSubject start error: {:?}", e);
        }
        slf
    }

    fn __exit__(
        &self,
        py: Python<'_>,
        _exc_type: PyObject,
        _exc_val: PyObject,
        _exc_tb: PyObject,
    ) -> bool {
        self.stop(py);
        false
    }

    fn __del__(&mut self) {
        Python::with_gil(|py| {
            self.stop(py).ok();
        });
    }
}
