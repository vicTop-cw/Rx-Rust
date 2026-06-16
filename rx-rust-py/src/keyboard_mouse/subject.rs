// 键盘鼠标 Subject 模块：KeySubject / MouseSubject
// 封装 Dispatcher，提供 Python 友好的高层接口

use std::sync::atomic::{AtomicBool, Ordering};

use pyo3::prelude::*;

use crate::PublishSubject;

/// KeySubject：键盘事件主题，封装 KeyboardDispatcher
#[pyclass(name = "KeySubject")]
pub struct KeySubject {
    dispatcher: PyObject,
    started: AtomicBool,
}

/// MouseSubject：鼠标事件主题，封装 MouseDispatcher
#[pyclass(name = "MouseSubject")]
pub struct MouseSubject {
    dispatcher: PyObject,
    started: AtomicBool,
}

#[pymethods]
impl KeySubject {
    #[new]
    #[pyo3(signature = (backend="auto".to_string(), interval=0.05, filter_self=true))]
    fn new(py: Python<'_>, backend: String, interval: f64, filter_self: bool) -> PyResult<Self> {
        let dispatcher_class = py.get_type_bound::<crate::keyboard_mouse::dispatcher::KeyboardDispatcher>();
        let dispatcher = dispatcher_class.call1((backend, interval, filter_self, None::<PyObject>, 32))?;

        let subject = Self {
            dispatcher: dispatcher.unbind(),
            started: AtomicBool::new(false),
        };

        subject.start(py)?;
        Ok(subject)
    }

    #[getter]
    fn dispatcher(&self, py: Python<'_>) -> PyObject {
        self.dispatcher.clone_ref(py)
    }

    #[getter]
    fn subject(&self, py: Python<'_>) -> PyResult<PyObject> {
        let subject = self.dispatcher.bind(py).getattr("subject")?;
        Ok(subject.unbind())
    }

    #[getter]
    fn backend_name(&self, py: Python<'_>) -> PyResult<String> {
        let name = self.dispatcher.bind(py).getattr("backend_name")?;
        name.extract()
    }

    #[getter]
    fn dispatch_count(&self, py: Python<'_>) -> PyResult<u64> {
        let count = self.dispatcher.bind(py).getattr("dispatch_count")?;
        count.extract()
    }

    #[getter]
    fn self_filtered_count(&self, py: Python<'_>) -> PyResult<u64> {
        let count = self.dispatcher.bind(py).getattr("self_filtered_count")?;
        count.extract()
    }

    #[getter]
    fn is_running(&self) -> bool {
        self.started.load(Ordering::SeqCst)
    }

    fn start(&self, py: Python<'_>) -> PyResult<()> {
        if self.started.load(Ordering::SeqCst) {
            return Ok(());
        }
        self.dispatcher.bind(py).call_method0("start")?;
        self.started.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn stop(&self, py: Python<'_>) {
        self.started.store(false, Ordering::SeqCst);
        let _ = self.dispatcher.bind(py).call_method0("stop");
    }

    fn subscribe(&self, py: Python<'_>, observer: PyObject) -> PyResult<PyObject> {
        let subject = self.dispatcher.bind(py).getattr("subject")?;
        let res = subject.call_method1("subscribe", (observer,))?;
        Ok(res.unbind())
    }

    fn press(&self, key: &str, py: Python<'_>) -> PyResult<()> {
        self.dispatcher.bind(py).call_method1("press", (key,))?;
        Ok(())
    }

    fn release(&self, key: &str, py: Python<'_>) -> PyResult<()> {
        self.dispatcher.bind(py).call_method1("release", (key,))?;
        Ok(())
    }

    fn type_text(&self, text: &str, py: Python<'_>) -> PyResult<()> {
        self.dispatcher.bind(py).call_method1("type_text", (text,))?;
        Ok(())
    }

    fn hotkey(&self, keys: Vec<String>, py: Python<'_>) -> PyResult<()> {
        self.dispatcher.bind(py).call_method1("hotkey", (keys,))?;
        Ok(())
    }

    fn tap(&self, key: &str, py: Python<'_>) -> PyResult<()> {
        self.dispatcher.bind(py).call_method1("tap", (key,))?;
        Ok(())
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        Python::with_gil(|py| {
            let slf_ref = slf.borrow(py);
            if let Err(e) = slf_ref.start(py) {
                eprintln!("KeySubject start error: {:?}", e);
            }
        });
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
            self.stop(py);
        });
    }
}

#[pymethods]
impl MouseSubject {
    #[new]
    #[pyo3(signature = (backend="auto".to_string(), interval=0.05, filter_self=true))]
    fn new(py: Python<'_>, backend: String, interval: f64, filter_self: bool) -> PyResult<Self> {
        let dispatcher_class = py.get_type_bound::<crate::keyboard_mouse::dispatcher::MouseDispatcher>();
        let dispatcher = dispatcher_class.call1((backend, interval, filter_self, None::<PyObject>, 32))?;

        let subject = Self {
            dispatcher: dispatcher.unbind(),
            started: AtomicBool::new(false),
        };

        subject.start(py)?;
        Ok(subject)
    }

    #[getter]
    fn dispatcher(&self, py: Python<'_>) -> PyObject {
        self.dispatcher.clone_ref(py)
    }

    #[getter]
    fn subject(&self, py: Python<'_>) -> PyResult<PyObject> {
        let subject = self.dispatcher.bind(py).getattr("subject")?;
        Ok(subject.unbind())
    }

    #[getter]
    fn backend_name(&self, py: Python<'_>) -> PyResult<String> {
        let name = self.dispatcher.bind(py).getattr("backend_name")?;
        name.extract()
    }

    #[getter]
    fn dispatch_count(&self, py: Python<'_>) -> PyResult<u64> {
        let count = self.dispatcher.bind(py).getattr("dispatch_count")?;
        count.extract()
    }

    #[getter]
    fn self_filtered_count(&self, py: Python<'_>) -> PyResult<u64> {
        let count = self.dispatcher.bind(py).getattr("self_filtered_count")?;
        count.extract()
    }

    #[getter]
    fn is_running(&self) -> bool {
        self.started.load(Ordering::SeqCst)
    }

    fn start(&self, py: Python<'_>) -> PyResult<()> {
        if self.started.load(Ordering::SeqCst) {
            return Ok(());
        }
        self.dispatcher.bind(py).call_method0("start")?;
        self.started.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn stop(&self, py: Python<'_>) {
        self.started.store(false, Ordering::SeqCst);
        let _ = self.dispatcher.bind(py).call_method0("stop");
    }

    fn subscribe(&self, py: Python<'_>, observer: PyObject) -> PyResult<PyObject> {
        let subject = self.dispatcher.bind(py).getattr("subject")?;
        let res = subject.call_method1("subscribe", (observer,))?;
        Ok(res.unbind())
    }

    fn move_to(&self, x: i32, y: i32, py: Python<'_>) -> PyResult<()> {
        self.dispatcher.bind(py).call_method1("move_to", (x, y))?;
        Ok(())
    }

    fn click(&self, button: &str, py: Python<'_>) -> PyResult<()> {
        self.dispatcher.bind(py).call_method1("click", (button,))?;
        Ok(())
    }

    fn scroll(&self, delta: i32, py: Python<'_>) -> PyResult<()> {
        self.dispatcher.bind(py).call_method1("scroll", (delta,))?;
        Ok(())
    }

    fn drag(&self, from_x: i32, from_y: i32, to_x: i32, to_y: i32, py: Python<'_>) -> PyResult<()> {
        self.dispatcher.bind(py).call_method1("drag", (from_x, from_y, to_x, to_y))?;
        Ok(())
    }

    fn double_click(&self, button: &str, py: Python<'_>) -> PyResult<()> {
        self.dispatcher.bind(py).call_method1("double_click", (button,))?;
        Ok(())
    }

    fn move_relative(&self, dx: i32, dy: i32, py: Python<'_>) -> PyResult<()> {
        self.dispatcher.bind(py).call_method1("move_relative", (dx, dy))?;
        Ok(())
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        Python::with_gil(|py| {
            let slf_ref = slf.borrow(py);
            if let Err(e) = slf_ref.start(py) {
                eprintln!("MouseSubject start error: {:?}", e);
            }
        });
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
            self.stop(py);
        });
    }
}