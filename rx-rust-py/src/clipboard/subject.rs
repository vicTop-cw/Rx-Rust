// ClipSubject：自包含 Dispatcher 的 Subject

use std::sync::atomic::{AtomicBool, Ordering};

use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::clipboard::dispatcher::ClipboardDispatcher;
use crate::clipboard::types::{ClipChangeType, ClipData};
use crate::PublishSubject;

#[pyclass(name = "ClipSubject")]
pub struct ClipSubject {
    dispatcher: Py<ClipboardDispatcher>,
    auto_start: bool,
    started: AtomicBool,
}

#[pymethods]
impl ClipSubject {
    #[new]
    #[pyo3(signature = (interval=0.2, backend="auto".to_string(), change_types=None, tags=None, filter_self=true, self_filter=None, self_source=None, auto_start=true, on_change_data=None))]
    fn new(
        py: Python<'_>,
        interval: f64,
        backend: String,
        change_types: Option<PyObject>,
        tags: Option<Vec<String>>,
        filter_self: bool,
        self_filter: Option<PyObject>,
        self_source: Option<String>,
        auto_start: bool,
        on_change_data: Option<PyObject>,
    ) -> PyResult<Self> {
        let dispatcher = Py::new(
            py,
            ClipboardDispatcher::new(
                py,
                interval,
                backend,
                change_types,
                tags,
                filter_self,
                self_filter,
                self_source,
                32,
                on_change_data,
            )?,
        )?;

        let subject = ClipSubject {
            dispatcher: dispatcher.clone_ref(py),
            auto_start,
            started: AtomicBool::new(false),
        };

        if auto_start {
            // 直接调用 dispatcher 的 start_me
            dispatcher.borrow(py).start_me(py)?;
            // 手动设置 started 标志
            std::mem::forget(subject.dispatcher.clone_ref(py));
        }

        Ok(subject)
    }

    #[getter]
    fn dispatcher(&self, py: Python<'_>) -> Py<ClipboardDispatcher> {
        self.dispatcher.clone_ref(py)
    }

    #[getter]
    fn subject(&self, py: Python<'_>) -> PyResult<Py<PublishSubject>> {
        Ok(self.dispatcher.call_method0(py, "subject")?.extract(py)?)
    }

    #[getter]
    fn backend_name(&self, py: Python<'_>) -> PyResult<String> {
        Ok(self.dispatcher.call_method0(py, "backend_name")?.extract(py)?)
    }

    #[getter]
    fn dispatch_count(&self, py: Python<'_>) -> PyResult<u64> {
        Ok(self.dispatcher.call_method0(py, "dispatch_count")?.extract(py)?)
    }

    #[getter]
    fn self_filtered_count(&self, py: Python<'_>) -> PyResult<u64> {
        Ok(self.dispatcher.call_method0(py, "self_filtered_count")?.extract(py)?)
    }

    #[getter]
    fn is_running(&self) -> bool {
        self.started.load(Ordering::SeqCst)
    }

    fn start(slf: Py<Self>) -> PyResult<()> {
        Python::with_gil(|py| {
            if slf.borrow(py).started.load(Ordering::SeqCst) {
                return Ok(());
            }
            let dispatcher = slf.borrow(py).dispatcher.clone_ref(py);
            dispatcher.borrow(py).start_me(py)?;
            slf.borrow(py).started.store(true, Ordering::SeqCst);
            Ok(())
        })
    }

    fn stop(slf: Py<Self>) {
        Python::with_gil(|py| {
            slf.borrow(py).started.store(false, Ordering::SeqCst);
            let dispatcher = slf.borrow(py).dispatcher.clone_ref(py);
            dispatcher.borrow(py).stop_me();
        });
    }

    fn on_next(&self, py: Python<'_>, value: PyObject) {
        if let Ok(subj) = self.subject(py) {
            subj.borrow(py).on_next(value);
        }
    }

    fn on_completed(&self, py: Python<'_>) {
        if let Ok(subj) = self.subject(py) {
            subj.borrow(py).on_completed();
        }
    }

    fn subscribe(&self, py: Python<'_>, observer: PyObject) -> PyObject {
        match self.subject(py) {
            Ok(subj) => {
                let res = subj.borrow(py).subscribe(observer, None, None);
                Python::with_gil(|py| res.into_any().clone_ref(py))
            }
            Err(_) => py.None(),
        }
    }

    // set_text / set_clipboard
    #[pyo3(signature = (text, *, source=None, tags=None, metadata=None))]
    fn set_text(
        slf: Py<Self>,
        text: String,
        source: Option<String>,
        tags: Option<Vec<String>>,
        metadata: Option<Py<PyDict>>,
    ) -> PyResult<Py<ClipData>> {
        Python::with_gil(|py| {
            let dispatcher = slf.borrow(py).dispatcher.clone_ref(py);
            let content_obj = text.clone().to_object(py).into_any();
            let ct = Py::new(py, ClipChangeType { value: 0 })?;
            dispatcher.call_method1(
                py,
                "set_clipboard",
                (Some(content_obj), None::<Vec<String>>, Some(ct), source, tags, metadata),
            )?.extract(py)
        })
    }

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
            let dispatcher = slf.borrow(py).dispatcher.clone_ref(py);
            dispatcher.call_method1(
                py,
                "set_clipboard",
                (content, files, change_type, source, tags, metadata),
            )?.extract(py)
        })
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __exit__(slf: Py<Self>, _exc_type: PyObject, _exc_val: PyObject, _exc_tb: PyObject) -> bool {
        false
    }
}
