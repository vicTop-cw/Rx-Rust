// 顶层 API：from_clipboard / write_to_clipboard

use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::clipboard::dispatcher::ClipboardDispatcher;
use crate::clipboard::types::{ClipChangeType, ClipData};
use crate::{Observable, PublishSubject};

#[pyfunction]
#[pyo3(signature = (*, interval=0.2, backend="auto".to_string(), on_change_data=None, change_types=None, tags=None, auto_start=true, filter_self=true, self_source=None))]
pub fn from_clipboard(
    py: Python<'_>,
    interval: f64,
    backend: String,
    on_change_data: Option<PyObject>,
    change_types: Option<PyObject>,
    tags: Option<Vec<String>>,
    auto_start: bool,
    filter_self: bool,
    self_source: Option<String>,
) -> PyResult<(PyObject, PyObject)> {
    let dispatcher = Py::new(
        py,
        ClipboardDispatcher::new(
            py,
            interval,
            backend,
            change_types,
            tags,
            filter_self,
            None,
            self_source,
            32,
            on_change_data,
        )?,
    )?;

    if auto_start {
        dispatcher.borrow(py).start(py)?;
    }

    // 返回 (observable_subject, dispatcher)
    let subject = dispatcher.borrow(py).subject.clone_ref(py);
    Ok((subject.into_any(), dispatcher.into_any()))
}

#[pyclass(name = "_WriteToClipboardOperator")]
pub struct WriteToClipboardOperator {
    pub dispatcher: Py<ClipboardDispatcher>,
    pub source: Option<String>,
}

#[pymethods]
impl WriteToClipboardOperator {
    fn __call__(&self, py: Python<'_>, upstream: PyObject) -> PyResult<PyObject> {
        // 构造一个新的 Observable，对上游的值先写回剪贴板再向下游转发
        let dispatcher = self.dispatcher.clone_ref(py);
        let source = self.source.clone();

        // 构造订阅函数
        let d = dispatcher.clone_ref(py);
        let s = source.clone();
        let subscribe_fn: PyObject = Py::new(
            py,
            WriteToClipboardSubscribeFn {
                dispatcher: d,
                source: s,
                upstream: upstream.clone_ref(py),
            },
        )?.into_any();

        let obs = crate::Observable::from_subscribe_fn(py, subscribe_fn)?;
        Ok(obs.into_any())
    }
}

#[pyclass(name = "_WriteToClipboardSubscribeFn")]
pub struct WriteToClipboardSubscribeFn {
    pub dispatcher: Py<ClipboardDispatcher>,
    pub source: Option<String>,
    pub upstream: PyObject,
}

#[pymethods]
impl WriteToClipboardSubscribeFn {
    fn __call__(&self, downstream: PyObject) -> PyResult<PyObject> {
        Python::with_gil(|py| {
            // 订阅上游，将每个值先写回剪贴板，再向下游转发
            let dispatcher = self.dispatcher.clone_ref(py);
            let source = self.source.clone();

            // 构造一个 "写入+转发" callable
            let writer = Py::new(
                py,
                ValueWriter {
                    dispatcher: dispatcher.clone_ref(py),
                    source,
                    downstream: downstream.clone_ref(py),
                },
            )?;
            let writer_callable: PyObject = writer.into_any();

            // 订阅上游
            let sub = if let Ok(method) = self.upstream.getattr(py, "subscribe") {
                method.call1(py, (writer_callable,))?
            } else {
                return Err(pyo3::exceptions::PyTypeError::new_err(
                    "write_to_clipboard 上游必须有 subscribe 方法",
                ));
            };
            Ok(sub)
        })
    }
}

#[pyclass(name = "_ValueWriter")]
pub struct ValueWriter {
    pub dispatcher: Py<ClipboardDispatcher>,
    pub source: Option<String>,
    pub downstream: PyObject,
}

#[pymethods]
impl ValueWriter {
    fn __call__(&self, value: PyObject) -> PyResult<()> {
        Python::with_gil(|py| {
            // 从 value 构造 content / files / change_type / tags / metadata
            let (content, files, change_type, tags, metadata) =
                parse_value(py, value.clone_ref(py))?;
            // 调用 dispatcher.set_clipboard
            let clip = ClipboardDispatcher::set_clipboard(
                self.dispatcher.clone_ref(py),
                content,
                files,
                change_type,
                self.source.clone(),
                tags,
                metadata,
            )?;
            // 向下游分发
            let _ = self.downstream.call1(py, (clip,));
            Ok(())
        })
    }
}

#[pyfunction]
#[pyo3(signature = (dispatcher, source=None))]
pub fn write_to_clipboard(
    py: Python<'_>,
    dispatcher: PyObject,
    source: Option<String>,
) -> PyResult<PyObject> {
    // 支持接受 dispatcher 参数为 PyObject
    if let Ok(d) = dispatcher.extract::<Py<ClipboardDispatcher>>(py) {
        let op = Py::new(
            py,
            WriteToClipboardOperator {
                dispatcher: d.clone_ref(py),
                source,
            },
        )?;
        return Ok(op.into_any());
    }
    // 尝试从属性获取
    if let Ok(d) = dispatcher.getattr(py, "dispatcher") {
        if let Ok(disp) = d.extract::<Py<ClipboardDispatcher>>(py) {
            let op = Py::new(
                py,
                WriteToClipboardOperator {
                    dispatcher: disp.clone_ref(py),
                    source,
                },
            )?;
            return Ok(op.into_any());
        }
    }
    Err(pyo3::exceptions::PyTypeError::new_err(
        "write_to_clipboard 第一个参数必须是 ClipboardDispatcher 或 ClipSubject",
    ))
}

fn parse_value(
    py: Python<'_>,
    value: PyObject,
) -> PyResult<(
    Option<PyObject>,
    Option<Vec<String>>,
    Option<Py<ClipChangeType>>,
    Option<Vec<String>>,
    Option<Py<PyDict>>,
)> {
    // 如果是 ClipData，直接取内容
    if let Ok(clip) = value.extract::<Py<ClipData>>(py) {
        let clip_b = clip.borrow(py);
        let content = Some(clip_b.get_content(py));
        let files = Some(clip_b.files.clone());
        let change_type = Some(clip_b.change_type.clone_ref(py));
        let tags = Some(clip_b.tags.clone());
        let metadata = Some(clip_b.metadata.clone_ref(py));
        return Ok((content, files, change_type, tags, metadata));
    }

    // string
    if let Ok(s) = value.extract::<String>(py) {
        return Ok((
            Some(s.into_pyobject(py)?.unbind().into_any()),
            None,
            Some(Py::new(py, ClipChangeType { value: 0 })?),
            None,
            None,
        ));
    }

    // bytes
    if let Ok(b) = value.extract::<Vec<u8>>(py) {
        return Ok((
            Some(b.into_pyobject(py)?.unbind().into_any()),
            None,
            Some(Py::new(py, ClipChangeType { value: 2 })?),
            None,
            None,
        ));
    }

    // dict
    if let Ok(d) = value.extract::<Py<PyDict>>(py) {
        let bound = d.bind(py);
        let content = bound.get_item("content")?.and_then(|v| Some(v.unbind()));
        let files = bound
            .get_item("files")?
            .and_then(|v| v.extract::<Vec<String>>().ok());
        let ct = bound
            .get_item("change_type")?
            .and_then(|v| v.extract::<Py<ClipChangeType>>().ok());
        let tags = bound
            .get_item("tags")?
            .and_then(|v| v.extract::<Vec<String>>().ok());
        let metadata = bound
            .get_item("metadata")?
            .and_then(|v| v.extract::<Py<PyDict>>().ok());
        return Ok((content, files, ct, tags, metadata));
    }

    // tuple / list
    if let Ok(tup) = value.extract::<Vec<PyObject>>(py) {
        let mut iter = tup.into_iter();
        let first = iter.next();
        let second = iter.next();
        let third = iter.next();
        let fourth = iter.next();
        let fifth = iter.next();
        let files = second.and_then(|v| v.extract::<Vec<String>>(py).ok());
        let ct = third.and_then(|v| v.extract::<Py<ClipChangeType>>(py).ok());
        let tags = fourth.and_then(|v| v.extract::<Vec<String>>(py).ok());
        let metadata = fifth.and_then(|v| v.extract::<Py<PyDict>>(py).ok());
        return Ok((first, files, ct, tags, metadata));
    }

    // 作为字符串写回
    let s = value.str()?.to_string();
    Ok((
        Some(s.into_pyobject(py)?.unbind().into_any()),
        None,
        Some(Py::new(py, ClipChangeType { value: 0 })?),
        None,
        None,
    ))
}

// 模块注册辅助
pub fn register_clipboard_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<ClipChangeType>()?;
    m.add_class::<ClipData>()?;
    m.add_class::<ClipboardDispatcher>()?;
    m.add_class::<crate::clipboard::observer::ClipObserver>()?;
    m.add_class::<crate::clipboard::subject::ClipSubject>()?;
    m.add_class::<WriteToClipboardOperator>()?;
    m.add_class::<WriteToClipboardSubscribeFn>()?;
    m.add_class::<ValueWriter>()?;
    m.add_function(wrap_pyfunction!(from_clipboard, m)?)?;
    m.add_function(wrap_pyfunction!(write_to_clipboard, m)?)?;
    Ok(())
}
