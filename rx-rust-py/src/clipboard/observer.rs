// ClipObserver：根据 change_type 路由到不同回调

use std::sync::{Arc, Mutex as StdMutex};

use pyo3::prelude::*;

use crate::clipboard::types::ClipChangeType;

type Cb = Arc<dyn Fn(PyObject) + Send + Sync>;

struct Callbacks {
    on_text: Option<Cb>,
    on_files: Option<Cb>,
    on_image: Option<Cb>,
    on_any: Option<Cb>,
}

#[pyclass(name = "ClipObserver")]
pub struct ClipObserver {
    callbacks: StdMutex<Callbacks>,
    subscriptions: StdMutex<Vec<PyObject>>,
    // 用于存储 __call__ 回调的内部 closure
    handler: StdMutex<Option<PyObject>>,
}

#[pymethods]
impl ClipObserver {
    #[new]
    #[pyo3(signature = (on_text=None, on_files=None, on_image=None, on_any=None))]
    fn new(
        on_text: Option<PyObject>,
        on_files: Option<PyObject>,
        on_image: Option<PyObject>,
        on_any: Option<PyObject>,
    ) -> PyResult<Self> {
        let wrap = |cb: Option<PyObject>| -> Option<Cb> {
            cb.map(|c| {
                Arc::new(move |clip: PyObject| {
                    Python::with_gil(|py| {
                        let _ = c.call1(py, (clip,));
                    });
                })
            })
        };

        Ok(ClipObserver {
            callbacks: StdMutex::new(Callbacks {
                on_text: wrap(on_text),
                on_files: wrap(on_files),
                on_image: wrap(on_image),
                on_any: wrap(on_any),
            }),
            subscriptions: StdMutex::new(Vec::new()),
            handler: StdMutex::new(None),
        })
    }

    fn __call__(&self, clip: PyObject) -> PyResult<()> {
        // 路由回调：根据 ClipData.change_type 选择
        Python::with_gil(|py| {
            let ct = clip
                .getattr(py, "change_type")
                .ok();
            let value: i64 = match ct {
                Some(v) => {
                    if let Ok(val) = v.call_method0(py, "__int__") {
                        val.extract(py).unwrap_or(0)
                    } else if let Ok(v2) = v.extract::<i64>(py) {
                        v2
                    } else {
                        0
                    }
                }
                None => 0,
            };

            let cbs = self.callbacks.lock().unwrap();
            match value {
                0 => {
                    if let Some(cb) = &cbs.on_text {
                        cb(clip.clone_ref(py));
                    }
                }
                1 => {
                    if let Some(cb) = &cbs.on_files {
                        cb(clip.clone_ref(py));
                    }
                }
                2 => {
                    if let Some(cb) = &cbs.on_image {
                        cb(clip.clone_ref(py));
                    }
                }
                // 其它类型不特定路由
                _ => {}
            }
            if let Some(cb) = &cbs.on_any {
                cb(clip);
            }
        });
        Ok(())
    }

    fn subscribe(&self, subject_or_observable: PyObject) -> PyResult<PyObject> {
        Python::with_gil(|py| {
            // 构造一个"绑定 self 为 callable"的 Python 对象
            // 我们用一个 closure 作为 subscribe 入参
            let self_py = Py::new(py, self)?;
            let observer_callable: PyObject = self_py.into_any();

            // 尝试 subject.subscribe(observer)
            let sub = if let Ok(method) = subject_or_observable.getattr(py, "subscribe") {
                method.call1(py, (observer_callable,))?
            } else if subject_or_observable.hasattr(py, "subject")? {
                let inner = subject_or_observable.getattr(py, "subject")?;
                inner.call_method1(py, "subscribe", (observer_callable,))?
            } else {
                return Err(pyo3::exceptions::PyTypeError::new_err(
                    "subject_or_observable 必须是有 subscribe 方法的对象",
                ));
            };
            // 记录订阅，以便取消
            self.subscriptions.lock().unwrap().push(sub.clone_ref(py));
            Ok(sub)
        })
    }

    fn unsubscribe(&self) {
        let subs = std::mem::take(&mut *self.subscriptions.lock().unwrap());
        Python::with_gil(|py| {
            for sub in subs {
                if let Ok(method) = sub.getattr(py, "dispose") {
                    let _ = method.call0(py);
                }
            }
        });
    }
}
