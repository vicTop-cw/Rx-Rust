// KeyObserver / MouseObserver：根据事件类型路由到不同回调

use std::sync::{Arc, Mutex as StdMutex};

use pyo3::prelude::*;

use crate::keyboard_mouse::types::{KeyData, MouseData, MouseEventType};

type Cb = Arc<dyn Fn(PyObject) + Send + Sync>;

// ============================================================================
// KeyObserver - 键盘事件观察者
// ============================================================================

struct KeyObserverCallbacks {
    on_press: Option<Cb>,     // key_code 刚按下
    on_release: Option<Cb>,   // key_code 刚释放
    on_hold: Option<Cb>,      // key_code 持续按住
    on_any: Option<Cb>,       // 所有事件
    on_hotkey: Option<Cb>,    // 组合键（暂不支持，留 API）
    on_error: Option<Cb>,     // 错误回调
    on_completed: Option<Cb>, // 完成回调
}

#[pyclass(name = "KeyObserver")]
pub struct KeyObserver {
    callbacks: Arc<StdMutex<KeyObserverCallbacks>>,
    subscription: Arc<StdMutex<Option<PyObject>>>,
}

#[pymethods]
impl KeyObserver {
    #[new]
    #[pyo3(signature = (on_press=None, on_release=None, on_hold=None, on_any=None, on_hotkey=None, on_error=None, on_completed=None))]
    fn new(
        on_press: Option<PyObject>,
        on_release: Option<PyObject>,
        on_hold: Option<PyObject>,
        on_any: Option<PyObject>,
        on_hotkey: Option<PyObject>,
        on_error: Option<PyObject>,
        on_completed: Option<PyObject>,
    ) -> PyResult<Self> {
        let wrap = |cb: Option<PyObject>| -> Option<Cb> {
            cb.map(|c| {
                Arc::new(move |data: PyObject| {
                    Python::with_gil(|py| {
                        let _ = c.call1(py, (data,));
                    });
                })
            })
        };

        Ok(KeyObserver {
            callbacks: Arc::new(StdMutex::new(KeyObserverCallbacks {
                on_press: wrap(on_press),
                on_release: wrap(on_release),
                on_hold: wrap(on_hold),
                on_any: wrap(on_any),
                on_hotkey: wrap(on_hotkey),
                on_error: wrap(on_error),
                on_completed: wrap(on_completed),
            })),
            subscription: Arc::new(StdMutex::new(None)),
        })
    }

    /// 事件入口：根据 is_press 路由到对应回调
    fn __call__(&self, fd: Py<KeyData>, py: Python<'_>) -> PyResult<()> {
        Python::with_gil(|py| {
            let is_press = fd.borrow(py).is_press;
            let cbs = self.callbacks.lock().unwrap();

            // 根据 is_press 路由
            if is_press {
                if let Some(cb) = &cbs.on_press {
                    cb(fd.clone_ref(py));
                }
            } else {
                if let Some(cb) = &cbs.on_release {
                    cb(fd.clone_ref(py));
                }
            }

            // on_any 始终调用
            if let Some(cb) = &cbs.on_any {
                cb(fd);
            }
        });
        Ok(())
    }

    /// 订阅主题（通常是 KeyboardDispatcher 或类似对象）
    fn subscribe(&self, subject: PyObject, py: Python<'_>) -> PyResult<PyObject> {
        let self_py = Py::new(py, self)?;
        let wrapper = KeyObserverWrapper {
            inner: self_py.clone(),
        };
        let wrapper_py = Py::new(py, wrapper)?.into_any();

        // 获取 on_error 和 on_completed 回调
        let on_error_cb = self.callbacks.lock().unwrap().on_error.clone();
        let on_completed_cb = self.callbacks.lock().unwrap().on_completed.clone();

        // 创建 on_error wrapper
        let on_error_py = if let Some(cb) = on_error_cb {
            let cb_clone = cb.clone();
            Some(Py::new(py, KeyObserverErrorWrapper { inner: cb_clone })?.into_any())
        } else {
            None
        };

        // 创建 on_completed wrapper
        let on_completed_py = if let Some(cb) = on_completed_cb {
            let cb_clone = cb.clone();
            Some(Py::new(py, KeyObserverCompletedWrapper { inner: cb_clone })?.into_any())
        } else {
            None
        };

        // 调用 subject.subscribe(on_next, on_error, on_completed)
        let args = if on_error_py.is_some() && on_completed_py.is_some() {
            (wrapper_py, on_error_py.unwrap(), on_completed_py.unwrap())
        } else if on_error_py.is_some() {
            (wrapper_py, on_error_py.unwrap())
        } else {
            (wrapper_py,)
        };
        let sub = subject.call_method1(py, "subscribe", args)?;
        self.subscription.lock().unwrap().replace(sub.extract(py)?);
        Ok(sub)
    }

    /// 链式订阅：订阅后返回 self
    fn attach<'py>(&self, subject: PyObject, py: Python<'py>) -> PyResult<PyRef<'py, Self>> {
        self.subscribe(subject, py)?;
        // 返回 self 需要通过 PyRef
        // 由于无法直接返回 PyRef，我们返回一个 clone 的方式
        // 实际上这里需要重新考虑实现方式
        Ok(PyRef::new(
            py,
            KeyObserver {
                callbacks: self.callbacks.clone(),
                subscription: self.subscription.clone(),
            },
        )?)
    }

    /// 退订：释放订阅
    fn unsubscribe(&self, py: Python<'_>) -> PyResult<()> {
        if let Some(sub) = self.subscription.lock().unwrap().take() {
            sub.call_method0(py, "dispose")?;
        }
        Ok(())
    }

    /// 是否已订阅
    #[getter]
    fn is_subscribed(&self) -> bool {
        self.subscription.lock().unwrap().is_some()
    }

    /// Context manager 入口
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Context manager 出口
    fn __exit__(
        &self,
        py: Python<'_>,
        _exc_type: PyObject,
        _exc_val: PyObject,
        _exc_tb: PyObject,
    ) -> PyResult<bool> {
        self.unsubscribe(py)?;
        Ok(false)
    }
}

// 用于 subscribe 时包装 KeyObserver 的 Rust callable
#[pyclass]
struct KeyObserverWrapper {
    inner: Py<KeyObserver>,
}

#[pymethods]
impl KeyObserverWrapper {
    fn __call__(&self, fd: Py<KeyData>, py: Python<'_>) -> PyResult<()> {
        self.inner.call__(fd, py)
    }
}

// 用于包装 on_error 回调
#[pyclass]
struct KeyObserverErrorWrapper {
    inner: Cb,
}

#[pymethods]
impl KeyObserverErrorWrapper {
    fn __call__(&self, err: PyObject) -> PyResult<()> {
        self.inner(err);
        Ok(())
    }
}

// 用于包装 on_completed 回调
#[pyclass]
struct KeyObserverCompletedWrapper {
    inner: Cb,
}

#[pymethods]
impl KeyObserverCompletedWrapper {
    fn __call__(&self) -> PyResult<()> {
        // on_completed 不需要参数，但我们传入 None
        Python::with_gil(|py| {
            self.inner(py.None());
        });
        Ok(())
    }
}

// ============================================================================
// MouseObserver - 鼠标事件观察者
// ============================================================================

struct MouseObserverCallbacks {
    on_move: Option<Cb>,       // 移动
    on_left_down: Option<Cb>,  // 左键按下
    on_left_up: Option<Cb>,    // 左键释放
    on_right_down: Option<Cb>, // 右键按下
    on_right_up: Option<Cb>,   // 右键释放
    on_middle_down: Option<Cb>,
    on_middle_up: Option<Cb>,
    on_scroll: Option<Cb>,    // 滚轮
    on_drag: Option<Cb>,      // 拖拽
    on_click: Option<Cb>,     // 点击（down+up）
    on_any: Option<Cb>,       // 所有事件
    on_error: Option<Cb>,     // 错误回调
    on_completed: Option<Cb>, // 完成回调
}

#[pyclass(name = "MouseObserver")]
pub struct MouseObserver {
    callbacks: Arc<StdMutex<MouseObserverCallbacks>>,
    subscription: Arc<StdMutex<Option<PyObject>>>,
    // 点击检测状态：记录最近一次 LeftDown 的位置
    last_click_down: Arc<StdMutex<Option<(i32, i32)>>>,
    // 拖拽检测状态
    is_dragging: Arc<StdMutex<bool>>,
    drag_start: Arc<StdMutex<Option<(i32, i32)>>>,
}

#[pymethods]
impl MouseObserver {
    #[new]
    #[pyo3(signature = (on_move=None, on_left_down=None, on_left_up=None, on_right_down=None, on_right_up=None, on_middle_down=None, on_middle_up=None, on_scroll=None, on_drag=None, on_click=None, on_any=None, on_error=None, on_completed=None))]
    fn new(
        on_move: Option<PyObject>,
        on_left_down: Option<PyObject>,
        on_left_up: Option<PyObject>,
        on_right_down: Option<PyObject>,
        on_right_up: Option<PyObject>,
        on_middle_down: Option<PyObject>,
        on_middle_up: Option<PyObject>,
        on_scroll: Option<PyObject>,
        on_drag: Option<PyObject>,
        on_click: Option<PyObject>,
        on_any: Option<PyObject>,
        on_error: Option<PyObject>,
        on_completed: Option<PyObject>,
    ) -> PyResult<Self> {
        let wrap = |cb: Option<PyObject>| -> Option<Cb> {
            cb.map(|c| {
                Arc::new(move |data: PyObject| {
                    Python::with_gil(|py| {
                        let _ = c.call1(py, (data,));
                    });
                })
            })
        };

        Ok(MouseObserver {
            callbacks: Arc::new(StdMutex::new(MouseObserverCallbacks {
                on_move: wrap(on_move),
                on_left_down: wrap(on_left_down),
                on_left_up: wrap(on_left_up),
                on_right_down: wrap(on_right_down),
                on_right_up: wrap(on_right_up),
                on_middle_down: wrap(on_middle_down),
                on_middle_up: wrap(on_middle_up),
                on_scroll: wrap(on_scroll),
                on_drag: wrap(on_drag),
                on_click: wrap(on_click),
                on_any: wrap(on_any),
                on_error: wrap(on_error),
                on_completed: wrap(on_completed),
            })),
            subscription: Arc::new(StdMutex::new(None)),
            last_click_down: Arc::new(StdMutex::new(None)),
            is_dragging: Arc::new(StdMutex::new(false)),
            drag_start: Arc::new(StdMutex::new(None)),
        })
    }

    /// 事件入口：根据 event_type 路由到对应回调
    fn __call__(&self, md: Py<MouseData>, py: Python<'_>) -> PyResult<()> {
        Python::with_gil(|py| {
            let event_type = md.borrow(py).event_type;
            let x = md.borrow(py).x;
            let y = md.borrow(py).y;
            let cbs = self.callbacks.lock().unwrap();

            match event_type {
                0 => {
                    // MOVE
                    if let Some(cb) = &cbs.on_move {
                        cb(md.clone_ref(py));
                    }
                }
                1 => {
                    // LEFT_DOWN
                    if let Some(cb) = &cbs.on_left_down {
                        cb(md.clone_ref(py));
                    }
                    // 记录点击位置用于 click 检测
                    *self.last_click_down.lock().unwrap() = Some((x, y));
                    // 开始拖拽
                    *self.is_dragging.lock().unwrap() = true;
                    *self.drag_start.lock().unwrap() = Some((x, y));
                }
                2 => {
                    // LEFT_UP
                    if let Some(cb) = &cbs.on_left_up {
                        cb(md.clone_ref(py));
                    }
                    // click 检测：检查是否在原位置（±2 像素）
                    if let Some((sx, sy)) = self.last_click_down.lock().unwrap().take() {
                        if (x - sx).abs() <= 2 && (y - sy).abs() <= 2 {
                            if let Some(cb) = &cbs.on_click {
                                cb(md.clone_ref(py));
                            }
                        }
                    }
                    // 结束拖拽
                    *self.is_dragging.lock().unwrap() = false;
                }
                3 => {
                    // RIGHT_DOWN
                    if let Some(cb) = &cbs.on_right_down {
                        cb(md.clone_ref(py));
                    }
                }
                4 => {
                    // RIGHT_UP
                    if let Some(cb) = &cbs.on_right_up {
                        cb(md.clone_ref(py));
                    }
                }
                5 => {
                    // MIDDLE_DOWN
                    if let Some(cb) = &cbs.on_middle_down {
                        cb(md.clone_ref(py));
                    }
                }
                6 => {
                    // MIDDLE_UP
                    if let Some(cb) = &cbs.on_middle_up {
                        cb(md.clone_ref(py));
                    }
                }
                7 => {
                    // SCROLL
                    if let Some(cb) = &cbs.on_scroll {
                        cb(md.clone_ref(py));
                    }
                }
                8 => {
                    // DRAG
                    if let Some(cb) = &cbs.on_drag {
                        cb(md.clone_ref(py));
                    }
                }
                _ => {}
            }

            // on_any 始终调用
            if let Some(cb) = &cbs.on_any {
                cb(md);
            }
        });
        Ok(())
    }

    /// 内部处理拖拽事件的回调（由外部系统调用）
    fn _handle_drag_move(&self, md: Py<MouseData>, py: Python<'_>) -> PyResult<()> {
        let is_dragging = *self.is_dragging.lock().unwrap();
        if !is_dragging {
            return Ok(());
        }

        let x = md.borrow(py).x;
        let y = md.borrow(py).y;

        if let Some((sx, sy)) = self.drag_start.lock().unwrap().take() {
            Python::with_gil(|py| {
                let cbs = self.callbacks.lock().unwrap();
                if let Some(cb) = &cbs.on_drag {
                    // 构造拖拽信息
                    let drag_info = (sx, sy, x, y);
                    let _ = cb(drag_info.into_pyobject(py).unwrap().unbind());
                }
            });
        }
        *self.drag_start.lock().unwrap() = Some((x, y));
        Ok(())
    }

    /// 订阅主题
    fn subscribe(&self, subject: PyObject, py: Python<'_>) -> PyResult<PyObject> {
        let self_py = Py::new(py, self)?;
        let wrapper = MouseObserverWrapper {
            inner: self_py.clone(),
        };
        let wrapper_py = Py::new(py, wrapper)?.into_any();

        // 获取 on_error 和 on_completed 回调
        let on_error_cb = self.callbacks.lock().unwrap().on_error.clone();
        let on_completed_cb = self.callbacks.lock().unwrap().on_completed.clone();

        // 创建 on_error wrapper
        let on_error_py = if let Some(cb) = on_error_cb {
            let cb_clone = cb.clone();
            Some(Py::new(py, MouseObserverErrorWrapper { inner: cb_clone })?.into_any())
        } else {
            None
        };

        // 创建 on_completed wrapper
        let on_completed_py = if let Some(cb) = on_completed_cb {
            let cb_clone = cb.clone();
            Some(Py::new(py, MouseObserverCompletedWrapper { inner: cb_clone })?.into_any())
        } else {
            None
        };

        // 调用 subject.subscribe(on_next, on_error, on_completed)
        let args = if on_error_py.is_some() && on_completed_py.is_some() {
            (wrapper_py, on_error_py.unwrap(), on_completed_py.unwrap())
        } else if on_error_py.is_some() {
            (wrapper_py, on_error_py.unwrap())
        } else {
            (wrapper_py,)
        };
        let sub = subject.call_method1(py, "subscribe", args)?;
        self.subscription.lock().unwrap().replace(sub.extract(py)?);
        Ok(sub)
    }

    /// 链式订阅：订阅后返回 self
    fn attach<'py>(&self, subject: PyObject, py: Python<'py>) -> PyResult<PyRef<'py, Self>> {
        self.subscribe(subject, py)?;
        Ok(PyRef::new(
            py,
            MouseObserver {
                callbacks: self.callbacks.clone(),
                subscription: self.subscription.clone(),
                last_click_down: self.last_click_down.clone(),
                is_dragging: self.is_dragging.clone(),
                drag_start: self.drag_start.clone(),
            },
        )?)
    }

    /// 退订：释放订阅
    fn unsubscribe(&self, py: Python<'_>) -> PyResult<()> {
        if let Some(sub) = self.subscription.lock().unwrap().take() {
            sub.call_method0(py, "dispose")?;
        }
        *self.last_click_down.lock().unwrap() = None;
        *self.is_dragging.lock().unwrap() = false;
        *self.drag_start.lock().unwrap() = None;
        Ok(())
    }

    /// 是否已订阅
    #[getter]
    fn is_subscribed(&self) -> bool {
        self.subscription.lock().unwrap().is_some()
    }

    /// Context manager 入口
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Context manager 出口
    fn __exit__(
        &self,
        py: Python<'_>,
        _exc_type: PyObject,
        _exc_val: PyObject,
        _exc_tb: PyObject,
    ) -> PyResult<bool> {
        self.unsubscribe(py)?;
        Ok(false)
    }
}

// 用于 subscribe 时包装 MouseObserver 的 Rust callable
#[pyclass]
struct MouseObserverWrapper {
    inner: Py<MouseObserver>,
}

#[pymethods]
impl MouseObserverWrapper {
    fn __call__(&self, md: Py<MouseData>, py: Python<'_>) -> PyResult<()> {
        self.inner.call__(md, py)
    }
}

// 用于包装 on_error 回调
#[pyclass]
struct MouseObserverErrorWrapper {
    inner: Cb,
}

#[pymethods]
impl MouseObserverErrorWrapper {
    fn __call__(&self, err: PyObject) -> PyResult<()> {
        self.inner(err);
        Ok(())
    }
}

// 用于包装 on_completed 回调
#[pyclass]
struct MouseObserverCompletedWrapper {
    inner: Cb,
}

#[pymethods]
impl MouseObserverCompletedWrapper {
    fn __call__(&self) -> PyResult<()> {
        // on_completed 不需要参数，但我们传入 None
        Python::with_gil(|py| {
            self.inner(py.None());
        });
        Ok(())
    }
}
