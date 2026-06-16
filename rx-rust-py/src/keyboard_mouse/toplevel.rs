// 键盘鼠标顶层 API：from_keyboard / from_mouse / write_to_keyboard / write_to_mouse

use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::keyboard_mouse::dispatcher::{KeyboardDispatcher, MouseDispatcher};
use crate::keyboard_mouse::observer::{KeyObserver, MouseObserver};
use crate::keyboard_mouse::subject::{KeySubject, MouseSubject};
use crate::keyboard_mouse::types::{KeyData, KeyEventType, KeyModifier, MouseData, MouseEventType};
use crate::Subscription;

// =====================================================================
// 顶层工厂函数
// =====================================================================

/// 从键盘创建设 Subject + Dispatcher 元组
#[pyfunction]
#[pyo3(signature = (backend="auto", interval=0.05, filter_self=true, auto_start=true, self_filter=None))]
pub fn from_keyboard(
    py: Python<'_>,
    backend: &str,
    interval: f64,
    filter_self: bool,
    auto_start: bool,
    self_filter: Option<PyObject>,
) -> PyResult<(PyObject, PyObject)> {
    let dispatcher_class = py.get_type_bound::<KeyboardDispatcher>();
    let dispatcher = dispatcher_class.call1((backend.to_string(), interval, filter_self, self_filter, 32))?;

    if auto_start {
        dispatcher.call_method0("start")?;
    }

    let subject = dispatcher.getattr("subject")?.unbind();
    Ok((subject, dispatcher.into()))
}

/// 从鼠标创建 Subject + Dispatcher 元组
#[pyfunction]
#[pyo3(signature = (backend="auto", interval=0.05, filter_self=true, auto_start=true, self_filter=None))]
pub fn from_mouse(
    py: Python<'_>,
    backend: &str,
    interval: f64,
    filter_self: bool,
    auto_start: bool,
    self_filter: Option<PyObject>,
) -> PyResult<(PyObject, PyObject)> {
    let dispatcher_class = py.get_type_bound::<MouseDispatcher>();
    let dispatcher = dispatcher_class.call1((backend.to_string(), interval, filter_self, self_filter, 32))?;

    if auto_start {
        dispatcher.call_method0("start")?;
    }

    let subject = dispatcher.getattr("subject")?.unbind();
    Ok((subject, dispatcher.into()))
}

// =====================================================================
// 写入键盘操作符
// =====================================================================

/// WriteKeyboardOp：键盘写入操作符，接收上游 Observable
#[pyclass(name = "_WriteKeyboardOperator")]
pub struct WriteKeyboardOp {
    dispatcher: Py<KeyboardDispatcher>,
}

#[pymethods]
impl WriteKeyboardOp {
    fn __call__(&self, py: Python<'_>, source: PyObject) -> PyResult<WriteKeyboardObs> {
        let obs = WriteKeyboardObs {
            source,
            dispatcher: self.dispatcher.clone_ref(py),
        };
        Ok(obs)
    }
}

/// WriteKeyboardObs：包装上游 Observable，实现 subscribe 方法
#[pyclass(name = "_WriteKeyboardObservable")]
pub struct WriteKeyboardObs {
    source: PyObject,
    dispatcher: Py<KeyboardDispatcher>,
}

#[pymethods]
impl WriteKeyboardObs {
    fn subscribe(&self, on_next: PyObject, py: Python<'_>) -> PyResult<PyObject> {
        let handler = WriteKeyboardHandler {
            downstream: on_next,
            dispatcher: self.dispatcher.clone_ref(py),
        };
        let handler_py = Py::new(py, handler)?.into_any();
        let source_ref = self.source.bind(py);
        let result = source_ref.call_method1("subscribe", (handler_py,))?;
        Ok(result.unbind())
    }
}

/// WriteKeyboardHandler：处理每个值，写入键盘事件后透传给下游
#[pyclass(name = "_WriteKeyboardHandler")]
pub struct WriteKeyboardHandler {
    downstream: PyObject,
    dispatcher: Py<KeyboardDispatcher>,
}

#[pymethods]
impl WriteKeyboardHandler {
    fn __call__(&self, item: PyObject, py: Python<'_>) -> PyResult<()> {
        // 支持的类型：
        // 1. str: 直接 type_text
        // 2. int: 作为 key_code，按下+释放
        // 3. dict: {"key": "A"} 或 {"text": "hello"} 或 {"key_code": 65, "is_press": true}
        // 4. KeyData: 使用 key_code 和 is_press

        let disp = self.dispatcher.bind(py);

        // 尝试 str: 直接 type_text
        if let Ok(text) = item.extract::<String>(py) {
            disp.call_method1("type_text", (text.as_str(),))?;
            let _ = self.downstream.call1(py, (item,));
            return Ok(());
        }

        // 尝试 int: 作为 key_code，按下+释放
        if let Ok(code) = item.extract::<i32>(py) {
            let key_name = format!("VK_0x{:02X}", code);
            disp.call_method1("press", (&key_name,))?;
            disp.call_method1("release", (&key_name,))?;
            let _ = self.downstream.call1(py, (item,));
            return Ok(());
        }

        // 尝试 KeyData
        if let Ok(key_data) = item.extract::<Py<KeyData>>(py) {
            let key_data_b = key_data.borrow(py);
            let key_name = &key_data_b.key_name;
            if key_data_b.is_press {
                disp.call_method1("press", (key_name,))?;
            } else {
                disp.call_method1("release", (key_name,))?;
            }
            let _ = self.downstream.call1(py, (item,));
            return Ok(());
        }

        // 尝试 dict
        if let Ok(dict) = item.extract::<Py<PyDict>>(py) {
            let dict_b = dict.bind(py);

            // 优先取 text
            if let Ok(Some(text)) = dict_b.get_item("text") {
                let t: String = text.extract()?;
                disp.call_method1("type_text", (t.as_str(),))?;
                let _ = self.downstream.call1(py, (item,));
                return Ok(());
            }

            // 取 key
            if let Ok(Some(key)) = dict_b.get_item("key") {
                let k: String = key.extract()?;
                if let Ok(Some(is_press)) = dict_b.get_item("is_press") {
                    let press: bool = is_press.extract()?;
                    if press {
                        disp.call_method1("press", (&k,))?;
                    } else {
                        disp.call_method1("release", (&k,))?;
                    }
                } else {
                    // 默认：按下 + 释放
                    disp.call_method1("press", (&k,))?;
                    disp.call_method1("release", (&k,))?;
                }
                let _ = self.downstream.call1(py, (item,));
                return Ok(());
            }

            // 取 key_code
            if let Ok(Some(key_code_val)) = dict_b.get_item("key_code") {
                let key_code: u32 = key_code_val.extract()?;
                let key_name = format!("VK_0x{:02X}", key_code);
                if let Ok(Some(is_press_val)) = dict_b.get_item("is_press") {
                    let is_press: bool = is_press_val.extract()?;
                    if is_press {
                        disp.call_method1("press", (&key_name,))?;
                    } else {
                        disp.call_method1("release", (&key_name,))?;
                    }
                } else {
                    disp.call_method1("press", (&key_name,))?;
                    disp.call_method1("release", (&key_name,))?;
                }
                let _ = self.downstream.call1(py, (item,));
                return Ok(());
            }
        }

        // 无法解析，直接透传
        let _ = self.downstream.call1(py, (item,));
        Ok(())
    }
}

// =====================================================================
// 写入鼠标操作符
// =====================================================================

/// WriteMouseOp：鼠标写入操作符，接收上游 Observable
#[pyclass(name = "_WriteMouseOperator")]
pub struct WriteMouseOp {
    dispatcher: Py<MouseDispatcher>,
}

#[pymethods]
impl WriteMouseOp {
    fn __call__(&self, py: Python<'_>, source: PyObject) -> PyResult<WriteMouseObs> {
        let obs = WriteMouseObs {
            source,
            dispatcher: self.dispatcher.clone_ref(py),
        };
        Ok(obs)
    }
}

/// WriteMouseObs：包装上游 Observable，实现 subscribe 方法
#[pyclass(name = "_WriteMouseObservable")]
pub struct WriteMouseObs {
    source: PyObject,
    dispatcher: Py<MouseDispatcher>,
}

#[pymethods]
impl WriteMouseObs {
    fn subscribe(&self, on_next: PyObject, py: Python<'_>) -> PyResult<PyObject> {
        let handler = WriteMouseHandler {
            downstream: on_next,
            dispatcher: self.dispatcher.clone_ref(py),
        };
        let handler_py = Py::new(py, handler)?.into_any();
        let source_ref = self.source.bind(py);
        let result = source_ref.call_method1("subscribe", (handler_py,))?;
        Ok(result.unbind())
    }
}

/// WriteMouseHandler：处理每个值，执行鼠标操作后透传给下游
#[pyclass(name = "_WriteMouseHandler")]
pub struct WriteMouseHandler {
    downstream: PyObject,
    dispatcher: Py<MouseDispatcher>,
}

#[pymethods]
impl WriteMouseHandler {
    fn __call__(&self, item: PyObject, py: Python<'_>) -> PyResult<()> {
        // 支持的类型：
        // 1. dict: {"x": 100, "y": 200, "event": "move"|"click"|"scroll"|"drag"}
        // 2. tuple/list: (x, y, event_type)
        // 3. MouseData 对象

        let disp = self.dispatcher.bind(py);

        // 尝试 MouseData
        if let Ok(mouse_data) = item.extract::<Py<MouseData>>(py) {
            let mouse_data_b = mouse_data.borrow(py);
            match mouse_data_b.event_type {
                0 => {
                    // MOVE
                    disp.call_method1("move_to", (mouse_data_b.x, mouse_data_b.y))?;
                }
                1 | 3 | 5 => {
                    // LEFT_DOWN / RIGHT_DOWN / MIDDLE_DOWN
                    disp.call_method1("move_to", (mouse_data_b.x, mouse_data_b.y))?;
                    disp.call_method1("click", (&mouse_data_b.button,))?;
                }
                2 | 4 | 6 => {
                    // LEFT_UP / RIGHT_UP / MIDDLE_UP
                    disp.call_method1("click", (&mouse_data_b.button,))?;
                }
                7 => {
                    // SCROLL
                    disp.call_method1("scroll", (mouse_data_b.delta,))?;
                }
                8 => {
                    // DRAG - MouseData 不直接支持，需要传入完整参数
                    disp.call_method1("move_to", (mouse_data_b.x, mouse_data_b.y))?;
                }
                _ => {}
            }
            let _ = self.downstream.call1(py, (item,));
            return Ok(());
        }

        // 尝试 tuple/list: (x, y, event_type) 或 (x, y, event_type, button)
        if let Ok(tup) = item.extract::<Vec<PyObject>>(py) {
            if tup.len() >= 3 {
                let x: i32 = tup[0].extract(py)?;
                let y: i32 = tup[1].extract(py)?;
                let event_type: u8 = tup[2].extract(py)?;

                match event_type {
                    0 => {
                        disp.call_method1("move_to", (x, y))?;
                    }
                    1 => {
                        disp.call_method1("move_to", (x, y))?;
                        disp.call_method1("click", ("left",))?;
                    }
                    2 => {
                        disp.call_method1("click", ("left",))?;
                    }
                    3 => {
                        disp.call_method1("move_to", (x, y))?;
                        disp.call_method1("click", ("right",))?;
                    }
                    4 => {
                        disp.call_method1("click", ("right",))?;
                    }
                    5 => {
                        disp.call_method1("move_to", (x, y))?;
                        disp.call_method1("click", ("middle",))?;
                    }
                    6 => {
                        disp.call_method1("click", ("middle",))?;
                    }
                    7 => {
                        // scroll: delta in y
                        let delta: i32 = if tup.len() >= 4 {
                            tup[3].extract(py)?
                        } else {
                            y
                        };
                        disp.call_method1("scroll", (delta,))?;
                    }
                    _ => {}
                }
                let _ = self.downstream.call1(py, (item,));
                return Ok(());
            }
        }

        // 尝试 dict
        if let Ok(dict) = item.extract::<Py<PyDict>>(py) {
            let dict_b = dict.bind(py);

            // 获取 x, y
            let x: i32 = dict_b.get_item("x")?.map_or(Ok(0), |v| v.extract())?;
            let y: i32 = dict_b.get_item("y")?.map_or(Ok(0), |v| v.extract())?;

            // 获取 event_type，默认为 "move"
            let event_str: String = dict_b
                .get_item("event")?
                .map_or(Ok("move".to_string()), |v| v.extract())?;
            let event_str_lower = event_str.to_lowercase();

            match event_str_lower.as_str() {
                "move" => {
                    disp.call_method1("move_to", (x, y))?;
                }
                "click" | "left" | "left_click" => {
                    let button: String = dict_b
                        .get_item("button")?
                        .map_or(Ok("left".to_string()), |v| v.extract())?;
                    disp.call_method1("move_to", (x, y))?;
                    disp.call_method1("click", (&button,))?;
                }
                "right" | "right_click" => {
                    disp.call_method1("move_to", (x, y))?;
                    disp.call_method1("click", ("right",))?;
                }
                "middle" | "middle_click" => {
                    disp.call_method1("move_to", (x, y))?;
                    disp.call_method1("click", ("middle",))?;
                }
                "scroll" => {
                    let delta: i32 = dict_b
                        .get_item("delta")?
                        .map_or(Ok(120), |v| v.extract())?;
                    disp.call_method1("scroll", (delta,))?;
                }
                "drag" => {
                    // drag 需要 from_x, from_y, to_x, to_to
                    let from_x: i32 = dict_b
                        .get_item("from_x")?
                        .map_or(Ok(x), |v| v.extract())?;
                    let from_y: i32 = dict_b
                        .get_item("from_y")?
                        .map_or(Ok(y), |v| v.extract())?;
                    let to_x: i32 = dict_b.get_item("to_x")?.map_or(Ok(x), |v| v.extract())?;
                    let to_y: i32 = dict_b.get_item("to_y")?.map_or(Ok(y), |v| v.extract())?;
                    disp.call_method1("drag", (from_x, from_y, to_x, to_y))?;
                }
                _ => {}
            }
            let _ = self.downstream.call1(py, (item,));
            return Ok(());
        }

        // 无法解析，直接透传
        let _ = self.downstream.call1(py, (item,));
        Ok(())
    }
}

// =====================================================================
// 函数式 API
// =====================================================================

/// 创建键盘写入操作符
#[pyfunction]
pub fn write_to_keyboard(dispatcher: PyObject, py: Python<'_>) -> PyResult<PyObject> {
    // 支持接受 dispatcher 参数为 PyObject
    if let Ok(d) = dispatcher.extract::<Py<KeyboardDispatcher>>(py) {
        let op = Py::new(
            py,
            WriteKeyboardOp {
                dispatcher: d.clone_ref(py),
            },
        )?;
        return Ok(op.into_any());
    }
    // 尝试从属性获取
    if let Ok(d) = dispatcher.getattr(py, "dispatcher") {
        if let Ok(disp) = d.extract::<Py<KeyboardDispatcher>>(py) {
            let op = Py::new(
                py,
                WriteKeyboardOp {
                    dispatcher: disp.clone_ref(py),
                },
            )?;
            return Ok(op.into_any());
        }
    }
    Err(pyo3::exceptions::PyTypeError::new_err(
        "write_to_keyboard 第一个参数必须是 KeyboardDispatcher 或 KeySubject",
    ))
}

/// 创建鼠标写入操作符
#[pyfunction]
pub fn write_to_mouse(dispatcher: PyObject, py: Python<'_>) -> PyResult<PyObject> {
    if let Ok(d) = dispatcher.extract::<Py<MouseDispatcher>>(py) {
        let op = Py::new(
            py,
            WriteMouseOp {
                dispatcher: d.clone_ref(py),
            },
        )?;
        return Ok(op.into_any());
    }
    if let Ok(d) = dispatcher.getattr(py, "dispatcher") {
        if let Ok(disp) = d.extract::<Py<MouseDispatcher>>(py) {
            let op = Py::new(
                py,
                WriteMouseOp {
                    dispatcher: disp.clone_ref(py),
                },
            )?;
            return Ok(op.into_any());
        }
    }
    Err(pyo3::exceptions::PyTypeError::new_err(
        "write_to_mouse 第一个参数必须是 MouseDispatcher 或 MouseSubject",
    ))
}

// =====================================================================
// 模块注册辅助
// =====================================================================

pub fn register_keyboard_mouse_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // ===== 类型与枚举 =====
    m.add_class::<KeyEventType>()?;
    m.add_class::<MouseEventType>()?;
    m.add_class::<KeyModifier>()?;
    m.add_class::<KeyData>()?;
    m.add_class::<MouseData>()?;

    // ===== 分发器与主题 =====
    m.add_class::<KeyboardDispatcher>()?;
    m.add_class::<MouseDispatcher>()?;
    m.add_class::<KeySubject>()?;
    m.add_class::<MouseSubject>()?;

    // ===== 观察者 =====
    m.add_class::<KeyObserver>()?;
    m.add_class::<MouseObserver>()?;

    // ===== 写入操作符 =====
    m.add_class::<WriteKeyboardOp>()?;
    m.add_class::<WriteKeyboardObs>()?;
    m.add_class::<WriteKeyboardHandler>()?;
    m.add_class::<WriteMouseOp>()?;
    m.add_class::<WriteMouseObs>()?;
    m.add_class::<WriteMouseHandler>()?;

    // ===== 顶层函数 =====
    m.add_function(wrap_pyfunction!(from_keyboard, m)?)?;
    m.add_function(wrap_pyfunction!(from_mouse, m)?)?;
    m.add_function(wrap_pyfunction!(write_to_keyboard, m)?)?;
    m.add_function(wrap_pyfunction!(write_to_mouse, m)?)?;

    Ok(())
}
