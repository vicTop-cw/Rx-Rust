// 数据类型：KeyEventType、MouseEventType、KeyModifier、KeyData、MouseData

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};

// 全局单调递增 sequence，为 KeyData/MouseData 生成 sequence 值
static SEQUENCE_COUNTER: AtomicU64 = AtomicU64::new(1);

fn next_sequence() -> u64 {
    SEQUENCE_COUNTER.fetch_add(1, Ordering::SeqCst)
}

// ============================================================================
// KeyEventType - 键盘事件类型
// ============================================================================

#[pyclass(name = "KeyEventType")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyEventType(pub u8);

impl KeyEventType {
    pub const KEY_DOWN: u8 = 0;
    pub const KEY_UP: u8 = 1;
    pub const KEY_HOLD: u8 = 2;

    pub fn to_name(&self) -> &'static str {
        match self.0 {
            0 => "KEY_DOWN",
            1 => "KEY_UP",
            2 => "KEY_HOLD",
            _ => "UNKNOWN",
        }
    }
}

#[pymethods]
impl KeyEventType {
    #[classattr]
    const KEY_DOWN: Self = KeyEventType(0);
    #[classattr]
    const KEY_UP: Self = KeyEventType(1);
    #[classattr]
    const KEY_HOLD: Self = KeyEventType(2);

    #[new]
    fn new(value: u8) -> Self {
        Self(value)
    }

    fn __int__(&self) -> u8 {
        self.0
    }

    fn __str__(&self) -> String {
        self.to_name().to_string()
    }

    fn __repr__(&self) -> String {
        format!("KeyEventType.{}({})", self.to_name(), self.0)
    }

    fn __eq__(&self, other: PyObject, py: Python<'_>) -> PyObject {
        if let Ok(val) = other.extract::<u8>(py) {
            return (self.0 == val)
                .into_pyobject(py)
                .unwrap()
                .unbind()
                .into_any();
        }
        if let Ok(kt) = other.extract::<PyRef<KeyEventType>>(py) {
            return (self.0 == kt.0)
                .into_pyobject(py)
                .unwrap()
                .unbind()
                .into_any();
        }
        false.into_pyobject(py).unwrap().unbind().into_any()
    }
}

// ============================================================================
// MouseEventType - 鼠标事件类型
// ============================================================================

#[pyclass(name = "MouseEventType")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MouseEventType(pub u8);

impl MouseEventType {
    pub const MOVE: u8 = 0;
    pub const LEFT_DOWN: u8 = 1;
    pub const LEFT_UP: u8 = 2;
    pub const RIGHT_DOWN: u8 = 3;
    pub const RIGHT_UP: u8 = 4;
    pub const MIDDLE_DOWN: u8 = 5;
    pub const MIDDLE_UP: u8 = 6;
    pub const SCROLL: u8 = 7;
    pub const DRAG: u8 = 8;

    pub fn to_name(&self) -> &'static str {
        match self.0 {
            0 => "MOVE",
            1 => "LEFT_DOWN",
            2 => "LEFT_UP",
            3 => "RIGHT_DOWN",
            4 => "RIGHT_UP",
            5 => "MIDDLE_DOWN",
            6 => "MIDDLE_UP",
            7 => "SCROLL",
            8 => "DRAG",
            _ => "UNKNOWN",
        }
    }
}

#[pymethods]
impl MouseEventType {
    #[classattr]
    const MOVE: Self = MouseEventType(0);
    #[classattr]
    const LEFT_DOWN: Self = MouseEventType(1);
    #[classattr]
    const LEFT_UP: Self = MouseEventType(2);
    #[classattr]
    const RIGHT_DOWN: Self = MouseEventType(3);
    #[classattr]
    const RIGHT_UP: Self = MouseEventType(4);
    #[classattr]
    const MIDDLE_DOWN: Self = MouseEventType(5);
    #[classattr]
    const MIDDLE_UP: Self = MouseEventType(6);
    #[classattr]
    const SCROLL: Self = MouseEventType(7);
    #[classattr]
    const DRAG: Self = MouseEventType(8);

    #[new]
    fn new(value: u8) -> Self {
        Self(value)
    }

    fn __int__(&self) -> u8 {
        self.0
    }

    fn __str__(&self) -> String {
        self.to_name().to_string()
    }

    fn __repr__(&self) -> String {
        format!("MouseEventType.{}({})", self.to_name(), self.0)
    }

    fn __eq__(&self, other: PyObject, py: Python<'_>) -> PyObject {
        if let Ok(val) = other.extract::<u8>(py) {
            return (self.0 == val)
                .into_pyobject(py)
                .unwrap()
                .unbind()
                .into_any();
        }
        if let Ok(mt) = other.extract::<PyRef<MouseEventType>>(py) {
            return (self.0 == mt.0)
                .into_pyobject(py)
                .unwrap()
                .unbind()
                .into_any();
        }
        false.into_pyobject(py).unwrap().unbind().into_any()
    }
}

// ============================================================================
// KeyModifier - 键盘修饰符位标志
// ============================================================================

#[pyclass(name = "KeyModifier")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyModifier(pub i32);

impl KeyModifier {
    pub const NONE: i32 = 0;
    pub const SHIFT: i32 = 1;
    pub const CTRL: i32 = 2;
    pub const ALT: i32 = 4;
    pub const WIN: i32 = 8;
    pub const CAPSLOCK: i32 = 16;
    pub const LSHIFT: i32 = 32;
    pub const RSHIFT: i32 = 64;
    pub const LCTRL: i32 = 128;
    pub const RCTRL: i32 = 256;

    pub fn to_name(&self) -> &'static str {
        match self.0 {
            0 => "NONE",
            1 => "SHIFT",
            2 => "CTRL",
            4 => "ALT",
            8 => "WIN",
            16 => "CAPSLOCK",
            32 => "LSHIFT",
            64 => "RSHIFT",
            128 => "LCTRL",
            256 => "RCTRL",
            _ => "UNKNOWN",
        }
    }
}

#[pymethods]
impl KeyModifier {
    #[classattr]
    const NONE: Self = KeyModifier(0);
    #[classattr]
    const SHIFT: Self = KeyModifier(1);
    #[classattr]
    const CTRL: Self = KeyModifier(2);
    #[classattr]
    const ALT: Self = KeyModifier(4);
    #[classattr]
    const WIN: Self = KeyModifier(8);
    #[classattr]
    const CAPSLOCK: Self = KeyModifier(16);
    #[classattr]
    const LSHIFT: Self = KeyModifier(32);
    #[classattr]
    const RSHIFT: Self = KeyModifier(64);
    #[classattr]
    const LCTRL: Self = KeyModifier(128);
    #[classattr]
    const RCTRL: Self = KeyModifier(256);

    #[new]
    fn new(value: i32) -> Self {
        Self(value)
    }

    fn __int__(&self) -> i32 {
        self.0
    }

    fn __str__(&self) -> String {
        self.to_name().to_string()
    }

    fn __repr__(&self) -> String {
        format!("KeyModifier.{}({})", self.to_name(), self.0)
    }

    fn __eq__(&self, other: PyObject, py: Python<'_>) -> PyObject {
        if let Ok(val) = other.extract::<i32>(py) {
            return (self.0 == val)
                .into_pyobject(py)
                .unwrap()
                .unbind()
                .into_any();
        }
        if let Ok(km) = other.extract::<PyRef<KeyModifier>>(py) {
            return (self.0 == km.0)
                .into_pyobject(py)
                .unwrap()
                .unbind()
                .into_any();
        }
        false.into_pyobject(py).unwrap().unbind().into_any()
    }

    // 位运算：或
    fn __or__(&self, other: PyObject, py: Python<'_>) -> PyResult<PyObject> {
        let other_val = if let Ok(km) = other.extract::<PyRef<KeyModifier>>(py) {
            km.0
        } else if let Ok(val) = other.extract::<i32>(py) {
            val
        } else {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "KeyModifier.__or__ requires KeyModifier or int",
            ));
        };
        Ok(KeyModifier(self.0 | other_val)
            .into_pyobject(py)
            .unwrap()
            .unbind())
    }

    // 位运算：与
    fn __and__(&self, other: PyObject, py: Python<'_>) -> PyResult<PyObject> {
        let other_val = if let Ok(km) = other.extract::<PyRef<KeyModifier>>(py) {
            km.0
        } else if let Ok(val) = other.extract::<i32>(py) {
            val
        } else {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "KeyModifier.__and__ requires KeyModifier or int",
            ));
        };
        Ok(KeyModifier(self.0 & other_val)
            .into_pyobject(py)
            .unwrap()
            .unbind())
    }
}

// ============================================================================
// KeyData - 键盘事件数据
// ============================================================================

#[pyclass(name = "KeyData")]
#[derive(Debug, Clone)]
pub struct KeyData {
    #[pyo3(get)]
    pub key_code: u32,
    #[pyo3(get)]
    pub key_name: String,
    #[pyo3(get)]
    pub is_press: bool,
    #[pyo3(get)]
    pub event_type: KeyEventType, // 新增：自动从 is_press 推导
    #[pyo3(get)]
    pub modifiers: i32,
    #[pyo3(get)]
    pub timestamp: i64, // 改为 i64 毫秒（原来是 f64 秒）
    #[pyo3(get)]
    pub sequence: u64,
    #[pyo3(get)]
    pub window_title: Option<String>,
}

#[pymethods]
impl KeyData {
    #[new]
    #[pyo3(signature = (key_code, key_name="", is_press=true, event_type=None, modifiers=0, timestamp=None, sequence=None, window_title=None))]
    fn new(
        key_code: u32,
        key_name: String,
        is_press: bool,
        event_type: Option<KeyEventType>,
        modifiers: i32,
        timestamp: Option<i64>,
        sequence: Option<u64>,
        window_title: Option<String>,
    ) -> Self {
        let key_name = if key_name.is_empty() {
            key_code_to_name(key_code)
        } else {
            key_name
        };
        let ts = timestamp.unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64
        });
        let sequence = sequence.unwrap_or_else(next_sequence);
        // 自动推导 event_type
        let event_type = event_type.unwrap_or_else(|| {
            if is_press {
                KeyEventType(0)
            } else {
                KeyEventType(1)
            }
        });
        Self {
            key_code,
            key_name,
            is_press,
            event_type,
            modifiers,
            timestamp: ts,
            sequence,
            window_title,
        }
    }

    #[staticmethod]
    #[pyo3(signature = (key_code, is_press, modifiers=None, window_title=None))]
    fn now(
        key_code: u32,
        is_press: bool,
        modifiers: Option<i32>,
        window_title: Option<String>,
    ) -> Self {
        let key_name = key_code_to_name(key_code);
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let sequence = next_sequence();
        // 自动推导 event_type
        let event_type = if is_press {
            KeyEventType(0)
        } else {
            KeyEventType(1)
        };
        Self {
            key_code,
            key_name,
            is_press,
            event_type,
            modifiers: modifiers.unwrap_or(0),
            timestamp: ts,
            sequence,
            window_title,
        }
    }

    fn to_dict(&self, py: Python<'_>) -> Py<PyDict> {
        let d = PyDict::new_bound(py);
        let _ = d.set_item("key_code", self.key_code);
        let _ = d.set_item("key_name", &self.key_name);
        let _ = d.set_item("is_press", self.is_press);
        let _ = d.set_item("event_type", self.event_type.0); // 新增
        let _ = d.set_item("event_type_name", self.event_type.to_name()); // 新增
        let _ = d.set_item("modifiers", self.modifiers);
        let _ = d.set_item("timestamp", self.timestamp);
        let _ = d.set_item("sequence", self.sequence);
        let _ = d.set_item("window_title", &self.window_title);
        d.unbind()
    }

    #[staticmethod]
    fn from_dict(py: Python<'_>, data: &Bound<'_, PyDict>) -> PyResult<Self> {
        let key_code: u32 = data.get_item("key_code")?.map_or(Ok(0), |v| v.extract())?;
        let key_name: String = data
            .get_item("key_name")?
            .map_or(Ok(String::new()), |v| v.extract())?;
        let is_press: bool = data
            .get_item("is_press")?
            .map_or(Ok(false), |v| v.extract())?;
        // 解析 event_type（可选，默认从 is_press 推导）
        let event_type: Option<KeyEventType> =
            data.get_item("event_type")?.map_or(Ok(None), |v| {
                v.extract::<u8>().map(|et| Some(KeyEventType(et)))
            })?;
        let modifiers: i32 = data.get_item("modifiers")?.map_or(Ok(0), |v| v.extract())?;
        let timestamp: i64 = data.get_item("timestamp")?.map_or_else(
            || {
                Ok(SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64)
            },
            |v| v.extract(),
        )?;
        let sequence: u64 = data
            .get_item("sequence")?
            .map_or_else(|| Ok(next_sequence()), |v| v.extract())?;
        let window_title: Option<String> = data
            .get_item("window_title")?
            .map_or(Ok(None), |v| v.extract())?;

        // 自动推导 event_type
        let event_type = event_type.unwrap_or_else(|| {
            if is_press {
                KeyEventType(0)
            } else {
                KeyEventType(1)
            }
        });

        Ok(Self {
            key_code,
            key_name,
            is_press,
            event_type,
            modifiers,
            timestamp,
            sequence,
            window_title,
        })
    }

    fn to_json(&self, py: Python<'_>) -> PyResult<String> {
        let json_module = py.import_bound("json")?;
        let d = self.to_dict(py);
        let result = json_module.call_method1("dumps", (d,))?;
        result.extract::<String>()
    }

    #[staticmethod]
    fn from_json(py: Python<'_>, s: &str) -> PyResult<Self> {
        let json_module = py.import_bound("json")?;
        let data = json_module.call_method1("loads", (s,))?;
        let dict = data.downcast_into::<PyDict>()?;
        Self::from_dict(py, &dict)
    }

    fn __repr__(&self) -> String {
        format!(
            "KeyData(key_code={}, key_name={:?}, is_press={}, event_type={}, modifiers={}, timestamp={}, seq={})",
            self.key_code, self.key_name, self.is_press, self.event_type.0, self.modifiers, self.timestamp, self.sequence
        )
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }

    /// 使用 Python pickle 模块序列化为字节
    fn to_pickle(&self, py: Python<'_>) -> PyResult<Py<PyBytes>> {
        let pickle = py.import_bound("pickle")?;
        let dict = self.to_dict(py);
        let result = pickle.call_method1("dumps", (dict,))?;
        result.extract::<Py<PyBytes>>()
    }

    /// 从 pickle 字节反序列化
    #[staticmethod]
    #[pyo3(signature = (data, trusted=false))]
    fn from_pickle(py: Python<'_>, data: Py<PyBytes>, trusted: bool) -> PyResult<Self> {
        if !trusted {
            eprintln!("[WARN] KeyData.from_pickle: loading untrusted pickle data");
        }
        let pickle = py.import_bound("pickle")?;
        let dict_obj = pickle.call_method1("loads", (data,))?;
        let dict = dict_obj.downcast_into::<PyDict>()?;
        Self::from_dict(py, &dict)
    }
}

// ============================================================================
// MouseData - 鼠标事件数据
// ============================================================================

#[pyclass(name = "MouseData")]
#[derive(Debug, Clone)]
pub struct MouseData {
    #[pyo3(get)]
    pub x: i32,
    #[pyo3(get)]
    pub y: i32,
    #[pyo3(get)]
    pub event_type: u8,
    #[pyo3(get)]
    pub button: String,
    #[pyo3(get)]
    pub delta: i32,
    #[pyo3(get)]
    pub timestamp: i64, // 改为 i64 毫秒（原来是 f64 秒）
    #[pyo3(get)]
    pub sequence: u64,
}

#[pymethods]
impl MouseData {
    #[new]
    #[pyo3(signature = (x, y, event_type, button="left", delta=0, timestamp=None, sequence=None))]
    fn new(
        x: i32,
        y: i32,
        event_type: u8,
        button: String,
        delta: i32,
        timestamp: Option<i64>,
        sequence: Option<u64>,
    ) -> Self {
        let ts = timestamp.unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64
        });
        Self {
            x,
            y,
            event_type,
            button,
            delta,
            timestamp: ts,
            sequence: sequence.unwrap_or_else(next_sequence),
        }
    }

    #[staticmethod]
    #[pyo3(signature = (x, y, event_type, button="left", delta=0))]
    fn now(x: i32, y: i32, event_type: u8, button: String, delta: i32) -> Self {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        Self {
            x,
            y,
            event_type,
            button,
            delta,
            timestamp: ts,
            sequence: next_sequence(),
        }
    }

    fn to_dict(&self, py: Python<'_>) -> Py<PyDict> {
        let d = PyDict::new_bound(py);
        let _ = d.set_item("x", self.x);
        let _ = d.set_item("y", self.y);
        let _ = d.set_item("event_type", self.event_type);
        let _ = d.set_item("button", &self.button);
        let _ = d.set_item("delta", self.delta);
        let _ = d.set_item("timestamp", self.timestamp);
        let _ = d.set_item("sequence", self.sequence);
        d.unbind()
    }

    #[staticmethod]
    fn from_dict(py: Python<'_>, data: &Bound<'_, PyDict>) -> PyResult<Self> {
        let x: i32 = data.get_item("x")?.map_or(Ok(0), |v| v.extract())?;
        let y: i32 = data.get_item("y")?.map_or(Ok(0), |v| v.extract())?;
        let event_type: u8 = data
            .get_item("event_type")?
            .map_or(Ok(0), |v| v.extract())?;
        let button: String = data
            .get_item("button")?
            .map_or(Ok(String::from("left")), |v| v.extract())?;
        let delta: i32 = data.get_item("delta")?.map_or(Ok(0), |v| v.extract())?;
        let timestamp: i64 = data.get_item("timestamp")?.map_or_else(
            || {
                Ok(SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64)
            },
            |v| v.extract(),
        )?;
        let sequence: u64 = data
            .get_item("sequence")?
            .map_or_else(|| Ok(next_sequence()), |v| v.extract())?;

        Ok(Self {
            x,
            y,
            event_type,
            button,
            delta,
            timestamp,
            sequence,
        })
    }

    fn to_json(&self, py: Python<'_>) -> PyResult<String> {
        let json_module = py.import_bound("json")?;
        let d = self.to_dict(py);
        let result = json_module.call_method1("dumps", (d,))?;
        result.extract::<String>()
    }

    #[staticmethod]
    fn from_json(py: Python<'_>, s: &str) -> PyResult<Self> {
        let json_module = py.import_bound("json")?;
        let data = json_module.call_method1("loads", (s,))?;
        let dict = data.downcast_into::<PyDict>()?;
        Self::from_dict(py, &dict)
    }

    fn __repr__(&self) -> String {
        format!(
            "MouseData(x={}, y={}, event_type={}, button={:?}, delta={}, timestamp={}, seq={})",
            self.x, self.y, self.event_type, self.button, self.delta, self.timestamp, self.sequence
        )
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }

    /// 使用 Python pickle 模块序列化为字节
    fn to_pickle(&self, py: Python<'_>) -> PyResult<Py<PyBytes>> {
        let pickle = py.import_bound("pickle")?;
        let dict = self.to_dict(py);
        let result = pickle.call_method1("dumps", (dict,))?;
        result.extract::<Py<PyBytes>>()
    }

    /// 从 pickle 字节反序列化
    #[staticmethod]
    #[pyo3(signature = (data, trusted=false))]
    fn from_pickle(py: Python<'_>, data: Py<PyBytes>, trusted: bool) -> PyResult<Self> {
        if !trusted {
            eprintln!("[WARN] MouseData.from_pickle: loading untrusted pickle data");
        }
        let pickle = py.import_bound("pickle")?;
        let dict_obj = pickle.call_method1("loads", (data,))?;
        let dict = dict_obj.downcast_into::<PyDict>()?;
        Self::from_dict(py, &dict)
    }
}

// ============================================================================
// 辅助函数：虚拟键码与名称转换
// ============================================================================

// Win32 虚拟键码表（部分常用键）
const VK_NAMES: &[(u32, &str)] = &[
    (0x08, "Backspace"),
    (0x09, "Tab"),
    (0x0C, "Clear"),
    (0x0D, "Enter"),
    (0x10, "Shift"),
    (0x11, "Ctrl"),
    (0x12, "Alt"),
    (0x13, "Pause"),
    (0x14, "CapsLock"),
    (0x15, "Kana"),
    (0x1B, "Escape"),
    (0x20, "Space"),
    (0x21, "PageUp"),
    (0x22, "PageDown"),
    (0x23, "End"),
    (0x24, "Home"),
    (0x25, "Left"),
    (0x26, "Up"),
    (0x27, "Right"),
    (0x28, "Down"),
    (0x29, "Select"),
    (0x2D, "Insert"),
    (0x2E, "Delete"),
    (0x30, "0"),
    (0x31, "1"),
    (0x32, "2"),
    (0x33, "3"),
    (0x34, "4"),
    (0x35, "5"),
    (0x36, "6"),
    (0x37, "7"),
    (0x38, "8"),
    (0x39, "9"),
    (0x41, "A"),
    (0x42, "B"),
    (0x43, "C"),
    (0x44, "D"),
    (0x45, "E"),
    (0x46, "F"),
    (0x47, "G"),
    (0x48, "H"),
    (0x49, "I"),
    (0x4A, "J"),
    (0x4B, "K"),
    (0x4C, "L"),
    (0x4D, "M"),
    (0x4E, "N"),
    (0x4F, "O"),
    (0x50, "P"),
    (0x51, "Q"),
    (0x52, "R"),
    (0x53, "S"),
    (0x54, "T"),
    (0x55, "U"),
    (0x56, "V"),
    (0x57, "W"),
    (0x58, "X"),
    (0x59, "Y"),
    (0x5A, "Z"),
    (0x60, "Num0"),
    (0x61, "Num1"),
    (0x62, "Num2"),
    (0x63, "Num3"),
    (0x64, "Num4"),
    (0x65, "Num5"),
    (0x66, "Num6"),
    (0x67, "Num7"),
    (0x68, "Num8"),
    (0x69, "Num9"),
    (0x6A, "NumMultiply"),
    (0x6B, "NumAdd"),
    (0x6C, "NumSeparator"),
    (0x6D, "NumSubtract"),
    (0x6E, "NumDecimal"),
    (0x6F, "NumDivide"),
    (0x70, "F1"),
    (0x71, "F2"),
    (0x72, "F3"),
    (0x73, "F4"),
    (0x74, "F5"),
    (0x75, "F6"),
    (0x76, "F7"),
    (0x77, "F8"),
    (0x78, "F9"),
    (0x79, "F10"),
    (0x7A, "F11"),
    (0x7B, "F12"),
    (0x7C, "F13"),
    (0x7D, "F14"),
    (0x7E, "F15"),
    (0x7F, "F16"),
    (0x80, "F17"),
    (0x81, "F18"),
    (0x82, "F19"),
    (0x83, "F20"),
    (0x84, "F21"),
    (0x85, "F22"),
    (0x86, "F23"),
    (0x87, "F24"),
    (0x90, "NumLock"),
    (0x91, "ScrollLock"),
    (0xA0, "LShift"),
    (0xA1, "RShift"),
    (0xA2, "LCtrl"),
    (0xA3, "RCtrl"),
    (0xA4, "LAlt"),
    (0xA5, "RAlt"),
    (0xA6, "BrowserBack"),
    (0xA7, "BrowserForward"),
    (0xA8, "BrowserRefresh"),
    (0xA9, "BrowserStop"),
    (0xAA, "BrowserSearch"),
    (0xAB, "BrowserFavorites"),
    (0xAC, "BrowserHome"),
    (0xAD, "VolumeMute"),
    (0xAE, "VolumeDown"),
    (0xAF, "VolumeUp"),
    (0xB0, "MediaPrevTrack"),
    (0xB1, "MediaNextTrack"),
    (0xB2, "MediaStop"),
    (0xB3, "MediaPlayPause"),
    (0xBA, "OemSemicolon"),
    (0xBB, "OemPlus"),
    (0xBC, "OemComma"),
    (0xBD, "OemMinus"),
    (0xBE, "OemPeriod"),
    (0xBF, "OemQuestion"),
    (0xC0, "OemTilde"),
    (0xDB, "OemLeftBracket"),
    (0xDC, "OemBackslash"),
    (0xDD, "OemRightBracket"),
    (0xDE, "OemQuote"),
];

/// 将虚拟键码转换为可读名称
pub fn key_code_to_name(code: u32) -> String {
    VK_NAMES
        .iter()
        .find(|(vk, _)| *vk == code)
        .map(|(_, name)| name.to_string())
        .unwrap_or_else(|| format!("VK_0x{:02X}", code))
}

/// 将名称反向转换为虚拟键码
pub fn name_to_key_code(name: &str) -> Option<u32> {
    // 先尝试精确匹配
    for (vk, n) in VK_NAMES {
        if n == name {
            return Some(vk);
        }
    }
    // 尝试大小写不敏感匹配
    let name_upper = name.to_uppercase();
    for (vk, n) in VK_NAMES {
        if n.to_uppercase() == name_upper {
            return Some(vk);
        }
    }
    // 尝试 VK_0xXX 格式
    if name.starts_with("VK_0x") || name.starts_with("vk_0x") {
        if let Ok(code) = u32::from_str_radix(&name[5..], 16) {
            return Some(code);
        }
    }
    None
}

// ============================================================================
// 导出
// ============================================================================

pub use KeyData;
pub use KeyEventType;
pub use KeyModifier;
pub use MouseData;
pub use MouseEventType;
