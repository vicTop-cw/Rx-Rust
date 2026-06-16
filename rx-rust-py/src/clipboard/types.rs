// 数据类型：ClipChangeType（对应 Python 的 ChangeType）和 ClipData

use std::sync::atomic::{AtomicU64, Ordering};

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};
use serde::{Deserialize, Serialize};

// 全局单调递增 sequence，为 ClipData 生成 sequence 值
static SEQUENCE_COUNTER: AtomicU64 = AtomicU64::new(1);

fn next_sequence() -> u64 {
    SEQUENCE_COUNTER.fetch_add(1, Ordering::SeqCst)
}

// =====================================================================
// ClipChangeType (ChangeType)
// =====================================================================

#[pyclass(name = "ChangeType")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ClipChangeType {
    #[pyo3(get)]
    pub value: u8,
}

impl ClipChangeType {
    pub const TEXT: Self = ClipChangeType { value: 0 };
    pub const FILES: Self = ClipChangeType { value: 1 };
    pub const IMAGE: Self = ClipChangeType { value: 2 };
    pub const HTML: Self = ClipChangeType { value: 3 };
    pub const RTF: Self = ClipChangeType { value: 4 };
    pub const CLEAR: Self = ClipChangeType { value: 5 };
    pub const OTHER: Self = ClipChangeType { value: 6 };
}

#[pymethods]
impl ClipChangeType {
    // --- 工厂
    #[staticmethod]
    fn from_value(v: i64) -> PyResult<Self> {
        if !(0..=6).contains(&v) {
            Ok(ClipChangeType { value: 6 })
        } else {
            Ok(ClipChangeType { value: v as u8 })
        }
    }

    #[staticmethod]
    fn from_name(name: &str) -> PyResult<Self> {
        Ok(match name.to_uppercase().as_str() {
            "TEXT" => ClipChangeType::TEXT,
            "FILES" => ClipChangeType::FILES,
            "IMAGE" => ClipChangeType::IMAGE,
            "HTML" => ClipChangeType::HTML,
            "RTF" => ClipChangeType::RTF,
            "CLEAR" => ClipChangeType::CLEAR,
            _ => ClipChangeType::OTHER,
        })
    }

    // --- Python 协议
    fn __int__(&self) -> i64 {
        self.value as i64
    }

    fn __str__(&self) -> String {
        self.name()
    }

    fn __repr__(&self) -> String {
        format!("ChangeType.{}(value={})", self.name(), self.value)
    }

    fn __hash__(&self) -> isize {
        self.value as isize
    }

    fn __eq__(&self, other: &Bound<'_, PyAny>) -> PyResult<bool> {
        // 支持与 int / ChangeType 比较
        if let Ok(other_int) = other.extract::<i64>() {
            return Ok(other_int == self.value as i64);
        }
        if let Ok(other_ct) = other.extract::<Py<ClipChangeType>>() {
            Python::with_gil(|py| {
                Ok(other_ct.borrow(py).value == self.value)
            })
        } else {
            Ok(false)
        }
    }

    #[getter]
    fn name(&self) -> String {
        match self.value {
            0 => "TEXT",
            1 => "FILES",
            2 => "IMAGE",
            3 => "HTML",
            4 => "RTF",
            5 => "CLEAR",
            _ => "OTHER",
        }
        .to_string()
    }
}

// =====================================================================
// ClipContent 内部枚举：区分 None / 文本 / 二进制
// =====================================================================

#[derive(Debug, Clone)]
pub enum ClipContent {
    None,
    Text(String),
    Bytes(Vec<u8>),
}

// =====================================================================
// ClipData
// =====================================================================

#[pyclass(name = "ClipData")]
#[derive(Debug)]
pub struct ClipData {
    pub content: ClipContent,
    #[pyo3(get)]
    pub files: Vec<String>,
    #[pyo3(get)]
    pub change_type: Py<ClipChangeType>,
    #[pyo3(get)]
    pub tags: Vec<String>,
    pub metadata: Py<PyDict>,
    #[pyo3(get)]
    pub timestamp: f64,
    #[pyo3(get)]
    pub sequence: u64,
}

#[pymethods]
impl ClipData {
    #[new]
    #[pyo3(signature = (content=None, files=None, change_type=None, tags=None, metadata=None, timestamp=None, sequence=None))]
    fn new(
        content: Option<PyObject>,
        files: Option<Vec<String>>,
        change_type: Option<Py<ClipChangeType>>,
        tags: Option<Vec<String>>,
        metadata: Option<Py<PyDict>>,
        timestamp: Option<f64>,
        sequence: Option<u64>,
    ) -> PyResult<Self> {
        Python::with_gil(|py| {
            let content = match content {
                Some(c) => {
                    if let Ok(b) = c.extract::<Vec<u8>>(py) {
                        ClipContent::Bytes(b)
                    } else if let Ok(s) = c.extract::<String>(py) {
                        ClipContent::Text(s)
                    } else if c.is_none(py) {
                        ClipContent::None
                    } else {
                        ClipContent::None
                    }
                }
                None => ClipContent::None,
            };

            let files = files.unwrap_or_default();
            let ct = match change_type {
                Some(c) => c,
                None => Py::new(py, ClipChangeType::TEXT)?,
            };
            let tags = tags.unwrap_or_default();
            let metadata = match metadata {
                Some(m) => m,
                None => PyDict::new_bound(py).unbind(),
            };
            let ts = timestamp.unwrap_or_else(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs_f64())
                    .unwrap_or(0.0)
            });
            let seq = sequence.unwrap_or_else(next_sequence);

            Ok(ClipData {
                content,
                files,
                change_type: ct,
                tags,
                metadata,
                timestamp: ts,
                sequence: seq,
            })
        })
    }

    #[staticmethod]
    #[pyo3(signature = (content=None, files=None, change_type=None, tags=None, metadata=None))]
    fn now(
        content: Option<PyObject>,
        files: Option<Vec<String>>,
        change_type: Option<Py<ClipChangeType>>,
        tags: Option<Vec<String>>,
        metadata: Option<Py<PyDict>>,
    ) -> PyResult<Self> {
        Self::new(
            content,
            files,
            change_type,
            tags,
            metadata,
            None,
            None,
        )
    }

    // --- content 属性（支持 str / bytes / None）
    #[getter]
    fn get_content(&self, py: Python<'_>) -> PyObject {
        match &self.content {
            ClipContent::None => py.None(),
            ClipContent::Text(s) => s.to_object(py),
            ClipContent::Bytes(b) => b.to_object(py),
        }
    }

    #[setter]
    fn set_content(&mut self, py: Python<'_>, value: PyObject) -> PyResult<()> {
        if value.is_none(py) {
            self.content = ClipContent::None;
        } else if let Ok(b) = value.extract::<Vec<u8>>(py) {
            self.content = ClipContent::Bytes(b);
        } else if let Ok(s) = value.extract::<String>(py) {
            self.content = ClipContent::Text(s);
        } else {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "content must be str / bytes / None",
            ));
        }
        Ok(())
    }

    // --- metadata getter
    #[getter]
    fn get_metadata(&self, py: Python<'_>) -> Py<PyDict> {
        self.metadata.clone_ref(py)
    }

    // --- 序列化/反序列化

    fn to_dict(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let d = PyDict::new_bound(py);

        match &self.content {
            ClipContent::None => {
                d.set_item("content", py.None())?;
            }
            ClipContent::Text(s) => {
                d.set_item("content", s)?;
            }
            ClipContent::Bytes(b) => {
                let encoded = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, b);
                d.set_item("content", encoded)?;
                d.set_item("_encoding", "base64")?;
            }
        }

        d.set_item("files", self.files.clone())?;
        let ct_val: i64 = self.change_type.borrow(py).value as i64;
        d.set_item("change_type", ct_val)?;
        d.set_item("change_type_name", self.change_type.borrow(py).name())?;
        d.set_item("tags", self.tags.clone())?;
        d.set_item("metadata", self.metadata.clone_ref(py))?;
        d.set_item("timestamp", self.timestamp)?;
        d.set_item("sequence", self.sequence)?;
        Ok(d.unbind())
    }

    #[staticmethod]
    fn from_dict(py: Python<'_>, data: &Bound<'_, PyDict>) -> PyResult<Self> {
        let encoding: Option<String> = data.get_item("_encoding")?.and_then(|v| v.extract().ok());

        let content_obj = data.get_item("content")?;
        let content = if let Some(enc) = encoding.as_deref() {
            if enc == "base64" {
                if let Some(s) = content_obj.as_ref().and_then(|v| v.extract::<String>().ok()) {
                    let decoded = base64::Engine::decode(
                        &base64::engine::general_purpose::STANDARD,
                        s.as_bytes(),
                    )
                    .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
                    Some(Python::with_gil(|py| decoded.to_object(py)))
                } else {
                    None
                }
            } else {
                content_obj.as_ref().map(|v| v.clone().unbind())
            }
        } else {
            content_obj.as_ref().map(|v| v.clone().unbind())
        };

        let files: Vec<String> = data
            .get_item("files")?
            .and_then(|v| v.extract().ok())
            .unwrap_or_default();

        let ct_val: i64 = data
            .get_item("change_type")?
            .and_then(|v| v.extract().ok())
            .unwrap_or(0);
        let ct = Py::new(py, ClipChangeType { value: ct_val as u8 })?;

        let tags: Vec<String> = data
            .get_item("tags")?
            .and_then(|v| v.extract().ok())
            .unwrap_or_default();

        let metadata: Py<PyDict> = data
            .get_item("metadata")?
            .and_then(|v| {
                if let Ok(d) = v.extract::<Py<PyDict>>() {
                    Some(d)
                } else {
                    None
                }
            })
            .unwrap_or_else(|| PyDict::new_bound(py).unbind());

        let timestamp: f64 = data
            .get_item("timestamp")?
            .and_then(|v| v.extract().ok())
            .unwrap_or(0.0);

        let sequence: u64 = data
            .get_item("sequence")?
            .and_then(|v| v.extract().ok())
            .unwrap_or_else(next_sequence);

        Self::new(
            content,
            Some(files),
            Some(ct),
            Some(tags),
            Some(metadata),
            Some(timestamp),
            Some(sequence),
        )
    }

    fn to_json(&self, py: Python<'_>) -> PyResult<String> {
        let d = self.to_dict(py)?;
        let json_module = py.import_bound("json")?;
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

    fn __repr__(&self, py: Python<'_>) -> String {
        let ct = self.change_type.borrow(py).name();
        let body = match &self.content {
            ClipContent::None => "None".to_string(),
            ClipContent::Text(s) => {
                if s.len() > 80 {
                    format!("\"{}...\"", &s[..80])
                } else {
                    format!("\"{}\"", s)
                }
            }
            ClipContent::Bytes(b) => format!("<bytes {}>", b.len()),
        };
        format!(
            "ClipData(type={}, content={}, files={:?}, tags={:?}, seq={})",
            ct, body, self.files, self.tags, self.sequence
        )
    }
}

// Helper：从 PyObject 获取一个 ClipContent 作为元组元素的使用
pub fn pyobj_to_clipcontent(py: Python<'_>, obj: &PyObject) -> ClipContent {
    if obj.is_none(py) {
        ClipContent::None
    } else if let Ok(b) = obj.extract::<Vec<u8>>(py) {
        ClipContent::Bytes(b)
    } else if let Ok(s) = obj.extract::<String>(py) {
        ClipContent::Text(s)
    } else {
        ClipContent::None
    }
}

// 计算内容签名（用于去重和自过滤）
pub fn compute_signature(
    change_type_value: u8,
    content: &ClipContent,
    files: &[String],
) -> (i64, String, i64, Vec<String>) {
    let hash_str = match content {
        ClipContent::None => String::new(),
        ClipContent::Text(s) => {
            let digest = md5::compute(s.as_bytes());
            format!("{:x}", digest)
        }
        ClipContent::Bytes(b) => {
            let digest = md5::compute(b);
            format!("{:x}", digest)
        }
    };
    let size = match content {
        ClipContent::None => 0,
        ClipContent::Text(s) => s.len() as i64,
        ClipContent::Bytes(b) => b.len() as i64,
    };
    (
        change_type_value as i64,
        hash_str,
        size,
        files.to_vec(),
    )
}
