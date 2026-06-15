// Clipboard 相关的 Rust 实现模块。

pub mod types;       // ClipChangeType / ClipData 类型
pub mod io;          // 剪贴板 I/O 封装 (跨平台)
pub mod backends;     // Win32 Hook + Polling 后端
pub mod dispatcher;   // ClipboardDispatcher
pub mod observer;     // ClipObserver
pub mod subject;      // ClipSubject
pub mod toplevel;    // from_clipboard / write_to_clipboard

pub use types::{ClipChangeType, ClipData};
pub use dispatcher::ClipboardDispatcher;
pub use observer::ClipObserver;
pub use subject::ClipSubject;
