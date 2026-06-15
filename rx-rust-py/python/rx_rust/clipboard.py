"""
rx-rust.clipboard - 剪贴板监控与响应式分发

核心公共 API:
    ChangeType(IntEnum):          剪贴板内容类型枚举
    ClipData:                     结构化剪贴板数据（支持 JSON/Pickle 往返）
    ClipboardDispatcher:          监控与分发器（Windows Hook + 其它平台 polling）
    from_clipboard(...):          顶层工厂：返回 (Observable[ClipData], Dispatcher)
    write_to_clipboard(d, src):  响应式操作符：把流内容写回剪贴板

自我过滤机制 (self-filter):
    下游通过 Dispatcher.set_clipboard(...) 写回剪贴板时，
    Dispatcher 登记本次写入的内容签名。当系统再次通知剪贴板变化时，
    命中签名的内容会被丢弃，从而避免"下游写回又触发自己"的死循环。
"""

from __future__ import annotations

import base64
import ctypes
import hashlib
import itertools
import json
import logging
import os
import pickle
import sys
import threading
import time
import tkinter as tk
from collections import deque
from ctypes import wintypes as wt
from dataclasses import dataclass
from datetime import datetime
from enum import IntEnum
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    TypeVar,
)

from . import (
    Observable,
    _PyObservable,
    PublishSubject,
    Subscription,
)


log = logging.getLogger("rx_rust.clipboard")

T = TypeVar("T")
R = TypeVar("R")


# ====================================================================
# 数据类型：ChangeType / ClipData
# ====================================================================


class ChangeType(IntEnum):
    """剪贴板内容类型枚举。"""

    TEXT = 0       # 纯文本（str）
    FILES = 1      # 文件路径列表（拖放）
    IMAGE = 2      # 图片（bytes）
    HTML = 3       # HTML 片段
    RTF = 4        # 富文本
    CLEAR = 5      # 清空（无内容）
    OTHER = 6      # 其它 / 未知格式

    def __str__(self) -> str:
        return self.name


# ClipData 用的全局单调序号
_seq_counter = itertools.count(1)


@dataclass(slots=True)  # type: ignore[call-overload]
class ClipData:
    """结构化的剪贴板数据。

    字段:
        content:        文本或二进制内容；None 表示无主内容
        files:          文件路径列表（ChangeType.FILES 时填充）
        change_type:    内容类型（ChangeType）
        tags:           用户自定义标签
        metadata:       扩展元信息；保留键:
                            _source      写回来源标识
                            _owner_seq  写回序号（单调递增）
                            _encoding   为 "base64" 时表示 content 原为 bytes
                            error        读取异常
        timestamp:      创建/变更时间
        sequence:       全局序号（单调递增）
    """

    content: str | bytes | None
    files: List[str]
    change_type: ChangeType
    tags: List[str]
    metadata: Dict[str, Any]
    timestamp: datetime
    sequence: int

    # ---- 工厂 --------------------------------------------------------
    @classmethod
    def now(
        cls,
        content: str | bytes | None = None,
        files: Iterable[str] | None = None,
        change_type: ChangeType = ChangeType.TEXT,
        tags: Iterable[str] = (),
        metadata: Dict[str, Any] | None = None,
    ) -> "ClipData":
        return cls(
            content=content,
            files=list(files or []),
            change_type=change_type,
            tags=list(tags or ()),
            metadata=dict(metadata or {}),
            timestamp=datetime.now(),
            sequence=next(_seq_counter),
        )

    # ---- 序列化 ------------------------------------------------------
    def to_dict(self) -> Dict[str, Any]:
        content = self.content
        encoding = None
        if isinstance(content, (bytes, bytearray)):
            content = base64.b64encode(bytes(content)).decode("ascii")
            encoding = "base64"
        ts = self.timestamp
        if isinstance(ts, datetime):
            ts_str = ts.isoformat()
        elif isinstance(ts, (int, float)):
            ts_str = datetime.fromtimestamp(float(ts)).isoformat()
        else:
            ts_str = datetime.now().isoformat()
        data: Dict[str, Any] = {
            "content": content,
            "files": list(self.files),
            "change_type": int(self.change_type),
            "change_type_name": str(self.change_type),
            "tags": list(self.tags),
            "metadata": dict(self.metadata),
            "timestamp": ts_str,
            "sequence": self.sequence,
        }
        if encoding:
            data["_encoding"] = encoding
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ClipData":
        d = dict(data or {})
        encoding = d.pop("_encoding", None)
        ct_raw = d.pop("change_type", ChangeType.TEXT.value)
        try:
            ct = ChangeType(int(ct_raw))
        except (TypeError, ValueError):
            try:
                ct = ChangeType[str(ct_raw).upper()]
            except KeyError:
                ct = ChangeType.OTHER
        for k in ("change_type_name",):
            d.pop(k, None)

        content = d.get("content")
        if encoding == "base64" and isinstance(content, str):
            try:
                content = base64.b64decode(content)
            except Exception:
                content = None
        ts = d.get("timestamp")
        if isinstance(ts, str):
            try:
                ts = datetime.fromisoformat(ts)
            except ValueError:
                ts = datetime.now()
        elif not isinstance(ts, datetime):
            ts = datetime.now()

        return cls(
            content=content,
            files=list(d.get("files") or []),
            change_type=ct,
            tags=list(d.get("tags") or []),
            metadata=dict(d.get("metadata") or {}),
            timestamp=ts,
            sequence=int(d.get("sequence") or next(_seq_counter)),
        )

    def to_json(self, **kw: Any) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, **kw)

    @classmethod
    def from_json(cls, s: str, **kw: Any) -> "ClipData":
        return cls.from_dict(json.loads(s, **kw))

    def to_pickle(self, path: str) -> None:
        with open(path, "wb") as f:
            pickle.dump(self, f)

    @classmethod
    def from_pickle(cls, path: str) -> "ClipData":
        with open(path, "rb") as f:
            return pickle.load(f)

    # ---- 表示 --------------------------------------------------------
    def __str__(self) -> str:
        body = self.content
        if isinstance(body, (bytes, bytearray)):
            body = f"<bytes {len(body)}>"
        return (
            f"ClipData(type={self.change_type.name}, "
            f"content={body!r}, files={self.files}, tags={self.tags}, "
            f"seq={self.sequence})"
        )


# ====================================================================
# 剪贴板读写：ctypes (Windows) + tkinter (兜底)
# ====================================================================

# Clipboard 格式常量
_CF = {
    "TEXT": 1, "BITMAP": 2, "METAFILEPICT": 3, "SYLK": 4, "DIF": 5,
    "TIFF": 6, "OEMTEXT": 7, "DIB": 8, "PALETTE": 9, "PENDATA": 10,
    "RIFF": 11, "WAVE": 12, "UNICODETEXT": 13, "ENHMETAFILE": 14,
    "HDROP": 15, "LOCALE": 16, "DIBV5": 17, "MAX": 18,
}
_GMEM_MOVEABLE = 0x0002
_GMEM_ZEROINIT = 0x0040
_GHND = _GMEM_MOVEABLE | _GMEM_ZEROINIT

# -- Win32 ctypes 类型安全设置 --
# 注意: ctypes.windll.* 默认 restype 是 c_int; 在 64 位 Windows 上指针会被截断。
# 这里创建两个独立的 WinDLL 引用并显式设置关键函数的 argtypes/restype。
if sys.platform == "win32":
    _kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    _user32 = ctypes.WinDLL("user32", use_last_error=True)

    _kernel32.GlobalAlloc.argtypes = [ctypes.c_uint, ctypes.c_size_t]
    _kernel32.GlobalAlloc.restype = ctypes.c_void_p
    _kernel32.GlobalLock.argtypes = [ctypes.c_void_p]
    _kernel32.GlobalLock.restype = ctypes.c_void_p
    _kernel32.GlobalUnlock.argtypes = [ctypes.c_void_p]
    _kernel32.GlobalUnlock.restype = ctypes.c_int
    _kernel32.GlobalSize.argtypes = [ctypes.c_void_p]
    _kernel32.GlobalSize.restype = ctypes.c_size_t
    _kernel32.GetModuleHandleW.argtypes = [ctypes.c_wchar_p]
    _kernel32.GetModuleHandleW.restype = ctypes.c_void_p
    _kernel32.lstrcpyW.argtypes = [ctypes.c_void_p, ctypes.c_wchar_p]
    _kernel32.lstrcpyW.restype = ctypes.c_void_p

    _user32.OpenClipboard.argtypes = [ctypes.c_void_p]
    _user32.OpenClipboard.restype = ctypes.c_int
    _user32.EmptyClipboard.argtypes = []
    _user32.EmptyClipboard.restype = ctypes.c_int
    _user32.CloseClipboard.argtypes = []
    _user32.CloseClipboard.restype = ctypes.c_int
    _user32.EnumClipboardFormats.argtypes = [ctypes.c_uint]
    _user32.EnumClipboardFormats.restype = ctypes.c_uint
    _user32.CountClipboardFormats.argtypes = []
    _user32.CountClipboardFormats.restype = ctypes.c_int
    _user32.GetClipboardData.argtypes = [ctypes.c_uint]
    _user32.GetClipboardData.restype = ctypes.c_void_p
    _user32.SetClipboardData.argtypes = [ctypes.c_uint, ctypes.c_void_p]
    _user32.SetClipboardData.restype = ctypes.c_void_p
    _user32.RegisterClipboardFormatW.argtypes = [ctypes.c_wchar_p]
    _user32.RegisterClipboardFormatW.restype = ctypes.c_uint
    # DragQueryFileW 位于 shell32.dll
    _shell32 = ctypes.WinDLL("shell32", use_last_error=True)
    _shell32.DragQueryFileW.argtypes = [ctypes.c_void_p, ctypes.c_uint, ctypes.c_void_p, ctypes.c_uint]
    _shell32.DragQueryFileW.restype = ctypes.c_uint
    _user32.CreateWindowExW.argtypes = [
        ctypes.c_uint, ctypes.c_void_p, ctypes.c_wchar_p, ctypes.c_uint,
        ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_int,
        ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p,
    ]
    _user32.CreateWindowExW.restype = ctypes.c_void_p
    _user32.DestroyWindow.argtypes = [ctypes.c_void_p]
    _user32.DestroyWindow.restype = ctypes.c_int
    _user32.SetWindowLongPtrW.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_void_p]
    _user32.SetWindowLongPtrW.restype = ctypes.c_void_p
    _user32.AddClipboardFormatListener.argtypes = [ctypes.c_void_p]
    _user32.AddClipboardFormatListener.restype = ctypes.c_int
    _user32.RemoveClipboardFormatListener.argtypes = [ctypes.c_void_p]
    _user32.RemoveClipboardFormatListener.restype = ctypes.c_int
    _user32.ShowWindow.argtypes = [ctypes.c_void_p, ctypes.c_int]
    _user32.ShowWindow.restype = ctypes.c_int
    _user32.PostMessageW.argtypes = [ctypes.c_void_p, ctypes.c_uint, ctypes.c_size_t, ctypes.c_long]
    _user32.PostMessageW.restype = ctypes.c_int
    _user32.GetMessageW.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_uint, ctypes.c_uint]
    _user32.GetMessageW.restype = ctypes.c_int
    _user32.TranslateMessage.argtypes = [ctypes.c_void_p]
    _user32.TranslateMessage.restype = ctypes.c_int
    _user32.DispatchMessageW.argtypes = [ctypes.c_void_p]
    _user32.DispatchMessageW.restype = ctypes.c_long
    _user32.DefWindowProcW.argtypes = [ctypes.c_void_p, ctypes.c_uint, ctypes.c_size_t, ctypes.c_long]
    _user32.DefWindowProcW.restype = ctypes.c_long
    _user32.RegisterClassExW.argtypes = [ctypes.c_void_p]
    _user32.RegisterClassExW.restype = ctypes.c_ushort
    _user32.UnregisterClassW.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
    _user32.UnregisterClassW.restype = ctypes.c_int
    _user32.PostQuitMessage.argtypes = [ctypes.c_int]
    _user32.PostQuitMessage.restype = None
else:
    _kernel32 = None  # type: ignore[assignment]
    _user32 = None  # type: ignore[assignment]
    _shell32 = None  # type: ignore[assignment]


class _ClipboardReader:
    """统一的剪贴板读写接口。"""

    __slots__ = ("_is_win", "_html_fmt", "_rtf_fmt", "_tk_cache")

    _instance_lock = threading.Lock()
    _shared: Optional["_ClipboardReader"] = None

    @classmethod
    def shared(cls) -> "_ClipboardReader":
        with cls._instance_lock:
            if cls._shared is None:
                cls._shared = cls()
            return cls._shared

    def __init__(self) -> None:
        self._is_win = bool(sys.platform == "win32")
        self._html_fmt: Optional[int] = None
        self._rtf_fmt: Optional[int] = None
        self._tk_cache: Optional[tk.Tk] = None
        if self._is_win:
            try:
                self._html_fmt = _user32.RegisterClipboardFormatW("HTML Format")
                self._rtf_fmt = _user32.RegisterClipboardFormatW("Rich Text Format")
            except Exception as e:  # pragma: no cover
                log.debug("RegisterClipboardFormatW 失败: %s", e)

    # --- 读取 ---
    def read(self) -> Tuple[ChangeType, str | bytes | None, List[str], Dict[str, Any]]:
        try:
            if self._is_win:
                return self._read_win()
            return self._read_tk()
        except Exception as e:
            log.debug("_ClipboardReader.read 异常: %s", e)
            return ChangeType.OTHER, None, [], {"error": repr(e)}

    def _read_win(self) -> Tuple[ChangeType, str | bytes | None, List[str], Dict[str, Any]]:
        for attempt in range(10):
            if not _user32.OpenClipboard(None):
                time.sleep(0.05 * (attempt + 1))
                continue
            try:
                # 枚举可用格式
                fmt = _user32.EnumClipboardFormats(0)
                available: List[int] = []
                while fmt:
                    available.append(int(fmt))
                    fmt = _user32.EnumClipboardFormats(fmt)
                if not available:
                    return ChangeType.CLEAR, None, [], {}

                priority: List[int] = [
                    _CF["HDROP"],
                    _CF["DIB"],
                    self._html_fmt or 0,
                    self._rtf_fmt or 0,
                    _CF["UNICODETEXT"],
                    _CF["TEXT"],
                ]
                chosen = next((f for f in priority if f and f in available), available[0])

                if chosen == _CF["HDROP"]:
                    hdrop = _user32.GetClipboardData(chosen)
                    if not hdrop:
                        return ChangeType.OTHER, None, [], {
                            "error": "GetClipboardData(HDROP) 失败"
                        }
                    count = _shell32.DragQueryFileW(hdrop, -1, None, 0)
                    files: List[str] = []
                    buf = ctypes.create_unicode_buffer(260)
                    for i in range(count):
                        if _shell32.DragQueryFileW(hdrop, i, buf, 260):
                            files.append(buf.value)
                    return ChangeType.FILES, None, files, {}

                if chosen == _CF["DIB"]:
                    h_mem = _user32.GetClipboardData(chosen)
                    if not h_mem:
                        return ChangeType.OTHER, None, [], {
                            "error": "GetClipboardData(DIB) 失败"
                        }
                    ptr = _kernel32.GlobalLock(h_mem)
                    try:
                        size = _kernel32.GlobalSize(h_mem)
                        if size <= 0:
                            return ChangeType.OTHER, None, [], {"error": "DIB 空内容"}
                        data = ctypes.string_at(ptr, size)
                        return ChangeType.IMAGE, data, [], {}
                    finally:
                        _kernel32.GlobalUnlock(h_mem)

                if chosen == _CF["UNICODETEXT"] or chosen == _CF["TEXT"]:
                    h_mem = _user32.GetClipboardData(_CF["UNICODETEXT"])
                    if not h_mem:
                        h_mem = _user32.GetClipboardData(_CF["TEXT"])
                    if not h_mem:
                        return ChangeType.OTHER, None, [], {
                            "error": "GetClipboardData(TEXT) 失败"
                        }
                    ptr = _kernel32.GlobalLock(h_mem)
                    try:
                        size = _kernel32.GlobalSize(h_mem)
                        if size <= 0:
                            return ChangeType.TEXT, "", [], {}
                        raw = ctypes.string_at(ptr, size)
                        try:
                            text = raw.decode("utf-16-le").rstrip("\x00")
                        except Exception:
                            text = raw.decode("utf-8", errors="replace").rstrip("\x00")
                        return ChangeType.TEXT, text, [], {}
                    finally:
                        _kernel32.GlobalUnlock(h_mem)

                # 自定义 / HTML / RTF
                h_mem = _user32.GetClipboardData(chosen)
                if h_mem:
                    ptr = _kernel32.GlobalLock(h_mem)
                    try:
                        size = _kernel32.GlobalSize(h_mem)
                        if size > 0:
                            data = ctypes.string_at(ptr, size)
                            if chosen == (self._html_fmt or -1):
                                return ChangeType.HTML, data.decode("utf-8", errors="replace"), [], {}
                            if chosen == (self._rtf_fmt or -1):
                                return ChangeType.RTF, data.decode("utf-8", errors="replace"), [], {}
                            return ChangeType.OTHER, data, [], {}
                    finally:
                        _kernel32.GlobalUnlock(h_mem)

                return ChangeType.OTHER, None, [], {}
            finally:
                try:
                    _user32.CloseClipboard()
                except Exception:
                    pass
        return ChangeType.OTHER, None, [], {"error": "OpenClipboard 重试失败"}

    def _read_tk(self) -> Tuple[ChangeType, str | bytes | None, List[str], Dict[str, Any]]:
        root = self._tk()
        if root is None:
            return ChangeType.OTHER, None, [], {"error": "tk 不可用"}
        try:
            try:
                text = root.clipboard_get()
            except tk.TclError:
                text = ""
            if text == "":
                return ChangeType.CLEAR, None, [], {}
            return ChangeType.TEXT, text, [], {}
        except Exception as e:
            return ChangeType.OTHER, None, [], {"error": repr(e)}

    def _tk(self) -> Optional[tk.Tk]:
        try:
            if self._tk_cache is None:
                self._tk_cache = tk.Tk()
                self._tk_cache.withdraw()
            return self._tk_cache
        except Exception as e:
            log.debug("tkinter 初始化失败: %s", e)
            return None

    # --- 写入 ---
    def write(
        self,
        content: str | bytes | None = None,
        files: Iterable[str] | None = None,
        change_type: Optional[ChangeType] = None,
    ) -> None:
        files = list(files or []) if files else []
        if files and self._is_win:
            self._write_hdrop(files)
            if isinstance(content, str):
                self._write_unicode(content)
            return
        if isinstance(content, str) and (
            change_type is None or change_type == ChangeType.TEXT
        ):
            self._write_unicode(content) if self._is_win else self._write_tk(content)
            return
        if isinstance(content, (bytes, bytearray)) and self._is_win:
            self._write_bytes(bytes(content), _CF["DIB"])
            return
        if isinstance(content, str):
            self._write_unicode(content) if self._is_win else self._write_tk(content)
            return
        # 回退：清空
        self._clear()

    def _write_unicode(self, text: str) -> None:
        src = (text or "").encode("utf-16-le") + b"\x00\x00"
        last_err: Optional[OSError] = None
        for attempt in range(10):
            try:
                if not _user32.OpenClipboard(None):
                    last_err = OSError("OpenClipboard 失败")
                    time.sleep(0.05 * (attempt + 1))
                    continue
                try:
                    _user32.EmptyClipboard()
                    h_mem = _kernel32.GlobalAlloc(_GHND, len(src))
                    if not h_mem:
                        raise OSError("GlobalAlloc 失败")
                    ptr = _kernel32.GlobalLock(h_mem)
                    try:
                        ctypes.memmove(ptr, src, len(src))
                    finally:
                        _kernel32.GlobalUnlock(h_mem)
                    if not _user32.SetClipboardData(_CF["UNICODETEXT"], h_mem):
                        raise OSError("SetClipboardData 失败")
                finally:
                    _user32.CloseClipboard()
                return
            except OSError as e:
                last_err = e
                time.sleep(0.05 * (attempt + 1))
        if last_err is not None:
            raise last_err

    def _write_bytes(self, data: bytes, fmt: int) -> None:
        last_err: Optional[OSError] = None
        for attempt in range(10):
            try:
                if not _user32.OpenClipboard(None):
                    last_err = OSError("OpenClipboard 失败")
                    time.sleep(0.05 * (attempt + 1))
                    continue
                try:
                    _user32.EmptyClipboard()
                    h_mem = _kernel32.GlobalAlloc(_GHND, len(data))
                    if not h_mem:
                        raise OSError("GlobalAlloc 失败")
                    ptr = _kernel32.GlobalLock(h_mem)
                    try:
                        ctypes.memmove(ptr, data, len(data))
                    finally:
                        _kernel32.GlobalUnlock(h_mem)
                    if not _user32.SetClipboardData(fmt, h_mem):
                        raise OSError("SetClipboardData 失败")
                finally:
                    _user32.CloseClipboard()
                return
            except OSError as e:
                last_err = e
                time.sleep(0.05 * (attempt + 1))
        if last_err is not None:
            raise last_err

    def _write_hdrop(self, files: List[str]) -> None:
        if not files:
            return

        class _DROPFILES(ctypes.Structure):
            _fields_ = [
                ("pFiles", wt.DWORD), ("pt_x", wt.LONG), ("pt_y", wt.LONG),
                ("fNC", wt.BOOL), ("fWide", wt.BOOL),
            ]

        paths_buf = "".join(os.path.abspath(p) + "\x00" for p in files) + "\x00"
        paths_bytes = paths_buf.encode("utf-16-le")
        struct = _DROPFILES(
            pFiles=ctypes.sizeof(_DROPFILES), pt_x=0, pt_y=0, fNC=False, fWide=True,
        )
        struct_bytes = ctypes.string_at(ctypes.byref(struct), ctypes.sizeof(_DROPFILES))
        total = struct_bytes + paths_bytes

        last_err: Optional[OSError] = None
        for attempt in range(10):
            try:
                if not _user32.OpenClipboard(None):
                    last_err = OSError("OpenClipboard 失败")
                    time.sleep(0.05 * (attempt + 1))
                    continue
                try:
                    _user32.EmptyClipboard()
                    h_mem = _kernel32.GlobalAlloc(_GHND, len(total))
                    if not h_mem:
                        raise OSError("GlobalAlloc 失败")
                    ptr = _kernel32.GlobalLock(h_mem)
                    try:
                        ctypes.memmove(ptr, total, len(total))
                    finally:
                        _kernel32.GlobalUnlock(h_mem)
                    if not _user32.SetClipboardData(_CF["HDROP"], h_mem):
                        raise OSError("SetClipboardData(HDROP) 失败")
                finally:
                    _user32.CloseClipboard()
                return
            except OSError as e:
                last_err = e
                time.sleep(0.05 * (attempt + 1))
        if last_err is not None:
            raise last_err

    def _write_tk(self, text: str) -> None:
        root = self._tk()
        if root is None:
            raise OSError("tk 不可用")
        root.clipboard_clear()
        root.clipboard_append(text or "")
        root.update()

    def _clear(self) -> None:
        if self._is_win:
            if _user32.OpenClipboard(None):
                try:
                    _user32.EmptyClipboard()
                finally:
                    _user32.CloseClipboard()
        else:
            self._write_tk("")


# ====================================================================
# 后端：Win32 Hook + Polling
# ====================================================================

_WM_CLIPBOARDUPDATE = 0x031D
_WM_CLOSE = 0x0010
_WM_DESTROY = 0x0002
# 注意: 不是所有 Windows 版本都支持 HWND_MESSAGE(-3) 作为 hWndParent,
# 所以这里不使用 HWND_MESSAGE, 而是创建普通不可见窗口
_WS_EX_TOOLWINDOW = 0x00000080
_WS_POPUP = 0x80000000
_SW_HIDE = 0

# 使用指针尺寸的有符号整数作为 LRESULT 替代；其余窗口字段在现代 Windows
# 上使用 void*/指针大小即可正常工作，无需依赖 wintypes 里那些并非处处都有的名称
_LRESULT = ctypes.c_ssize_t
_WNDPROC = ctypes.WINFUNCTYPE(_LRESULT, ctypes.c_void_p, ctypes.c_uint, ctypes.c_size_t, ctypes.c_long)


class _WNDCLASSEXW(ctypes.Structure):
    _fields_ = [
        ("cbSize", ctypes.c_uint),
        ("style", ctypes.c_uint),
        ("lpfnWndProc", _WNDPROC),
        ("cbClsExtra", ctypes.c_int),
        ("cbWndExtra", ctypes.c_int),
        ("hInstance", ctypes.c_void_p),
        ("hIcon", ctypes.c_void_p),
        ("hCursor", ctypes.c_void_p),
        ("hbrBackground", ctypes.c_void_p),
        ("lpszMenuName", ctypes.c_wchar_p),
        ("lpszClassName", ctypes.c_wchar_p),
        ("hIconSm", ctypes.c_void_p),
    ]


class _Win32HookBackend:
    """Windows 下基于 AddClipboardFormatListener 的事件驱动后端。

    在一个单独的后台线程内：
      1) 注册窗口类 + 创建隐藏 HWND_MESSAGE 窗口
      2) 调用 user32.AddClipboardFormatListener(hwnd)
      3) 进入 GetMessageW 消息循环；WM_CLIPBOARDUPDATE 触发回调
      4) 停止时 PostMessageW(hwnd, WM_CLOSE, 0, 0) → 窗口过程收到 WM_CLOSE
         → PostQuitMessage(0) → GetMessageW 返回 0 → 线程退出
    """

    name = "win32"

    def __init__(self, on_change: Callable[[], None]) -> None:
        self._on_change = on_change
        self._thread: Optional[threading.Thread] = None
        self._hwnd: Optional[int] = None
        self._wnd_proc_ref: Optional[_WNDPROC] = None
        self._running = False
        self._lock = threading.Lock()
        self._start_done = threading.Event()
        self._start_error: Optional[str] = None

    @property
    def is_running(self) -> bool:
        return self._running

    def start(self) -> None:
        with self._lock:
            if self._running:
                return
            self._start_done.clear()
            self._start_error = None
            self._thread = threading.Thread(
                target=self._run, name="rx-rust-clip-win32", daemon=True,
            )
            self._thread.start()
        # 等待线程完成窗口注册（最多 2 秒）
        self._start_done.wait(timeout=2.0)
        if self._start_error:
            raise OSError(self._start_error)
        self._running = True

    def stop(self) -> None:
        with self._lock:
            hwnd = self._hwnd
            if not self._running:
                return
            self._running = False
        if hwnd:
            try:
                _user32.PostMessageW(hwnd, _WM_CLOSE, 0, 0)
            except Exception:  # pragma: no cover
                pass
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)

    # ---- 线程主循环 --------------------------------------------------
    def _run(self) -> None:
        try:
            def wnd_proc(hwnd: int, msg: int, wparam: int, lparam: int) -> int:
                if msg == _WM_CLIPBOARDUPDATE:
                    try:
                        self._on_change()
                    except Exception as e:  # pragma: no cover
                        log.debug("Win32 on_change 回调异常: %s", e)
                    return 0
                if msg in (_WM_CLOSE, _WM_DESTROY):
                    try:
                        _user32.PostQuitMessage(0)
                    except Exception:
                        pass
                    return 0
                return _user32.DefWindowProcW(hwnd, msg, wparam, lparam)

            self._wnd_proc_ref = _WNDPROC(wnd_proc)

            # 直接使用 Windows 内建的 'STATIC' 控件类 + SetWindowLongPtrW 子类化
            hwnd = _user32.CreateWindowExW(
                _WS_EX_TOOLWINDOW, "STATIC", "RxRustClip", _WS_POPUP,
                0, 0, 0, 0, 0, 0, 0, 0,
            )
            if not hwnd:
                self._start_error = "CreateWindowExW(STATIC) 返回 NULL"
                self._start_done.set()
                return
            _user32.ShowWindow(hwnd, _SW_HIDE)

            # GWLP_WNDPROC = -4: 替换窗口过程(子类化)
            if not _user32.SetWindowLongPtrW(hwnd, -4, self._wnd_proc_ref):
                self._start_error = "SetWindowLongPtrW 失败"
                try:
                    _user32.DestroyWindow(hwnd)
                except Exception:
                    pass
                self._start_done.set()
                return

            if not _user32.AddClipboardFormatListener(hwnd):
                self._start_error = "AddClipboardFormatListener 失败"
                try:
                    _user32.DestroyWindow(hwnd)
                except Exception:
                    pass
                self._start_done.set()
                return

            self._hwnd = hwnd
            self._start_done.set()

            # GetMessageW 阻塞直到有消息到达; 对所有线程消息(第二个参数=0)
            msg = wt.MSG()
            while _user32.GetMessageW(ctypes.byref(msg), 0, 0, 0) != 0:
                _user32.TranslateMessage(ctypes.byref(msg))
                _user32.DispatchMessageW(ctypes.byref(msg))

            try:
                _user32.RemoveClipboardFormatListener(hwnd)
            except Exception:
                pass
            try:
                _user32.DestroyWindow(hwnd)
            except Exception:
                pass
            self._hwnd = None
        except Exception as e:  # pragma: no cover
            self._start_error = f"Win32 hook 异常: {e}"
            self._start_done.set()
            log.debug("win32 hook 运行异常: %s", e)
        finally:
            self._running = False


class _PollingBackend:
    """其它平台的保底路径：每隔 interval 秒检查一次剪贴板。"""

    name = "polling"

    def __init__(self, on_change: Callable[[], None], interval: float = 0.2) -> None:
        self._on_change = on_change
        self._interval = max(0.02, float(interval))
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._running = False
        self._lock = threading.Lock()

    @property
    def is_running(self) -> bool:
        return self._running

    def start(self) -> None:
        with self._lock:
            if self._running:
                return
            self._stop.clear()
            self._running = True
            self._thread = threading.Thread(
                target=self._run, name="rx-rust-clip-poll", daemon=True,
            )
            self._thread.start()

    def stop(self) -> None:
        with self._lock:
            if not self._running:
                return
            self._running = False
            self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=self._interval * 2 + 0.5)

    def _run(self) -> None:
        try:
            while not self._stop.is_set():
                try:
                    self._on_change()
                except Exception as e:  # pragma: no cover
                    log.debug("polling on_change 异常: %s", e)
                if self._stop.wait(self._interval):
                    break
        finally:
            self._running = False


# ====================================================================
# 签名计算：用于自过滤与重复内容识别
# ====================================================================


def _stable_hash(content: str | bytes | None) -> Optional[str]:
    """计算内容的稳定 hash（用于签名去重）。"""
    if content is None:
        return None
    if isinstance(content, (bytes, bytearray)):
        return hashlib.md5(bytes(content)).hexdigest()
    if isinstance(content, str):
        if not content:
            return None
        return hashlib.md5(content.encode("utf-8")).hexdigest()
    return None


def _make_signature(
    change_type: ChangeType,
    content: str | bytes | None,
    files: Iterable[str],
) -> Tuple[int, Optional[str], int, Tuple[str, ...]]:
    file_tuple = tuple(files or ())
    size = 0
    if isinstance(content, (bytes, bytearray)):
        size = len(content)
    elif isinstance(content, str):
        size = len(content)
    return (int(change_type), _stable_hash(content), size, file_tuple)


# ====================================================================
# ClipboardDispatcher：主入口
# ====================================================================


class ClipboardDispatcher:
    """剪贴板监控与分发器。

    典型用法:
        >>> d = ClipboardDispatcher()
        >>> d.subject.pipe(
        ...     ops.filter(lambda x: x.change_type == ChangeType.TEXT),
        ...     ops.map(lambda x: x.content.strip().upper()),
        ...     ops.write_to_clipboard(d, source="my-pipe"),
        ... ).subscribe(on_next=print)
        >>> d.start()
        >>> d.stop()

    或作为上下文管理器:
        >>> with ClipboardDispatcher() as d:
        ...     d.subject.subscribe(on_next=print)
    """

    __slots__ = (
        "_reader",
        "_backend",
        "_subject",
        "_lock",
        "_change_types_allowed",
        "_tags",
        "_interval",
        "_last_signature",
        "_self_signatures",
        "_dispatch_count",
        "_error_count",
        "_duplicate_count",
        "_self_filtered_count",
        "on_change_data",
        "filter_self",
        "self_filter",
        "self_source",
        "_backend_name",
        "_running",
    )

    def __init__(
        self,
        *,
        on_change_data: Callable[[], ClipData] | None = None,
        interval: float = 0.2,
        change_types: Iterable[ChangeType] | None = None,
        tags: Iterable[str] = (),
        backend: str = "auto",
        filter_self: bool = True,
        self_filter: Callable[[ClipData], bool] | None = None,
        self_source: str | None = None,
        self_signature_capacity: int = 32,
    ) -> None:
        self._reader = _ClipboardReader.shared()
        self._lock = threading.RLock()
        self._change_types_allowed: Optional[set] = (
            set(change_types) if change_types else None
        )
        self._tags: List[str] = list(tags or ())
        self._interval = max(0.02, float(interval))
        self._last_signature: Optional[Tuple[Any, ...]] = None
        self._self_signatures: "deque[Tuple[Any, ...]]" = deque(
            maxlen=max(1, int(self_signature_capacity))
        )
        self._dispatch_count = 0
        self._error_count = 0
        self._duplicate_count = 0
        self._self_filtered_count = 0

        self.on_change_data = on_change_data
        self.filter_self = bool(filter_self)
        self.self_filter = self_filter
        self.self_source = self_source or f"rx-rust:{os.getpid()}:{id(self)}"

        # Subject：可直接 pipe(...).subscribe(...)
        self._subject: PublishSubject = PublishSubject()

        # 选择后端
        backend = (backend or "auto").lower()
        be: Optional[Any] = None
        if backend in ("auto", "win32") and sys.platform == "win32":
            try:
                trial = _Win32HookBackend(self._dispatch_once)
                trial.start()
                trial.stop()
                be = _Win32HookBackend(self._dispatch_once)
                self._backend_name = "win32"
            except Exception as e:
                log.debug("win32 hook 不可用，回退到 polling: %s", e)
                be = None
                if backend == "win32":
                    raise
        if be is None:
            if backend == "win32" and sys.platform != "win32":
                raise OSError("win32 后端仅在 Windows 可用")
            be = _PollingBackend(self._dispatch_once, self._interval)
            self._backend_name = "polling"
        self._backend: Any = be
        self._running = False

    # ---- 属性 --------------------------------------------------------
    @property
    def subject(self) -> PublishSubject:
        return self._subject

    @property
    def backend_name(self) -> str:
        return self._backend_name

    @property
    def dispatch_count(self) -> int:
        return self._dispatch_count

    @property
    def error_count(self) -> int:
        return self._error_count

    @property
    def duplicate_count(self) -> int:
        return self._duplicate_count

    @property
    def self_filtered_count(self) -> int:
        return self._self_filtered_count

    @property
    def is_running(self) -> bool:
        return self._running and bool(getattr(self._backend, "is_running", False))

    # ---- 生命周期 ----------------------------------------------------
    def start(self) -> None:
        with self._lock:
            if self._running and getattr(self._backend, "is_running", False):
                return
            self._backend.start()
            self._running = True

    def stop(self) -> None:
        with self._lock:
            if not self._running:
                return
            self._running = False
            try:
                self._backend.stop()
            except Exception:
                pass

    def __enter__(self) -> "ClipboardDispatcher":
        self.start()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.stop()

    # ---- 核心：一次分发 ----------------------------------------------
    def _dispatch_once(self) -> None:
        change_type, content, files, meta = self._reader.read()

        if (
            self._change_types_allowed is not None
            and change_type not in self._change_types_allowed
        ):
            return

        sig = _make_signature(change_type, content, files)

        # 自过滤：命中签名 → 丢弃
        if self.filter_self and sig in self._self_signatures:
            try:
                self._self_signatures.remove(sig)
            except ValueError:
                pass
            self._self_filtered_count += 1
            return

        # 自定义 self_filter
        if self.self_filter is not None:
            try:
                draft = ClipData.now(
                    content=content, files=files, change_type=change_type,
                    tags=self._tags, metadata=meta,
                )
                if self.self_filter(draft):
                    self._self_filtered_count += 1
                    return
            except Exception as e:
                log.debug("self_filter 异常: %s", e)
                self._error_count += 1

        # 内容签名去重（外部写了完全相同的内容）
        if sig == self._last_signature:
            self._duplicate_count += 1
            return
        self._last_signature = sig

        # 构造 ClipData：优先 on_change_data，否则默认
        clip: Optional[ClipData] = None
        if self.on_change_data is not None:
            try:
                clip = self.on_change_data()
                if not isinstance(clip, ClipData):
                    raise TypeError("on_change_data 必须返回 ClipData")
            except Exception as e:
                log.debug("on_change_data 异常，回退默认 ClipData: %s", e)
                self._error_count += 1
                clip = None
        if clip is None:
            clip = ClipData.now(
                content=content, files=files, change_type=change_type,
                tags=list(self._tags), metadata=meta,
            )

        try:
            self._subject.on_next(clip)
        except Exception as e:
            log.debug("subject.on_next 异常: %s", e)
            self._error_count += 1
        self._dispatch_count += 1

    # ---- 标准写回 ----------------------------------------------------
    def set_clipboard(
        self,
        content: str | bytes | None = None,
        files: Iterable[str] | None = None,
        change_type: Optional[ChangeType] = None,
        *,
        source: str | None = None,
        tags: Iterable[str] = (),
        metadata: Dict[str, Any] | None = None,
    ) -> ClipData:
        """把内容写回系统剪贴板，登记为"自己写回"避免死循环。

        返回构造完成的 ClipData（同时通过 subject.on_next 分发给订阅者）。
        """
        files = list(files or [])
        if change_type is None:
            if files:
                change_type = ChangeType.FILES
            elif isinstance(content, (bytes, bytearray)):
                change_type = ChangeType.IMAGE
            else:
                change_type = ChangeType.TEXT
        with self._lock:
            self._reader.write(content, files, change_type)

            # 读取系统实际写入的内容用于签名，确保与 hook 路径读到一致
            try:
                ct2, c2, f2, _meta = self._reader.read()
                sig = _make_signature(ct2, c2, f2)
            except Exception:
                sig = _make_signature(change_type, content, files)

            # 登记签名（hook 路径触发回调时会命中并丢弃）
            self._self_signatures.append(sig)

            meta = dict(metadata or {})
            meta["_source"] = source or self.self_source
            meta["_owner_seq"] = next(_seq_counter)

            clip = ClipData.now(
                content=content, files=files, change_type=change_type,
                tags=list(tags) + list(self._tags), metadata=meta,
            )
            try:
                self._subject.on_next(clip)
            except Exception as e:
                log.debug("set_clipboard subject.on_next 异常: %s", e)
                self._error_count += 1
            self._dispatch_count += 1
            return clip


# ====================================================================
# 顶层工厂 & 操作符
# ====================================================================


def from_clipboard(
    *,
    interval: float = 0.2,
    backend: str = "auto",
    on_change_data: Callable[[], ClipData] | None = None,
    change_types: Iterable[ChangeType] | None = None,
    tags: Iterable[str] = (),
    auto_start: bool = True,
    filter_self: bool = True,
    self_source: str | None = None,
) -> Tuple[Any, ClipboardDispatcher]:
    """顶层工厂函数：返回 (Observable[ClipData], Dispatcher) 二元组。

    Subject 拥有 pipe 方法，可直接链式组合响应式算子:
        >>> obs, d = from_clipboard()
        >>> obs.pipe(
        ...     ops.filter(lambda x: x.change_type == ChangeType.TEXT),
        ...     ops.write_to_clipboard(d, source="my-pipe"),
        ... ).subscribe(on_next=print)
    """
    d = ClipboardDispatcher(
        interval=interval, backend=backend, on_change_data=on_change_data,
        change_types=change_types, tags=tags, filter_self=filter_self,
        self_source=self_source,
    )
    if auto_start:
        d.start()
    # Subject 本身支持订阅；包装为 Observable 以便调用方统一使用
    inner_sub = d._subject

    def _subscribe_func(observer: Any) -> Subscription:
        # observer 是 _PyObservable 传入的回调 callable
        if callable(observer):
            return inner_sub.subscribe(observer)
        return inner_sub.subscribe(lambda v: None)

    obs = Observable(_PyObservable(_subscribe_func))
    return obs, d


def write_to_clipboard(
    dispatcher: ClipboardDispatcher,
    source: str | None = None,
) -> Callable[[Any], Any]:
    """响应式操作符：把上游每一项写回剪贴板，并把构造的 ClipData 继续下发。

    上游可接受:
        ClipData  → 用 content/files/change_type/tags/metadata 写回
        str       → 作为纯文本
        bytes     → 作为图片（DIB）
        dict      → {"content", "files", "change_type", "tags", "metadata"}
        tuple/list→ (content, files, change_type, tags, metadata) 按位置，可缺项
    """

    def operator(source_observable: Any) -> Any:
        def subscribe(observer: Any) -> Subscription:
            def on_next(item: Any) -> None:
                try:
                    content: str | bytes | None = None
                    files: Optional[List[str]] = None
                    ct: Optional[ChangeType] = None
                    tags: List[str] = []
                    meta: Dict[str, Any] = {}
                    if isinstance(item, ClipData):
                        content = item.content
                        files = list(item.files)
                        ct = item.change_type
                        tags = list(item.tags)
                        meta = dict(item.metadata)
                    elif isinstance(item, (bytes, bytearray)):
                        content = bytes(item)
                    elif isinstance(item, str):
                        content = item
                    elif isinstance(item, dict):
                        content = item.get("content")
                        files = list(item.get("files") or [])
                        ct = item.get("change_type")
                        tags = list(item.get("tags") or [])
                        meta = dict(item.get("metadata") or {})
                    elif isinstance(item, (list, tuple)):
                        items = list(item) + [None] * (5 - len(item))
                        content, files, ct, tags, meta = items[:5]
                        files = list(files or [])
                        tags = list(tags or [])
                        meta = dict(meta or {})
                    else:
                        content = str(item)

                    clip = dispatcher.set_clipboard(
                        content=content, files=files, change_type=ct,
                        source=source, tags=tags, metadata=meta,
                    )
                    if callable(observer):
                        observer(clip)
                except Exception as e:
                    dispatcher._error_count += 1
                    log.debug("write_to_clipboard operator 异常: %s", e)

            return source_observable.subscribe(on_next=on_next)

        return Observable(_PyObservable(subscribe))

    return operator


# ====================================================================
# ClipSubject: 自包含 Dispatcher 的 Subject
# ====================================================================


class ClipSubject:
    """一个带剪贴板监控能力的 Subject。

    与普通 Subject 的区别:
      - 内部持有 ClipboardDispatcher
      - 上下文管理器 (with)
      - 直接暴露 start/stop/set_text/set_files/set_bytes/set_clipboard
      - 继承 Subject[ClipData], 支持 .pipe(...).subscribe(...)

    用法:
        >>> with ClipSubject() as clip:
        ...     clip.pipe(ops.filter(lambda c: c.change_type == ChangeType.TEXT)) \
        ...         .subscribe(on_next=lambda c: print(c.content))
        ...     clip.set_text("Hello")
    """

    __slots__ = ("_dispatcher", "_subject")

    def __init__(
        self,
        *,
        interval: float = 0.2,
        backend: str = "auto",
        change_types: Iterable[ChangeType] | None = None,
        tags: Iterable[str] = (),
        filter_self: bool = True,
        self_filter: Callable[[ClipData], bool] | None = None,
        self_source: str | None = None,
        auto_start: bool = True,
        on_change_data: Callable[[], ClipData] | None = None,
    ) -> None:
        self._dispatcher: ClipboardDispatcher = ClipboardDispatcher(
            on_change_data=on_change_data,
            interval=interval,
            backend=backend,
            change_types=change_types,
            tags=tags,
            filter_self=filter_self,
            self_filter=self_filter,
            self_source=self_source,
        )
        self._subject: PublishSubject = self._dispatcher.subject
        if auto_start:
            self._dispatcher.start()

    @property
    def dispatcher(self) -> ClipboardDispatcher:
        return self._dispatcher

    @property
    def subject(self) -> PublishSubject:
        return self._subject

    @property
    def backend_name(self) -> str:
        return self._dispatcher.backend_name

    @property
    def dispatch_count(self) -> int:
        return self._dispatcher.dispatch_count

    @property
    def self_filtered_count(self) -> int:
        return self._dispatcher.self_filtered_count

    @property
    def is_running(self) -> bool:
        return self._dispatcher.is_running

    def on_next(self, value: Any) -> None:
        self._subject.on_next(value)

    def on_completed(self) -> None:
        self._subject.on_completed()

    def subscribe(self, on_next=None, on_error=None, on_completed=None):
        return self._subject.subscribe(
            on_next=on_next, on_error=on_error, on_completed=on_completed
        )

    def pipe(self, *operators):
        # 返回 Observable 以便继续链式调用
        inner_sub = self._subject

        def _subscribe_func(observer: Any) -> Subscription:
            if callable(observer):
                return inner_sub.subscribe(observer)
            return inner_sub.subscribe(lambda v: None)

        obs = Observable(_PyObservable(_subscribe_func))
        result = obs
        for op in operators:
            result = op(result)
        return result

    def start(self) -> None:
        self._dispatcher.start()

    def stop(self) -> None:
        self._dispatcher.stop()

    def __enter__(self) -> "ClipSubject":
        self._dispatcher.start()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self._dispatcher.stop()

    def set_text(
        self, text: str, *, source: str | None = None,
        tags: Iterable[str] = (), metadata: Dict[str, Any] | None = None,
    ) -> ClipData:
        return self._dispatcher.set_clipboard(
            content=text, change_type=ChangeType.TEXT,
            source=source, tags=tags, metadata=metadata,
        )

    def set_files(
        self, files: Iterable[str], *, source: str | None = None,
        tags: Iterable[str] = (), metadata: Dict[str, Any] | None = None,
    ) -> ClipData:
        return self._dispatcher.set_clipboard(
            files=files, change_type=ChangeType.FILES,
            source=source, tags=tags, metadata=metadata,
        )

    def set_bytes(
        self, data: bytes, *, source: str | None = None,
        tags: Iterable[str] = (), metadata: Dict[str, Any] | None = None,
    ) -> ClipData:
        return self._dispatcher.set_clipboard(
            content=data, change_type=ChangeType.IMAGE,
            source=source, tags=tags, metadata=metadata,
        )

    def set_clipboard(
        self,
        content: str | bytes | None = None,
        files: Iterable[str] | None = None,
        change_type: ChangeType | None = None,
        *,
        source: str | None = None,
        tags: Iterable[str] = (),
        metadata: Dict[str, Any] | None = None,
    ) -> ClipData:
        return self._dispatcher.set_clipboard(
            content=content, files=files, change_type=change_type,
            source=source, tags=tags, metadata=metadata,
        )


# ====================================================================
# ClipObserver：按 ChangeType 路由的便捷观察者
# ====================================================================


class ClipObserver:
    """声明式地监听 ClipSubject / ClipboardDispatcher 发出的事件。

    用它代替手写 lambda + 类型判断。

    用法:
        >>> obs = ClipObserver(
        ...     on_text=lambda cd: print("文本:", cd.content),
        ...     on_files=lambda cd: print("文件:", cd.files),
        ...     on_any=lambda cd: print("事件:", cd.change_type.name),
        ... )
        >>> obs.subscribe(clip_subject)
    """

    __slots__ = (
        "_on_text", "_on_files", "_on_image", "_on_html", "_on_rtf",
        "_on_clear", "_on_other", "_on_any", "_on_error",
        "_on_completed", "_subscription",
    )

    def __init__(
        self,
        *,
        on_text: Callable[[ClipData], Any] | None = None,
        on_files: Callable[[ClipData], Any] | None = None,
        on_image: Callable[[ClipData], Any] | None = None,
        on_html: Callable[[ClipData], Any] | None = None,
        on_rtf: Callable[[ClipData], Any] | None = None,
        on_clear: Callable[[ClipData], Any] | None = None,
        on_other: Callable[[ClipData], Any] | None = None,
        on_any: Callable[[ClipData], Any] | None = None,
        on_error: Callable[[Exception], Any] | None = None,
        on_completed: Callable[[], Any] | None = None,
    ) -> None:
        self._on_text = on_text
        self._on_files = on_files
        self._on_image = on_image
        self._on_html = on_html
        self._on_rtf = on_rtf
        self._on_clear = on_clear
        self._on_other = on_other
        self._on_any = on_any
        self._on_error = on_error
        self._on_completed = on_completed
        self._subscription: Optional[Subscription] = None

    def _on_next(self, cd: ClipData) -> None:
        if self._on_any is not None:
            try:
                self._on_any(cd)
            except Exception as e:
                log.debug("ClipObserver.on_any 异常: %s", e)

        ct = cd.change_type
        handler = {
            ChangeType.TEXT: self._on_text,
            ChangeType.FILES: self._on_files,
            ChangeType.IMAGE: self._on_image,
            ChangeType.HTML: self._on_html,
            ChangeType.RTF: self._on_rtf,
            ChangeType.CLEAR: self._on_clear,
            ChangeType.OTHER: self._on_other,
        }.get(ct, None)

        if handler is not None:
            try:
                handler(cd)
            except Exception as e:
                log.debug("ClipObserver 回调 %s 异常: %s", ct.name, e)

    def _on_error_handler(self, err: Exception) -> None:
        if self._on_error is not None:
            try:
                self._on_error(err)
            except Exception as e:
                log.debug("ClipObserver.on_error 异常: %s", e)

    def _on_completed_handler(self) -> None:
        if self._on_completed is not None:
            try:
                self._on_completed()
            except Exception as e:
                log.debug("ClipObserver.on_completed 异常: %s", e)

    def subscribe(self, observable: Any) -> Optional[Subscription]:
        """订阅 Observable/Subject/ClipSubject。返回 Subscription。"""
        self.unsubscribe()
        self._subscription = observable.subscribe(
            on_next=self._on_next,
            on_error=self._on_error_handler,
            on_completed=self._on_completed_handler,
        )
        return self._subscription

    def attach(self, subject_or_dispatcher: Any) -> "ClipObserver":
        self.subscribe(subject_or_dispatcher)
        return self

    def unsubscribe(self) -> None:
        if self._subscription is not None:
            self._subscription = None

    @property
    def is_subscribed(self) -> bool:
        return self._subscription is not None

    def __enter__(self) -> "ClipObserver":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.unsubscribe()


# ====================================================================
# 对外导出
# ====================================================================

__all__ = [
    "ChangeType",
    "ClipData",
    "ClipboardDispatcher",
    "ClipSubject",
    "ClipObserver",
    "from_clipboard",
    "write_to_clipboard",
]
