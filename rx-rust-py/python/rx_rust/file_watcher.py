"""
rx-rust.file_watcher - 文件系统监控与响应式分发

核心公共 API:
    FileChangeType(IntEnum):    文件变更类型枚举
    FileData:                    结构化文件事件数据（支持 JSON/Pickle 往返）
    FileDispatcher:              文件系统监控与分发器（Windows ReadDirectoryChangesW / macOS polling / Linux inotify）
    FileSubject:                 带文件监控的 Subject（自包含 Dispatcher）
    FileObserver:                按 FileChangeType 路由回调的便捷观察者
    from_filesystem(...):        顶层工厂：返回 (Observable[FileData], Dispatcher)
    write_to_filesystem(...):   响应式操作符：把流内容写入文件系统

注意:
    文件系统监控不需要"自过滤"机制，因为写入文件与监控事件是两条独立路径。
"""

from __future__ import annotations

import ctypes
import itertools
import json
import logging
import os
import pickle
import sys
import threading
import time
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


log = logging.getLogger("rx_rust.file_watcher")

T = TypeVar("T")
R = TypeVar("R")


# ====================================================================
# 数据类型：FileChangeType / FileData
# ====================================================================


class FileChangeType(IntEnum):
    """文件变更类型枚举。"""

    CREATED = 0      # 文件/目录被创建
    MODIFIED = 1     # 文件内容被修改
    DELETED = 2      # 文件/目录被删除
    RENAMED = 3      # 文件/目录被重命名（old_path → new_path）
    MOVED_IN = 4     # 文件从监控目录外移入
    MOVED_OUT = 5    # 文件从监控目录移出
    ACCESS = 6       # 文件被读取
    ATTRIB = 7       # 文件属性/元数据变化

    def __str__(self) -> str:
        return self.name


# FileData 用的全局单调序号
_seq_counter = itertools.count(1)


@dataclass(slots=True)  # type: ignore[call-overload]
class FileData:
    """结构化的文件事件数据。

    字段:
        path:           触发变更的完整路径
        old_path:       重命名时旧路径；其它情况 None
        change_type:    变更类型（FileChangeType）
        is_directory:   是否为目录
        size:           变更后文件大小（删除时 None）
        timestamp:      检测到变更的时间
        sequence:       全局序号（单调递增）
        tags:           用户自定义标签
        metadata:       扩展元信息
    """

    path: str
    old_path: str | None
    change_type: FileChangeType
    is_directory: bool
    size: int | None
    timestamp: datetime
    sequence: int
    tags: List[str]
    metadata: Dict[str, Any]

    # ---- 工厂 --------------------------------------------------------
    @classmethod
    def now(
        cls,
        path: str,
        old_path: str | None = None,
        change_type: FileChangeType = FileChangeType.MODIFIED,
        is_directory: bool = False,
        size: int | None = None,
        tags: Iterable[str] = (),
        metadata: Dict[str, Any] | None = None,
    ) -> "FileData":
        return cls(
            path=path,
            old_path=old_path,
            change_type=change_type,
            is_directory=is_directory,
            size=size,
            timestamp=datetime.now(),
            sequence=next(_seq_counter),
            tags=list(tags or ()),
            metadata=dict(metadata or {}),
        )

    # ---- 序列化 ------------------------------------------------------
    def to_dict(self) -> Dict[str, Any]:
        ts = self.timestamp
        if isinstance(ts, datetime):
            ts_str = ts.isoformat()
        elif isinstance(ts, (int, float)):
            ts_str = datetime.fromtimestamp(float(ts)).isoformat()
        else:
            ts_str = datetime.now().isoformat()
        data: Dict[str, Any] = {
            "path": self.path,
            "old_path": self.old_path,
            "change_type": int(self.change_type),
            "change_type_name": str(self.change_type),
            "is_directory": self.is_directory,
            "size": self.size,
            "timestamp": ts_str,
            "sequence": self.sequence,
            "tags": list(self.tags),
            "metadata": dict(self.metadata),
        }
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FileData":
        d = dict(data or {})

        ct_raw = d.get("change_type", FileChangeType.MODIFIED.value)
        try:
            ct = FileChangeType(int(ct_raw))
        except (TypeError, ValueError):
            try:
                ct = FileChangeType[str(ct_raw).upper()]
            except KeyError:
                ct = FileChangeType.MODIFIED

        for k in ("change_type_name",):
            d.pop(k, None)

        ts = d.get("timestamp")
        if isinstance(ts, str):
            try:
                ts = datetime.fromisoformat(ts)
            except ValueError:
                ts = datetime.now()
        elif isinstance(ts, (int, float)):
            ts = datetime.fromtimestamp(float(ts))
        elif not isinstance(ts, datetime):
            ts = datetime.now()

        return cls(
            path=d.get("path", ""),
            old_path=d.get("old_path"),
            change_type=ct,
            is_directory=bool(d.get("is_directory", False)),
            size=d.get("size"),
            timestamp=ts,
            sequence=int(d.get("sequence", next(_seq_counter))),
            tags=list(d.get("tags") or []),
            metadata=dict(d.get("metadata") or {}),
        )

    def to_json(self, **kw: Any) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, **kw)

    @classmethod
    def from_json(cls, s: str, **kw: Any) -> "FileData":
        return cls.from_dict(json.loads(s, **kw))

    def to_pickle(self) -> bytes:
        return pickle.dumps(self)

    @classmethod
    def from_pickle(cls, b: bytes) -> "FileData":
        return pickle.loads(b)

    # ---- 表示 --------------------------------------------------------
    def __str__(self) -> str:
        return (
            f"FileData(path={self.path!r}, change_type={self.change_type.name}, "
            f"is_directory={self.is_directory}, size={self.size}, "
            f"seq={self.sequence})"
        )


# ====================================================================
# 签名计算：用于去重
# ====================================================================


def _make_signature(
    change_type: FileChangeType,
    path: str,
) -> Tuple[int, str]:
    """计算文件事件的稳定签名（用于去重）。"""
    return (int(change_type), os.path.normpath(path))


# ====================================================================
# 后端：Windows Win32 / macOS polling / Linux inotify / Polling
# ====================================================================


class _PollingWatchBackend:
    """通用平台的轮询监控后端：每隔 interval 秒检查一次文件状态。

    通过维护 {path: (mtime, size, is_dir)} 的快照字典来检测变更。
    """

    name = "polling"

    def __init__(
        self,
        on_change: Callable[[str, str | None, FileChangeType, bool], None],
        paths: Iterable[str] | None = None,
        change_types: Iterable[FileChangeType] | None = None,
        interval: float = 0.5,
    ) -> None:
        self._on_change = on_change
        self._paths: List[str] = list(paths) if paths else []
        self._change_types: Optional[set] = (
            set(change_types) if change_types else None
        )
        self._interval = max(0.02, float(interval))
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._running = False
        self._lock = threading.Lock()
        # 状态快照：full_path -> (mtime, size, is_dir)
        self._state: Dict[str, Tuple[float, int, bool]] = {}

    @property
    def is_running(self) -> bool:
        return self._running

    def add_path(self, path: str) -> None:
        path = os.path.abspath(path)
        with self._lock:
            if path not in self._paths:
                self._paths.append(path)

    def remove_path(self, path: str) -> None:
        path = os.path.abspath(path)
        with self._lock:
            if path in self._paths:
                self._paths.remove(path)
            # 清理不再监控的子路径
            for key in list(self._state.keys()):
                if key == path or key.startswith(path + os.sep):
                    self._state.pop(key, None)

    def start(self) -> None:
        with self._lock:
            if self._running:
                return
            self._stop.clear()
            self._running = True
            # 建立初始快照，不触发事件
            self._state.clear()
            for path_str in self._paths:
                abs_path = os.path.abspath(path_str)
                try:
                    for root, dirs, files in os.walk(abs_path):
                        for name in itertools.chain(files, dirs):
                            full = os.path.join(root, name)
                            try:
                                mtime = os.path.getmtime(full)
                                size = (
                                    os.path.getsize(full)
                                    if os.path.isfile(full)
                                    else 0
                                )
                                is_dir = os.path.isdir(full)
                                self._state[full] = (mtime, size, is_dir)
                            except OSError:
                                continue
                except Exception:
                    pass
            self._thread = threading.Thread(
                target=self._run, name="rx-rust-file-polling", daemon=True,
            )
            self._thread.start()

    def stop(self) -> None:
        with self._lock:
            if not self._running:
                return
            self._running = False
            self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=self._interval * 3 + 0.5)

    def _run(self) -> None:
        try:
            while not self._stop.is_set():
                time.sleep(self._interval)
                # 在循环体内部复制路径列表，避免持有锁太久
                paths_snapshot = list(self._paths)
                current_files: Dict[str, Tuple[float, int, bool]] = {}

                for path_str in paths_snapshot:
                    abs_path = os.path.abspath(path_str)
                    try:
                        for root, dirs, files in os.walk(abs_path):
                            for name in itertools.chain(files, dirs):
                                full = os.path.join(root, name)
                                try:
                                    mtime = os.path.getmtime(full)
                                    size = (
                                        os.path.getsize(full)
                                        if os.path.isfile(full)
                                        else 0
                                    )
                                    is_dir = os.path.isdir(full)
                                    current_files[full] = (mtime, size, is_dir)
                                except OSError:
                                    continue
                    except Exception:
                        pass

                # 对比旧快照和当前快照，分发事件
                with self._lock:
                    old_state = self._state
                    new_state = current_files

                    # 删除：存在于旧快照，不在新快照
                    for full, info in old_state.items():
                        if full not in new_state:
                            _, _, is_dir = info
                            try:
                                self._on_change(
                                    full, None, FileChangeType.DELETED, is_dir
                                )
                            except Exception as e:
                                log.debug("on_change 删除事件异常: %s", e)

                    # 新增或修改：存在于新快照
                    for full, info in new_state.items():
                        mtime, size, is_dir = info
                        prev = old_state.get(full)
                        try:
                            if prev is None:
                                self._on_change(
                                    full, None, FileChangeType.CREATED, is_dir
                                )
                            elif prev != info:
                                self._on_change(
                                    full, None, FileChangeType.MODIFIED, is_dir
                                )
                        except Exception as e:
                            log.debug("on_change 异常: %s", e)

                    self._state = new_state
        finally:
            self._running = False


class _Win32WatchBackend:
    """Windows 下基于 ReadDirectoryChangesW + OVERLAPPED I/O 的文件监控后端。

    为每个监控路径创建独立的异步监控句柄，使用 I/O Completion Port
    接收完成通知，解析 FILE_NOTIFY_INFORMATION 后触发回调。
    """

    name = "win32"

    def __init__(
        self,
        on_change: Callable[[str, str | None, FileChangeType, bool], None],
        paths: Iterable[str] | None = None,
        change_types: Iterable[FileChangeType] | None = None,
        interval: float = 0.2,
    ) -> None:
        self._on_change = on_change
        self._paths: List[str] = list(paths) if paths else []
        self._change_types: Optional[set] = (
            set(change_types) if change_types else None
        )
        self._interval = max(0.02, float(interval))
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._running = False
        self._lock = threading.Lock()

        # Win32 常量
        self._FILE_LIST_DIRECTORY = 0x0001
        self._FILE_FLAG_BACKUP_SEMANTICS = 0x02000000
        self._FILE_FLAG_OVERLAPPED = 0x40000000
        self._GENERIC_READ = 0x80000000
        self._FILE_SHARE_READ = 1
        self._FILE_SHARE_WRITE = 2
        self._OPEN_EXISTING = 3
        self._FILE_ACTION_ADDED = 1
        self._FILE_ACTION_MODIFIED = 2
        self._FILE_ACTION_REMOVED = 3
        self._FILE_ACTION_RENAMED_OLD_NAME = 4
        self._FILE_ACTION_RENAMED_NEW_NAME = 5
        self._INVALID_HANDLE_VALUE = -1
        self._FILE_NOTIFY_CHANGE_FILE_NAME = 0x00000001
        self._FILE_NOTIFY_CHANGE_DIR_NAME = 0x00000002
        self._FILE_NOTIFY_CHANGE_SIZE = 0x00000008
        self._FILE_NOTIFY_CHANGE_LAST_WRITE = 0x00000010
        self._FILTER = (
            self._FILE_NOTIFY_CHANGE_FILE_NAME
            | self._FILE_NOTIFY_CHANGE_DIR_NAME
            | self._FILE_NOTIFY_CHANGE_SIZE
            | self._FILE_NOTIFY_CHANGE_LAST_WRITE
        )
        self._BUFFER_SIZE = 65536

        # Win32 DLL 引用
        self._kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)

        # 设置 argtypes/restype 以避免 64 位指针截断
        self._kernel32.CreateFileW.argtypes = [
            wt.LPCWSTR, wt.DWORD, wt.DWORD,
            ctypes.c_void_p, wt.DWORD, wt.DWORD, ctypes.c_void_p,
        ]
        self._kernel32.CreateFileW.restype = ctypes.c_void_p

        self._kernel32.CreateIoCompletionPort.argtypes = [
            ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p, wt.DWORD,
        ]
        self._kernel32.CreateIoCompletionPort.restype = ctypes.c_void_p

        self._kernel32.GetQueuedCompletionStatus.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(wt.DWORD),
            ctypes.POINTER(ctypes.c_void_p),
            ctypes.POINTER(ctypes.c_void_p),
            wt.DWORD,
        ]
        self._kernel32.GetQueuedCompletionStatus.restype = wt.BOOL

        self._kernel32.ReadDirectoryChangesW.argtypes = [
            ctypes.c_void_p,
            ctypes.c_void_p,
            wt.DWORD,
            wt.BOOL,
            wt.DWORD,
            ctypes.POINTER(wt.DWORD),
            ctypes.c_void_p,
            ctypes.c_void_p,
        ]
        self._kernel32.ReadDirectoryChangesW.restype = wt.BOOL

        self._kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
        self._kernel32.CloseHandle.restype = wt.BOOL

    # ---- 结构定义（需要时动态创建，避免污染外部命名空间） ----
    def _make_overlapped(self) -> Any:
        class OVERLAPPED(ctypes.Structure):
            _fields_ = [
                ("Internal", ctypes.c_void_p),
                ("InternalHigh", ctypes.c_void_p),
                ("Offset", wt.DWORD),
                ("OffsetHigh", wt.DWORD),
                ("hEvent", ctypes.c_void_p),
            ]
        return OVERLAPPED()

    def _make_notify_info(self) -> Any:
        class FILE_NOTIFY_INFORMATION(ctypes.Structure):
            _fields_ = [
                ("NextEntryOffset", wt.DWORD),
                ("Action", wt.DWORD),
                ("FileNameLength", wt.DWORD),
                ("FileName", wt.WCHAR * 1),
            ]
        return FILE_NOTIFY_INFORMATION

    @property
    def is_running(self) -> bool:
        return self._running

    def add_path(self, path: str) -> None:
        path = os.path.abspath(path)
        with self._lock:
            if path not in self._paths:
                self._paths.append(path)

    def remove_path(self, path: str) -> None:
        path = os.path.abspath(path)
        with self._lock:
            if path in self._paths:
                self._paths.remove(path)

    def start(self) -> None:
        with self._lock:
            if self._running:
                return
            self._stop.clear()
            self._running = True
            self._thread = threading.Thread(
                target=self._run, name="rx-rust-file-win32", daemon=True,
            )
            self._thread.start()

    def stop(self) -> None:
        with self._lock:
            if not self._running:
                return
            self._running = False
            self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=self._interval * 3 + 0.5)

    def _run(self) -> None:
        kernel32 = self._kernel32
        FILE_NOTIFY_INFORMATION = self._make_notify_info()

        # 每个条目：(hDir, overlapped, buffer, base_path)
        watch_entries: Dict[int, Tuple[int, Any, bytes, str]] = {}

        hCompPort = kernel32.CreateIoCompletionPort(
            self._INVALID_HANDLE_VALUE, None, 0, 1
        )
        if hCompPort is None or hCompPort == self._INVALID_HANDLE_VALUE:
            log.debug("CreateIoCompletionPort 失败，回退到 polling")
            self._running = False
            return

        for path_str in list(self._paths):
            abs_path = os.path.abspath(path_str)
            hDir = kernel32.CreateFileW(
                abs_path,
                self._GENERIC_READ,
                self._FILE_SHARE_READ | self._FILE_SHARE_WRITE,
                None,
                self._OPEN_EXISTING,
                self._FILE_FLAG_BACKUP_SEMANTICS | self._FILE_FLAG_OVERLAPPED,
                None,
            )
            if hDir == self._INVALID_HANDLE_VALUE or hDir is None:
                log.debug("无法打开目录 %s", abs_path)
                continue

            buffer = ctypes.create_string_buffer(self._BUFFER_SIZE)
            bytes_returned = wt.DWORD()
            overlapped = self._make_overlapped()

            kernel32.CreateIoCompletionPort(hDir, hCompPort, 0, 1)

            success = kernel32.ReadDirectoryChangesW(
                hDir,
                ctypes.byref(buffer),
                self._BUFFER_SIZE,
                True,  # bWatchSubtree
                self._FILTER,
                ctypes.byref(bytes_returned),
                ctypes.byref(overlapped),
                None,
            )

            if success:
                watch_entries[id(buffer)] = (hDir, overlapped, buffer, abs_path)
            else:
                log.debug("ReadDirectoryChangesW 失败 for %s", abs_path)
                kernel32.CloseHandle(hDir)

        if not watch_entries:
            kernel32.CloseHandle(hCompPort)
            self._running = False
            return

        # 记录 old_name -> new_name 配对
        pending_old_names: Dict[str, str] = {}  # 暂未使用

        while not self._stop.is_set():
            # 等待 I/O 完成
            completion_key = ctypes.c_void_p()
            bytes_out = wt.DWORD()
            overlapped_ptr_raw = ctypes.c_void_p()

            rc = kernel32.GetQueuedCompletionStatus(
                hCompPort,
                ctypes.byref(bytes_out),
                ctypes.byref(completion_key),
                ctypes.byref(overlapped_ptr_raw),
                int(self._interval * 1000),
            )

            if rc == 0:
                err = ctypes.get_last_error()
                # WAIT_TIMEOUT = 258
                if err == 258:
                    continue
                # 其它错误，继续尝试
                continue

            # 查找匹配的条目：通过 overlapped_ptr_raw 找到 buffer 和 base_path
            matched_entry: Optional[Tuple[int, Any, bytes, str]] = None
            for key, entry in watch_entries.items():
                _, ov, buffer, base_path = entry
                # 比较 overlapped 地址
                if ctypes.byref(ov).value == overlapped_ptr_raw.value:
                    matched_entry = entry
                    break

            if matched_entry is None:
                # 重新为所有目录发起监控
                self._rearm_all(kernel32, watch_entries)
                continue

            hDir, old_overlapped, buffer, base_path = matched_entry

            # 为该目录重新发起 ReadDirectoryChangesW
            new_bytes = wt.DWORD()
            kernel32.ReadDirectoryChangesW(
                hDir,
                ctypes.byref(buffer),
                self._BUFFER_SIZE,
                True,
                self._FILTER,
                ctypes.byref(new_bytes),
                ctypes.byref(old_overlapped),
                None,
            )

            # 解析 FILE_NOTIFY_INFORMATION
            buf_addr = ctypes.addressof(buffer)
            pos = 0
            pending_renamed_old: Dict[str, str] = {}

            try:
                while True:
                    info_ptr = ctypes.cast(
                        buf_addr + pos,
                        ctypes.POINTER(FILE_NOTIFY_INFORMATION),
                    )
                    info = info_ptr.contents

                    if info.FileNameLength > 0:
                        name_chars = info.FileNameLength // 2
                        # 读取文件名：从 FILE_NOTIFY_INFORMATION 的 FileName 字段起
                        # 使用 ctypes.wstring_at 读取宽字符串
                        name_addr = buf_addr + pos + 12  # NextEntryOffset(4) + Action(4) + FileNameLength(4) = 12
                        file_name = ctypes.wstring_at(name_addr, name_chars)
                        full_path = os.path.join(base_path, file_name)

                        action = info.Action
                        old_path = None
                        if action == self._FILE_ACTION_ADDED:
                            ct = FileChangeType.CREATED
                        elif action == self._FILE_ACTION_MODIFIED:
                            ct = FileChangeType.MODIFIED
                        elif action == self._FILE_ACTION_REMOVED:
                            ct = FileChangeType.DELETED
                        elif action == self._FILE_ACTION_RENAMED_OLD_NAME:
                            # 等待 NEW_NAME 配对
                            pending_renamed_old[file_name] = full_path
                            ct = None
                        elif action == self._FILE_ACTION_RENAMED_NEW_NAME:
                            ct = FileChangeType.RENAMED
                            # 尝试从 pending 中找 old_name
                            old_path = None
                            for old_fn, old_fp in list(pending_renamed_old.items()):
                                if old_fn and file_name:
                                    old_path = old_fp
                                    del pending_renamed_old[old_fn]
                                    break
                        else:
                            ct = FileChangeType.MODIFIED

                        if ct is not None:
                            is_dir = False
                            try:
                                if os.path.exists(full_path):
                                    is_dir = os.path.isdir(full_path)
                            except OSError:
                                pass
                            try:
                                self._on_change(full_path, old_path, ct, is_dir)
                            except Exception as e:
                                log.debug("on_change 异常: %s", e)

                    if info.NextEntryOffset == 0:
                        break
                    pos += info.NextEntryOffset
            except Exception as e:
                log.debug("解析 FILE_NOTIFY_INFORMATION 异常: %s", e)

        # ---- 清理 ----
        for _, (hDir, _, _, _) in watch_entries.items():
            try:
                kernel32.CloseHandle(hDir)
            except Exception:
                pass
        try:
            kernel32.CloseHandle(hCompPort)
        except Exception:
            pass
        self._running = False

    def _rearm_all(self, kernel32: Any, entries: Dict[int, Tuple[int, Any, bytes, str]]) -> None:
        """为所有监控目录重新发起 ReadDirectoryChangesW。"""
        for key, (hDir, overlapped, buffer, base_path) in entries.items():
            try:
                bytes_returned = wt.DWORD()
                kernel32.ReadDirectoryChangesW(
                    hDir,
                    ctypes.byref(buffer),
                    self._BUFFER_SIZE,
                    True,
                    self._FILTER,
                    ctypes.byref(bytes_returned),
                    ctypes.byref(overlapped),
                    None,
                )
            except Exception:
                pass


class _InotifyWatchBackend:
    """Linux 下基于 inotify_init / inotify_add_watch + epoll 的文件监控后端。"""

    name = "inotify"

    def __init__(
        self,
        on_change: Callable[[str, str | None, FileChangeType, bool], None],
        paths: Iterable[str] | None = None,
        change_types: Iterable[FileChangeType] | None = None,
        interval: float = 0.2,
    ) -> None:
        self._on_change = on_change
        self._paths: List[str] = list(paths) if paths else []
        self._change_types: Optional[set] = (
            set(change_types) if change_types else None
        )
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
                target=self._run, name="rx-rust-file-inotify", daemon=True,
            )
            self._thread.start()

    def stop(self) -> None:
        with self._lock:
            if not self._running:
                return
            self._running = False
            self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=self._interval * 3 + 0.5)

    def _run(self) -> None:
        import struct as _struct
        import select as _select

        try:
            _libc = ctypes.CDLL("libc.so.6", use_errno=True)
        except (OSError, AttributeError):
            log.debug("无法加载 libc，inotify 后端不可用")
            self._running = False
            return

        IN_ACCESS = 0x00000001
        IN_MODIFY = 0x00000002
        IN_ATTRIB = 0x00000004
        IN_CREATE = 0x00000100
        IN_DELETE = 0x00000200
        IN_MOVED_FROM = 0x00000040
        IN_MOVED_TO = 0x00000080
        IN_DELETE_SELF = 0x00000400
        IN_MOVE_SELF = 0x00000800
        IN_ISDIR = 0x40000000
        IN_ALL_EVENTS = (
            IN_ACCESS | IN_MODIFY | IN_ATTRIB |
            IN_CREATE | IN_DELETE |
            IN_MOVED_FROM | IN_MOVED_TO |
            IN_DELETE_SELF | IN_MOVE_SELF
        )

        _libc.inotify_init.argtypes = []
        _libc.inotify_init.restype = ctypes.c_int
        _libc.inotify_add_watch.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_uint32]
        _libc.inotify_add_watch.restype = ctypes.c_int
        _libc.inotify_rm_watch.argtypes = [ctypes.c_int, ctypes.c_int]
        _libc.inotify_rm_watch.restype = ctypes.c_int
        _libc.close.argtypes = [ctypes.c_int]
        _libc.close.restype = ctypes.c_int

        fd = _libc.inotify_init()
        if fd < 0:
            log.debug("inotify_init 失败")
            self._running = False
            return

        wd_map: Dict[int, str] = {}

        try:
            for p in list(self._paths):
                abs_p = os.path.abspath(p)
                if not os.path.exists(abs_p):
                    continue
                wd = _libc.inotify_add_watch(fd, abs_p.encode("utf-8"), IN_ALL_EVENTS)
                if wd >= 0:
                    wd_map[wd] = abs_p
                else:
                    log.debug("inotify_add_watch 失败 for %s", abs_p)

            if not wd_map:
                log.debug("没有有效监控路径")
                _libc.close(fd)
                self._running = False
                return

            ep = _select.epoll()
            ep.register(fd, _select.EPOLLIN)

            pending_moves: Dict[int, Tuple[str, str]] = {}  # cookie -> (old_path, base_path)

            while not self._stop.is_set():
                try:
                    events = ep.poll(timeout=self._interval)
                except OSError:
                    break

                for fd_, _ in events:
                    if fd_ != fd:
                        continue

                    try:
                        data = os.read(fd, 8192)
                    except OSError:
                        continue

                    pos = 0
                    while pos < len(data):
                        ev_size = 16
                        if pos + ev_size > len(data):
                            break
                        ev_data = data[pos:pos + ev_size]
                        wd = _struct.unpack("i", ev_data[0:4])[0]
                        mask = _struct.unpack("I", ev_data[4:8])[0]
                        cookie = _struct.unpack("I", ev_data[8:12])[0]
                        name_len = _struct.unpack("I", ev_data[12:16])[0]

                        is_dir = bool(mask & IN_ISDIR)

                        name_bytes = b""
                        if name_len > 0:
                            name_end = pos + ev_size + name_len
                            if name_end <= len(data):
                                name_bytes = data[pos + ev_size:name_end]
                            padded_len = ((name_len + 7) // 8) * 8
                            pos += ev_size + padded_len
                        else:
                            pos += ev_size

                        if wd not in wd_map:
                            continue

                        base_path = wd_map[wd]
                        file_name = name_bytes.rstrip(b"\x00").decode(
                            "utf-8", errors="replace"
                        ) if name_bytes else ""
                        full_path = (
                            os.path.join(base_path, file_name)
                            if file_name else base_path
                        )

                        old_path: str | None = None
                        ct: Optional[FileChangeType] = None

                        if mask & IN_CREATE:
                            ct = FileChangeType.CREATED
                        elif mask & IN_DELETE:
                            ct = FileChangeType.DELETED
                        elif mask & IN_MODIFY:
                            ct = FileChangeType.MODIFIED
                        elif mask & IN_ATTRIB:
                            ct = FileChangeType.ATTRIB
                        elif mask & IN_ACCESS:
                            ct = FileChangeType.ACCESS
                        elif mask & IN_MOVED_FROM:
                            pending_moves[cookie] = (full_path, base_path)
                            continue
                        elif mask & IN_MOVED_TO:
                            if cookie in pending_moves:
                                old_path, _ = pending_moves[cookie]
                                del pending_moves[cookie]
                                ct = FileChangeType.RENAMED
                            else:
                                ct = FileChangeType.MOVED_IN
                        else:
                            ct = FileChangeType.MODIFIED

                        try:
                            self._on_change(full_path, old_path, ct, is_dir)
                        except Exception as e:
                            log.debug("on_change 异常: %s", e)
        finally:
            try:
                ep.unregister(fd)
                ep.close()
            except Exception:
                pass
            _libc.close(fd)
            self._running = False


# ====================================================================
# FileDispatcher：主入口
# ====================================================================


class FileDispatcher:
    """文件系统监控与分发器。

    典型用法:
        >>> d = FileDispatcher(paths=["./src"])
        >>> d.subject.pipe(
        ...     ops.filter(lambda f: f.change_type == FileChangeType.MODIFIED),
        ... ).subscribe(on_next=lambda f: print("修改了:", f.path))
        >>> d.start()
        >>> d.stop()

    或作为上下文管理器:
        >>> with FileDispatcher(paths=["./src"]) as d:
        ...     d.subject.subscribe(on_next=print)

    构造参数:
        paths:             初始监控路径列表
        backend:           "auto" | "win32" | "inotify" | "polling"（默认 auto）
        change_types:      白名单；仅分发列出的 FileChangeType；None 表示全部
        tags:              默认附加的标签
        interval:          轮询或 IOCP 的检查间隔（秒），默认 0.5
    """

    __slots__ = (
        "_backend",
        "_subject",
        "_lock",
        "_paths",
        "_change_types_allowed",
        "_tags",
        "_interval",
        "_dispatch_count",
        "_error_count",
        "_backend_name",
        "_running",
    )

    def __init__(
        self,
        *,
        paths: Iterable[str] | None = None,
        backend: str = "auto",
        change_types: Iterable[FileChangeType] | None = None,
        tags: Iterable[str] = (),
        interval: float = 0.5,
    ) -> None:
        self._lock = threading.RLock()
        self._paths: List[str] = list(paths) if paths else []
        self._change_types_allowed: Optional[set] = (
            set(change_types) if change_types else None
        )
        self._tags: List[str] = list(tags or ())
        self._interval = max(0.02, float(interval))
        self._dispatch_count = 0
        self._error_count = 0

        # Subject：可直接 pipe(...).subscribe(...)
        self._subject: PublishSubject = PublishSubject()

        # 选择后端
        backend = (backend or "auto").lower()
        be: Optional[Any] = None
        self._backend_name = "polling"

        try:
            if backend in ("auto", "win32") and sys.platform == "win32":
                be = _Win32WatchBackend(
                    self._dispatch_once,
                    paths=self._paths,
                    change_types=self._change_types_allowed,
                    interval=self._interval,
                )
                self._backend_name = "win32"
            elif backend == "inotify" or (
                backend == "auto" and sys.platform.startswith("linux")
            ):
                be = _InotifyWatchBackend(
                    self._dispatch_once,
                    paths=self._paths,
                    change_types=self._change_types_allowed,
                    interval=self._interval,
                )
                self._backend_name = "inotify"
        except Exception as e:
            log.debug("平台后端不可用，回退到 polling: %s", e)
            be = None

        if be is None:
            be = _PollingWatchBackend(
                self._dispatch_once,
                paths=self._paths,
                change_types=self._change_types_allowed,
                interval=self._interval,
            )
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

    def __enter__(self) -> "FileDispatcher":
        self.start()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.stop()

    # ---- 路径管理 ----------------------------------------------------
    def add_path(self, path: str) -> None:
        """动态添加监控路径。"""
        path = os.path.abspath(path)
        with self._lock:
            if path not in self._paths:
                self._paths.append(path)
            if hasattr(self._backend, "add_path"):
                try:
                    self._backend.add_path(path)
                except Exception:
                    pass

    def remove_path(self, path: str) -> None:
        """动态移除监控路径。"""
        path = os.path.abspath(path)
        with self._lock:
            if path in self._paths:
                self._paths.remove(path)
            if hasattr(self._backend, "remove_path"):
                try:
                    self._backend.remove_path(path)
                except Exception:
                    pass

    # ---- 核心：一次分发 ----------------------------------------------
    def _dispatch_once(
        self,
        path: str,
        old_path: str | None,
        change_type: FileChangeType,
        is_directory: bool,
    ) -> None:
        if (
            self._change_types_allowed is not None
            and change_type not in self._change_types_allowed
        ):
            return

        try:
            size: int | None = None
            if not is_directory and os.path.exists(path):
                try:
                    size = os.path.getsize(path)
                except OSError:
                    pass

            fd = FileData.now(
                path=path,
                old_path=old_path,
                change_type=change_type,
                is_directory=is_directory,
                size=size,
                tags=self._tags,
            )
            self._subject.on_next(fd)
            self._dispatch_count += 1
        except Exception as e:
            log.debug("subject.on_next 异常: %s", e)
            self._error_count += 1


# ====================================================================
# 顶层工厂 & 操作符
# ====================================================================


def from_filesystem(
    *,
    paths: Iterable[str] | None = None,
    backend: str = "auto",
    change_types: Iterable[FileChangeType] | None = None,
    tags: Iterable[str] = (),
    interval: float = 0.5,
    auto_start: bool = True,
) -> Tuple[Any, FileDispatcher]:
    """顶层工厂函数：返回 (Observable[FileData], FileDispatcher) 二元组。

    Subject 拥有 pipe 方法，可直接链式组合响应式算子:
        >>> obs, d = from_filesystem(paths=["./src"])
        >>> obs.pipe(
        ...     ops.filter(lambda f: f.change_type == FileChangeType.MODIFIED),
        ... ).subscribe(on_next=lambda f: print("修改了:", f.path))
    """
    d = FileDispatcher(
        paths=paths,
        backend=backend,
        change_types=change_types,
        tags=tags,
        interval=interval,
    )
    if auto_start:
        d.start()

    # 包装 subject 为 Observable，便于链式调用
    inner_sub = d.subject

    def _subscribe_func(observer: Any) -> Subscription:
        if callable(observer):
            return inner_sub.subscribe(observer)
        return inner_sub.subscribe(lambda v: None)

    obs = Observable(_PyObservable(_subscribe_func))
    return obs, d


def write_to_filesystem(
    dispatcher: FileDispatcher,
    mode: str = "create",
) -> Callable[[Any], Any]:
    """响应式操作符：把上游每一项写入文件系统，并把构造的 FileData 继续下发。

    上游可接受:
        FileData  → 用其 path/change_type/metadata 写入
        str       → 作为文件路径（空内容写入）
        dict      → {"path", "content", "change_type", "tags", "metadata"}
        tuple/list→ (path, content) 或 (path, content, change_type)
    """

    def operator(source_observable: Any) -> Any:
        def subscribe(observer: Any) -> Subscription:
            def on_next(item: Any) -> None:
                try:
                    path: str = ""
                    content: str | bytes = ""
                    ct: FileChangeType = FileChangeType.MODIFIED
                    tags: List[str] = []
                    meta: Dict[str, Any] = {}

                    if isinstance(item, FileData):
                        path = item.path
                        ct = item.change_type
                        tags = list(item.tags)
                        meta = dict(item.metadata)
                    elif isinstance(item, str):
                        path = item
                    elif isinstance(item, dict):
                        path = item.get("path", "")
                        content = item.get("content", "")
                        ct = item.get("change_type", FileChangeType.MODIFIED)
                        tags = list(item.get("tags") or [])
                        meta = dict(item.get("metadata") or {})
                    elif isinstance(item, (list, tuple)):
                        items = list(item)
                        path = items[0] if items else ""
                        content = items[1] if len(items) > 1 else ""
                        if len(items) > 2:
                            ct = items[2]
                    else:
                        path = str(item)

                    if path:
                        try:
                            if mode == "append":
                                with open(path, "ab") as f:
                                    if isinstance(content, str):
                                        f.write(content.encode("utf-8"))
                                    else:
                                        f.write(bytes(content))
                            else:
                                with open(path, "wb") as f:
                                    if isinstance(content, str):
                                        f.write(content.encode("utf-8"))
                                    else:
                                        f.write(bytes(content))

                            write_ct = (
                                FileChangeType.CREATED
                                if mode == "create"
                                else FileChangeType.MODIFIED
                            )
                            fd = FileData.now(
                                path=path,
                                change_type=write_ct,
                                is_directory=False,
                                size=len(content) if content else 0,
                                tags=tags,
                                metadata=meta,
                            )
                            if callable(observer):
                                observer(fd)
                        except Exception as e:
                            log.debug("write_to_filesystem 写入异常: %s", e)
                            dispatcher._error_count += 1
                except Exception as e:
                    log.debug("write_to_filesystem operator 异常: %s", e)
                    dispatcher._error_count += 1

            return source_observable.subscribe(on_next=on_next)

        return Observable(_PyObservable(subscribe))

    return operator


# ====================================================================
# FileSubject：自包含 Dispatcher 的 Subject
# ====================================================================


class FileSubject:
    """一个带文件系统监控能力的 Subject。

    与普通 Subject 的区别:
      - 内部持有 FileDispatcher
      - 上下文管理器 (with)
      - 直接暴露 start/stop/add_path/remove_path
      - 支持 .pipe(...).subscribe(...)

    用法:
        >>> with FileSubject(paths=["./src"]) as fs:
        ...     fs.pipe(
        ...         ops.filter(lambda f: f.change_type == FileChangeType.MODIFIED),
        ...     ).subscribe(on_next=lambda f: print("修改了:", f.path))
    """

    __slots__ = ("_dispatcher", "_subject")

    def __init__(
        self,
        *,
        paths: Iterable[str] | None = None,
        backend: str = "auto",
        change_types: Iterable[FileChangeType] | None = None,
        tags: Iterable[str] = (),
        interval: float = 0.5,
        auto_start: bool = True,
    ) -> None:
        self._dispatcher: FileDispatcher = FileDispatcher(
            paths=paths,
            backend=backend,
            change_types=change_types,
            tags=tags,
            interval=interval,
        )
        self._subject: PublishSubject = self._dispatcher.subject
        if auto_start:
            self._dispatcher.start()

    @property
    def dispatcher(self) -> FileDispatcher:
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
        inner_sub = self._subject

        def _subscribe_func(observer: Any) -> Subscription:
            if callable(observer):
                return inner_sub.subscribe(observer)
            return inner_sub.subscribe(lambda v: None)

        result = Observable(_PyObservable(_subscribe_func))
        for op in operators:
            result = op(result)
        return result

    def start(self) -> None:
        self._dispatcher.start()

    def stop(self) -> None:
        self._dispatcher.stop()

    def __enter__(self) -> "FileSubject":
        self._dispatcher.start()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self._dispatcher.stop()

    def add_path(self, path: str) -> None:
        """动态添加监控路径。"""
        self._dispatcher.add_path(path)

    def remove_path(self, path: str) -> None:
        """动态移除监控路径。"""
        self._dispatcher.remove_path(path)


# ====================================================================
# FileObserver：按 FileChangeType 路由的便捷观察者
# ====================================================================


class FileObserver:
    """声明式地监听 FileSubject / FileDispatcher 发出的事件。

    用它代替手写 lambda + 类型判断。

    用法:
        >>> obs = FileObserver(
        ...     on_created=lambda fd: print("新建:", fd.path),
        ...     on_modified=lambda fd: print("修改:", fd.path),
        ...     on_deleted=lambda fd: print("删除:", fd.path),
        ...     on_renamed=lambda fd: print(f"{fd.old_path} -> {fd.path}"),
        ...     on_any=lambda fd: print("事件:", fd.change_type.name),
        ... )
        >>> obs.subscribe(fs)
    """

    __slots__ = (
        "_on_created",
        "_on_modified",
        "_on_deleted",
        "_on_renamed",
        "_on_moved_in",
        "_on_moved_out",
        "_on_access",
        "_on_attrib",
        "_on_any",
        "_on_error",
        "_on_completed",
        "_subscription",
    )

    def __init__(
        self,
        *,
        on_created: Callable[[FileData], Any] | None = None,
        on_modified: Callable[[FileData], Any] | None = None,
        on_deleted: Callable[[FileData], Any] | None = None,
        on_renamed: Callable[[FileData], Any] | None = None,
        on_moved_in: Callable[[FileData], Any] | None = None,
        on_moved_out: Callable[[FileData], Any] | None = None,
        on_access: Callable[[FileData], Any] | None = None,
        on_attrib: Callable[[FileData], Any] | None = None,
        on_any: Callable[[FileData], Any] | None = None,
        on_error: Callable[[Exception], Any] | None = None,
        on_completed: Callable[[], Any] | None = None,
    ) -> None:
        self._on_created = on_created
        self._on_modified = on_modified
        self._on_deleted = on_deleted
        self._on_renamed = on_renamed
        self._on_moved_in = on_moved_in
        self._on_moved_out = on_moved_out
        self._on_access = on_access
        self._on_attrib = on_attrib
        self._on_any = on_any
        self._on_error = on_error
        self._on_completed = on_completed
        self._subscription: Optional[Subscription] = None

    def _on_next(self, fd: FileData) -> None:
        if self._on_any is not None:
            try:
                self._on_any(fd)
            except Exception as e:
                log.debug("FileObserver.on_any 异常: %s", e)

        ct = fd.change_type
        handler = {
            FileChangeType.CREATED: self._on_created,
            FileChangeType.MODIFIED: self._on_modified,
            FileChangeType.DELETED: self._on_deleted,
            FileChangeType.RENAMED: self._on_renamed,
            FileChangeType.MOVED_IN: self._on_moved_in,
            FileChangeType.MOVED_OUT: self._on_moved_out,
            FileChangeType.ACCESS: self._on_access,
            FileChangeType.ATTRIB: self._on_attrib,
        }.get(ct, None)

        if handler is not None:
            try:
                handler(fd)
            except Exception as e:
                log.debug("FileObserver 回调 %s 异常: %s", ct.name, e)

    def _on_error_handler(self, err: Exception) -> None:
        if self._on_error is not None:
            try:
                self._on_error(err)
            except Exception as e:
                log.debug("FileObserver.on_error 异常: %s", e)

    def _on_completed_handler(self) -> None:
        if self._on_completed is not None:
            try:
                self._on_completed()
            except Exception as e:
                log.debug("FileObserver.on_completed 异常: %s", e)

    def subscribe(self, observable: Any) -> Optional[Subscription]:
        """订阅 Observable/Subject/FileSubject。返回 Subscription。"""
        self.unsubscribe()
        self._subscription = observable.subscribe(
            on_next=self._on_next,
            on_error=self._on_error_handler,
            on_completed=self._on_completed_handler,
        )
        return self._subscription

    def attach(self, subject_or_dispatcher: Any) -> "FileObserver":
        self.subscribe(subject_or_dispatcher)
        return self

    def unsubscribe(self) -> None:
        if self._subscription is not None:
            self._subscription = None

    @property
    def is_subscribed(self) -> bool:
        return self._subscription is not None

    def __enter__(self) -> "FileObserver":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.unsubscribe()


# ====================================================================
# 对外导出
# ====================================================================

__all__ = [
    "FileChangeType",
    "FileData",
    "FileDispatcher",
    "FileSubject",
    "FileObserver",
    "from_filesystem",
    "write_to_filesystem",
]
