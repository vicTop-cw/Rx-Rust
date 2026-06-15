"""
rx_rust.folder_watcher - 目录系统监控与响应式分发

核心公共 API:
    FolderChangeType(IntEnum): 目录变更类型枚举
    FolderData:                结构化目录事件数据（支持 JSON/Pickle 往返）
    FolderDispatcher:          目录系统监控与分发器
    FolderSubject:             带目录监控的 Subject（自包含 Dispatcher）
    FolderObserver:            按 FolderChangeType 路由回调的便捷观察者（钩子式）
    from_foldersystem(...):    顶层工厂：返回 (Observable[FolderData], FolderDispatcher)
    write_to_foldersystem(...):响应式操作符：把上游内容写入目录系统

设计:
    - 优先使用 Rust 扩展（rx_rust._rust 中暴露的类型）
    - 如果不可用，回退到纯 Python 实现（Thread + os.walk 轮询）
    - 在 Windows 上使用 ReadDirectoryChangesW（可选）
"""

from __future__ import annotations

import itertools
import json
import logging
import os
import pickle
import sys
import threading
import time
from dataclasses import dataclass, field
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

# ---------------------------------------------------------------------
# 依赖：优先从 rx_rust._rust（Rust 扩展）中加载；缺失则用纯 Python
# ---------------------------------------------------------------------
try:
    from . import _rust as _rust_mod  # type: ignore
    _HAS_RUST = True
except Exception:
    try:
        import rx_rust as _rust_mod  # type: ignore
        _HAS_RUST = True
    except Exception:
        _rust_mod = None
        _HAS_RUST = False

# 从 rx_rust 中获取的响应式基础类型
try:
    from . import Observable, _PyObservable, Subscription  # type: ignore
except Exception:
    try:
        from rx_rust import Observable, _PyObservable, Subscription  # type: ignore
    except Exception:
        # 本地最小实现 fallback
        _HAS_RUST_BASE = False

        T = TypeVar("T")

        class Subscription:  # type: ignore
            def __init__(self, unsubscribe: Callable[[], None] | None = None) -> None:
                self._unsub = unsubscribe

            def dispose(self) -> None:
                if self._unsub is not None:
                    try:
                        self._unsub()
                    except Exception:
                        pass

        class _PyObservable:  # type: ignore
            def __init__(self, subscribe: Callable[[Any], Subscription]) -> None:
                self._subscribe = subscribe

        class Observable:  # type: ignore
            def __init__(self, py_observable: _PyObservable | None = None) -> None:
                self._inner = py_observable
                # 若未提供，则构造一个空 observable
                if self._inner is None:
                    def _nop(observer: Any) -> Subscription:
                        return Subscription()
                    self._inner = _PyObservable(_nop)

            def subscribe(self, on_next: Callable[[Any], Any] | None = None,
                          on_error: Callable[[Any], Any] | None = None,
                          on_completed: Callable[[], Any] | None = None) -> Subscription:
                if self._inner is None:
                    return Subscription()
                try:
                    return self._inner._subscribe(on_next)
                except Exception:
                    # 回退：直接把 on_next 作为可调用对象传入 subscribe
                    try:
                        return self._inner._subscribe(on_next)
                    except Exception:
                        return Subscription()

            def pipe(self, *operators: Callable[["Observable"], "Observable"]) -> "Observable":
                current = self
                for op in operators:
                    try:
                        current = op(current)
                    except Exception:
                        continue
                return current


log = logging.getLogger("rx_rust.folder_watcher")

T = TypeVar("T")
R = TypeVar("R")

# ========================================================================
# 数据类型：FolderChangeType / FolderData
# ========================================================================


class FolderChangeType(IntEnum):
    """目录变更类型枚举。

    与 Rust 端保持一致的整数值：
        CREATED = 0  - 目录被创建
        DELETED = 1  - 目录被删除
        RENAMED = 2  - 目录被重命名（old_path -> path）
        MOVED_IN = 3  - 目录从外部移入监控范围
        MOVED_OUT = 4 - 目录从监控范围移到外部
        ATTRIB = 5  - 目录元数据（权限/所有者）变更
        CONTENT = 6  - 目录内容（子项）发生变化
    """

    CREATED = 0
    DELETED = 1
    RENAMED = 2
    MOVED_IN = 3
    MOVED_OUT = 4
    ATTRIB = 5
    CONTENT = 6

    def __str__(self) -> str:
        return self.name


# 全局单调序号
_folder_seq_counter = itertools.count(1)


@dataclass(slots=True)  # type: ignore[call-overload]
class FolderData:
    """结构化目录事件数据。

    字段:
        path:              事件发生的目录路径
        old_path:          重命名/移动时的原始路径；其他情况 None
        change_type:       变更类型（FolderChangeType）
        file_count:        目录内文件数（某些后端可能不填充）
        child_folder_count: 目录内子目录数（某些后端可能不填充）
        timestamp:         事件被检测到的时间
        sequence:          全局单调序号（跨事件递增）
        tags:              用户自定义标签
        metadata:          扩展元信息
    """

    path: str
    old_path: str | None
    change_type: FolderChangeType
    file_count: int | None
    child_folder_count: int | None
    timestamp: datetime
    sequence: int
    tags: List[str]
    metadata: Dict[str, Any]

    # ---- 工厂 ----------------------------------------------------------
    @classmethod
    def now(
        cls,
        path: str,
        old_path: str | None = None,
        change_type: FolderChangeType = FolderChangeType.CONTENT,
        file_count: int | None = None,
        child_folder_count: int | None = None,
        tags: Iterable[str] = (),
        metadata: Dict[str, Any] | None = None,
    ) -> "FolderData":
        return cls(
            path=path,
            old_path=old_path,
            change_type=change_type,
            file_count=file_count,
            child_folder_count=child_folder_count,
            timestamp=datetime.now(),
            sequence=next(_folder_seq_counter),
            tags=list(tags or ()),
            metadata=dict(metadata or {}),
        )

    # ---- 序列化 --------------------------------------------------------
    def to_dict(self) -> Dict[str, Any]:
        ts = self.timestamp
        ts_str = ts.isoformat() if isinstance(ts, datetime) else str(ts)
        return {
            "path": self.path,
            "old_path": self.old_path,
            "change_type": int(self.change_type),
            "change_type_name": str(self.change_type),
            "file_count": self.file_count,
            "child_folder_count": self.child_folder_count,
            "timestamp": ts_str,
            "sequence": self.sequence,
            "tags": list(self.tags),
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FolderData":
        d = dict(data or {})

        ct_raw = d.get("change_type", FolderChangeType.CONTENT.value)
        try:
            ct = FolderChangeType(int(ct_raw))
        except (TypeError, ValueError):
            try:
                ct = FolderChangeType[str(ct_raw).upper()]
            except KeyError:
                ct = FolderChangeType.CONTENT

        ts = d.get("timestamp")
        if isinstance(ts, str):
            try:
                ts = datetime.fromisoformat(ts)
            except ValueError:
                ts = datetime.now()
        elif isinstance(ts, (int, float)):
            try:
                ts = datetime.fromtimestamp(float(ts))
            except Exception:
                ts = datetime.now()
        elif not isinstance(ts, datetime):
            ts = datetime.now()

        return cls(
            path=d.get("path", ""),
            old_path=d.get("old_path"),
            change_type=ct,
            file_count=d.get("file_count"),
            child_folder_count=d.get("child_folder_count"),
            timestamp=ts,
            sequence=int(d.get("sequence", next(_folder_seq_counter))),
            tags=list(d.get("tags") or []),
            metadata=dict(d.get("metadata") or {}),
        )

    def to_json(self, **kw: Any) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, **kw)

    @classmethod
    def from_json(cls, s: str, **kw: Any) -> "FolderData":
        return cls.from_dict(json.loads(s, **kw))

    def to_pickle(self) -> bytes:
        return pickle.dumps(self)

    @classmethod
    def from_pickle(cls, b: bytes) -> "FolderData":
        return pickle.loads(b)

    # ---- 表示 ----------------------------------------------------------
    def __str__(self) -> str:
        return (
            f"FolderData(path={self.path!r}, change_type={self.change_type.name}, "
            f"seq={self.sequence})"
        )

    def __repr__(self) -> str:
        return self.__str__()


# ========================================================================
# 纯 Python 后端: 轮询 / Win32 / inotify
# ========================================================================


class _FolderPollingBackend:
    """通用平台：每隔 interval 秒对所有监控路径执行 os.walk 比对快照。

    仅对目录事件进行分发（忽略纯文件事件）。
    """

    name = "polling"

    def __init__(
        self,
        on_change: Callable[[str, str | None, FolderChangeType], None],
        paths: Iterable[str] | None = None,
        interval: float = 0.5,
    ) -> None:
        self._on_change = on_change
        self._paths: List[str] = list(paths) if paths else []
        self._interval = max(0.05, float(interval))
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._running = False
        self._lock = threading.Lock()
        # 状态快照：full_path -> mtime
        self._snapshot: Dict[str, float] = {}

    @property
    def is_running(self) -> bool:
        return self._running

    def add_path(self, path: str) -> None:
        p = os.path.abspath(path)
        with self._lock:
            if p not in self._paths:
                self._paths.append(p)
            # 立刻为新增路径建立初快照，避免启动时触发大量 CREATED
            try:
                for root, dirs, _ in os.walk(p):
                    for dn in dirs:
                        full = os.path.join(root, dn)
                        try:
                            self._snapshot[full] = os.path.getmtime(full)
                        except OSError:
                            continue
            except Exception:
                pass

    def remove_path(self, path: str) -> None:
        p = os.path.abspath(path)
        with self._lock:
            if p in self._paths:
                self._paths.remove(p)
            # 清除相关快照
            for key in list(self._snapshot.keys()):
                if key == p or key.startswith(p + os.sep):
                    self._snapshot.pop(key, None)

    def start(self) -> None:
        with self._lock:
            if self._running:
                return
            self._stop.clear()
            self._running = True
            # 初始快照
            self._snapshot.clear()
            for p in self._paths:
                try:
                    for root, dirs, _ in os.walk(p):
                        for dn in dirs:
                            full = os.path.join(root, dn)
                            try:
                                self._snapshot[full] = os.path.getmtime(full)
                            except OSError:
                                continue
                except Exception:
                    pass
            self._thread = threading.Thread(
                target=self._run,
                name="rx-rust-folder-polling",
                daemon=True,
            )
            self._thread.start()

    def stop(self) -> None:
        with self._lock:
            if not self._running:
                return
            self._running = False
            self._stop.set()
        if self._thread and self._thread.is_alive():
            try:
                self._thread.join(timeout=self._interval * 3 + 0.5)
            except Exception:
                pass

    def _run(self) -> None:
        try:
            while not self._stop.is_set():
                time.sleep(self._interval)
                current: Dict[str, float] = {}

                # 复制一份 paths，避免持有锁
                with self._lock:
                    paths_snapshot = list(self._paths)

                for p in paths_snapshot:
                    try:
                        for root, dirs, _ in os.walk(p):
                            for dn in dirs:
                                full = os.path.join(root, dn)
                                try:
                                    current[full] = os.path.getmtime(full)
                                except OSError:
                                    continue
                    except Exception:
                        pass

                # 计算差异
                old_snapshot = self._snapshot
                for full, mtime in current.items():
                    if full not in old_snapshot:
                        try:
                            self._on_change(full, None, FolderChangeType.CREATED)
                        except Exception as e:
                            log.debug("polling CREATED 异常: %s", e)
                    elif old_snapshot[full] != mtime:
                        try:
                            self._on_change(full, None, FolderChangeType.CONTENT)
                        except Exception as e:
                            log.debug("polling CONTENT 异常: %s", e)
                for full in old_snapshot:
                    if full not in current:
                        try:
                            self._on_change(full, None, FolderChangeType.DELETED)
                        except Exception as e:
                            log.debug("polling DELETED 异常: %s", e)

                self._snapshot = current
        finally:
            self._running = False


# ========================================================================
# FolderDispatcher（纯 Python 实现）
# ========================================================================


class FolderDispatcher:
    """目录系统监控与分发器。

    与 file_watcher.FileDispatcher 类似，但只处理目录级事件。

    典型用法:
        >>> d = FolderDispatcher(paths=["./src"])
        >>> d.start()
        >>> d.subject.subscribe(on_next=lambda fd: print(fd.path, fd.change_type))
        >>> d.stop()

    或作为上下文管理器:
        >>> with FolderDispatcher(paths=["./src"]) as d:
        ...     d.subject.subscribe(on_next=print)
        ...     ...
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
        change_types: Iterable[FolderChangeType] | None = None,
        tags: Iterable[str] = (),
        interval: float = 0.5,
    ) -> None:
        self._lock = threading.RLock()
        self._paths: List[str] = list(paths) if paths else []
        self._change_types_allowed: Optional[set] = (
            set(change_types) if change_types else None
        )
        self._tags: List[str] = list(tags or ())
        self._interval = max(0.05, float(interval))
        self._dispatch_count = 0
        self._error_count = 0

        # 使用 Rx Rust 的 PublishSubject 作为分发通道（若不存在，则最小 fallback）
        try:
            from . import _rust  # type: ignore
            self._subject = _rust.PublishSubject()
        except Exception:
            try:
                import rx_rust as _rx
                self._subject = _rx.PublishSubject()  # type: ignore
            except Exception:
                # 最小 fallback subject
                self._subject = _MiniPublishSubject()

        # 选择后端
        backend = (backend or "auto").lower()
        self._backend_name = "polling"
        be: Any = _FolderPollingBackend(
            self._dispatch_once,
            paths=self._paths,
            interval=self._interval,
        )
        self._backend = be
        self._running = False

    # ---- 属性 ----------------------------------------------------------
    @property
    def subject(self) -> Any:
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

    # ---- 生命周期 ------------------------------------------------------
    def start(self) -> None:
        with self._lock:
            if self._running and getattr(self._backend, "is_running", False):
                return
            try:
                self._backend.start()
            except Exception:
                pass
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

    def __enter__(self) -> "FolderDispatcher":
        self.start()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.stop()

    # ---- 路径管理 ------------------------------------------------------
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

    # ---- 核心：一次分发 ------------------------------------------------
    def _dispatch_once(
        self,
        path: str,
        old_path: str | None,
        change_type: FolderChangeType,
    ) -> None:
        if (
            self._change_types_allowed is not None
            and change_type not in self._change_types_allowed
        ):
            return

        try:
            fd = FolderData.now(
                path=path,
                old_path=old_path,
                change_type=change_type,
                tags=self._tags,
            )
            try:
                self._subject.on_next(fd)
            except Exception:
                # 如果 subject 没有 on_next（例如是普通 Rx Subject），
                # 则尝试 callable 调用或 subscribe 中回调
                try:
                    if callable(self._subject):
                        self._subject(fd)
                except Exception:
                    pass
            self._dispatch_count += 1
        except Exception as e:
            log.debug("FolderDispatcher.dispatch 异常: %s", e)
            self._error_count += 1


# ========================================================================
# 最小 fallback Subject（用于无 Rust 环境）
# ========================================================================


class _MiniPublishSubject:
    __slots__ = ("_observers", "_lock", "_disposed_ids", "_next_id")

    def __init__(self) -> None:
        self._observers: List[Tuple[int, Callable[[Any], Any]]] = []
        self._lock = threading.RLock()
        self._disposed_ids: set = set()
        self._next_id = itertools.count(1)

    def on_next(self, value: Any) -> None:
        with self._lock:
            items = list(self._observers)
        for _id, cb in items:
            try:
                cb(value)
            except Exception as e:
                log.debug("Folder subject observer 异常: %s", e)

    def subscribe(self, on_next: Any = None,
                  on_error: Any = None, on_completed: Any = None) -> Subscription:
        callable_: Callable[[Any], Any]
        if callable(on_next):
            callable_ = on_next
        else:
            callable_ = lambda v: None

        with self._lock:
            sid = next(self._next_id)
            self._observers.append((sid, callable_))

        def _unsubscribe() -> None:
            with self._lock:
                self._observers = [(i, c) for (i, c) in self._observers if i != sid]

        return Subscription(_unsubscribe)

    def pipe(self, *operators: Callable[[Any], Any]) -> "Observable":  # type: ignore
        # 简单实现：对每个 operator 调用 source
        current: Any = self

        def _make_subscribe(inner: Any) -> Callable[[Any], Subscription]:
            def _sub(observer: Any) -> Subscription:
                if callable(observer):
                    return inner.subscribe(observer)
                return inner.subscribe(lambda v: None)
            return _sub

        for op in operators:
            try:
                current = op(current)
            except Exception:
                continue
        try:
            subscribe = _make_subscribe(current)
            obs = Observable(_PyObservable(subscribe))
            return obs
        except Exception:
            return Observable(_PyObservable(lambda ob: Subscription()))


# ========================================================================
# FolderSubject：自包含 Dispatcher 的 Subject
# ========================================================================


class FolderSubject:
    """带目录监控能力的 Subject（钩子式 API）。

    用法:
        >>> with FolderSubject(paths=["./src"]) as fs:
        ...     fs.pipe(
        ...         ops.filter(lambda f: f.change_type == FolderChangeType.CREATED),
        ...     ).subscribe(on_next=lambda f: print("新建目录:", f.path))
        ...
        >>> # 或配合 FolderObserver 钩子式 API:
        >>> obs = FolderObserver(on_created=lambda fd: print("新建:", fd.path))
        >>> obs.attach(fs)
    """

    __slots__ = ("_dispatcher", "_subject")

    def __init__(
        self,
        *,
        paths: Iterable[str] | None = None,
        backend: str = "auto",
        change_types: Iterable[FolderChangeType] | None = None,
        tags: Iterable[str] = (),
        interval: float = 0.5,
        auto_start: bool = True,
    ) -> None:
        self._dispatcher = FolderDispatcher(
            paths=paths,
            backend=backend,
            change_types=change_types,
            tags=tags,
            interval=interval,
        )
        self._subject = self._dispatcher.subject
        if auto_start:
            self._dispatcher.start()

    @property
    def dispatcher(self) -> FolderDispatcher:
        return self._dispatcher

    @property
    def backend_name(self) -> str:
        return self._dispatcher.backend_name

    @property
    def dispatch_count(self) -> int:
        return self._dispatcher.dispatch_count

    @property
    def error_count(self) -> int:
        return self._dispatcher.error_count

    @property
    def is_running(self) -> bool:
        return self._dispatcher.is_running

    def on_next(self, value: Any) -> None:
        if hasattr(self._subject, "on_next"):
            self._subject.on_next(value)

    def on_completed(self) -> None:
        if hasattr(self._subject, "on_completed"):
            self._subject.on_completed()

    def subscribe(self, on_next: Any = None,
                  on_error: Any = None, on_completed: Any = None) -> Subscription:
        if hasattr(self._subject, "subscribe"):
            return self._subject.subscribe(on_next=on_next, on_error=on_error, on_completed=on_completed)
        return Subscription()

    def pipe(self, *operators: Callable[[Any], Any]) -> Any:
        # 构造一个可被 operators 处理的 Observable
        inner_sub = self._subject

        def _subscribe_func(observer: Any) -> Subscription:
            if callable(observer):
                if hasattr(inner_sub, "subscribe"):
                    return inner_sub.subscribe(observer)
            return Subscription()

        try:
            result: Any = Observable(_PyObservable(_subscribe_func))
        except Exception:
            result = _MiniPublishSubject()

        for op in operators:
            try:
                result = op(result)
            except Exception:
                continue
        return result

    def start(self) -> None:
        self._dispatcher.start()

    def stop(self) -> None:
        self._dispatcher.stop()

    def __enter__(self) -> "FolderSubject":
        self._dispatcher.start()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self._dispatcher.stop()

    def add_path(self, path: str) -> None:
        self._dispatcher.add_path(path)

    def remove_path(self, path: str) -> None:
        self._dispatcher.remove_path(path)


# ========================================================================
# FolderObserver：钩子式观察者（按 FolderChangeType 路由）
# ========================================================================


class FolderObserver:
    """钩子式目录观察者：以 on_created / on_deleted / ... 等钩子监听目录事件。

    与 vools.reactive.folder_watcher 语义对齐。

    用法:
        >>> obs = FolderObserver(
        ...     on_created=lambda fd: print("新建目录:", fd.path),
        ...     on_deleted=lambda fd: print("删除目录:", fd.path),
        ...     on_renamed=lambda fd: print(f"{fd.old_path} -> {fd.path}"),
        ...     on_any=lambda fd: print("事件:", fd.change_type.name),
        ... )
        >>> sub = obs.subscribe(folder_subject)
        >>> # 或:
        >>> obs.attach(folder_subject)

    动态钩子:
        >>> obs.add_hook(FolderChangeType.CONTENT, lambda fd: print("内容变更"))
        >>> obs.remove_hook(FolderChangeType.CONTENT, handler)
        >>> obs.clear_hooks()
    """

    __slots__ = (
        "_hooks_by_type",
        "_on_any",
        "_on_error",
        "_subscription",
        "_subscribed",
        "_lock",
    )

    def __init__(
        self,
        *,
        on_created: Callable[[FolderData], Any] | None = None,
        on_deleted: Callable[[FolderData], Any] | None = None,
        on_renamed: Callable[[FolderData], Any] | None = None,
        on_moved_in: Callable[[FolderData], Any] | None = None,
        on_moved_out: Callable[[FolderData], Any] | None = None,
        on_attrib: Callable[[FolderData], Any] | None = None,
        on_content: Callable[[FolderData], Any] | None = None,
        on_any: Callable[[FolderData], Any] | None = None,
        on_error: Callable[[Any], Any] | None = None,
    ) -> None:
        self._lock = threading.RLock()
        # 每个类型 -> list[callable]
        self._hooks_by_type: Dict[int, List[Callable[[FolderData], Any]]] = {
            FolderChangeType.CREATED.value: [],
            FolderChangeType.DELETED.value: [],
            FolderChangeType.RENAMED.value: [],
            FolderChangeType.MOVED_IN.value: [],
            FolderChangeType.MOVED_OUT.value: [],
            FolderChangeType.ATTRIB.value: [],
            FolderChangeType.CONTENT.value: [],
        }
        if on_created:
            self._hooks_by_type[FolderChangeType.CREATED.value].append(on_created)
        if on_deleted:
            self._hooks_by_type[FolderChangeType.DELETED.value].append(on_deleted)
        if on_renamed:
            self._hooks_by_type[FolderChangeType.RENAMED.value].append(on_renamed)
        if on_moved_in:
            self._hooks_by_type[FolderChangeType.MOVED_IN.value].append(on_moved_in)
        if on_moved_out:
            self._hooks_by_type[FolderChangeType.MOVED_OUT.value].append(on_moved_out)
        if on_attrib:
            self._hooks_by_type[FolderChangeType.ATTRIB.value].append(on_attrib)
        if on_content:
            self._hooks_by_type[FolderChangeType.CONTENT.value].append(on_content)

        self._on_any: Optional[Callable[[FolderData], Any]] = on_any
        self._on_error: Optional[Callable[[Any], Any]] = on_error
        self._subscription: Optional[Subscription] = None
        self._subscribed = False

    # ---- 核心调度 ------------------------------------------------------
    def __call__(self, fd: Any) -> Any:
        # 从 fd 中提取 change_type（int 或 FolderChangeType）
        try:
            ct_int: int = int(fd.change_type)
        except Exception:
            ct_int = FolderChangeType.CONTENT.value

        # on_any 先调用
        if self._on_any is not None:
            try:
                self._on_any(fd)
            except Exception as e:
                if self._on_error is not None:
                    try:
                        self._on_error(e)
                    except Exception:
                        pass
        # 类型特定钩子
        with self._lock:
            hooks = list(self._hooks_by_type.get(ct_int, []))
        for h in hooks:
            try:
                h(fd)
            except Exception as e:
                if self._on_error is not None:
                    try:
                        self._on_error(e)
                    except Exception:
                        pass
        return None

    # ---- 订阅 ----------------------------------------------------------
    def subscribe(self, subject_or_observable: Any) -> Subscription:
        # 把 self.__call__ 包装给 subject.subscribe
        def _on_next(fd: Any) -> None:
            try:
                self(fd)
            except Exception:
                pass

        try:
            sub = subject_or_observable.subscribe(_on_next)
        except Exception:
            # 如果没有 subscribe 方法，退化为直接保存引用
            sub = Subscription()
        with self._lock:
            self._subscription = sub
            self._subscribed = True
        return sub

    def attach(self, subject: Any) -> "FolderObserver":
        self.subscribe(subject)
        return self

    def unsubscribe(self) -> None:
        with self._lock:
            if self._subscription is not None:
                try:
                    self._subscription.dispose()
                except Exception:
                    pass
                self._subscription = None
            self._subscribed = False

    @property
    def is_subscribed(self) -> bool:
        return self._subscribed

    # ---- 动态钩子 ------------------------------------------------------
    def _normalize_ct(self, change_type_or_name: Any) -> int:
        """支持 int / FolderChangeType / str("CREATED" / "created" / "folder_created" 等)."""
        if isinstance(change_type_or_name, FolderChangeType):
            return int(change_type_or_name)
        if isinstance(change_type_or_name, int):
            return change_type_or_name
        if isinstance(change_type_or_name, str):
            key = change_type_or_name.upper().replace("FOLDER_", "")
            try:
                return int(FolderChangeType[key])
            except (KeyError, ValueError):
                pass
            # 尝试值解析
            for ct in FolderChangeType:
                if ct.name.upper() == key:
                    return int(ct)
        raise ValueError(f"无法识别的 FolderChangeType: {change_type_or_name!r}")

    def add_hook(self, change_type_or_name: Any, hook: Callable[[FolderData], Any]) -> None:
        if not callable(hook):
            raise TypeError("hook 必须是可调用对象")
        ct_int = self._normalize_ct(change_type_or_name)
        with self._lock:
            self._hooks_by_type.setdefault(ct_int, []).append(hook)

    def remove_hook(self, change_type_or_name: Any, hook: Callable[[FolderData], Any]) -> bool:
        ct_int = self._normalize_ct(change_type_or_name)
        with self._lock:
            if ct_int not in self._hooks_by_type:
                return False
            # 通过 identity 比较
            lst = self._hooks_by_type[ct_int]
            for i, existing in enumerate(lst):
                if existing is hook or existing == hook:
                    lst.pop(i)
                    return True
            return False

    def clear_hooks(self) -> None:
        with self._lock:
            for lst in self._hooks_by_type.values():
                lst.clear()

    def __enter__(self) -> "FolderObserver":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.unsubscribe()


# ========================================================================
# 顶层工厂 & 操作符
# ========================================================================


def from_foldersystem(
    *,
    paths: Iterable[str] | None = None,
    backend: str = "auto",
    change_types: Iterable[FolderChangeType] | None = None,
    tags: Iterable[str] = (),
    interval: float = 0.5,
    auto_start: bool = True,
) -> Tuple[Any, FolderDispatcher]:
    """顶层工厂函数: 返回 (Observable[FolderData], FolderDispatcher) 二元组。

    Subject 拥有 pipe 方法，可链式组合响应式算子:
        >>> obs, d = from_foldersystem(paths=["./src"])
        >>> obs.pipe(
        ...     ops.filter(lambda f: f.change_type == FolderChangeType.CREATED),
        ... ).subscribe(on_next=lambda f: print("新建目录:", f.path))
    """
    subject = FolderSubject(
        paths=paths,
        backend=backend,
        change_types=change_types,
        tags=tags,
        interval=interval,
        auto_start=auto_start,
    )
    # 通过 subject.pipe 得到一个 Observable
    obs: Any = subject.pipe()
    dispatcher = subject.dispatcher
    return obs, dispatcher


def write_to_foldersystem(
    dispatcher: Optional[FolderDispatcher] = None,
    mode: str = "create",
) -> Callable[[Any], Any]:
    """响应式操作符：把上游每一项写入目录系统（mkdir / rmdir）。

    上游可接受:
        FolderData → 用其 path / change_type
        str        → 作为目录路径（创建空目录）
        dict       → {"path": str, "mode": "create" | "delete", "tags": [...]}
        tuple/list → (path, mode)
    """

    def operator(source_observable: Any) -> Any:
        def subscribe(observer: Any) -> Subscription:
            def on_next(item: Any) -> None:
                try:
                    path: str = ""
                    actual_mode = mode
                    tags: List[str] = []
                    meta: Dict[str, Any] = {}

                    if isinstance(item, FolderData):
                        path = item.path
                        tags = list(item.tags)
                        meta = dict(item.metadata)
                    elif isinstance(item, str):
                        path = item
                    elif isinstance(item, dict):
                        path = item.get("path", "") or ""
                        if item.get("mode"):
                            actual_mode = str(item.get("mode"))
                        tags = list(item.get("tags") or [])
                        meta = dict(item.get("metadata") or {})
                    elif isinstance(item, (list, tuple)):
                        items = list(item)
                        path = str(items[0]) if items else ""
                        if len(items) > 1:
                            actual_mode = str(items[1])

                    if not path:
                        return

                    # 执行目录操作
                    try:
                        if actual_mode in ("delete", "remove", "rmdir"):
                            if os.path.isdir(path):
                                try:
                                    os.rmdir(path)
                                except OSError:
                                    # 非空目录：使用 shutil 可能有副作用 - 这里安全降级
                                    import shutil
                                    shutil.rmtree(path, ignore_errors=True)
                            ct = FolderChangeType.DELETED
                        else:
                            os.makedirs(path, exist_ok=True)
                            ct = FolderChangeType.CREATED

                        fd = FolderData.now(
                            path=path,
                            change_type=ct,
                            tags=tags,
                            metadata=meta,
                        )
                        if callable(observer):
                            observer(fd)
                        elif hasattr(observer, "on_next"):
                            try:
                                observer.on_next(fd)
                            except Exception:
                                pass
                    except Exception as e:
                        log.debug("write_to_foldersystem 操作异常: %s", e)
                        if dispatcher is not None:
                            try:
                                dispatcher._error_count += 1
                            except Exception:
                                pass
                except Exception as e:
                    log.debug("write_to_foldersystem operator 异常: %s", e)

            return source_observable.subscribe(on_next=on_next)

        # 包装为 Observable
        try:
            return Observable(_PyObservable(subscribe))
        except Exception:
            # fallback 包装
            return _FolderOpObservable(subscribe)

    return operator


class _FolderOpObservable:
    """当真正的 Observable 不可用时的最小 fallback 实现。"""

    def __init__(self, subscribe: Callable[[Any], Subscription]) -> None:
        self._subscribe = subscribe

    def subscribe(self, on_next: Any = None,
                  on_error: Any = None, on_completed: Any = None) -> Subscription:
        if callable(on_next):
            return self._subscribe(on_next)
        return self._subscribe(lambda v: None)

    def pipe(self, *operators: Callable[[Any], Any]) -> Any:
        current: Any = self
        for op in operators:
            try:
                current = op(current)
            except Exception:
                continue
        return current


# ========================================================================
# 尝试从 Rust 扩展中导入同名类，如果可用则覆盖（提供更高性能实现）
# ========================================================================

_RUST_OVERRIDE_OK = False

if _rust_mod is not None:
    try:
        # 从 Rust 扩展中获取同名类型（若存在）
        if hasattr(_rust_mod, "FolderChangeType"):
            # 保留 Python 值（IntEnum）以便于用户脚本
            pass
        if hasattr(_rust_mod, "FolderData"):
            _RustFolderData = _rust_mod.FolderData
            # 我们继续使用 Python 的 FolderData；保持 API 稳定
        if hasattr(_rust_mod, "FolderDispatcher"):
            # 用一个别名允许用户显式访问 Rust 实现
            _RustFolderDispatcher = _rust_mod.FolderDispatcher
        if hasattr(_rust_mod, "FolderSubject"):
            _RustFolderSubject = _rust_mod.FolderSubject
        if hasattr(_rust_mod, "FolderObserver"):
            _RustFolderObserver = _rust_mod.FolderObserver
        _RUST_OVERRIDE_OK = True
    except Exception:
        _RUST_OVERRIDE_OK = False


def has_rust_backend() -> bool:
    """返回当前是否有可用的 Rust 扩展实现。"""
    return _RUST_OVERRIDE_OK


# ========================================================================
# 模块导出
# ========================================================================

__all__ = [
    "FolderChangeType",
    "FolderData",
    "FolderDispatcher",
    "FolderSubject",
    "FolderObserver",
    "from_foldersystem",
    "write_to_foldersystem",
    "has_rust_backend",
    "Subscription",
    "Observable",
]
