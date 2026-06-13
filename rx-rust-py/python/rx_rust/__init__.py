"""Rx-Rust - Reactive Extensions for Python powered by Rust.

Rx-Rust 是一个用于组合异步和基于事件的程序的 Python 库，灵感来自微软的 Reactive Extensions (Rx) 库。
它建立在 Rust 之上，通过 PyO3 提供高性能的响应式编程体验。

核心概念：
    Observable  - 在未来可能发射 0 个或多个值的程序抽象
    Observer    - 订阅者，接收值的接收者
    Operator    - 转换和组合 Observable 的操作符
    Subscription - 订阅句柄，用来取消订阅
    Subject     - 既是 Observable 又是 Observer，可以手动发射值

快速开始：
    >>> import rx_rust
    >>> result = []
    >>> Rx-Rust.Observable.from_iter([1, 2, 3, 4, 5]) \
    ...     .filter(lambda x: x % 2 == 0) \
    ...     .map(lambda x: x * 10) \
    ...     .subscribe(on_next=lambda v: result.append(v))
    <Rx-Rust.Subscription object>
    >>> result
    [20, 40]

模块结构：
    Observable             - 可观察对象（核心类）
    PublishSubject         - 广播型主题（事件总线）
    BehaviorSubject        - 带当前值的主题
    ReplaySubject          - 重放历史值主题
    CurrentThreadScheduler - 当前线程调度器（同步）
    ThreadPoolScheduler    - 线程池调度器（并发）
    AsyncScheduler         - 异步调度器
    ImmediateScheduler     - 立即调度器
    Subscription           - 订阅句柄

版本：0.1.0
许可：MIT
"""

from __future__ import annotations

import time
import threading as _threading
from typing import Any, Callable, Iterable, List, Optional

# ============================================================================
# 尝试加载 Rust 扩展
# ============================================================================

_USE_RUST = False

try:
    from . import rx_rust as _rust_mod  # type: ignore
    _USE_RUST = True
except (ImportError, AttributeError):
    # 纯 Python 回退实现
    _USE_RUST = False


# ============================================================================
# Subscription - 订阅句柄
# ============================================================================

class Subscription:
    """表示对某个 Observable 的订阅句柄。

    你可以通过调用 ``dispose()`` 来取消订阅。
    一旦取消后，Observer 将不再接收任何值。

    典型用法：
        >>> sub = observable.subscribe(on_next=lambda x: print(x))
        >>> # ... 之后
        >>> sub.dispose()  # 取消订阅
        >>> sub.is_disposed()
        True

    注意事项：
        Subscription 是不可重放的 —— 一旦取消后不能恢复；
        多次调用 dispose() 是安全的（不会报错）。
    """

    def __init__(self, inner=None):
        """创建一个新的 Subscription。

        Args:
            inner: 从底层实现返回的原始订阅对象。
        """
        self._inner = inner
        self._disposed = False

    def dispose(self):
        """取消订阅。

        调用后 Observer 将不再接收值。
        这是一个幂等操作，多次调用安全。

        返回:
            None

        示例:
            >>> sub = Observable.from_iter([1, 2, 3]).subscribe(on_next=print)
            >>> sub.dispose()  # 取消订阅
        """
        self._disposed = True
        if self._inner is not None and hasattr(self._inner, "dispose"):
            try:
                self._inner.dispose()
            except Exception:
                pass

    def is_disposed(self):
        """检查订阅是否已被取消。

        返回:
            bool: 如果订阅已取消则返回 True，否则返回 False。

        示例:
            >>> sub = Observable.of(42).subscribe(on_next=lambda x: None)
            >>> sub.is_disposed()
            False
            >>> sub.dispose()
            >>> sub.is_disposed()
            True
        """
        if self._inner is not None and hasattr(self._inner, "is_disposed"):
            try:
                return bool(self._inner.is_disposed())
            except Exception:
                pass
        return self._disposed

    def __enter__(self):
        """进入 with 上下文。"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """退出 with 上下文时自动取消订阅。"""
        self.dispose()
        return False

    def __repr__(self):
        return f"Subscription(disposed={self.is_disposed()})"


# ============================================================================
# Observable - 纯 Python 实现
# ============================================================================

class _PyObservable:
    """纯 Python 版 Observable。"""

    def __init__(self, subscribe_fn: Callable[[Callable[[Any], None]], Subscription],
                 ops=None):
        self._subscribe = subscribe_fn
        self._ops = ops or []  # 延迟操作链

    # ---------- 操作链管理 ----------
    def _add_op(self, op_type, *args, **kwargs):
        """添加一个操作到链中，返回新的 _PyObservable（不立即创建订阅包装）。"""
        new_ops = self._ops + [(op_type, args, kwargs)]
        return _PyObservable(self._subscribe, new_ops)

    @staticmethod
    def of(*values):
        if len(values) == 0:
            raise TypeError("of() requires at least 1 argument")
        vals = list(values)

        def _sub(observer):
            for v in vals:
                observer(v)
            return Subscription()
        return _PyObservable(_sub)

    @staticmethod
    def from_iter(values):
        vals = list(values)

        def _sub(observer):
            for v in vals:
                observer(v)
            return Subscription()
        return _PyObservable(_sub)

    @staticmethod
    def range(start, count):
        start = int(start)
        count = int(count)

        def _sub(observer):
            for i in range(count):
                observer(start + i)
            return Subscription()
        return _PyObservable(_sub)

    @staticmethod
    def repeat(value, count):
        count = int(count)

        def _sub(observer):
            for _ in range(count):
                observer(value)
            return Subscription()
        return _PyObservable(_sub)

    @staticmethod
    def empty():
        def _sub(observer):
            return Subscription()
        return _PyObservable(_sub)

    @staticmethod
    def never():
        def _sub(observer):
            return Subscription()
        return _PyObservable(_sub)

    def subscribe(self, on_next, on_error=None, on_completed=None):
        """订阅，自动构建操作链并处理错误。"""
        if self._ops:
            def build_observer(ops, final_observer, _on_error):
                if not ops:
                    return final_observer
                op_type, args, kwargs = ops[0]
                rest = ops[1:]
                # 递归构建下一层 observer（只构建一次），避免计数器被重置
                next_observer = build_observer(rest, final_observer, _on_error)

                if op_type == 'map':
                    mapper = args[0]

                    def wrapped(value):
                        try:
                            result = mapper(value)
                            next_observer(result)
                        except Exception as e:
                            if _on_error:
                                _on_error(e)
                            else:
                                raise

                    return wrapped
                elif op_type == 'filter':
                    predicate = args[0]

                    def wrapped(value):
                        try:
                            if predicate(value):
                                next_observer(value)
                        except Exception as e:
                            if _on_error:
                                _on_error(e)
                            else:
                                raise

                    return wrapped
                elif op_type == 'take':
                    n = args[0]
                    taken = [0]

                    def wrapped(value):
                        if taken[0] < n:
                            taken[0] += 1
                            next_observer(value)

                    return wrapped
                elif op_type == 'skip':
                    n = args[0]
                    skipped = [0]

                    def wrapped(value):
                        if skipped[0] < n:
                            skipped[0] += 1
                            return
                        next_observer(value)

                    return wrapped
                elif op_type == 'first':
                    taken = [0]

                    def wrapped(value):
                        if taken[0] < 1:
                            taken[0] += 1
                            next_observer(value)

                    return wrapped
                elif op_type == 'do_on_next':
                    action = args[0]

                    def wrapped(value):
                        try:
                            action(value)
                        except Exception as e:
                            if _on_error:
                                _on_error(e)
                        next_observer(value)

                    return wrapped
                # 未知操作类型 — 传递给下一个 observer
                return final_observer

            observer = build_observer(self._ops, on_next, on_error)
            return self._subscribe(observer)
        else:
            return self._subscribe(on_next)

    # ---------- pipe ----------
    def pipe(self, *operators):
        """管道操作符，每个 operator 是 (Observable) -> Observable 的函数。"""
        result = self
        for op in operators:
            result = op(result)
        return result

    # ---------- 转换操作符（_add_op 延迟构建） ----------
    def map(self, mapper):
        return self._add_op('map', mapper)

    def filter(self, predicate):
        return self._add_op('filter', predicate)

    # ---------- 过滤操作符（_add_op 延迟构建） ----------
    def take(self, n):
        return self._add_op('take', int(n))

    def skip(self, n):
        return self._add_op('skip', int(n))

    def first(self):
        return self._add_op('first')

    def do_on_next(self, action):
        return self._add_op('do_on_next', action)

    def last(self):
        def _sub(observer):
            last_value = [None]
            have_value = [False]

            def wrapped(value):
                last_value[0] = value
                have_value[0] = True
            self.subscribe(wrapped)
            if have_value[0]:
                observer(last_value[0])
            return Subscription()
        return _PyObservable(_sub)

    # ---------- 聚合操作符 ----------
    def count(self):
        def _sub(observer):
            counter = [0]

            def wrapped(value):
                counter[0] += 1
            self.subscribe(wrapped)
            observer(counter[0])
            return Subscription()
        return _PyObservable(_sub)

    def sum(self):
        def _sub(observer):
            total = [0]
            has_value = [False]

            def wrapped(value):
                total[0] = total[0] + value
                has_value[0] = True
            self.subscribe(wrapped)
            observer(total[0])
            return Subscription()

        return _PyObservable(_sub)

    def reduce(self, initial, reducer):
        def _sub(observer):
            acc = [initial]

            def wrapped(value):
                try:
                    acc[0] = reducer(acc[0], value)
                except Exception:
                    raise

            self.subscribe(wrapped)
            observer(acc[0])
            return Subscription()
        return _PyObservable(_sub)

    def scan(self, initial, scanner):
        def _sub(observer):
            acc = [initial]
            observer(acc[0])

            def wrapped(value):
                try:
                    acc[0] = scanner(acc[0], value)
                except Exception:
                    raise
                observer(acc[0])

            self.subscribe(wrapped)
            return Subscription()
        return _PyObservable(_sub)

    def flat_map(self, mapper):
        def _sub(observer):
            def wrapped(value):
                inner = mapper(value)
                # 兼容：薄包装 Observable 或 _PyObservable
                if hasattr(inner, "subscribe"):
                    inner.subscribe(on_next=lambda v: observer(v))
                elif hasattr(inner, "_subscribe"):
                    inner._subscribe(observer)
                else:
                    try:
                        for item in inner:
                            observer(item)
                    except TypeError:
                        observer(inner)
            return self.subscribe(wrapped)
        return _PyObservable(_sub)

    def start_with(self, *values):
        def _sub(observer):
            for v in values:
                observer(v)
            return self.subscribe(observer)
        return _PyObservable(_sub)

    def default_if_empty(self, default):
        def _sub(observer):
            emitted = [False]

            def wrapped(value):
                emitted[0] = True
                observer(value)
            self.subscribe(wrapped)
            if not emitted[0]:
                observer(default)
            return Subscription()
        return _PyObservable(_sub)

    def contains(self, target):
        def _sub(observer):
            found = [False]

            def wrapped(value):
                if not found[0]:
                    if value == target:
                        found[0] = True
            self.subscribe(wrapped)
            observer(found[0])
            return Subscription()
        return _PyObservable(_sub)

    def all(self, predicate):
        def _sub(observer):
            all_pass = [True]

            def wrapped(value):
                if all_pass[0]:
                    try:
                        if not predicate(value):
                            all_pass[0] = False
                    except Exception:
                        raise

            self.subscribe(wrapped)
            observer(all_pass[0])
            return Subscription()
        return _PyObservable(_sub)

    def merge(self, other):
        other_inner = getattr(other, "_inner", None)
        if other_inner is None and hasattr(other, "_subscribe"):
            other_subscribe = other._subscribe
        elif other_inner is not None and hasattr(other_inner, "subscribe"):
            def other_subscribe(observer):
                other_inner.subscribe(observer)
        else:
            def other_subscribe(observer):
                return Subscription()

        def _sub(observer):
            self.subscribe(observer)
            other_subscribe(observer)
            return Subscription()
        return _PyObservable(_sub)

    def concat(self, other):
        return self.merge(other)

    # ---------- 时间操作符 ----------
    def delay(self, seconds):
        """延迟发射所有值。"""
        def _sub(observer):
            def wrapped(value):
                _threading.Timer(seconds, lambda v=value: observer(v)).start()
            return self.subscribe(wrapped)
        return _PyObservable(_sub)

    def debounce(self, seconds):
        """防抖：静默期后才发射最后一个值。"""
        def _sub(observer):
            timer = [None]
            last_value = [None]

            def wrapped(value):
                last_value[0] = value
                if timer[0] is not None:
                    timer[0].cancel()
                timer[0] = _threading.Timer(seconds, lambda: observer(last_value[0]))
                timer[0].start()
            return self.subscribe(wrapped)
        return _PyObservable(_sub)

    def throttle(self, seconds):
        """节流：固定间隔内只发射第一个值。"""
        def _sub(observer):
            last_time = [0.0]

            def wrapped(value):
                now = time.time()
                if now - last_time[0] >= seconds:
                    last_time[0] = now
                    observer(value)
            return self.subscribe(wrapped)
        return _PyObservable(_sub)

    def timeout(self, seconds):
        """超时：如果 seconds 秒内无值则终止。"""
        def _sub(observer):
            timed_out = [False]
            timer = _threading.Timer(seconds, lambda: timed_out.__setitem__(0, True))
            timer.start()

            def wrapped(value):
                if not timed_out[0]:
                    observer(value)
            sub = self.subscribe(wrapped)
            timer.cancel()
            return sub
        return _PyObservable(_sub)

    # ---------- 静态时间工厂 ----------
    @staticmethod
    def interval(period):
        """定期发射递增整数。"""
        def _sub(observer):
            counter = [0]
            stopped = [False]

            def tick():
                if not stopped[0]:
                    observer(counter[0])
                    counter[0] += 1
                    _threading.Timer(period, tick).start()

            _threading.Timer(period, tick).start()
            sub = Subscription()
            sub.dispose = lambda: stopped.__setitem__(0, True)
            return sub
        return _PyObservable(_sub)

    @staticmethod
    def timer(delay):
        """延迟后发射 0。"""
        def _sub(observer):
            _threading.Timer(delay, lambda: observer(0)).start()
            return Subscription()
        return _PyObservable(_sub)

    def collect(self):
        """收集所有发射的值到一个列表（阻塞操作）。"""
        items: List[Any] = []

        def observer(value):
            items.append(value)
        self.subscribe(observer)
        return list(items)

    def run(self):
        self.subscribe(lambda _: None)
        return self


# ============================================================================
# Subject - 纯 Python 实现
# ============================================================================

class _PyPublishSubject:
    def __init__(self):
        self._observers = []

    def on_next(self, value):
        for obs in list(self._observers):
            if not obs[1].is_disposed():
                obs[0](value)

    def on_completed(self):
        self._observers.clear()

    def subscribe(self, on_next, on_error=None, on_completed=None):
        sub = Subscription()
        self._observers.append((on_next, sub))
        return sub


class _PyBehaviorSubject:
    def __init__(self, initial_value):
        self._current = initial_value
        self._observers = []

    def on_next(self, value):
        self._current = value
        for obs in list(self._observers):
            if not obs[1].is_disposed():
                obs[0](value)

    def on_completed(self):
        self._observers.clear()

    def subscribe(self, on_next, on_error=None, on_completed=None):
        sub = Subscription()
        on_next(self._current)
        self._observers.append((on_next, sub))
        return sub

    @property
    def value(self):
        return self._current


class _PyReplaySubject:
    def __init__(self, capacity=None, window=None):
        self._capacity = int(capacity) if capacity is not None else None  # None=无限
        self._window = window  # timedelta 或 None
        self._buffer = []  # 存储 (timestamp, value) 元组
        self._observers = []

    def on_next(self, value):
        now = time.time()
        self._buffer.append((now, value))
        # 按容量裁剪
        if self._capacity is not None and len(self._buffer) > self._capacity:
            self._buffer.pop(0)
        # 按时间窗口裁剪
        if self._window is not None:
            cutoff = now - self._window.total_seconds() if hasattr(self._window, 'total_seconds') else now - self._window
            self._buffer = [(t, v) for t, v in self._buffer if t >= cutoff]
        for obs in list(self._observers):
            if not obs[1].is_disposed():
                obs[0](value)

    def on_completed(self):
        self._observers.clear()

    def subscribe(self, on_next, on_error=None, on_completed=None):
        sub = Subscription()
        for ts, buffered in self._buffer:
            on_next(buffered)
        self._observers.append((on_next, sub))
        return sub


# ============================================================================
# Scheduler - 纯 Python 实现
# ============================================================================

class _PyCurrentThreadScheduler:
    def now(self):
        return time.time() * 1000.0


class _PyThreadPoolScheduler:
    def __init__(self, num_threads):
        self._num_threads = int(num_threads)

    def now(self):
        return time.time() * 1000.0

    def get_num_threads(self):
        return self._num_threads


class _PyAsyncScheduler:
    def now(self):
        return time.time() * 1000.0


class _PyImmediateScheduler:
    def now(self):
        return time.time() * 1000.0


# ============================================================================
# 公共 API: Observable
# ============================================================================

class Observable:
    """表示一个在未来可能发射 0 个或多个值的可观察对象。

    这是 Rx-Rust 的核心类。你可以：

    1. 通过静态工厂方法创建 Observable:
       - ``of(value)``            - 发射单个值
       - ``from_iter(iterable)``   - 发射迭代器中所有值
       - ``range(start, count)``   - 发射 count 个连续整数
       - ``repeat(value, n)``      - 重复发射 value n 次
       - ``empty()``               - 什么都不发射，立即完成
       - ``never()``               - 什么都不发射，也不完成

    2. 通过操作符组合和转换：
       - ``map(mapper)``          - 一对一转换
       - ``filter(predicate)``    - 条件过滤
       - ``take(n)``              - 只取前 n 个
       - ``skip(n)``              - 跳过前 n 个
       - ``first()``              - 只取第一个
       - ``reduce(initial, fn)``  - 累积并发射结果

    3. 通过 subscribe() 订阅：
       - ``subscribe(on_next=..., on_completed=...)``

    示例:
        >>> result = []
        >>> Observable.from_iter([1, 2, 3, 4, 5]) \
        ...     .filter(lambda x: x % 2 == 0) \
        ...     .map(lambda x: x * 10) \
        ...     .subscribe(on_next=lambda v: result.append(v))
        >>> result
        [20, 40]
    """

    def __init__(self, inner):
        """创建一个新的 Observable（通常从底层实现对象包装）。"""
        self._inner = inner

    # ---------- 静态工厂方法 ----------
    @staticmethod
    def of(*values):
        """创建一个发射给定值然后完成的 Observable。"""
        if len(values) == 0:
            raise TypeError("of() requires at least 1 argument")
        if _USE_RUST:
            try:
                if len(values) == 1:
                    return Observable(_rust_mod.Observable.of(values[0]))
                return Observable(_PyObservable.of(*values))
            except Exception:
                pass
        return Observable(_PyObservable.of(*values))

    @staticmethod
    def from_iter(values):
        """从一个可迭代对象创建 Observable，按顺序发射每个值。"""
        if _USE_RUST:
            try:
                return Observable(_rust_mod.Observable.from_iter(list(values)))
            except Exception:
                pass
        return Observable(_PyObservable.from_iter(values))

    @staticmethod
    def range(start, count):
        """创建一个发射连续整数范围的 Observable。"""
        if _USE_RUST:
            try:
                return Observable(_rust_mod.Observable.range(int(start), int(count)))
            except Exception:
                pass
        return Observable(_PyObservable.range(start, count))

    @staticmethod
    def repeat(value, count):
        """创建一个重复发射同一个值 count 次的 Observable。"""
        if _USE_RUST:
            try:
                return Observable(_rust_mod.Observable.repeat(value, int(count)))
            except Exception:
                pass
        return Observable(_PyObservable.repeat(value, count))

    @staticmethod
    def empty():
        """创建一个什么都不发射、立即完成的 Observable。"""
        if _USE_RUST:
            try:
                return Observable(_rust_mod.Observable.empty())
            except Exception:
                pass
        return Observable(_PyObservable.empty())

    @staticmethod
    def never():
        """创建一个什么都不发射、也不完成的 Observable。"""
        if _USE_RUST:
            try:
                return Observable(_rust_mod.Observable.never())
            except Exception:
                pass
        return Observable(_PyObservable.never())

    # ---------- 订阅方法 ----------
    def subscribe(self, on_next=None, on_error=None, on_completed=None):
        """订阅 Observable，开始接收值。

        Args:
            on_next: 接收每个值的回调。
            on_error: 发生异常时的回调（可选）。
            on_completed: 完成时的回调（可选）。

        Returns:
            Subscription: 订阅句柄，可用于取消订阅。
        """
        next_cb = on_next if on_next is not None else (lambda v: None)
        try:
            if _USE_RUST and hasattr(self._inner, "subscribe"):
                try:
                    rust_sub = self._inner.subscribe(next_cb)
                    if isinstance(rust_sub, Subscription):
                        return rust_sub
                    return Subscription(rust_sub)
                except Exception:
                    pass
            return self._inner.subscribe(next_cb, on_error=on_error, on_completed=on_completed)
        except Exception as e:
            if on_error:
                on_error(e)
                return Subscription()
            raise

    # ---------- pipe ----------
    def pipe(self, *operators):
        """管道操作符，每个 operator 是 (Observable) -> Observable 的函数。"""
        result = self
        for op in operators:
            result = op(result)
        return result

    # ---------- 转换操作符 ----------
    def map(self, mapper):
        """对每个发射的值应用映射函数。"""
        if _USE_RUST and hasattr(self._inner, "map"):
            try:
                return Observable(self._inner.map(mapper))
            except Exception:
                pass
        return Observable(self._inner.map(mapper))

    def filter(self, predicate):
        """只发射满足条件的值。"""
        if _USE_RUST and hasattr(self._inner, "filter"):
            try:
                return Observable(self._inner.filter(predicate))
            except Exception:
                pass
        return Observable(self._inner.filter(predicate))

    # ---------- 过滤操作符 ----------
    def take(self, n):
        """只发射前 n 个值。"""
        if _USE_RUST and hasattr(self._inner, "take"):
            try:
                return Observable(self._inner.take(int(n)))
            except Exception:
                pass
        return Observable(self._inner.take(n))

    def skip(self, n):
        """跳过前 n 个值。"""
        if _USE_RUST and hasattr(self._inner, "skip"):
            try:
                return Observable(self._inner.skip(int(n)))
            except Exception:
                pass
        return Observable(self._inner.skip(n))

    def first(self):
        """只发射第一个值。"""
        if _USE_RUST and hasattr(self._inner, "first"):
            try:
                return Observable(self._inner.first())
            except Exception:
                pass
        return Observable(self._inner.first())

    def last(self):
        """只发射最后一个值。"""
        if _USE_RUST and hasattr(self._inner, "last"):
            try:
                return Observable(self._inner.last())
            except Exception:
                pass
        return Observable(self._inner.last())

    def count(self):
        """发射收到的值总数量。"""
        if _USE_RUST and hasattr(self._inner, "count"):
            try:
                return Observable(self._inner.count())
            except Exception:
                pass
        return Observable(self._inner.count())

    def sum(self):
        """发射所有值的累加和。"""
        if _USE_RUST and hasattr(self._inner, "sum"):
            try:
                return Observable(self._inner.sum())
            except Exception:
                pass
        return Observable(self._inner.sum())

    # ---------- 聚合/累积操作符 ----------
    def reduce(self, initial, reducer):
        """累积所有值，在完成时发射最终累积结果。"""
        if _USE_RUST and hasattr(self._inner, "reduce"):
            try:
                return Observable(self._inner.reduce(initial, reducer))
            except Exception:
                pass
        return Observable(self._inner.reduce(initial, reducer))

    def scan(self, initial, scanner):
        """逐步累积并发射每一步的中间结果。"""
        if _USE_RUST and hasattr(self._inner, "scan"):
            try:
                return Observable(self._inner.scan(initial, scanner))
            except Exception:
                pass
        return Observable(self._inner.scan(initial, scanner))

    def flat_map(self, mapper):
        """对每个值应用 mapper，然后将结果展平发射。"""
        if _USE_RUST and hasattr(self._inner, "flat_map"):
            try:
                return Observable(self._inner.flat_map(mapper))
            except Exception:
                pass
        return Observable(self._inner.flat_map(mapper))

    def start_with(self, *values):
        """在序列开头插入一个或多个值。"""
        if _USE_RUST and hasattr(self._inner, "start_with"):
            try:
                return Observable(self._inner.start_with(*values))
            except Exception:
                pass
        return Observable(self._inner.start_with(*values))

    def default_if_empty(self, default):
        """如果源为空则发射 default，否则与源相同。"""
        if _USE_RUST and hasattr(self._inner, "default_if_empty"):
            try:
                return Observable(self._inner.default_if_empty(default))
            except Exception:
                pass
        return Observable(self._inner.default_if_empty(default))

    def contains(self, target):
        """检测序列中是否包含 target，发射单个布尔值。"""
        if _USE_RUST and hasattr(self._inner, "contains"):
            try:
                return Observable(self._inner.contains(target))
            except Exception:
                pass
        return Observable(self._inner.contains(target))

    def all(self, predicate):
        """检测是否所有值都满足 predicate，发射单个布尔值。"""
        if _USE_RUST and hasattr(self._inner, "all"):
            try:
                return Observable(self._inner.all(predicate))
            except Exception:
                pass
        return Observable(self._inner.all(predicate))

    def do_on_next(self, action):
        """为每个 on_next 产生副作用但不改变值本身。"""
        if _USE_RUST and hasattr(self._inner, "do_on_next"):
            try:
                return Observable(self._inner.do_on_next(action))
            except Exception:
                pass
        return Observable(self._inner.do_on_next(action))

    def merge(self, other):
        """将两个 Observable 的值合并到一个序列中。"""
        if _USE_RUST and hasattr(self._inner, "merge"):
            try:
                other_inner = getattr(other, "_inner", other)
                return Observable(self._inner.merge(other_inner))
            except Exception:
                pass
        return Observable(self._inner.merge(other))

    def concat(self, other):
        """连接两个 Observable（同步模式下等同于 merge）。"""
        if _USE_RUST and hasattr(self._inner, "concat"):
            try:
                other_inner = getattr(other, "_inner", other)
                return Observable(self._inner.concat(other_inner))
            except Exception:
                pass
        return Observable(self._inner.concat(other))

    # ---------- 时间操作符 ----------
    def delay(self, seconds):
        """延迟发射所有值。"""
        return Observable(self._inner.delay(seconds))

    def debounce(self, seconds):
        """防抖：静默期后才发射最后一个值。"""
        return Observable(self._inner.debounce(seconds))

    def throttle(self, seconds):
        """节流：固定间隔内只发射第一个值。"""
        return Observable(self._inner.throttle(seconds))

    def timeout(self, seconds):
        """超时：如果 seconds 秒内无值则终止。"""
        return Observable(self._inner.timeout(seconds))

    # ---------- 静态时间工厂 ----------
    @staticmethod
    def interval(period):
        """定期发射递增整数。"""
        return Observable(_PyObservable.interval(period))

    @staticmethod
    def timer(delay):
        """延迟后发射 0。"""
        return Observable(_PyObservable.timer(delay))

    # ---------- 收集 ----------
    def collect(self):
        """收集所有发射的值到一个列表（阻塞操作）。"""
        if _USE_RUST and hasattr(self._inner, "collect"):
            try:
                return list(self._inner.collect())
            except Exception:
                pass
        return self._inner.collect()

    def run(self):
        """订阅并消费所有值。"""
        if _USE_RUST and hasattr(self._inner, "run"):
            try:
                self._inner.run()
            except Exception:
                self._inner.subscribe(lambda _: None)
        else:
            self._inner.subscribe(lambda _: None)
        return self

    def __repr__(self):
        return "Observable()"


# ============================================================================
# Subject - 广播型主题
# ============================================================================

class PublishSubject:
    """广播型主题。向所有订阅者广播发射的值。

    新订阅者只能收到订阅之后发射的值。
    适合用作事件总线、信号分发等场景。
    """

    def __init__(self):
        """创建一个新的 PublishSubject。"""
        if _USE_RUST:
            try:
                self._inner = _rust_mod.PublishSubject()
            except Exception:
                self._inner = _PyPublishSubject()
        else:
            self._inner = _PyPublishSubject()

    def on_next(self, value):
        """向所有订阅者发射一个值。"""
        if hasattr(self._inner, "on_next"):
            self._inner.on_next(value)

    def on_completed(self):
        """向所有订阅者发出完成信号。"""
        if hasattr(self._inner, "on_completed"):
            self._inner.on_completed()

    def subscribe(self, on_next=None, on_error=None, on_completed=None):
        """订阅这个 Subject，接收后续发射的值。"""
        next_cb = on_next if on_next is not None else (lambda v: None)
        rust_sub = self._inner.subscribe(next_cb, on_error=on_error, on_completed=on_completed)
        if isinstance(rust_sub, Subscription):
            return rust_sub
        return Subscription(rust_sub)

    def __repr__(self):
        return "PublishSubject()"


class BehaviorSubject:
    """有"当前值"的主题。

    每个新订阅者会立即收到当前最新的值。
    适合用于表示应用状态、配置值等。
    """

    def __init__(self, initial_value):
        """创建一个新的 BehaviorSubject，指定初始值。"""
        if _USE_RUST:
            try:
                self._inner = _rust_mod.BehaviorSubject(initial_value)
            except Exception:
                self._inner = _PyBehaviorSubject(initial_value)
        else:
            self._inner = _PyBehaviorSubject(initial_value)

    def on_next(self, value):
        """发射一个值，并更新"当前值"。"""
        if hasattr(self._inner, "on_next"):
            self._inner.on_next(value)

    def on_completed(self):
        """发出完成信号。"""
        if hasattr(self._inner, "on_completed"):
            self._inner.on_completed()

    def subscribe(self, on_next=None, on_error=None, on_completed=None):
        """订阅，立即收到当前最新值。"""
        next_cb = on_next if on_next is not None else (lambda v: None)
        rust_sub = self._inner.subscribe(next_cb, on_error=on_error, on_completed=on_completed)
        if isinstance(rust_sub, Subscription):
            return rust_sub
        return Subscription(rust_sub)

    @property
    def value(self):
        """返回当前值。"""
        if hasattr(self._inner, "value"):
            v = self._inner.value
            if callable(v):
                return v()
            return v
        if hasattr(self._inner, "_current"):
            return self._inner._current
        return None

    def __repr__(self):
        return "BehaviorSubject()"


class ReplaySubject:
    """可重放历史值的主题。

    新订阅者会收到最近 capacity 个值的重放，以及后续的值。
    适合用于缓存历史事件、聊天消息等场景。
    """

    def __init__(self, capacity=None, window=None):
        """创建一个新的 ReplaySubject。

        Args:
            capacity: 缓冲区最大容量（None 表示无限）
            window: 时间窗口（timedelta 或秒数），超过此时间的值将被丢弃
        """
        if _USE_RUST:
            try:
                self._inner = _rust_mod.ReplaySubject(int(capacity) if capacity is not None else 100)
            except Exception:
                self._inner = _PyReplaySubject(capacity, window)
        else:
            self._inner = _PyReplaySubject(capacity, window)

    def on_next(self, value):
        """发射一个值，并将其加入缓存。"""
        if hasattr(self._inner, "on_next"):
            self._inner.on_next(value)

    def on_completed(self):
        """发出完成信号。"""
        if hasattr(self._inner, "on_completed"):
            self._inner.on_completed()

    def subscribe(self, on_next=None, on_error=None, on_completed=None):
        """订阅，会先收到缓存的历史值重放。"""
        next_cb = on_next if on_next is not None else (lambda v: None)
        rust_sub = self._inner.subscribe(next_cb, on_error=on_error, on_completed=on_completed)
        if isinstance(rust_sub, Subscription):
            return rust_sub
        return Subscription(rust_sub)

    def __repr__(self):
        return "ReplaySubject()"


# ============================================================================
# Scheduler - 调度器
# ============================================================================

class CurrentThreadScheduler:
    """当前线程调度器（同步执行）。"""

    def __init__(self):
        if _USE_RUST:
            try:
                self._inner = _rust_mod.CurrentThreadScheduler()
            except Exception:
                self._inner = _PyCurrentThreadScheduler()
        else:
            self._inner = _PyCurrentThreadScheduler()

    def now(self):
        return self._inner.now()

    def __repr__(self):
        return "CurrentThreadScheduler()"


class ThreadPoolScheduler:
    """线程池调度器（并发执行）。"""

    def __init__(self, workers):
        if _USE_RUST:
            try:
                self._inner = _rust_mod.ThreadPoolScheduler(int(workers))
            except Exception:
                self._inner = _PyThreadPoolScheduler(workers)
        else:
            self._inner = _PyThreadPoolScheduler(workers)
        self._workers = int(workers)

    def now(self):
        return self._inner.now()

    def get_num_threads(self):
        if hasattr(self._inner, "get_num_threads"):
            return self._inner.get_num_threads()
        return self._workers

    def __repr__(self):
        return f"ThreadPoolScheduler(workers={self._workers})"


class AsyncScheduler:
    """异步调度器。"""

    def __init__(self):
        if _USE_RUST:
            try:
                self._inner = _rust_mod.AsyncScheduler()
            except Exception:
                self._inner = _PyAsyncScheduler()
        else:
            self._inner = _PyAsyncScheduler()

    def now(self):
        return self._inner.now()

    def __repr__(self):
        return "AsyncScheduler()"


class ImmediateScheduler:
    """立即调度器。"""

    def __init__(self):
        if _USE_RUST:
            try:
                self._inner = _rust_mod.ImmediateScheduler()
            except Exception:
                self._inner = _PyImmediateScheduler()
        else:
            self._inner = _PyImmediateScheduler()

    def now(self):
        return self._inner.now()

    def __repr__(self):
        return "ImmediateScheduler()"


# ============================================================================
# CompositeSubscription
# ============================================================================

class CompositeSubscription:
    """组合多个 Subscription 统一管理。

    示例:
        >>> cs = CompositeSubscription()
        >>> cs.add(sub1)
        >>> cs.add(sub2)
        >>> cs.dispose()  # 一次性释放所有
    """

    def __init__(self):
        self._subs = []
        self._disposed = False

    def add(self, sub):
        """添加一个子订阅"""
        self._subs.append(sub)

    def remove(self, sub):
        """移除一个子订阅"""
        try:
            self._subs.remove(sub)
        except ValueError:
            pass

    def dispose(self):
        """释放所有子订阅"""
        self._disposed = True
        for sub in self._subs:
            if hasattr(sub, 'dispose'):
                sub.dispose()
        self._subs.clear()

    def is_disposed(self):
        if self._disposed:
            return True
        return all(
            (sub.is_disposed() if hasattr(sub, 'is_disposed') else True)
            for sub in self._subs
        ) if self._subs else False

    def __repr__(self):
        return f"CompositeSubscription({len(self._subs)} subs)"


# ============================================================================
# ops 模块 — 函数式操作符
# ============================================================================

class _OpModule:
    """提供函数式操作符，用于 pipe() 风格"""

    @staticmethod
    def map(mapper):
        return lambda obs: obs.map(mapper)

    @staticmethod
    def filter(predicate):
        return lambda obs: obs.filter(predicate)

    @staticmethod
    def take(n):
        return lambda obs: obs.take(n)

    @staticmethod
    def skip(n):
        return lambda obs: obs.skip(n)

    @staticmethod
    def first():
        return lambda obs: obs.first()

    @staticmethod
    def reduce(initial, reducer):
        return lambda obs: obs.reduce(initial, reducer)

    @staticmethod
    def scan(initial, scanner):
        return lambda obs: obs.scan(initial, scanner)

    @staticmethod
    def flat_map(mapper):
        return lambda obs: obs.flat_map(mapper)

    @staticmethod
    def start_with(*values):
        return lambda obs: obs.start_with(*values)

    @staticmethod
    def default_if_empty(default):
        return lambda obs: obs.default_if_empty(default)

    @staticmethod
    def contains(target):
        return lambda obs: obs.contains(target)

    @staticmethod
    def all(predicate):
        return lambda obs: obs.all(predicate)

    @staticmethod
    def do_on_next(action):
        return lambda obs: obs.do_on_next(action)


ops = _OpModule()


# ============================================================================
# 导出符号
# ============================================================================

__all__ = [
    "Observable",
    "Subscription",
    "CompositeSubscription",
    "PublishSubject",
    "BehaviorSubject",
    "ReplaySubject",
    "CurrentThreadScheduler",
    "ThreadPoolScheduler",
    "AsyncScheduler",
    "ImmediateScheduler",
    "ops",
]
