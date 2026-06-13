"""RxPY - Reactive Extensions for Python powered by Rust.

RxPY 是一个用于组合异步和基于事件的程序的 Python 库，灵感来自微软的 Reactive Extensions (Rx) 库。
它建立在 Rust 之上，通过 PyO3 提供高性能的响应式编程体验。

核心概念：
    Observable  - 在未来可能发射 0 个或多个值的程序抽象
    Observer    - 订阅者，接收值的接收者
    Operator    - 转换和组合 Observable 的操作符
    Subscription - 订阅句柄，用来取消订阅
    Subject     - 既是 Observable 又是 Observer，可以手动发射值

快速开始：
    >>> import rxpy
    >>> result = []
    >>> rxpy.Observable.from_iter([1, 2, 3, 4, 5]) \
    ...     .filter(lambda x: x % 2 == 0) \
    ...     .map(lambda x: x * 10) \
    ...     .subscribe(on_next=lambda v: result.append(v))
    <rxpy.Subscription object>
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

# ============================================================================
# 版本信息
# ============================================================================

__version__ = "0.1.0"
__author__ = "RxPY Contributors"
__license__ = "MIT"

# ============================================================================
# 导入 Rust 扩展模块（通过 PyO3 编译的高性能实现）
# ============================================================================

from .rxpy import (
    Observable as _PyObservable,
    Subscription as _PySubscription,
    PublishSubject as _PyPublishSubject,
    BehaviorSubject as _PyBehaviorSubject,
    ReplaySubject as _PyReplaySubject,
    CurrentThreadScheduler as _PyCurrentThreadScheduler,
    ThreadPoolScheduler as _PyThreadPoolScheduler,
    AsyncScheduler as _PyAsyncScheduler,
    ImmediateScheduler as _PyImmediateScheduler,
)

# 私有辅助函数：创建一个 no-op 回调函数
def _noop_callback(value):
    pass

# ============================================================================
# Subscription - 订阅句柄（带 docstring 增强）
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
        >>>

    注意事项：
        Subscription 是不可重放的 —— 一旦取消后不能恢复；
        多次调用 dispose() 是安全的（不会报错）。
    """

    def __init__(self, inner):
        """创建一个新的 Subscription。

        Args:
            inner: 从 Rust 层返回的原始 Subscription 对象。
        """
        self._inner = inner

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
        self._inner.dispose()

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
        return self._inner.is_disposed()

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
# Observable - 可观察对象（Python 层增强，带完整 docstring）
# ============================================================================

class Observable:
    """表示一个在未来可能发射 0 个或多个值的可观察对象。

    这是 RxPY 的核心类。你可以：

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
        """创建一个新的 Observable（通常从 Rust 层对象包装）。

        Args:
            inner: Rust 层的 PyObservable 对象。
        """
        self._inner = inner

    # ---------- 静态工厂方法 ----------

    @staticmethod
    def of(value):
        """创建一个发射单个值然后完成的 Observable。

        Args:
            value: 要发射的任意值。

        Returns:
            Observable: 发射 value 然后完成的新 Observable。

        示例:
            >>> result = []
            >>> Observable.of("hello").subscribe(
            ...     on_next=lambda v: result.append(v),
            ...     on_completed=lambda: result.append("DONE"))
            >>> result
            ['hello', 'DONE']
        """
        return Observable(_PyObservable.of(value))

    @staticmethod
    def from_iter(values):
        """从一个可迭代对象创建 Observable，按顺序发射每个值。

        Args:
            values: 任何可迭代对象（list, tuple, range, 生成器等）。

        Returns:
            Observable: 按顺序发射每个值的新 Observable。

        示例:
            >>> result = []
            >>> Observable.from_iter([10, 20, 30]).subscribe(on_next=lambda v: result.append(v))
            >>> result
            [10, 20, 30]
        """
        val_list = list(values)
        return Observable(_PyObservable.from_iter(val_list))

    @staticmethod
    def range(start, count):
        """创建一个发射连续整数范围的 Observable。

        从 start 开始，发射 count 个连续整数。
        等价于 Python 的 range(start, start + count)。

        Args:
            start (int): 起始值（包含）。
            count (int): 要发射的值的数量。

        Returns:
            Observable: 发射 [start, start+1, ..., start+count-1] 的 Observable。

        示例:
            >>> result = []
            >>> Observable.range(5, 3).subscribe(on_next=lambda v: result.append(v))
            >>> result
            [5, 6, 7]
        """
        return Observable(_PyObservable.range(int(start), int(count)))

    @staticmethod
    def repeat(value, count):
        """创建一个重复发射同一个值 count 次的 Observable。

        Args:
            value: 要重复发射的值。
            count (int): 重复次数。

        Returns:
            Observable: 重复发射 value count 次的新 Observable。

        示例:
            >>> result = []
            >>> Observable.repeat("hi", 3).subscribe(on_next=lambda v: result.append(v))
            >>> result
            ['hi', 'hi', 'hi']
        """
        return Observable(_PyObservable.repeat(value, int(count)))

    @staticmethod
    def empty():
        """创建一个什么都不发射、立即完成的 Observable。

        Returns:
            Observable: 立即完成的空 Observable。

        示例:
            >>> result = []
            >>> Observable.empty().subscribe(
            ...     on_next=lambda v: result.append(("got", v)),
            ...     on_completed=lambda: result.append("DONE"))
            >>> result
            ['DONE']
        """
        return Observable(_PyObservable.empty())

    @staticmethod
    def never():
        """创建一个什么都不发射、也不完成的 Observable。

        Returns:
            Observable: 永远沉默的 Observable。

        示例:
            >>> never_obs = Observable.never()
        """
        return Observable(_PyObservable.never())

    # ---------- 订阅方法 ----------

    def subscribe(self, on_next=None, on_error=None, on_completed=None):
        """订阅 Observable，开始接收值。

        Args:
            on_next: 当有新值发射时调用的回调函数 (value) -> None
            on_error: 当发生错误时调用的回调函数 (error) -> None
            on_completed: 当完成时调用的回调函数 () -> None

        Returns:
            Subscription: 可用于取消订阅的句柄。

        示例:
            >>> Observable.of(42).subscribe(
            ...     on_next=lambda v: print(f"got: {v}"),
            ...     on_completed=lambda: print("done"))
            got: 42
            done
        """
        next_cb = on_next if on_next is not None else _noop_callback
        sub = self._inner.subscribe(next_cb)
        return Subscription(sub)

    # ---------- 转换操作符 ----------

    def map(self, mapper):
        """对每个发射的值应用映射函数。

        Args:
            mapper: 转换函数 (value) -> new_value。

        Returns:
            Observable: 值被转换后的新 Observable。

        示例:
            >>> result = []
            >>> Observable.from_iter([1, 2, 3]).map(lambda x: x * 10).subscribe(on_next=lambda v: result.append(v))
            >>> result
            [10, 20, 30]
        """
        return Observable(self._inner.map(mapper))

    def filter(self, predicate):
        """只发射满足条件的值。

        Args:
            predicate: 断言函数 (value) -> bool。

        Returns:
            Observable: 仅包含 predicate(value) 为 True 的值的新 Observable。

        示例:
            >>> result = []
            >>> Observable.from_iter([1, 2, 3, 4, 5]).filter(lambda x: x > 2).subscribe(on_next=lambda v: result.append(v))
            >>> result
            [3, 4, 5]
        """
        return Observable(self._inner.filter(predicate))

    # ---------- 过滤操作符 ----------

    def take(self, n):
        """只发射前 n 个值。

        Args:
            n (int): 要发射的值的数量。

        Returns:
            Observable: 仅包含前 n 个值的新 Observable。

        示例:
            >>> result = []
            >>> Observable.from_iter([1, 2, 3, 4, 5]).take(3).subscribe(on_next=lambda v: result.append(v))
            >>> result
            [1, 2, 3]
        """
        return Observable(self._inner.take(int(n)))

    def skip(self, n):
        """跳过前 n 个值。

        Args:
            n (int): 要跳过的值的数量。

        Returns:
            Observable: 跳过前 n 个值后的新 Observable。

        示例:
            >>> result = []
            >>> Observable.from_iter([1, 2, 3, 4, 5]).skip(2).subscribe(on_next=lambda v: result.append(v))
            >>> result
            [3, 4, 5]
        """
        return Observable(self._inner.skip(int(n)))

    def first(self):
        """只发射第一个值。

        Returns:
            Observable: 仅包含第一个值的新 Observable。

        示例:
            >>> result = []
            >>> Observable.from_iter([1, 2, 3]).first().subscribe(on_next=lambda v: result.append(v))
            >>> result
            [1]
        """
        return Observable(self._inner.first())

    # ---------- 聚合操作符 ----------

    def reduce(self, initial, reducer):
        """累积所有值，在完成时发射最终累积结果。

        Args:
            initial: 初始累积值。
            reducer: 累积函数 (accumulator, value) -> new_accumulator。

        Returns:
            Observable: 在源完成时发射单个累积值的新 Observable。

        示例:
            >>> result = []
            >>> Observable.from_iter([1, 2, 3, 4]).reduce(0, lambda acc, x: acc + x).subscribe(on_next=lambda v: result.append(v))
            >>> result
            [10]
        """
        return Observable(self._inner.reduce(initial, reducer))

    # ---------- 收集操作符 ----------

    def collect(self):
        """收集所有发射的值到一个列表。

        这是一个阻塞操作，会等待 Observable 完成，然后返回所有值的列表。

        Returns:
            list: 所有发射值的列表。

        示例:
            >>> vals = Observable.from_iter([1, 2, 3]).collect()
            >>> vals
            [1, 2, 3]
        """
        return list(self._inner.collect())

    def __repr__(self):
        return "Observable()"


# ============================================================================
# Subject - 主题（既是 Observable 也是 Observer）
# ============================================================================

class PublishSubject:
    """广播型主题。向所有订阅者广播发射的值。

    新订阅者只能收到订阅之后发射的值。
    适合用作事件总线、信号分发等场景。

    示例:
        >>> subject = PublishSubject()
        >>> result_a = []
        >>> subject.subscribe(on_next=lambda v: result_a.append(("A", v)))
        >>> subject.on_next(1)
        >>> subject.on_next(2)
        >>> result_b = []
        >>> subject.subscribe(on_next=lambda v: result_b.append(("B", v)))
        >>> subject.on_next(3)
        >>> subject.on_completed()
        >>> result_a
        [('A', 1), ('A', 2), ('A', 3)]
        >>> result_b
        [('B', 3)]
    """

    def __init__(self):
        """创建一个新的 PublishSubject。"""
        self._inner = _PyPublishSubject()

    def on_next(self, value):
        """向所有订阅者发射一个值。

        Args:
            value: 要发射的值。

        示例:
            >>> subject = PublishSubject()
            >>> subject.on_next("hello")
        """
        self._inner.on_next(value)

    def on_completed(self):
        """向所有订阅者发出完成信号。

        调用后，后续的 on_next 调用将被静默丢弃。

        示例:
            >>> subject = PublishSubject()
            >>> subject.on_completed()
        """
        self._inner.on_completed()

    def subscribe(self, on_next=None, on_error=None, on_completed=None):
        """订阅这个 Subject，接收后续发射的值。

        Args:
            on_next: 接收值回调函数 (value) -> None。
            on_error: 错误回调函数 (error) -> None。
            on_completed: 完成回调函数 () -> None。

        Returns:
            Subscription: 订阅句柄。

        示例:
            >>> subject = PublishSubject()
            >>> sub = subject.subscribe(on_next=lambda v: print(f"got: {v}"))
            >>> subject.on_next(42)
            got: 42
        """
        next_cb = on_next if on_next is not None else _noop_callback
        sub = self._inner.subscribe(next_cb)
        return Subscription(sub)

    def __repr__(self):
        return "PublishSubject()"


class BehaviorSubject:
    """有"当前值"的主题。

    每个新订阅者会立即收到当前最新的值。
    适合用于表示应用状态、配置值等。

    示例:
        >>> subject = BehaviorSubject(0)
        >>> result = []
        >>> subject.subscribe(on_next=lambda v: result.append(("A", v)))
        >>> subject.on_next(1)
        >>> subject.on_next(2)
        >>> result_b = []
        >>> subject.subscribe(on_next=lambda v: result_b.append(("B", v)))
        >>> subject.on_next(3)
        >>> subject.on_completed()
        >>> result
        [('A', 0), ('A', 1), ('A', 2), ('A', 3)]
        >>> result_b
        [('B', 2), ('B', 3)]
    """

    def __init__(self, initial_value):
        """创建一个新的 BehaviorSubject，指定初始值。

        Args:
            initial_value: 初始值，新订阅者会立即收到它。
        """
        self._inner = _PyBehaviorSubject(initial_value)

    def on_next(self, value):
        """发射一个值，并更新"当前值"。

        Args:
            value: 要发射的值。
        """
        self._inner.on_next(value)

    def on_completed(self):
        """发出完成信号。"""
        self._inner.on_completed()

    def subscribe(self, on_next=None, on_error=None, on_completed=None):
        """订阅，立即收到当前最新值。

        Args:
            on_next: 接收值回调函数。
            on_error: 错误回调函数。
            on_completed: 完成回调函数。

        Returns:
            Subscription: 订阅句柄。
        """
        next_cb = on_next if on_next is not None else _noop_callback
        sub = self._inner.subscribe(next_cb)
        return Subscription(sub)

    def __repr__(self):
        return "BehaviorSubject()"


class ReplaySubject:
    """可重放历史值的主题。

    新订阅者会收到最近 capacity 个值的重放，以及后续的值。
    适合用于缓存历史事件、聊天消息等场景。

    示例:
        >>> subject = ReplaySubject(2)
        >>> subject.on_next(1)
        >>> subject.on_next(2)
        >>> subject.on_next(3)
        >>> result = []
        >>> subject.subscribe(on_next=lambda v: result.append(v))
        >>> result
        [2, 3]
    """

    def __init__(self, capacity):
        """创建一个新的 ReplaySubject。

        Args:
            capacity (int): 要缓存并在新订阅时重放的值数量。
        """
        self._inner = _PyReplaySubject(int(capacity))

    def on_next(self, value):
        """发射一个值，并将其加入缓存。

        Args:
            value: 要发射的值。
        """
        self._inner.on_next(value)

    def on_completed(self):
        """发出完成信号。"""
        self._inner.on_completed()

    def subscribe(self, on_next=None, on_error=None, on_completed=None):
        """订阅，会先收到缓存的历史值重放。

        Args:
            on_next: 接收值回调函数。
            on_error: 错误回调函数。
            on_completed: 完成回调函数。

        Returns:
            Subscription: 订阅句柄。
        """
        next_cb = on_next if on_next is not None else _noop_callback
        sub = self._inner.subscribe(next_cb)
        return Subscription(sub)

    def __repr__(self):
        return "ReplaySubject()"


# ============================================================================
# Scheduler - 调度器
# ============================================================================

class CurrentThreadScheduler:
    """当前线程调度器（同步执行）。

    在当前线程上同步执行所有工作，不会切换线程。
    这是默认的、最简单的调度器。

    示例:
        >>> scheduler = CurrentThreadScheduler()
    """

    def __init__(self):
        """创建一个新的 CurrentThreadScheduler。"""
        self._inner = _PyCurrentThreadScheduler()

    def __repr__(self):
        return "CurrentThreadScheduler()"


class ThreadPoolScheduler:
    """线程池调度器（并发执行）。

    在一个固定大小的线程池中并发执行工作。
    适合 CPU 密集型任务。

    示例:
        >>> scheduler = ThreadPoolScheduler(4)
    """

    def __init__(self, workers):
        """创建一个新的 ThreadPoolScheduler。

        Args:
            workers (int): 线程池中工作线程数。
        """
        self._inner = _PyThreadPoolScheduler(int(workers))
        self._workers = int(workers)

    def __repr__(self):
        return f"ThreadPoolScheduler(workers={self._workers})"


class AsyncScheduler:
    """异步调度器。

    在异步任务上异步执行工作，适合 I/O 密集型任务。

    示例:
        >>> scheduler = AsyncScheduler()
    """

    def __init__(self):
        """创建一个新的 AsyncScheduler。"""
        self._inner = _PyAsyncScheduler()

    def __repr__(self):
        return "AsyncScheduler()"


class ImmediateScheduler:
    """立即调度器。

    立即执行，不做任何调度。
    主要用于测试和简单同步操作。

    示例:
        >>> scheduler = ImmediateScheduler()
    """

    def __init__(self):
        """创建一个新的 ImmediateScheduler。"""
        self._inner = _PyImmediateScheduler()

    def __repr__(self):
        return "ImmediateScheduler()"


# ============================================================================
# 导出符号
# ============================================================================

__all__ = [
    "Observable",
    "Subscription",
    "PublishSubject",
    "BehaviorSubject",
    "ReplaySubject",
    "CurrentThreadScheduler",
    "ThreadPoolScheduler",
    "AsyncScheduler",
    "ImmediateScheduler",
]
