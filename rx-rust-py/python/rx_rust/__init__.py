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
import collections as _collections
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

    # ---------- 统计聚合 ----------
    def min(self):
        """发射流中的最小值。空流不发射。"""
        def _sub(observer):
            min_val = [None]
            has_val = [False]

            def wrapped(value):
                try:
                    if not has_val[0] or value < min_val[0]:
                        min_val[0] = value
                    has_val[0] = True
                except Exception:
                    raise
            self.subscribe(wrapped)
            if has_val[0]:
                observer(min_val[0])
            return Subscription()
        return _PyObservable(_sub)

    def max(self):
        """发射流中的最大值。空流不发射。"""
        def _sub(observer):
            max_val = [None]
            has_val = [False]

            def wrapped(value):
                if not has_val[0] or value > max_val[0]:
                    max_val[0] = value
                has_val[0] = True
            self.subscribe(wrapped)
            if has_val[0]:
                observer(max_val[0])
            return Subscription()
        return _PyObservable(_sub)

    def mean(self):
        """发射数值的平均值。空流不发射。"""
        def _sub(observer):
            total = [0.0]
            count = [0]

            def wrapped(value):
                total[0] += value
                count[0] += 1
            self.subscribe(wrapped)
            if count[0] > 0:
                observer(total[0] / count[0])
            return Subscription()
        return _PyObservable(_sub)

    def average(self):
        """同 mean()。发射数值的平均值。"""
        return self.mean()

    def median(self):
        """发射中位数（需要缓冲所有值，仅对有限流适用）。"""
        def _sub(observer):
            vals = []

            def wrapped(value):
                vals.append(value)
            self.subscribe(wrapped)
            if vals:
                sorted_vals = sorted(vals)
                n = len(sorted_vals)
                if n % 2 == 1:
                    observer(sorted_vals[n // 2])
                else:
                    observer((sorted_vals[n // 2 - 1] + sorted_vals[n // 2]) / 2.0)
            return Subscription()
        return _PyObservable(_sub)

    def variance(self, ddof=0):
        """发射方差。ddof=0 为总体方差，ddof=1 为样本方差。"""
        def _sub(observer):
            vals = []

            def wrapped(value):
                vals.append(value)
            self.subscribe(wrapped)
            n = len(vals)
            if n > ddof:
                mean_val = sum(vals) / n
                var = sum((x - mean_val) ** 2 for x in vals) / (n - ddof)
                observer(var)
            return Subscription()
        return _PyObservable(_sub)

    def std(self, ddof=0):
        """发射标准差。"""
        def _sub(observer):
            vals = []

            def wrapped(value):
                vals.append(value)
            self.subscribe(wrapped)
            n = len(vals)
            if n > ddof:
                mean_val = sum(vals) / n
                var = sum((x - mean_val) ** 2 for x in vals) / (n - ddof)
                observer(var ** 0.5)
            return Subscription()
        return _PyObservable(_sub)

    def quantile(self, q):
        """发射分位数。q ∈ [0, 1]。0.5 为中位数。"""
        def _sub(observer):
            vals = []

            def wrapped(value):
                vals.append(value)
            self.subscribe(wrapped)
            if vals:
                sorted_vals = sorted(vals)
                n = len(sorted_vals)
                idx = q * (n - 1)
                lo = int(idx)
                hi = min(lo + 1, n - 1)
                frac = idx - lo
                observer(sorted_vals[lo] + frac * (sorted_vals[hi] - sorted_vals[lo]))
            return Subscription()
        return _PyObservable(_sub)

    def arg_min(self):
        """发射最小值的下标索引（从 0 开始）。"""
        def _sub(observer):
            best_idx = [-1]
            best_val = [None]
            idx = [0]

            def wrapped(value):
                if best_idx[0] == -1 or value < best_val[0]:
                    best_val[0] = value
                    best_idx[0] = idx[0]
                idx[0] += 1
            self.subscribe(wrapped)
            if best_idx[0] != -1:
                observer(best_idx[0])
            return Subscription()
        return _PyObservable(_sub)

    def arg_max(self):
        """发射最大值的下标索引（从 0 开始）。"""
        def _sub(observer):
            best_idx = [-1]
            best_val = [None]
            idx = [0]

            def wrapped(value):
                if best_idx[0] == -1 or value > best_val[0]:
                    best_val[0] = value
                    best_idx[0] = idx[0]
                idx[0] += 1
            self.subscribe(wrapped)
            if best_idx[0] != -1:
                observer(best_idx[0])
            return Subscription()
        return _PyObservable(_sub)

    def n_unique(self):
        """发射流中不重复值的数量。"""
        def _sub(observer):
            seen = set()

            def wrapped(value):
                seen.add(value)
            self.subscribe(wrapped)
            observer(len(seen))
            return Subscription()
        return _PyObservable(_sub)

    def any(self, predicate):
        """只要有一个值满足谓词就发射 True，遍历结束无则发射 False。"""
        def _sub(observer):
            found = [False]

            def wrapped(value):
                if not found[0]:
                    if predicate(value):
                        found[0] = True
            self.subscribe(wrapped)
            observer(found[0])
            return Subscription()
        return _PyObservable(_sub)

    # ---------- 滚动窗口 ----------
    def rolling_min(self, window_size):
        """维护最近 window_size 个值的滚动最小值。窗口未满也发射。"""
        window_size = int(window_size)
        if window_size <= 0:
            raise ValueError("window_size must be positive")

        def _sub(observer):
            from collections import deque
            dq = deque(maxlen=window_size)

            def wrapped(value):
                dq.append(value)
                observer(min(dq))
            self.subscribe(wrapped)
            return Subscription()
        return _PyObservable(_sub)

    def rolling_max(self, window_size):
        """维护最近 window_size 个值的滚动最大值。"""
        window_size = int(window_size)
        if window_size <= 0:
            raise ValueError("window_size must be positive")

        def _sub(observer):
            from collections import deque
            dq = deque(maxlen=window_size)

            def wrapped(value):
                dq.append(value)
                observer(max(dq))
            self.subscribe(wrapped)
            return Subscription()
        return _PyObservable(_sub)

    def rolling_sum(self, window_size):
        """维护最近 window_size 个值的滚动和。"""
        window_size = int(window_size)
        if window_size <= 0:
            raise ValueError("window_size must be positive")

        def _sub(observer):
            from collections import deque
            dq = deque(maxlen=window_size)
            current_sum = [0]

            def wrapped(value):
                if len(dq) == window_size:
                    current_sum[0] -= dq[0]
                current_sum[0] += value
                dq.append(value)
                observer(current_sum[0])
            self.subscribe(wrapped)
            return Subscription()
        return _PyObservable(_sub)

    def rolling_mean(self, window_size):
        """维护最近 window_size 个值的滚动均值。"""
        window_size = int(window_size)
        if window_size <= 0:
            raise ValueError("window_size must be positive")

        def _sub(observer):
            from collections import deque
            dq = deque(maxlen=window_size)
            current_sum = [0]

            def wrapped(value):
                if len(dq) == window_size:
                    current_sum[0] -= dq[0]
                current_sum[0] += value
                dq.append(value)
                observer(current_sum[0] / len(dq))
            self.subscribe(wrapped)
            return Subscription()
        return _PyObservable(_sub)

    # ---------- 累积变换 ----------
    def cum_sum(self):
        """每步累积求和。"""
        def _sub(observer):
            acc = [0]

            def wrapped(value):
                acc[0] += value
                observer(acc[0])
            self.subscribe(wrapped)
            return Subscription()
        return _PyObservable(_sub)

    def cum_min(self):
        """每步累积最小值。"""
        def _sub(observer):
            acc = [None]
            first = [True]

            def wrapped(value):
                if first[0]:
                    acc[0] = value
                    first[0] = False
                else:
                    acc[0] = min(acc[0], value)
                observer(acc[0])
            self.subscribe(wrapped)
            return Subscription()
        return _PyObservable(_sub)

    def cum_max(self):
        """每步累积最大值。"""
        def _sub(observer):
            acc = [None]
            first = [True]

            def wrapped(value):
                if first[0]:
                    acc[0] = value
                    first[0] = False
                else:
                    acc[0] = max(acc[0], value)
                observer(acc[0])
            self.subscribe(wrapped)
            return Subscription()
        return _PyObservable(_sub)

    def cum_mean(self):
        """每步累积均值。"""
        def _sub(observer):
            running_sum = [0.0]
            running_count = [0]

            def wrapped(value):
                running_sum[0] += value
                running_count[0] += 1
                observer(running_sum[0] / running_count[0])
            self.subscribe(wrapped)
            return Subscription()
        return _PyObservable(_sub)

    def cum_prod(self):
        """每步累积乘积。"""
        def _sub(observer):
            acc = [1]

            def wrapped(value):
                acc[0] *= value
                observer(acc[0])
            self.subscribe(wrapped)
            return Subscription()
        return _PyObservable(_sub)

    # ---------- 排序 Top-N ----------
    def sort(self, key=None, reverse=False):
        """收集全部值排序后发射（仅对有限流适用）。"""
        def _sub(observer):
            vals = []

            def wrapped(value):
                vals.append(value)
            self.subscribe(wrapped)
            for v in sorted(vals, key=key, reverse=reverse):
                observer(v)
            return Subscription()
        return _PyObservable(_sub)

    def top_k(self, k, key=None):
        """返回前 k 个最大值（堆实现，节省内存）。"""
        k = int(k)
        if k <= 0:
            raise ValueError("k must be positive")

        def _sub(observer):
            import heapq
            heap = []  # min-heap of size at most k
            counter = [0]

            def wrapped(value):
                kvalue = key(value) if key else value
                if counter[0] < k:
                    heapq.heappush(heap, (kvalue, counter[0], value))
                else:
                    if kvalue > heap[0][0]:
                        heapq.heapreplace(heap, (kvalue, counter[0], value))
                counter[0] += 1
            self.subscribe(wrapped)
            sorted_items = sorted(heap, key=lambda t: t[0], reverse=True)
            for _, _, v in sorted_items:
                observer(v)
            return Subscription()
        return _PyObservable(_sub)

    def bottom_k(self, k, key=None):
        """返回最小的 k 个值（堆实现），按升序排列。"""
        k = int(k)
        if k <= 0:
            raise ValueError("k must be positive")

        def _sub(observer):
            import heapq
            heap = []  # max-heap via negation: stores (-value, counter, value)
            counter = [0]

            def wrapped(value):
                kvalue = key(value) if key else value
                if counter[0] < k:
                    heapq.heappush(heap, (-kvalue, counter[0], value))
                else:
                    if kvalue < -heap[0][0]:
                        heapq.heapreplace(heap, (-kvalue, counter[0], value))
                counter[0] += 1
            self.subscribe(wrapped)
            # 按原始值升序发射
            sorted_items = sorted(heap, key=lambda t: t[0], reverse=True)
            for _, _, v in sorted_items:
                observer(v)
            return Subscription()
        return _PyObservable(_sub)

    # ---------- 过滤/选择算子 ----------
    def distinct(self):
        """去重：每个值只发射一次。"""
        def _sub(observer):
            seen = set()

            def wrapped(value):
                if value not in seen:
                    seen.add(value)
                    observer(value)
            self.subscribe(wrapped)
            return Subscription()
        return _PyObservable(_sub)

    def element_at(self, idx):
        """发射第 idx 个值（0-based）。越界不发射。"""
        idx = int(idx)

        def _sub(observer):
            i = [0]

            def wrapped(value):
                if i[0] == idx:
                    observer(value)
                    i[0] = idx + 2  # mark done，避免二次匹配
                else:
                    i[0] += 1
            self.subscribe(wrapped)
            return Subscription()
        return _PyObservable(_sub)

    def take_while(self, predicate):
        """满足谓词时取，遇到第一个不满足值即终止。"""
        def _sub(observer):
            stopped = [False]

            def wrapped(value):
                if stopped[0]:
                    return
                if predicate(value):
                    observer(value)
                else:
                    stopped[0] = True
            self.subscribe(wrapped)
            return Subscription()
        return _PyObservable(_sub)

    def skip_while(self, predicate):
        """跳过满足谓词的值，直到第一个不满足后全部发射。"""
        def _sub(observer):
            skipping = [True]

            def wrapped(value):
                if skipping[0]:
                    if predicate(value):
                        return
                    skipping[0] = False
                observer(value)
            self.subscribe(wrapped)
            return Subscription()
        return _PyObservable(_sub)

    def take_last(self, n):
        """取最后 n 个值（需要缓冲）。"""
        n = int(n)
        if n <= 0:
            raise ValueError("n must be positive")

        def _sub(observer):
            from collections import deque
            dq = deque(maxlen=n)

            def wrapped(value):
                dq.append(value)
            self.subscribe(wrapped)
            for v in dq:
                observer(v)
            return Subscription()
        return _PyObservable(_sub)

    def skip_last(self, n):
        """跳过最后 n 个值（需要缓冲）。"""
        n = int(n)
        if n <= 0:
            raise ValueError("n must be positive")

        def _sub(observer):
            from collections import deque
            buffer = deque()

            def wrapped(value):
                if len(buffer) >= n:
                    observer(buffer.popleft())
                buffer.append(value)
            self.subscribe(wrapped)
            return Subscription()
        return _PyObservable(_sub)

    # ---------- 组合算子 ----------
    def switch_map(self, mapper):
        """新值到来时取消前一个内层订阅，切换到新的 Observable。"""
        def _sub(observer):
            current_sub = [None]

            def wrapped(value):
                inner = mapper(value)
                if hasattr(inner, "_subscribe"):
                    inner_src = inner._subscribe
                elif hasattr(inner, "subscribe"):
                    inner_src = inner.subscribe
                else:
                    # 兼容可迭代
                    try:
                        for item in inner:
                            observer(item)
                        return
                    except TypeError:
                        observer(inner)
                        return
                if current_sub[0] is not None and hasattr(current_sub[0], 'dispose'):
                    try:
                        current_sub[0].dispose()
                    except Exception:
                        pass
                current_sub[0] = inner_src(observer)
            self.subscribe(wrapped)
            return Subscription()
        return _PyObservable(_sub)

    def combine_latest(self, other, combiner):
        """两边都有过值后，任一方更新都发射最新组合。"""
        def _sub(observer):
            latest_a = [None]
            latest_b = [None]
            has_a = [False]
            has_b = [False]

            def on_a(v):
                latest_a[0] = v
                has_a[0] = True
                if has_b[0]:
                    observer(combiner(latest_a[0], latest_b[0]))

            def on_b(v):
                latest_b[0] = v
                has_b[0] = True
                if has_a[0]:
                    observer(combiner(latest_a[0], latest_b[0]))
            self.subscribe(on_a)
            if hasattr(other, "_subscribe"):
                other._subscribe(on_b)
            elif hasattr(other, "subscribe"):
                other.subscribe(on_next=on_b)
            else:
                # 兼容可迭代
                for item in other:
                    on_b(item)
            return Subscription()
        return _PyObservable(_sub)

    # ---------- 错误处理 ----------
    def catch_error(self, handler):
        """异常时调用 handler(err) 返回新的 Observable 继续。"""
        def _sub(observer):
            has_error = [False]

            def wrapped(value):
                if not has_error[0]:
                    observer(value)

            def err_handler(err):
                has_error[0] = True
                fallback = handler(err)
                if hasattr(fallback, "_subscribe"):
                    fallback._subscribe(observer)
                elif hasattr(fallback, "subscribe"):
                    fallback.subscribe(on_next=observer)
                else:
                    observer(fallback)
            return self.subscribe(wrapped, on_error=err_handler)
        return _PyObservable(_sub)

    def retry(self, count):
        """失败时重试最多 count 次。"""
        count = int(count)

        def _sub(observer):
            for _ in range(count + 1):
                err_occurred = [False]
                completed = [False]

                def on_next(v):
                    if not err_occurred[0] and not completed[0]:
                        observer(v)

                def on_err(e):
                    err_occurred[0] = True

                def on_completed():
                    completed[0] = True
                try:
                    self.subscribe(on_next, on_error=on_err, on_completed=on_completed)
                except Exception as e:
                    err_occurred[0] = True
                if not err_occurred[0]:
                    return Subscription()
            return Subscription()
        return _PyObservable(_sub)

    def retry_with_delay(self, count, delay_seconds):
        """失败后延迟 delay_seconds 再重试。"""
        count = int(count)
        delay_seconds = float(delay_seconds)

        def _sub(observer):
            for attempt in range(count + 1):
                err_occurred = [False]
                err_val = [None]

                def on_next(v):
                    observer(v)

                def on_err(e):
                    err_occurred[0] = True
                    err_val[0] = e
                try:
                    self.subscribe(on_next, on_error=on_err)
                except Exception as e:
                    err_occurred[0] = True
                    err_val[0] = e
                if not err_occurred[0]:
                    return Subscription()
                if attempt == count:
                    # 最后一次也失败
                    return Subscription()
                time.sleep(delay_seconds)
            return Subscription()
        return _PyObservable(_sub)

    # ---------- 轻量过滤 ----------
    def distinct_until_changed(self):
        """只当值与上一个不同时才发射。"""
        def _sub(observer):
            last = [None]
            first = [True]

            def wrapped(value):
                if first[0]:
                    observer(value)
                    last[0] = value
                    first[0] = False
                    return
                if last[0] != value:
                    observer(value)
                    last[0] = value
            self.subscribe(wrapped)
            return Subscription()
        return _PyObservable(_sub)

    def ignore_elements(self):
        """不发射任何值，只转发完成信号。"""
        def _sub(observer):
            self.subscribe(lambda v: None)
            return Subscription()
        return _PyObservable(_sub)

    # ---------- 多播 share/publish ----------
    def share(self):
        """多个订阅者共享同一个源订阅。"""
        source = self
        subscribers = []
        ref_count = [0]
        src_sub = [None]

        def dispatch(value):
            for fn, sub in list(subscribers):
                if not sub.is_disposed():
                    fn(value)

        def subscribe(on_next, on_error=None, on_completed=None):
            sub = Subscription()
            subscribers.append((on_next, sub))
            if ref_count[0] == 0:
                src_sub[0] = source._subscribe(dispatch)
            ref_count[0] += 1
            # 包装 dispose 以减少 ref_count
            original_dispose = sub.dispose

            def wrapped_dispose():
                try:
                    original_dispose()
                except Exception:
                    pass
                ref_count[0] -= 1
                if ref_count[0] <= 0:
                    if src_sub[0] is not None and hasattr(src_sub[0], 'dispose'):
                        try:
                            src_sub[0].dispose()
                        except Exception:
                            pass
                    src_sub[0] = None
                    ref_count[0] = 0
            sub.dispose = wrapped_dispose
            return sub

        def _sub(observer):
            return subscribe(observer)
        return _PyObservable(_sub)

    def publish(self):
        """publish：类似 share，但需要手动 connect()。简化版等同 share。"""
        return self.share()

    # ---------- None 处理 & 数学工具 ----------
    def drop_none(self):
        """过滤掉值为 None 的元素。"""
        return self.filter(lambda x: x is not None)

    def fill_none(self, default_value):
        """将 None 替换为 default_value。"""
        return self.map(lambda x: default_value if x is None else x)

    def abs(self):
        """对每个值取绝对值。"""
        return self.map(lambda x: x if x >= 0 else -x)

    def clamp(self, min_val, max_val):
        """将值限制在 [min_val, max_val] 区间。"""
        return self.map(lambda x: max(min_val, min(max_val, x)))

    # ---------- 嵌套展开 ----------
    def explode(self):
        """Iterable 展开为逐个值发射；str/bytes 作为单个值。"""
        def _sub(observer):
            def wrapped(value):
                if isinstance(value, (str, bytes)):
                    observer(value)
                    return
                try:
                    for item in value:
                        observer(item)
                except TypeError:
                    observer(value)
            self.subscribe(wrapped)
            return Subscription()
        return _PyObservable(_sub)

    def flatten(self):
        """同 explode()。展平嵌套序列。"""
        return self.explode()

    def dispatch_to_workers(self, fn=None, num_workers=4, buffer_size=0,
                            on_drop=None, drop_strategy="oldest", **kwargs):
        """按闲/忙状态分发到 worker 池（带并发上限的 flat_map）。

        核心语义:
          - 上游每个值 -> 找一个"空闲"的 worker -> 调用 ``fn(value)`` -> 结果发给下游
          - worker "忙"期间不会再分配新值
          - 所有 worker 都忙时: 新值进入缓冲队列
          - 缓冲队列满时: 按 ``drop_strategy`` 丢弃（并调用 ``on_drop``）
          - 结果按"先完成先发出"的顺序输出。

        Args:
            fn: 每个值的处理函数，支持同步函数
            num_workers: 最大并发 worker 数，必须 >= 1（默认 4）
            buffer_size: 缓冲队列大小，0 表示不限（可能导致内存无限增长）
            on_drop: 值被丢弃时的回调，接收被丢弃的值作为唯一参数
            drop_strategy: ``"oldest"`` 缓冲满时丢弃最旧的；
                            ``"newest"`` 缓冲满时丢弃新来的

        Returns:
            Observable: 发射 fn 处理后的结果流
        """
        if num_workers < 1:
            raise ValueError("num_workers must be >= 1, got %d" % num_workers)
        if drop_strategy not in ("oldest", "newest"):
            raise ValueError(
                "drop_strategy must be 'oldest' or 'newest', got %r"
                % drop_strategy
            )
        if fn is None:
            fn = lambda x: x

        def _sub(observer):
            from collections import deque
            from concurrent.futures import ThreadPoolExecutor

            executor = ThreadPoolExecutor(max_workers=num_workers)
            state_lock = _threading.Lock()
            done_event = _threading.Event()
            state = {
                "buffer": deque(),
                "active": 0,
                "closed": False,
                "errored": False,
                "completed_called": False,
            }

            def submit_next_from_buffer():
                """从 buffer 中取一项提交到线程池（无竞态: 返回前释放锁）。"""
                with state_lock:
                    if state["errored"] or state["completed_called"]:
                        return False
                    if state["active"] >= num_workers or len(state["buffer"]) == 0:
                        return False
                    value = state["buffer"].popleft()
                    state["active"] += 1

                def worker_task(v=value):
                    try:
                        result = fn(v)
                    except Exception as e:
                        with state_lock:
                            state["active"] -= 1
                            if (not state["errored"]
                                    and not state["completed_called"]
                                    and hasattr(observer, "on_error")):
                                observer.on_error(e)
                        _check_and_signal_done()
                        return

                    try:
                        if hasattr(observer, "on_next"):
                            observer.on_next(result)
                        elif callable(observer):
                            observer(result)
                    except Exception as e:
                        with state_lock:
                            state["errored"] = True
                        _check_and_signal_done()
                        return

                    should_finish = False
                    with state_lock:
                        state["active"] -= 1
                        if (state["closed"] and not state["errored"]
                                and not state["completed_called"]
                                and state["active"] == 0
                                and len(state["buffer"]) == 0):
                            state["completed_called"] = True
                            should_finish = True

                    if should_finish:
                        if hasattr(observer, "on_completed"):
                            observer.on_completed()
                        done_event.set()
                        return

                    submit_next_from_buffer()

                executor.submit(worker_task)
                return True

            def pump_buffer():
                """尽可能从 buffer 提交任务（直到 active 满或 buffer 空）。"""
                while submit_next_from_buffer():
                    pass

            def _check_and_signal_done():
                with state_lock:
                    if ((state["closed"] or state["errored"])
                            and state["active"] == 0
                            and len(state["buffer"]) == 0):
                        done_event.set()

            def on_next(value):
                with state_lock:
                    if state["errored"] or state["completed_called"] or state["closed"]:
                        return
                    full = (buffer_size > 0
                            and state["active"] >= num_workers
                            and len(state["buffer"]) >= buffer_size)
                    if full:
                        if drop_strategy == "oldest":
                            dropped = state["buffer"].popleft()
                        else:
                            dropped = value
                        if on_drop is not None:
                            try:
                                on_drop(dropped)
                            except Exception:
                                pass
                        if drop_strategy == "newest":
                            return
                    state["buffer"].append(value)
                pump_buffer()

            def on_error(error):
                with state_lock:
                    if state["errored"] or state["completed_called"]:
                        return
                    state["errored"] = True
                    state["buffer"].clear()
                if hasattr(observer, "on_error"):
                    observer.on_error(error)
                done_event.set()

            def on_completed():
                with state_lock:
                    if state["closed"] or state["errored"] or state["completed_called"]:
                        return
                    state["closed"] = True
                    if state["active"] == 0 and len(state["buffer"]) == 0:
                        state["completed_called"] = True
                        if hasattr(observer, "on_completed"):
                            observer.on_completed()
                        done_event.set()

            source_sub = self.subscribe(
                on_next=on_next,
                on_error=on_error,
                on_completed=on_completed,
            )

            def unsubscribe():
                with state_lock:
                    state["closed"] = True
                    state["errored"] = True
                    state["buffer"].clear()
                done_event.set()
                try:
                    executor.shutdown(wait=False, cancel_futures=True)
                except TypeError:
                    executor.shutdown(wait=False)
                if hasattr(source_sub, "unsubscribe"):
                    source_sub.unsubscribe()
                elif hasattr(source_sub, "dispose"):
                    source_sub.dispose()

            # 等待所有 worker 完成（对同步源场景很重要）
            done_event.wait()
            try:
                executor.shutdown(wait=True)
            except Exception:
                pass

            return Subscription(unsubscribe)

        return _PyObservable(_sub)

    def dispatch_workers(self, fn=None, num_workers=4, buffer_size=0,
                        on_drop=None, drop_strategy="oldest", **kwargs):
        """``dispatch_to_workers`` 的短别名。"""
        return self.dispatch_to_workers(
            fn=fn, num_workers=num_workers, buffer_size=buffer_size,
            on_drop=on_drop, drop_strategy=drop_strategy, **kwargs
        )

    def run(self):
        self.subscribe(lambda _: None)
        return self


# ============================================================================
# Subject - 广播型主题
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

    # ---------- 统计聚合 ----------
    def min(self):
        """发射流中的最小值。空流不发射。"""
        if _USE_RUST and hasattr(self._inner, "min"):
            try:
                return Observable(self._inner.min())
            except Exception:
                pass
        return Observable(self._inner.min())

    def max(self):
        """发射流中的最大值。空流不发射。"""
        if _USE_RUST and hasattr(self._inner, "max"):
            try:
                return Observable(self._inner.max())
            except Exception:
                pass
        return Observable(self._inner.max())

    def mean(self):
        """发射数值的平均值。空流不发射。"""
        if _USE_RUST and hasattr(self._inner, "mean"):
            try:
                return Observable(self._inner.mean())
            except Exception:
                pass
        return Observable(self._inner.mean())

    def average(self):
        """同 mean()。发射数值的平均值。"""
        return self.mean()

    def median(self):
        """发射中位数（需要缓冲所有值，仅对有限流适用）。"""
        if _USE_RUST and hasattr(self._inner, "median"):
            try:
                return Observable(self._inner.median())
            except Exception:
                pass
        return Observable(self._inner.median())

    def variance(self, ddof=0):
        """发射方差。ddof=0 为总体方差，ddof=1 为样本方差。"""
        if _USE_RUST and hasattr(self._inner, "variance"):
            try:
                return Observable(self._inner.variance(ddof))
            except Exception:
                pass
        return Observable(self._inner.variance(ddof))

    def std(self, ddof=0):
        """发射标准差。"""
        if _USE_RUST and hasattr(self._inner, "std"):
            try:
                return Observable(self._inner.std(ddof))
            except Exception:
                pass
        return Observable(self._inner.std(ddof))

    def quantile(self, q):
        """发射分位数。q ∈ [0, 1]。0.5 为中位数。"""
        if _USE_RUST and hasattr(self._inner, "quantile"):
            try:
                return Observable(self._inner.quantile(q))
            except Exception:
                pass
        return Observable(self._inner.quantile(q))

    def arg_min(self):
        """发射最小值的下标索引（从 0 开始）。"""
        if _USE_RUST and hasattr(self._inner, "arg_min"):
            try:
                return Observable(self._inner.arg_min())
            except Exception:
                pass
        return Observable(self._inner.arg_min())

    def arg_max(self):
        """发射最大值的下标索引（从 0 开始）。"""
        if _USE_RUST and hasattr(self._inner, "arg_max"):
            try:
                return Observable(self._inner.arg_max())
            except Exception:
                pass
        return Observable(self._inner.arg_max())

    def n_unique(self):
        """发射流中不重复值的数量。"""
        if _USE_RUST and hasattr(self._inner, "n_unique"):
            try:
                return Observable(self._inner.n_unique())
            except Exception:
                pass
        return Observable(self._inner.n_unique())

    def any(self, predicate):
        """只要有一个值满足谓词就发射 True，遍历结束无则发射 False。"""
        if _USE_RUST and hasattr(self._inner, "any"):
            try:
                return Observable(self._inner.any(predicate))
            except Exception:
                pass
        return Observable(self._inner.any(predicate))

    # ---------- 滚动窗口 ----------
    def rolling_min(self, window_size):
        """维护最近 window_size 个值的滚动最小值。窗口未满也发射。"""
        if _USE_RUST and hasattr(self._inner, "rolling_min"):
            try:
                return Observable(self._inner.rolling_min(window_size))
            except Exception:
                pass
        return Observable(self._inner.rolling_min(window_size))

    def rolling_max(self, window_size):
        """维护最近 window_size 个值的滚动最大值。"""
        if _USE_RUST and hasattr(self._inner, "rolling_max"):
            try:
                return Observable(self._inner.rolling_max(window_size))
            except Exception:
                pass
        return Observable(self._inner.rolling_max(window_size))

    def rolling_sum(self, window_size):
        """维护最近 window_size 个值的滚动和。"""
        if _USE_RUST and hasattr(self._inner, "rolling_sum"):
            try:
                return Observable(self._inner.rolling_sum(window_size))
            except Exception:
                pass
        return Observable(self._inner.rolling_sum(window_size))

    def rolling_mean(self, window_size):
        """维护最近 window_size 个值的滚动均值。"""
        if _USE_RUST and hasattr(self._inner, "rolling_mean"):
            try:
                return Observable(self._inner.rolling_mean(window_size))
            except Exception:
                pass
        return Observable(self._inner.rolling_mean(window_size))

    # ---------- 累积变换 ----------
    def cum_sum(self):
        """每步累积求和。"""
        if _USE_RUST and hasattr(self._inner, "cum_sum"):
            try:
                return Observable(self._inner.cum_sum())
            except Exception:
                pass
        return Observable(self._inner.cum_sum())

    def cum_min(self):
        """每步累积最小值。"""
        if _USE_RUST and hasattr(self._inner, "cum_min"):
            try:
                return Observable(self._inner.cum_min())
            except Exception:
                pass
        return Observable(self._inner.cum_min())

    def cum_max(self):
        """每步累积最大值。"""
        if _USE_RUST and hasattr(self._inner, "cum_max"):
            try:
                return Observable(self._inner.cum_max())
            except Exception:
                pass
        return Observable(self._inner.cum_max())

    def cum_mean(self):
        """每步累积均值。"""
        if _USE_RUST and hasattr(self._inner, "cum_mean"):
            try:
                return Observable(self._inner.cum_mean())
            except Exception:
                pass
        return Observable(self._inner.cum_mean())

    def cum_prod(self):
        """每步累积乘积。"""
        if _USE_RUST and hasattr(self._inner, "cum_prod"):
            try:
                return Observable(self._inner.cum_prod())
            except Exception:
                pass
        return Observable(self._inner.cum_prod())

    # ---------- 排序 Top-N ----------
    def sort(self, key=None, reverse=False):
        """收集全部值排序后发射（仅对有限流适用）。"""
        if _USE_RUST and hasattr(self._inner, "sort"):
            try:
                return Observable(self._inner.sort(key, reverse))
            except Exception:
                pass
        return Observable(self._inner.sort(key, reverse))

    def top_k(self, k, key=None):
        """返回前 k 个最大值（堆实现，节省内存）。"""
        if _USE_RUST and hasattr(self._inner, "top_k"):
            try:
                return Observable(self._inner.top_k(k, key))
            except Exception:
                pass
        return Observable(self._inner.top_k(k, key))

    def bottom_k(self, k, key=None):
        """返回最小的 k 个值（堆实现）。"""
        if _USE_RUST and hasattr(self._inner, "bottom_k"):
            try:
                return Observable(self._inner.bottom_k(k, key))
            except Exception:
                pass
        return Observable(self._inner.bottom_k(k, key))

    # ---------- 过滤/选择算子 ----------
    def distinct(self):
        """去重：每个值只发射一次。"""
        if _USE_RUST and hasattr(self._inner, "distinct"):
            try:
                return Observable(self._inner.distinct())
            except Exception:
                pass
        return Observable(self._inner.distinct())

    def element_at(self, idx):
        """发射第 idx 个值（0-based）。越界不发射。"""
        if _USE_RUST and hasattr(self._inner, "element_at"):
            try:
                return Observable(self._inner.element_at(idx))
            except Exception:
                pass
        return Observable(self._inner.element_at(idx))

    def take_while(self, predicate):
        """满足谓词时取，遇到第一个不满足值即终止。"""
        if _USE_RUST and hasattr(self._inner, "take_while"):
            try:
                return Observable(self._inner.take_while(predicate))
            except Exception:
                pass
        return Observable(self._inner.take_while(predicate))

    def skip_while(self, predicate):
        """跳过满足谓词的值，直到第一个不满足后全部发射。"""
        if _USE_RUST and hasattr(self._inner, "skip_while"):
            try:
                return Observable(self._inner.skip_while(predicate))
            except Exception:
                pass
        return Observable(self._inner.skip_while(predicate))

    def take_last(self, n):
        """取最后 n 个值（需要缓冲）。"""
        if _USE_RUST and hasattr(self._inner, "take_last"):
            try:
                return Observable(self._inner.take_last(n))
            except Exception:
                pass
        return Observable(self._inner.take_last(n))

    def skip_last(self, n):
        """跳过最后 n 个值（需要缓冲）。"""
        if _USE_RUST and hasattr(self._inner, "skip_last"):
            try:
                return Observable(self._inner.skip_last(n))
            except Exception:
                pass
        return Observable(self._inner.skip_last(n))

    # ---------- 组合算子 ----------
    def switch_map(self, mapper):
        """新值到来时取消前一个内层订阅，切换到新的 Observable。"""
        if _USE_RUST and hasattr(self._inner, "switch_map"):
            try:
                return Observable(self._inner.switch_map(mapper))
            except Exception:
                pass
        return Observable(self._inner.switch_map(mapper))

    def combine_latest(self, other, combiner):
        """两边都有过值后，任一方更新都发射最新组合。"""
        if _USE_RUST and hasattr(self._inner, "combine_latest"):
            try:
                return Observable(self._inner.combine_latest(other, combiner))
            except Exception:
                pass
        return Observable(self._inner.combine_latest(other, combiner))

    # ---------- 错误处理 ----------
    def catch_error(self, handler):
        """异常时调用 handler(err) 返回新的 Observable 继续。"""
        if _USE_RUST and hasattr(self._inner, "catch_error"):
            try:
                return Observable(self._inner.catch_error(handler))
            except Exception:
                pass
        return Observable(self._inner.catch_error(handler))

    def retry(self, count):
        """失败时重试最多 count 次。"""
        if _USE_RUST and hasattr(self._inner, "retry"):
            try:
                return Observable(self._inner.retry(count))
            except Exception:
                pass
        return Observable(self._inner.retry(count))

    def retry_with_delay(self, count, delay_seconds):
        """失败后延迟 delay_seconds 再重试。"""
        if _USE_RUST and hasattr(self._inner, "retry_with_delay"):
            try:
                return Observable(self._inner.retry_with_delay(count, delay_seconds))
            except Exception:
                pass
        return Observable(self._inner.retry_with_delay(count, delay_seconds))

    # ---------- 轻量过滤 ----------
    def distinct_until_changed(self):
        """只当值与上一个不同时才发射。"""
        if _USE_RUST and hasattr(self._inner, "distinct_until_changed"):
            try:
                return Observable(self._inner.distinct_until_changed())
            except Exception:
                pass
        return Observable(self._inner.distinct_until_changed())

    def ignore_elements(self):
        """不发射任何值，只转发完成信号。"""
        if _USE_RUST and hasattr(self._inner, "ignore_elements"):
            try:
                return Observable(self._inner.ignore_elements())
            except Exception:
                pass
        return Observable(self._inner.ignore_elements())

    # ---------- 多播 share/publish ----------
    def share(self):
        """多个订阅者共享同一个源订阅。"""
        if _USE_RUST and hasattr(self._inner, "share"):
            try:
                return Observable(self._inner.share())
            except Exception:
                pass
        return Observable(self._inner.share())

    def publish(self):
        """publish：类似 share，但需要手动 connect()。简化版等同 share。"""
        return self.share()

    def dispatch_to_workers(self, fn=None, num_workers=4, buffer_size=0,
                            on_drop=None, drop_strategy="oldest", **kwargs):
        """按闲/忙状态分发到 worker 池（带并发上限的 flat_map）。

        核心语义:
          - 上游每个值 -> 找一个"空闲"的 worker -> 调用 ``fn(value)`` -> 结果发给下游
          - worker "忙"期间不会再分配新值
          - 所有 worker 都忙时: 新值进入缓冲队列
          - 缓冲队列满时: 按 ``drop_strategy`` 丢弃（并调用 ``on_drop``）
          - 结果按"先完成先发出"的顺序输出。

        Args:
            fn: 每个值的处理函数，支持同步函数
            num_workers: 最大并发 worker 数，必须 >= 1（默认 4）
            buffer_size: 缓冲队列大小，0 表示不限（可能导致内存无限增长）
            on_drop: 值被丢弃时的回调，接收被丢弃的值作为唯一参数
            drop_strategy: ``"oldest"`` 缓冲满时丢弃最旧的；
                            ``"newest"`` 缓冲满时丢弃新来的

        Returns:
            Observable: 发射 fn 处理后的结果流
        """
        if _USE_RUST and hasattr(self._inner, "dispatch_to_workers"):
            try:
                return Observable(
                    self._inner.dispatch_to_workers(
                        fn=fn, num_workers=num_workers,
                        buffer_size=buffer_size, on_drop=on_drop,
                        drop_strategy=drop_strategy, **kwargs
                    )
                )
            except Exception:
                pass
        return Observable(
            self._inner.dispatch_to_workers(
                fn=fn, num_workers=num_workers,
                buffer_size=buffer_size, on_drop=on_drop,
                drop_strategy=drop_strategy, **kwargs
            )
        )

    def dispatch_workers(self, fn=None, num_workers=4, buffer_size=0,
                        on_drop=None, drop_strategy="oldest", **kwargs):
        """``dispatch_to_workers`` 的短别名。"""
        return self.dispatch_to_workers(
            fn=fn, num_workers=num_workers, buffer_size=buffer_size,
            on_drop=on_drop, drop_strategy=drop_strategy, **kwargs
        )

    # ---------- None 处理 & 数学工具 ----------
    def drop_none(self):
        """过滤掉值为 None 的元素。"""
        if _USE_RUST and hasattr(self._inner, "drop_none"):
            try:
                return Observable(self._inner.drop_none())
            except Exception:
                pass
        return Observable(self._inner.drop_none())

    def fill_none(self, default_value):
        """将 None 替换为 default_value。"""
        if _USE_RUST and hasattr(self._inner, "fill_none"):
            try:
                return Observable(self._inner.fill_none(default_value))
            except Exception:
                pass
        return Observable(self._inner.fill_none(default_value))

    def abs(self):
        """对每个值取绝对值。"""
        if _USE_RUST and hasattr(self._inner, "abs"):
            try:
                return Observable(self._inner.abs())
            except Exception:
                pass
        return Observable(self._inner.abs())

    def clamp(self, min_val, max_val):
        """将值限制在 [min_val, max_val] 区间。"""
        if _USE_RUST and hasattr(self._inner, "clamp"):
            try:
                return Observable(self._inner.clamp(min_val, max_val))
            except Exception:
                pass
        return Observable(self._inner.clamp(min_val, max_val))

    # ---------- 嵌套展开 ----------
    def explode(self):
        """Iterable 展开为逐个值发射；str/bytes 作为单个值。"""
        if _USE_RUST and hasattr(self._inner, "explode"):
            try:
                return Observable(self._inner.explode())
            except Exception:
                pass
        return Observable(self._inner.explode())

    def flatten(self):
        """同 explode()。展平嵌套序列。"""
        return self.explode()

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

    @staticmethod
    def min():
        return lambda obs: obs.min()

    @staticmethod
    def max():
        return lambda obs: obs.max()

    @staticmethod
    def mean():
        return lambda obs: obs.mean()

    @staticmethod
    def average():
        return lambda obs: obs.average()

    @staticmethod
    def median():
        return lambda obs: obs.median()

    @staticmethod
    def variance(ddof=0):
        return lambda obs: obs.variance(ddof)

    @staticmethod
    def std(ddof=0):
        return lambda obs: obs.std(ddof)

    @staticmethod
    def quantile(q):
        return lambda obs: obs.quantile(q)

    @staticmethod
    def arg_min():
        return lambda obs: obs.arg_min()

    @staticmethod
    def arg_max():
        return lambda obs: obs.arg_max()

    @staticmethod
    def n_unique():
        return lambda obs: obs.n_unique()

    @staticmethod
    def any_op(predicate):
        return lambda obs: obs.any(predicate)

    @staticmethod
    def rolling_min(window_size):
        return lambda obs: obs.rolling_min(window_size)

    @staticmethod
    def rolling_max(window_size):
        return lambda obs: obs.rolling_max(window_size)

    @staticmethod
    def rolling_sum(window_size):
        return lambda obs: obs.rolling_sum(window_size)

    @staticmethod
    def rolling_mean(window_size):
        return lambda obs: obs.rolling_mean(window_size)

    @staticmethod
    def cum_sum():
        return lambda obs: obs.cum_sum()

    @staticmethod
    def cum_min():
        return lambda obs: obs.cum_min()

    @staticmethod
    def cum_max():
        return lambda obs: obs.cum_max()

    @staticmethod
    def cum_mean():
        return lambda obs: obs.cum_mean()

    @staticmethod
    def cum_prod():
        return lambda obs: obs.cum_prod()

    @staticmethod
    def sort(key=None, reverse=False):
        return lambda obs: obs.sort(key, reverse)

    @staticmethod
    def top_k(k, key=None):
        return lambda obs: obs.top_k(k, key)

    @staticmethod
    def bottom_k(k, key=None):
        return lambda obs: obs.bottom_k(k, key)

    @staticmethod
    def distinct():
        return lambda obs: obs.distinct()

    @staticmethod
    def element_at(idx):
        return lambda obs: obs.element_at(idx)

    @staticmethod
    def take_while(predicate):
        return lambda obs: obs.take_while(predicate)

    @staticmethod
    def skip_while(predicate):
        return lambda obs: obs.skip_while(predicate)

    @staticmethod
    def take_last(n):
        return lambda obs: obs.take_last(n)

    @staticmethod
    def skip_last(n):
        return lambda obs: obs.skip_last(n)

    @staticmethod
    def switch_map(mapper):
        return lambda obs: obs.switch_map(mapper)

    @staticmethod
    def combine_latest(other, combiner):
        return lambda obs: obs.combine_latest(other, combiner)

    @staticmethod
    def catch_error(handler):
        return lambda obs: obs.catch_error(handler)

    @staticmethod
    def retry(count):
        return lambda obs: obs.retry(count)

    @staticmethod
    def retry_with_delay(count, delay_seconds):
        return lambda obs: obs.retry_with_delay(count, delay_seconds)

    @staticmethod
    def distinct_until_changed():
        return lambda obs: obs.distinct_until_changed()

    @staticmethod
    def ignore_elements():
        return lambda obs: obs.ignore_elements()

    @staticmethod
    def share():
        return lambda obs: obs.share()

    @staticmethod
    def publish():
        return lambda obs: obs.publish()

    @staticmethod
    def drop_none():
        return lambda obs: obs.drop_none()

    @staticmethod
    def fill_none(default_value):
        return lambda obs: obs.fill_none(default_value)

    @staticmethod
    def abs_op():
        return lambda obs: obs.abs()

    @staticmethod
    def clamp(min_val, max_val):
        return lambda obs: obs.clamp(min_val, max_val)

    @staticmethod
    def explode():
        return lambda obs: obs.explode()

    @staticmethod
    def flatten():
        return lambda obs: obs.flatten()

    @staticmethod
    def sum():
        return lambda obs: obs.sum()

    @staticmethod
    def dispatch_to_workers(fn=None, num_workers=4, buffer_size=0,
                            on_drop=None, drop_strategy="oldest", **kwargs):
        return lambda obs: obs.dispatch_to_workers(
            fn=fn, num_workers=num_workers, buffer_size=buffer_size,
            on_drop=on_drop, drop_strategy=drop_strategy, **kwargs
        )

    @staticmethod
    def dispatch_workers(fn=None, num_workers=4, buffer_size=0,
                        on_drop=None, drop_strategy="oldest", **kwargs):
        return lambda obs: obs.dispatch_workers(
            fn=fn, num_workers=num_workers, buffer_size=buffer_size,
            on_drop=on_drop, drop_strategy=drop_strategy, **kwargs
        )

    @staticmethod
    def write_to_clipboard(dispatcher, source=None):
        """响应式操作符：把上游每一项写回剪贴板，并继续下发 ClipData。"""
        from .clipboard import write_to_clipboard as _wtc
        return _wtc(dispatcher, source=source)

    @staticmethod
    def write_to_filesystem(dispatcher, mode="create"):
        """响应式操作符：把上游每一项写入文件系统，并继续下发 FileData。"""
        from .file_watcher import write_to_filesystem as _wtfs
        return _wtfs(dispatcher, mode=mode)

    @staticmethod
    def write_to_foldersystem(dispatcher, mode="create"):
        """响应式操作符：把上游每一项写入目录系统，并继续下发 FolderData。"""
        from .folder_watcher import write_to_foldersystem as _wtfs2
        return _wtfs2(dispatcher, mode=mode)


ops = _OpModule()


# ============================================================================
# 导出符号
# ============================================================================

# 剪贴板响应式模块
from .clipboard import (
    ChangeType,
    ClipData,
    ClipboardDispatcher,
    ClipSubject,
    ClipObserver,
    from_clipboard,
    write_to_clipboard,
)

# 文件系统监控模块
from .file_watcher import (
    FileChangeType,
    FileData,
    FileDispatcher,
    FileSubject,
    FileObserver,
    from_filesystem,
    write_to_filesystem,
)

# 目录系统监控模块（子系统级）
from .folder_watcher import (
    FolderChangeType,
    FolderData,
    FolderDispatcher,
    FolderSubject,
    FolderObserver,
    from_foldersystem,
    write_to_foldersystem,
)

# 键盘鼠标响应式模块
from .keyboard_mouse import (
    KeyEventType,
    KeyData,
    MouseEventType,
    MouseData,
    KeyModifier,
    KeyboardDispatcher,
    MouseDispatcher,
    KeySubject,
    MouseSubject,
    KeyObserver,
    MouseObserver,
    from_keyboard,
    from_mouse,
    write_to_keyboard,
    write_to_mouse,
)

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
    "ChangeType",
    "ClipData",
    "ClipboardDispatcher",
    "ClipSubject",
    "ClipObserver",
    "from_clipboard",
    "write_to_clipboard",
    "FileChangeType",
    "FileData",
    "FileDispatcher",
    "FileSubject",
    "FileObserver",
    "from_filesystem",
    "write_to_filesystem",
    "FolderChangeType",
    "FolderData",
    "FolderDispatcher",
    "FolderSubject",
    "FolderObserver",
    "from_foldersystem",
    "write_to_foldersystem",
    # 键盘鼠标
    "KeyEventType",
    "KeyData",
    "MouseEventType",
    "MouseData",
    "KeyModifier",
    "KeyboardDispatcher",
    "MouseDispatcher",
    "KeySubject",
    "MouseSubject",
    "KeyObserver",
    "MouseObserver",
    "from_keyboard",
    "from_mouse",
    "write_to_keyboard",
    "write_to_mouse",
]
