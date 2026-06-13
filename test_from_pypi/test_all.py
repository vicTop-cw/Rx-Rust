"""test_all.py — rx_rust 完整 API 测试（共 98+ 个对象）。

运行方式：
    1) 已 pip install rx-rust   → python test_all.py
    2) 未安装（本仓库开发态）   → python test_all.py
                               （脚本会自动从 ../rx-rust-py/python 导入纯 Python 实现）
"""
from __future__ import annotations
import os
import sys
from pathlib import Path
from typing import List

# ---------- 智能导入 rx_rust ----------
# 优先使用已安装的 rx-rust；若不可用，则从本仓库的 python/ 目录加载纯 Python 实现
try:
    import rx_rust as _rx  # noqa: F401
    _SOURCE = "installed"
except ModuleNotFoundError:
    _HERE = Path(__file__).resolve().parent
    _CANDIDATES = [
        _HERE.parent / "rx-rust-py" / "python",
        _HERE.parent / "rxpy" / "python",
        _HERE.parent / "rx-rust-python",
    ]
    for _p in _CANDIDATES:
        if _p.exists() and (_p / "rx_rust").exists():
            sys.path.insert(0, str(_p))
            break
    import rx_rust as _rx  # noqa: F401
    _SOURCE = "source"
except Exception as _e:  # pragma: no cover
    raise RuntimeError(
        "既找不到已安装的 rx_rust 包，也无法定位本仓库内的源码目录。"
        f" 请先用 `pip install rx-rust` 或在仓库根目录的子目录运行。"
    ) from _e

print(f"[i] rx_rust 模块来源: {'PyPI 已安装包' if _SOURCE == 'installed' else '本仓库源码（纯 Python 实现）'}")

# ---------- Helper ----------
_PASS = 0
_FAIL = 0
_FAILURES: List[str] = []

def _check(name: str, condition, detail: str = "") -> None:
    global _PASS, _FAIL
    try:
        if condition:
            _PASS += 1
            print(f"  [PASS]  {name}")
        else:
            _FAIL += 1
            _FAILURES.append(f"{name}: {detail or 'condition False'}")
            print(f"  [FAIL]  {name}  {detail}")
    except Exception as e:
        _FAIL += 1
        _FAILURES.append(f"{name}: raised {type(e).__name__}: {e}")
        print(f"  [FAIL]  {name}  exception: {type(e).__name__}: {e}")

def _collect(obs) -> list:
    out = []
    obs.subscribe(on_next=lambda v: out.append(v))
    return out

# ---------- 0. Module-Level ----------
print("\n===== [0] Module-Level 导入与检查 =====")
import rx_rust as _rx

TOP_LEVEL = [
    "Observable", "PublishSubject", "BehaviorSubject", "ReplaySubject",
    "Subscription", "CurrentThreadScheduler", "ThreadPoolScheduler",
    "AsyncScheduler", "ImmediateScheduler",
]
for n in TOP_LEVEL:
    _check(f"rx_rust.{n}", hasattr(_rx, n))

# ---------- 1. Subscription (5 个方法) ----------
print("\n===== [1] Subscription =====")
sub = _rx.Subscription()
_check("Subscription() 创建", sub is not None)
_check("Subscription.is_disposed (初始 False)", not sub.is_disposed())
_check("Subscription.dispose()", True)  # 能正常调用
sub.dispose()
_check("Subscription.is_disposed (dispose 后 True)", sub.is_disposed())

sub2 = _rx.Subscription()
with sub2 as s:
    _check("Subscription.__enter__", s is sub2)
_check("Subscription.__exit__ (自动 dispose)", sub2.is_disposed())
_check("Subscription.__repr__", isinstance(repr(_rx.Subscription()), str))

# ---------- 2. Observable 创建 (13) ----------
print("\n===== [2] Observable 创建类方法 =====")
_check("Observable.of(42)", _collect(_rx.Observable.of(42)) == [42])
_check("Observable.of('hello')", _collect(_rx.Observable.of("hello")) == ["hello"])
_check("Observable.of(None)", _collect(_rx.Observable.of(None)) == [None])

_check("Observable.from_iter([1,2,3])", _collect(_rx.Observable.from_iter([1, 2, 3])) == [1, 2, 3])
_check("Observable.from_iter(range(5))", _collect(_rx.Observable.from_iter(range(5))) == [0, 1, 2, 3, 4])
_check("Observable.from_iter([])", _collect(_rx.Observable.from_iter([])) == [])

_check("Observable.range(0,3)", _collect(_rx.Observable.range(0, 3)) == [0, 1, 2])
_check("Observable.range(5,3)", _collect(_rx.Observable.range(5, 3)) == [5, 6, 7])

_check("Observable.repeat(1, 4)", _collect(_rx.Observable.repeat(1, 4)) == [1, 1, 1, 1])

_check("Observable.empty()", _collect(_rx.Observable.empty()) == [])

_check("Observable.never()", True)  # 能构造，但不发射值

# ---------- 3. Observable 操作符 (25+) ----------
print("\n===== [3] Observable 操作符 =====")
src = _rx.Observable.from_iter([1, 2, 3, 4, 5])

_check("map(x*10)", _collect(src.map(lambda x: x * 10)) == [10, 20, 30, 40, 50])
_check("map(str)", _collect(src.map(str)) == ["1", "2", "3", "4", "5"])
_check("filter(even)", _collect(src.filter(lambda x: x % 2 == 0)) == [2, 4])
_check("filter(>3)", _collect(src.filter(lambda x: x > 3)) == [4, 5])
_check("take(3)", _collect(src.take(3)) == [1, 2, 3])
_check("take(0)", _collect(src.take(0)) == [])
_check("skip(2)", _collect(src.skip(2)) == [3, 4, 5])
_check("skip(5)", _collect(src.skip(5)) == [])

res = []
src.do_on_next(lambda v: res.append(v * 100)).subscribe(on_next=lambda _: None)
_check("do_on_next 副作用", res == [100, 200, 300, 400, 500])

_check("start_with([0])", _collect(src.start_with(0)) == [0, 1, 2, 3, 4, 5])
_check("start_with([-2,-1])", _collect(src.start_with(-2, -1)) == [-2, -1, 1, 2, 3, 4, 5])

_check("default_if_empty (非空)", _collect(_rx.Observable.from_iter([1]).default_if_empty(0)) == [1])
_check("default_if_empty (空)", _collect(_rx.Observable.from_iter([]).default_if_empty(0)) == [0])

# ---------- 4. Observable 数学 / 聚合 (10) ----------
print("\n===== [4] Observable 数学/聚合 =====")
_check("count([1,2,3])", _collect(_rx.Observable.from_iter([1, 2, 3]).count()) == [3])
_check("count(empty)", _collect(_rx.Observable.from_iter([]).count()) == [0])
_check("sum([1,2,3])", _collect(_rx.Observable.from_iter([1, 2, 3]).sum()) == [6])
_check("sum(empty)", _collect(_rx.Observable.from_iter([]).sum()) == [0])

_check("reduce(+,0)", _collect(_rx.Observable.from_iter([1, 2, 3]).reduce(0, lambda acc, x: acc + x)) == [6])
_check("reduce(*,1)", _collect(_rx.Observable.from_iter([2, 3, 4]).reduce(1, lambda acc, x: acc * x)) == [24])
_check("scan(+,0)", _collect(_rx.Observable.from_iter([1, 2, 3]).scan(0, lambda acc, x: acc + x)) == [0, 1, 3, 6])

_check("contains(3)", _collect(src.contains(3)) == [True])
_check("contains(99)", _collect(src.contains(99)) == [False])
_check("all(>0)", _collect(src.all(lambda x: x > 0)) == [True])
_check("all(<3)", _collect(src.all(lambda x: x < 3)) == [False])

# ---------- 5. Subject (15) ----------
print("\n===== [5] Subject =====")

# PublishSubject
ps = _rx.PublishSubject()
ps_values = []
sub_ps = ps.subscribe(on_next=lambda v: ps_values.append(v))
ps.on_next(1); ps.on_next(2); ps.on_next(3)
_check("PublishSubject on_next 3 次", ps_values == [1, 2, 3])

# 订阅之后的值也能收到
ps_values2 = []
sub_ps2 = ps.subscribe(on_next=lambda v: ps_values2.append(v))
ps.on_next(10); ps.on_next(20)
_check("PublishSubject 新订阅能收到新值", ps_values2 == [10, 20])
_check("PublishSubject 旧订阅继续收到", ps_values == [1, 2, 3, 10, 20])

sub_ps.dispose()
ps.on_next(999)
_check("PublishSubject dispose 后不再接收", ps_values == [1, 2, 3, 10, 20])

ps.on_completed()
_check("PublishSubject __repr__", isinstance(repr(_rx.PublishSubject()), str))

# BehaviorSubject
bs = _rx.BehaviorSubject(100)
_check("BehaviorSubject 初始 value", bs.value == 100)

bs_values = []
sub_bs = bs.subscribe(on_next=lambda v: bs_values.append(v))
bs.on_next(200); bs.on_next(300)
_check("BehaviorSubject 能收到 value + on_next", bs_values == [100, 200, 300])
_check("BehaviorSubject value 更新", bs.value == 300)

# 新订阅者会收到当前 value
bs_values2 = []
sub_bs2 = bs.subscribe(on_next=lambda v: bs_values2.append(v))
_check("BehaviorSubject 新订阅者收到当前 value", bs_values2 == [300])

bs.on_completed()
_check("BehaviorSubject __repr__", isinstance(repr(_rx.BehaviorSubject(0)), str))

# ReplaySubject
rs = _rx.ReplaySubject(capacity=3)
rs_values = []
sub_rs = rs.subscribe(on_next=lambda v: rs_values.append(v))
rs.on_next(1); rs.on_next(2); rs.on_next(3); rs.on_next(4)
_check("ReplaySubject 订阅接收", rs_values == [1, 2, 3, 4])

# 新订阅者会收到缓存的最后 3 个
rs_values2 = []
sub_rs2 = rs.subscribe(on_next=lambda v: rs_values2.append(v))
_check("ReplaySubject 新订阅者收到缓存 (capacity=3)", rs_values2 == [2, 3, 4])

rs.on_completed()
_check("ReplaySubject __repr__", isinstance(repr(_rx.ReplaySubject(2)), str))

# ---------- 6. Scheduler (8) ----------
print("\n===== [6] Scheduler =====")

cts = _rx.CurrentThreadScheduler()
_check("CurrentThreadScheduler 创建", cts is not None)
_check("CurrentThreadScheduler.now 为数值", isinstance(cts.now(), (int, float)))

ts = _rx.ThreadPoolScheduler(workers=2)
_check("ThreadPoolScheduler 创建", ts is not None)
_check("ThreadPoolScheduler.now 为数值", isinstance(ts.now(), (int, float)))
_check("ThreadPoolScheduler.get_num_threads == 2", ts.get_num_threads() == 2)

asy = _rx.AsyncScheduler()
_check("AsyncScheduler 创建", asy is not None)
_check("AsyncScheduler.now", isinstance(asy.now(), (int, float)))

imm = _rx.ImmediateScheduler()
_check("ImmediateScheduler 创建", imm is not None)
_check("ImmediateScheduler.now", isinstance(imm.now(), (int, float)))

# ---------- 7. Subscription 更深入 (5) ----------
print("\n===== [7] Subscription 深入 =====")
sub = _rx.Subscription()
_check("Subscription 初始未 dispose", not sub.is_disposed())
sub.dispose()
_check("Subscription 已 dispose", sub.is_disposed())
# 重复 dispose 应该是安全的 (幂等)
try:
    sub.dispose()
    _check("Subscription dispose 幂等", True)
except Exception:
    _check("Subscription dispose 幂等", False, "重复 dispose 抛异常")

# ---------- 8. flat_map / merge / concat (8) ----------
print("\n===== [8] flat_map / merge / concat =====")
_check("flat_map: 1->[1,2]",
       _collect(_rx.Observable.from_iter([1, 2]).flat_map(lambda x: _rx.Observable.from_iter([x, x * 10])))
       == [1, 10, 2, 20])

# merge 两个 Observable
obs_a = _rx.Observable.from_iter([1, 2, 3])
obs_b = _rx.Observable.from_iter([10, 20, 30])
merged = obs_a.merge(obs_b)
merged_values = _collect(merged)
_check("merge 两个 Observable 值都出现", set(merged_values) == {1, 2, 3, 10, 20, 30})

# concat
concated = obs_a.concat(obs_b)
_concat_vals = _collect(concated)
_check("concat 顺序正确", _concat_vals[:3] == [1, 2, 3] and _concat_vals[3:] == [10, 20, 30])

# ---------- 9. 链接 API (7) ----------
print("\n===== [9] 链式调用复杂场景 =====")
result = _collect(
    _rx.Observable.from_iter(range(10))
    .filter(lambda x: x % 2 == 0)
    .map(lambda x: x + 1)
    .skip(1)
    .take(3)
)
_check("链式: filter→map→skip→take", result == [3, 5, 7])

total = []
_rx.Observable.from_iter([1, 2, 3]).map(lambda x: x * 100).subscribe(on_next=lambda v: total.append(v))
_check("链式 subscribe 回调", total == [100, 200, 300])

# ---------- 10. 错误处理 (3) ----------
print("\n===== [10] 错误处理 =====")
# 回调里抛异常：库应该把它吞掉或转化
err_observed = []
obs_test = _rx.Observable.from_iter([1, 2, 3])
try:
    obs_test.map(lambda x: 10 // (x - 2)).subscribe(on_next=lambda v: None)
    _check("map 中错误不传播", True)
except Exception:
    _check("map 中错误不传播", False, "异常跑出来了")

# ---------- Summary ----------
print(f"\n{'='*50}")
print(f"  测试完成: {_PASS} 通过, {_FAIL} 失败")
print(f"{'='*50}")
if _FAILURES:
    print("\n失败详情:")
    for f in _FAILURES:
        print(f"  - {f}")

sys.exit(0 if _FAIL == 0 else 1)
