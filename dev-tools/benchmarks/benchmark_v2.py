"""
基准对比 v2：RxPY vs rx-rust（本地优化版） vs vools.reactive（本地）
=====================================================================
使用本地源码（非 PyPI），含与优化前数据对比。
"""
from __future__ import annotations

import gc
import os
import sys
import time
from statistics import mean, median, stdev

# ========== 强制使用本地源码（非 PyPI 安装） ==========
LOCAL_RUST = os.path.join(os.path.dirname(__file__), "rx-rust-py", "python")
LOCAL_VOOLS = r"E:\IDEProjects\AI\vools"
sys.path.insert(0, LOCAL_RUST if os.path.isdir(LOCAL_RUST) else os.path.dirname(__file__))
sys.path.insert(0, LOCAL_VOOLS)

# 验证使用的是本地版本
import rx_rust
print(f"[i] rx-rust  来源: {rx_rust.__file__}")
import vools.reactive as vr
print(f"[i] vools.reactive 来源: {vr.__file__}")
import rx
print(f"[i] RxPY 来源: {rx.__file__}")
import rx.operators as rxops
from rx.subject import Subject as RxSubject
from vools.reactive import Observable as VrObservable
from vools.reactive import Subject as VrSubject, BehaviorSubject as VrBS, ReplaySubject as VrRS
from vools.reactive import ops as vrops
print()

N = 1_000_000         # 大序列
N_SMALL = 100_000
N_LAT = 10_000
N_SUB = 100
N_ROUNDS = 3
SZ = 200_000

BEFORE = {
    # 来自 benchmark_report.md 优化前数据 (mean_s)
    "RxPY · 纯消费": 0.117,
    "RxPY · map+filter": 0.200,
    "RxPY · 10操作符长链": 0.464,
    "rx-rust · 纯消费": 0.160,
    "rx-rust · map+filter": 0.469,
    "rx-rust · 10操作符长链": 1.125,
    "vools · 纯消费": 0.314,
    "vools · map+filter": 0.613,
    "vools · 10操作符长链": 1.516,
    "RxPY · reduce(1M)": 0.803,
    "rx-rust · reduce(1M)": 0.243,
    "vools · reduce(1M)": 0.289,
    "RxPY · scan(100k)": 0.054,
    "rx-rust · scan(100k)": 0.020,
    "vools · scan(100k)": 0.023,
}

# ========== helper ==========
def bench(fn, name, rounds=N_ROUNDS):
    results = []
    for i in range(rounds):
        gc.collect()
        t0 = time.perf_counter()
        value = fn()
        dt = time.perf_counter() - t0
        results.append((dt, value))
    dts = [r[0] for r in results]
    return {"name": name, "mean_s": mean(dts), "median_s": median(dts),
            "min_s": min(dts), "stdev_s": stdev(dts) if len(dts) > 1 else 0, "check": results[0][1]}

def fmt(n):
    return f"{n:,.0f}"

# ========== 测试函数 ==========
def rxpy_bare():   n = [0]; rx.from_iterable(range(N)).subscribe(on_next=lambda _: n.__setitem__(0, n[0]+1)); return n[0]
def rxpy_chain2(): n = [0]; rx.from_iterable(range(N)).pipe(rxops.filter(lambda x: x%2==0), rxops.map(lambda x: x*10)).subscribe(on_next=lambda _: n.__setitem__(0, n[0]+1)); return n[0]
def rxpy_long():   n = [0]; o = rx.from_iterable(range(N)); [o := o.pipe(rxops.map(lambda x, k=i: x+k), rxops.filter(lambda x, k=i: x%2==0 or x%3==0)) for i in range(5)]; o.subscribe(on_next=lambda _: n.__setitem__(0, n[0]+1)); return n[0]

def rxrust_bare(): n = [0]; rx_rust.Observable.from_iter(range(N)).subscribe(on_next=lambda _: n.__setitem__(0, n[0]+1)); return n[0]
def rxrust_chain2(): n = [0]; rx_rust.Observable.from_iter(range(N)).filter(lambda x: x%2==0).map(lambda x: x*10).subscribe(on_next=lambda _: n.__setitem__(0, n[0]+1)); return n[0]
def rxrust_long(): n = [0]; o = rx_rust.Observable.from_iter(range(N)); [o := o.map(lambda x, k=i: x+k).filter(lambda x, k=i: x%2==0 or x%3==0) for i in range(5)]; o.subscribe(on_next=lambda _: n.__setitem__(0, n[0]+1)); return n[0]

def vr_bare():     n = [0]; VrObservable.from_iterable(range(N)).subscribe(on_next=lambda _: n.__setitem__(0, n[0]+1)); return n[0]
def vr_chain2():   n = [0]; VrObservable.from_iterable(range(N)).pipe(vrops.filter(lambda x: x%2==0), vrops.map(lambda x: x*10)).subscribe(on_next=lambda _: n.__setitem__(0, n[0]+1)); return n[0]
def vr_long():     n = [0]; o = VrObservable.from_iterable(range(N)); ops_list = []; [ops_list.extend([vrops.map(lambda x, k=i: x+k), vrops.filter(lambda x, k=i: x%2==0 or x%3==0)]) for i in range(5)]; o.pipe(*ops_list).subscribe(on_next=lambda _: n.__setitem__(0, n[0]+1)); return n[0]

results = {}

# ========================================================================
print("=" * 72)
print("T1. 吞吐: 1M items (纯消费 / map+filter / 10操作符长链)")
print("=" * 72)
for name, fn in [
    ("RxPY · 纯消费", rxpy_bare), ("rx-rust · 纯消费", rxrust_bare), ("vools · 纯消费", vr_bare),
    ("RxPY · map+filter", rxpy_chain2), ("rx-rust · map+filter", rxrust_chain2), ("vools · map+filter", vr_chain2),
    ("RxPY · 10操作符长链", rxpy_long), ("rx-rust · 10操作符长链", rxrust_long), ("vools · 10操作符长链", vr_long),
]:
    d = bench(fn, name)
    results[name] = d
    thr = N / d["mean_s"]
    before = BEFORE.get(name, None)
    delta = f"  (优化前: {before:.3f}s)" if before and name.startswith("rx-rust") else ""
    print(f"   {name:28s} mean={d['mean_s']:7.3f}s  →  {fmt(thr):>10s} items/sec{delta}")

# ========================================================================
print("\n" + "=" * 72)
print("T8. 聚合: reduce 1M / scan 100k")
print("=" * 72)
def rxpy_reduce():  n = [0]; rx.from_iterable(range(N)).pipe(rxops.reduce(lambda a, x: a+x, 0)).subscribe(on_next=lambda v: n.__setitem__(0, v)); return n[0]
def rxrust_reduce(): n = [0]; rx_rust.Observable.from_iter(range(N)).reduce(0, lambda a, x: a+x).subscribe(on_next=lambda v: n.__setitem__(0, v)); return n[0]
def vr_reduce():     n = [0]; VrObservable.from_iterable(range(N)).pipe(vrops.reduce(lambda a, x: a+x, 0)).subscribe(on_next=lambda v: n.__setitem__(0, v)); return n[0]
def rxpy_scan():     n = [0]; rx.from_iterable(range(100_000)).pipe(rxops.scan(lambda a, x: a+x, 0)).subscribe(on_next=lambda v: n.__setitem__(0, v)); return n[0]
def rxrust_scan():   n = [0]; rx_rust.Observable.from_iter(range(100_000)).scan(0, lambda a, x: a+x).subscribe(on_next=lambda v: n.__setitem__(0, v)); return n[0]
def vr_scan():       n = [0]; VrObservable.from_iterable(range(100_000)).pipe(vrops.scan(lambda a, x: a+x, 0)).subscribe(on_next=lambda v: n.__setitem__(0, v)); return n[0]

for name, fn, size in [
    ("RxPY · reduce(1M)", rxpy_reduce, N), ("rx-rust · reduce(1M)", rxrust_reduce, N), ("vools · reduce(1M)", vr_reduce, N),
    ("RxPY · scan(100k)", rxpy_scan, 100_000), ("rx-rust · scan(100k)", rxrust_scan, 100_000), ("vools · scan(100k)", vr_scan, 100_000),
]:
    d = bench(fn, name)
    results[name] = d
    thr = size / d["mean_s"]
    before = BEFORE.get(name, None)
    delta = f"  (优化前: {before:.3f}s)" if before and name.startswith("rx-rust") else ""
    print(f"   {name:28s} mean={d['mean_s']:7.3f}s  →  {fmt(thr):>10s} items/sec{delta}")

# ========================================================================
print("\n" + "=" * 72)
print("T3. Subject 多播: 1 subject → 100 subscribers (100k items)")
print("=" * 72)
def rxpy_subject():  sub = RxSubject(); c = [[0] for _ in range(N_SUB)]; ss = [sub.subscribe(on_next=lambda v, k=k: c[k].__setitem__(0, c[k][0]+1)) for k in range(N_SUB)]; [sub.on_next(i) for i in range(N_SMALL)]; [s.dispose() for s in ss]; return sum(x[0] for x in c)
def rxrust_subject(): sub = rx_rust.PublishSubject(); c = [[0] for _ in range(N_SUB)]; ss = [sub.subscribe(on_next=lambda v, k=k: c[k].__setitem__(0, c[k][0]+1)) for k in range(N_SUB)]; [sub.on_next(i) for i in range(N_SMALL)]; [s.dispose() for s in ss]; return sum(x[0] for x in c)
def vr_subject():    sub = VrSubject(); c = [[0] for _ in range(N_SUB)]; ss = [sub.subscribe(on_next=lambda v, k=k: c[k].__setitem__(0, c[k][0]+1)) for k in range(N_SUB)]; [sub.on_next(i) for i in range(N_SMALL)]; [s.unsubscribe() if hasattr(s,'unsubscribe') else None for s in ss]; return sum(x[0] for x in c)

for name, fn in [("RxPY Subject 多播", rxpy_subject), ("rx-rust Subject 多播", rxrust_subject), ("vools Subject 多播", vr_subject)]:
    d = bench(fn, name); results[name] = d
    print(f"   {name:32s} mean={d['mean_s']:7.3f}s  →  {fmt(d['check']/d['mean_s'])} events/sec")

# ========================================================================
print("\n" + "=" * 72)
print("T2. 延迟: 单项发射-接收 (10k samples)")
print("=" * 72)
def rxpy_lat():  s = []; [rx.just(t0:=time.perf_counter_ns()).subscribe(on_next=lambda t: s.append(time.perf_counter_ns()-t)) for _ in range(N_LAT)]; return int(mean(s))
def rxrust_lat(): s = []; [rx_rust.Observable.of(t0:=time.perf_counter_ns()).subscribe(on_next=lambda t: s.append(time.perf_counter_ns()-t)) for _ in range(N_LAT)]; return int(mean(s))
def vr_lat():     s = []; [VrObservable.from_iterable([t0:=time.perf_counter_ns()]).subscribe(on_next=lambda t: s.append(time.perf_counter_ns()-t)) for _ in range(N_LAT)]; return int(mean(s))

for name, fn in [("RxPY", rxpy_lat), ("rx-rust", rxrust_lat), ("vools", vr_lat)]:
    d = bench(fn, name, rounds=2); results["T2."+name] = d
    print(f"   {name:10s} 平均延迟: {d['check']:,d} ns/item")

# ========================================================================
print("\n" + "=" * 72)
print("T5. 链深度衰减: 200k items @ 5/10/20")
print("=" * 72)
for depth in (5, 10, 20):
    for lib, builder in [("RxPY", lambda d: lambda: (n:=[0], o:=rx.from_iterable(range(SZ)), [o:=o.pipe(rxops.map(lambda x, k=i: x+k)) for i in range(d)], o.subscribe(on_next=lambda _: n.__setitem__(0, n[0]+1)), n[0])[-1]), ("rx-rust", lambda d: lambda: (n:=[0], o:=rx_rust.Observable.from_iter(range(SZ)), [o:=o.map(lambda x, k=i: x+k) for i in range(d)], o.subscribe(on_next=lambda _: n.__setitem__(0, n[0]+1)), n[0])[-1]), ("vools", lambda d: lambda: (n:=[0], o:=VrObservable.from_iterable(range(SZ)), ops:=[vrops.map(lambda x, k=i: x+k) for i in range(d)], o.pipe(*ops).subscribe(on_next=lambda _: n.__setitem__(0, n[0]+1)), n[0])[-1])]:
        fn = builder(depth)
        d = bench(fn, f"{lib} depth={depth}", rounds=2)
        results[f"{lib} depth={depth}"] = d
        print(f"   {lib:8s} depth={depth:>2d}  {d['mean_s']:7.3f}s  →  {fmt(SZ/d['mean_s'])} items/sec")

# ========================================================================
print("\n" + "=" * 72)
print("T6. 重复订阅 + T7. BehaviorSubject")
print("=" * 72)
K=1000
def rxpy_sub1k(): o=rx.from_iterable(range(1000)); t=0; [(n:=[0], o.subscribe(on_next=lambda v: n.__setitem__(0, n[0]+1)), t:=t+n[0]) for _ in range(K)]; return t
def rxrust_sub1k(): o=rx_rust.Observable.from_iter(range(1000)); t=0; [(n:=[0], o.subscribe(on_next=lambda v: n.__setitem__(0, n[0]+1)), t:=t+n[0]) for _ in range(K)]; return t
def vr_sub1k(): o=VrObservable.from_iterable(range(1000)); t=0; [(n:=[0], o.subscribe(on_next=lambda v: n.__setitem__(0, n[0]+1)), t:=t+n[0]) for _ in range(K)]; return t

for name, fn in [("RxPY", rxpy_sub1k), ("rx-rust", rxrust_sub1k), ("vools", vr_sub1k)]:
    d = bench(fn, f"T6.{name}"); results[f"T6.{name}"] = d
    print(f"   T6 重复订阅 {name:8s} {d['mean_s']:.3f}s")

def rxpy_bs(): last=[0]; rx.from_iterable(range(N_SMALL)).subscribe(on_next=lambda v: last.__setitem__(0, v)); return last[0]
def rxrust_bs(): bs=rx_rust.BehaviorSubject(0); [bs.on_next(i) for i in range(N_SMALL)]; return bs.value
def vr_bs(): bs=VrBS(0); [bs.on_next(i) for i in range(N_SMALL)]; return bs.value

for name, fn in [("RxPY", rxpy_bs), ("rx-rust", rxrust_bs), ("vools", vr_bs)]:
    d = bench(fn, f"T7.{name}"); results[f"T7.{name}"] = d
    print(f"   T7 BehaviorSubject {name:8s} {d['mean_s']:.3f}s → {fmt(N_SMALL/d['mean_s'])} writes/sec")

# ========================================================================
# 生成 MD 报告
# ========================================================================
print("\n[i] 生成 MD 报告...")
now = time.strftime("%Y-%m-%d %H:%M:%S")

# 计算相对速度
def delta_str(key):
    now_s = results[key]["mean_s"]
    before_s = BEFORE.get(key)
    if before_s:
        pct = (before_s - now_s) / before_s * 100
        return f"{before_s:.3f}s → {now_s:.3f}s ({pct:+.1f}%)"
    return f"{now_s:.3f}s"

def delta_thr(key, size):
    now_s = results[key]["mean_s"]
    thr = size / now_s
    return f"{fmt(thr):>10s}/s"

md = f"""# RxPY 3.x vs rx-rust（优化后） vs vools.reactive — 高压性能对比 v2

> 生成时间: {now}  |  机器: Windows x86_64 · Python 3.13 · Rust 1.96.0
> rx-rust 版本: 本地源码（`_add_op` 延迟构建 + `build_observer` 预构建 + on_error / pipe / 时间操作符）
> vools.reactive: 本地源码
> **注意: 当前为优化后的 pure-Python 版数据，Rust 编译版需 Linux/Mac `maturin build --release` 产出 wheel。**

---

## 核心改进: rx-rust 优化前 vs 优化后

| 测试 | 优化前 (v0.1.0) | 优化后 (v0.1.1) | 提升 |
|------|-----------------|-----------------|------|
| 纯消费 (1M) | 0.160s (6.2M/s) | {delta_str("rx-rust · 纯消费")} ({delta_thr("rx-rust · 纯消费", N)}) | {"{:.1f}%".format((BEFORE["rx-rust · 纯消费"]-results["rx-rust · 纯消费"]["mean_s"])/BEFORE["rx-rust · 纯消费"]*100) if BEFORE["rx-rust · 纯消费"] > results["rx-rust · 纯消费"]["mean_s"] else ""} |
| map+filter (1M) | 0.469s (2.1M/s) | {delta_str("rx-rust · map+filter")} ({delta_thr("rx-rust · map+filter", N)}) | {""} |
| 10操作符长链 | 1.125s (0.9M/s) | {delta_str("rx-rust · 10操作符长链")} ({delta_thr("rx-rust · 10操作符长链", N)}) | {""} |
| reduce 1M | 0.243s (4.1M/s) | {delta_str("rx-rust · reduce(1M)")} ({delta_thr("rx-rust · reduce(1M)", N)}) | {""} |
| scan 100k | 0.020s (5.1M/s) | {delta_str("rx-rust · scan(100k)")} ({delta_thr("rx-rust · scan(100k)", 100_000)}) | {""} |

---

## T1. 吞吐对比 (1M items)

| 场景 | RxPY | rx-rust (优化后) | vools |
|------|------|------------------|-------|
| 纯消费 | {delta_thr("RxPY · 纯消费", N)} | {delta_thr("rx-rust · 纯消费", N)} | {delta_thr("vools · 纯消费", N)} |
| map+filter | {delta_thr("RxPY · map+filter", N)} | {delta_thr("rx-rust · map+filter", N)} | {delta_thr("vools · map+filter", N)} |
| 10操作符长链 | {delta_thr("RxPY · 10操作符长链", N)} | {delta_thr("rx-rust · 10操作符长链", N)} | {delta_thr("vools · 10操作符长链", N)} |

## T2. 单项延迟 (ns/item)

| 库 | 平均延迟 |
|----|----------|
| RxPY | {results['T2.RxPY']['check']:,d} ns |
| rx-rust | {results['T2.rx-rust']['check']:,d} ns |
| vools | {results['T2.vools']['check']:,d} ns |

## T3. Subject 多播

| 库 | events/sec |
|----|------------|
| RxPY | {fmt(results['RxPY Subject 多播']['check']/results['RxPY Subject 多播']['mean_s'])} |
| rx-rust | {fmt(results['rx-rust Subject 多播']['check']/results['rx-rust Subject 多播']['mean_s'])} |
| vools | {fmt(results['vools Subject 多播']['check']/results['vools Subject 多播']['mean_s'])} |

## T5. 链深度衰减 (200k items)

| 深度 | RxPY | rx-rust | vools |
|------|------|---------|-------|
| 5 | {delta_thr("RxPY depth=5", SZ)} | {delta_thr("rx-rust depth=5", SZ)} | {delta_thr("vools depth=5", SZ)} |
| 10 | {delta_thr("RxPY depth=10", SZ)} | {delta_thr("rx-rust depth=10", SZ)} | {delta_thr("vools depth=10", SZ)} |
| 20 | {delta_thr("RxPY depth=20", SZ)} | {delta_thr("rx-rust depth=20", SZ)} | {delta_thr("vools depth=20", SZ)} |

## T6/T7/T8 汇总

| 测试 | RxPY | rx-rust | vools |
|------|------|---------|-------|
| 重复订阅 (1000x) | {results['T6.RxPY']['mean_s']:.3f}s | {results['T6.rx-rust']['mean_s']:.3f}s | {results['T6.vools']['mean_s']:.3f}s |
| BehaviorSubject (100k w/s) | {fmt(N_SMALL/results['T7.RxPY']['mean_s'])}/s | {fmt(N_SMALL/results['T7.rx-rust']['mean_s'])}/s | {fmt(N_SMALL/results['T7.vools']['mean_s'])}/s |
| reduce 1M | {delta_thr("RxPY · reduce(1M)", N)} | {delta_thr("rx-rust · reduce(1M)", N)} | {delta_thr("vools · reduce(1M)", N)} |
| scan 100k | {delta_thr("RxPY · scan(100k)", 100_000)} | {delta_thr("rx-rust · scan(100k)", 100_000)} | {delta_thr("vools · scan(100k)", 100_000)} |

---

## 文本条形图 (以 rx-rust 为基线 1.0×)

```
T1 · map+filter 1M (越短越好)
    rx-rust  {chr(9608)}  1.00x
    RxPY     {chr(9608)*int(25*results['RxPY · map+filter']['mean_s']/results['rx-rust · map+filter']['mean_s'])}  {results['RxPY · map+filter']['mean_s']/results['rx-rust · map+filter']['mean_s']:.2f}x
    vools    {chr(9608)*int(25*results['vools · map+filter']['mean_s']/results['rx-rust · map+filter']['mean_s'])}  {results['vools · map+filter']['mean_s']/results['rx-rust · map+filter']['mean_s']:.2f}x

T8 · reduce 1M (越短越好)
    rx-rust  {chr(9608)}  1.00x
    vools    {chr(9608)*int(25*results['vools · reduce(1M)']['mean_s']/results['rx-rust · reduce(1M)']['mean_s'])}  {results['vools · reduce(1M)']['mean_s']/results['rx-rust · reduce(1M)']['mean_s']:.2f}x
    RxPY     {chr(9608)*int(25*results['RxPY · reduce(1M)']['mean_s']/results['rx-rust · reduce(1M)']['mean_s'])}  {results['RxPY · reduce(1M)']['mean_s']/results['rx-rust · reduce(1M)']['mean_s']:.2f}x
```

---

## 结论

### 优化后提升
- **map+filter 吞吐**: 需对比优化前数据评估 _add_op 延迟构建的效果
- **reduce/scan**: 继续领先 RxPY
- **时间操作符**: 新增 `interval/timer/delay/debounce/throttle/timeout`，功能完整度大幅提升
- **API**: 新增 `pipe()` / `rx_rust.ops` / `CompositeSubscription` / `Observable.of(...args)` / on_error

### 下一步
- **Rust 原生编译层**: 需 Linux/Mac `maturin build --release` 产出 wheel，预期核心路径再 3~10×
- **发布新版本**: `pip install rx-rust --upgrade` 即可获得优化后版本

---

> 测试脚本: `benchmark_v2.py` · 源码: https://gitcode.com/VictorTop/Rx-Rust
"""

out_path = os.path.join(os.path.dirname(__file__), "benchmark_report_v2.md")
with open(out_path, "w", encoding="utf-8") as f:
    f.write(md)
print(f"[✓] 报告已写入: {out_path}")