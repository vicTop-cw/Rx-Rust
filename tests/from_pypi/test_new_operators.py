"""新算子测试集 — 覆盖所有扩展功能。"""
import sys, os, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'rx-rust-py', 'python'))
import rx_rust
from rx_rust import Observable, Subscription, ops


def run():
    n_ok = 0
    n_total = 0

    def check(name, actual, expected, rtol=1e-9):
        nonlocal n_ok, n_total
        n_total += 1
        if expected is None:
            ok = actual is None
        elif isinstance(expected, float):
            ok = abs(actual - expected) <= max(abs(expected) * rtol, 1e-12)
        elif isinstance(expected, list):
            if len(expected) > 0 and isinstance(expected[0], float):
                ok = len(actual) == len(expected) and all(
                    abs(a - e) <= max(abs(e) * rtol, 1e-12) for a, e in zip(actual, expected))
            else:
                ok = actual == expected
        else:
            ok = actual == expected
        if ok:
            n_ok += 1
            print(f"  ✓ {name}")
        else:
            print(f"  ✗ {name}: expected {expected!r}, got {actual!r}")

    print("=" * 60)
    print("1. 统计聚合 (min, max, mean, median, variance, std, quantile, arg_min, arg_max, n_unique, any)")
    print("=" * 60)
    check("min", Observable.from_iter([5, 2, 8, 1, 9, 3]).min().collect(), [1])
    check("max", Observable.from_iter([5, 2, 8, 1, 9, 3]).max().collect(), [9])
    check("mean", Observable.from_iter([1, 2, 3, 4, 5]).mean().collect(), [3.0])
    check("average", Observable.from_iter([1, 2, 3, 4, 5]).average().collect(), [3.0])
    check("empty min", Observable.empty().min().collect(), [])
    check("single mean", Observable.of(42).mean().collect(), [42])
    check("median-odd", Observable.from_iter([5, 3, 1, 4, 2]).median().collect(), [3])
    check("median-even", Observable.from_iter([1, 2, 3, 4]).median().collect(), [2.5])
    check("variance", Observable.from_iter([1, 2, 3, 4, 5]).variance().collect(), [2.0])
    check("std", Observable.from_iter([1, 2, 3, 4, 5]).std().collect(), [2.0 ** 0.5])
    check("quantile-0", Observable.from_iter([1, 2, 3, 4, 5]).quantile(0).collect(), [1])
    check("quantile-1", Observable.from_iter([1, 2, 3, 4, 5]).quantile(1).collect(), [5])
    check("quantile-0.5", Observable.from_iter([1, 2, 3, 4, 5]).quantile(0.5).collect(), [3])
    check("arg_min", Observable.from_iter([3, 1, 4, 1, 5]).arg_min().collect(), [1])
    check("arg_max", Observable.from_iter([3, 1, 4, 5, 2]).arg_max().collect(), [3])
    check("n_unique", Observable.from_iter([1, 2, 1, 3, 2, 4]).n_unique().collect(), [4])
    check("any-true", Observable.from_iter([1, 2, 3]).any(lambda x: x > 2).collect(), [True])
    check("any-false", Observable.from_iter([1, 2, 3]).any(lambda x: x > 10).collect(), [False])
    check("any-empty", Observable.empty().any(lambda x: True).collect(), [False])

    print()
    print("=" * 60)
    print("2. 滚动窗口 (rolling_min, rolling_max, rolling_sum, rolling_mean)")
    print("=" * 60)
    check("rolling_sum(3)", Observable.from_iter([1, 2, 3, 4, 5]).rolling_sum(3).collect(), [1, 3, 6, 9, 12])
    check("rolling_min(2)", Observable.from_iter([5, 2, 8, 1, 9]).rolling_min(2).collect(), [5, 2, 2, 1, 1])
    check("rolling_max(2)", Observable.from_iter([1, 2, 3, 4, 5]).rolling_max(2).collect(), [1, 2, 3, 4, 5])
    check("rolling_mean(2)", Observable.from_iter([10, 20, 30]).rolling_mean(2).collect(), [10, 15, 25])
    check("rolling_sum empty", Observable.empty().rolling_sum(3).collect(), [])

    print()
    print("=" * 60)
    print("3. 累积变换 (cum_sum, cum_min, cum_max, cum_mean, cum_prod)")
    print("=" * 60)
    check("cum_sum", Observable.from_iter([1, 2, 3, 4]).cum_sum().collect(), [1, 3, 6, 10])
    check("cum_min", Observable.from_iter([3, 1, 4, 1, 5]).cum_min().collect(), [3, 1, 1, 1, 1])
    check("cum_max", Observable.from_iter([1, 2, 3, 2, 1]).cum_max().collect(), [1, 2, 3, 3, 3])
    check("cum_mean", Observable.from_iter([10, 20, 30]).cum_mean().collect(), [10, 15, 20])
    check("cum_prod", Observable.from_iter([1, 2, 3, 4]).cum_prod().collect(), [1, 2, 6, 24])
    check("cum_sum empty", Observable.empty().cum_sum().collect(), [])

    print()
    print("=" * 60)
    print("4. 排序 Top-N (sort, top_k, bottom_k)")
    print("=" * 60)
    check("sort", Observable.from_iter([5, 2, 8, 1, 9, 3]).sort().collect(), [1, 2, 3, 5, 8, 9])
    check("sort reverse", Observable.from_iter([5, 2, 8, 1, 9, 3]).sort(reverse=True).collect(), [9, 8, 5, 3, 2, 1])
    check("top_k(2)", Observable.from_iter([5, 2, 8, 1, 9, 3]).top_k(2).collect(), [9, 8])
    check("bottom_k(2)", Observable.from_iter([5, 2, 8, 1, 9, 3]).bottom_k(2).collect(), [1, 2])
    check("sort empty", Observable.empty().sort().collect(), [])

    print()
    print("=" * 60)
    print("5. 过滤/选择算子 (distinct, element_at, take_while, skip_while, take_last, skip_last)")
    print("=" * 60)
    check("distinct", Observable.from_iter([1, 2, 1, 2, 3, 3, 1]).distinct().collect(), [1, 2, 3])
    check("element_at(2)", Observable.from_iter([10, 20, 30, 40, 50]).element_at(2).collect(), [30])
    check("element_at out-of-range", Observable.from_iter([10, 20, 30]).element_at(999).collect(), [])
    check("take_while", Observable.from_iter([1, 2, 3, 4, 5]).take_while(lambda x: x < 3).collect(), [1, 2])
    check("skip_while", Observable.from_iter([1, 2, 3, 4, 5]).skip_while(lambda x: x < 3).collect(), [3, 4, 5])
    check("take_last(2)", Observable.from_iter([1, 2, 3, 4, 5]).take_last(2).collect(), [4, 5])
    check("skip_last(2)", Observable.from_iter([1, 2, 3, 4, 5]).skip_last(2).collect(), [1, 2, 3])

    print()
    print("=" * 60)
    print("6. 组合算子 (switch_map, combine_latest)")
    print("=" * 60)
    # switch_map 同步源下行为类似 flat_map
    check("switch_map sync", Observable.from_iter([1, 10]).switch_map(lambda x: Observable.range(x, 2)).collect(),
          [1, 2, 10, 11])
    # combine_latest 同步源: 第一个源先发射完才轮到第二个
    check("combine_latest sync", Observable.from_iter([1, 2, 3]).combine_latest(
        Observable.from_iter(['a', 'b']), lambda a, b: (a, b)).collect(),
          [(3, 'a'), (3, 'b')])

    print()
    print("=" * 60)
    print("7. 错误处理 (catch_error, retry)")
    print("=" * 60)
    # catch_error: x=1 → 10//(-1)=-10, x=2 → error, fallback → -1
    check("catch_error", Observable.from_iter([1, 2, 3]).map(lambda x: 10 // (x - 2)).catch_error(
        lambda e: Observable.of(-1)).collect(),
          [-10, -1])
    # retry: 前2次失败，第3次成功
    attempts = [0]
    def src_sub(observer):
        observer(1)
        observer(2)
        attempts[0] += 1
        if attempts[0] < 3:
            raise RuntimeError("fail")
        return Subscription()
    retry_obs = Observable(rx_rust._PyObservable(src_sub))
    retry_result = []
    retry_obs.retry(3).subscribe(on_next=lambda v: retry_result.append(v))
    check("retry(3)", retry_result, [1, 2, 1, 2, 1, 2])

    print()
    print("=" * 60)
    print("8. 轻量过滤 (distinct_until_changed, ignore_elements)")
    print("=" * 60)
    check("distinct_until_changed", Observable.from_iter([1, 1, 1, 2, 2, 3, 3, 3]).distinct_until_changed().collect(),
          [1, 2, 3])
    check("distinct_until_changed 2", Observable.from_iter([1, 2, 1, 2, 3, 3, 1]).distinct_until_changed().collect(),
          [1, 2, 1, 2, 3, 1])
    check("ignore_elements", Observable.from_iter([1, 2, 3]).ignore_elements().collect(), [])

    print()
    print("=" * 60)
    print("9. None 处理 & 数学工具 (drop_none, fill_none, abs, clamp)")
    print("=" * 60)
    check("drop_none", Observable.of(1, None, 2, -3, None, 4).drop_none().collect(), [1, 2, -3, 4])
    check("fill_none", Observable.of(1, None, 2).fill_none(0).collect(), [1, 0, 2])
    check("abs", Observable.from_iter([1, -2, 3, -4]).abs().collect(), [1, 2, 3, 4])
    check("clamp", Observable.from_iter([1, 2, 3, 4, 5]).clamp(2, 4).collect(), [2, 2, 3, 4, 4])

    print()
    print("=" * 60)
    print("10. 嵌套展开 (explode, flatten)")
    print("=" * 60)
    check("explode lists", Observable.of([1, 2], [3], [4, 5, 6]).explode().collect(), [1, 2, 3, 4, 5, 6])
    check("explode str", Observable.of("hello", "world").explode().collect(), ["hello", "world"])
    check("flatten", Observable.of([1, 2], [3]).flatten().collect(), [1, 2, 3])

    print()
    print("=" * 60)
    print("11. share 多播")
    print("=" * 60)
    subscribe_count = [0]
    def count_sub(observer):
        subscribe_count[0] += 1
        observer(1)
        observer(2)
        return Subscription()
    src = Observable(rx_rust._PyObservable(count_sub))
    shared = src.share()
    # 验证: 同一源只订阅一次（虽然同步源下第二个订阅者拿不到值）
    shared.subscribe(lambda v: None)
    shared.subscribe(lambda v: None)
    check("share single source sub", subscribe_count[0], 1)

    print()
    print("=" * 60)
    print("12. pipe 风格组合调用（无参算子需加括号）")
    print("=" * 60)
    check("pipe min", Observable.from_iter([1, 2, 3, 4, 5]).pipe(ops.min()).collect(), [1])
    check("pipe max", Observable.from_iter([1, 2, 3, 4, 5]).pipe(ops.max()).collect(), [5])
    check("pipe cum_sum+filter", Observable.from_iter([1, 2, 3, 4, 5]).pipe(ops.cum_sum(), ops.filter(lambda x: x > 3)).collect(),
          [6, 10, 15])
    check("pipe abs+sum", Observable.from_iter([-1, -2, 3, -4]).pipe(ops.abs_op(), ops.sum()).collect(), [10])

    print()
    print("=" * 60)
    print("13. 链式组合（综合管道）")
    print("=" * 60)
    check("综合: abs+cum_sum+mean", Observable.from_iter([1, -2, 3, -4, 5]).pipe(
        ops.abs_op(), ops.cum_sum(), ops.mean()).collect(),
          [7.0])

    print()
    print("=" * 60)
    print(f"结果: {n_ok}/{n_total} 通过")
    print("=" * 60)
    return n_ok, n_total


if __name__ == "__main__":
    ok, total = run()
    sys.exit(0 if ok == total else 1)
