"""纯 Python 测试：验证 rx_rust / rxpy 库的所有主要功能。

用法:
    pytest test_rx_rust.py
    或
    python test_rx_rust.py
"""

import os
import sys

# 确保能从当前目录导入包
_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

# ===== 导入部分 =====
try:
    from rx_rust import Observable, Subscription, PublishSubject, BehaviorSubject, ReplaySubject
    _PKG = "rx_rust"
except ImportError:
    from rxpy import Observable, Subscription, PublishSubject, BehaviorSubject, ReplaySubject
    _PKG = "rxpy"


def collect(obs):
    """小工具：订阅 observable 并收集所有发射值到 list"""
    result = []
    obs.subscribe(on_next=lambda v: result.append(v))
    return result


# ========== 1. of ==========
def test_of():
    assert collect(Observable.of(42)) == [42]
    assert collect(Observable.of("hello")) == ["hello"]


# ========== 2. from_iter ==========
def test_from_iter():
    assert collect(Observable.from_iter([1, 2, 3])) == [1, 2, 3]
    assert collect(Observable.from_iter([])) == []


# ========== 3. range ==========
def test_range():
    assert collect(Observable.range(0, 5)) == [0, 1, 2, 3, 4]
    assert collect(Observable.range(10, 3)) == [10, 11, 12]


# ========== 4. repeat ==========
def test_repeat():
    assert collect(Observable.repeat("x", 3)) == ["x", "x", "x"]
    assert collect(Observable.repeat(7, 0)) == []


# ========== 5. empty ==========
def test_empty():
    assert collect(Observable.empty()) == []


# ========== 6. map ==========
def test_map():
    assert collect(Observable.from_iter([1, 2, 3]).map(lambda x: x * 2)) == [2, 4, 6]
    assert collect(Observable.from_iter(["a", "b"]).map(lambda x: x.upper())) == ["A", "B"]


# ========== 7. filter ==========
def test_filter():
    assert collect(Observable.from_iter([1, 2, 3, 4, 5]).filter(lambda x: x > 2)) == [3, 4, 5]
    assert collect(Observable.from_iter([1, 2, 3]).filter(lambda x: False)) == []


# ========== 8. take ==========
def test_take():
    assert collect(Observable.from_iter([1, 2, 3, 4, 5]).take(2)) == [1, 2]
    assert collect(Observable.from_iter([1, 2, 3]).take(0)) == []


# ========== 9. skip ==========
def test_skip():
    assert collect(Observable.from_iter([1, 2, 3, 4, 5]).skip(2)) == [3, 4, 5]
    assert collect(Observable.from_iter([1, 2]).skip(5)) == []


# ========== 10. first ==========
def test_first():
    assert collect(Observable.from_iter([1, 2, 3]).first()) == [1]


# ========== 11. last ==========
def test_last():
    assert collect(Observable.from_iter([1, 2, 3]).last()) == [3]
    assert collect(Observable.empty().last()) == []


# ========== 12. count ==========
def test_count():
    assert collect(Observable.from_iter([10, 20, 30]).count()) == [3]
    assert collect(Observable.empty().count()) == [0]


# ========== 13. sum ==========
def test_sum():
    assert collect(Observable.from_iter([1, 2, 3]).sum()) == [6]
    assert collect(Observable.from_iter([1.5, 2.5]).sum()) == [4.0]


# ========== 14. collect ==========
def test_collect():
    assert Observable.from_iter([1, 2, 3]).collect() == [1, 2, 3]
    assert Observable.empty().collect() == []


# ========== 15. reduce ==========
def test_reduce():
    assert collect(Observable.from_iter([1, 2, 3, 4]).reduce(0, lambda acc, x: acc + x)) == [10]
    assert collect(Observable.from_iter(["a", "b", "c"]).reduce("", lambda acc, x: acc + x)) == ["abc"]


# ========== 16. scan ==========
def test_scan():
    assert collect(Observable.from_iter([1, 2, 3]).scan(0, lambda acc, x: acc + x)) == [1, 3, 6]


# ========== 17. flat_map ==========
def test_flat_map():
    assert collect(Observable.from_iter([1, 2]).flat_map(lambda x: [x, x * 10])) == [1, 10, 2, 20]


# ========== 18. start_with ==========
def test_start_with():
    assert collect(Observable.from_iter([2, 3]).start_with(1)) == [1, 2, 3]
    assert collect(Observable.empty().start_with(99)) == [99]


# ========== 19. default_if_empty - has value ==========
def test_default_if_empty_has_value():
    assert collect(Observable.from_iter([1]).default_if_empty(99)) == [1]
    assert collect(Observable.from_iter([1, 2, 3]).default_if_empty(99)) == [1, 2, 3]


# ========== 20. default_if_empty - empty ==========
def test_default_if_empty_empty():
    assert collect(Observable.empty().default_if_empty(99)) == [99]


# ========== 21. contains - True ==========
def test_contains_true():
    assert collect(Observable.from_iter([1, 2, 3]).contains(2)) == [True]


# ========== 22. contains - False ==========
def test_contains_false():
    assert collect(Observable.from_iter([1, 2, 3]).contains(42)) == [False]
    assert collect(Observable.empty().contains(1)) == [False]


# ========== 23. all - True ==========
def test_all_true():
    assert collect(Observable.from_iter([2, 4, 6]).all(lambda x: x % 2 == 0)) == [True]


# ========== 24. all - False ==========
def test_all_false():
    assert collect(Observable.from_iter([2, 3, 4]).all(lambda x: x % 2 == 0)) == [False]


# ========== 25. do_on_next ==========
def test_do_on_next():
    counter = [0]
    values = []

    def action(v):
        counter[0] += 1

    result = collect(Observable.from_iter([10, 20, 30]).do_on_next(action))
    assert counter[0] == 3
    assert result == [10, 20, 30]


# ========== 26. subscription dispose ==========
def test_subscription_dispose():
    subject = PublishSubject()
    received = []
    sub = subject.subscribe(on_next=lambda v: received.append(v))
    subject.on_next(1)
    subject.on_next(2)
    sub.dispose()
    subject.on_next(3)
    assert received == [1, 2]


# ========== 27. subscription is_disposed ==========
def test_subscription_is_disposed():
    subject = PublishSubject()
    sub = subject.subscribe(on_next=lambda v: None)
    assert sub.is_disposed() is False
    sub.dispose()
    assert sub.is_disposed() is True


# ========== 28. PublishSubject - multiple subscribers ==========
def test_publish_subject_multiple_subscribers():
    subject = PublishSubject()
    a_values = []
    b_values = []
    subject.subscribe(on_next=lambda v: a_values.append(v))
    subject.subscribe(on_next=lambda v: b_values.append(v))
    subject.on_next(1)
    subject.on_next(2)
    subject.on_next(3)
    assert a_values == [1, 2, 3]
    assert b_values == [1, 2, 3]


# ========== 29. BehaviorSubject - current value ==========
def test_behavior_subject_current_value():
    subject = BehaviorSubject(0)
    received_a = []
    received_b = []
    subject.subscribe(on_next=lambda v: received_a.append(v))
    subject.on_next(1)
    subject.on_next(2)
    subject.subscribe(on_next=lambda v: received_b.append(v))
    subject.on_next(3)
    assert received_a == [0, 1, 2, 3]
    assert received_b == [2, 3]


# ========== 30. ReplaySubject - buffer ==========
def test_replay_subject_buffer():
    subject = ReplaySubject(2)
    subject.on_next(1)
    subject.on_next(2)
    subject.on_next(3)
    received = []
    subject.subscribe(on_next=lambda v: received.append(v))
    subject.on_next(4)
    assert received == [2, 3, 4]


# ========== 31. concat ==========
def test_concat():
    a = Observable.from_iter([1, 2, 3])
    b = Observable.from_iter([4, 5, 6])
    assert collect(a.concat(b)) == [1, 2, 3, 4, 5, 6]


# ========== 直接运行 ==========
if __name__ == "__main__":
    print(f"Using package: {_PKG}")
    all_tests = [v for k, v in sorted(globals().items())
                 if k.startswith("test_") and callable(v)]
    passed = 0
    for test in all_tests:
        try:
            test()
            print(f"  [PASS]  {test.__name__}")
            passed += 1
        except AssertionError as e:
            print(f"  [FAIL]  {test.__name__}: {e}")
        except Exception as e:
            print(f"  [ERROR] {test.__name__}: {type(e).__name__}: {e}")
    print(f"\n{passed}/{len(all_tests)} tests passed")
