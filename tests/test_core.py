import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'rx-rust-py', 'python'))

import rx_rust
from rx_rust import Observable, PublishSubject, BehaviorSubject, ReplaySubject, Subscription

# Test Observable.of
result = []
obs = Observable.of(1, 2, 3)
def on_next(v):
    result.append(v)
obs.subscribe(on_next)
print("of() result:", result)

# Test Observable.range
result = []
obs = Observable.range(0, 5)
obs.subscribe(on_next)
print("range() result:", result)

# Test map
result = []
obs = Observable.range(0, 3).map(lambda x: x * 2)
obs.subscribe(on_next)
print("map() result:", result)

# Test filter
result = []
obs = Observable.range(0, 10).filter(lambda x: x % 2 == 0)
obs.subscribe(on_next)
print("filter() result:", result)

# Test reduce
result = []
obs = Observable.range(1, 5).reduce(0, lambda acc, x: acc + x)
obs.subscribe(on_next)
print("reduce() result:", result)

# Test PublishSubject
result = []
subject = PublishSubject()
subject.subscribe(on_next)
subject.on_next(10)
subject.on_next(20)
subject.on_next(30)
print("PublishSubject result:", result)

# Test BehaviorSubject
result = []
bs = BehaviorSubject(100)
bs.subscribe(on_next)
print("BehaviorSubject initial:", result)
bs.on_next(200)
print("BehaviorSubject after on_next:", result)

# Test ReplaySubject
result = []
rs = ReplaySubject(2)
rs.on_next(1)
rs.on_next(2)
rs.on_next(3)
rs.subscribe(on_next)
print("ReplaySubject (buffer 2) result:", result)

# Test map + filter chain
result = []
obs = Observable.range(1, 11).map(lambda x: x * x).filter(lambda x: x > 10)
obs.subscribe(on_next)
print("map+filter chain:", result)

# Test flat_map
result = []
obs = Observable.range(1, 4).flat_map(lambda x: Observable.range(1, x + 1))
obs.subscribe(on_next)
print("flat_map result:", result)

# Test scan
result = []
obs = Observable.range(1, 5).scan(0, lambda acc, x: acc + x)
obs.subscribe(on_next)
print("scan result:", result)

# Test take
result = []
obs = Observable.range(0, 100).take(5)
obs.subscribe(on_next)
print("take result:", result)

# Test skip
result = []
obs = Observable.range(0, 10).skip(3)
obs.subscribe(on_next)
print("skip result:", result)

# Test contains
result = []
obs = Observable.range(0, 10).contains(5)
obs.subscribe(on_next)
print("contains(5) result:", result)

result = []
obs = Observable.range(0, 10).contains(100)
obs.subscribe(on_next)
print("contains(100) result:", result)

# Test sum
result = []
obs = Observable.range(1, 6).sum()
obs.subscribe(on_next)
print("sum result:", result)

# Test pipe composition
result = []
obs = Observable.range(1, 11).pipe(
    lambda o: o.map(lambda x: x * 2),
    lambda o: o.filter(lambda x: x > 10),
    lambda o: o.take(3),
)
obs.subscribe(on_next)
print("pipe composition:", result)

print()
print("=== All core tests passed! ===")