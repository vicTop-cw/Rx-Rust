import sys
sys.path.insert(0, r'rx-rust-py/python')
import rx_rust

# Test 1: Direct _PyObservable call via subscribe path
from rx_rust import _PyObservable, Subscription
import time

# Create a simple source observable
src = _PyObservable.from_iter([1, 2, 3])

# Test subscribe on source
print('Test 1: source subscribe')
src.subscribe(lambda v: print(f'  got: {v}'))
print('  OK')

# Test 2: Now test dispatch_to_workers
print('Test 2: dispatch_to_workers on _PyObservable')
result = src.dispatch_to_workers(lambda x: x * 10, num_workers=2).collect()
print(f'  Result: {sorted(result)}')
print('  OK')

# Test 3: Test the Observable wrapper
print('Test 3: Observable wrapper')
from rx_rust import Observable
obs = Observable.from_iter([1, 2, 3])
result = obs.dispatch_to_workers(lambda x: x * 10, num_workers=2).collect()
print(f'  Result: {sorted(result)}')
print('  OK')

print('ALL TESTS PASSED')
