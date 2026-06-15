import sys, time
sys.path.insert(0, r'rx-rust-py/python')
import rx_rust

# Timeout mechanism: use thread
import threading

def run_with_timeout(fn, timeout_seconds, name):
    result = [None]
    err = [None]
    done = threading.Event()
    def target():
        try:
            result[0] = fn()
        except Exception as e:
            err[0] = e
        finally:
            done.set()
    t = threading.Thread(target=target, daemon=True)
    t.start()
    finished = done.wait(timeout_seconds)
    if not finished:
        print(f'  [{name}] HANGED after {timeout_seconds}s!')
        return None
    if err[0]:
        print(f'  [{name}] Error: {err[0]}')
        return None
    print(f'  [{name}] OK: {result[0]}')
    return result[0]

print('=== Test 1: _PyObservable.from_iter basic subscribe ===')
src = rx_rust._PyObservable.from_iter([1, 2, 3])
def test1():
    items = []
    src.subscribe(lambda v: items.append(v))
    return sorted(items)
run_with_timeout(test1, 3, 'basic-subscribe')

print()
print('=== Test 2: _PyObservable.dispatch_to_workers ===')
def test2():
    items = []
    src.dispatch_to_workers(lambda x: x * 10, num_workers=2).subscribe(
        on_next=lambda v: items.append(v)
    )
    return sorted(items)
run_with_timeout(test2, 5, 'dispatch-workers')

print()
print('=== Test 3: Observable wrapper ===')
from rx_rust import Observable
def test3():
    return Observable.from_iter([1, 2, 3]).dispatch_to_workers(
        lambda x: x * 10, num_workers=2
    ).collect()
run_with_timeout(test3, 5, 'observable-wrapper')

print()
print('=== End ===')
