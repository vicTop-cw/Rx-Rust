import sys, time
sys.path.insert(0, r'rx-rust-py/python')
import rx_rust
from rx_rust import Observable

# 带 timeout 的简单测试
start = time.time()
try:
    result = Observable.from_iter([1, 2, 3]).dispatch_to_workers(lambda x: x * 10, num_workers=2).collect()
    print('Result:', sorted(result))
except Exception as e:
    print('Error:', e)
elapsed = time.time() - start
print(f'Elapsed: {elapsed:.2f}s')
