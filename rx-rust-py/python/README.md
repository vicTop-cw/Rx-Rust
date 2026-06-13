# rx-rust — Reactive Extensions for Python

一个高性能的响应式编程库，提供 Observable / Observer / Subject / Scheduler 支持。

## 快速开始

```bash
pip install rx-rust
```

```python
import rx_rust

result = []
rx_rust.Observable.from_iter([1, 2, 3, 4, 5]) \
    .filter(lambda x: x % 2 == 0) \
    .map(lambda x: x * 10) \
    .subscribe(on_next=lambda v: result.append(v))

print(result)  # [20, 40]
```

## 核心对象

- **Observable** — 可观察序列（`from_iter`, `of`, `range`, `repeat`, `empty`, `never`）
- **操作符** — `map`, `filter`, `take`, `skip`, `reduce`, `scan`, `flat_map`, `merge`, `concat`, `start_with`, `default_if_empty`, `contains`, `all`, `sum`, `count`, `do_on_next`
- **Subject** — `PublishSubject`, `BehaviorSubject`, `ReplaySubject`
- **Subscription** — 订阅管理，支持 `dispose()` 和 `is_disposed()`
- **Scheduler** — `CurrentThreadScheduler`, `ThreadPoolScheduler`, `AsyncScheduler`, `ImmediateScheduler`

## License

MIT
