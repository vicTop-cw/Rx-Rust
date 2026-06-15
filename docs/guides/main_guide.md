# Rx-Rust 使用指南 (GUIDE)

> 一份从 **零基础** 到 **熟练掌握** 的完整教程

---

## 目录

1. [第一部分：入门](#第一部分入门)
   - 1.1 什么是响应式编程
   - 1.2 核心概念图解
   - 1.3 三分钟第一个程序
2. [第二部分：操作符详解](#第二部分操作符详解)
   - 2.1 创建型操作符
   - 2.2 转换型操作符
   - 2.3 过滤型操作符
   - 2.4 组合型操作符
   - 2.5 数学型操作符
   - 2.6 错误处理
   - 2.7 时间型操作符
   - 2.8 调度型操作符
3. [第三部分：Subject 专题](#第三部分subject-专题)
   - 3.1 PublishSubject
   - 3.2 BehaviorSubject
   - 3.3 ReplaySubject
   - 3.4 ConnectableObservable
4. [第四部分：实战案例](#第四部分实战案例)
   - 4.1 案例：日志分析流水线
   - 4.2 案例：带防抖的搜索
   - 4.3 案例：事件总线（Event Bus）
   - 4.4 案例：网络请求自动重试
5. [第五部分：最佳实践](#第五部分最佳实践)
   - 5.1 线程安全与资源管理
   - 5.2 常见陷阱
   - 5.3 性能建议
6. [Rust API 参考](#rust-api-参考)
7. [Python (rxpy) API 参考](#python-rxpy-api-参考)
8. [调试与排错](#调试与排错)

---

## 第一部分：入门

### 1.1 什么是响应式编程

响应式编程（Reactive Programming）是一种基于**数据流**和**变化传播**的编程范式。

**传统（命令式）：**
```text
a = 1
b = 2
c = a + b  // c = 3
a = 10      // c 还是 3，没有自动更新
```

**响应式（声明式）：**
```text
a$ = Observable(1, 10, ...)   // 随时间变化的数据流
b$ = Observable(2, ...)
c$ = combine_latest(a$, b$, |a, b| a + b)
// c$ 会随着 a$ 或 b$ 的变化自动更新
```

核心思想：**一切都是流** —— 鼠标点击、HTTP 响应、定时器、用户输入、日志消息……都可以抽象为 Observable。

### 1.2 核心概念图解

```text
Observable 是什么？
┌──────────────────────────────────────────────┐
│  一段在"未来"可能发射 0 个或多个值的程序   │
│                                              │
│  时间线: ───(1)───(2)───(3)───[完成]───▶   │
│            值    值    值    结束信号        │
└──────────────────────────────────────────────┘

订阅者 (Observer) 订阅之后:

           subscribe()
Observable ──────────────► Observer
           on_next(值)
Observable ──────────────► Observer  // 收到值 -> 处理
           on_next(值)
Observable ──────────────► Observer  // 收到值 -> 处理
           on_next(Err(...))
Observable ──────────────► Observer  // 收到错误
           on_completed()
Observable ──────────────► Observer  // 完成信号，不会再有值

任何时刻你可以调用: Subscription.dispose() 来取消订阅
```

三个核心协议：

| 调用 | 语义 | 次数 |
|------|------|------|
| `on_next(Ok(T))` | 发射正常值 | 0 ~ N 次 |
| `on_next(Err(E))` | 发射错误 | 0 ~ 1 次 |
| `on_completed()` | 正常完成 | 0 ~ 1 次 |

> **关键：** Observable 是**惰性**的。你不 `subscribe()`，它什么都不会做。

### 1.3 三分钟第一个程序

```rust
use rx_rust::prelude::*;

fn main() {
    // 1. 创建: 从 1 开始的 5 个数
    let source = range::<i32, ()>(1, 5);

    // 2. 管道: 只处理偶数，然后平方
    let pipeline = source
        .filter(|n| n % 2 == 0)    // 只留下偶数
        .map(|n| n * n);            // 平方

    // 3. 订阅 (subscribe)，开始消费
    pipeline.subscribe(FnObserver::new(
        |v| println!("收到: {:?}", v),
        || println!("完成！"),
    ));
}
```

运行后输出：
```text
收到: Ok(4)
收到: Ok(16)
完成！
```

**流程分解：**

```text
源 range(1,5):   ─(1)─(2)─(3)─(4)─(5)─[完成]─▶
经过 filter:     ─────(2)─────(4)────────────▶
经过 map:        ─────(4)─────(16)───────────▶
最终 Observer:            打印4     打印16   打印"完成"
```

---

## 第二部分：操作符详解

操作符是 Rx 的灵魂。它们就像 Unix 管道的 `grep`、`awk`、`sort`：小而专注、可组合、可复用。

### 2.1 创建型操作符

**`of(value)` - 发射单个值**

```rust
of::<i32, ()>(42)
    .subscribe(FnObserver::new(
        |v| println!("{:?}", v),   // Ok(42)
        || println!("done"),
    ));
```

**`from_iter(iter)` - 从集合创建**

```rust
from_iter::<i32, ()>(vec![10, 20, 30])
    .subscribe(FnObserver::new(
        |v| println!("{:?}", v),   // Ok(10), Ok(20), Ok(30)
        || println!("done"),
    ));
```

**`range(start, count)` - 数字范围**

```rust
range::<i32, ()>(5, 3)  // 5, 6, 7
    .subscribe(FnObserver::new(
        |v| println!("{:?}", v),
        || {},
    ));
```

**`empty()` - 什么都不发射，直接完成**

```rust
// 只打印 "done"
empty::<i32, ()>()
    .subscribe(FnObserver::new(
        |_| println!("不会到这里"),
        || println!("done"),
    ));
```

**`never()` - 什么都不发射，也不完成**

```rust
// 永远什么都不打印 (一个无限长的流)
never::<i32, ()>()
    .subscribe(FnObserver::new(
        |_| println!("永远不会到这里"),
        || println!("也永远不会到这里"),
    ));
```

**`throw(error)` - 直接发射错误**

```rust
throw::<i32, String>("文件不存在".into())
    .subscribe(FnObserver::new(
        |v| println!("{:?}", v),   // Err("文件不存在")
        || println!("也会打印 done"),
    ));
```

**`repeat(value, count)` - 重复**

```rust
repeat::<String, ()>("hello".into(), 3)
    .subscribe(FnObserver::new(
        |v| println!("{:?}", v),  // Ok("hello") × 3
        || {},
    ));
```

**`defer(factory)` - 延迟创建（订阅时才创建）**

```rust
// 每次订阅都新建一个不同的 Observable
let deferred = defer::<ObservableFn<i32, ()>, _, i32, ()>(|| {
    of::<i32, ()>(100)  // 订阅时才创建
});
```

### 2.2 转换型操作符

**`map(f)` - 一对一转换**

```rust
from_iter::<i32, ()>(vec![1, 2, 3])
    .map(|x| x * 10)        // 10, 20, 30
    .subscribe(FnObserver::new(
        |v| println!("{:?}", v),
        || {},
    ));
```

**`flat_map(f)` - 转换并展平**

当你的映射函数返回的是一个 Observable，而不是一个值时用它：

```rust
from_iter::<i32, ()>(vec![1, 2])
    .flat_map(|n| from_iter::<i32, ()>(vec![n, n*10]))
    .subscribe(FnObserver::new(
        |v| println!("{:?}", v),  // Ok(1), Ok(10), Ok(2), Ok(20)
        || {},
    ));
```

图解:
```text
from_iter:            ─(1)────(2)─────▶
                       │       │
          flat_map:   │       └─▶ [2, 20] ─▶
                       └─▶ [1, 10] ─▶
结果合并后:            ─(1)─(10)─(2)─(20)─▶
```

**`scan(initial, f)` - 累积并发射中间结果**

```rust
from_iter::<i32, ()>(vec![1, 2, 3, 4])
    .scan(0, |acc, x| acc + x)  // 1, 3, 6, 10
    .subscribe(FnObserver::new(
        |v| println!("{:?}", v),
        || {},
    ));
```

**`reduce(initial, f)` - 累积只发射最终结果**

```rust
from_iter::<i32, ()>(vec![1, 2, 3, 4])
    .reduce(0, |acc, x| acc + x)  // 只发射 10
    .subscribe(FnObserver::new(
        |v| println!("{:?}", v),  // Ok(10)
        || {},
    ));
```

**`buffer(n)` - 将 n 个值打包成一个 Vec**

```rust
from_iter::<i32, ()>(vec![1, 2, 3, 4, 5])
    .buffer(2)
    .subscribe(FnObserver::new(
        |v| println!("{:?}", v),  // Ok(vec![1,2]), Ok(vec![3,4]), Ok(vec![5])
        || {},
    ));
```

### 2.3 过滤型操作符

**`filter(pred)` - 条件过滤**

```rust
from_iter::<i32, ()>(vec![1, 2, 3, 4, 5])
    .filter(|x| x > 2)          // 3, 4, 5
    .subscribe(...);
```

**`take(n)` - 只取前 n 个**

```rust
from_iter::<i32, ()>(vec![1, 2, 3, 4, 5])
    .take(3)                    // 1, 2, 3
    .subscribe(...);
```

**`skip(n)` - 跳过前 n 个**

```rust
from_iter::<i32, ()>(vec![1, 2, 3, 4, 5])
    .skip(2)                    // 3, 4, 5
    .subscribe(...);
```

**`take_last(n)` - 只取最后 n 个**

```rust
from_iter::<i32, ()>(vec![1, 2, 3, 4, 5])
    .take_last(2)               // 4, 5
    .subscribe(...);
```

**`first()` - 只取第一个**

```rust
from_iter::<i32, ()>(vec![1, 2, 3])
    .first()                    // 1
    .subscribe(...);
```

**`last()` - 只取最后一个**

```rust
from_iter::<i32, ()>(vec![1, 2, 3])
    .last()                     // 3
    .subscribe(...);
```

**`take_while(pred)` - 取到条件失败为止**

```rust
from_iter::<i32, ()>(vec![1, 2, 3, 4, 5, 1, 2])
    .take_while(|x| x < 4)      // 1, 2, 3
    .subscribe(...);
```

**`skip_while(pred)` - 跳过直到条件失败**

```rust
from_iter::<i32, ()>(vec![1, 2, 3, 4, 5, 1, 2])
    .skip_while(|x| x < 4)      // 4, 5, 1, 2
    .subscribe(...);
```

**`element_at(n)` - 只取第 n 个**

```rust
from_iter::<i32, ()>(vec![10, 20, 30, 40])
    .element_at(2)              // 30 (0-indexed)
    .subscribe(...);
```

**`distinct()` - 去重**

```rust
from_iter::<i32, ()>(vec![1, 2, 2, 3, 3, 1, 4])
    .distinct()                 // 1, 2, 3, 4
    .subscribe(...);
```

**`distinct_until_changed()` - 去掉连续重复**

```rust
from_iter::<i32, ()>(vec![1, 1, 2, 2, 3, 1, 1])
    .distinct_until_changed()   // 1, 2, 3, 1
    .subscribe(...);
```

**`default_if_empty(default)` - 空流时发射一个默认值**

```rust
empty::<i32, ()>()
    .default_if_empty(999)      // 空流时发射 999
    .subscribe(...);            // Ok(999)
```

**`contains(target)` - 是否包含某个值**

```rust
from_iter::<i32, ()>(vec![1, 2, 3])
    .contains(2)
    .subscribe(FnObserver::new(
        |v| println!("{:?}", v),  // Ok(true)
        || {},
    ));
```

**`ignore_elements()` - 忽略所有值**

```rust
from_iter::<i32, ()>(vec![1, 2, 3])
    .ignore_elements()          // 忽略 1,2,3，但保留完成信号
    .subscribe(...);            // 只打印 "done"
```

### 2.4 组合型操作符

**`merge(other)` - 并行合并（有值就发射）**

```rust
let a = from_iter::<i32, ()>(vec![1, 3, 5]);
let b = from_iter::<i32, ()>(vec![2, 4, 6]);

a.merge(b)
    .subscribe(FnObserver::new(
        |v| println!("{:?}", v),
        || {},
    ));
// 可能输出: 1, 2, 3, 4, 5, 6（取决于发射时序）
```

**`concat(other)` - 顺序连接（等第一个完成后才开始第二个）**

```rust
let a = from_iter::<i32, ()>(vec![1, 2]);
let b = from_iter::<i32, ()>(vec![10, 20]);

a.concat(b)
    .subscribe(...);            // 1, 2, 10, 20
```

**`zip(other, f)` - 按位置一一配对**

```rust
let names = from_iter::<String, ()>(vec!["Alice".into(), "Bob".into()]);
let ages  = from_iter::<i32, ()>(vec![25, 30]);

names.zip(ages, |n, a| format!("{}:{}岁", n, a))
    .subscribe(...);            // "Alice:25岁", "Bob:30岁"
```

**`combine_latest(other, f)` - 组合各自的最新值**

```rust
let a = from_iter::<i32, ()>(vec![1, 2, 3]);
let b = from_iter::<i32, ()>(vec![10, 20]);

a.combine_latest(b, |x, y| x + y)
    .subscribe(...);            // 11 (1+10), 12 (2+10), 22 (2+20), 23 (3+20)
```

**`switch_map(f)` - 切换到最新的 inner Observable（重要！）**

这是响应式编程中最强大的操作符之一。当上游有新值产生了一个新的 Observable 时，**自动取消上一个 inner Observable 的订阅**。

典型应用场景：搜索框。用户输入 "h" → "he" → "hel" → "hello"，每次都触发一个网络请求，但只需要保留最后一次请求的结果。

```rust
let search_input = from_iter::<String, ()>(vec![
    "h".into(), "he".into(), "hello".into()
]);

search_input
    .switch_map(|query| {
        // 模拟网络请求（此处简化为一个 of）
        of::<String, ()>(format!("搜索: {}", query))
    })
    .subscribe(FnObserver::new(
        |v| println!("{:?}", v),
        || {},
    ));
// 最终只会有 "搜索: hello"（以及可能中间的结果，取决于时序）
```

### 2.5 数学型操作符

**`count()` - 发射值的个数**

```rust
from_iter::<i32, ()>(vec![10, 20, 30])
    .count()                    // 3
    .subscribe(...);
```

**`sum()` - 求和**

```rust
from_iter::<i32, ()>(vec![1, 2, 3, 4])
    .sum()                      // 10
    .subscribe(...);
```

**`min()` - 最小值**

```rust
from_iter::<i32, ()>(vec![5, 1, 3])
    .min()                      // 1
    .subscribe(...);
```

**`max()` - 最大值**

```rust
from_iter::<i32, ()>(vec![5, 1, 3])
    .max()                      // 5
    .subscribe(...);
```

**`average()` - 平均值**

```rust
from_iter::<f64, ()>(vec![1.0, 2.0, 3.0, 4.0])
    .average()                  // 2.5
    .subscribe(...);
```

### 2.6 错误处理

**`catch_error(f)` - 捕获错误，切换到替代 Observable**

```rust
// 模拟网络请求失败
let source = throw::<i32, String>("网络失败".into());

source
    .catch_error(|e| {
        println!("捕获错误: {:?}", e);
        of::<i32, String>(-1)   // 返回 -1 作为 fallback
    })
    .subscribe(FnObserver::new(
        |v| println!("{:?}", v),  // Ok(-1)
        || println!("完成"),
    ));
```

**`on_error_resume_next(other)` - 错误后切换到另一个 Observable**

```rust
let source = throw::<i32, String>("失败".into());
let backup = from_iter::<i32, String>(vec![99, 100]);

source
    .on_error_resume_next(backup)
    .subscribe(...);             // 99, 100
```

**`retry(n)` - 失败重试 n 次**

```rust
use rx_rust::observable::ObservableFn;
use std::sync::{Arc, Mutex};

let attempts = Arc::new(Mutex::new(0));
let attempts_clone = attempts.clone();

let source = ObservableFn::<i32, String>::new(move |observer| {
    let mut c = attempts_clone.lock().unwrap();
    *c += 1;
    if *c <= 2 {
        observer.on_next(Ok(1));
        observer.on_next(Err("临时错误".into()));
    } else {
        observer.on_next(Ok(42));
        observer.on_completed();
    }
    Subscription::empty()
});

source.retry(3).subscribe(FnObserver::new(
    |v| println!("{:?}", v),    // Ok(1), Ok(1), Ok(42)
    || println!("完成"),
));
```

**`retry_when(f)` - 根据自定义策略重试**

```rust
source
    .retry_when(|_err| {
        // 根据错误类型决定是否重试
        // 返回的 Observable 发射值 -> 重试；完成 -> 不再重试
        of::<(), String>(())    // 简单示例：立即重试一次
    })
    .subscribe(...);
```

### 2.7 时间型操作符

**`debounce(duration)` - 防抖**

只在"安静一段时间"后发射最后一个值。典型用途：搜索框输入。

```rust
use std::time::Duration;

let subject = PublishSubject::<String, ()>::new();

subject
    .debounce(Duration::from_millis(300))
    .subscribe(FnObserver::new(
        |v| println!("{:?}", v),
        || {},
    ));

subject.on_next(Ok("h".into()));
subject.on_next(Ok("he".into()));
subject.on_next(Ok("hel".into()));
subject.on_next(Ok("hello".into()));
// 300ms 内没有新输入，最后只发射 "hello"
```

**`throttle(duration)` - 节流**

在每个时间窗口内只发射第一个值。典型用途：按钮防重复点击。

```rust
use std::time::Duration;

let subject = PublishSubject::<i32, ()>::new();

subject
    .throttle(Duration::from_secs(1))
    .subscribe(FnObserver::new(
        |v| println!("{:?}", v),
        || {},
    ));

// 0ms: on_next(Ok(1))  -> 发射 1（窗口开始）
// 300ms: on_next(Ok(2)) -> 被丢弃（窗口内）
// 800ms: on_next(Ok(3)) -> 被丢弃（窗口内）
// 1200ms: on_next(Ok(4)) -> 发射 4（新窗口）
```

**`timeout(duration)` - 超时保护**

如果在 `duration` 内没有新值，就产生一个超时错误。

```rust
use std::time::Duration;

some_slow_observable
    .timeout(Duration::from_secs(5))
    .subscribe(FnObserver::new(
        |v| { /* Ok(v) 或 Err(超时) */ },
        || {},
    ));
```

### 2.8 调度型操作符

**`observe_on(scheduler)` - 让下游在指定调度器上执行**

```rust
use rx_rust::scheduler::ThreadPoolScheduler;

from_iter::<i32, ()>(vec![1, 2, 3])
    .observe_on(ThreadPoolScheduler::new(2))  // on_next 在工作线程上执行
    .subscribe(FnObserver::new(
        |v| println!("线程 {:?}: {:?}", std::thread::current().id(), v),
        || {},
    ));
```

**`subscribe_on(scheduler)` - 让 subscribe 调用在指定调度器上执行**

```rust
use rx_rust::scheduler::AsyncScheduler;

from_iter::<i32, ()>(vec![1, 2, 3])
    .subscribe_on(AsyncScheduler::new())  // 整个订阅过程异步执行
    .subscribe(FnObserver::new(
        |v| println!("{:?}", v),
        || {},
    ));
```

**四种调度器总结：**

| 调度器 | 行为 | 适用场景 |
|--------|------|---------|
| `CurrentThreadScheduler` | 当前线程同步执行 | 简单同步流 |
| `ImmediateScheduler` | 立即执行，不做调度 | 测试、最内层 |
| `ThreadPoolScheduler::new(n)` | n 个 worker 线程池 | CPU 密集型并发任务 |
| `AsyncScheduler` | 异步调度执行 | I/O 密集型并发任务 |

---

## 第三部分：Subject 专题

### 3.1 PublishSubject：事件广播

**最简单的 Subject。新订阅者只能收到"订阅之后"发射的值。**

```rust
let subject = PublishSubject::<i32, ()>::new();

// 订阅者 A
subject.subscribe_ref(FnObserver::new(
    |v| println!("A: {:?}", v),
    || println!("A 完成"),
));

subject.on_next(Ok(1));   // A 收到
subject.on_next(Ok(2));   // A 收到

// 订阅者 B（"迟到"的订阅者）
subject.subscribe_ref(FnObserver::new(
    |v| println!("B: {:?}", v),
    || println!("B 完成"),
));

subject.on_next(Ok(3));   // A 和 B 都收到
subject.on_completed();    // A 和 B 都收到完成信号

// 输出:
// A: Ok(1)
// A: Ok(2)
// A: Ok(3)
// B: Ok(3)
// A 完成
// B 完成
```

**适用场景**：按钮点击、键盘事件、日志流。

### 3.2 BehaviorSubject：有"当前值"的 Subject

**每个新订阅者都会立刻收到当前最新值。**

```rust
let subject = BehaviorSubject::<i32, ()>::new(0);   // 初始值 0

// S1 订阅
subject.subscribe_ref(FnObserver::new(
    |v| println!("S1: {:?}", v),
    || {},
));
// S1 立刻收到 Ok(0)

subject.on_next(Ok(1));
// S1 收到 Ok(1)

// S2 订阅
subject.subscribe_ref(FnObserver::new(
    |v| println!("S2: {:?}", v),
    || {},
));
// S2 立刻收到 Ok(1)（当前最新值）

subject.on_next(Ok(2));
// S1 和 S2 都收到 Ok(2)
```

**适用场景**：应用状态、用户登录信息、配置、主题切换——任何"总有一个当前值"的东西。

### 3.3 ReplaySubject：缓存并重放

**每个新订阅者都会收到"最近 N 个值"的重放。**

```rust
let subject = ReplaySubject::<i32, ()>::new(2);   // 缓存最近 2 个值

subject.on_next(Ok(1));
subject.on_next(Ok(2));
subject.on_next(Ok(3));

// 新订阅者
subject.subscribe_ref(FnObserver::new(
    |v| println!("{:?}", v),
    || {},
));
// 立刻收到: Ok(2), Ok(3)

subject.on_next(Ok(4));
// 再收到 Ok(4)
```

**适用场景**：需要重放历史的场景，如聊天消息缓存、操作日志。

### 3.4 ConnectableObservable：多播 + 延迟启动（热 Observable）

普通 Observable 是"冷"的——每个订阅者都会触发一次独立的执行。
ConnectableObservable 是"热"的——所有订阅者**共享**同一次执行。

```rust
use rx_rust::prelude::*;
use std::sync::{Arc, Mutex};

let counter = Arc::new(Mutex::new(0));
let counter_clone = counter.clone();

let conn = ObservableFn::<i32, ()>::new(move |observer| {
    // 这个闭包只会被调用 1 次（无论有多少订阅者）
    let mut c = counter_clone.lock().unwrap();
    *c += 1;
    let call_count = *c;
    drop(c);
    observer.on_next(Ok(call_count));
    observer.on_next(Ok(100));
    observer.on_completed();
    Subscription::empty()
})
.publish();  // 转换为 ConnectableObservable

// 先让多个订阅者订阅（此时源还没开始）
conn.subscribe_ref(FnObserver::new(|v| println!("A: {:?}", v), || {}));
conn.subscribe_ref(FnObserver::new(|v| println!("B: {:?}", v), || {}));

// connect() 之后源才真正开始
conn.connect();

// 输出:
// A: Ok(1)   ← 同一个 1，证明源只执行了一次
// B: Ok(1)
// A: Ok(100)
// B: Ok(100)
```

**适用场景**：昂贵的网络请求、数据库查询、文件 I/O——不希望每个订阅者都重复执行一次的。

---

## 第四部分：实战案例

### 4.1 案例：日志分析流水线

```rust
use rx_rust::prelude::*;

#[derive(Debug)]
enum LogLevel { Info, Warn, Error }

#[derive(Debug)]
struct LogEntry { level: LogLevel, msg: String }

fn main() {
    let logs = from_iter::<LogEntry, ()>(vec![
        LogEntry { level: LogLevel::Info,  msg: "程序启动".into() },
        LogEntry { level: LogLevel::Warn,  msg: "内存使用率 85%".into() },
        LogEntry { level: LogLevel::Error, msg: "数据库连接失败".into() },
        LogEntry { level: LogLevel::Error, msg: "请求超时".into() },
        LogEntry { level: LogLevel::Info,  msg: "用户登录".into() },
    ]);

    println!("=== 错误警报流 ===");
    logs.clone()
        .filter(|e| matches!(e.level, LogLevel::Error))
        .map(|e| format!("[ERROR] {}", e.msg))
        .subscribe(FnObserver::new(
            |v| if let Ok(msg) = v { println!("{}", msg) },
            || {},
        ));

    println!("=== 错误统计 ===");
    logs.clone()
        .filter(|e| matches!(e.level, LogLevel::Error))
        .count()
        .subscribe(FnObserver::new(
            |v| println!("错误总数: {:?}", v),   // Ok(2)
            || {},
        ));
}
```

### 4.2 案例：带防抖的搜索

```rust
use rx_rust::prelude::*;
use std::time::Duration;

struct SearchEngine;
impl SearchEngine {
    fn search(query: &str) -> Vec<String> {
        // 模拟真实的搜索结果
        vec![format!("{}的结果1", query), format!("{}的结果2", query)]
    }
}

fn main() {
    let input = PublishSubject::<String, ()>::new();

    input.clone()
        .debounce(Duration::from_millis(250))      // 250ms 内没有新输入才发射
        .filter(|q| q.len() >= 2)                  // 至少 2 个字符才搜索
        .map(|q| SearchEngine::search(&q))
        .subscribe(FnObserver::new(
            |results| println!("搜索结果: {:?}", results),
            || {},
        ));

    // 模拟用户输入（通常来自 UI 事件循环）
    input.on_next(Ok("h".into()));
    input.on_next(Ok("he".into()));
    input.on_next(Ok("hel".into()));
    input.on_next(Ok("hell".into()));
    input.on_next(Ok("hello".into()));
    input.on_completed();  // 完成信号会触发 debounce 发射最后一个值
}
```

### 4.3 案例：事件总线

```rust
use rx_rust::prelude::*;
use std::sync::{Arc, Mutex};

// 事件类型
enum Event {
    UserLoggedIn { name: String },
    UserLoggedOut { name: String },
    Purchase { item: String, amount: f64 },
}

// 全局事件总线（通常通过依赖注入管理）
struct EventBus {
    subject: Arc<PublishSubject<Event, ()>>,
}
impl EventBus {
    fn new() -> Self { Self { subject: Arc::new(PublishSubject::new()) } }
    fn emit(&self, event: Event) { self.subject.on_next(Ok(event)); }
    fn subscribe(&self, on_event: impl Fn(&Event) + Send + Sync + 'static) -> Subscription {
        let subject = Arc::clone(&self.subject);
        subject.subscribe_ref(FnObserver::new(
            move |v| if let Ok(e) = v { on_event(&e) },
            || {},
        ))
    }
}

fn main() {
    let bus = Arc::new(EventBus::new());

    // 模块 A：监听登录事件
    let bus_clone = bus.clone();
    std::thread::spawn(move || {
        bus_clone.subscribe(|e| {
            if let Event::UserLoggedIn { name } = e {
                println!("日志模块: 用户 {} 登录", name);
            }
        });
    });

    // 模块 B：监听购买事件
    let bus_clone = bus.clone();
    std::thread::spawn(move || {
        bus_clone.subscribe(|e| {
            if let Event::Purchase { item, amount } = e {
                println!("财务模块: 购买 {} 金额 ¥{}", item, amount);
            }
        });
    });

    // 发射事件
    bus.emit(Event::UserLoggedIn { name: "Victor".into() });
    bus.emit(Event::Purchase { item: "Rust入门".into(), amount: 99.0 });
}
```

### 4.4 案例：网络请求自动重试

```rust
use rx_rust::prelude::*;
use rx_rust::observable::ObservableFn;
use std::sync::{Arc, Mutex};

// 模拟不稳定的网络请求（前 2 次失败，第 3 次成功）
fn fetch_user(id: u32) -> ObservableFn<String, String> {
    let attempts = Arc::new(Mutex::new(0));
    ObservableFn::<String, String>::new(move |observer| {
        let mut a = attempts.lock().unwrap();
        *a += 1;
        if *a <= 2 {
            observer.on_next(Err(format!("请求用户 {} 失败 (第{}次)", id, a)));
        } else {
            observer.on_next(Ok(format!("用户{}资料", id)));
            observer.on_completed();
        }
        Subscription::empty()
    })
}

fn main() {
    fetch_user(123)
        .retry(3)     // 最多重试 3 次（加上初始调用共 4 次机会）
        .subscribe(FnObserver::new(
            |v| match v {
                Ok(name) => println!("获取成功: {}", name),
                Err(e)   => println!("最终失败: {}", e),
            },
            || println!("请求流程结束"),
        ));
}
```

---

## 第五部分：最佳实践

### 5.1 线程安全与资源管理

**`Arc<Mutex<_>>` 的持有策略**

在闭包里如果需要共享状态，优先用 `Arc<Mutex<_>>`：

```rust
let counter = Arc::new(Mutex::new(0));
let counter_clone = counter.clone();

some_observable.subscribe(FnObserver::new(
    move |v| {
        let mut c = counter_clone.lock().unwrap();
        *c += 1;
    },
    || {},
));
```

**记住 `Mutex` 不是可重入的**：如果在持锁期间调用 observer 的方法，而 observer 又回到同一段代码，会导致**死锁**。

**永远不要**：
```rust
// ❌ 错误示范
let count = Arc::new(Mutex::new(0));
let source = ObservableFn::<i32, ()>::new(move |observer| {
    let mut c = count.lock().unwrap();  // 第一次上锁
    *c += 1;
    observer.on_next(Ok(*c));            // ← 如果 on_next 里又调用了 retry 等，
    //                                     // 可能重新执行本闭包 → 第二次 lock → 死锁！
    Subscription::empty()
});
```

**正确做法**：读完值就 `drop` 锁，或把状态读取和副作用分开：

```rust
// ✅ 正确示范
let source = ObservableFn::<i32, ()>::new(move |observer| {
    // 先读取并释放锁
    let (should_fail, current) = {
        let mut c = count.lock().unwrap();
        *c += 1;
        (*c <= 2, *c)
    };
    // 现在锁已经释放，可以安全调用 observer
    if should_fail {
        observer.on_next(Err(format!("第{}次失败", current)));
    } else {
        observer.on_next(Ok(current));
        observer.on_completed();
    }
    Subscription::empty()
});
```

### 5.2 常见陷阱

**陷阱 1：忘记 `clone()`**

`from_iter`, `of`, `throw` 等创建函数返回的 `ObservableFn` 是可 `Clone` 的（内部用 `Arc`）。很多操作符（`retry`, `concat`, `switch_map` 等）要求源 Observable 可克隆。

```rust
// ❌ 不能这样：源消费后不能再使用
let s = of::<i32, ()>(1);
s.retry(3);   // 编译错误：s 需要 Clone 边界

// ✅ 这样：源可以被克隆
let s = of::<i32, ()>(1);
s.clone().retry(3);  // OK
```

**陷阱 2：`Result<T, E>` 的错误类型不匹配**

```rust
// ❌ 错误类型不一致
let a = of::<i32, ()>(1);    // E = ()
let b = of::<i32, String>(2); // E = String
a.merge(b);  // 编译错误：类型不统一
```

**解决方案**：统一整个管道的错误类型。

**陷阱 3：同步 Observable 中的 `observe_on` 不生效**

`observe_on` 的效果需要依赖调度器真正地"排队再执行"。对于同步流，`CurrentThreadScheduler` 不会切换线程。如果需要真正的并发，用 `ThreadPoolScheduler` 或 `AsyncScheduler`。

**陷阱 4：Subject 完成后再发射的值会被丢弃**

```rust
let subject = PublishSubject::<i32, ()>::new();
subject.on_completed();
subject.on_next(Ok(42));  // ❌ 这个值会被静默丢弃！
```

规则：一旦 on_completed / on_next(Err) 发生，Subject 就"终结"了。

### 5.3 性能建议

1. **不要创建不必要的 Observable**：`of` 和 `from_iter` 是轻量的，但多层嵌套会产生多层闭包调用。
2. **优先使用 `filter` + `map`**：它们是零分配的（在栈上完成）。
3. **长时间流记得 `dispose()`**：如果 Observable 会持续发射（例如定时器），保存 `Subscription`，在不再需要时调用 `dispose()`。
4. **Subject 做事件总线时用 `Arc` 共享**：避免跨线程时每个组件都持有独立的 Subject。
5. **调试时先在同步环境下测试**：在 `CurrentThreadScheduler` 上跑通逻辑后，再引入并发调度。

---

## Rust API 参考

### 模块导入

```rust
// 方案 1：用 prelude（推荐，最简洁）
use rx_rust::prelude::*;

// 方案 2：按需导入
use rx_rust::observable::{Observable, ObservableFn};
use rx_rust::observer::{Observer, FnObserver};
use rx_rust::operators::{ObservableExt, ObservableExtFilter, ObservableExtWithTime, ObservableExtError, ObservableExtMath};
use rx_rust::subject::{PublishSubject, BehaviorSubject, ReplaySubject};
use rx_rust::scheduler::{Scheduler, CurrentThreadScheduler, ThreadPoolScheduler, AsyncScheduler, ImmediateScheduler};
use rx_rust::subscription::{Subscription, Disposable};
```

### 创建函数（`observable::base`）

```rust
pub fn range<T, E>(start: T, count: usize) -> ObservableFn<T, E>
pub fn repeat<T, E>(value: T, count: usize) -> ObservableFn<T, E>
pub fn defer<Obs, F, T, E>(factory: F) -> ObservableFn<T, E>
pub fn generate<Init, F, T, E>(initial: Init, f: F) -> ObservableFn<T, E>
pub fn of<T, E>(value: T) -> ObservableFn<T, E>
pub fn from_iter<T, E>(iter: Vec<T>) -> ObservableFn<T, E>
pub fn empty<T, E>() -> ObservableFn<T, E>
pub fn never<T, E>() -> ObservableFn<T, E>
pub fn throw<T, E>(error: E) -> ObservableFn<T, E>
```

### Observable trait

```rust
pub trait Observable<T, E> {
    fn subscribe(self, observer: impl Observer<T, E> + Send + Sync + 'static) -> Subscription;
}
```

### Observer trait

```rust
pub trait Observer<T, E> {
    fn on_next(&self, value: Result<T, E>);
    fn on_completed(&self);
}
```

### Subject

```rust
impl PublishSubject<T, E> {
    pub fn new() -> Self;
    pub fn subscribe_ref(&self, observer: impl Observer<T, E> + Send + Sync + 'static) -> Subscription;
    pub fn on_next(&self, value: Result<T, E>);
    pub fn on_completed(&self);
}

impl BehaviorSubject<T, E> {
    pub fn new(initial: T) -> Self;
    // 其余同上
}

impl ReplaySubject<T, E> {
    pub fn new(buffer_size: usize) -> Self;
    // 其余同上
}
```

### 操作符一览

全部都可以通过 `use rx_rust::operators::ObservableExt;`（或 prelude）通过方法调用。

| 扩展 trait | 操作符 |
|-----------|--------|
| `ObservableExt` | `map`, `flat_map`, `scan`, `filter`, `take`, `skip`, `take_while`, `skip_while`, `last`, `first`, `element_at`, `distinct`, `distinct_until_changed`, `default_if_empty`, `ignore_elements`, `merge`, `concat`, `zip`, `combine_latest`, `reduce`, `count`, `sum`, `min`, `max`, `average`, `switch_map`, `buffer`, `contains`, `all` |
| `ObservableExtWithTime` | `debounce`, `throttle`, `timeout` |
| `ObservableExtFilter` | (`filter`, `take` 等已在上面列出，部分归组) |
| `ObservableExtError` | `catch_error`, `on_error_resume_next`, `retry`, `retry_when` |
| `ObservableExtMath` | (`count`, `sum`, `min`, `max`, `average` 已在上面列出) |

---

## Python (rxpy) API 参考

rxpy 是 rx_rust 的 Python 绑定，提供相同的语义。

### 安装

```bash
pip install rxpy
```

### 基本用法

```python
import rxpy

# 1. 创建 Observable
source = rxpy.Observable.from_iter([1, 2, 3, 4, 5])

# 2. 链式操作
result = (
    source
    .filter(lambda x: x % 2 == 0)
    .map(lambda x: x * 10)
)

# 3. 订阅
result.subscribe(
    on_next=lambda v: print(f"值: {v}"),
    on_error=lambda e: print(f"错误: {e}"),
    on_completed=lambda: print("完成"),
)
```

### 创建方法

```python
rxpy.Observable.of(value)
rxpy.Observable.from_iter(iterable)
rxpy.Observable.range(start, count)
rxpy.Observable.repeat(value, count)
rxpy.Observable.empty()
rxpy.Observable.never()
rxpy.Observable.throw(error_msg)
```

### 操作符方法

```python
.map(mapper)              # 转换
.filter(predicate)        # 过滤
.take(n)                  # 取前 n 个
.skip(n)                  # 跳过前 n 个
.first()                  # 取第一个
.last()                   # 取最后一个
.take_while(predicate)    # 取到条件失败
.skip_while(predicate)    # 跳至条件失败
.scan(initial, reducer)   # 累积中间结果
.reduce(initial, reducer) # 累积最终结果
.count()                  # 统计
.collect()                # 收集为列表
.flat_map(mapper)         # 展平映射
.distinct_until_changed() # 去连续重复
.debounce(seconds)        # 防抖
.throttle(seconds)        # 节流
.timeout(seconds)         # 超时
.catch_error(handler)     # 错误恢复
.subscribe_on(scheduler)  # 上游调度
.observe_on(scheduler)    # 下游调度
```

### Subject

```python
# PublishSubject：事件广播
sub = rxpy.PublishSubject()
sub.on_next(value)
sub.on_completed()
sub.subscribe(on_next=print, on_completed=lambda: print("done"))

# BehaviorSubject：保持当前值
sub = rxpy.BehaviorSubject(initial_value)
# 新订阅者会立刻收到 initial_value（或最新值）

# ReplaySubject：重放历史
sub = rxpy.ReplaySubject(buffer_size)
# 新订阅者会立刻收到最近 buffer_size 个值
```

### 调度器

```python
rxpy.CurrentThreadScheduler()   # 当前线程同步
rxpy.ImmediateScheduler()       # 立即执行
rxpy.ThreadPoolScheduler(4)     # 4 个线程的线程池
rxpy.AsyncScheduler()           # 异步调度
```

### 返回的 Subscription

```python
subscription = observable.subscribe(...)

# 之后可以取消
subscription.dispose()

# 检查是否已取消
if subscription.is_disposed():
    print("已取消")
```

### 完整示例：带防抖的搜索

```python
import rxpy
from rxpy import Observable, PublishSubject
import time

# 创建一个 Subject 模拟用户输入
input_stream = PublishSubject()

# 建立搜索管道
input_stream \
    .debounce(0.3) \
    .filter(lambda q: len(q) >= 2) \
    .subscribe(
        on_next=lambda q: print(f"🔍 搜索: {q}"),
        on_completed=lambda: print("搜索结束"),
    )

# 模拟输入
for text in ["h", "he", "hel", "hell", "hello"]:
    input_stream.on_next(text)
    time.sleep(0.1)  # 模拟连续输入，每次间隔 100ms

# 让 debounce 有时间触发最后一次
time.sleep(0.5)
input_stream.on_completed()
# 输出: 🔍 搜索: hello
```

---

## 调试与排错

### 测试策略

rx_rust 自带 62 个单元测试，覆盖了所有核心功能。你可以在本地运行：

```bash
cd rx-rust
cargo test                     # 运行全部测试
cargo test test_retry          # 只跑 retry 相关
cargo test test_publish        # 只跑 publish/connect 相关
cargo test test_debounce       # 只跑 debounce 相关
```

### 调试技巧

**技巧 1：在管道里插入"日志打印"**

```rust
source
    .map(|v| { println!("经过 map: {}", v); v })   // 临时插入
    .filter(...)
    .subscribe(...)
```

**技巧 2：用 `first()` / `take(n)` 截断长流**

```rust
// 只看前 3 个值，验证管道是否正确
some_big_pipeline
    .take(3)
    .subscribe(FnObserver::new(|v| println!("{:?}", v), || {}));
```

**技巧 3：用 `collect()` 获取完整结果用于断言**

```rust
// collect() 会在完成后把所有值发射为一个 Vec（在 Python 绑定中可用）
// 在 Rust 中也可以通过闭包收集
let result = Arc::new(Mutex::new(Vec::new()));
let result_clone = result.clone();

source.subscribe(FnObserver::new(
    move |v| if let Ok(x) = v { result_clone.lock().unwrap().push(x) },
    || {},
));

assert_eq!(result.lock().unwrap().as_slice(), &[1, 2, 3]);
```

### 常见错误信息

| 编译错误 | 含义 | 解决方法 |
|---------|------|---------|
| `the trait Clone is not implemented` | 该 Observable 不能被克隆 | 用 `of` / `from_iter` / `repeat` 等可克隆的创建函数 |
| `type annotations needed` | 编译器无法推断 T 或 E | 显式标注 `of::<i32, ()>(42)` |
| `cannot move value into closure` | 在循环中消费了变量 | 用 `Arc` 或 `clone()` |

### 运行时死锁排查

如果程序挂住不动，通常是 `Mutex` 的可重入问题：

```text
典型调用栈：
  1. source 闭包持有 lock
  2. 闭包内调用 observer.on_next(Err(...))
  3. observer 是 retry 的 InnerObserver
  4. retry 会克隆并重新订阅 source → 回到 step 1
  5. 再次 lock 同一个 Mutex → 死锁
```

**诊断方法**：

```bash
# 1. 打印线程 ID
println!("当前线程: {:?}", std::thread::current().id());

# 2. 打印每一次锁获取
println!("尝试获取锁...");
let _guard = some_mutex.lock().unwrap();
println!("成功获取锁");

# 3. 检查是否在同一线程出现两次"尝试获取锁"
#    而第二次没有对应"成功获取锁"——那就是死锁点
```

**修复模板**：

```rust
// 在调用 observer 之前释放所有锁
let action = {
    let state = self.state.lock().unwrap();
    state.compute_action()   // 只计算，不调用 observer
};  // ← 锁在此处释放

// 现在可以安全调用
if action.should_emit {
    observer.on_next(Ok(action.value));
}
```

---

## 下一步

恭喜！你已经完整地了解了 rx_rust / rxpy 的能力。推荐学习路径：

1. **从简单的例子开始**——用 `of`, `from_iter`, `map`, `filter` 写第一个流
2. **尝试 Subject**——体会"热"流和"冷"流的区别
3. **掌握组合**——用 `merge`, `concat`, `zip` 写更复杂的管道
4. **时间和错误**——`debounce`, `throttle`, `timeout`, `retry` 是生产环境最有用的
5. **阅读源码**——`rx-rust/src/operators/mod.rs` 中每个操作符的实现非常清晰，是学习 Rust 抽象能力的好素材

---

*祝编码愉快！🚀*

*— Rx-Rust Team*
