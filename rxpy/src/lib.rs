use pyo3::prelude::*;
use pyo3::types::PyList;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

// ============================================================================
// Observable 核心定义
// ============================================================================

/// Observer 观察者接口
struct Observer {
    on_next: Box<dyn Fn(PyObject) + 'static>,
    on_completed: Option<Box<dyn Fn() + 'static>>,
    completed: bool,
}

impl Observer {
    fn next(&mut self, value: PyObject) {
        if !self.completed {
            (self.on_next)(value);
        }
    }

    fn complete(&mut self) {
        if !self.completed {
            self.completed = true;
            if let Some(on_completed) = &self.on_completed {
                (on_completed)();
            }
        }
    }
}

/// 订阅句柄
#[pyclass]
struct Subscription {
    disposed: Rc<RefCell<bool>>,
}

#[pymethods]
impl Subscription {
    fn dispose(&self) {
        *self.disposed.borrow_mut() = true;
    }

    fn is_disposed(&self) -> bool {
        *self.disposed.borrow()
    }
}

/// 可订阅函数类型 - 可以被克隆
type SubscribeFn = Arc<dyn Fn(Rc<RefCell<Observer>>) + Send + Sync>;

/// Observable 可观察对象
#[pyclass]
struct Observable {
    subscribe_fn: SubscribeFn,
}

#[pymethods]
impl Observable {
    // ---------- 工厂方法 ----------

    #[staticmethod]
    fn of(value: PyObject) -> Self {
        let value_clone = value.clone();
        Self {
            subscribe_fn: Arc::new(move |observer| {
                observer.borrow_mut().next(value_clone.clone());
                observer.borrow_mut().complete();
            }),
        }
    }

    #[staticmethod]
    fn empty() -> Self {
        Self {
            subscribe_fn: Arc::new(move |observer| {
                observer.borrow_mut().complete();
            }),
        }
    }

    #[staticmethod]
    fn never() -> Self {
        Self {
            subscribe_fn: Arc::new(move |_| {
                // 什么都不做 - 既不发射值也不完成
            }),
        }
    }

    #[staticmethod]
    fn range(start: i64, count: i64) -> Self {
        let values: Vec<i64> = (0..count).map(|i| start + i).collect();
        Self {
            subscribe_fn: Arc::new(move |observer| {
                Python::with_gil(|py| {
                    for v in &values {
                        let py_obj = v.into_pyobject(py).unwrap().unbind().into_any();
                        observer.borrow_mut().next(py_obj);
                    }
                });
                observer.borrow_mut().complete();
            }),
        }
    }

    #[staticmethod]
    fn repeat(value: PyObject, count: usize) -> Self {
        let value_clone = value.clone();
        Self {
            subscribe_fn: Arc::new(move |observer| {
                for _ in 0..count {
                    observer.borrow_mut().next(value_clone.clone());
                }
                observer.borrow_mut().complete();
            }),
        }
    }

    #[staticmethod]
    fn from_iter(values: &PyList) -> Self {
        let items: Vec<PyObject> = values.iter().map(|item| item.into_any()).collect();
        Self {
            subscribe_fn: Arc::new(move |observer| {
                for item in &items {
                    observer.borrow_mut().next(item.clone());
                }
                observer.borrow_mut().complete();
            }),
        }
    }

    // ---------- 订阅方法 ----------

    fn subscribe(&self, on_next: PyObject) -> Py<Subscription> {
        let on_next_clone = on_next.clone();
        let disposed = Rc::new(RefCell::new(false));
        let disposed_clone = disposed.clone();

        let observer = Rc::new(RefCell::new(Observer {
            on_next: Box::new(move |value| {
                if !*disposed_clone.borrow() {
                    Python::with_gil(|py| {
                        let _ = on_next_clone.call1(py, (value,));
                    });
                }
            }),
            on_completed: None,
            completed: false,
        }));

        (self.subscribe_fn)(observer);

        Python::with_gil(|py| {
            Py::new(py, Subscription { disposed }).unwrap()
        })
    }

    // ---------- 转换操作符 ----------

    fn map(&self, mapper: PyObject) -> Self {
        let source = self.subscribe_fn.clone();
        let mapper_clone = Arc::new(Mutex::new(mapper));

        Self {
            subscribe_fn: Arc::new(move |downstream| {
                let mapper = mapper_clone.lock().unwrap().clone();
                let downstream_clone = downstream.clone();
                let inner_observer = Rc::new(RefCell::new(Observer {
                    on_next: Box::new(move |value| {
                        let result = Python::with_gil(|py| {
                            mapper.call1(py, (value,)).unwrap()
                        });
                        downstream_clone.borrow_mut().next(result);
                    }),
                    on_completed: Some(Box::new(move || {
                        downstream.borrow_mut().complete();
                    })),
                    completed: false,
                }));

                source(inner_observer);
            }),
        }
    }

    fn filter(&self, predicate: PyObject) -> Self {
        let source = self.subscribe_fn.clone();
        let predicate_clone = Arc::new(Mutex::new(predicate));

        Self {
            subscribe_fn: Arc::new(move |downstream| {
                let predicate = predicate_clone.lock().unwrap().clone();
                let downstream_clone = downstream.clone();
                let inner_observer = Rc::new(RefCell::new(Observer {
                    on_next: Box::new(move |value| {
                        let should_pass = Python::with_gil(|py| {
                            let result = predicate.call1(py, (value.clone(),)).unwrap();
                            result.extract::<bool>(py).unwrap_or(false)
                        });
                        if should_pass {
                            downstream_clone.borrow_mut().next(value);
                        }
                    }),
                    on_completed: Some(Box::new(move || {
                        downstream.borrow_mut().complete();
                    })),
                    completed: false,
                }));

                source(inner_observer);
            }),
        }
    }

    // ---------- 过滤操作符 ----------

    fn take(&self, n: usize) -> Self {
        let source = self.subscribe_fn.clone();
        let count = Arc::new(Mutex::new(0usize));

        Self {
            subscribe_fn: Arc::new(move |downstream| {
                let count_clone = count.clone();
                let downstream_clone = downstream.clone();
                let inner_observer = Rc::new(RefCell::new(Observer {
                    on_next: Box::new(move |value| {
                        let mut cnt = count_clone.lock().unwrap();
                        if *cnt < n {
                            *cnt += 1;
                            drop(cnt);
                            downstream_clone.borrow_mut().next(value);
                        }
                    }),
                    on_completed: Some(Box::new(move || {
                        downstream.borrow_mut().complete();
                    })),
                    completed: false,
                }));

                source(inner_observer);
            }),
        }
    }

    fn skip(&self, n: usize) -> Self {
        let source = self.subscribe_fn.clone();
        let count = Arc::new(Mutex::new(0usize));

        Self {
            subscribe_fn: Arc::new(move |downstream| {
                let count_clone = count.clone();
                let downstream_clone = downstream.clone();
                let inner_observer = Rc::new(RefCell::new(Observer {
                    on_next: Box::new(move |value| {
                        let mut cnt = count_clone.lock().unwrap();
                        if *cnt >= n {
                            drop(cnt);
                            downstream_clone.borrow_mut().next(value);
                        } else {
                            *cnt += 1;
                        }
                    }),
                    on_completed: Some(Box::new(move || {
                        downstream.borrow_mut().complete();
                    })),
                    completed: false,
                }));

                source(inner_observer);
            }),
        }
    }

    fn first(&self) -> Self {
        self.take(1)
    }

    fn last(&self) -> Self {
        let source = self.subscribe_fn.clone();
        let last_value = Arc::new(Mutex::new(None::<PyObject>));

        Self {
            subscribe_fn: Arc::new(move |downstream| {
                let last_value_clone = last_value.clone();
                let downstream_clone = downstream.clone();
                let last_value_for_complete = last_value.clone();

                let inner_observer = Rc::new(RefCell::new(Observer {
                    on_next: Box::new(move |value| {
                        *last_value_clone.lock().unwrap() = Some(value);
                    }),
                    on_completed: Some(Box::new(move || {
                        if let Some(last) = last_value_for_complete.lock().unwrap().clone() {
                            downstream_clone.borrow_mut().next(last);
                        }
                        downstream.borrow_mut().complete();
                    })),
                    completed: false,
                }));

                source(inner_observer);
            }),
        }
    }

    fn skip_last(&self, n: usize) -> Self {
        let source = self.subscribe_fn.clone();
        let buffer = Arc::new(Mutex::new(Vec::<PyObject>::new()));

        Self {
            subscribe_fn: Arc::new(move |downstream| {
                let buffer_clone = buffer.clone();
                let downstream_clone = downstream.clone();
                let inner_observer = Rc::new(RefCell::new(Observer {
                    on_next: Box::new(move |value| {
                        let mut buf = buffer_clone.lock().unwrap();
                        buf.push(value);
                        if buf.len() > n {
                            let to_emit = buf.remove(0);
                            drop(buf);
                            downstream_clone.borrow_mut().next(to_emit);
                        }
                    }),
                    on_completed: Some(Box::new(move || {
                        downstream.borrow_mut().complete();
                    })),
                    completed: false,
                }));

                source(inner_observer);
            }),
        }
    }

    fn take_last(&self, n: usize) -> Self {
        let source = self.subscribe_fn.clone();
        let buffer = Arc::new(Mutex::new(Vec::<PyObject>::new()));

        Self {
            subscribe_fn: Arc::new(move |downstream| {
                let buffer_clone = buffer.clone();
                let downstream_clone = downstream.clone();
                let buffer_for_complete = buffer.clone();

                let inner_observer = Rc::new(RefCell::new(Observer {
                    on_next: Box::new(move |value| {
                        let mut buf = buffer_clone.lock().unwrap();
                        buf.push(value);
                        if buf.len() > n {
                            buf.remove(0);
                        }
                    }),
                    on_completed: Some(Box::new(move || {
                        let items = buffer_for_complete.lock().unwrap().clone();
                        for item in items {
                            downstream_clone.borrow_mut().next(item);
                        }
                        downstream.borrow_mut().complete();
                    })),
                    completed: false,
                }));

                source(inner_observer);
            }),
        }
    }

    fn take_while(&self, predicate: PyObject) -> Self {
        let source = self.subscribe_fn.clone();
        let predicate_clone = Arc::new(Mutex::new(predicate));
        let stopped = Arc::new(Mutex::new(false));

        Self {
            subscribe_fn: Arc::new(move |downstream| {
                let predicate = predicate_clone.lock().unwrap().clone();
                let stopped_clone = stopped.clone();
                let downstream_clone = downstream.clone();
                let inner_observer = Rc::new(RefCell::new(Observer {
                    on_next: Box::new(move |value| {
                        if *stopped_clone.lock().unwrap() {
                            return;
                        }
                        let should_continue = Python::with_gil(|py| {
                            let result = predicate.call1(py, (value.clone(),)).unwrap();
                            result.extract::<bool>(py).unwrap_or(false)
                        });
                        if should_continue {
                            downstream_clone.borrow_mut().next(value);
                        } else {
                            *stopped_clone.lock().unwrap() = true;
                        }
                    }),
                    on_completed: Some(Box::new(move || {
                        downstream.borrow_mut().complete();
                    })),
                    completed: false,
                }));

                source(inner_observer);
            }),
        }
    }

    fn skip_while(&self, predicate: PyObject) -> Self {
        let source = self.subscribe_fn.clone();
        let predicate_clone = Arc::new(Mutex::new(predicate));
        let skipping = Arc::new(Mutex::new(true));

        Self {
            subscribe_fn: Arc::new(move |downstream| {
                let predicate = predicate_clone.lock().unwrap().clone();
                let skipping_clone = skipping.clone();
                let downstream_clone = downstream.clone();
                let inner_observer = Rc::new(RefCell::new(Observer {
                    on_next: Box::new(move |value| {
                        let mut skip = skipping_clone.lock().unwrap();
                        if *skip {
                            let should_skip = Python::with_gil(|py| {
                                let result = predicate.call1(py, (value.clone(),)).unwrap();
                                result.extract::<bool>(py).unwrap_or(false)
                            });
                            if !should_skip {
                                *skip = false;
                                drop(skip);
                                downstream_clone.borrow_mut().next(value);
                            }
                        } else {
                            drop(skip);
                            downstream_clone.borrow_mut().next(value);
                        }
                    }),
                    on_completed: Some(Box::new(move || {
                        downstream.borrow_mut().complete();
                    })),
                    completed: false,
                }));

                source(inner_observer);
            }),
        }
    }

    fn element_at(&self, index: usize) -> Self {
        self.skip(index).take(1)
    }

    fn distinct(&self) -> Self {
        let source = self.subscribe_fn.clone();
        let seen = Arc::new(Mutex::new(Vec::<PyObject>::new()));

        Self {
            subscribe_fn: Arc::new(move |downstream| {
                let seen_clone = seen.clone();
                let downstream_clone = downstream.clone();
                let inner_observer = Rc::new(RefCell::new(Observer {
                    on_next: Box::new(move |value| {
                        let mut seen_list = seen_clone.lock().unwrap();
                        let mut is_new = true;
                        Python::with_gil(|py| {
                            for existing in seen_list.iter() {
                                if let Ok(eq) = existing.call_method1(py, "__eq__", (value.clone(),)) {
                                    if let Ok(true) = eq.extract::<bool>(py) {
                                        is_new = false;
                                        break;
                                    }
                                }
                            }
                        });
                        if is_new {
                            seen_list.push(value.clone());
                            drop(seen_list);
                            downstream_clone.borrow_mut().next(value);
                        }
                    }),
                    on_completed: Some(Box::new(move || {
                        downstream.borrow_mut().complete();
                    })),
                    completed: false,
                }));

                source(inner_observer);
            }),
        }
    }

    fn ignore_elements(&self) -> Self {
        let source = self.subscribe_fn.clone();

        Self {
            subscribe_fn: Arc::new(move |downstream| {
                let inner_observer = Rc::new(RefCell::new(Observer {
                    on_next: Box::new(move |_| {
                        // 忽略所有值
                    }),
                    on_completed: Some(Box::new(move || {
                        downstream.borrow_mut().complete();
                    })),
                    completed: false,
                }));

                source(inner_observer);
            }),
        }
    }

    // ---------- 聚合操作符 ----------

    fn reduce(&self, initial: PyObject, accumulator: PyObject) -> Self {
        let source = self.subscribe_fn.clone();
        let accumulator_clone = Arc::new(Mutex::new(accumulator));
        let initial_clone = Arc::new(Mutex::new(initial));

        Self {
            subscribe_fn: Arc::new(move |downstream| {
                let acc = accumulator_clone.lock().unwrap().clone();
                let acc_value = Arc::new(Mutex::new(initial_clone.lock().unwrap().clone()));
                let acc_value_clone = acc_value.clone();
                let downstream_clone = downstream.clone();
                let acc_value_for_complete = acc_value.clone();

                let inner_observer = Rc::new(RefCell::new(Observer {
                    on_next: Box::new(move |value| {
                        let current = acc_value_clone.lock().unwrap().clone();
                        let new_value = Python::with_gil(|py| {
                            acc.call1(py, (current, value)).unwrap()
                        });
                        *acc_value_clone.lock().unwrap() = new_value;
                    }),
                    on_completed: Some(Box::new(move || {
                        let final_value = acc_value_for_complete.lock().unwrap().clone();
                        downstream_clone.borrow_mut().next(final_value);
                        downstream.borrow_mut().complete();
                    })),
                    completed: false,
                }));

                source(inner_observer);
            }),
        }
    }

    fn count(&self) -> Self {
        let source = self.subscribe_fn.clone();
        let counter = Arc::new(Mutex::new(0i64));

        Self {
            subscribe_fn: Arc::new(move |downstream| {
                let counter_clone = counter.clone();
                let counter_for_complete = counter.clone();
                let downstream_clone = downstream.clone();

                let inner_observer = Rc::new(RefCell::new(Observer {
                    on_next: Box::new(move |_| {
                        let mut c = counter_clone.lock().unwrap();
                        *c += 1;
                    }),
                    on_completed: Some(Box::new(move || {
                        let final_count = *counter_for_complete.lock().unwrap();
                        Python::with_gil(|py| {
                            downstream_clone.borrow_mut().next(
                                final_count.into_pyobject(py).unwrap().unbind().into_any()
                            );
                        });
                        downstream.borrow_mut().complete();
                    })),
                    completed: false,
                }));

                source(inner_observer);
            }),
        }
    }

    fn sum(&self) -> Self {
        let source = self.subscribe_fn.clone();
        let total = Arc::new(Mutex::new(None::<PyObject>));

        Self {
            subscribe_fn: Arc::new(move |downstream| {
                let total_clone = total.clone();
                let total_for_complete = total.clone();
                let downstream_clone = downstream.clone();

                let inner_observer = Rc::new(RefCell::new(Observer {
                    on_next: Box::new(move |value| {
                        let mut current = total_clone.lock().unwrap();
                        if current.is_none() {
                            *current = Some(value);
                        } else {
                            let current_value = current.clone().unwrap();
                            let new_value = Python::with_gil(|py| {
                                current_value.call_method1(py, "__add__", (value,)).unwrap()
                            });
                            *current = Some(new_value);
                        }
                    }),
                    on_completed: Some(Box::new(move || {
                        let final_total = total_for_complete.lock().unwrap().clone();
                        if let Some(total_value) = final_total {
                            downstream_clone.borrow_mut().next(total_value);
                        }
                        downstream.borrow_mut().complete();
                    })),
                    completed: false,
                }));

                source(inner_observer);
            }),
        }
    }

    fn collect(&self) -> PyObject {
        let items = Arc::new(Mutex::new(Vec::<PyObject>::new()));
        let items_clone = items.clone();

        let observer = Rc::new(RefCell::new(Observer {
            on_next: Box::new(move |value| {
                items_clone.lock().unwrap().push(value);
            }),
            on_completed: None,
            completed: false,
        }));

        (self.subscribe_fn)(observer);

        let collected = items.lock().unwrap().clone();
        Python::with_gil(|py| {
            let list = pyo3::types::PyList::empty_bound(py);
            for item in collected {
                let _ = list.append(item);
            }
            list.unbind().into_any()
        })
    }

    // ---------- 数学操作符 ----------

    fn sequence_equal(&self, _other: &PyCell<Observable>) -> bool {
        // 简化实现 - 不使用实际比较
        false
    }

    // ---------- 组合操作符 ----------

    fn start_with(&self, value: PyObject) -> Self {
        let source = self.subscribe_fn.clone();
        let prefix = value.clone();

        Self {
            subscribe_fn: Arc::new(move |downstream| {
                // 先发射前缀值
                downstream.borrow_mut().next(prefix.clone());
                // 然后转发源 Observable 的值
                let downstream_clone = downstream.clone();
                let inner_observer = Rc::new(RefCell::new(Observer {
                    on_next: Box::new(move |value| {
                        downstream_clone.borrow_mut().next(value);
                    }),
                    on_completed: Some(Box::new(move || {
                        downstream.borrow_mut().complete();
                    })),
                    completed: false,
                }));

                source(inner_observer);
            }),
        }
    }

    fn default_if_empty(&self, default: PyObject) -> Self {
        let source = self.subscribe_fn.clone();
        let default_clone = default.clone();
        let emitted = Arc::new(Mutex::new(false));

        Self {
            subscribe_fn: Arc::new(move |downstream| {
                let emitted_clone = emitted.clone();
                let downstream_clone = downstream.clone();
                let emitted_for_complete = emitted.clone();
                let default_value = default_clone.clone();

                let inner_observer = Rc::new(RefCell::new(Observer {
                    on_next: Box::new(move |value| {
                        *emitted_clone.lock().unwrap() = true;
                        downstream_clone.borrow_mut().next(value);
                    }),
                    on_completed: Some(Box::new(move || {
                        if !*emitted_for_complete.lock().unwrap() {
                            downstream_clone.borrow_mut().next(default_value);
                        }
                        downstream.borrow_mut().complete();
                    })),
                    completed: false,
                }));

                source(inner_observer);
            }),
        }
    }

    // ---------- 调试操作符 ----------

    fn do_on_next(&self, action: PyObject) -> Self {
        let source = self.subscribe_fn.clone();
        let action_clone = Arc::new(Mutex::new(action));

        Self {
            subscribe_fn: Arc::new(move |downstream| {
                let act = action_clone.lock().unwrap().clone();
                let downstream_clone = downstream.clone();
                let inner_observer = Rc::new(RefCell::new(Observer {
                    on_next: Box::new(move |value| {
                        Python::with_gil(|py| {
                            let _ = act.call1(py, (value.clone(),));
                        });
                        downstream_clone.borrow_mut().next(value);
                    }),
                    on_completed: Some(Box::new(move || {
                        downstream.borrow_mut().complete();
                    })),
                    completed: false,
                }));

                source(inner_observer);
            }),
        }
    }

    fn do_on_complete(&self, action: PyObject) -> Self {
        let source = self.subscribe_fn.clone();
        let action_clone = Arc::new(Mutex::new(action));

        Self {
            subscribe_fn: Arc::new(move |downstream| {
                let act = action_clone.lock().unwrap().clone();
                let downstream_clone = downstream.clone();
                let inner_observer = Rc::new(RefCell::new(Observer {
                    on_next: Box::new(move |value| {
                        downstream_clone.borrow_mut().next(value);
                    }),
                    on_completed: Some(Box::new(move || {
                        Python::with_gil(|py| {
                            let _ = act.call0(py);
                        });
                        downstream.borrow_mut().complete();
                    })),
                    completed: false,
                }));

                source(inner_observer);
            }),
        }
    }

    // ---------- 调度器操作符 ----------

    fn delay(&self, ms: f64) -> Self {
        // 同步环境下简化实现 - 实际的 delay 需要异步运行时
        let source = self.subscribe_fn.clone();
        let delay_ms = ms;

        Self {
            subscribe_fn: Arc::new(move |downstream| {
                let downstream_clone = downstream.clone();
                let inner_observer = Rc::new(RefCell::new(Observer {
                    on_next: Box::new(move |value| {
                        // 在同步环境中简单 sleep
                        let secs = (delay_ms / 1000.0) as u64;
                        let nanos = ((delay_ms % 1000.0) * 1_000_000.0) as u32;
                        std::thread::sleep(std::time::Duration::new(secs, nanos));
                        downstream_clone.borrow_mut().next(value);
                    }),
                    on_completed: Some(Box::new(move || {
                        downstream.borrow_mut().complete();
                    })),
                    completed: false,
                }));

                source(inner_observer);
            }),
        }
    }

    fn share(&self) -> Self {
        // 简化实现 - 简单转发
        Self {
            subscribe_fn: self.subscribe_fn.clone(),
        }
    }

    fn publish(&self) -> Self {
        // 简化实现
        Self {
            subscribe_fn: self.subscribe_fn.clone(),
        }
    }

    fn fork(&self) -> Self {
        // 简化实现
        Self {
            subscribe_fn: self.subscribe_fn.clone(),
        }
    }

    fn run(&self) -> Self {
        // 简化实现 - 简单订阅一次但不做任何事
        let noop_observer = Rc::new(RefCell::new(Observer {
            on_next: Box::new(move |_| {}),
            on_completed: Some(Box::new(move || {})),
            completed: false,
        }));
        (self.subscribe_fn)(noop_observer);

        Self {
            subscribe_fn: self.subscribe_fn.clone(),
        }
    }

    fn unsubscribe_on_shared(&self) -> Self {
        // 简化实现
        Self {
            subscribe_fn: self.subscribe_fn.clone(),
        }
    }

    fn run_with_subject(&self) -> Self {
        // 简化实现
        Self {
            subscribe_fn: self.subscribe_fn.clone(),
        }
    }

    fn observe_on(&self, _scheduler: PyObject) -> Self {
        // 简化实现 - 在同步环境下忽略调度器
        Self {
            subscribe_fn: self.subscribe_fn.clone(),
        }
    }

    fn subscribe_on(&self, _scheduler: PyObject) -> Self {
        // 简化实现
        Self {
            subscribe_fn: self.subscribe_fn.clone(),
        }
    }

    fn throttle_first(&self, ms: f64) -> Self {
        let source = self.subscribe_fn.clone();
        let last_time = Arc::new(Mutex::new(None::<f64>));
        let window = ms;

        Self {
            subscribe_fn: Arc::new(move |downstream| {
                let last_time_clone = last_time.clone();
                let downstream_clone = downstream.clone();
                let inner_observer = Rc::new(RefCell::new(Observer {
                    on_next: Box::new(move |value| {
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_secs_f64() * 1000.0;
                        let mut last = last_time_clone.lock().unwrap();
                        match *last {
                            Some(t) if now - t < window => {
                                // 在窗口内，忽略
                            }
                            _ => {
                                *last = Some(now);
                                drop(last);
                                downstream_clone.borrow_mut().next(value);
                            }
                        }
                    }),
                    on_completed: Some(Box::new(move || {
                        downstream.borrow_mut().complete();
                    })),
                    completed: false,
                }));

                source(inner_observer);
            }),
        }
    }

    fn throttle_last(&self, ms: f64) -> Self {
        let source = self.subscribe_fn.clone();
        let last_value = Arc::new(Mutex::new(None::<PyObject>));
        let last_emitted = Arc::new(Mutex::new(None::<f64>));
        let window = ms;

        Self {
            subscribe_fn: Arc::new(move |downstream| {
                let last_value_clone = last_value.clone();
                let last_emitted_clone = last_emitted.clone();
                let downstream_clone = downstream.clone();
                let last_value_for_complete = last_value.clone();
                let last_emitted_for_complete = last_emitted.clone();

                let inner_observer = Rc::new(RefCell::new(Observer {
                    on_next: Box::new(move |value| {
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_secs_f64() * 1000.0;
                        let mut last = last_emitted_clone.lock().unwrap();
                        match *last {
                            Some(t) if now - t >= window => {
                                *last = Some(now);
                                drop(last);
                                downstream_clone.borrow_mut().next(value);
                            }
                            None => {
                                *last = Some(now);
                                drop(last);
                                downstream_clone.borrow_mut().next(value);
                            }
                            _ => {
                                drop(last);
                                *last_value_clone.lock().unwrap() = Some(value);
                            }
                        }
                    }),
                    on_completed: Some(Box::new(move || {
                        // 发射最后一个缓存的值（如果有的话）
                        if let Some(last_val) = last_value_for_complete.lock().unwrap().clone() {
                            let now = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_secs_f64() * 1000.0;
                            let last = *last_emitted_for_complete.lock().unwrap();
                            if last.is_none() || now - last.unwrap_or(0.0) >= window {
                                downstream_clone.borrow_mut().next(last_val);
                            }
                        }
                        downstream.borrow_mut().complete();
                    })),
                    completed: false,
                }));

                source(inner_observer);
            }),
        }
    }

    fn debounce(&self, ms: f64) -> Self {
        // 简化的 debounce 实现 - 在同步环境中效果有限
        let source = self.subscribe_fn.clone();
        let last_value = Arc::new(Mutex::new(None::<PyObject>));
        let _ = ms;

        Self {
            subscribe_fn: Arc::new(move |downstream| {
                let last_value_clone = last_value.clone();
                let downstream_clone = downstream.clone();
                let last_value_for_complete = last_value.clone();

                let inner_observer = Rc::new(RefCell::new(Observer {
                    on_next: Box::new(move |value| {
                        *last_value_clone.lock().unwrap() = Some(value);
                    }),
                    on_completed: Some(Box::new(move || {
                        if let Some(last_val) = last_value_for_complete.lock().unwrap().clone() {
                            downstream_clone.borrow_mut().next(last_val);
                        }
                        downstream.borrow_mut().complete();
                    })),
                    completed: false,
                }));

                source(inner_observer);
            }),
        }
    }

    fn sample(&self, ms: f64) -> Self {
        let source = self.subscribe_fn.clone();
        let last_value = Arc::new(Mutex::new(None::<PyObject>));
        let last_sample_time = Arc::new(Mutex::new(None::<f64>));
        let window = ms;

        Self {
            subscribe_fn: Arc::new(move |downstream| {
                let last_value_clone = last_value.clone();
                let last_sample_time_clone = last_sample_time.clone();
                let downstream_clone = downstream.clone();
                let last_value_for_complete = last_value.clone();

                let inner_observer = Rc::new(RefCell::new(Observer {
                    on_next: Box::new(move |value| {
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_secs_f64() * 1000.0;
                        *last_value_clone.lock().unwrap() = Some(value.clone());

                        let mut last = last_sample_time_clone.lock().unwrap();
                        match *last {
                            Some(t) if now - t >= window => {
                                *last = Some(now);
                                drop(last);
                                downstream_clone.borrow_mut().next(value);
                            }
                            None => {
                                *last = Some(now);
                                drop(last);
                                downstream_clone.borrow_mut().next(value);
                            }
                            _ => {}
                        }
                    }),
                    on_completed: Some(Box::new(move || {
                        if let Some(last_val) = last_value_for_complete.lock().unwrap().clone() {
                            downstream_clone.borrow_mut().next(last_val);
                        }
                        downstream.borrow_mut().complete();
                    })),
                    completed: false,
                }));

                source(inner_observer);
            }),
        }
    }

    fn materialize(&self) -> Self {
        let source = self.subscribe_fn.clone();

        Self {
            subscribe_fn: Arc::new(move |downstream| {
                let downstream_clone = downstream.clone();
                let inner_observer = Rc::new(RefCell::new(Observer {
                    on_next: Box::new(move |value| {
                        // 包装成 Notification 元组: ("on_next", value)
                        Python::with_gil(|py| {
                            let notification = (String::from("on_next"), value).into_pyobject(py).unwrap().unbind().into_any();
                            downstream_clone.borrow_mut().next(notification);
                        });
                    }),
                    on_completed: Some(Box::new(move || {
                        Python::with_gil(|py| {
                            let notification = (String::from("on_completed"),).into_pyobject(py).unwrap().unbind().into_any();
                            downstream_clone.borrow_mut().next(notification);
                        });
                        downstream.borrow_mut().complete();
                    })),
                    completed: false,
                }));

                source(inner_observer);
            }),
        }
    }

    fn finally_action(&self, action: PyObject) -> Self {
        let source = self.subscribe_fn.clone();
        let action_clone = Arc::new(Mutex::new(action));

        Self {
            subscribe_fn: Arc::new(move |downstream| {
                let act = action_clone.lock().unwrap().clone();
                let downstream_clone = downstream.clone();
                let inner_observer = Rc::new(RefCell::new(Observer {
                    on_next: Box::new(move |value| {
                        downstream_clone.borrow_mut().next(value);
                    }),
                    on_completed: Some(Box::new(move || {
                        Python::with_gil(|py| {
                            let _ = act.call0(py);
                        });
                        downstream.borrow_mut().complete();
                    })),
                    completed: false,
                }));

                source(inner_observer);
            }),
        }
    }

    fn max(&self) -> Self {
        let source = self.subscribe_fn.clone();
        let current_max = Arc::new(Mutex::new(None::<PyObject>));

        Self {
            subscribe_fn: Arc::new(move |downstream| {
                let current_max_clone = current_max.clone();
                let current_max_for_complete = current_max.clone();
                let downstream_clone = downstream.clone();

                let inner_observer = Rc::new(RefCell::new(Observer {
                    on_next: Box::new(move |value| {
                        let mut max = current_max_clone.lock().unwrap();
                        match max.clone() {
                            None => *max = Some(value),
                            Some(current) => {
                                let is_greater = Python::with_gil(|py| {
                                    if let Ok(cmp) = current.call_method1(py, "__gt__", (value.clone(),)) {
                                        cmp.extract::<bool>(py).unwrap_or(false)
                                    } else {
                                        false
                                    }
                                });
                                if !is_greater {
                                    *max = Some(value);
                                }
                            }
                        }
                    }),
                    on_completed: Some(Box::new(move || {
                        if let Some(max_val) = current_max_for_complete.lock().unwrap().clone() {
                            downstream_clone.borrow_mut().next(max_val);
                        }
                        downstream.borrow_mut().complete();
                    })),
                    completed: false,
                }));

                source(inner_observer);
            }),
        }
    }

    fn min(&self) -> Self {
        let source = self.subscribe_fn.clone();
        let current_min = Arc::new(Mutex::new(None::<PyObject>));

        Self {
            subscribe_fn: Arc::new(move |downstream| {
                let current_min_clone = current_min.clone();
                let current_min_for_complete = current_min.clone();
                let downstream_clone = downstream.clone();

                let inner_observer = Rc::new(RefCell::new(Observer {
                    on_next: Box::new(move |value| {
                        let mut min = current_min_clone.lock().unwrap();
                        match min.clone() {
                            None => *min = Some(value),
                            Some(current) => {
                                let is_less = Python::with_gil(|py| {
                                    if let Ok(cmp) = current.call_method1(py, "__lt__", (value.clone(),)) {
                                        cmp.extract::<bool>(py).unwrap_or(false)
                                    } else {
                                        false
                                    }
                                });
                                if !is_less {
                                    *min = Some(value);
                                }
                            }
                        }
                    }),
                    on_completed: Some(Box::new(move || {
                        if let Some(min_val) = current_min_for_complete.lock().unwrap().clone() {
                            downstream_clone.borrow_mut().next(min_val);
                        }
                        downstream.borrow_mut().complete();
                    })),
                    completed: false,
                }));

                source(inner_observer);
            }),
        }
    }

    fn average(&self) -> Self {
        let source = self.subscribe_fn.clone();
        let sum = Arc::new(Mutex::new(None::<PyObject>));
        let count = Arc::new(Mutex::new(0i64));

        Self {
            subscribe_fn: Arc::new(move |downstream| {
                let sum_clone = sum.clone();
                let count_clone = count.clone();
                let sum_for_complete = sum.clone();
                let count_for_complete = count.clone();
                let downstream_clone = downstream.clone();

                let inner_observer = Rc::new(RefCell::new(Observer {
                    on_next: Box::new(move |value| {
                        let mut current_sum = sum_clone.lock().unwrap();
                        match current_sum.clone() {
                            None => *current_sum = Some(value),
                            Some(current) => {
                                let new_sum = Python::with_gil(|py| {
                                    current.call_method1(py, "__add__", (value,)).unwrap()
                                });
                                *current_sum = Some(new_sum);
                            }
                        }
                        *count_clone.lock().unwrap() += 1;
                    }),
                    on_completed: Some(Box::new(move || {
                        let final_sum = sum_for_complete.lock().unwrap().clone();
                        let final_count = *count_for_complete.lock().unwrap();
                        if let Some(sum_val) = final_sum {
                            if final_count > 0 {
                                let avg = Python::with_gil(|py| {
                                    let count_py = final_count.into_pyobject(py).unwrap().unbind().into_any();
                                    sum_val.call_method1(py, "__truediv__", (count_py,)).unwrap()
                                });
                                downstream_clone.borrow_mut().next(avg);
                            }
                        }
                        downstream.borrow_mut().complete();
                    })),
                    completed: false,
                }));

                source(inner_observer);
            }),
        }
    }

    fn debug(&self, name: Option<String>) -> Self {
        let source = self.subscribe_fn.clone();
        let prefix = name.unwrap_or_else(|| String::from("[Observable]"));

        Self {
            subscribe_fn: Arc::new(move |downstream| {
                println!("{} 订阅", prefix);
                let downstream_clone = downstream.clone();
                let prefix_clone = prefix.clone();
                let inner_observer = Rc::new(RefCell::new(Observer {
                    on_next: Box::new(move |value| {
                        Python::with_gil(|py| {
                            let repr = value.call_method0(py, "__repr__").unwrap();
                            println!("{} on_next: {}", prefix_clone, repr.extract::<String>(py).unwrap_or_default());
                        });
                        downstream_clone.borrow_mut().next(value);
                    }),
                    on_completed: Some(Box::new(move || {
                        println!("{} on_completed", prefix);
                        downstream.borrow_mut().complete();
                    })),
                    completed: false,
                }));

                source(inner_observer);
            }),
        }
    }

    fn interval(ms: f64) -> Self {
        // 同步环境下的简化实现 - 只发射一个初始值
        Self {
            subscribe_fn: Arc::new(move |_downstream| {
                // 在真正的异步环境中会周期性发射值
                let _ = ms;
            }),
        }
    }

    fn timer(ms: f64) -> Self {
        // 简化实现
        Self {
            subscribe_fn: Arc::new(move |_downstream| {
                let _ = ms;
            }),
        }
    }

    fn timeout(&self, ms: f64) -> Self {
        // 简化实现 - 直接转发
        let _ = ms;
        Self {
            subscribe_fn: self.subscribe_fn.clone(),
        }
    }

    fn retry(&self, _count: usize) -> Self {
        // 简化实现
        Self {
            subscribe_fn: self.subscribe_fn.clone(),
        }
    }

    fn retry_infinite(&self) -> Self {
        // 简化实现
        Self {
            subscribe_fn: self.subscribe_fn.clone(),
        }
    }

    fn do_on_subscribe(&self, action: PyObject) -> Self {
        let source = self.subscribe_fn.clone();
        let act = Arc::new(Mutex::new(action));

        Self {
            subscribe_fn: Arc::new(move |downstream| {
                Python::with_gil(|py| {
                    let _ = act.lock().unwrap().call0(py);
                });
                let downstream_clone = downstream.clone();
                let inner_observer = Rc::new(RefCell::new(Observer {
                    on_next: Box::new(move |value| {
                        downstream_clone.borrow_mut().next(value);
                    }),
                    on_completed: Some(Box::new(move || {
                        downstream.borrow_mut().complete();
                    })),
                    completed: false,
                }));

                source(inner_observer);
            }),
        }
    }

    fn do_on_dispose(&self, _action: PyObject) -> Self {
        // 简化实现
        Self {
            subscribe_fn: self.subscribe_fn.clone(),
        }
    }

    fn as_subject(&self) -> Self {
        Self {
            subscribe_fn: self.subscribe_fn.clone(),
        }
    }
}

// ============================================================================
// Subject - 主题
// ============================================================================

#[pyclass]
struct PublishSubject {
    observers: Arc<Mutex<Vec<Rc<RefCell<Observer>>>>>,
}

#[pymethods]
impl PublishSubject {
    #[new]
    fn new() -> Self {
        Self {
            observers: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn on_next(&self, value: PyObject) {
        let observers = self.observers.lock().unwrap();
        for observer in observers.iter() {
            observer.borrow_mut().next(value.clone());
        }
    }

    fn on_completed(&self) {
        let observers = self.observers.lock().unwrap();
        for observer in observers.iter() {
            observer.borrow_mut().complete();
        }
    }

    fn subscribe(&self, on_next: PyObject) -> Py<Subscription> {
        let disposed = Rc::new(RefCell::new(false));
        let disposed_clone = disposed.clone();

        let observer = Rc::new(RefCell::new(Observer {
            on_next: Box::new(move |value| {
                if !*disposed_clone.borrow() {
                    Python::with_gil(|py| {
                        let _ = on_next.call1(py, (value,));
                    });
                }
            }),
            on_completed: None,
            completed: false,
        }));

        self.observers.lock().unwrap().push(observer);

        Python::with_gil(|py| {
            Py::new(py, Subscription { disposed }).unwrap()
        })
    }
}

#[pyclass]
struct BehaviorSubject {
    observers: Arc<Mutex<Vec<Rc<RefCell<Observer>>>>>,
    current_value: Arc<Mutex<PyObject>>,
}

#[pymethods]
impl BehaviorSubject {
    #[new]
    fn new(initial_value: PyObject) -> Self {
        Self {
            observers: Arc::new(Mutex::new(Vec::new())),
            current_value: Arc::new(Mutex::new(initial_value)),
        }
    }

    fn on_next(&self, value: PyObject) {
        *self.current_value.lock().unwrap() = value.clone();
        let observers = self.observers.lock().unwrap();
        for observer in observers.iter() {
            observer.borrow_mut().next(value.clone());
        }
    }

    fn on_completed(&self) {
        let observers = self.observers.lock().unwrap();
        for observer in observers.iter() {
            observer.borrow_mut().complete();
        }
    }

    fn subscribe(&self, on_next: PyObject) -> Py<Subscription> {
        let disposed = Rc::new(RefCell::new(false));
        let disposed_clone = disposed.clone();

        let observer = Rc::new(RefCell::new(Observer {
            on_next: Box::new(move |value| {
                if !*disposed_clone.borrow() {
                    Python::with_gil(|py| {
                        let _ = on_next.call1(py, (value,));
                    });
                }
            }),
            on_completed: None,
            completed: false,
        }));

        // 立即发送当前值
        let current = self.current_value.lock().unwrap().clone();
        observer.borrow_mut().next(current);

        self.observers.lock().unwrap().push(observer);

        Python::with_gil(|py| {
            Py::new(py, Subscription { disposed }).unwrap()
        })
    }

    fn value(&self) -> PyObject {
        self.current_value.lock().unwrap().clone()
    }
}

#[pyclass]
struct ReplaySubject {
    observers: Arc<Mutex<Vec<Rc<RefCell<Observer>>>>>,
    buffer: Arc<Mutex<Vec<PyObject>>>,
    buffer_size: usize,
}

#[pymethods]
impl ReplaySubject {
    #[new]
    fn new(buffer_size: usize) -> Self {
        Self {
            observers: Arc::new(Mutex::new(Vec::new())),
            buffer: Arc::new(Mutex::new(Vec::new())),
            buffer_size,
        }
    }

    fn on_next(&self, value: PyObject) {
        let mut buf = self.buffer.lock().unwrap();
        buf.push(value.clone());
        if buf.len() > self.buffer_size {
            buf.remove(0);
        }
        drop(buf);

        let observers = self.observers.lock().unwrap();
        for observer in observers.iter() {
            observer.borrow_mut().next(value.clone());
        }
    }

    fn on_completed(&self) {
        let observers = self.observers.lock().unwrap();
        for observer in observers.iter() {
            observer.borrow_mut().complete();
        }
    }

    fn subscribe(&self, on_next: PyObject) -> Py<Subscription> {
        let disposed = Rc::new(RefCell::new(false));
        let disposed_clone = disposed.clone();

        let observer = Rc::new(RefCell::new(Observer {
            on_next: Box::new(move |value| {
                if !*disposed_clone.borrow() {
                    Python::with_gil(|py| {
                        let _ = on_next.call1(py, (value,));
                    });
                }
            }),
            on_completed: None,
            completed: false,
        }));

        // 重放缓冲区中的值
        let buffered = self.buffer.lock().unwrap().clone();
        for item in buffered {
            observer.borrow_mut().next(item);
        }

        self.observers.lock().unwrap().push(observer);

        Python::with_gil(|py| {
            Py::new(py, Subscription { disposed }).unwrap()
        })
    }
}

// ============================================================================
// Scheduler - 调度器
// ============================================================================

#[pyclass]
struct CurrentThreadScheduler;

#[pymethods]
impl CurrentThreadScheduler {
    #[new]
    fn new() -> Self {
        CurrentThreadScheduler
    }

    fn now(&self) -> f64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs_f64() * 1000.0
    }
}

#[pyclass]
struct ThreadPoolScheduler {
    num_threads: usize,
}

#[pymethods]
impl ThreadPoolScheduler {
    #[new]
    #[pyo3(signature = (num_threads = 2))]
    fn new(num_threads: usize) -> Self {
        ThreadPoolScheduler { num_threads }
    }

    fn now(&self) -> f64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs_f64() * 1000.0
    }

    fn get_num_threads(&self) -> usize {
        self.num_threads
    }
}

#[pyclass]
struct AsyncScheduler;

#[pymethods]
impl AsyncScheduler {
    #[new]
    fn new() -> Self {
        AsyncScheduler
    }

    fn now(&self) -> f64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs_f64() * 1000.0
    }
}

#[pyclass]
struct ImmediateScheduler;

#[pymethods]
impl ImmediateScheduler {
    #[new]
    fn new() -> Self {
        ImmediateScheduler
    }

    fn now(&self) -> f64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs_f64() * 1000.0
    }
}

// ============================================================================
// 模块入口
// ============================================================================

#[pymodule]
fn rxpy(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Subscription>()?;
    m.add_class::<Observable>()?;
    m.add_class::<PublishSubject>()?;
    m.add_class::<BehaviorSubject>()?;
    m.add_class::<ReplaySubject>()?;
    m.add_class::<CurrentThreadScheduler>()?;
    m.add_class::<ThreadPoolScheduler>()?;
    m.add_class::<AsyncScheduler>()?;
    m.add_class::<ImmediateScheduler>()?;
    Ok(())
}
