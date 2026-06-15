use pyo3::prelude::*;
use pyo3::types::PyList;
use std::sync::{Arc, Mutex};

mod file_watcher;
use file_watcher::*;

mod folder_watcher;
use folder_watcher::*;

pub mod clipboard;

pub mod keyboard_mouse;

// ============================================================================
// Subscription - 订阅句柄
// ============================================================================

#[pyclass]
pub struct Subscription {
    disposed: Arc<Mutex<bool>>,
}

#[pymethods]
impl Subscription {
    #[new]
    fn new() -> Self {
        Self {
            disposed: Arc::new(Mutex::new(false)),
        }
    }

    fn dispose(&self) {
        *self.disposed.lock().unwrap() = true;
    }

    fn is_disposed(&self) -> bool {
        *self.disposed.lock().unwrap()
    }
}

// ============================================================================
// 核心类型别名
// ============================================================================

type SubscribeFn = Arc<dyn Fn(PyObject) -> Py<Subscription> + Send + Sync>;

// ============================================================================
// Observable - 可观察对象
// ============================================================================

#[pyclass]
pub struct Observable {
    subscribe_fn: SubscribeFn,
}

impl Observable {
    fn new_impl<F>(f: F) -> Self
    where
        F: Fn(PyObject) -> Py<Subscription> + Send + Sync + 'static,
    {
        Self {
            subscribe_fn: Arc::new(f),
        }
    }
}

#[pymethods]
impl Observable {
    // ---------- 工厂方法 ----------

    #[staticmethod]
    fn from_subscribe_fn(py: Python<'_>, fn_obj: PyObject) -> PyResult<Self> {
        // 将 Python callable 包装为 SubscribeFn：(observer) -> subscription
        let fn_handle = fn_obj.clone_ref(py);
        Ok(Self::new_impl(move |observer: PyObject| {
            Python::with_gil(|py| {
                match fn_handle.call1(py, (observer,)) {
                    Ok(ret) => {
                        if let Ok(sub) = ret.extract::<Py<Subscription>>(py) {
                            return sub;
                        }
                        Py::new(py, Subscription::new()).unwrap()
                    }
                    Err(_) => Py::new(py, Subscription::new()).unwrap(),
                }
            })
        }))
    }

    #[staticmethod]
    fn of(value: PyObject) -> Self {
        Self::new_impl(move |observer| {
            Python::with_gil(|py| {
                let _ = observer.call1(py, (value.clone_ref(py),));
                Py::new(py, Subscription::new()).unwrap()
            })
        })
    }

    #[staticmethod]
    fn empty() -> Self {
        Self::new_impl(move |_| {
            Python::with_gil(|py| Py::new(py, Subscription::new()).unwrap())
        })
    }

    #[staticmethod]
    fn never() -> Self {
        Self::new_impl(move |_| {
            Python::with_gil(|py| Py::new(py, Subscription::new()).unwrap())
        })
    }

    #[staticmethod]
    fn range(start: i64, count: i64) -> Self {
        Self::new_impl(move |observer| {
            Python::with_gil(|py| {
                for i in 0..count {
                    let value = (start + i).to_object(py);
                    let _ = observer.call1(py, (value,));
                }
                Py::new(py, Subscription::new()).unwrap()
            })
        })
    }

    #[staticmethod]
    fn repeat(value: PyObject, count: usize) -> Self {
        Self::new_impl(move |observer| {
            Python::with_gil(|py| {
                for _ in 0..count {
                    let _ = observer.call1(py, (value.clone_ref(py),));
                }
                Py::new(py, Subscription::new()).unwrap()
            })
        })
    }

    #[staticmethod]
    fn from_iter(values: Bound<'_, PyList>) -> Self {
        let items: Vec<PyObject> = Python::with_gil(|py| {
            values.iter().map(|item| item.unbind()).collect()
        });
        Self::new_impl(move |observer| {
            Python::with_gil(|py| {
                for item in &items {
                    let _ = observer.call1(py, (item.clone_ref(py),));
                }
                Py::new(py, Subscription::new()).unwrap()
            })
        })
    }

    // ---------- 订阅方法 ----------

    fn subscribe(&self, on_next: PyObject) -> Py<Subscription> {
        (self.subscribe_fn)(on_next)
    }

    // ---------- 转换操作符 ----------

    fn map(&self, mapper: PyObject) -> Self {
        let source_fn = self.subscribe_fn.clone();
        Self::new_impl(move |downstream_observer| {
            Python::with_gil(|py| {
                let mapper_clone = mapper.clone_ref(py);
                let downstream_clone = downstream_observer.clone_ref(py);
                let wrapped = RustObserver::new(move |py, value| {
                    let result = mapper_clone.call1(py, (value,)).unwrap();
                    let _ = downstream_clone.call1(py, (result,));
                    Ok(())
                });
                source_fn(wrapped.to_object(py));
                Py::new(py, Subscription::new()).unwrap()
            })
        })
    }

    fn filter(&self, predicate: PyObject) -> Self {
        let source_fn = self.subscribe_fn.clone();
        Self::new_impl(move |downstream_observer| {
            Python::with_gil(|py| {
                let predicate_clone = predicate.clone_ref(py);
                let downstream_clone = downstream_observer.clone_ref(py);
                let wrapped = RustObserver::new(move |py, value| {
                    let should_pass: bool = predicate_clone
                        .call1(py, (value.clone_ref(py),))
                        .unwrap()
                        .extract(py)
                        .unwrap_or(false);
                    if should_pass {
                        let _ = downstream_clone.call1(py, (value,));
                    }
                    Ok(())
                });
                source_fn(wrapped.to_object(py));
                Py::new(py, Subscription::new()).unwrap()
            })
        })
    }

    fn flat_map(&self, mapper: PyObject) -> Self {
        let source_fn = self.subscribe_fn.clone();
        Self::new_impl(move |downstream_observer| {
            Python::with_gil(|py| {
                let mapper_clone = mapper.clone_ref(py);
                let downstream_clone = downstream_observer.clone_ref(py);
                let wrapped = RustObserver::new(move |py, value| {
                    let inner = mapper_clone.call1(py, (value,)).unwrap();
                    if let Ok(iter) = inner.call_method0(py, "__iter__") {
                        for item in iter.iter(py).unwrap() {
                            let item = item.unwrap().unbind();
                            let _ = downstream_clone.call1(py, (item,));
                        }
                    }
                    Ok(())
                });
                source_fn(wrapped.to_object(py));
                Py::new(py, Subscription::new()).unwrap()
            })
        })
    }

    fn scan(&self, initial: PyObject, scanner: PyObject) -> Self {
        let source_fn = self.subscribe_fn.clone();
        Self::new_impl(move |downstream_observer| {
            let accumulator = Arc::new(Mutex::new(None::<PyObject>));
            {
                let init_clone = initial.clone();
                *accumulator.lock().unwrap() = Some(init_clone);
            }
            Python::with_gil(|py| {
                let accumulator_clone = accumulator.clone();
                let scanner_clone = scanner.clone_ref(py);
                let downstream_clone = downstream_observer.clone_ref(py);
                let wrapped = RustObserver::new(move |py, value| {
                    let acc_opt = accumulator_clone.lock().unwrap().clone();
                    if let Some(acc) = acc_opt {
                        let new_acc = scanner_clone
                            .call1(py, (acc.clone_ref(py), value.clone_ref(py)))
                            .unwrap();
                        let _ = downstream_clone.call1(py, (new_acc.clone_ref(py),));
                        *accumulator_clone.lock().unwrap() = Some(new_acc);
                    }
                    Ok(())
                });
                source_fn(wrapped.to_object(py));
                Py::new(py, Subscription::new()).unwrap()
            })
        })
    }

    // ---------- 过滤操作符 ----------

    fn skip(&self, n: usize) -> Self {
        let source_fn = self.subscribe_fn.clone();
        Self::new_impl(move |downstream_observer| {
            let counter = Arc::new(Mutex::new(0usize));
            Python::with_gil(|py| {
                let counter_clone = counter.clone();
                let downstream_clone = downstream_observer.clone_ref(py);
                let wrapped = RustObserver::new(move |py, value| {
                    let mut cnt = counter_clone.lock().unwrap();
                    if *cnt < n {
                        *cnt += 1;
                        return Ok(());
                    }
                    drop(cnt);
                    let _ = downstream_clone.call1(py, (value,));
                    Ok(())
                });
                source_fn(wrapped.to_object(py));
                Py::new(py, Subscription::new()).unwrap()
            })
        })
    }

    fn take(&self, n: usize) -> Self {
        let source_fn = self.subscribe_fn.clone();
        Self::new_impl(move |downstream_observer| {
            let counter = Arc::new(Mutex::new(0usize));
            Python::with_gil(|py| {
                let counter_clone = counter.clone();
                let downstream_clone = downstream_observer.clone_ref(py);
                let wrapped = RustObserver::new(move |py, value| {
                    let mut cnt = counter_clone.lock().unwrap();
                    if *cnt < n {
                        *cnt += 1;
                        drop(cnt);
                        let _ = downstream_clone.call1(py, (value,));
                    }
                    Ok(())
                });
                source_fn(wrapped.to_object(py));
                Py::new(py, Subscription::new()).unwrap()
            })
        })
    }

    fn first(&self) -> Self {
        self.take(1)
    }

    fn last(&self) -> Self {
        let source_fn = self.subscribe_fn.clone();
        Self::new_impl(move |downstream_observer| {
            Python::with_gil(|py| {
                let last_value = Arc::new(Mutex::new(py.None()));
                let have_value = Arc::new(Mutex::new(false));
                let last_value_clone = last_value.clone();
                let have_value_clone = have_value.clone();
                let downstream_clone = downstream_observer.clone_ref(py);

                let wrapped = RustObserver::new(move |py, value| {
                    *last_value_clone.lock().unwrap() = value.clone_ref(py);
                    *have_value_clone.lock().unwrap() = true;
                    Ok(())
                });
                source_fn(wrapped.to_object(py));

                if *have_value.lock().unwrap() {
                    let last = last_value.lock().unwrap().clone_ref(py);
                    let _ = downstream_observer.call1(py, (last,));
                }
                Py::new(py, Subscription::new()).unwrap()
            })
        })
    }

    fn count(&self) -> Self {
        let source_fn = self.subscribe_fn.clone();
        Self::new_impl(move |downstream_observer| {
            let counter = Arc::new(Mutex::new(0i64));
            Python::with_gil(|py| {
                let counter_clone = counter.clone();
                let wrapped = RustObserver::new(move |_py, _value| {
                    *counter_clone.lock().unwrap() += 1;
                    Ok(())
                });
                source_fn(wrapped.to_object(py));
                let final_count = *counter.lock().unwrap();
                let _ = downstream_observer.call1(py, (final_count,));
                Py::new(py, Subscription::new()).unwrap()
            })
        })
    }

    fn sum(&self) -> Self {
        let source_fn = self.subscribe_fn.clone();
        Self::new_impl(move |downstream_observer| {
            Python::with_gil(|py| {
                let total = Arc::new(Mutex::new(py.None()));
                let have_value = Arc::new(Mutex::new(false));
                let total_clone = total.clone();
                let have_value_clone = have_value.clone();
                let downstream_clone = downstream_observer.clone_ref(py);

                let wrapped = RustObserver::new(move |py, value| {
                    let mut current = total_clone.lock().unwrap();
                    if !*have_value_clone.lock().unwrap() {
                        *current = value.clone_ref(py);
                        *have_value_clone.lock().unwrap() = true;
                    } else {
                        let current_value = current.clone_ref(py);
                        let new_value = current_value.call_method1(py, "__add__", (value,)).unwrap();
                        *current = new_value;
                    }
                    Ok(())
                });
                source_fn(wrapped.to_object(py));

                if *have_value.lock().unwrap() {
                    let total_value = total.lock().unwrap().clone_ref(py);
                    let _ = downstream_clone.call1(py, (total_value,));
                }
                Py::new(py, Subscription::new()).unwrap()
            })
        })
    }

    fn reduce(&self, initial: PyObject, reducer: PyObject) -> Self {
        let source_fn = self.subscribe_fn.clone();
        Self::new_impl(move |downstream_observer| {
            let accumulator = Arc::new(Mutex::new(Some(initial.clone())));
            Python::with_gil(|py| {
                let accumulator_clone = accumulator.clone();
                let reducer_clone = reducer.clone_ref(py);
                let wrapped = RustObserver::new(move |py, value| {
                    let acc_opt = accumulator_clone.lock().unwrap().clone();
                    if let Some(acc) = acc_opt {
                        let new_acc = reducer_clone
                            .call1(py, (acc.clone_ref(py), value.clone_ref(py)))
                            .unwrap();
                        *accumulator_clone.lock().unwrap() = Some(new_acc);
                    }
                    Ok(())
                });
                source_fn(wrapped.to_object(py));

                if let Some(final_acc) = accumulator.lock().unwrap().take() {
                    let _ = downstream_observer.call1(py, (final_acc,));
                }
                Py::new(py, Subscription::new()).unwrap()
            })
        })
    }

    fn contains(&self, target: PyObject) -> Self {
        let source_fn = self.subscribe_fn.clone();
        Self::new_impl(move |downstream_observer| {
            let found = Arc::new(Mutex::new(false));
            Python::with_gil(|py| {
                let found_clone = found.clone();
                let target_clone = target.clone_ref(py);
                let wrapped = RustObserver::new(move |py, value| {
                    if !*found_clone.lock().unwrap() {
                        let eq = target_clone
                            .call_method1(py, "__eq__", (value.clone_ref(py),))
                            .unwrap();
                        let is_eq: bool = eq.extract(py).unwrap_or(false);
                        if is_eq {
                            *found_clone.lock().unwrap() = true;
                        }
                    }
                    Ok(())
                });
                source_fn(wrapped.to_object(py));

                let result = if *found.lock().unwrap() { true.to_object(py) } else { false.to_object(py) };
                let _ = downstream_observer.call1(py, (result,));
                Py::new(py, Subscription::new()).unwrap()
            })
        })
    }

    fn all(&self, predicate: PyObject) -> Self {
        let source_fn = self.subscribe_fn.clone();
        Self::new_impl(move |downstream_observer| {
            let all_pass = Arc::new(Mutex::new(true));
            Python::with_gil(|py| {
                let all_pass_clone = all_pass.clone();
                let predicate_clone = predicate.clone_ref(py);
                let wrapped = RustObserver::new(move |py, value| {
                    if *all_pass_clone.lock().unwrap() {
                        let result = predicate_clone
                            .call1(py, (value.clone_ref(py),))
                            .unwrap();
                        let pass: bool = result.extract(py).unwrap_or(false);
                        if !pass {
                            *all_pass_clone.lock().unwrap() = false;
                        }
                    }
                    Ok(())
                });
                source_fn(wrapped.to_object(py));

                let result = if *all_pass.lock().unwrap() { true.to_object(py) } else { false.to_object(py) };
                let _ = downstream_observer.call1(py, (result,));
                Py::new(py, Subscription::new()).unwrap()
            })
        })
    }

    fn collect(&self) -> PyObject {
        let source_fn = self.subscribe_fn.clone();
        Python::with_gil(|py| {
            let items: Arc<Mutex<Vec<PyObject>>> = Arc::new(Mutex::new(Vec::new()));
            let items_clone = items.clone();
            let wrapped = RustObserver::new(move |py, value| {
                items_clone.lock().unwrap().push(value.clone_ref(py));
                Ok(())
            });
            source_fn(wrapped.to_object(py));

            let collected = items.lock().unwrap();
            let list = PyList::empty_bound(py);
            for item in collected.iter() {
                let _ = list.append(item.clone_ref(py));
            }
            list.unbind().into_any()
        })
    }

    fn merge(&self, other: PyObject) -> Self {
        let source_fn = self.subscribe_fn.clone();
        Self::new_impl(move |downstream_observer| {
            Python::with_gil(|py| {
                let downstream_clone = downstream_observer.clone_ref(py);
                let wrapped_self = RustObserver::new(move |py, value| {
                    let _ = downstream_clone.call1(py, (value,));
                    Ok(())
                });
                source_fn(wrapped_self.to_object(py));

                if let Ok(other_inner) = other.getattr(py, "_inner") {
                    if let Ok(other_sub_fn) = other_inner.getattr(py, "subscribe_fn") {
                        let _ = other_sub_fn.call1(py, (RustObserver::new(move |py, value| {
                            let _ = downstream_observer.call1(py, (value,));
                            Ok(())
                        }).to_object(py),));
                    }
                }
                Py::new(py, Subscription::new()).unwrap()
            })
        })
    }

    fn concat(&self, other: PyObject) -> Self {
        self.merge(other)
    }

    // ---------- 组合操作符 ----------

    fn start_with(&self, value: PyObject) -> Self {
        let source_fn = self.subscribe_fn.clone();
        Self::new_impl(move |downstream_observer| {
            Python::with_gil(|py| {
                let _ = downstream_observer.call1(py, (value.clone_ref(py),));
                source_fn(downstream_observer);
                Py::new(py, Subscription::new()).unwrap()
            })
        })
    }

    fn default_if_empty(&self, default: PyObject) -> Self {
        let source_fn = self.subscribe_fn.clone();
        Self::new_impl(move |downstream_observer| {
            let emitted = Arc::new(Mutex::new(false));
            Python::with_gil(|py| {
                let emitted_clone = emitted.clone();
                let downstream_clone = downstream_observer.clone_ref(py);
                let default_clone = default.clone_ref(py);

                let wrapped = RustObserver::new(move |py, value| {
                    *emitted_clone.lock().unwrap() = true;
                    let _ = downstream_clone.call1(py, (value,));
                    Ok(())
                });
                source_fn(wrapped.to_object(py));

                if !*emitted.lock().unwrap() {
                    let _ = downstream_observer.call1(py, (default_clone,));
                }
                Py::new(py, Subscription::new()).unwrap()
            })
        })
    }

    // ---------- 调试操作符 ----------

    fn do_on_next(&self, action: PyObject) -> Self {
        let source_fn = self.subscribe_fn.clone();
        Self::new_impl(move |downstream_observer| {
            Python::with_gil(|py| {
                let action_clone = action.clone_ref(py);
                let downstream_clone = downstream_observer.clone_ref(py);
                let wrapped = RustObserver::new(move |py, value| {
                    let _ = action_clone.call1(py, (value.clone_ref(py),));
                    let _ = downstream_clone.call1(py, (value,));
                    Ok(())
                });
                source_fn(wrapped.to_object(py));
                Py::new(py, Subscription::new()).unwrap()
            })
        })
    }

    fn run(&self) -> Self {
        Python::with_gil(|py| {
            let noop = py.None();
            let source_fn = self.subscribe_fn.clone();
            source_fn(noop);
        });
        let source_fn = self.subscribe_fn.clone();
        Self::new_impl(move |observer| source_fn(observer))
    }

    #[pyo3(signature = (name=None))]
    fn debug(&self, name: Option<String>) -> Self {
        let prefix = name.unwrap_or_else(|| String::from("[Observable]"));
        println!("{} subscribed", prefix);
        let source_fn = self.subscribe_fn.clone();
        Self::new_impl(move |downstream_observer| {
            let prefix = prefix.clone();
            Python::with_gil(|py| {
                let prefix_clone = prefix.clone();
                let downstream_clone = downstream_observer.clone_ref(py);
                let wrapped = RustObserver::new(move |py, value| {
                    let repr = value.call_method0(py, "__repr__").unwrap();
                    let repr_str: String = repr.extract(py).unwrap_or_default();
                    println!("{} on_next: {}", prefix_clone, repr_str);
                    let _ = downstream_clone.call1(py, (value,));
                    Ok(())
                });
                source_fn(wrapped.to_object(py));
                println!("{} on_completed", prefix);
                Py::new(py, Subscription::new()).unwrap()
            })
        })
    }
}

// ============================================================================
// RustObserver - 将 Rust 闭包包装成 Python 可调用对象
// ============================================================================

type ObserverClosure = Arc<dyn Fn(Python<'_>, PyObject) -> PyResult<()> + Send + Sync>;

#[pyclass]
pub struct RustObserver {
    inner: ObserverClosure,
}

impl RustObserver {
    fn new<F>(f: F) -> Self
    where
        F: Fn(Python<'_>, PyObject) -> PyResult<()> + Send + Sync + 'static,
    {
        Self {
            inner: Arc::new(f),
        }
    }

    fn to_object(&self, py: Python<'_>) -> PyObject {
        let cloned = RustObserver {
            inner: self.inner.clone(),
        };
        Py::new(py, cloned).unwrap().into_any()
    }
}

#[pymethods]
impl RustObserver {
    fn __call__(&self, value: PyObject) -> PyResult<()> {
        Python::with_gil(|py| (self.inner)(py, value))
    }
}

// ============================================================================
// Subject - 主题
// ============================================================================

#[pyclass]
pub struct PublishSubject {
    observers: Arc<Mutex<Vec<(usize, Arc<Mutex<bool>>, PyObject)>>>,
    next_id: Arc<Mutex<usize>>,
}

#[pymethods]
impl PublishSubject {
    #[new]
    fn new() -> Self {
        Self {
            observers: Arc::new(Mutex::new(Vec::new())),
            next_id: Arc::new(Mutex::new(0)),
        }
    }

    fn on_next(&self, value: PyObject) {
        Python::with_gil(|py| {
            let snapshot: Vec<(usize, Arc<Mutex<bool>>, PyObject)> = self
                .observers
                .lock()
                .unwrap()
                .iter()
                .map(|(id, flag, cb)| (*id, Arc::clone(flag), cb.clone_ref(py)))
                .collect();
            for (_, flag, cb) in snapshot.iter() {
                if !*flag.lock().unwrap() {
                    let _ = cb.call1(py, (value.clone_ref(py),));
                }
            }
            // 事后清理：从列表中移除已 disposed 的观察者
            self.observers.lock().unwrap().retain(|_, flag, _| !*flag.lock().unwrap());
        })
    }

    fn on_completed(&self) {
        self.observers.lock().unwrap().clear();
    }

    fn subscribe(&self, on_next: PyObject) -> Py<Subscription> {
        Python::with_gil(|py| {
            let mut id = self.next_id.lock().unwrap();
            let my_id = *id;
            *id += 1;
            drop(id);

            let disposed = Arc::new(Mutex::new(false));
            let disposed_clone = disposed.clone();

            let wrapped = RustObserver::new(move |py, value| {
                if !*disposed_clone.lock().unwrap() {
                    let _ = on_next.call1(py, (value,));
                }
                Ok(())
            });

            self.observers.lock().unwrap().push((my_id, disposed.clone(), wrapped.to_object(py)));
            Py::new(py, Subscription { disposed }).unwrap()
        })
    }
}

#[pyclass]
pub struct BehaviorSubject {
    observers: Arc<Mutex<Vec<(usize, Arc<Mutex<bool>>, PyObject)>>>,
    next_id: Arc<Mutex<usize>>,
    current_value: Arc<Mutex<PyObject>>,
}

#[pymethods]
impl BehaviorSubject {
    #[new]
    fn new(initial_value: PyObject) -> Self {
        Self {
            observers: Arc::new(Mutex::new(Vec::new())),
            next_id: Arc::new(Mutex::new(0)),
            current_value: Arc::new(Mutex::new(initial_value)),
        }
    }

    fn on_next(&self, value: PyObject) {
        Python::with_gil(|py| {
            *self.current_value.lock().unwrap() = value.clone_ref(py);
            let snapshot: Vec<(usize, Arc<Mutex<bool>>, PyObject)> = self
                .observers
                .lock()
                .unwrap()
                .iter()
                .map(|(id, flag, cb)| (*id, Arc::clone(flag), cb.clone_ref(py)))
                .collect();
            for (_, flag, cb) in snapshot.iter() {
                if !*flag.lock().unwrap() {
                    let _ = cb.call1(py, (value.clone_ref(py),));
                }
            }
            self.observers.lock().unwrap().retain(|_, flag, _| !*flag.lock().unwrap());
        })
    }

    fn on_completed(&self) {
        self.observers.lock().unwrap().clear();
    }

    fn subscribe(&self, on_next: PyObject) -> Py<Subscription> {
        Python::with_gil(|py| {
            let mut id = self.next_id.lock().unwrap();
            let my_id = *id;
            *id += 1;
            drop(id);

            let disposed = Arc::new(Mutex::new(false));
            let disposed_clone = disposed.clone();

            let current = self.current_value.lock().unwrap().clone_ref(py);
            let _ = on_next.call1(py, (current,));

            let wrapped = RustObserver::new(move |py, value| {
                if !*disposed_clone.lock().unwrap() {
                    let _ = on_next.call1(py, (value,));
                }
                Ok(())
            });

            self.observers.lock().unwrap().push((my_id, disposed.clone(), wrapped.to_object(py)));
            Py::new(py, Subscription { disposed }).unwrap()
        })
    }

    fn value(&self) -> PyObject {
        Python::with_gil(|py| self.current_value.lock().unwrap().clone_ref(py))
    }
}

#[pyclass]
pub struct ReplaySubject {
    observers: Arc<Mutex<Vec<(usize, Arc<Mutex<bool>>, PyObject)>>>,
    next_id: Arc<Mutex<usize>>,
    buffer: Arc<Mutex<Vec<PyObject>>>,
    buffer_size: usize,
}

#[pymethods]
impl ReplaySubject {
    #[new]
    fn new(buffer_size: usize) -> Self {
        Self {
            observers: Arc::new(Mutex::new(Vec::new())),
            next_id: Arc::new(Mutex::new(0)),
            buffer: Arc::new(Mutex::new(Vec::new())),
            buffer_size,
        }
    }

    fn on_next(&self, value: PyObject) {
        Python::with_gil(|py| {
            {
                let mut buf = self.buffer.lock().unwrap();
                buf.push(value.clone_ref(py));
                if buf.len() > self.buffer_size {
                    buf.remove(0);
                }
            }
            let snapshot: Vec<(usize, Arc<Mutex<bool>>, PyObject)> = self
                .observers
                .lock()
                .unwrap()
                .iter()
                .map(|(id, flag, cb)| (*id, Arc::clone(flag), cb.clone_ref(py)))
                .collect();
            for (_, flag, cb) in snapshot.iter() {
                if !*flag.lock().unwrap() {
                    let _ = cb.call1(py, (value.clone_ref(py),));
                }
            }
            self.observers.lock().unwrap().retain(|_, flag, _| !*flag.lock().unwrap());
        })
    }

    fn on_completed(&self) {
        self.observers.lock().unwrap().clear();
    }

    fn subscribe(&self, on_next: PyObject) -> Py<Subscription> {
        Python::with_gil(|py| {
            let mut id = self.next_id.lock().unwrap();
            let my_id = *id;
            *id += 1;
            drop(id);

            let disposed = Arc::new(Mutex::new(false));
            let disposed_clone = disposed.clone();

            let buffered: Vec<PyObject> = self
                .buffer
                .lock()
                .unwrap()
                .iter()
                .map(|v| v.clone_ref(py))
                .collect();
            for item in buffered {
                let _ = on_next.call1(py, (item,));
            }

            let wrapped = RustObserver::new(move |py, value| {
                if !*disposed_clone.lock().unwrap() {
                    let _ = on_next.call1(py, (value,));
                }
                Ok(())
            });

            self.observers.lock().unwrap().push((my_id, disposed.clone(), wrapped.to_object(py)));
            Py::new(py, Subscription { disposed }).unwrap()
        })
    }
}

// ============================================================================
// Scheduler - 调度器
// ============================================================================

#[pyclass]
pub struct CurrentThreadScheduler;

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
            .as_secs_f64()
            * 1000.0
    }
}

#[pyclass]
pub struct ThreadPoolScheduler {
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
            .as_secs_f64()
            * 1000.0
    }

    fn get_num_threads(&self) -> usize {
        self.num_threads
    }
}

#[pyclass]
pub struct AsyncScheduler;

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
            .as_secs_f64()
            * 1000.0
    }
}

#[pyclass]
pub struct ImmediateScheduler;

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
            .as_secs_f64()
            * 1000.0
    }
}

// ============================================================================
// 模块入口
// ============================================================================

#[pymodule]
fn rx_rust(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Subscription>()?;
    m.add_class::<Observable>()?;
    m.add_class::<PublishSubject>()?;
    m.add_class::<BehaviorSubject>()?;
    m.add_class::<ReplaySubject>()?;
    m.add_class::<CurrentThreadScheduler>()?;
    m.add_class::<ThreadPoolScheduler>()?;
    m.add_class::<AsyncScheduler>()?;
    m.add_class::<ImmediateScheduler>()?;
    add_file_watcher_to_module(m)?;
    crate::clipboard::toplevel::register_clipboard_module(m)?;
    crate::keyboard_mouse::toplevel::register_keyboard_mouse_module(m)?;
    add_folder_watcher_to_module(m)?;
    Ok(())
}
