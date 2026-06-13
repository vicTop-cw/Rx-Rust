#[cfg(test)]
mod tests {
    use rx_rust::prelude::*;
    use std::sync::{Arc, Mutex, atomic::{AtomicUsize, Ordering}};
    use std::task::{Context, Poll, Waker, RawWaker, RawWakerVTable};
    use std::future::Future;
    use std::pin::Pin;

    fn noop_raw_waker() -> RawWaker {
        RawWaker::new(std::ptr::null(), &RawWakerVTable::new(
            |_| noop_raw_waker(),
            |_| {},
            |_| {},
            |_| {},
        ))
    }

    fn noop_waker() -> Waker {
        unsafe { Waker::from_raw(noop_raw_waker()) }
    }

    #[test]
    fn test_of() {
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);
        
        of::<i32, ()>(42).subscribe(FnObserver::new(
            move |v: Result<i32, ()>| {
                if let Ok(val) = v {
                    values_clone.lock().unwrap().push(val);
                }
            },
            || {},
        ));
        
        assert_eq!(values.lock().unwrap().as_slice(), &[42]);
    }

    #[test]
    fn test_from_iter() {
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);
        
        from_iter::<i32, ()>(vec![1, 2, 3]).subscribe(FnObserver::new(
            move |v: Result<i32, ()>| {
                if let Ok(val) = v {
                    values_clone.lock().unwrap().push(val);
                }
            },
            || {},
        ));
        
        assert_eq!(values.lock().unwrap().as_slice(), &[1, 2, 3]);
    }

    #[test]
    fn test_empty() {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);
        
        empty::<i32, ()>().subscribe(FnObserver::new(
            move |_v: Result<i32, ()>| {
                counter_clone.fetch_add(1, Ordering::SeqCst);
            },
            || {},
        ));
        
        assert_eq!(counter.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn test_map() {
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);
        
        from_iter::<i32, ()>(vec![1, 2, 3])
            .map(|x| x * 2)
            .subscribe(FnObserver::new(
                move |v: Result<i32, ()>| {
                    if let Ok(val) = v {
                        values_clone.lock().unwrap().push(val);
                    }
                },
                || {},
            ));
        
        assert_eq!(values.lock().unwrap().as_slice(), &[2, 4, 6]);
    }

    #[test]
    fn test_filter() {
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);
        
        from_iter::<i32, ()>(vec![1, 2, 3, 4, 5])
            .filter(|x| *x > 2)
            .subscribe(FnObserver::new(
                move |v: Result<i32, ()>| {
                    if let Ok(val) = v {
                        values_clone.lock().unwrap().push(val);
                    }
                },
                || {},
            ));
        
        assert_eq!(values.lock().unwrap().as_slice(), &[3, 4, 5]);
    }

    #[test]
    fn test_map_filter_chain() {
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);
        
        from_iter::<i32, ()>(vec![1, 2, 3, 4, 5])
            .map(|x| x * 2)
            .filter(|x| *x > 4)
            .subscribe(FnObserver::new(
                move |v: Result<i32, ()>| {
                    if let Ok(val) = v {
                        values_clone.lock().unwrap().push(val);
                    }
                },
                || {},
            ));
        
        assert_eq!(values.lock().unwrap().as_slice(), &[6, 8, 10]);
    }

    #[test]
    fn test_collect() {
        let mut future = from_iter::<i32, ()>(vec![1, 2, 3])
            .map(|x| x * 2)
            .filter(|x| *x > 2)
            .collect();
        
        let waker = noop_waker();
        let mut context = Context::from_waker(&waker);
        let pinned = Pin::new(&mut future);
        if let Poll::Ready(result) = pinned.poll(&mut context) {
            assert_eq!(result, vec![4, 6]);
        } else {
            panic!("Future should be ready");
        }
    }

    #[test]
    fn test_take() {
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);
        
        from_iter::<i32, ()>(vec![1, 2, 3, 4, 5])
            .take(3)
            .subscribe(FnObserver::new(
                move |v: Result<i32, ()>| {
                    if let Ok(val) = v {
                        values_clone.lock().unwrap().push(val);
                    }
                },
                || {},
            ));
        
        assert_eq!(values.lock().unwrap().as_slice(), &[1, 2, 3]);
    }

    #[test]
    fn test_skip() {
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);
        
        from_iter::<i32, ()>(vec![1, 2, 3, 4, 5])
            .skip(2)
            .subscribe(FnObserver::new(
                move |v: Result<i32, ()>| {
                    if let Ok(val) = v {
                        values_clone.lock().unwrap().push(val);
                    }
                },
                || {},
            ));
        
        assert_eq!(values.lock().unwrap().as_slice(), &[3, 4, 5]);
    }

    #[test]
    fn test_first() {
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);
        
        from_iter::<i32, ()>(vec![1, 2, 3])
            .first()
            .subscribe(FnObserver::new(
                move |v: Result<i32, ()>| {
                    if let Ok(val) = v {
                        values_clone.lock().unwrap().push(val);
                    }
                },
                || {},
            ));
        
        assert_eq!(values.lock().unwrap().as_slice(), &[1]);
    }

    #[test]
    fn test_last() {
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);
        
        from_iter::<i32, ()>(vec![1, 2, 3])
            .last()
            .subscribe(FnObserver::new(
                move |v: Result<i32, ()>| {
                    if let Ok(val) = v {
                        values_clone.lock().unwrap().push(val);
                    }
                },
                || {},
            ));
        
        assert_eq!(values.lock().unwrap().as_slice(), &[3]);
    }

    #[test]
    fn test_take_while() {
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);
        
        from_iter::<i32, ()>(vec![1, 2, 3, 4, 5])
            .take_while(|x| *x < 4)
            .subscribe(FnObserver::new(
                move |v: Result<i32, ()>| {
                    if let Ok(val) = v {
                        values_clone.lock().unwrap().push(val);
                    }
                },
                || {},
            ));
        
        assert_eq!(values.lock().unwrap().as_slice(), &[1, 2, 3]);
    }

    #[test]
    fn test_skip_while() {
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);
        
        from_iter::<i32, ()>(vec![1, 2, 3, 4, 5])
            .skip_while(|x| *x < 3)
            .subscribe(FnObserver::new(
                move |v: Result<i32, ()>| {
                    if let Ok(val) = v {
                        values_clone.lock().unwrap().push(val);
                    }
                },
                || {},
            ));
        
        assert_eq!(values.lock().unwrap().as_slice(), &[3, 4, 5]);
    }

    #[test]
    fn test_flat_map() {
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);
        
        from_iter::<i32, ()>(vec![1, 2, 3])
            .flat_map(|x| from_iter::<i32, ()>(vec![x, x * 10]))
            .subscribe(FnObserver::new(
                move |v: Result<i32, ()>| {
                    if let Ok(val) = v {
                        values_clone.lock().unwrap().push(val);
                    }
                },
                || {},
            ));
        
        assert_eq!(values.lock().unwrap().as_slice(), &[1, 10, 2, 20, 3, 30]);
    }

    #[test]
    fn test_scan() {
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);
        
        from_iter::<i32, ()>(vec![1, 2, 3])
            .scan(0, |acc, x| acc + x)
            .subscribe(FnObserver::new(
                move |v: Result<i32, ()>| {
                    if let Ok(val) = v {
                        values_clone.lock().unwrap().push(val);
                    }
                },
                || {},
            ));
        
        assert_eq!(values.lock().unwrap().as_slice(), &[1, 3, 6]);
    }

    #[test]
    fn test_buffer() {
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);
        
        from_iter::<i32, ()>(vec![1, 2, 3, 4, 5])
            .buffer(2)
            .subscribe(FnObserver::new(
                move |v: Result<Vec<i32>, ()>| {
                    if let Ok(val) = v {
                        values_clone.lock().unwrap().push(val);
                    }
                },
                || {},
            ));
        
        let result = values.lock().unwrap().clone();
        assert_eq!(result, vec![vec![1, 2], vec![3, 4], vec![5]]);
    }

    #[test]
    fn test_merge() {
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);
        
        from_iter::<i32, ()>(vec![1, 2, 3])
            .merge(from_iter::<i32, ()>(vec![4, 5, 6]))
            .subscribe(FnObserver::new(
                move |v: Result<i32, ()>| {
                    if let Ok(val) = v {
                        values_clone.lock().unwrap().push(val);
                    }
                },
                || {},
            ));
        
        let mut result = values.lock().unwrap().clone();
        result.sort();
        assert_eq!(result, vec![1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn test_zip() {
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);
        
        from_iter::<i32, ()>(vec![1, 2, 3])
            .zip(from_iter::<i32, ()>(vec![10, 20, 30]), |a, b| a + b)
            .subscribe(FnObserver::new(
                move |v: Result<i32, ()>| {
                    if let Ok(val) = v {
                        values_clone.lock().unwrap().push(val);
                    }
                },
                || {},
            ));
        
        assert_eq!(values.lock().unwrap().as_slice(), &[11, 22, 33]);
    }

    #[test]
    fn test_concat() {
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);
        
        from_iter::<i32, ()>(vec![1, 2, 3])
            .concat(from_iter::<i32, ()>(vec![4, 5, 6]))
            .subscribe(FnObserver::new(
                move |v: Result<i32, ()>| {
                    if let Ok(val) = v {
                        values_clone.lock().unwrap().push(val);
                    }
                },
                || {},
            ));
        
        assert_eq!(values.lock().unwrap().as_slice(), &[1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn test_combine_latest() {
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);
        
        from_iter::<i32, ()>(vec![1, 2, 3])
            .combine_latest(from_iter::<i32, ()>(vec![10, 20]), |a, b| a + b)
            .subscribe(FnObserver::new(
                move |v: Result<i32, ()>| {
                    if let Ok(val) = v {
                        values_clone.lock().unwrap().push(val);
                    }
                },
                || {},
            ));
        
        assert_eq!(values.lock().unwrap().as_slice(), &[13, 23]);
    }

    #[test]
    fn test_publish_subject() {
        let subject = PublishSubject::<i32, ()>::new();
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);
        
        subject.subscribe_ref(FnObserver::new(
            move |v: Result<i32, ()>| {
                if let Ok(val) = v {
                    values_clone.lock().unwrap().push(val);
                }
            },
            || {},
        ));
        
        subject.on_next(Ok(1));
        subject.on_next(Ok(2));
        subject.on_next(Ok(3));
        
        assert_eq!(values.lock().unwrap().as_slice(), &[1, 2, 3]);
    }

    #[test]
    fn test_behavior_subject() {
        let subject = BehaviorSubject::<i32, ()>::new(0);
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);
        
        subject.subscribe_ref(FnObserver::new(
            move |v: Result<i32, ()>| {
                if let Ok(val) = v {
                    values_clone.lock().unwrap().push(val);
                }
            },
            || {},
        ));
        
        subject.on_next(Ok(1));
        subject.on_next(Ok(2));
        
        assert_eq!(values.lock().unwrap().as_slice(), &[0, 1, 2]);
    }

    #[test]
    fn test_replay_subject() {
        let subject = ReplaySubject::<i32, ()>::new(2);
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);
        
        subject.on_next(Ok(1));
        subject.on_next(Ok(2));
        subject.on_next(Ok(3));
        
        subject.subscribe_ref(FnObserver::new(
            move |v: Result<i32, ()>| {
                if let Ok(val) = v {
                    values_clone.lock().unwrap().push(val);
                }
            },
            || {},
        ));
        
        subject.on_next(Ok(4));
        
        assert_eq!(values.lock().unwrap().as_slice(), &[2, 3, 4]);
    }


    // ===== 创建操作符：never, throw =====
    #[test]
    fn test_never() {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);

        never::<i32, ()>().subscribe(FnObserver::new(
            move |_v: Result<i32, ()>| {
                counter_clone.fetch_add(1, Ordering::SeqCst);
            },
            || {},
        ));

        assert_eq!(counter.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn test_throw() {
        let errors = Arc::new(Mutex::new(Vec::new()));
        let errors_clone = Arc::clone(&errors);

        throw::<i32, String>("oops".into()).subscribe(FnObserver::new(
            move |v: Result<i32, String>| {
                if let Err(e) = v {
                    errors_clone.lock().unwrap().push(e);
                }
            },
            || {},
        ));

        assert_eq!(errors.lock().unwrap().as_slice(), &["oops".to_string()]);
    }

    // ===== 创建操作符：range, repeat, defer, generate =====
    #[test]
    fn test_range() {
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);

        range::<i32, ()>(0, 5).subscribe(FnObserver::new(
            move |v: Result<i32, ()>| {
                if let Ok(val) = v {
                    values_clone.lock().unwrap().push(val);
                }
            },
            || {},
        ));

        assert_eq!(values.lock().unwrap().as_slice(), &[0, 1, 2, 3, 4]);
    }

    #[test]
    fn test_repeat() {
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);

        repeat::<i32, ()>(42, 3).subscribe(FnObserver::new(
            move |v: Result<i32, ()>| {
                if let Ok(val) = v {
                    values_clone.lock().unwrap().push(val);
                }
            },
            || {},
        ));

        assert_eq!(values.lock().unwrap().as_slice(), &[42, 42, 42]);
    }

    #[test]
    fn test_defer() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = Arc::clone(&call_count);
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);

        let obs = defer::<_, _, i32, ()>(move || {
            call_count_clone.fetch_add(1, Ordering::SeqCst);
            from_iter::<i32, ()>(vec![1, 2, 3])
        });

        assert_eq!(call_count.load(Ordering::SeqCst), 0);

        obs.subscribe(FnObserver::new(
            move |v: Result<i32, ()>| {
                if let Ok(val) = v {
                    values_clone.lock().unwrap().push(val);
                }
            },
            || {},
        ));

        assert_eq!(call_count.load(Ordering::SeqCst), 1);
        assert_eq!(values.lock().unwrap().as_slice(), &[1, 2, 3]);
    }

    #[test]
    fn test_generate() {
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);

        generate::<_, _, i32, ()>(0, |state| {
            let next = state + 1;
            let cont = state < 5;
            (state, next, cont)
        }).subscribe(FnObserver::new(
            move |v: Result<i32, ()>| {
                if let Ok(val) = v {
                    values_clone.lock().unwrap().push(val);
                }
            },
            || {},
        ));

        assert_eq!(values.lock().unwrap().as_slice(), &[0, 1, 2, 3, 4]);
    }

    // ===== 过滤扩展：take_last, skip_last, element_at, distinct, ignore_elements =====
    #[test]
    fn test_take_last() {
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);

        from_iter::<i32, ()>(vec![1, 2, 3, 4, 5])
            .take_last(2)
            .subscribe(FnObserver::new(
                move |v: Result<Vec<i32>, ()>| {
                    if let Ok(val) = v {
                        values_clone.lock().unwrap().extend(val);
                    }
                },
                || {},
            ));

        assert_eq!(values.lock().unwrap().as_slice(), &[4, 5]);
    }

    #[test]
    fn test_skip_last() {
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);

        from_iter::<i32, ()>(vec![1, 2, 3, 4, 5])
            .skip_last(2)
            .subscribe(FnObserver::new(
                move |v: Result<i32, ()>| {
                    if let Ok(val) = v {
                        values_clone.lock().unwrap().push(val);
                    }
                },
                || {},
            ));

        assert_eq!(values.lock().unwrap().as_slice(), &[1, 2, 3]);
    }

    #[test]
    fn test_element_at() {
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);

        from_iter::<i32, ()>(vec![10, 20, 30, 40])
            .element_at(2)
            .subscribe(FnObserver::new(
                move |v: Result<i32, ()>| {
                    if let Ok(val) = v {
                        values_clone.lock().unwrap().push(val);
                    }
                },
                || {},
            ));

        assert_eq!(values.lock().unwrap().as_slice(), &[30]);
    }

    #[test]
    fn test_distinct() {
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);

        from_iter::<i32, ()>(vec![1, 2, 1, 3, 2, 4, 1])
            .distinct()
            .subscribe(FnObserver::new(
                move |v: Result<i32, ()>| {
                    if let Ok(val) = v {
                        values_clone.lock().unwrap().push(val);
                    }
                },
                || {},
            ));

        assert_eq!(values.lock().unwrap().as_slice(), &[1, 2, 3, 4]);
    }

    #[test]
    fn test_ignore_elements() {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);
        let completed = Arc::new(AtomicUsize::new(0));
        let completed_clone = Arc::clone(&completed);

        from_iter::<i32, ()>(vec![1, 2, 3])
            .ignore_elements()
            .subscribe(FnObserver::new(
                move |_v: Result<i32, ()>| {
                    counter_clone.fetch_add(1, Ordering::SeqCst);
                },
                move || {
                    completed_clone.fetch_add(1, Ordering::SeqCst);
                },
            ));

        assert_eq!(counter.load(Ordering::SeqCst), 0);
        assert_eq!(completed.load(Ordering::SeqCst), 1);
    }

    // ===== 数学操作符：reduce, count, sum, min, max, average =====
    #[test]
    fn test_reduce() {
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);

        from_iter::<i32, ()>(vec![1, 2, 3, 4])
            .reduce(0, |acc, x| acc + x)
            .subscribe(FnObserver::new(
                move |v: Result<i32, ()>| {
                    if let Ok(val) = v {
                        values_clone.lock().unwrap().push(val);
                    }
                },
                || {},
            ));

        assert_eq!(values.lock().unwrap().as_slice(), &[10]);
    }

    #[test]
    fn test_count() {
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);

        from_iter::<i32, ()>(vec![10, 20, 30])
            .count()
            .subscribe(FnObserver::new(
                move |v: Result<usize, ()>| {
                    if let Ok(val) = v {
                        values_clone.lock().unwrap().push(val);
                    }
                },
                || {},
            ));

        assert_eq!(values.lock().unwrap().as_slice(), &[3]);
    }

    #[test]
    fn test_sum() {
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);

        from_iter::<i32, ()>(vec![1, 2, 3, 4])
            .sum()
            .subscribe(FnObserver::new(
                move |v: Result<i32, ()>| {
                    if let Ok(val) = v {
                        values_clone.lock().unwrap().push(val);
                    }
                },
                || {},
            ));

        assert_eq!(values.lock().unwrap().as_slice(), &[10]);
    }

    #[test]
    fn test_min() {
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);

        from_iter::<i32, ()>(vec![3, 1, 4, 1, 5, 9, 2])
            .min()
            .subscribe(FnObserver::new(
                move |v: Result<i32, ()>| {
                    if let Ok(val) = v {
                        values_clone.lock().unwrap().push(val);
                    }
                },
                || {},
            ));

        assert_eq!(values.lock().unwrap().as_slice(), &[1]);
    }

    #[test]
    fn test_max() {
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);

        from_iter::<i32, ()>(vec![3, 1, 4, 1, 5, 9, 2])
            .max()
            .subscribe(FnObserver::new(
                move |v: Result<i32, ()>| {
                    if let Ok(val) = v {
                        values_clone.lock().unwrap().push(val);
                    }
                },
                || {},
            ));

        assert_eq!(values.lock().unwrap().as_slice(), &[9]);
    }

    #[test]
    fn test_average() {
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);

        from_iter::<i32, ()>(vec![10, 20, 30])
            .average()
            .subscribe(FnObserver::new(
                move |v: Result<i32, ()>| {
                    if let Ok(val) = v {
                        values_clone.lock().unwrap().push(val);
                    }
                },
                || {},
            ));

        assert_eq!(values.lock().unwrap().as_slice(), &[20]);
    }

    // ===== 时间相关：distinct_until_changed, timeout =====
    #[test]
    fn test_distinct_until_changed() {
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);

        from_iter::<i32, ()>(vec![1, 1, 2, 2, 3, 1, 1])
            .distinct_until_changed()
            .subscribe(FnObserver::new(
                move |v: Result<i32, ()>| {
                    if let Ok(val) = v {
                        values_clone.lock().unwrap().push(val);
                    }
                },
                || {},
            ));

        assert_eq!(values.lock().unwrap().as_slice(), &[1, 2, 3, 1]);
    }

    #[test]
    fn test_timeout_basic() {
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);

        from_iter::<i32, String>(vec![1, 2, 3])
            .timeout(std::time::Duration::from_secs(1))
            .subscribe(FnObserver::new(
                move |v: Result<i32, String>| {
                    if let Ok(val) = v {
                        values_clone.lock().unwrap().push(val);
                    }
                },
                || {},
            ));

        assert_eq!(values.lock().unwrap().as_slice(), &[1, 2, 3]);
    }

    // ===== 错误处理：catch_error, on_error_resume_next =====
    #[test]
    fn test_catch_error() {
        use rx_rust::observable::ObservableFn;
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);

        let source = ObservableFn::<i32, String>::new(|observer| {
            observer.on_next(Ok(1));
            observer.on_next(Ok(2));
            observer.on_next(Err("something bad".into()));
            Subscription::empty()
        });

        source.catch_error(|_e| {
            Box::new(|obs: Box<dyn Observer<i32, String> + Send + Sync>| {
                obs.on_next(Ok(99));
                obs.on_completed();
                Subscription::empty()
            })
        }).subscribe(FnObserver::new(
            move |v: Result<i32, String>| {
                if let Ok(val) = v {
                    values_clone.lock().unwrap().push(val);
                }
            },
            || {},
        ));

        assert_eq!(values.lock().unwrap().as_slice(), &[1, 2, 99]);
    }

    #[test]
    fn test_on_error_resume_next() {
        use rx_rust::observable::ObservableFn;
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);

        let source = ObservableFn::<i32, String>::new(|observer| {
            observer.on_next(Ok(1));
            observer.on_next(Ok(2));
            observer.on_next(Err("boom".into()));
            Subscription::empty()
        });

        source.on_error_resume_next(from_iter::<i32, String>(vec![10, 20]))
            .subscribe(FnObserver::new(
                move |v: Result<i32, String>| {
                    if let Ok(val) = v {
                        values_clone.lock().unwrap().push(val);
                    }
                },
                || {},
            ));

        assert_eq!(values.lock().unwrap().as_slice(), &[1, 2, 10, 20]);
    }

    // ===== 新操作符测试 =====

    #[test]
    fn test_default_if_empty_with_values() {
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);

        from_iter::<i32, ()>(vec![1, 2, 3])
            .default_if_empty(42)
            .subscribe(FnObserver::new(
                move |v: Result<i32, ()>| {
                    if let Ok(val) = v {
                        values_clone.lock().unwrap().push(val);
                    }
                },
                || {},
            ));

        assert_eq!(values.lock().unwrap().as_slice(), &[1, 2, 3]);
    }

    #[test]
    fn test_default_if_empty_no_values() {
        use rx_rust::observable::ObservableFn;
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);

        ObservableFn::<i32, ()>::new(|observer| {
            observer.on_completed();
            Subscription::empty()
        }).default_if_empty(42).subscribe(FnObserver::new(
            move |v: Result<i32, ()>| {
                if let Ok(val) = v {
                    values_clone.lock().unwrap().push(val);
                }
            },
            || {},
        ));

        assert_eq!(values.lock().unwrap().as_slice(), &[42]);
    }

    #[test]
    fn test_contains_found() {
        let result = Arc::new(Mutex::new(false));
        let result_clone = Arc::clone(&result);

        from_iter::<i32, ()>(vec![1, 2, 3, 4, 5])
            .contains(3)
            .subscribe(FnObserver::new(
                move |v: Result<bool, ()>| {
                    if let Ok(val) = v {
                        *result_clone.lock().unwrap() = val;
                    }
                },
                || {},
            ));

        assert_eq!(*result.lock().unwrap(), true);
    }

    #[test]
    fn test_contains_not_found() {
        let result = Arc::new(Mutex::new(true));
        let result_clone = Arc::clone(&result);

        from_iter::<i32, ()>(vec![1, 2, 3])
            .contains(99)
            .subscribe(FnObserver::new(
                move |v: Result<bool, ()>| {
                    if let Ok(val) = v {
                        *result_clone.lock().unwrap() = val;
                    }
                },
                || {},
            ));

        assert_eq!(*result.lock().unwrap(), false);
    }

    #[test]
    fn test_all_true() {
        let result = Arc::new(Mutex::new(false));
        let result_clone = Arc::clone(&result);

        from_iter::<i32, ()>(vec![2, 4, 6, 8])
            .all(|x| *x % 2 == 0)
            .subscribe(FnObserver::new(
                move |v: Result<bool, ()>| {
                    if let Ok(val) = v {
                        *result_clone.lock().unwrap() = val;
                    }
                },
                || {},
            ));

        assert_eq!(*result.lock().unwrap(), true);
    }

    #[test]
    fn test_all_false() {
        let result = Arc::new(Mutex::new(true));
        let result_clone = Arc::clone(&result);

        from_iter::<i32, ()>(vec![2, 4, 5, 8])
            .all(|x| *x % 2 == 0)
            .subscribe(FnObserver::new(
                move |v: Result<bool, ()>| {
                    if let Ok(val) = v {
                        *result_clone.lock().unwrap() = val;
                    }
                },
                || {},
            ));

        assert_eq!(*result.lock().unwrap(), false);
    }

    // ===== Subject 取消机制测试 =====

    #[test]
    fn test_subject_unsubscribe() {
        let subject = PublishSubject::<i32, ()>::new();
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);

        let sub = subject.subscribe_ref(FnObserver::new(
            move |v: Result<i32, ()>| {
                if let Ok(val) = v {
                    values_clone.lock().unwrap().push(val);
                }
            },
            || {},
        ));

        subject.on_next(Ok(1));
        subject.on_next(Ok(2));
        sub.dispose();
        subject.on_next(Ok(3));
        subject.on_next(Ok(4));

        assert_eq!(values.lock().unwrap().as_slice(), &[1, 2]);
    }

    // ===== switch_map 测试 =====
    #[test]
    fn test_switch_map_basic() {
        use rx_rust::observable::ObservableFn;
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);

        from_iter::<i32, ()>(vec![1, 2])
            .switch_map(|x| {
                ObservableFn::<i32, ()>::new(move |observer| {
                    observer.on_next(Ok(x * 10));
                    observer.on_next(Ok(x * 10 + 1));
                    observer.on_completed();
                    Subscription::empty()
                })
            })
            .subscribe(FnObserver::new(
                move |v: Result<i32, ()>| {
                    if let Ok(val) = v {
                        values_clone.lock().unwrap().push(val);
                    }
                },
                || {},
            ));

        assert_eq!(values.lock().unwrap().as_slice(), &[10, 11, 20, 21]);
    }

    // ===== ConnectableObservable / publish 测试 =====
    #[test]
    fn test_publish_connect() {
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);

        let conn = from_iter::<i32, ()>(vec![5, 6, 7]).publish();
        let sub = conn.subscribe_ref(FnObserver::new(
            move |v: Result<i32, ()>| {
                if let Ok(val) = v {
                    values_clone.lock().unwrap().push(val);
                }
            },
            || {},
        ));
        conn.connect();

        assert_eq!(values.lock().unwrap().as_slice(), &[5, 6, 7]);
    }

    // ===== retry 测试 =====
    #[test]
    fn test_retry_success_after_failure() {
        use rx_rust::observable::ObservableFn;
        let call_count = Arc::new(Mutex::new(0));
        let call_count_clone = Arc::clone(&call_count);
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);

        let source = ObservableFn::<i32, String>::new(move |observer| {
            // 先读取 count 并释放锁，避免死锁
            let should_fail = {
                let mut count = call_count_clone.lock().unwrap();
                *count += 1;
                *count <= 1
            };
            if should_fail {
                observer.on_next(Ok(1));
                observer.on_next(Err("failed".into()));
            } else {
                observer.on_next(Ok(2));
                observer.on_next(Ok(3));
                observer.on_completed();
            }
            Subscription::empty()
        });

        source.clone().retry(3).subscribe(FnObserver::new(
            move |v: Result<i32, String>| {
                if let Ok(val) = v {
                    values_clone.lock().unwrap().push(val);
                }
            },
            || {},
        ));

        // 第一次调用：[1, Err] -> 重试
        // 第二次调用：[2, 3, completed]
        // 结果：[1, 2, 3]
        assert_eq!(values.lock().unwrap().as_slice(), &[1, 2, 3]);
    }

    #[test]
    fn test_retry_exhausted() {
        use rx_rust::observable::ObservableFn;
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);
        let errors = Arc::new(Mutex::new(Vec::new()));
        let errors_clone = Arc::clone(&errors);

        let source = ObservableFn::<i32, String>::new(move |observer| {
            observer.on_next(Ok(1));
            observer.on_next(Err("always fail".into()));
            Subscription::empty()
        });

        source.clone().retry(2).subscribe(FnObserver::new(
            move |v: Result<i32, String>| {
                match v {
                    Ok(val) => values_clone.lock().unwrap().push(val),
                    Err(e) => errors_clone.lock().unwrap().push(e),
                }
            },
            || {},
        ));

        // 重试 2 次后仍然失败，最终应该收到 3 次 1（初始 + 2 次重试 = 3 次调用）
        // 和 1 次 "always fail" 错误
        assert_eq!(values.lock().unwrap().as_slice(), &[1, 1, 1]);
        assert_eq!(errors.lock().unwrap().as_slice(), &["always fail".to_string()]);
    }

    // ===== debounce 测试 =====
    #[test]
    fn test_debounce_basic() {
        use rx_rust::observable::ObservableFn;
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);

        // 简单测试：debounce 能编译并接收值
        let source = ObservableFn::<i32, ()>::new(|observer| {
            observer.on_next(Ok(1));
            observer.on_next(Ok(2));
            observer.on_next(Ok(3));
            observer.on_completed();
            Subscription::empty()
        });

        source.debounce(std::time::Duration::from_millis(10))
            .subscribe(FnObserver::new(
                move |v: Result<i32, ()>| {
                    if let Ok(val) = v {
                        values_clone.lock().unwrap().push(val);
                    }
                },
                || {},
            ));

        // debounce 是异步的（有 thread spawn），给它一点时间
        std::thread::sleep(std::time::Duration::from_millis(100));
        let v = values.lock().unwrap();
        assert!(v.len() > 0, "debounce should emit at least one value");
    }

    // ===== throttle 测试 =====
    #[test]
    fn test_throttle_basic() {
        use rx_rust::observable::ObservableFn;
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);

        let source = ObservableFn::<i32, ()>::new(|observer| {
            observer.on_next(Ok(1));
            observer.on_next(Ok(2));
            observer.on_next(Ok(3));
            observer.on_completed();
            Subscription::empty()
        });

        source.throttle(std::time::Duration::from_millis(100))
            .subscribe(FnObserver::new(
                move |v: Result<i32, ()>| {
                    if let Ok(val) = v {
                        values_clone.lock().unwrap().push(val);
                    }
                },
                || {},
            ));

        // throttle 是同步的（通过时间窗口判断），所以所有值应该被过滤除了第一个
        assert_eq!(values.lock().unwrap().as_slice(), &[1]);
    }

    // ===== observe_on 测试 =====
    #[test]
    fn test_observe_on_basic() {
        use rx_rust::scheduler::CurrentThreadScheduler;
        use rx_rust::observable::ObservableFn;
        let values = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);

        let source = ObservableFn::<i32, ()>::new(|observer| {
            observer.on_next(Ok(1));
            observer.on_next(Ok(2));
            observer.on_next(Ok(3));
            observer.on_completed();
            Subscription::empty()
        });

        source.observe_on(CurrentThreadScheduler)
            .subscribe(FnObserver::new(
                move |v: Result<i32, ()>| {
                    if let Ok(val) = v {
                        values_clone.lock().unwrap().push(val);
                    }
                },
                || {},
            ));

        assert_eq!(values.lock().unwrap().as_slice(), &[1, 2, 3]);
    }

    // ===== 新增测试：完整覆盖 range / from_iter / map / filter / take / skip / reduce / subject_broadcast =====

    fn collect_obs<T: Clone + Send + Sync + 'static>(
        obs: impl Observable<T, ()> + 'static,
    ) -> Vec<T> {
        let values: Arc<Mutex<Vec<T>>> = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);
        obs.subscribe(FnObserver::new(
            move |v: Result<T, ()>| {
                if let Ok(val) = v {
                    values_clone.lock().unwrap().push(val);
                }
            },
            || {},
        ));
        let snapshot = values.lock().unwrap().clone();
        snapshot
    }

    #[test]
    fn test_range_emits_correct_count() {
        let v = collect_obs(range::<i32, ()>(5, 5));
        assert_eq!(v.len(), 5);
        assert_eq!(v[0], 5);
        assert_eq!(v[4], 9);
        assert_eq!(v.as_slice(), &[5, 6, 7, 8, 9]);
    }

    #[test]
    fn test_from_iter_order_preserved() {
        let v = collect_obs(from_iter::<i32, ()>(vec![10, 20, 30, 40]));
        assert_eq!(v, vec![10, 20, 30, 40]);
    }

    #[test]
    fn test_map_transform_values() {
        let v = collect_obs(from_iter::<i32, ()>(vec![1, 2, 3]).map(|x| x * 100));
        assert_eq!(v, vec![100, 200, 300]);
    }

    #[test]
    fn test_filter_by_predicate() {
        let v = collect_obs(
            from_iter::<i32, ()>(vec![1, 2, 3, 4, 5, 6]).filter(|x| x % 2 == 0),
        );
        assert_eq!(v, vec![2, 4, 6]);
    }

    #[test]
    fn test_take_halts_early() {
        let v = collect_obs(from_iter::<i32, ()>(vec![1, 2, 3, 4, 5]).take(3));
        assert_eq!(v, vec![1, 2, 3]);
    }

    #[test]
    fn test_skip_skips_prefix() {
        let v = collect_obs(from_iter::<i32, ()>(vec![1, 2, 3, 4, 5]).skip(2));
        assert_eq!(v, vec![3, 4, 5]);
    }

    #[test]
    fn test_reduce_sum_emits_final() {
        let v = collect_obs(from_iter::<i32, ()>(vec![1, 2, 3, 4]).reduce(0, |acc, x| acc + x));
        assert_eq!(v, vec![10]);
    }

    #[test]
    fn test_publish_subject_broadcasts_to_all() {
        let subject = PublishSubject::<i32, ()>::new();
        let v1: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));
        let v2: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));
        let v1c = Arc::clone(&v1);
        let v2c = Arc::clone(&v2);

        let _sub1 = subject.subscribe_ref(FnObserver::new(
            move |v: Result<i32, ()>| {
                if let Ok(val) = v {
                    v1c.lock().unwrap().push(val);
                }
            },
            || {},
        ));
        let _sub2 = subject.subscribe_ref(FnObserver::new(
            move |v: Result<i32, ()>| {
                if let Ok(val) = v {
                    v2c.lock().unwrap().push(val);
                }
            },
            || {},
        ));

        subject.on_next(Ok(1));
        subject.on_next(Ok(2));
        subject.on_next(Ok(3));

        assert_eq!(*v1.lock().unwrap(), vec![1, 2, 3]);
        assert_eq!(*v2.lock().unwrap(), vec![1, 2, 3]);
    }
}
