#[cfg(test)]
mod tests {
    use rx_rust::subscription::{Subscription, CompositeDisposable, Disposable};
    use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};

    #[test]
    fn test_subscription_empty() {
        let sub = Subscription::empty();
        assert!(!sub.is_disposed());
        sub.dispose();
        assert!(!sub.is_disposed());
    }

    #[test]
    fn test_subscription_from_fn() {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);
        
        let sub = Subscription::from_fn(move || {
            counter_clone.fetch_add(1, Ordering::SeqCst);
        });
        
        assert!(!sub.is_disposed());
        assert_eq!(counter.load(Ordering::SeqCst), 0);
        
        sub.dispose();
        assert!(sub.is_disposed());
        assert_eq!(counter.load(Ordering::SeqCst), 1);
        
        sub.dispose();
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_composite_disposable() {
        let counter1 = Arc::new(AtomicUsize::new(0));
        let counter2 = Arc::new(AtomicUsize::new(0));
        let counter1_clone = Arc::clone(&counter1);
        let counter2_clone = Arc::clone(&counter2);
        
        let composite = CompositeDisposable::new();
        composite.add(Subscription::from_fn(move || {
            counter1_clone.fetch_add(1, Ordering::SeqCst);
        }));
        composite.add(Subscription::from_fn(move || {
            counter2_clone.fetch_add(1, Ordering::SeqCst);
        }));
        
        assert!(!composite.is_disposed());
        assert_eq!(counter1.load(Ordering::SeqCst), 0);
        assert_eq!(counter2.load(Ordering::SeqCst), 0);
        
        composite.dispose();
        assert!(composite.is_disposed());
        assert_eq!(counter1.load(Ordering::SeqCst), 1);
        assert_eq!(counter2.load(Ordering::SeqCst), 1);
        
        composite.dispose();
        assert_eq!(counter1.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_composite_disposable_add_after_dispose() {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);
        
        let composite = CompositeDisposable::new();
        composite.dispose();
        
        composite.add(Subscription::from_fn(move || {
            counter_clone.fetch_add(1, Ordering::SeqCst);
        }));
        
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }
}
