use std::sync::{Arc, Mutex, atomic::{AtomicBool, Ordering}};

pub trait Disposable {
    fn dispose(&self);
    fn is_disposed(&self) -> bool;
}

pub struct Subscription(Arc<dyn Disposable + Send + Sync>);

impl Subscription {
    pub fn new<D: Disposable + Send + Sync + 'static>(disposable: D) -> Self {
        Self(Arc::new(disposable))
    }

    pub fn dispose(&self) {
        self.0.dispose()
    }

    pub fn is_disposed(&self) -> bool {
        self.0.is_disposed()
    }

    pub fn empty() -> Self {
        Self(Arc::new(EmptyDisposable))
    }

    pub fn from_fn<F: FnOnce() + Send + Sync + 'static>(f: F) -> Self {
        Self(Arc::new(FnDisposable::new(f)))
    }
}

impl Clone for Subscription {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

struct EmptyDisposable;

impl Disposable for EmptyDisposable {
    fn dispose(&self) {}
    fn is_disposed(&self) -> bool { false }
}

struct FnDisposable<F> {
    f: Mutex<Option<F>>,
    disposed: AtomicBool,
}

impl<F: FnOnce() + Send + Sync + 'static> FnDisposable<F> {
    fn new(f: F) -> Self {
        Self {
            f: Mutex::new(Some(f)),
            disposed: AtomicBool::new(false),
        }
    }
}

impl<F: FnOnce() + Send + Sync + 'static> Disposable for FnDisposable<F> {
    fn dispose(&self) {
        if self.disposed.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Some(f) = self.f.lock().unwrap().take() {
            f();
        }
    }

    fn is_disposed(&self) -> bool {
        self.disposed.load(Ordering::Acquire)
    }
}

pub struct CompositeDisposable {
    disposables: Mutex<Vec<Subscription>>,
    disposed: AtomicBool,
}

impl CompositeDisposable {
    pub fn new() -> Self {
        Self {
            disposables: Mutex::new(Vec::new()),
            disposed: AtomicBool::new(false),
        }
    }

    pub fn add(&self, subscription: Subscription) {
        if self.disposed.load(Ordering::Acquire) {
            subscription.dispose();
            return;
        }
        self.disposables.lock().unwrap().push(subscription);
    }
}

impl Disposable for CompositeDisposable {
    fn dispose(&self) {
        if self.disposed.swap(true, Ordering::AcqRel) {
            return;
        }
        for d in self.disposables.lock().unwrap().drain(..) {
            d.dispose();
        }
    }

    fn is_disposed(&self) -> bool {
        self.disposed.load(Ordering::Acquire)
    }
}