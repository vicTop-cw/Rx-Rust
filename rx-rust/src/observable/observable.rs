use std::sync::Arc;
use crate::{observer::Observer, subscription::Subscription};

pub trait Observable<T, E> {
    fn subscribe(self, observer: impl Observer<T, E> + Send + Sync + 'static) -> Subscription;
}

pub struct ObservableFn<T, E>(Arc<dyn Fn(Box<dyn Observer<T, E> + Send + Sync>) -> Subscription + Send + Sync>);

impl<T, E> ObservableFn<T, E> {
    pub fn new<F>(f: F) -> Self
    where
        F: Fn(Box<dyn Observer<T, E> + Send + Sync>) -> Subscription + Send + Sync + 'static,
        T: Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        Self(Arc::new(f))
    }
}

impl<T, E> Clone for ObservableFn<T, E> {
    fn clone(&self) -> Self {
        ObservableFn(Arc::clone(&self.0))
    }
}

impl<T, E> Observable<T, E> for ObservableFn<T, E>
where
    T: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    fn subscribe(self, observer: impl Observer<T, E> + Send + Sync + 'static) -> Subscription {
        (self.0)(Box::new(observer))
    }
}