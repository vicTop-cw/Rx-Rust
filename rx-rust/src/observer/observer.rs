use std::sync::Arc;

pub trait Observer<T, E> {
    fn on_next(&self, value: Result<T, E>);
    fn on_completed(&self);
}

pub struct ArcObserver<T, E>(Arc<dyn Observer<T, E> + Send + Sync>);

impl<T, E> ArcObserver<T, E> {
    pub fn new<O>(observer: O) -> Self
    where
        O: Observer<T, E> + Send + Sync + 'static,
    {
        Self(Arc::new(observer))
    }
}

impl<T, E> Observer<T, E> for ArcObserver<T, E> {
    fn on_next(&self, value: Result<T, E>) {
        self.0.on_next(value)
    }

    fn on_completed(&self) {
        self.0.on_completed()
    }
}

impl<T, E, O: Observer<T, E> + ?Sized> Observer<T, E> for Box<O> {
    fn on_next(&self, value: Result<T, E>) {
        (**self).on_next(value)
    }

    fn on_completed(&self) {
        (**self).on_completed()
    }
}

pub struct FnObserver<T, E, N, C> {
    on_next: N,
    on_completed: C,
    _marker: std::marker::PhantomData<(T, E)>,
}

impl<T, E, N, C> FnObserver<T, E, N, C> {
    pub fn new(on_next: N, on_completed: C) -> Self {
        Self {
            on_next,
            on_completed,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T, E, N, C> Observer<T, E> for FnObserver<T, E, N, C>
where
    N: Fn(Result<T, E>) + Send + Sync + 'static,
    C: Fn() + Send + Sync + 'static,
{
    fn on_next(&self, value: Result<T, E>) {
        (self.on_next)(value)
    }

    fn on_completed(&self) {
        (self.on_completed)()
    }
}