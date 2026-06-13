use crate::{
    observable::{Observable, ObservableFn},
    subscription::Subscription,
};

pub fn range<T, E>(start: T, count: usize) -> ObservableFn<T, E>
where
    T: Copy + std::ops::Add<Output = T> + From<u8> + Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    ObservableFn::new(move |observer| {
        for i in 0..count {
            let value = start + T::from(i as u8);
            observer.on_next(Ok(value));
        }
        observer.on_completed();
        Subscription::empty()
    })
}

pub fn repeat<T, E>(value: T, count: usize) -> ObservableFn<T, E>
where
    T: Clone + Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    ObservableFn::new(move |observer| {
        for _ in 0..count {
            observer.on_next(Ok(value.clone()));
        }
        observer.on_completed();
        Subscription::empty()
    })
}

pub fn defer<Obs, F, T, E>(factory: F) -> ObservableFn<T, E>
where
    F: Fn() -> Obs + Send + Sync + 'static,
    Obs: Observable<T, E>,
    T: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    ObservableFn::new(move |observer| {
        let obs = factory();
        obs.subscribe(observer)
    })
}

pub fn generate<Init, F, T, E>(initial: Init, f: F) -> ObservableFn<T, E>
where
    F: Fn(Init) -> (T, Init, bool) + Send + Sync + 'static,
    Init: Clone + Send + Sync + 'static,
    T: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    ObservableFn::new(move |observer| {
        let mut state = initial.clone();
        loop {
            let (value, new_state, cont) = f(state);
            if !cont {
                break;
            }
            observer.on_next(Ok(value));
            state = new_state;
        }
        observer.on_completed();
        Subscription::empty()
    })
}

pub fn of<T, E>(value: T) -> ObservableFn<T, E>
where
    T: Clone + Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    ObservableFn::new(move |observer| {
        observer.on_next(Ok(value.clone()));
        observer.on_completed();
        Subscription::empty()
    })
}

pub fn from_iter<T, E>(iter: Vec<T>) -> ObservableFn<T, E>
where
    T: Clone + Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    ObservableFn::new(move |observer| {
        for item in iter.clone() {
            observer.on_next(Ok(item));
        }
        observer.on_completed();
        Subscription::empty()
    })
}

pub fn empty<T, E>() -> ObservableFn<T, E>
where
    T: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    ObservableFn::new(move |observer| {
        observer.on_completed();
        Subscription::empty()
    })
}

pub fn never<T, E>() -> ObservableFn<T, E>
where
    T: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    ObservableFn::new(move |_observer| {
        Subscription::empty()
    })
}

pub fn throw<T, E>(error: E) -> ObservableFn<T, E>
where
    T: Send + Sync + 'static,
    E: Clone + Send + Sync + 'static,
{
    ObservableFn::new(move |observer| {
        observer.on_next(Err(error.clone()));
        observer.on_completed();
        Subscription::empty()
    })
}