use std::sync::{Arc, Mutex};
use std::marker::PhantomData;

use crate::{Observable, Observer, subscription::Subscription};

type ObserverBox<T, E> = Arc<Box<dyn Observer<T, E> + Send + Sync + 'static>>;

pub struct PublishSubject<T, E> {
    observers: Arc<Mutex<Vec<ObserverBox<T, E>>>>,
    _marker: PhantomData<(T, E)>,
}

impl<T: Send + Sync + 'static, E: Send + Sync + 'static> PublishSubject<T, E> {
    pub fn new() -> Self {
        Self {
            observers: Arc::new(Mutex::new(Vec::new())),
            _marker: PhantomData,
        }
    }

    pub fn subscribe_ref(&self, observer: impl Observer<T, E> + Send + Sync + 'static) -> Subscription {
        let observer: ObserverBox<T, E> = Arc::new(Box::new(observer));
        let observer_for_cleanup = Arc::clone(&observer);
        let observers_arc = Arc::clone(&self.observers);

        self.observers.lock().unwrap().push(observer);

        Subscription::from_fn(move || {
            let mut obs = observers_arc.lock().unwrap();
            obs.retain(|o| !Arc::ptr_eq(o, &observer_for_cleanup));
        })
    }
}

impl<T, E> Observer<T, E> for PublishSubject<T, E>
where
    T: Send + Sync + Clone + 'static,
    E: Send + Sync + Clone + 'static,
{
    fn on_next(&self, value: Result<T, E>) {
        for observer in self.observers.lock().unwrap().iter() {
            observer.on_next(value.clone());
        }
    }

    fn on_completed(&self) {
        for observer in self.observers.lock().unwrap().iter() {
            observer.on_completed();
        }
    }
}

impl<T, E> Observable<T, E> for PublishSubject<T, E>
where
    T: Send + Sync + Clone + 'static,
    E: Send + Sync + Clone + 'static,
{
    fn subscribe(self, observer: impl Observer<T, E> + Send + Sync + 'static) -> Subscription {
        let observer: ObserverBox<T, E> = Arc::new(Box::new(observer));
        let observer_for_cleanup = Arc::clone(&observer);
        let observers_arc = Arc::clone(&self.observers);

        self.observers.lock().unwrap().push(observer);

        Subscription::from_fn(move || {
            let mut obs = observers_arc.lock().unwrap();
            obs.retain(|o| !Arc::ptr_eq(o, &observer_for_cleanup));
        })
    }
}

pub struct BehaviorSubject<T, E> {
    value: Arc<Mutex<Option<T>>>,
    observers: Arc<Mutex<Vec<ObserverBox<T, E>>>>,
}

impl<T: Send + Sync + 'static, E: Send + Sync + 'static> BehaviorSubject<T, E> {
    pub fn new(initial_value: T) -> Self {
        Self {
            value: Arc::new(Mutex::new(Some(initial_value))),
            observers: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn subscribe_ref(&self, observer: impl Observer<T, E> + Send + Sync + 'static) -> Subscription
    where
        T: Clone,
    {
        let observer: ObserverBox<T, E> = Arc::new(Box::new(observer));

        if let Some(value) = self.value.lock().unwrap().as_ref().cloned() {
            observer.on_next(Ok(value));
        }

        let observer_for_cleanup = Arc::clone(&observer);
        let observers_arc = Arc::clone(&self.observers);

        self.observers.lock().unwrap().push(observer);

        Subscription::from_fn(move || {
            let mut obs = observers_arc.lock().unwrap();
            obs.retain(|o| !Arc::ptr_eq(o, &observer_for_cleanup));
        })
    }
}

impl<T, E> Observer<T, E> for BehaviorSubject<T, E>
where
    T: Send + Sync + Clone + 'static,
    E: Send + Sync + Clone + 'static,
{
    fn on_next(&self, value: Result<T, E>) {
        if let Ok(t) = &value {
            *self.value.lock().unwrap() = Some(t.clone());
        }
        for observer in self.observers.lock().unwrap().iter() {
            observer.on_next(value.clone());
        }
    }

    fn on_completed(&self) {
        for observer in self.observers.lock().unwrap().iter() {
            observer.on_completed();
        }
    }
}

impl<T, E> Observable<T, E> for BehaviorSubject<T, E>
where
    T: Send + Sync + Clone + 'static,
    E: Send + Sync + Clone + 'static,
{
    fn subscribe(self, observer: impl Observer<T, E> + Send + Sync + 'static) -> Subscription {
        let observer: ObserverBox<T, E> = Arc::new(Box::new(observer));

        if let Some(value) = self.value.lock().unwrap().as_ref().cloned() {
            observer.on_next(Ok(value));
        }

        let observer_for_cleanup = Arc::clone(&observer);
        let observers_arc = Arc::clone(&self.observers);

        self.observers.lock().unwrap().push(observer);

        Subscription::from_fn(move || {
            let mut obs = observers_arc.lock().unwrap();
            obs.retain(|o| !Arc::ptr_eq(o, &observer_for_cleanup));
        })
    }
}

pub struct ReplaySubject<T, E> {
    buffer: Arc<Mutex<Vec<T>>>,
    capacity: usize,
    observers: Arc<Mutex<Vec<ObserverBox<T, E>>>>,
}

impl<T: Send + Sync + 'static, E: Send + Sync + 'static> ReplaySubject<T, E> {
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: Arc::new(Mutex::new(Vec::new())),
            capacity,
            observers: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn subscribe_ref(&self, observer: impl Observer<T, E> + Send + Sync + 'static) -> Subscription
    where
        T: Clone,
    {
        let observer: ObserverBox<T, E> = Arc::new(Box::new(observer));

        for value in self.buffer.lock().unwrap().iter().cloned() {
            observer.on_next(Ok(value));
        }

        let observer_for_cleanup = Arc::clone(&observer);
        let observers_arc = Arc::clone(&self.observers);

        self.observers.lock().unwrap().push(observer);

        Subscription::from_fn(move || {
            let mut obs = observers_arc.lock().unwrap();
            obs.retain(|o| !Arc::ptr_eq(o, &observer_for_cleanup));
        })
    }
}

impl<T, E> Observer<T, E> for ReplaySubject<T, E>
where
    T: Send + Sync + Clone + 'static,
    E: Send + Sync + Clone + 'static,
{
    fn on_next(&self, value: Result<T, E>) {
        if let Ok(t) = &value {
            let mut buffer = self.buffer.lock().unwrap();
            buffer.push(t.clone());
            if buffer.len() > self.capacity {
                buffer.remove(0);
            }
        }
        for observer in self.observers.lock().unwrap().iter() {
            observer.on_next(value.clone());
        }
    }

    fn on_completed(&self) {
        for observer in self.observers.lock().unwrap().iter() {
            observer.on_completed();
        }
    }
}

impl<T, E> Observable<T, E> for ReplaySubject<T, E>
where
    T: Send + Sync + Clone + 'static,
    E: Send + Sync + Clone + 'static,
{
    fn subscribe(self, observer: impl Observer<T, E> + Send + Sync + 'static) -> Subscription {
        let observer: ObserverBox<T, E> = Arc::new(Box::new(observer));

        for value in self.buffer.lock().unwrap().iter().cloned() {
            observer.on_next(Ok(value));
        }

        let observer_for_cleanup = Arc::clone(&observer);
        let observers_arc = Arc::clone(&self.observers);

        self.observers.lock().unwrap().push(observer);

        Subscription::from_fn(move || {
            let mut obs = observers_arc.lock().unwrap();
            obs.retain(|o| !Arc::ptr_eq(o, &observer_for_cleanup));
        })
    }
}

// ===================== ConnectableObservable =====================
// 把冷 Observable 变成热的：只有在 connect() 被调用后才真正订阅源，
// 并将源的值广播给所有下游观察者。

pub struct ConnectableObservable<Src, T, E>
where
    Src: Observable<T, E>,
{
    source: Option<Src>,
    subject: Arc<PublishSubject<T, E>>,
    connected: Arc<Mutex<bool>>,
}

impl<Src, T, E> ConnectableObservable<Src, T, E>
where
    Src: Observable<T, E> + Send + Sync + 'static,
    T: Send + Sync + Clone + 'static,
    E: Send + Sync + Clone + 'static,
{
    pub fn new(source: Src) -> Self {
        Self {
            source: Some(source),
            subject: Arc::new(PublishSubject::new()),
            connected: Arc::new(Mutex::new(false)),
        }
    }

    pub fn subscribe_ref(&self, observer: impl Observer<T, E> + Send + Sync + 'static) -> Subscription {
        self.subject.subscribe_ref(observer)
    }

    pub fn connect(mut self) -> Subscription
    where
        Src: Send + Sync + 'static,
    {
        let mut connected = self.connected.lock().unwrap();
        if *connected {
            return Subscription::empty();
        }
        *connected = true;
        drop(connected);

        if let Some(source) = self.source.take() {
            let subject = Arc::clone(&self.subject);
            source.subscribe(ConnectableObserver { subject })
        } else {
            Subscription::empty()
        }
    }
}

struct ConnectableObserver<T, E>
where
    T: Send + Sync + Clone + 'static,
    E: Send + Sync + Clone + 'static,
{
    subject: Arc<PublishSubject<T, E>>,
}

impl<T, E> Observer<T, E> for ConnectableObserver<T, E>
where
    T: Send + Sync + Clone + 'static,
    E: Send + Sync + Clone + 'static,
{
    fn on_next(&self, value: Result<T, E>) {
        self.subject.on_next(value);
    }
    fn on_completed(&self) {
        self.subject.on_completed();
    }
}

impl<Src, T, E> Observable<T, E> for ConnectableObservable<Src, T, E>
where
    Src: Observable<T, E> + Send + Sync + 'static,
    T: Send + Sync + Clone + 'static,
    E: Send + Sync + Clone + 'static,
{
    fn subscribe(self, observer: impl Observer<T, E> + Send + Sync + 'static) -> Subscription {
        self.subject.subscribe_ref(observer)
    }
}
