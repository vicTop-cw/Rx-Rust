use crate::{Observable, Observer, subscription::{Subscription, CompositeDisposable}};
use crate::prelude::Disposable;
use std::sync::{Arc, Mutex};

pub trait ObservableExt<T, E>: Observable<T, E> + Sized {
    fn map<U, F>(self, f: F) -> MapObservable<Self, F, T, U, E>
    where
        F: Fn(T) -> U + Send + Sync + 'static,
        T: Send + Sync + 'static,
        U: Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        MapObservable::new(self, f)
    }

    fn filter<P>(self, predicate: P) -> FilterObservable<Self, P, T, E>
    where
        P: Fn(&T) -> bool + Send + Sync + 'static,
        T: Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        FilterObservable::new(self, predicate)
    }

    fn collect(self) -> CollectFuture<T, E>
    where
        T: Send + Sync + Clone + 'static,
        E: Send + Sync + 'static,
    {
        CollectFuture::new(self)
    }

    fn take(self, count: usize) -> TakeObservable<Self, T, E>
    where
        T: Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        TakeObservable::new(self, count)
    }

    fn skip(self, count: usize) -> SkipObservable<Self, T, E>
    where
        T: Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        SkipObservable::new(self, count)
    }

    fn first(self) -> FirstObservable<Self, T, E>
    where
        T: Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        FirstObservable::new(self)
    }

    fn last(self) -> LastObservable<Self, T, E>
    where
        T: Send + Sync + Clone + 'static,
        E: Send + Sync + 'static,
    {
        LastObservable::new(self)
    }

    fn take_while<P>(self, predicate: P) -> TakeWhileObservable<Self, P, T, E>
    where
        P: Fn(&T) -> bool + Send + Sync + 'static,
        T: Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        TakeWhileObservable::new(self, predicate)
    }

    fn skip_while<P>(self, predicate: P) -> SkipWhileObservable<Self, P, T, E>
    where
        P: Fn(&T) -> bool + Send + Sync + 'static,
        T: Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        SkipWhileObservable::new(self, predicate)
    }

    fn flat_map<U, F, Inner>(self, f: F) -> FlatMapObservable<Self, F, Inner, T, U, E>
    where
        F: Fn(T) -> Inner + Send + Sync + 'static,
        Inner: Observable<U, E>,
        T: Send + Sync + 'static,
        U: Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        FlatMapObservable::new(self, f)
    }

    fn scan<Acc, F>(self, initial: Acc, f: F) -> ScanObservable<Self, F, Acc, T, E>
    where
        F: Fn(Acc, T) -> Acc + Send + Sync + 'static,
        Acc: Send + Sync + Clone + 'static,
        T: Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        ScanObservable::new(self, initial, f)
    }

    fn buffer(self, count: usize) -> BufferObservable<Self, T, E>
    where
        T: Send + Sync + Clone + 'static,
        E: Send + Sync + 'static,
    {
        BufferObservable::new(self, count)
    }

    fn merge<Other>(self, other: Other) -> MergeObservable<Self, Other, T, E>
    where
        Other: Observable<T, E>,
        T: Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        MergeObservable::new(self, other)
    }

    fn zip<Other, U, R, F>(self, other: Other, f: F) -> ZipObservable<Self, Other, F, T, U, R, E>
    where
        Other: Observable<U, E>,
        F: Fn(T, U) -> R + Send + Sync + 'static,
        T: Send + Sync + Clone + 'static,
        U: Send + Sync + Clone + 'static,
        R: Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        ZipObservable::new(self, other, f)
    }

    fn concat<Other>(self, other: Other) -> ConcatObservable<Self, Other, T, E>
    where
        Other: Observable<T, E>,
        T: Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        ConcatObservable::new(self, other)
    }

    fn combine_latest<Other, U, R, F>(self, other: Other, f: F) -> CombineLatestObservable<Self, Other, F, T, U, R, E>
    where
        Other: Observable<U, E>,
        F: Fn(T, U) -> R + Send + Sync + 'static,
        T: Send + Sync + Clone + 'static,
        U: Send + Sync + Clone + 'static,
        R: Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        CombineLatestObservable::new(self, other, f)
    }

    fn default_if_empty(self, default: T) -> DefaultIfEmptyObservable<Self, T, E>
    where
        T: Send + Sync + Clone + 'static,
        E: Send + Sync + 'static,
    {
        DefaultIfEmptyObservable::new(self, default)
    }

    fn switch_map<F, Obs, U>(self, f: F) -> SwitchMapObservable<Self, F, U, T, E>
    where
        F: Fn(T) -> Obs + Send + Sync + 'static,
        Obs: Observable<U, E> + Send + Sync + 'static,
        U: Send + Sync + 'static,
        T: Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        SwitchMapObservable::new(self, f)
    }

    fn publish(self) -> crate::subject::ConnectableObservable<Self, T, E>
    where
        T: Send + Sync + Clone + 'static,
        E: Send + Sync + Clone + 'static,
        Self: Send + Sync + 'static,
    {
        crate::subject::ConnectableObservable::new(self)
    }
}

impl<O, T, E> ObservableExt<T, E> for O where O: Observable<T, E> + Sized {}

pub struct MapObservable<Src, F, T, U, E> {
    source: Src,
    f: F,
    _marker: std::marker::PhantomData<(T, U, E)>,
}

impl<Src, F, T, U, E> MapObservable<Src, F, T, U, E> {
    pub fn new(source: Src, f: F) -> Self {
        Self {
            source,
            f,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<Src, F, T, U, E> Observable<U, E> for MapObservable<Src, F, T, U, E>
where
    Src: Observable<T, E>,
    F: Fn(T) -> U + Send + Sync + 'static,
    T: Send + Sync + 'static,
    U: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    fn subscribe(self, observer: impl Observer<U, E> + Send + Sync + 'static) -> Subscription {
        let observer = Box::new(observer);
        self.source.subscribe(MapObserver::<F, U, E> { observer, f: self.f })
    }
}

struct MapObserver<F, U, E> {
    observer: Box<dyn Observer<U, E> + Send + Sync>,
    f: F,
}

impl<F, T, U, E> Observer<T, E> for MapObserver<F, U, E>
where
    F: Fn(T) -> U + Send + Sync + 'static,
{
    fn on_next(&self, value: Result<T, E>) {
        match value {
            Ok(t) => self.observer.on_next(Ok((self.f)(t))),
            Err(e) => self.observer.on_next(Err(e)),
        }
    }

    fn on_completed(&self) {
        self.observer.on_completed()
    }
}

pub struct FilterObservable<Src, P, T, E> {
    source: Src,
    predicate: P,
    _marker: std::marker::PhantomData<(T, E)>,
}

impl<Src, P, T, E> FilterObservable<Src, P, T, E> {
    pub fn new(source: Src, predicate: P) -> Self {
        Self {
            source,
            predicate,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<Src, P, T, E> Observable<T, E> for FilterObservable<Src, P, T, E>
where
    Src: Observable<T, E>,
    P: Fn(&T) -> bool + Send + Sync + 'static,
    T: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    fn subscribe(self, observer: impl Observer<T, E> + Send + Sync + 'static) -> Subscription {
        let observer = Box::new(observer);
        self.source.subscribe(FilterObserver::<P, T, E> { observer, predicate: self.predicate })
    }
}

struct FilterObserver<P, T, E> {
    observer: Box<dyn Observer<T, E> + Send + Sync>,
    predicate: P,
}

impl<P, T, E> Observer<T, E> for FilterObserver<P, T, E>
where
    P: Fn(&T) -> bool + Send + Sync + 'static,
{
    fn on_next(&self, value: Result<T, E>) {
        match value {
            Ok(t) => {
                if (self.predicate)(&t) {
                    self.observer.on_next(Ok(t));
                }
            }
            Err(e) => self.observer.on_next(Err(e)),
        }
    }

    fn on_completed(&self) {
        self.observer.on_completed()
    }
}

pub struct CollectFuture<T, E> {
    values: Vec<T>,
    _marker: std::marker::PhantomData<E>,
}

impl<T, E> CollectFuture<T, E>
where
    T: Send + Sync + Clone + 'static,
    E: Send + Sync + 'static,
{
    pub fn new<Src>(source: Src) -> Self
    where
        Src: Observable<T, E>,
    {
        let values: Arc<Mutex<Vec<T>>> = Arc::new(Mutex::new(Vec::new()));
        let values_clone = Arc::clone(&values);
        source.subscribe(CollectObserver::<T> { values: values_clone });
        let collected = values.lock().unwrap().clone();
        Self { values: collected, _marker: std::marker::PhantomData }
    }
}

impl<T, E> std::future::Future for CollectFuture<T, E>
where
    T: Send + Sync + Clone + std::marker::Unpin + 'static,
    E: Send + Sync + std::marker::Unpin + 'static,
{
    type Output = Vec<T>;

    fn poll(self: std::pin::Pin<&mut Self>, _cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        std::task::Poll::Ready(self.values.clone())
    }
}

struct CollectObserver<T> {
    values: Arc<Mutex<Vec<T>>>,
}

impl<T, E> Observer<T, E> for CollectObserver<T>
where
    T: Send + Sync + 'static,
{
    fn on_next(&self, value: Result<T, E>) {
        if let Ok(t) = value {
            self.values.lock().unwrap().push(t);
        }
    }

    fn on_completed(&self) {}
}

pub struct TakeObservable<Src, T, E> {
    source: Src,
    count: usize,
    _marker: std::marker::PhantomData<(T, E)>,
}

impl<Src, T, E> TakeObservable<Src, T, E> {
    pub fn new(source: Src, count: usize) -> Self {
        Self {
            source,
            count,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<Src, T, E> Observable<T, E> for TakeObservable<Src, T, E>
where
    Src: Observable<T, E>,
    T: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    fn subscribe(self, observer: impl Observer<T, E> + Send + Sync + 'static) -> Subscription {
        let observer = Box::new(observer);
        self.source.subscribe(TakeObserver::new(observer, self.count))
    }
}

struct TakeObserver<T, E> {
    observer: Box<dyn Observer<T, E> + Send + Sync>,
    count: std::sync::atomic::AtomicUsize,
}

impl<T, E> TakeObserver<T, E> {
    fn new(observer: Box<dyn Observer<T, E> + Send + Sync>, count: usize) -> Self {
        Self {
            observer,
            count: std::sync::atomic::AtomicUsize::new(count),
        }
    }
}

impl<T, E> Observer<T, E> for TakeObserver<T, E>
where
    T: Send + Sync + 'static,
{
    fn on_next(&self, value: Result<T, E>) {
        let success = self.count.fetch_update(
            std::sync::atomic::Ordering::AcqRel,
            std::sync::atomic::Ordering::Acquire,
            |current| {
                if current > 0 {
                    Some(current - 1)
                } else {
                    None
                }
            },
        );
        
        if success.is_ok() {
            self.observer.on_next(value);
        }
    }

    fn on_completed(&self) {
        self.observer.on_completed()
    }
}

pub struct SkipObservable<Src, T, E> {
    source: Src,
    count: usize,
    _marker: std::marker::PhantomData<(T, E)>,
}

impl<Src, T, E> SkipObservable<Src, T, E> {
    pub fn new(source: Src, count: usize) -> Self {
        Self {
            source,
            count,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<Src, T, E> Observable<T, E> for SkipObservable<Src, T, E>
where
    Src: Observable<T, E>,
    T: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    fn subscribe(self, observer: impl Observer<T, E> + Send + Sync + 'static) -> Subscription {
        let observer = Box::new(observer);
        self.source.subscribe(SkipObserver::new(observer, self.count))
    }
}

struct SkipObserver<T, E> {
    observer: Box<dyn Observer<T, E> + Send + Sync>,
    count: std::sync::atomic::AtomicUsize,
}

impl<T, E> SkipObserver<T, E> {
    fn new(observer: Box<dyn Observer<T, E> + Send + Sync>, count: usize) -> Self {
        Self {
            observer,
            count: std::sync::atomic::AtomicUsize::new(count),
        }
    }
}

impl<T, E> Observer<T, E> for SkipObserver<T, E>
where
    T: Send + Sync + 'static,
{
    fn on_next(&self, value: Result<T, E>) {
        let current = self.count.load(std::sync::atomic::Ordering::Acquire);
        if current == 0 {
            self.observer.on_next(value);
        } else {
            let _ = self.count.fetch_update(
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
                |c| if c > 0 { Some(c - 1) } else { None },
            );
        }
    }

    fn on_completed(&self) {
        self.observer.on_completed()
    }
}

pub struct FirstObservable<Src, T, E> {
    source: Src,
    _marker: std::marker::PhantomData<(T, E)>,
}

impl<Src, T, E> FirstObservable<Src, T, E> {
    pub fn new(source: Src) -> Self {
        Self {
            source,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<Src, T, E> Observable<T, E> for FirstObservable<Src, T, E>
where
    Src: Observable<T, E>,
    T: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    fn subscribe(self, observer: impl Observer<T, E> + Send + Sync + 'static) -> Subscription {
        self.source.take(1).subscribe(observer)
    }
}

pub struct LastObservable<Src, T, E> {
    source: Src,
    _marker: std::marker::PhantomData<(T, E)>,
}

impl<Src, T, E> LastObservable<Src, T, E> {
    pub fn new(source: Src) -> Self {
        Self {
            source,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<Src, T, E> Observable<T, E> for LastObservable<Src, T, E>
where
    Src: Observable<T, E>,
    T: Send + Sync + Clone + 'static,
    E: Send + Sync + 'static,
{
    fn subscribe(self, observer: impl Observer<T, E> + Send + Sync + 'static) -> Subscription {
        let observer = Box::new(observer);
        self.source.subscribe(LastObserver::new(observer))
    }
}

struct LastObserver<T, E> {
    observer: Box<dyn Observer<T, E> + Send + Sync>,
    last_value: Arc<Mutex<Option<T>>>,
}

impl<T, E> LastObserver<T, E> {
    fn new(observer: Box<dyn Observer<T, E> + Send + Sync>) -> Self {
        Self {
            observer,
            last_value: Arc::new(Mutex::new(None)),
        }
    }
}

impl<T, E> Observer<T, E> for LastObserver<T, E>
where
    T: Send + Sync + Clone + 'static,
{
    fn on_next(&self, value: Result<T, E>) {
        if let Ok(t) = value {
            *self.last_value.lock().unwrap() = Some(t);
        } else if let Err(e) = value {
            self.observer.on_next(Err(e));
        }
    }

    fn on_completed(&self) {
        if let Some(last) = self.last_value.lock().unwrap().clone() {
            self.observer.on_next(Ok(last));
        }
        self.observer.on_completed()
    }
}

pub struct TakeWhileObservable<Src, P, T, E> {
    source: Src,
    predicate: P,
    _marker: std::marker::PhantomData<(T, E)>,
}

impl<Src, P, T, E> TakeWhileObservable<Src, P, T, E> {
    pub fn new(source: Src, predicate: P) -> Self {
        Self {
            source,
            predicate,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<Src, P, T, E> Observable<T, E> for TakeWhileObservable<Src, P, T, E>
where
    Src: Observable<T, E>,
    P: Fn(&T) -> bool + Send + Sync + 'static,
    T: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    fn subscribe(self, observer: impl Observer<T, E> + Send + Sync + 'static) -> Subscription {
        let observer = Box::new(observer);
        self.source.subscribe(TakeWhileObserver::new(observer, self.predicate))
    }
}

struct TakeWhileObserver<P, T, E> {
    observer: Box<dyn Observer<T, E> + Send + Sync>,
    predicate: P,
    active: std::sync::atomic::AtomicBool,
}

impl<P, T, E> TakeWhileObserver<P, T, E> {
    fn new(observer: Box<dyn Observer<T, E> + Send + Sync>, predicate: P) -> Self {
        Self {
            observer,
            predicate,
            active: std::sync::atomic::AtomicBool::new(true),
        }
    }
}

impl<P, T, E> Observer<T, E> for TakeWhileObserver<P, T, E>
where
    P: Fn(&T) -> bool + Send + Sync + 'static,
    T: Send + Sync + 'static,
{
    fn on_next(&self, value: Result<T, E>) {
        if self.active.load(std::sync::atomic::Ordering::SeqCst) {
            match value {
                Ok(t) => {
                    if (self.predicate)(&t) {
                        self.observer.on_next(Ok(t));
                    } else {
                        self.active.store(false, std::sync::atomic::Ordering::SeqCst);
                    }
                }
                Err(e) => {
                    self.observer.on_next(Err(e));
                }
            }
        }
    }

    fn on_completed(&self) {
        self.observer.on_completed()
    }
}

pub struct SkipWhileObservable<Src, P, T, E> {
    source: Src,
    predicate: P,
    _marker: std::marker::PhantomData<(T, E)>,
}

impl<Src, P, T, E> SkipWhileObservable<Src, P, T, E> {
    pub fn new(source: Src, predicate: P) -> Self {
        Self {
            source,
            predicate,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<Src, P, T, E> Observable<T, E> for SkipWhileObservable<Src, P, T, E>
where
    Src: Observable<T, E>,
    P: Fn(&T) -> bool + Send + Sync + 'static,
    T: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    fn subscribe(self, observer: impl Observer<T, E> + Send + Sync + 'static) -> Subscription {
        let observer = Box::new(observer);
        self.source.subscribe(SkipWhileObserver::new(observer, self.predicate))
    }
}

struct SkipWhileObserver<P, T, E> {
    observer: Box<dyn Observer<T, E> + Send + Sync>,
    predicate: P,
    skipping: std::sync::atomic::AtomicBool,
}

impl<P, T, E> SkipWhileObserver<P, T, E> {
    fn new(observer: Box<dyn Observer<T, E> + Send + Sync>, predicate: P) -> Self {
        Self {
            observer,
            predicate,
            skipping: std::sync::atomic::AtomicBool::new(true),
        }
    }
}

impl<P, T, E> Observer<T, E> for SkipWhileObserver<P, T, E>
where
    P: Fn(&T) -> bool + Send + Sync + 'static,
    T: Send + Sync + 'static,
{
    fn on_next(&self, value: Result<T, E>) {
        if self.skipping.load(std::sync::atomic::Ordering::SeqCst) {
            match value {
                Ok(t) => {
                    if (self.predicate)(&t) {
                        return;
                    } else {
                        self.skipping.store(false, std::sync::atomic::Ordering::SeqCst);
                        self.observer.on_next(Ok(t));
                    }
                }
                Err(e) => {
                    self.observer.on_next(Err(e));
                }
            }
        } else {
            self.observer.on_next(value);
        }
    }

    fn on_completed(&self) {
        self.observer.on_completed()
    }
}

pub struct FlatMapObservable<Src, F, Inner, T, U, E> {
    source: Src,
    f: F,
    _marker: std::marker::PhantomData<(Inner, T, U, E)>,
}

impl<Src, F, Inner, T, U, E> FlatMapObservable<Src, F, Inner, T, U, E> {
    pub fn new(source: Src, f: F) -> Self {
        Self {
            source,
            f,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<Src, F, Inner, T, U, E> Observable<U, E> for FlatMapObservable<Src, F, Inner, T, U, E>
where
    Src: Observable<T, E>,
    F: Fn(T) -> Inner + Send + Sync + 'static,
    Inner: Observable<U, E> + Send + Sync + 'static,
    T: Send + Sync + 'static,
    U: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    fn subscribe(self, observer: impl Observer<U, E> + Send + Sync + 'static) -> Subscription {
        let observer: Arc<Box<dyn Observer<U, E> + Send + Sync>> = Arc::new(Box::new(observer));
        self.source.subscribe(FlatMapObserver { f: self.f, observer, _marker: std::marker::PhantomData })
    }
}

struct FlatMapObserver<F, Inner, U, E> {
    f: F,
    observer: Arc<Box<dyn Observer<U, E> + Send + Sync>>,
    _marker: std::marker::PhantomData<Inner>,
}

impl<F, Inner, T, U, E: 'static> Observer<T, E> for FlatMapObserver<F, Inner, U, E>
where
    F: Fn(T) -> Inner + Send + Sync + 'static,
    Inner: Observable<U, E>,
    T: Send + Sync + 'static,
    U: Send + Sync + 'static,
{
    fn on_next(&self, value: Result<T, E>) {
        match value {
            Ok(t) => {
                let inner_observable = (self.f)(t);
                let observer_clone = Arc::clone(&self.observer);
                let _ = inner_observable.subscribe(FlatMapInnerObserver { observer: observer_clone });
            }
            Err(e) => {
                self.observer.on_next(Err(e));
            }
        }
    }

    fn on_completed(&self) {
        self.observer.on_completed()
    }
}

struct FlatMapInnerObserver<U, E> {
    observer: Arc<Box<dyn Observer<U, E> + Send + Sync>>,
}

impl<U, E> Observer<U, E> for FlatMapInnerObserver<U, E>
where
    U: Send + Sync + 'static,
{
    fn on_next(&self, value: Result<U, E>) {
        self.observer.on_next(value)
    }

    fn on_completed(&self) {}
}

pub struct ScanObservable<Src, F, Acc, T, E> {
    source: Src,
    initial: Acc,
    f: F,
    _marker: std::marker::PhantomData<(T, E)>,
}

impl<Src, F, Acc, T, E> ScanObservable<Src, F, Acc, T, E> {
    pub fn new(source: Src, initial: Acc, f: F) -> Self {
        Self {
            source,
            initial,
            f,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<Src, F, Acc, T, E> Observable<Acc, E> for ScanObservable<Src, F, Acc, T, E>
where
    Src: Observable<T, E>,
    F: Fn(Acc, T) -> Acc + Send + Sync + 'static,
    Acc: Send + Sync + Clone + 'static,
    T: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    fn subscribe(self, observer: impl Observer<Acc, E> + Send + Sync + 'static) -> Subscription {
        let observer = Box::new(observer);
        self.source.subscribe(ScanObserver::<F, Acc, T, E> { observer, accumulator: Arc::new(Mutex::new(self.initial)), f: self.f, _marker: std::marker::PhantomData })
    }
}

struct ScanObserver<F, Acc, T, E> {
    observer: Box<dyn Observer<Acc, E> + Send + Sync>,
    accumulator: Arc<Mutex<Acc>>,
    f: F,
    _marker: std::marker::PhantomData<T>,
}

impl<F, Acc, T, E> Observer<T, E> for ScanObserver<F, Acc, T, E>
where
    F: Fn(Acc, T) -> Acc + Send + Sync + 'static,
    Acc: Send + Sync + Clone + 'static,
    T: Send + Sync + 'static,
{
    fn on_next(&self, value: Result<T, E>) {
        match value {
            Ok(t) => {
                let mut acc = self.accumulator.lock().unwrap();
                *acc = (self.f)(acc.clone(), t);
                self.observer.on_next(Ok(acc.clone()));
            }
            Err(e) => {
                self.observer.on_next(Err(e));
            }
        }
    }

    fn on_completed(&self) {
        self.observer.on_completed()
    }
}

pub struct BufferObservable<Src, T, E> {
    source: Src,
    count: usize,
    _marker: std::marker::PhantomData<(T, E)>,
}

impl<Src, T, E> BufferObservable<Src, T, E> {
    pub fn new(source: Src, count: usize) -> Self {
        Self {
            source,
            count,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<Src, T, E> Observable<Vec<T>, E> for BufferObservable<Src, T, E>
where
    Src: Observable<T, E>,
    T: Send + Sync + Clone + 'static,
    E: Send + Sync + 'static,
{
    fn subscribe(self, observer: impl Observer<Vec<T>, E> + Send + Sync + 'static) -> Subscription {
        let observer = Box::new(observer);
        self.source.subscribe(BufferObserver { observer, count: self.count, buffer: Arc::new(Mutex::new(Vec::new())) })
    }
}

struct BufferObserver<T, E> {
    observer: Box<dyn Observer<Vec<T>, E> + Send + Sync>,
    count: usize,
    buffer: Arc<Mutex<Vec<T>>>,
}

impl<T, E> Observer<T, E> for BufferObserver<T, E>
where
    T: Send + Sync + Clone + 'static,
{
    fn on_next(&self, value: Result<T, E>) {
        match value {
            Ok(t) => {
                let mut buffer = self.buffer.lock().unwrap();
                buffer.push(t);
                if buffer.len() >= self.count {
                    let chunk: Vec<T> = buffer.drain(..self.count).collect();
                    self.observer.on_next(Ok(chunk));
                }
            }
            Err(e) => {
                self.observer.on_next(Err(e));
            }
        }
    }

    fn on_completed(&self) {
        let buffer = self.buffer.lock().unwrap();
        if !buffer.is_empty() {
            self.observer.on_next(Ok(buffer.clone()));
        }
        self.observer.on_completed()
    }
}

pub struct MergeObservable<First, Second, T, E> {
    first: First,
    second: Second,
    _marker: std::marker::PhantomData<(T, E)>,
}

impl<First, Second, T, E> MergeObservable<First, Second, T, E> {
    pub fn new(first: First, second: Second) -> Self {
        Self {
            first,
            second,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<First, Second, T, E> Observable<T, E> for MergeObservable<First, Second, T, E>
where
    First: Observable<T, E>,
    Second: Observable<T, E>,
    T: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    fn subscribe(self, observer: impl Observer<T, E> + Send + Sync + 'static) -> Subscription {
        let observer: Arc<Box<dyn Observer<T, E> + Send + Sync>> = Arc::new(Box::new(observer));
        let observer_clone = Arc::clone(&observer);
        
        let sub1 = self.first.subscribe(MergeObserver { observer: observer_clone });
        let sub2 = self.second.subscribe(MergeObserver { observer });
        
        let composite = CompositeDisposable::new();
        composite.add(sub1);
        composite.add(sub2);
        
        Subscription::from_fn(move || {
            composite.dispose();
        })
    }
}

struct MergeObserver<T, E> {
    observer: Arc<Box<dyn Observer<T, E> + Send + Sync>>,
}

impl<T, E> Observer<T, E> for MergeObserver<T, E>
where
    T: Send + Sync + 'static,
{
    fn on_next(&self, value: Result<T, E>) {
        self.observer.on_next(value)
    }

    fn on_completed(&self) {}
}

pub struct ZipObservable<First, Second, F, T, U, R, E> {
    first: First,
    second: Second,
    f: F,
    _marker: std::marker::PhantomData<(T, U, R, E)>,
}

impl<First, Second, F, T, U, R, E> ZipObservable<First, Second, F, T, U, R, E> {
    pub fn new(first: First, second: Second, f: F) -> Self {
        Self {
            first,
            second,
            f,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<First, Second, F, T, U, R, E> Observable<R, E> for ZipObservable<First, Second, F, T, U, R, E>
where
    First: Observable<T, E>,
    Second: Observable<U, E>,
    F: Fn(T, U) -> R + Send + Sync + 'static,
    T: Send + Sync + Clone + 'static,
    U: Send + Sync + Clone + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    fn subscribe(self, observer: impl Observer<R, E> + Send + Sync + 'static) -> Subscription {
        let observer: Arc<Box<dyn Observer<R, E> + Send + Sync>> = Arc::new(Box::new(observer));
        let state = Arc::new(Mutex::new(ZipState {
            first_buffer: Vec::new(),
            second_buffer: Vec::new(),
            first_completed: false,
            second_completed: false,
        }));
        
        let observer_clone = Arc::clone(&observer);
        let state_clone = Arc::clone(&state);
        let f_clone = Arc::new(self.f);
        
        let sub1 = self.first.subscribe(ZipFirstObserver {
            observer: Arc::clone(&observer),
            state: Arc::clone(&state),
            f: Arc::clone(&f_clone),
        });
        
        let sub2 = self.second.subscribe(ZipSecondObserver {
            observer: observer_clone,
            state: state_clone,
            f: f_clone,
        });
        
        let composite = CompositeDisposable::new();
        composite.add(sub1);
        composite.add(sub2);
        
        Subscription::from_fn(move || {
            composite.dispose();
        })
    }
}

struct ZipState<T, U> {
    first_buffer: Vec<T>,
    second_buffer: Vec<U>,
    first_completed: bool,
    second_completed: bool,
}

struct ZipFirstObserver<F, T, U, R, E> {
    observer: Arc<Box<dyn Observer<R, E> + Send + Sync>>,
    state: Arc<Mutex<ZipState<T, U>>>,
    f: Arc<F>,
}

impl<F, T, U, R, E> Observer<T, E> for ZipFirstObserver<F, T, U, R, E>
where
    F: Fn(T, U) -> R + Send + Sync + 'static,
    T: Send + Sync + Clone + 'static,
    U: Send + Sync + Clone + 'static,
    R: Send + Sync + 'static,
{
    fn on_next(&self, value: Result<T, E>) {
        match value {
            Ok(t) => {
                let mut state = self.state.lock().unwrap();
                state.first_buffer.push(t);
                while !state.first_buffer.is_empty() && !state.second_buffer.is_empty() {
                    let t = state.first_buffer.remove(0);
                    let u = state.second_buffer.remove(0);
                    self.observer.on_next(Ok((self.f)(t, u)));
                }
            }
            Err(e) => {
                self.observer.on_next(Err(e));
            }
        }
    }

    fn on_completed(&self) {
        let mut state = self.state.lock().unwrap();
        state.first_completed = true;
        if state.second_completed {
            self.observer.on_completed();
        }
    }
}

struct ZipSecondObserver<F, T, U, R, E> {
    observer: Arc<Box<dyn Observer<R, E> + Send + Sync>>,
    state: Arc<Mutex<ZipState<T, U>>>,
    f: Arc<F>,
}

impl<F, T, U, R, E> Observer<U, E> for ZipSecondObserver<F, T, U, R, E>
where
    F: Fn(T, U) -> R + Send + Sync + 'static,
    T: Send + Sync + Clone + 'static,
    U: Send + Sync + Clone + 'static,
    R: Send + Sync + 'static,
{
    fn on_next(&self, value: Result<U, E>) {
        match value {
            Ok(u) => {
                let mut state = self.state.lock().unwrap();
                state.second_buffer.push(u);
                while !state.first_buffer.is_empty() && !state.second_buffer.is_empty() {
                    let t = state.first_buffer.remove(0);
                    let u = state.second_buffer.remove(0);
                    self.observer.on_next(Ok((self.f)(t, u)));
                }
            }
            Err(e) => {
                self.observer.on_next(Err(e));
            }
        }
    }

    fn on_completed(&self) {
        let mut state = self.state.lock().unwrap();
        state.second_completed = true;
        if state.first_completed {
            self.observer.on_completed();
        }
    }
}

pub struct ConcatObservable<First, Second, T, E> {
    first: First,
    second: Second,
    _marker: std::marker::PhantomData<(T, E)>,
}

impl<First, Second, T, E> ConcatObservable<First, Second, T, E> {
    pub fn new(first: First, second: Second) -> Self {
        Self {
            first,
            second,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<First, Second, T, E> Observable<T, E> for ConcatObservable<First, Second, T, E>
where
    First: Observable<T, E>,
    Second: Observable<T, E> + Send + Sync + 'static,
    T: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    fn subscribe(self, observer: impl Observer<T, E> + Send + Sync + 'static) -> Subscription {
        let observer: Arc<Box<dyn Observer<T, E> + Send + Sync>> = Arc::new(Box::new(observer));
        let second = Arc::new(Mutex::new(Some(self.second)));
        
        self.first.subscribe(ConcatFirstObserver {
            observer,
            second,
        })
    }
}

struct ConcatFirstObserver<Second, T, E> {
    observer: Arc<Box<dyn Observer<T, E> + Send + Sync>>,
    second: Arc<Mutex<Option<Second>>>,
}

impl<Second, T, E: 'static> Observer<T, E> for ConcatFirstObserver<Second, T, E>
where
    Second: Observable<T, E>,
    T: Send + Sync + 'static,
{
    fn on_next(&self, value: Result<T, E>) {
        self.observer.on_next(value)
    }

    fn on_completed(&self) {
        if let Some(second) = self.second.lock().unwrap().take() {
            second.subscribe(ConcatSecondObserver {
                observer: Arc::clone(&self.observer),
            });
        }
    }
}

struct ConcatSecondObserver<T, E> {
    observer: Arc<Box<dyn Observer<T, E> + Send + Sync>>,
}

impl<T, E> Observer<T, E> for ConcatSecondObserver<T, E>
where
    T: Send + Sync + 'static,
{
    fn on_next(&self, value: Result<T, E>) {
        self.observer.on_next(value)
    }

    fn on_completed(&self) {
        self.observer.on_completed()
    }
}

pub struct CombineLatestObservable<First, Second, F, T, U, R, E> {
    first: First,
    second: Second,
    f: F,
    _marker: std::marker::PhantomData<(T, U, R, E)>,
}

impl<First, Second, F, T, U, R, E> CombineLatestObservable<First, Second, F, T, U, R, E> {
    pub fn new(first: First, second: Second, f: F) -> Self {
        Self {
            first,
            second,
            f,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<First, Second, F, T, U, R, E> Observable<R, E> for CombineLatestObservable<First, Second, F, T, U, R, E>
where
    First: Observable<T, E>,
    Second: Observable<U, E>,
    F: Fn(T, U) -> R + Send + Sync + 'static,
    T: Send + Sync + Clone + 'static,
    U: Send + Sync + Clone + 'static,
    R: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    fn subscribe(self, observer: impl Observer<R, E> + Send + Sync + 'static) -> Subscription {
        let observer: Arc<Box<dyn Observer<R, E> + Send + Sync>> = Arc::new(Box::new(observer));
        let state = Arc::new(Mutex::new(CombineLatestState {
            first_value: None,
            second_value: None,
            first_completed: false,
            second_completed: false,
        }));
        
        let observer_clone = Arc::clone(&observer);
        let state_clone = Arc::clone(&state);
        let f_clone = Arc::new(self.f);
        
        let sub1 = self.first.subscribe(CombineLatestFirstObserver {
            observer: Arc::clone(&observer),
            state: Arc::clone(&state),
            f: Arc::clone(&f_clone),
        });
        
        let sub2 = self.second.subscribe(CombineLatestSecondObserver {
            observer: observer_clone,
            state: state_clone,
            f: f_clone,
        });
        
        let composite = CompositeDisposable::new();
        composite.add(sub1);
        composite.add(sub2);
        
        Subscription::from_fn(move || {
            composite.dispose();
        })
    }
}

struct CombineLatestState<T, U> {
    first_value: Option<T>,
    second_value: Option<U>,
    first_completed: bool,
    second_completed: bool,
}

struct CombineLatestFirstObserver<F, T, U, R, E> {
    observer: Arc<Box<dyn Observer<R, E> + Send + Sync>>,
    state: Arc<Mutex<CombineLatestState<T, U>>>,
    f: Arc<F>,
}

impl<F, T, U, R, E> Observer<T, E> for CombineLatestFirstObserver<F, T, U, R, E>
where
    F: Fn(T, U) -> R + Send + Sync + 'static,
    T: Send + Sync + Clone + 'static,
    U: Send + Sync + Clone + 'static,
    R: Send + Sync + 'static,
{
    fn on_next(&self, value: Result<T, E>) {
        match value {
            Ok(t) => {
                let mut state = self.state.lock().unwrap();
                state.first_value = Some(t.clone());
                if let Some(u) = &state.second_value {
                    self.observer.on_next(Ok((self.f)(t, u.clone())));
                }
            }
            Err(e) => {
                self.observer.on_next(Err(e));
            }
        }
    }

    fn on_completed(&self) {
        let mut state = self.state.lock().unwrap();
        state.first_completed = true;
        if state.second_completed {
            self.observer.on_completed();
        }
    }
}

struct CombineLatestSecondObserver<F, T, U, R, E> {
    observer: Arc<Box<dyn Observer<R, E> + Send + Sync>>,
    state: Arc<Mutex<CombineLatestState<T, U>>>,
    f: Arc<F>,
}

impl<F, T, U, R, E> Observer<U, E> for CombineLatestSecondObserver<F, T, U, R, E>
where
    F: Fn(T, U) -> R + Send + Sync + 'static,
    T: Send + Sync + Clone + 'static,
    U: Send + Sync + Clone + 'static,
    R: Send + Sync + 'static,
{
    fn on_next(&self, value: Result<U, E>) {
        match value {
            Ok(u) => {
                let mut state = self.state.lock().unwrap();
                state.second_value = Some(u.clone());
                if let Some(t) = &state.first_value {
                    self.observer.on_next(Ok((self.f)(t.clone(), u)));
                }
            }
            Err(e) => {
                self.observer.on_next(Err(e));
            }
        }
    }

    fn on_completed(&self) {
        let mut state = self.state.lock().unwrap();
        state.second_completed = true;
        if state.first_completed {
            self.observer.on_completed();
        }
    }
}



pub struct DistinctUntilChangedObservable<Src, T, E> {
    source: Src,
    _marker: std::marker::PhantomData<(T, E)>,
}

impl<Src, T, E> DistinctUntilChangedObservable<Src, T, E> {
    pub fn new(source: Src) -> Self {
        Self {
            source,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<Src, T, E> Observable<T, E> for DistinctUntilChangedObservable<Src, T, E>
where
    Src: Observable<T, E>,
    T: Send + Sync + Clone + PartialEq + 'static,
    E: Send + Sync + 'static,
{
    fn subscribe(self, observer: impl Observer<T, E> + Send + Sync + 'static) -> Subscription {
        let observer: Arc<Box<dyn Observer<T, E> + Send + Sync>> = Arc::new(Box::new(observer));
        self.source.subscribe(DistinctUntilChangedObserver {
            observer,
            last_value: Arc::new(Mutex::new(None)),
        })
    }
}

struct DistinctUntilChangedObserver<T, E> {
    observer: Arc<Box<dyn Observer<T, E> + Send + Sync>>,
    last_value: Arc<Mutex<Option<T>>>,
}

impl<T, E> Observer<T, E> for DistinctUntilChangedObserver<T, E>
where
    T: Send + Sync + Clone + PartialEq + 'static,
{
    fn on_next(&self, value: Result<T, E>) {
        match value {
            Ok(t) => {
                let mut last = self.last_value.lock().unwrap();
                if last.as_ref() != Some(&t) {
                    *last = Some(t.clone());
                    self.observer.on_next(Ok(t));
                }
            }
            Err(e) => {
                self.observer.on_next(Err(e));
            }
        }
    }

    fn on_completed(&self) {
        self.observer.on_completed();
    }
}

pub struct TimeoutObservable<Src, T, E> {
    source: Src,
    timeout: std::time::Duration,
    _marker: std::marker::PhantomData<(T, E)>,
}

impl<Src, T, E> TimeoutObservable<Src, T, E> {
    pub fn new(source: Src, timeout: std::time::Duration) -> Self {
        Self {
            source,
            timeout,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<Src, T, E> Observable<T, E> for TimeoutObservable<Src, T, E>
where
    Src: Observable<T, E>,
    T: Send + Sync + 'static,
    E: Send + Sync + Clone + From<&'static str> + 'static,
{
    fn subscribe(self, observer: impl Observer<T, E> + Send + Sync + 'static) -> Subscription {
        let observer: Arc<Box<dyn Observer<T, E> + Send + Sync>> = Arc::new(Box::new(observer));
        let timeout = self.timeout;
        let timeout_triggered = Arc::new(std::sync::atomic::AtomicBool::new(false));
        
        let timeout_observer = Arc::clone(&observer);
        let timeout_triggered_clone = Arc::clone(&timeout_triggered);
        
        std::thread::spawn(move || {
            std::thread::sleep(timeout);
            if timeout_triggered_clone.swap(true, std::sync::atomic::Ordering::Relaxed) == false {
                timeout_observer.on_next(Err(E::from("Timeout")));
                timeout_observer.on_completed();
            }
        });
        
        self.source.subscribe(TimeoutObserver {
            observer,
            timeout_triggered,
        })
    }
}

struct TimeoutObserver<T, E> {
    observer: Arc<Box<dyn Observer<T, E> + Send + Sync>>,
    timeout_triggered: Arc<std::sync::atomic::AtomicBool>,
}

impl<T, E> Observer<T, E> for TimeoutObserver<T, E>
where
    T: Send + Sync + 'static,
{
    fn on_next(&self, value: Result<T, E>) {
        if !self.timeout_triggered.load(std::sync::atomic::Ordering::Relaxed) {
            self.observer.on_next(value);
        }
    }

    fn on_completed(&self) {
        if !self.timeout_triggered.swap(true, std::sync::atomic::Ordering::Relaxed) {
            self.observer.on_completed();
        }
    }
}

pub trait ObservableExtWithTime<T, E>: Observable<T, E> + Sized {
    fn distinct_until_changed(self) -> DistinctUntilChangedObservable<Self, T, E>
    where
        T: Send + Sync + Clone + PartialEq + 'static,
        E: Send + Sync + 'static,
    {
        DistinctUntilChangedObservable::new(self)
    }

    fn timeout(self, timeout: std::time::Duration) -> TimeoutObservable<Self, T, E>
    where
        E: Send + Sync + Clone + From<&'static str> + 'static,
        T: Send + Sync + 'static,
    {
        TimeoutObservable::new(self, timeout)
    }

    fn debounce(self, delay: std::time::Duration) -> DebounceObservable<Self, T, E>
    where
        T: Send + Sync + Clone + 'static,
        E: Send + Sync + Clone + 'static,
    {
        DebounceObservable::new(self, delay)
    }

    fn throttle(self, duration: std::time::Duration) -> ThrottleObservable<Self, T, E>
    where
        T: Send + Sync + Clone + 'static,
        E: Send + Sync + Clone + 'static,
    {
        ThrottleObservable::new(self, duration)
    }

    fn observe_on<S>(self, scheduler: S) -> ObserveOnObservable<Self, S, T, E>
    where
        S: crate::scheduler::Scheduler + Send + Sync + 'static,
        T: Send + Sync + Clone + 'static,
        E: Send + Sync + Clone + 'static,
    {
        ObserveOnObservable::new(self, scheduler)
    }

    fn subscribe_on<S>(self, scheduler: S) -> SubscribeOnObservable<Self, S, T, E>
    where
        S: crate::scheduler::Scheduler + Send + Sync + 'static,
        T: Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        SubscribeOnObservable::new(self, scheduler)
    }
}

impl<O, T, E> ObservableExtWithTime<T, E> for O where O: Observable<T, E> + Sized {}

pub struct TakeLastObservable<Src, T, E> {
    source: Src,
    count: usize,
    _marker: std::marker::PhantomData<(T, E)>,
}

impl<Src, T, E> TakeLastObservable<Src, T, E> {
    pub fn new(source: Src, count: usize) -> Self {
        Self {
            source,
            count,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<Src, T, E> Observable<Vec<T>, E> for TakeLastObservable<Src, T, E>
where
    Src: Observable<T, E>,
    T: Send + Sync + Clone + 'static,
    E: Send + Sync + 'static,
{
    fn subscribe(self, observer: impl Observer<Vec<T>, E> + Send + Sync + 'static) -> Subscription {
        let observer: Arc<Box<dyn Observer<Vec<T>, E> + Send + Sync>> = Arc::new(Box::new(observer));
        self.source.subscribe(TakeLastObserver {
            observer,
            count: self.count,
            buffer: Arc::new(Mutex::new(Vec::new())),
        })
    }
}

struct TakeLastObserver<T, E> {
    observer: Arc<Box<dyn Observer<Vec<T>, E> + Send + Sync>>,
    count: usize,
    buffer: Arc<Mutex<Vec<T>>>,
}

impl<T, E> Observer<T, E> for TakeLastObserver<T, E>
where
    T: Send + Sync + Clone + 'static,
{
    fn on_next(&self, value: Result<T, E>) {
        match value {
            Ok(t) => {
                let mut buffer = self.buffer.lock().unwrap();
                buffer.push(t);
                if buffer.len() > self.count {
                    buffer.remove(0);
                }
            }
            Err(e) => {
                self.observer.on_next(Err(e));
            }
        }
    }

    fn on_completed(&self) {
        let buffer = self.buffer.lock().unwrap();
        if !buffer.is_empty() {
            self.observer.on_next(Ok(buffer.clone()));
        }
        self.observer.on_completed()
    }
}

pub struct SkipLastObservable<Src, T, E> {
    source: Src,
    count: usize,
    _marker: std::marker::PhantomData<(T, E)>,
}

impl<Src, T, E> SkipLastObservable<Src, T, E> {
    pub fn new(source: Src, count: usize) -> Self {
        Self {
            source,
            count,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<Src, T, E> Observable<T, E> for SkipLastObservable<Src, T, E>
where
    Src: Observable<T, E>,
    T: Send + Sync + Clone + 'static,
    E: Send + Sync + 'static,
{
    fn subscribe(self, observer: impl Observer<T, E> + Send + Sync + 'static) -> Subscription {
        let observer: Arc<Box<dyn Observer<T, E> + Send + Sync>> = Arc::new(Box::new(observer));
        self.source.subscribe(SkipLastObserver {
            observer,
            count: self.count,
            buffer: Arc::new(Mutex::new(Vec::with_capacity(self.count))),
        })
    }
}

struct SkipLastObserver<T, E> {
    observer: Arc<Box<dyn Observer<T, E> + Send + Sync>>,
    count: usize,
    buffer: Arc<Mutex<Vec<T>>>,
}

impl<T, E> Observer<T, E> for SkipLastObserver<T, E>
where
    T: Send + Sync + Clone + 'static,
{
    fn on_next(&self, value: Result<T, E>) {
        match value {
            Ok(t) => {
                let mut buffer = self.buffer.lock().unwrap();
                if self.count == 0 {
                    self.observer.on_next(Ok(t));
                } else {
                    buffer.push(t);
                    if buffer.len() > self.count {
                        let oldest = buffer.remove(0);
                        self.observer.on_next(Ok(oldest));
                    }
                }
            }
            Err(e) => {
                self.observer.on_next(Err(e));
            }
        }
    }

    fn on_completed(&self) {
        self.observer.on_completed()
    }
}

pub struct ElementAtObservable<Src, T, E> {
    source: Src,
    index: usize,
    _marker: std::marker::PhantomData<(T, E)>,
}

impl<Src, T, E> ElementAtObservable<Src, T, E> {
    pub fn new(source: Src, index: usize) -> Self {
        Self {
            source,
            index,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<Src, T, E> Observable<T, E> for ElementAtObservable<Src, T, E>
where
    Src: Observable<T, E>,
    T: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    fn subscribe(self, observer: impl Observer<T, E> + Send + Sync + 'static) -> Subscription {
        let observer = Box::new(observer);
        self.source.subscribe(ElementAtObserver::new(observer, self.index))
    }
}

struct ElementAtObserver<T, E> {
    observer: Box<dyn Observer<T, E> + Send + Sync>,
    index: std::sync::atomic::AtomicUsize,
    emitted: std::sync::atomic::AtomicBool,
}

impl<T, E> ElementAtObserver<T, E> {
    fn new(observer: Box<dyn Observer<T, E> + Send + Sync>, index: usize) -> Self {
        Self {
            observer,
            index: std::sync::atomic::AtomicUsize::new(index),
            emitted: std::sync::atomic::AtomicBool::new(false),
        }
    }
}

impl<T, E> Observer<T, E> for ElementAtObserver<T, E>
where
    T: Send + Sync + 'static,
{
    fn on_next(&self, value: Result<T, E>) {
        match value {
            Ok(t) => {
                if !self.emitted.load(std::sync::atomic::Ordering::SeqCst) {
                    let current = self.index.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                    if current == 0 {
                        self.emitted.store(true, std::sync::atomic::Ordering::SeqCst);
                        self.observer.on_next(Ok(t));
                    }
                }
            }
            Err(e) => {
                self.observer.on_next(Err(e));
            }
        }
    }

    fn on_completed(&self) {
        self.observer.on_completed()
    }
}

pub struct DistinctObservable<Src, T, E> {
    source: Src,
    _marker: std::marker::PhantomData<(T, E)>,
}

impl<Src, T, E> DistinctObservable<Src, T, E> {
    pub fn new(source: Src) -> Self {
        Self {
            source,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<Src, T, E> Observable<T, E> for DistinctObservable<Src, T, E>
where
    Src: Observable<T, E>,
    T: Send + Sync + Clone + std::cmp::Eq + std::hash::Hash + 'static,
    E: Send + Sync + 'static,
{
    fn subscribe(self, observer: impl Observer<T, E> + Send + Sync + 'static) -> Subscription {
        let observer: Arc<Box<dyn Observer<T, E> + Send + Sync>> = Arc::new(Box::new(observer));
        self.source.subscribe(DistinctObserver {
            observer,
            seen: Arc::new(Mutex::new(std::collections::HashSet::new())),
        })
    }
}

struct DistinctObserver<T, E> {
    observer: Arc<Box<dyn Observer<T, E> + Send + Sync>>,
    seen: Arc<Mutex<std::collections::HashSet<T>>>,
}

impl<T, E> Observer<T, E> for DistinctObserver<T, E>
where
    T: Send + Sync + Clone + std::cmp::Eq + std::hash::Hash + 'static,
{
    fn on_next(&self, value: Result<T, E>) {
        match value {
            Ok(t) => {
                let mut seen = self.seen.lock().unwrap();
                if seen.insert(t.clone()) {
                    self.observer.on_next(Ok(t));
                }
            }
            Err(e) => {
                self.observer.on_next(Err(e));
            }
        }
    }

    fn on_completed(&self) {
        self.observer.on_completed()
    }
}

pub struct IgnoreElementsObservable<Src, T, E> {
    source: Src,
    _marker: std::marker::PhantomData<(T, E)>,
}

impl<Src, T, E> IgnoreElementsObservable<Src, T, E> {
    pub fn new(source: Src) -> Self {
        Self {
            source,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<Src, T, E> Observable<T, E> for IgnoreElementsObservable<Src, T, E>
where
    Src: Observable<T, E>,
    T: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    fn subscribe(self, observer: impl Observer<T, E> + Send + Sync + 'static) -> Subscription {
        let observer: Arc<Box<dyn Observer<T, E> + Send + Sync>> = Arc::new(Box::new(observer));
        self.source.subscribe(IgnoreElementsObserver { observer })
    }
}

struct IgnoreElementsObserver<T, E> {
    observer: Arc<Box<dyn Observer<T, E> + Send + Sync>>,
}

impl<T, E> Observer<T, E> for IgnoreElementsObserver<T, E>
where
    T: Send + Sync + 'static,
{
    fn on_next(&self, value: Result<T, E>) {
        if let Err(e) = value {
            self.observer.on_next(Err(e));
        }
    }

    fn on_completed(&self) {
        self.observer.on_completed()
    }
}

pub struct CatchErrorObservable<Src, F, T, E> {
    source: Src,
    handler: F,
    _marker: std::marker::PhantomData<(T, E)>,
}

impl<Src, F, T, E> CatchErrorObservable<Src, F, T, E> {
    pub fn new(source: Src, handler: F) -> Self {
        Self {
            source,
            handler,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<Src, F, T, E> Observable<T, E> for CatchErrorObservable<Src, F, T, E>
where
    Src: Observable<T, E>,
    F: Fn(E) -> Box<dyn FnOnce(Box<dyn Observer<T, E> + Send + Sync>) -> Subscription + Send + Sync> + Send + Sync + 'static,
    T: Send + Sync + 'static,
    E: Send + Sync + Clone + 'static,
{
    fn subscribe(self, observer: impl Observer<T, E> + Send + Sync + 'static) -> Subscription {
        let observer: Arc<Box<dyn Observer<T, E> + Send + Sync>> = Arc::new(Box::new(observer));
        let handler: Arc<F> = Arc::new(self.handler);
        self.source.subscribe(CatchErrorObserver {
            observer,
            handler,
            _marker: std::marker::PhantomData,
        })
    }
}

struct CatchErrorObserver<F, T, E> {
    observer: Arc<Box<dyn Observer<T, E> + Send + Sync>>,
    handler: Arc<F>,
    _marker: std::marker::PhantomData<(T, E)>,
}

impl<F, T, E> Observer<T, E> for CatchErrorObserver<F, T, E>
where
    F: Fn(E) -> Box<dyn FnOnce(Box<dyn Observer<T, E> + Send + Sync>) -> Subscription + Send + Sync> + Send + Sync + 'static,
    T: Send + Sync + 'static,
    E: Send + Sync + Clone + 'static,
{
    fn on_next(&self, value: Result<T, E>) {
        match value {
            Ok(t) => {
                self.observer.on_next(Ok(t));
            }
            Err(e) => {
                let subscribe_fn = (self.handler)(e.clone());
                subscribe_fn(Box::new(CatchErrorInnerObserver {
                    observer: Arc::clone(&self.observer),
                }));
            }
        }
    }

    fn on_completed(&self) {
        self.observer.on_completed()
    }
}

struct CatchErrorInnerObserver<T, E> {
    observer: Arc<Box<dyn Observer<T, E> + Send + Sync>>,
}

impl<T, E> Observer<T, E> for CatchErrorInnerObserver<T, E>
where
    T: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    fn on_next(&self, value: Result<T, E>) {
        self.observer.on_next(value)
    }

    fn on_completed(&self) {
        self.observer.on_completed()
    }
}

pub struct OnErrorResumeNextObservable<Src, Other, T, E> {
    source: Src,
    other: Other,
    _marker: std::marker::PhantomData<(T, E)>,
}

impl<Src, Other, T, E> OnErrorResumeNextObservable<Src, Other, T, E> {
    pub fn new(source: Src, other: Other) -> Self {
        Self {
            source,
            other,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<Src, Other, T, E> Observable<T, E> for OnErrorResumeNextObservable<Src, Other, T, E>
where
    Src: Observable<T, E>,
    Other: Observable<T, E> + Send + Sync + 'static,
    T: Send + Sync + 'static,
    E: Send + Sync + Clone + 'static,
{
    fn subscribe(self, observer: impl Observer<T, E> + Send + Sync + 'static) -> Subscription {
        let observer: Arc<Box<dyn Observer<T, E> + Send + Sync>> = Arc::new(Box::new(observer));
        let other = Arc::new(Mutex::new(Some(self.other)));
        self.source.subscribe(OnErrorResumeNextObserver {
            observer,
            other,
        })
    }
}

struct OnErrorResumeNextObserver<Other, T, E> {
    observer: Arc<Box<dyn Observer<T, E> + Send + Sync>>,
    other: Arc<Mutex<Option<Other>>>,
}

impl<Other, T, E> Observer<T, E> for OnErrorResumeNextObserver<Other, T, E>
where
    Other: Observable<T, E> + Send + Sync + 'static,
    T: Send + Sync + 'static,
    E: Send + Sync + Clone + 'static,
{
    fn on_next(&self, value: Result<T, E>) {
        match value {
            Ok(t) => {
                self.observer.on_next(Ok(t));
            }
            Err(_e) => {
                if let Some(other) = self.other.lock().unwrap().take() {
                    other.subscribe(OnErrorResumeNextInnerObserver {
                        observer: Arc::clone(&self.observer),
                    });
                }
            }
        }
    }

    fn on_completed(&self) {
        self.observer.on_completed()
    }
}

struct OnErrorResumeNextInnerObserver<T, E> {
    observer: Arc<Box<dyn Observer<T, E> + Send + Sync>>,
}

impl<T, E> Observer<T, E> for OnErrorResumeNextInnerObserver<T, E>
where
    T: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    fn on_next(&self, value: Result<T, E>) {
        self.observer.on_next(value)
    }

    fn on_completed(&self) {
        self.observer.on_completed()
    }
}

pub trait ObservableExtFilter<T, E>: Observable<T, E> + Sized {
    fn take_last(self, count: usize) -> TakeLastObservable<Self, T, E>
    where
        T: Send + Sync + Clone + 'static,
        E: Send + Sync + 'static,
    {
        TakeLastObservable::new(self, count)
    }

    fn skip_last(self, count: usize) -> SkipLastObservable<Self, T, E>
    where
        T: Send + Sync + Clone + 'static,
        E: Send + Sync + 'static,
    {
        SkipLastObservable::new(self, count)
    }

    fn element_at(self, index: usize) -> ElementAtObservable<Self, T, E>
    where
        T: Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        ElementAtObservable::new(self, index)
    }

    fn distinct(self) -> DistinctObservable<Self, T, E>
    where
        T: Send + Sync + Clone + std::cmp::Eq + std::hash::Hash + 'static,
        E: Send + Sync + 'static,
    {
        DistinctObservable::new(self)
    }

    fn ignore_elements(self) -> IgnoreElementsObservable<Self, T, E>
    where
        T: Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        IgnoreElementsObservable::new(self)
    }

    fn contains(self, target: T) -> ContainsObservable<Self, T, E>
    where
        T: Send + Sync + Clone + PartialEq + 'static,
        E: Send + Sync + 'static,
    {
        ContainsObservable::new(self, target)
    }

    fn all<P>(self, predicate: P) -> AllObservable<Self, P, T, E>
    where
        P: Fn(&T) -> bool + Send + Sync + 'static,
        T: Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        AllObservable::new(self, predicate)
    }
}

impl<O, T, E> ObservableExtFilter<T, E> for O where O: Observable<T, E> + Sized {}

pub trait ObservableExtError<T, E>: Observable<T, E> + Sized {
    fn catch_error<F>(self, handler: F) -> CatchErrorObservable<Self, F, T, E>
    where
        F: Fn(E) -> Box<dyn FnOnce(Box<dyn Observer<T, E> + Send + Sync>) -> Subscription + Send + Sync> + Send + Sync + 'static,
        T: Send + Sync + 'static,
        E: Send + Sync + Clone + 'static,
    {
        CatchErrorObservable::new(self, handler)
    }

    fn on_error_resume_next<Other>(self, other: Other) -> OnErrorResumeNextObservable<Self, Other, T, E>
    where
        Other: Observable<T, E> + Send + Sync + 'static,
        T: Send + Sync + 'static,
        E: Send + Sync + Clone + 'static,
    {
        OnErrorResumeNextObservable::new(self, other)
    }

    fn retry(self, count: usize) -> RetryObservable<Self, T, E>
    where
        T: Send + Sync + 'static,
        E: Send + Sync + Clone + 'static,
        Self: Clone + Send + Sync + 'static,
    {
        RetryObservable::new(self, count)
    }

    fn retry_when<F, Notifier>(self, notifier_factory: F) -> RetryWhenObservable<Self, F, Notifier, T, E>
    where
        F: Fn(Arc<Mutex<Option<E>>>) -> Notifier + Send + Sync + 'static,
        Notifier: Observable<(), E> + Send + Sync + 'static,
        T: Send + Sync + 'static,
        E: Send + Sync + Clone + 'static,
        Self: Clone + Send + Sync + 'static,
    {
        RetryWhenObservable::new(self, notifier_factory)
    }
}

impl<O, T, E> ObservableExtError<T, E> for O where O: Observable<T, E> + Sized {}

// ===================== 数学操作符 =====================

pub struct ReduceObservable<Src, F, Acc, T, E> {
    source: Src,
    initial: Acc,
    f: F,
    _marker: std::marker::PhantomData<(T, E)>,
}

impl<Src, F, Acc, T, E> ReduceObservable<Src, F, Acc, T, E> {
    pub fn new(source: Src, initial: Acc, f: F) -> Self {
        Self {
            source,
            initial,
            f,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<Src, F, Acc, T, E> Observable<Acc, E> for ReduceObservable<Src, F, Acc, T, E>
where
    Src: Observable<T, E>,
    F: Fn(Acc, T) -> Acc + Send + Sync + 'static,
    Acc: Send + Sync + Clone + 'static,
    T: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    fn subscribe(self, observer: impl Observer<Acc, E> + Send + Sync + 'static) -> Subscription {
        let observer: Arc<Box<dyn Observer<Acc, E> + Send + Sync>> = Arc::new(Box::new(observer));
        self.source.subscribe(ReduceObserver {
            observer,
            accumulator: Arc::new(Mutex::new(self.initial)),
            f: self.f,
            _marker: std::marker::PhantomData,
        })
    }
}

struct ReduceObserver<F, Acc, T, E> {
    observer: Arc<Box<dyn Observer<Acc, E> + Send + Sync>>,
    accumulator: Arc<Mutex<Acc>>,
    f: F,
    _marker: std::marker::PhantomData<T>,
}

impl<F, Acc, T, E> Observer<T, E> for ReduceObserver<F, Acc, T, E>
where
    F: Fn(Acc, T) -> Acc + Send + Sync + 'static,
    Acc: Send + Sync + Clone + 'static,
    T: Send + Sync + 'static,
{
    fn on_next(&self, value: Result<T, E>) {
        match value {
            Ok(t) => {
                let mut acc = self.accumulator.lock().unwrap();
                *acc = (self.f)(acc.clone(), t);
            }
            Err(e) => {
                self.observer.on_next(Err(e));
            }
        }
    }

    fn on_completed(&self) {
        let acc = self.accumulator.lock().unwrap().clone();
        self.observer.on_next(Ok(acc));
        self.observer.on_completed();
    }
}

pub struct CountObservable<Src, T, E> {
    source: Src,
    _marker: std::marker::PhantomData<(T, E)>,
}

impl<Src, T, E> CountObservable<Src, T, E> {
    pub fn new(source: Src) -> Self {
        Self {
            source,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<Src, T, E> Observable<usize, E> for CountObservable<Src, T, E>
where
    Src: Observable<T, E>,
    T: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    fn subscribe(self, observer: impl Observer<usize, E> + Send + Sync + 'static) -> Subscription {
        let observer: Arc<Box<dyn Observer<usize, E> + Send + Sync>> = Arc::new(Box::new(observer));
        self.source.subscribe(CountObserver {
            observer,
            count: Arc::new(Mutex::new(0usize)),
            _marker: std::marker::PhantomData,
        })
    }
}

struct CountObserver<T, E> {
    observer: Arc<Box<dyn Observer<usize, E> + Send + Sync>>,
    count: Arc<Mutex<usize>>,
    _marker: std::marker::PhantomData<T>,
}

impl<T, E> Observer<T, E> for CountObserver<T, E>
where
    T: Send + Sync + 'static,
{
    fn on_next(&self, value: Result<T, E>) {
        match value {
            Ok(_) => {
                let mut c = self.count.lock().unwrap();
                *c += 1;
            }
            Err(e) => {
                self.observer.on_next(Err(e));
            }
        }
    }

    fn on_completed(&self) {
        let c = *self.count.lock().unwrap();
        self.observer.on_next(Ok(c));
        self.observer.on_completed();
    }
}

pub struct SumObservable<Src, T, E> {
    source: Src,
    _marker: std::marker::PhantomData<(T, E)>,
}

impl<Src, T, E> SumObservable<Src, T, E> {
    pub fn new(source: Src) -> Self {
        Self {
            source,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<Src, T, E> Observable<T, E> for SumObservable<Src, T, E>
where
    Src: Observable<T, E>,
    T: Send + Sync + Clone + std::ops::Add<Output = T> + Default + 'static,
    E: Send + Sync + 'static,
{
    fn subscribe(self, observer: impl Observer<T, E> + Send + Sync + 'static) -> Subscription {
        let observer: Arc<Box<dyn Observer<T, E> + Send + Sync>> = Arc::new(Box::new(observer));
        self.source.subscribe(SumObserver {
            observer,
            sum: Arc::new(Mutex::new(T::default())),
        })
    }
}

struct SumObserver<T, E> {
    observer: Arc<Box<dyn Observer<T, E> + Send + Sync>>,
    sum: Arc<Mutex<T>>,
}

impl<T, E> Observer<T, E> for SumObserver<T, E>
where
    T: Send + Sync + Clone + std::ops::Add<Output = T> + Default + 'static,
{
    fn on_next(&self, value: Result<T, E>) {
        match value {
            Ok(t) => {
                let mut s = self.sum.lock().unwrap();
                let new_val = s.clone() + t;
                *s = new_val;
            }
            Err(e) => {
                self.observer.on_next(Err(e));
            }
        }
    }

    fn on_completed(&self) {
        let s = self.sum.lock().unwrap().clone();
        self.observer.on_next(Ok(s));
        self.observer.on_completed();
    }
}

pub struct MinObservable<Src, T, E> {
    source: Src,
    _marker: std::marker::PhantomData<(T, E)>,
}

impl<Src, T, E> MinObservable<Src, T, E> {
    pub fn new(source: Src) -> Self {
        Self {
            source,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<Src, T, E> Observable<T, E> for MinObservable<Src, T, E>
where
    Src: Observable<T, E>,
    T: Send + Sync + Clone + Ord + 'static,
    E: Send + Sync + 'static,
{
    fn subscribe(self, observer: impl Observer<T, E> + Send + Sync + 'static) -> Subscription {
        let observer: Arc<Box<dyn Observer<T, E> + Send + Sync>> = Arc::new(Box::new(observer));
        self.source.subscribe(MinObserver {
            observer,
            min_value: Arc::new(Mutex::new(None)),
        })
    }
}

struct MinObserver<T, E> {
    observer: Arc<Box<dyn Observer<T, E> + Send + Sync>>,
    min_value: Arc<Mutex<Option<T>>>,
}

impl<T, E> Observer<T, E> for MinObserver<T, E>
where
    T: Send + Sync + Clone + Ord + 'static,
{
    fn on_next(&self, value: Result<T, E>) {
        match value {
            Ok(t) => {
                let mut min = self.min_value.lock().unwrap();
                match min.as_ref() {
                    Some(current) if &t < current => {
                        *min = Some(t);
                    }
                    None => {
                        *min = Some(t);
                    }
                    _ => {}
                }
            }
            Err(e) => {
                self.observer.on_next(Err(e));
            }
        }
    }

    fn on_completed(&self) {
        if let Some(v) = self.min_value.lock().unwrap().clone() {
            self.observer.on_next(Ok(v));
        }
        self.observer.on_completed();
    }
}

pub struct MaxObservable<Src, T, E> {
    source: Src,
    _marker: std::marker::PhantomData<(T, E)>,
}

impl<Src, T, E> MaxObservable<Src, T, E> {
    pub fn new(source: Src) -> Self {
        Self {
            source,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<Src, T, E> Observable<T, E> for MaxObservable<Src, T, E>
where
    Src: Observable<T, E>,
    T: Send + Sync + Clone + Ord + 'static,
    E: Send + Sync + 'static,
{
    fn subscribe(self, observer: impl Observer<T, E> + Send + Sync + 'static) -> Subscription {
        let observer: Arc<Box<dyn Observer<T, E> + Send + Sync>> = Arc::new(Box::new(observer));
        self.source.subscribe(MaxObserver {
            observer,
            max_value: Arc::new(Mutex::new(None)),
        })
    }
}

struct MaxObserver<T, E> {
    observer: Arc<Box<dyn Observer<T, E> + Send + Sync>>,
    max_value: Arc<Mutex<Option<T>>>,
}

impl<T, E> Observer<T, E> for MaxObserver<T, E>
where
    T: Send + Sync + Clone + Ord + 'static,
{
    fn on_next(&self, value: Result<T, E>) {
        match value {
            Ok(t) => {
                let mut max = self.max_value.lock().unwrap();
                match max.as_ref() {
                    Some(current) if &t > current => {
                        *max = Some(t);
                    }
                    None => {
                        *max = Some(t);
                    }
                    _ => {}
                }
            }
            Err(e) => {
                self.observer.on_next(Err(e));
            }
        }
    }

    fn on_completed(&self) {
        if let Some(v) = self.max_value.lock().unwrap().clone() {
            self.observer.on_next(Ok(v));
        }
        self.observer.on_completed();
    }
}

pub struct AverageObservable<Src, T, E> {
    source: Src,
    _marker: std::marker::PhantomData<(T, E)>,
}

impl<Src, T, E> AverageObservable<Src, T, E> {
    pub fn new(source: Src) -> Self {
        Self {
            source,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<Src, T, E> Observable<T, E> for AverageObservable<Src, T, E>
where
    Src: Observable<T, E>,
    T: Send + Sync + Clone + std::ops::Add<Output = T> + std::ops::Div<T, Output = T> + From<u8> + Default + 'static,
    E: Send + Sync + 'static,
{
    fn subscribe(self, observer: impl Observer<T, E> + Send + Sync + 'static) -> Subscription {
        let observer: Arc<Box<dyn Observer<T, E> + Send + Sync>> = Arc::new(Box::new(observer));
        self.source.subscribe(AverageObserver {
            observer,
            sum: Arc::new(Mutex::new(T::default())),
            count: Arc::new(Mutex::new(0usize)),
        })
    }
}

struct AverageObserver<T, E> {
    observer: Arc<Box<dyn Observer<T, E> + Send + Sync>>,
    sum: Arc<Mutex<T>>,
    count: Arc<Mutex<usize>>,
}

impl<T, E> Observer<T, E> for AverageObserver<T, E>
where
    T: Send + Sync + Clone + std::ops::Add<Output = T> + std::ops::Div<T, Output = T> + From<u8> + Default + 'static,
{
    fn on_next(&self, value: Result<T, E>) {
        match value {
            Ok(t) => {
                let mut s = self.sum.lock().unwrap();
                let new_sum = s.clone() + t;
                *s = new_sum;
                let mut c = self.count.lock().unwrap();
                *c += 1;
            }
            Err(e) => {
                self.observer.on_next(Err(e));
            }
        }
    }

    fn on_completed(&self) {
        let c = *self.count.lock().unwrap();
        if c > 0 {
            let s = self.sum.lock().unwrap().clone();
            let count_t = T::from(c as u8);
            let avg = s / count_t;
            self.observer.on_next(Ok(avg));
        }
        self.observer.on_completed();
    }
}

pub trait ObservableExtMath<T, E>: Observable<T, E> + Sized {
    fn reduce<Acc, F>(self, initial: Acc, f: F) -> ReduceObservable<Self, F, Acc, T, E>
    where
        F: Fn(Acc, T) -> Acc + Send + Sync + 'static,
        Acc: Send + Sync + Clone + 'static,
        T: Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        ReduceObservable::new(self, initial, f)
    }

    fn count(self) -> CountObservable<Self, T, E>
    where
        T: Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        CountObservable::new(self)
    }

    fn sum(self) -> SumObservable<Self, T, E>
    where
        T: Send + Sync + Clone + std::ops::Add<Output = T> + Default + 'static,
        E: Send + Sync + 'static,
    {
        SumObservable::new(self)
    }

    fn average(self) -> AverageObservable<Self, T, E>
    where
        T: Send + Sync + Clone + std::ops::Add<Output = T> + std::ops::Div<T, Output = T> + From<u8> + Default + 'static,
        E: Send + Sync + 'static,
    {
        AverageObservable::new(self)
    }

    fn min(self) -> MinObservable<Self, T, E>
    where
        T: Send + Sync + Clone + Ord + 'static,
        E: Send + Sync + 'static,
    {
        MinObservable::new(self)
    }

    fn max(self) -> MaxObservable<Self, T, E>
    where
        T: Send + Sync + Clone + Ord + 'static,
        E: Send + Sync + 'static,
    {
        MaxObservable::new(self)
    }
}

impl<O, T, E> ObservableExtMath<T, E> for O where O: Observable<T, E> + Sized {}


// ===================== default_if_empty =====================

pub struct DefaultIfEmptyObservable<Src, T, E> {
    source: Src,
    default: T,
    _marker: std::marker::PhantomData<E>,
}

impl<Src, T, E> DefaultIfEmptyObservable<Src, T, E> {
    pub fn new(source: Src, default: T) -> Self {
        Self {
            source,
            default,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<Src, T, E> Observable<T, E> for DefaultIfEmptyObservable<Src, T, E>
where
    Src: Observable<T, E>,
    T: Send + Sync + Clone + 'static,
    E: Send + Sync + 'static,
{
    fn subscribe(self, observer: impl Observer<T, E> + Send + Sync + 'static) -> Subscription {
        let observer: Arc<Box<dyn Observer<T, E> + Send + Sync>> = Arc::new(Box::new(observer));
        self.source.subscribe(DefaultIfEmptyObserver {
            observer,
            default: self.default,
            has_values: Arc::new(Mutex::new(false)),
        })
    }
}

struct DefaultIfEmptyObserver<T, E> {
    observer: Arc<Box<dyn Observer<T, E> + Send + Sync>>,
    default: T,
    has_values: Arc<Mutex<bool>>,
}

impl<T, E> Observer<T, E> for DefaultIfEmptyObserver<T, E>
where
    T: Send + Sync + Clone + 'static,
{
    fn on_next(&self, value: Result<T, E>) {
        match value {
            Ok(t) => {
                let mut has = self.has_values.lock().unwrap();
                *has = true;
                self.observer.on_next(Ok(t));
            }
            Err(e) => {
                self.observer.on_next(Err(e));
            }
        }
    }

    fn on_completed(&self) {
        let has = *self.has_values.lock().unwrap();
        if !has {
            self.observer.on_next(Ok(self.default.clone()));
        }
        self.observer.on_completed();
    }
}

// ===================== contains =====================

pub struct ContainsObservable<Src, T, E> {
    source: Src,
    target: T,
    _marker: std::marker::PhantomData<E>,
}

impl<Src, T, E> ContainsObservable<Src, T, E> {
    pub fn new(source: Src, target: T) -> Self {
        Self {
            source,
            target,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<Src, T, E> Observable<bool, E> for ContainsObservable<Src, T, E>
where
    Src: Observable<T, E>,
    T: Send + Sync + Clone + PartialEq + 'static,
    E: Send + Sync + 'static,
{
    fn subscribe(self, observer: impl Observer<bool, E> + Send + Sync + 'static) -> Subscription {
        let observer: Arc<Box<dyn Observer<bool, E> + Send + Sync>> = Arc::new(Box::new(observer));
        self.source.subscribe(ContainsObserver {
            observer,
            target: self.target,
            found: Arc::new(Mutex::new(false)),
        })
    }
}

struct ContainsObserver<T, E> {
    observer: Arc<Box<dyn Observer<bool, E> + Send + Sync>>,
    target: T,
    found: Arc<Mutex<bool>>,
}

impl<T, E> Observer<T, E> for ContainsObserver<T, E>
where
    T: Send + Sync + Clone + PartialEq + 'static,
{
    fn on_next(&self, value: Result<T, E>) {
        match value {
            Ok(t) => {
                if !*self.found.lock().unwrap() && t == self.target {
                    *self.found.lock().unwrap() = true;
                }
            }
            Err(e) => {
                self.observer.on_next(Err(e));
            }
        }
    }

    fn on_completed(&self) {
        let found = *self.found.lock().unwrap();
        self.observer.on_next(Ok(found));
        self.observer.on_completed();
    }
}

// ===================== all =====================

pub struct AllObservable<Src, P, T, E> {
    source: Src,
    predicate: P,
    _marker: std::marker::PhantomData<(T, E)>,
}

impl<Src, P, T, E> AllObservable<Src, P, T, E> {
    pub fn new(source: Src, predicate: P) -> Self {
        Self {
            source,
            predicate,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<Src, P, T, E> Observable<bool, E> for AllObservable<Src, P, T, E>
where
    Src: Observable<T, E>,
    P: Fn(&T) -> bool + Send + Sync + 'static,
    T: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    fn subscribe(self, observer: impl Observer<bool, E> + Send + Sync + 'static) -> Subscription {
        let observer: Arc<Box<dyn Observer<bool, E> + Send + Sync>> = Arc::new(Box::new(observer));
        self.source.subscribe(AllObserver {
            observer,
            predicate: self.predicate,
            all_true: Arc::new(Mutex::new(true)),
            has_values: Arc::new(Mutex::new(false)),
            _marker: std::marker::PhantomData,
        })
    }
}

struct AllObserver<P, T, E> {
    observer: Arc<Box<dyn Observer<bool, E> + Send + Sync>>,
    predicate: P,
    all_true: Arc<Mutex<bool>>,
    has_values: Arc<Mutex<bool>>,
    _marker: std::marker::PhantomData<T>,
}

impl<P, T, E> Observer<T, E> for AllObserver<P, T, E>
where
    P: Fn(&T) -> bool + Send + Sync + 'static,
    T: Send + Sync + 'static,
{
    fn on_next(&self, value: Result<T, E>) {
        match value {
            Ok(t) => {
                let mut has = self.has_values.lock().unwrap();
                *has = true;
                if !(self.predicate)(&t) {
                    let mut at = self.all_true.lock().unwrap();
                    *at = false;
                }
            }
            Err(e) => {
                self.observer.on_next(Err(e));
            }
        }
    }

    fn on_completed(&self) {
        let result = *self.all_true.lock().unwrap();
        self.observer.on_next(Ok(result));
        self.observer.on_completed();
    }
}

// ===================== debounce =====================
// 只在安静时间窗口后发出值。如果在延迟时间内有新值，则重置计时器。

pub struct DebounceObservable<Src, T, E> {
    source: Src,
    delay: std::time::Duration,
    _marker: std::marker::PhantomData<(T, E)>,
}

impl<Src, T, E> DebounceObservable<Src, T, E> {
    pub fn new(source: Src, delay: std::time::Duration) -> Self {
        Self { source, delay, _marker: std::marker::PhantomData }
    }
}

impl<Src, T, E> Observable<T, E> for DebounceObservable<Src, T, E>
where
    Src: Observable<T, E>,
    T: Send + Sync + Clone + 'static,
    E: Send + Sync + Clone + 'static,
{
    fn subscribe(self, observer: impl Observer<T, E> + Send + Sync + 'static) -> Subscription {
        let observer: Arc<Box<dyn Observer<T, E> + Send + Sync>> = Arc::new(Box::new(observer));
        self.source.subscribe(DebounceObserver::new(observer, self.delay))
    }
}

struct DebounceObserver<T, E> {
    observer: Arc<Box<dyn Observer<T, E> + Send + Sync>>,
    delay: std::time::Duration,
    latest_value: Arc<Mutex<Option<T>>>,
    latest_time: Arc<Mutex<std::time::Instant>>,
    stopped: Arc<Mutex<bool>>,
}

impl<T, E> DebounceObserver<T, E>
where
    T: Send + Sync + Clone + 'static,
    E: Send + Sync + Clone + 'static,
{
    fn new(observer: Arc<Box<dyn Observer<T, E> + Send + Sync>>, delay: std::time::Duration) -> Self {
        Self {
            observer,
            delay,
            latest_value: Arc::new(Mutex::new(None)),
            latest_time: Arc::new(Mutex::new(std::time::Instant::now())),
            stopped: Arc::new(Mutex::new(false)),
        }
    }
}

impl<T, E> Observer<T, E> for DebounceObserver<T, E>
where
    T: Send + Sync + Clone + 'static,
    E: Send + Sync + Clone + 'static,
{
    fn on_next(&self, value: Result<T, E>) {
        match value {
            Ok(t) => {
                *self.latest_time.lock().unwrap() = std::time::Instant::now();
                *self.latest_value.lock().unwrap() = Some(t);
                let observer = Arc::clone(&self.observer);
                let latest_value = Arc::clone(&self.latest_value);
                let latest_time = Arc::clone(&self.latest_time);
                let stopped = Arc::clone(&self.stopped);
                let delay = self.delay;
                std::thread::spawn(move || {
                    std::thread::sleep(delay);
                    if *stopped.lock().unwrap() { return; }
                    let now = std::time::Instant::now();
                    let emit_time = *latest_time.lock().unwrap();
                    if now.duration_since(emit_time) >= delay {
                        if let Some(v) = latest_value.lock().unwrap().take() {
                            observer.on_next(Ok(v));
                        }
                    }
                });
            }
            Err(e) => {
                self.observer.on_next(Err(e));
            }
        }
    }

    fn on_completed(&self) {
        *self.stopped.lock().unwrap() = true;
        if let Some(v) = self.latest_value.lock().unwrap().take() {
            self.observer.on_next(Ok(v));
        }
        self.observer.on_completed();
    }
}

// ===================== throttle =====================
// 在每个时间窗口内只发出第一个值，忽略后续值。

pub struct ThrottleObservable<Src, T, E> {
    source: Src,
    duration: std::time::Duration,
    _marker: std::marker::PhantomData<(T, E)>,
}

impl<Src, T, E> ThrottleObservable<Src, T, E> {
    pub fn new(source: Src, duration: std::time::Duration) -> Self {
        Self { source, duration, _marker: std::marker::PhantomData }
    }
}

impl<Src, T, E> Observable<T, E> for ThrottleObservable<Src, T, E>
where
    Src: Observable<T, E>,
    T: Send + Sync + Clone + 'static,
    E: Send + Sync + Clone + 'static,
{
    fn subscribe(self, observer: impl Observer<T, E> + Send + Sync + 'static) -> Subscription {
        let observer: Arc<Box<dyn Observer<T, E> + Send + Sync>> = Arc::new(Box::new(observer));
        self.source.subscribe(ThrottleObserver::new(observer, self.duration))
    }
}

struct ThrottleObserver<T, E> {
    observer: Arc<Box<dyn Observer<T, E> + Send + Sync>>,
    duration: std::time::Duration,
    last_emit: Arc<Mutex<Option<std::time::Instant>>>,
}

impl<T, E> ThrottleObserver<T, E>
where
    T: Send + Sync + Clone + 'static,
    E: Send + Sync + Clone + 'static,
{
    fn new(observer: Arc<Box<dyn Observer<T, E> + Send + Sync>>, duration: std::time::Duration) -> Self {
        Self {
            observer,
            duration,
            last_emit: Arc::new(Mutex::new(None)),
        }
    }
}

impl<T, E> Observer<T, E> for ThrottleObserver<T, E>
where
    T: Send + Sync + Clone + 'static,
    E: Send + Sync + Clone + 'static,
{
    fn on_next(&self, value: Result<T, E>) {
        match value {
            Ok(t) => {
                let mut last = self.last_emit.lock().unwrap();
                let now = std::time::Instant::now();
                match *last {
                    None => {
                        *last = Some(now);
                        drop(last);
                        self.observer.on_next(Ok(t));
                    }
                    Some(prev) => {
                        if now.duration_since(prev) >= self.duration {
                            *last = Some(now);
                            drop(last);
                            self.observer.on_next(Ok(t));
                        }
                    }
                }
            }
            Err(e) => {
                self.observer.on_next(Err(e));
            }
        }
    }

    fn on_completed(&self) {
        self.observer.on_completed();
    }
}

// ===================== observe_on =====================
// 在指定的调度器上发出值（下游调度）。

pub struct ObserveOnObservable<Src, S, T, E> {
    source: Src,
    scheduler: S,
    _marker: std::marker::PhantomData<(T, E)>,
}

impl<Src, S, T, E> ObserveOnObservable<Src, S, T, E> {
    pub fn new(source: Src, scheduler: S) -> Self {
        Self { source, scheduler, _marker: std::marker::PhantomData }
    }
}

impl<Src, S, T, E> Observable<T, E> for ObserveOnObservable<Src, S, T, E>
where
    Src: Observable<T, E>,
    S: crate::scheduler::Scheduler + Send + Sync + 'static,
    T: Send + Sync + Clone + 'static,
    E: Send + Sync + Clone + 'static,
{
    fn subscribe(self, observer: impl Observer<T, E> + Send + Sync + 'static) -> Subscription {
        let observer: Arc<Box<dyn Observer<T, E> + Send + Sync>> = Arc::new(Box::new(observer));
        let scheduler: Arc<S> = Arc::new(self.scheduler);
        self.source.subscribe(ObserveOnObserver { observer, scheduler })
    }
}

struct ObserveOnObserver<S, T, E> {
    observer: Arc<Box<dyn Observer<T, E> + Send + Sync>>,
    scheduler: Arc<S>,
}

impl<S, T, E> Observer<T, E> for ObserveOnObserver<S, T, E>
where
    S: crate::scheduler::Scheduler + Send + Sync + 'static,
    T: Send + Sync + Clone + 'static,
    E: Send + Sync + Clone + 'static,
{
    fn on_next(&self, value: Result<T, E>) {
        let observer = Arc::clone(&self.observer);
        let scheduler = Arc::clone(&self.scheduler);
        scheduler.schedule(move || {
            observer.on_next(value);
        });
    }

    fn on_completed(&self) {
        let observer = Arc::clone(&self.observer);
        let scheduler = Arc::clone(&self.scheduler);
        scheduler.schedule(move || {
            observer.on_completed();
        });
    }
}

// ===================== subscribe_on =====================
// 在指定的调度器上订阅（上游调度）。

pub struct SubscribeOnObservable<Src, S, T, E> {
    source: Src,
    scheduler: S,
    _marker: std::marker::PhantomData<(T, E)>,
}

impl<Src, S, T, E> SubscribeOnObservable<Src, S, T, E> {
    pub fn new(source: Src, scheduler: S) -> Self {
        Self { source, scheduler, _marker: std::marker::PhantomData }
    }
}

impl<Src, S, T, E> Observable<T, E> for SubscribeOnObservable<Src, S, T, E>
where
    Src: Observable<T, E> + Send + Sync + 'static,
    S: crate::scheduler::Scheduler + Send + Sync + 'static,
    T: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    fn subscribe(self, observer: impl Observer<T, E> + Send + Sync + 'static) -> Subscription {
        let observer: Arc<Box<dyn Observer<T, E> + Send + Sync>> = Arc::new(Box::new(observer));
        let source = self.source;
        let subscription_arc: Arc<Mutex<Option<Subscription>>> = Arc::new(Mutex::new(None));
        let sub_for_cleanup = Arc::clone(&subscription_arc);
        let scheduler: Arc<S> = Arc::new(self.scheduler);

        scheduler.schedule(move || {
            let sub = source.subscribe(SubscribeOnObserver { observer });
            *sub_for_cleanup.lock().unwrap() = Some(sub);
        });

        Subscription::from_fn(move || {
            if let Some(s) = subscription_arc.lock().unwrap().take() {
                s.dispose();
            }
        })
    }
}

struct SubscribeOnObserver<T, E> {
    observer: Arc<Box<dyn Observer<T, E> + Send + Sync>>,
}

impl<T, E> Observer<T, E> for SubscribeOnObserver<T, E>
where
    T: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    fn on_next(&self, value: Result<T, E>) {
        self.observer.on_next(value);
    }

    fn on_completed(&self) {
        self.observer.on_completed();
    }
}

// ===================== switch_map =====================
// 每次源 Observable 发出值时，切换到新的 inner Observable，
// 并取消订阅之前的 inner Observable。

pub struct SwitchMapObservable<Src, F, U, T, E> {
    source: Src,
    f: F,
    _marker: std::marker::PhantomData<(T, U, E)>,
}

impl<Src, F, U, T, E> SwitchMapObservable<Src, F, U, T, E> {
    pub fn new(source: Src, f: F) -> Self {
        Self { source, f, _marker: std::marker::PhantomData }
    }
}

impl<Src, F, Obs, U, T, E> Observable<U, E> for SwitchMapObservable<Src, F, U, T, E>
where
    Src: Observable<T, E>,
    F: Fn(T) -> Obs + Send + Sync + 'static,
    Obs: Observable<U, E> + Send + Sync + 'static,
    U: Send + Sync + 'static,
    T: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    fn subscribe(self, observer: impl Observer<U, E> + Send + Sync + 'static) -> Subscription {
        let observer: Arc<Box<dyn Observer<U, E> + Send + Sync>> = Arc::new(Box::new(observer));
        let inner_sub = Arc::new(Mutex::new(Option::<Subscription>::None));
        let source_sub = Arc::new(Mutex::new(Option::<Subscription>::None));
        let outer_completed = Arc::new(Mutex::new(false));

        let outer_observer = SwitchMapOuterObserver {
            observer: Arc::clone(&observer),
            f: self.f,
            inner_sub: Arc::clone(&inner_sub),
            outer_completed: Arc::clone(&outer_completed),
            _marker: std::marker::PhantomData::<T>,
        };

        let sub = self.source.subscribe(outer_observer);
        *source_sub.lock().unwrap() = Some(sub);

        Subscription::from_fn(move || {
            if let Some(s) = inner_sub.lock().unwrap().take() {
                s.dispose();
            }
            if let Some(s) = source_sub.lock().unwrap().take() {
                s.dispose();
            }
        })
    }
}

struct SwitchMapOuterObserver<F, T, U, E> {
    observer: Arc<Box<dyn Observer<U, E> + Send + Sync>>,
    f: F,
    inner_sub: Arc<Mutex<Option<Subscription>>>,
    outer_completed: Arc<Mutex<bool>>,
    _marker: std::marker::PhantomData<T>,
}

impl<F, Obs, U, T, E> Observer<T, E> for SwitchMapOuterObserver<F, T, U, E>
where
    F: Fn(T) -> Obs + Send + Sync + 'static,
    Obs: Observable<U, E> + Send + Sync + 'static,
    U: Send + Sync + 'static,
    T: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    fn on_next(&self, value: Result<T, E>) {
        match value {
            Ok(t) => {
                if let Some(old_sub) = self.inner_sub.lock().unwrap().take() {
                    old_sub.dispose();
                }
                let inner_obs = (self.f)(t);
                let inner_observer = SwitchMapInnerObserver {
                    observer: Arc::clone(&self.observer),
                    _marker: std::marker::PhantomData::<T>,
                };
                let sub = inner_obs.subscribe(inner_observer);
                *self.inner_sub.lock().unwrap() = Some(sub);
            }
            Err(e) => self.observer.on_next(Err(e)),
        }
    }

    fn on_completed(&self) {
        *self.outer_completed.lock().unwrap() = true;
        if self.inner_sub.lock().unwrap().is_none() {
            self.observer.on_completed();
        }
    }
}

struct SwitchMapInnerObserver<T, U, E> {
    observer: Arc<Box<dyn Observer<U, E> + Send + Sync>>,
    _marker: std::marker::PhantomData<T>,
}

impl<T, U, E> Observer<U, E> for SwitchMapInnerObserver<T, U, E>
where
    U: Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    fn on_next(&self, value: Result<U, E>) {
        self.observer.on_next(value);
    }

    fn on_completed(&self) {}
}

// ===================== retry =====================
// 在失败时重新订阅源 Observable，最多重试 count 次。

pub struct RetryObservable<Src, T, E> {
    source: Src,
    count: usize,
    _marker: std::marker::PhantomData<(T, E)>,
}

impl<Src, T, E> RetryObservable<Src, T, E> {
    pub fn new(source: Src, count: usize) -> Self {
        Self { source, count, _marker: std::marker::PhantomData }
    }
}

impl<Src, T, E> Observable<T, E> for RetryObservable<Src, T, E>
where
    Src: Observable<T, E> + Clone + Send + Sync + 'static,
    T: Send + Sync + 'static,
    E: Send + Sync + Clone + 'static,
{
    fn subscribe(self, observer: impl Observer<T, E> + Send + Sync + 'static) -> Subscription {
        let observer: Arc<Box<dyn Observer<T, E> + Send + Sync>> = Arc::new(Box::new(observer));
        let remaining = Arc::new(Mutex::new(self.count));
        let source = Arc::new(Mutex::new(self.source));
        let composite: Arc<Mutex<Option<Subscription>>> = Arc::new(Mutex::new(None));
        let composite_for_cleanup = Arc::clone(&composite);

        let retry_observer = RetryObserver {
            observer: Arc::clone(&observer),
            remaining: Arc::clone(&remaining),
            source: Arc::clone(&source),
            composite: Arc::clone(&composite),
        };

        // 在订阅前克隆 source 并释放锁，避免死锁
        let source_clone = source.lock().unwrap().clone();
        let initial_sub = source_clone.subscribe(retry_observer);
        *composite.lock().unwrap() = Some(initial_sub);

        Subscription::from_fn(move || {
            if let Some(s) = composite_for_cleanup.lock().unwrap().take() {
                s.dispose();
            }
        })
    }
}

struct RetryObserver<Src, T, E> {
    observer: Arc<Box<dyn Observer<T, E> + Send + Sync>>,
    remaining: Arc<Mutex<usize>>,
    source: Arc<Mutex<Src>>,
    composite: Arc<Mutex<Option<Subscription>>>,
}

impl<Src, T, E> Observer<T, E> for RetryObserver<Src, T, E>
where
    Src: Observable<T, E> + Clone + Send + Sync + 'static,
    T: Send + Sync + 'static,
    E: Send + Sync + Clone + 'static,
{
    fn on_next(&self, value: Result<T, E>) {
        match value {
            Ok(t) => self.observer.on_next(Ok(t)),
            Err(e) => {
                let mut rem = self.remaining.lock().unwrap();
                if *rem > 0 {
                    *rem -= 1;
                    drop(rem);
                    if let Some(old_sub) = self.composite.lock().unwrap().take() {
                        old_sub.dispose();
                    }
                    let retry_observer = RetryObserver {
                        observer: Arc::clone(&self.observer),
                        remaining: Arc::clone(&self.remaining),
                        source: Arc::clone(&self.source),
                        composite: Arc::clone(&self.composite),
                    };
                    // 在订阅前克隆 source 并释放锁，避免死锁
                    let source_clone = self.source.lock().unwrap().clone();
                    let new_sub = source_clone.subscribe(retry_observer);
                    *self.composite.lock().unwrap() = Some(new_sub);
                } else {
                    self.observer.on_next(Err(e));
                    self.observer.on_completed();
                }
            }
        }
    }

    fn on_completed(&self) {
        self.observer.on_completed();
    }
}

// ===================== retry_when =====================
// 使用自定义通知者决定是否继续重试。

pub struct RetryWhenObservable<Src, F, Notifier, T, E> {
    source: Src,
    notifier_factory: F,
    _marker: std::marker::PhantomData<(T, E, Notifier)>,
}

impl<Src, F, Notifier, T, E> RetryWhenObservable<Src, F, Notifier, T, E> {
    pub fn new(source: Src, notifier_factory: F) -> Self {
        Self { source, notifier_factory, _marker: std::marker::PhantomData }
    }
}

impl<Src, F, Notifier, T, E> Observable<T, E> for RetryWhenObservable<Src, F, Notifier, T, E>
where
    Src: Observable<T, E> + Clone + Send + Sync + 'static,
    F: Fn(Arc<Mutex<Option<E>>>) -> Notifier + Send + Sync + 'static,
    Notifier: Observable<(), E> + Send + Sync + 'static,
    T: Send + Sync + 'static,
    E: Send + Sync + Clone + 'static,
{
    fn subscribe(self, observer: impl Observer<T, E> + Send + Sync + 'static) -> Subscription {
        let observer: Arc<Box<dyn Observer<T, E> + Send + Sync>> = Arc::new(Box::new(observer));
        let source = Arc::new(Mutex::new(self.source));
        let notifier_factory = Arc::new(Mutex::new(self.notifier_factory));
        let error_sub: Arc<Mutex<Option<Subscription>>> = Arc::new(Mutex::new(None));
        let main_sub: Arc<Mutex<Option<Subscription>>> = Arc::new(Mutex::new(None));
        let stopped = Arc::new(Mutex::new(false));

        let error_sender = Arc::new(Mutex::new(None::<E>));

        let retry_when_observer = RetryWhenObserver {
            observer: Arc::clone(&observer),
            source: Arc::clone(&source),
            notifier_factory: Arc::clone(&notifier_factory),
            error_sender: Arc::clone(&error_sender),
            error_sub: Arc::clone(&error_sub),
            main_sub: Arc::clone(&main_sub),
            stopped: Arc::clone(&stopped),
            _marker: std::marker::PhantomData::<(T, E, Notifier)>,
        };

        // 在订阅前克隆 source 并释放锁，避免死锁
        let source_clone = source.lock().unwrap().clone();
        let sub = source_clone.subscribe(retry_when_observer);
        *main_sub.lock().unwrap() = Some(sub);

        Subscription::from_fn(move || {
            if let Some(s) = error_sub.lock().unwrap().take() {
                s.dispose();
            }
            if let Some(s) = main_sub.lock().unwrap().take() {
                s.dispose();
            }
        })
    }
}

struct RetryWhenObserver<Src, F, Notifier, T, E> {
    observer: Arc<Box<dyn Observer<T, E> + Send + Sync>>,
    source: Arc<Mutex<Src>>,
    notifier_factory: Arc<Mutex<F>>,
    error_sender: Arc<Mutex<Option<E>>>,
    error_sub: Arc<Mutex<Option<Subscription>>>,
    main_sub: Arc<Mutex<Option<Subscription>>>,
    stopped: Arc<Mutex<bool>>,
    _marker: std::marker::PhantomData<(T, E, Notifier)>,
}

impl<Src, F, Notifier, T, E> Observer<T, E> for RetryWhenObserver<Src, F, Notifier, T, E>
where
    Src: Observable<T, E> + Clone + Send + Sync + 'static,
    F: Fn(Arc<Mutex<Option<E>>>) -> Notifier + Send + Sync + 'static,
    Notifier: Observable<(), E> + Send + Sync + 'static,
    T: Send + Sync + 'static,
    E: Send + Sync + Clone + 'static,
{
    fn on_next(&self, value: Result<T, E>) {
        match value {
            Ok(t) => self.observer.on_next(Ok(t)),
            Err(e) => {
                if *self.stopped.lock().unwrap() {
                    return;
                }
                *self.error_sender.lock().unwrap() = Some(e);
                let notifier = (self.notifier_factory.lock().unwrap())(Arc::clone(&self.error_sender));
                let retry_observer = RetryWhenInnerObserver {
                    observer: Arc::clone(&self.observer),
                    source: Arc::clone(&self.source),
                    notifier_factory: Arc::clone(&self.notifier_factory),
                    error_sender: Arc::clone(&self.error_sender),
                    error_sub: Arc::clone(&self.error_sub),
                    main_sub: Arc::clone(&self.main_sub),
                    stopped: Arc::clone(&self.stopped),
                    _marker: std::marker::PhantomData::<(T, E, Notifier)>,
                };

                // 订阅通知者：通知者发信号就重新订阅源。
                let notifier_sub = notifier.subscribe(retry_observer);
                *self.error_sub.lock().unwrap() = Some(notifier_sub);
            }
        }
    }

    fn on_completed(&self) {
        self.observer.on_completed();
    }
}

struct RetryWhenInnerObserver<Src, F, Notifier, T, E> {
    observer: Arc<Box<dyn Observer<T, E> + Send + Sync>>,
    source: Arc<Mutex<Src>>,
    notifier_factory: Arc<Mutex<F>>,
    error_sender: Arc<Mutex<Option<E>>>,
    error_sub: Arc<Mutex<Option<Subscription>>>,
    main_sub: Arc<Mutex<Option<Subscription>>>,
    stopped: Arc<Mutex<bool>>,
    _marker: std::marker::PhantomData<(T, E, Notifier)>,
}

impl<Src, F, Notifier, T, E> Observer<(), E> for RetryWhenInnerObserver<Src, F, Notifier, T, E>
where
    Src: Observable<T, E> + Clone + Send + Sync + 'static,
    F: Fn(Arc<Mutex<Option<E>>>) -> Notifier + Send + Sync + 'static,
    Notifier: Observable<(), E> + Send + Sync + 'static,
    T: Send + Sync + 'static,
    E: Send + Sync + Clone + 'static,
{
    fn on_next(&self, value: Result<(), E>) {
        // 通知者发信号：重新订阅源
        if *self.stopped.lock().unwrap() {
            return;
        }
        match value {
            Ok(()) => {
                // 取消旧订阅
                if let Some(s) = self.main_sub.lock().unwrap().take() {
                    s.dispose();
                }
                // 重新订阅源
                let retry_when_observer = RetryWhenObserver {
                    observer: Arc::clone(&self.observer),
                    source: Arc::clone(&self.source),
                    notifier_factory: Arc::clone(&self.notifier_factory),
                    error_sender: Arc::clone(&self.error_sender),
                    error_sub: Arc::clone(&self.error_sub),
                    main_sub: Arc::clone(&self.main_sub),
                    stopped: Arc::clone(&self.stopped),
                    _marker: std::marker::PhantomData::<(T, E, Notifier)>,
                };
                // 在订阅前克隆 source 并释放锁，避免死锁
                let source_clone = self.source.lock().unwrap().clone();
                let new_sub = source_clone.subscribe(retry_when_observer);
                *self.main_sub.lock().unwrap() = Some(new_sub);
            }
            Err(e) => {
                *self.stopped.lock().unwrap() = true;
                self.observer.on_next(Err(e));
                self.observer.on_completed();
            }
        }
    }

    fn on_completed(&self) {
        // 通知者完成：不再重试
        *self.stopped.lock().unwrap() = true;
        self.observer.on_completed();
    }
}