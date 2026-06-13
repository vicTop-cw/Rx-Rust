use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

pub trait Scheduler {
    fn schedule<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static;
    
    fn schedule_with_delay<F>(&self, delay: Duration, f: F)
    where
        F: FnOnce() + Send + 'static;
}

pub struct CurrentThreadScheduler;

impl Scheduler for CurrentThreadScheduler {
    fn schedule<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        f()
    }

    fn schedule_with_delay<F>(&self, delay: Duration, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        thread::sleep(delay);
        f()
    }
}

impl CurrentThreadScheduler {
    pub fn new() -> Self {
        CurrentThreadScheduler
    }
}

pub struct ThreadPoolScheduler {
    workers: usize,
    queue: Arc<Mutex<Vec<Box<dyn FnOnce() + Send + 'static>>>>,
    #[allow(dead_code)]
    handles: Vec<thread::JoinHandle<()>>,
}

impl ThreadPoolScheduler {
    pub fn new(workers: usize) -> Self {
        let queue: Arc<Mutex<Vec<Box<dyn FnOnce() + Send + 'static>>>> = Arc::new(Mutex::new(Vec::new()));
        let mut handles = Vec::with_capacity(workers);
        
        for _ in 0..workers {
            let queue_clone = Arc::clone(&queue);
            let handle = thread::spawn(move || {
                loop {
                    let task = {
                        let mut queue = queue_clone.lock().unwrap();
                        queue.pop()
                    };
                    
                    if let Some(task) = task {
                        task();
                    } else {
                        thread::sleep(Duration::from_millis(1));
                    }
                }
            });
            handles.push(handle);
        }
        
        Self {
            workers,
            queue,
            handles,
        }
    }
}

impl Scheduler for ThreadPoolScheduler {
    fn schedule<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.queue.lock().unwrap().push(Box::new(f));
    }

    fn schedule_with_delay<F>(&self, delay: Duration, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let queue = Arc::clone(&self.queue);
        thread::spawn(move || {
            thread::sleep(delay);
            queue.lock().unwrap().push(Box::new(f));
        });
    }
}

impl Drop for ThreadPoolScheduler {
    fn drop(&mut self) {
        for _ in 0..self.workers {
            self.queue.lock().unwrap().push(Box::new(|| {}));
        }
    }
}

pub struct AsyncScheduler;

impl Scheduler for AsyncScheduler {
    fn schedule<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        thread::spawn(f);
    }

    fn schedule_with_delay<F>(&self, delay: Duration, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        thread::spawn(move || {
            thread::sleep(delay);
            f();
        });
    }
}

impl AsyncScheduler {
    pub fn new() -> Self {
        AsyncScheduler
    }
}

pub struct ImmediateScheduler;

impl Scheduler for ImmediateScheduler {
    fn schedule<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        f()
    }

    fn schedule_with_delay<F>(&self, delay: Duration, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        thread::sleep(delay);
        f()
    }
}

impl ImmediateScheduler {
    pub fn new() -> Self {
        ImmediateScheduler
    }
}

pub mod prelude {
    pub use super::{Scheduler, CurrentThreadScheduler, ThreadPoolScheduler, AsyncScheduler, ImmediateScheduler};
}