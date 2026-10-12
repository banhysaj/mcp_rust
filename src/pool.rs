//! A fixed-size worker thread pool.

use std::collections::VecDeque;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};

type Job = Box<dyn FnOnce() + Send + 'static>;

struct Queue {
    jobs: VecDeque<Job>,
    stop: bool,
}

struct Inner {
    queue: Mutex<Queue>,
    cv: Condvar,
}

/// A pool of worker threads that runs submitted closures in submission order per
/// worker.
///
/// Dropping the pool stops the workers and joins them. Jobs already queued when the
/// pool is dropped still run, so nothing submitted is lost; once the drop returns,
/// no job is still executing.
pub(crate) struct ThreadPool {
    inner: Arc<Inner>,
    threads: Mutex<Vec<JoinHandle<()>>>,
}

impl ThreadPool {
    /// Start `workers` threads. A count below one is raised to one.
    pub(crate) fn new(workers: usize) -> Self {
        let workers = workers.max(1);
        let inner = Arc::new(Inner {
            queue: Mutex::new(Queue {
                jobs: VecDeque::new(),
                stop: false,
            }),
            cv: Condvar::new(),
        });

        let mut threads = Vec::with_capacity(workers);
        for i in 0..workers {
            let inner = Arc::clone(&inner);
            let handle = thread::Builder::new()
                .name(format!("mcp-worker-{i}"))
                .spawn(move || worker_loop(inner))
                .expect("failed to spawn worker thread");
            threads.push(handle);
        }

        ThreadPool {
            inner,
            threads: Mutex::new(threads),
        }
    }

    /// Queue `job` for a worker thread.
    pub(crate) fn submit<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        {
            let mut q = self.inner.queue.lock().unwrap();
            q.jobs.push_back(Box::new(job));
        }
        self.inner.cv.notify_one();
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        {
            let mut q = self.inner.queue.lock().unwrap();
            q.stop = true;
        }
        self.inner.cv.notify_all();
        let mut threads = self.threads.lock().unwrap();
        for t in threads.drain(..) {
            let _ = t.join();
        }
    }
}

fn worker_loop(inner: Arc<Inner>) {
    loop {
        let job = {
            let mut q = inner.queue.lock().unwrap();
            loop {
                // Stop only once the queue has drained.
                if q.stop && q.jobs.is_empty() {
                    return;
                }
                if let Some(job) = q.jobs.pop_front() {
                    break job;
                }
                q = inner.cv.wait(q).unwrap();
            }
        };
        // Contain panics here so that one bad job cannot take a worker out of the
        // pool. Handlers that panics are turned into tool errors before they reach
        // this point; this is the backstop.
        let _ = catch_unwind(AssertUnwindSafe(job));
    }
}

/// The default worker count: one per hardware thread, or 4 when the platform does
/// not report a count.
pub(crate) fn default_worker_count() -> usize {
    match thread::available_parallelism() {
        Ok(n) if n.get() > 0 => n.get(),
        _ => 4,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::time::Duration;

    #[test]
    fn runs_every_job() {
        let pool = ThreadPool::new(4);
        let (tx, rx) = mpsc::channel();
        let done = Arc::new(AtomicUsize::new(0));
        for i in 0..64 {
            let tx = tx.clone();
            let done = Arc::clone(&done);
            pool.submit(move || {
                done.fetch_add(1, Ordering::SeqCst);
                tx.send(i).unwrap();
            });
        }
        drop(tx);
        assert_eq!(rx.iter().count(), 64);
        assert_eq!(done.load(Ordering::SeqCst), 64);
    }

    #[test]
    fn drop_drains_the_queue() {
        let ran = Arc::new(AtomicUsize::new(0));
        {
            let pool = ThreadPool::new(2);
            for _ in 0..32 {
                let ran = Arc::clone(&ran);
                pool.submit(move || {
                    thread::sleep(Duration::from_millis(1));
                    ran.fetch_add(1, Ordering::SeqCst);
                });
            }
        } // the drop joins, so nothing may still be running here
        assert_eq!(ran.load(Ordering::SeqCst), 32);
    }

    #[test]
    fn a_panicking_job_does_not_kill_the_pool() {
        let pool = ThreadPool::new(1);
        pool.submit(|| panic!("boom"));
        let (tx, rx) = mpsc::channel();
        pool.submit(move || tx.send(7).unwrap());
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(5)).unwrap(),
            7,
            "the single worker must survive the panic"
        );
    }

    #[test]
    fn zero_workers_is_raised_to_one() {
        let pool = ThreadPool::new(0);
        let (tx, rx) = mpsc::channel();
        pool.submit(move || tx.send(()).unwrap());
        rx.recv_timeout(Duration::from_secs(5)).unwrap();
    }
}
