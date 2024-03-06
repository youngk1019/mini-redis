use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Notify;

#[derive(Clone)]
pub struct Synchronization {
    shard: Arc<Shard>,
}

struct Shard {
    need_finish: u64,
    have_finish: AtomicU64,
    timeout: std::time::Duration,
    notify: Notify,
}

impl Synchronization {
    pub fn new(timeout: std::time::Duration, need_finish: u64) -> Self {
        Synchronization {
            shard: Arc::new(Shard {
                need_finish,
                have_finish: AtomicU64::new(0),
                timeout,
                notify: Notify::new(),
            })
        }
    }

    pub fn finish(&self) {
        let have_finish = self.shard.have_finish.fetch_add(1, Ordering::Relaxed);
        if have_finish + 1 == self.shard.need_finish {
            self.shard.notify.notify_waiters();
        }
    }

    pub fn have_finish(&self) -> u64 {
        self.shard.have_finish.load(Ordering::Relaxed)
    }

    pub async fn wait(&self) {
        let timeout_wait = async {
            if self.shard.have_finish.load(Ordering::Relaxed) >= self.shard.need_finish {
                return;
            } else {
                tokio::time::sleep(self.shard.timeout).await;
            }
        };
        tokio::select! {
            _ = self.shard.notify.notified() => {}
            _ = timeout_wait => {}
        }
    }
}