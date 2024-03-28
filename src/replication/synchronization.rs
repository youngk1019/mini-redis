use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use crate::utils::sync::Notifier;

#[derive(Clone)]
pub struct Synchronization {
    shard: Arc<Shard>,
    notifier: Notifier,
}

struct Shard {
    need_finish: u64,
    have_finish: AtomicU64,
    timeout: Option<Duration>,
}

impl Synchronization {
    pub fn new(timeout: Option<Duration>, need_finish: u64) -> Self {
        let notifier = Notifier::new();
        if need_finish == 0 {
            notifier.notify_all();
        }
        Synchronization {
            shard: Arc::new(Shard {
                need_finish,
                have_finish: AtomicU64::new(0),
                timeout,

            }),
            notifier,
        }
    }

    pub fn finish(&self) {
        let have_finish = self.shard.have_finish.fetch_add(1, Ordering::Relaxed);
        if have_finish + 1 >= self.shard.need_finish {
            self.notifier.notify_all();
        }
    }

    pub fn have_finish(&self) -> u64 {
        self.shard.have_finish.load(Ordering::Relaxed)
    }

    pub fn timeout(&self) -> Option<Duration> {
        self.shard.timeout
    }

    pub async fn wait(&mut self) {
        let timeout_wait = async {
            match self.shard.timeout {
                Some(timeout) => tokio::time::sleep(timeout).await,
                None => {
                    loop {
                        tokio::time::sleep(Duration::from_secs(u64::MAX)).await;
                    }
                }
            }
        };
        tokio::select! {
            _ = self.notifier.wait() => {}
            _ = timeout_wait => {}
        }
    }
}