use std::sync::Arc;
use tokio::sync::watch;

#[derive(Clone)]
pub struct Notifier {
    sender: Arc<watch::Sender<bool>>,
    receiver: watch::Receiver<bool>,
}

impl Notifier {
    pub fn new() -> Self {
        let (sender, receiver) = watch::channel(false);
        Notifier {
            sender: Arc::new(sender),
            receiver,
        }
    }

    pub async fn wait(&mut self) {
        while !*self.receiver.borrow() {
            if self.receiver.changed().await.is_err() {
                break;
            }
        }
    }

    pub fn notify_all(&self) {
        let _ = self.sender.send(true);
    }
}

#[tokio::test]
async fn test_notifier() {
    let mut notifier = Notifier::new();
    let mut waiter1 = notifier.clone();
    let handle1 = tokio::spawn(async move {
        waiter1.wait().await;
    });
    let mut waiter2 = notifier.clone();
    let handle2 = tokio::spawn(async move {
        waiter2.wait().await;
    });
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    notifier.notify_all();
    handle1.await.unwrap();
    handle2.await.unwrap();
    notifier.wait().await;
    let mut waiter = notifier.clone();
    let handle = tokio::spawn(async move {
        waiter.wait().await;
    });
    handle.await.unwrap();
}
