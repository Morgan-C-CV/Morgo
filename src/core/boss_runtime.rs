use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock as StdRwLock};

use crate::core::boss::BossCoordinator;
use crate::core::boss_state::{BossControlRequest, BossControlResponse};
use crate::interaction::dispatcher::NotificationDispatcher;
use crate::task::manager::TaskManager;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug)]
pub struct BossControlRuntime {
    tx: mpsc::Sender<ControlEnvelope>,
}

#[derive(Debug)]
struct ControlEnvelope {
    request: BossControlRequest,
    tasks: Arc<TaskManager>,
    dispatcher: NotificationDispatcher,
    respond_to: oneshot::Sender<anyhow::Result<BossControlResponse>>,
}

#[derive(Debug, Default)]
pub struct BossRuntimeRegistry {
    runtimes: StdRwLock<HashMap<String, Arc<BossControlRuntime>>>,
}

impl BossRuntimeRegistry {
    pub fn global() -> &'static Self {
        static REGISTRY: OnceLock<BossRuntimeRegistry> = OnceLock::new();
        REGISTRY.get_or_init(BossRuntimeRegistry::default)
    }

    pub fn bind(&self, key: String, runtime: Arc<BossControlRuntime>) {
        self.runtimes
            .write()
            .expect("boss runtime registry poisoned")
            .insert(key, runtime);
    }

    pub fn get(&self, key: &str) -> Option<Arc<BossControlRuntime>> {
        self.runtimes
            .read()
            .expect("boss runtime registry poisoned")
            .get(key)
            .cloned()
    }

    pub fn unbind(&self, key: &str) {
        self.runtimes
            .write()
            .expect("boss runtime registry poisoned")
            .remove(key);
    }
}

impl BossControlRuntime {
    pub fn spawn(coordinator: BossCoordinator) -> Arc<Self> {
        let (tx, mut rx) = mpsc::channel::<ControlEnvelope>(16);
        let runtime = Arc::new(Self { tx });
        tokio::spawn(async move {
            while let Some(envelope) = rx.recv().await {
                let response = coordinator
                    .handle_control_request_direct(
                        envelope.request,
                        &envelope.tasks,
                        &envelope.dispatcher,
                    )
                    .await;
                let _ = envelope.respond_to.send(response);
            }
        });
        runtime
    }

    pub async fn request(
        &self,
        request: BossControlRequest,
        tasks: Arc<TaskManager>,
        dispatcher: NotificationDispatcher,
    ) -> anyhow::Result<BossControlResponse> {
        let (respond_to, rx) = oneshot::channel();
        self.tx
            .send(ControlEnvelope {
                request,
                tasks,
                dispatcher,
                respond_to,
            })
            .await
            .map_err(|_| anyhow::anyhow!("boss control mailbox send failed"))?;
        rx.await
            .map_err(|_| anyhow::anyhow!("boss control mailbox receive failed"))?
    }
}
