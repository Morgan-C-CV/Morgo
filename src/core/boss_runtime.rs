use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, RwLock as StdRwLock};

use crate::core::boss::BossCoordinator;
use crate::core::boss_state::{BossControlRequest, BossControlResponse};
use crate::interaction::dispatcher::NotificationDispatcher;
use crate::task::manager::TaskManager;
use tokio::sync::{mpsc, oneshot};
use tokio::task::AbortHandle;

#[derive(Debug)]
pub struct BossControlRuntime {
    tx: mpsc::Sender<ControlEnvelope>,
    abort_handle: AbortHandle,
    closed: AtomicBool,
}

#[derive(Debug)]
struct ControlEnvelope {
    request: BossControlRequest,
    tasks: Arc<TaskManager>,
    dispatcher: NotificationDispatcher,
    respond_to: oneshot::Sender<anyhow::Result<BossControlResponse>>,
}

#[derive(Debug, Default)]
struct BossRuntimeRegistry {
    runtimes: StdRwLock<HashMap<String, Arc<BossControlRuntime>>>,
}

#[derive(Debug, Default)]
pub struct BossRuntimeOwner {
    registry: BossRuntimeRegistry,
}

impl BossRuntimeOwner {
    pub fn global() -> Arc<Self> {
        static OWNER: OnceLock<Arc<BossRuntimeOwner>> = OnceLock::new();
        OWNER.get_or_init(|| Arc::new(BossRuntimeOwner::default()))
            .clone()
    }

    pub fn bind_runtime(&self, key: String, runtime: Arc<BossControlRuntime>) {
        self.registry
            .runtimes
            .write()
            .expect("boss runtime registry poisoned")
            .insert(key, runtime);
    }

    pub fn get_runtime(&self, key: &str) -> Option<Arc<BossControlRuntime>> {
        self.registry
            .runtimes
            .read()
            .expect("boss runtime registry poisoned")
            .get(key)
            .cloned()
    }

    pub fn shutdown_runtime(&self, key: &str) -> Option<Arc<BossControlRuntime>> {
        let runtime = self
            .registry
            .runtimes
            .write()
            .expect("boss runtime registry poisoned")
            .remove(key);
        if let Some(runtime) = &runtime {
            runtime.shutdown();
        }
        runtime
    }

    pub fn shutdown_all(&self) {
        let runtimes = self
            .registry
            .runtimes
            .write()
            .expect("boss runtime registry poisoned")
            .drain()
            .map(|(_, runtime)| runtime)
            .collect::<Vec<_>>();
        for runtime in runtimes {
            runtime.shutdown();
        }
    }

    pub fn fresh_runtime_key(&self, plan_id: &str) -> String {
        static NEXT_RUNTIME_ID: AtomicU64 = AtomicU64::new(1);
        format!("{plan_id}::runtime-{}", NEXT_RUNTIME_ID.fetch_add(1, Ordering::SeqCst))
    }
}

impl BossControlRuntime {
    pub fn spawn(coordinator: BossCoordinator) -> Arc<Self> {
        let (tx, mut rx) = mpsc::channel::<ControlEnvelope>(16);
        let join_handle = tokio::spawn(async move {
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
        Arc::new(Self {
            tx,
            abort_handle: join_handle.abort_handle(),
            closed: AtomicBool::new(false),
        })
    }

    pub fn shutdown(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.abort_handle.abort();
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    pub async fn request(
        &self,
        request: BossControlRequest,
        tasks: Arc<TaskManager>,
        dispatcher: NotificationDispatcher,
    ) -> anyhow::Result<BossControlResponse> {
        if self.is_closed() {
            anyhow::bail!("boss control runtime is closed");
        }
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
