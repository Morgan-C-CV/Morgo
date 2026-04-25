use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot, RwLock};

use crate::core::boss_state::{BossActorRole, BossActorStatus, BossStage};

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

/// Commands that DesignerA can receive.
#[derive(Debug)]
pub enum DesignerACommand {
    /// Deliver a plan document for A to review.
    Plan { plan_id: String, document_spec: String },
    /// Ask A to review a completed step output.
    Review { step_id: usize, summary: String },
    /// Notify A that the user approved the plan.
    Approve,
    /// Stop A's runtime.
    Stop,
}

/// Commands that ExecutorB can receive.
#[derive(Debug)]
pub enum ExecutorBCommand {
    /// Dispatch a new step to B (spawn or continue).
    DispatchStep { step_id: usize, payload: String },
    /// Continue an in-progress step with updated context.
    ContinueStep { step_id: usize, task_id: String, payload: String },
    /// Stop B's runtime.
    Stop,
}

// ---------------------------------------------------------------------------
// Events emitted by actor runtimes
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BossActorEvent {
    StatusChanged { role: BossActorRole, status: BossActorStatus },
    StepDispatched { step_id: usize, task_id: String },
    ReviewComplete { step_id: usize, accepted: bool, summary: String },
    Stopped { role: BossActorRole },
}

// ---------------------------------------------------------------------------
// Mailbox handles
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct DesignerAMailbox {
    tx: mpsc::Sender<DesignerAEnvelope>,
    closed: Arc<AtomicBool>,
}

#[derive(Debug, Clone)]
pub struct ExecutorBMailbox {
    tx: mpsc::Sender<ExecutorBEnvelope>,
    closed: Arc<AtomicBool>,
}

struct DesignerAEnvelope {
    command: DesignerACommand,
    respond_to: Option<oneshot::Sender<BossActorEvent>>,
}

struct ExecutorBEnvelope {
    command: ExecutorBCommand,
    respond_to: Option<oneshot::Sender<BossActorEvent>>,
}

impl DesignerAMailbox {
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    pub async fn send(&self, command: DesignerACommand) -> anyhow::Result<()> {
        if self.is_closed() {
            anyhow::bail!("designer_a mailbox is closed");
        }
        self.tx
            .send(DesignerAEnvelope { command, respond_to: None })
            .await
            .map_err(|_| anyhow::anyhow!("designer_a mailbox send failed"))
    }

    pub async fn request(&self, command: DesignerACommand) -> anyhow::Result<BossActorEvent> {
        if self.is_closed() {
            anyhow::bail!("designer_a mailbox is closed");
        }
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(DesignerAEnvelope { command, respond_to: Some(tx) })
            .await
            .map_err(|_| anyhow::anyhow!("designer_a mailbox send failed"))?;
        rx.await.map_err(|_| anyhow::anyhow!("designer_a mailbox receive failed"))
    }
}

impl ExecutorBMailbox {
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    pub async fn send(&self, command: ExecutorBCommand) -> anyhow::Result<()> {
        if self.is_closed() {
            anyhow::bail!("executor_b mailbox is closed");
        }
        self.tx
            .send(ExecutorBEnvelope { command, respond_to: None })
            .await
            .map_err(|_| anyhow::anyhow!("executor_b mailbox send failed"))
    }

    pub async fn request(&self, command: ExecutorBCommand) -> anyhow::Result<BossActorEvent> {
        if self.is_closed() {
            anyhow::bail!("executor_b mailbox is closed");
        }
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(ExecutorBEnvelope { command, respond_to: Some(tx) })
            .await
            .map_err(|_| anyhow::anyhow!("executor_b mailbox send failed"))?;
        rx.await.map_err(|_| anyhow::anyhow!("executor_b mailbox receive failed"))
    }
}

// ---------------------------------------------------------------------------
// Actor runtimes
// ---------------------------------------------------------------------------

/// Observable state shared between the runtime loop and external observers.
#[derive(Debug, Default)]
pub struct BossActorState {
    pub status: BossActorStatus,
    pub current_step: Option<usize>,
    pub stage: BossStage,
}

#[derive(Debug)]
pub struct DesignerARuntime {
    pub mailbox: DesignerAMailbox,
    pub state: Arc<RwLock<BossActorState>>,
    abort_handle: tokio::task::AbortHandle,
}

#[derive(Debug)]
pub struct ExecutorBRuntime {
    pub mailbox: ExecutorBMailbox,
    pub state: Arc<RwLock<BossActorState>>,
    abort_handle: tokio::task::AbortHandle,
}

impl DesignerARuntime {
    pub fn spawn() -> Self {
        let (tx, mut rx) = mpsc::channel::<DesignerAEnvelope>(16);
        let closed = Arc::new(AtomicBool::new(false));
        let state = Arc::new(RwLock::new(BossActorState::default()));
        let state_loop = state.clone();
        let closed_loop = closed.clone();

        let join = tokio::spawn(async move {
            while let Some(envelope) = rx.recv().await {
                let event = handle_designer_a_command(envelope.command, &state_loop).await;
                if let Some(respond_to) = envelope.respond_to {
                    let _ = respond_to.send(event.clone());
                }
                if matches!(event, BossActorEvent::Stopped { .. }) {
                    closed_loop.store(true, Ordering::SeqCst);
                    break;
                }
            }
        });

        Self {
            mailbox: DesignerAMailbox { tx, closed },
            state,
            abort_handle: join.abort_handle(),
        }
    }

    pub fn shutdown(&self) {
        self.mailbox.closed.store(true, Ordering::SeqCst);
        self.abort_handle.abort();
    }

    pub async fn status(&self) -> BossActorStatus {
        self.state.read().await.status
    }
}

impl ExecutorBRuntime {
    pub fn spawn() -> Self {
        let (tx, mut rx) = mpsc::channel::<ExecutorBEnvelope>(16);
        let closed = Arc::new(AtomicBool::new(false));
        let state = Arc::new(RwLock::new(BossActorState::default()));
        let state_loop = state.clone();
        let closed_loop = closed.clone();

        let join = tokio::spawn(async move {
            while let Some(envelope) = rx.recv().await {
                let event = handle_executor_b_command(envelope.command, &state_loop).await;
                if let Some(respond_to) = envelope.respond_to {
                    let _ = respond_to.send(event.clone());
                }
                if matches!(event, BossActorEvent::Stopped { .. }) {
                    closed_loop.store(true, Ordering::SeqCst);
                    break;
                }
            }
        });

        Self {
            mailbox: ExecutorBMailbox { tx, closed },
            state,
            abort_handle: join.abort_handle(),
        }
    }

    pub fn shutdown(&self) {
        self.mailbox.closed.store(true, Ordering::SeqCst);
        self.abort_handle.abort();
    }

    pub async fn status(&self) -> BossActorStatus {
        self.state.read().await.status
    }
}

// ---------------------------------------------------------------------------
// Command handlers (pure state transitions — no real agent invocation yet)
// ---------------------------------------------------------------------------

async fn handle_designer_a_command(
    command: DesignerACommand,
    state: &Arc<RwLock<BossActorState>>,
) -> BossActorEvent {
    match command {
        DesignerACommand::Plan { .. } => {
            let mut s = state.write().await;
            s.status = BossActorStatus::Active;
            s.stage = BossStage::Documentation;
            BossActorEvent::StatusChanged {
                role: BossActorRole::DesignerA,
                status: BossActorStatus::Active,
            }
        }
        DesignerACommand::Review { step_id, summary } => {
            let mut s = state.write().await;
            s.status = BossActorStatus::Active;
            s.current_step = Some(step_id);
            BossActorEvent::ReviewComplete {
                step_id,
                accepted: true,
                summary,
            }
        }
        DesignerACommand::Approve => {
            let mut s = state.write().await;
            s.stage = BossStage::Execution;
            BossActorEvent::StatusChanged {
                role: BossActorRole::DesignerA,
                status: BossActorStatus::Active,
            }
        }
        DesignerACommand::Stop => {
            let mut s = state.write().await;
            s.status = BossActorStatus::Suspended;
            BossActorEvent::Stopped { role: BossActorRole::DesignerA }
        }
    }
}

async fn handle_executor_b_command(
    command: ExecutorBCommand,
    state: &Arc<RwLock<BossActorState>>,
) -> BossActorEvent {
    match command {
        ExecutorBCommand::DispatchStep { step_id, payload: _ } => {
            let mut s = state.write().await;
            s.status = BossActorStatus::Active;
            s.current_step = Some(step_id);
            BossActorEvent::StepDispatched {
                step_id,
                task_id: format!("b-task-step-{step_id}"),
            }
        }
        ExecutorBCommand::ContinueStep { step_id, task_id, payload: _ } => {
            let mut s = state.write().await;
            s.current_step = Some(step_id);
            BossActorEvent::StepDispatched { step_id, task_id }
        }
        ExecutorBCommand::Stop => {
            let mut s = state.write().await;
            s.status = BossActorStatus::Suspended;
            BossActorEvent::Stopped { role: BossActorRole::ExecutorB }
        }
    }
}

// ---------------------------------------------------------------------------
// Registry held by BossCoordinator
// ---------------------------------------------------------------------------

/// Holds the live actor runtimes for one boss session.
#[derive(Debug)]
pub struct BossActorRegistry {
    pub designer_a: DesignerARuntime,
    pub executor_b: ExecutorBRuntime,
}

impl BossActorRegistry {
    pub fn bootstrap() -> Self {
        Self {
            designer_a: DesignerARuntime::spawn(),
            executor_b: ExecutorBRuntime::spawn(),
        }
    }

    pub fn shutdown_all(&self) {
        self.designer_a.shutdown();
        self.executor_b.shutdown();
    }

    pub fn a_mailbox(&self) -> &DesignerAMailbox {
        &self.designer_a.mailbox
    }

    pub fn b_mailbox(&self) -> &ExecutorBMailbox {
        &self.executor_b.mailbox
    }
}
