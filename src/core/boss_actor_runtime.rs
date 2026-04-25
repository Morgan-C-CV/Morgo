use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot, RwLock};

use crate::core::boss_state::{BossActorRole, BossActorStatus, BossStage};

/// Callback type for B's execution side effect.
/// Takes the step payload string, returns the tool invocation result.
pub type ExecutionFn =
    Arc<dyn Fn(String) -> Pin<Box<dyn Future<Output = anyhow::Result<String>> + Send>> + Send + Sync>;

/// Callback type for A's review side effect.
/// Takes (step_id, accepted, summary, correction) — drives plan mutation + auto-advance.
pub type ReviewFn = Arc<
    dyn Fn(usize, bool, String, Option<String>) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>
        + Send
        + Sync,
>;

/// Callback type for A's documentation/approval side effect.
/// Takes a stage-transition signal string — drives finalize or approval transitions.
pub type DocumentationFn =
    Arc<dyn Fn(String) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>> + Send + Sync>;

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

/// Commands that DesignerA can receive.
#[derive(Debug)]
pub enum DesignerACommand {
    /// Deliver a plan document for A to review.
    Plan { plan_id: String, document_spec: String },
    /// Ask A to review a completed step output.
    Review { step_id: usize, accepted: bool, summary: String, correction: Option<String> },
    /// Finalize the documentation loop — A drives the transition to WaitingForApproval.
    FinalizeDocumentation { signal: String },
    /// User approval input — A drives the stage transition to Execution (or back to Documentation).
    UserApproval { input: String },
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
    DocumentationAdvanced { signal: String },
    ApprovalHandled { approved: bool },
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
        Self::spawn_with_callbacks(None, None)
    }

    /// Spawn with optional review and documentation callbacks.
    /// A's handler calls them to drive plan mutation and stage transitions.
    pub fn spawn_with_callbacks(
        review_fn: Option<ReviewFn>,
        doc_fn: Option<DocumentationFn>,
    ) -> Self {
        let (tx, mut rx) = mpsc::channel::<DesignerAEnvelope>(16);
        let closed = Arc::new(AtomicBool::new(false));
        let state = Arc::new(RwLock::new(BossActorState::default()));
        let state_loop = state.clone();
        let closed_loop = closed.clone();

        let join = tokio::spawn(async move {
            while let Some(envelope) = rx.recv().await {
                let event = handle_designer_a_command(
                    envelope.command,
                    &state_loop,
                    review_fn.as_ref(),
                    doc_fn.as_ref(),
                )
                .await;
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
    /// Spawn with no execution side effect (state-only).
    pub fn spawn() -> Self {
        Self::spawn_with_executor(None)
    }

    /// Spawn with an execution callback — B's handler calls it for DispatchStep/ContinueStep.
    pub fn spawn_with_executor(exec_fn: Option<ExecutionFn>) -> Self {
        let (tx, mut rx) = mpsc::channel::<ExecutorBEnvelope>(16);
        let closed = Arc::new(AtomicBool::new(false));
        let state = Arc::new(RwLock::new(BossActorState::default()));
        let state_loop = state.clone();
        let closed_loop = closed.clone();

        let join = tokio::spawn(async move {
            while let Some(envelope) = rx.recv().await {
                let event =
                    handle_executor_b_command(envelope.command, &state_loop, exec_fn.as_ref())
                        .await;
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
    review_fn: Option<&ReviewFn>,
    doc_fn: Option<&DocumentationFn>,
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
        DesignerACommand::Review { step_id, accepted, summary, correction } => {
            {
                let mut s = state.write().await;
                s.status = BossActorStatus::Active;
                s.current_step = Some(step_id);
            }
            // A's handler owns the review side effect — plan mutation + auto-advance.
            if let Some(f) = review_fn {
                let _ = f(step_id, accepted, summary.clone(), correction).await;
            }
            BossActorEvent::ReviewComplete { step_id, accepted, summary }
        }
        DesignerACommand::FinalizeDocumentation { signal } => {
            {
                let mut s = state.write().await;
                s.status = BossActorStatus::Active;
                s.stage = BossStage::WaitingForApproval;
            }
            if let Some(f) = doc_fn {
                let _ = f(signal.clone()).await;
            }
            BossActorEvent::DocumentationAdvanced { signal }
        }
        DesignerACommand::UserApproval { input } => {
            let approved = input.trim().to_uppercase() == "Y" || input.trim().is_empty();
            {
                let mut s = state.write().await;
                s.stage = if approved { BossStage::Execution } else { BossStage::Documentation };
            }
            if let Some(f) = doc_fn {
                let _ = f(input).await;
            }
            BossActorEvent::ApprovalHandled { approved }
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
    exec_fn: Option<&ExecutionFn>,
) -> BossActorEvent {
    match command {
        ExecutorBCommand::DispatchStep { step_id, payload } => {
            {
                let mut s = state.write().await;
                s.status = BossActorStatus::Active;
                s.current_step = Some(step_id);
            }
            // Call the execution side effect if wired — B owns the tool invocation.
            if let Some(f) = exec_fn {
                let _ = f(payload).await;
            }
            BossActorEvent::StepDispatched {
                step_id,
                task_id: format!("b-task-step-{step_id}"),
            }
        }
        ExecutorBCommand::ContinueStep { step_id, task_id, payload } => {
            {
                let mut s = state.write().await;
                s.current_step = Some(step_id);
            }
            if let Some(f) = exec_fn {
                let _ = f(payload).await;
            }
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
    /// True when B was spawned with a real execution callback.
    pub has_executor: bool,
    /// True when A was spawned with real review/documentation callbacks.
    pub has_a_callbacks: bool,
}

impl BossActorRegistry {
    pub fn bootstrap() -> Self {
        Self {
            designer_a: DesignerARuntime::spawn(),
            executor_b: ExecutorBRuntime::spawn(),
            has_executor: false,
            has_a_callbacks: false,
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

/// Convenience constructor for wiring an execution callback at bootstrap time.
pub fn bootstrap_with_executor(exec_fn: ExecutionFn) -> BossActorRegistry {
    BossActorRegistry {
        designer_a: DesignerARuntime::spawn(),
        executor_b: ExecutorBRuntime::spawn_with_executor(Some(exec_fn)),
        has_executor: true,
        has_a_callbacks: false,
    }
}

/// Convenience constructor for wiring both B's execution callback and A's review/doc callbacks.
pub fn bootstrap_with_all_callbacks(
    exec_fn: ExecutionFn,
    review_fn: ReviewFn,
    doc_fn: DocumentationFn,
) -> BossActorRegistry {
    BossActorRegistry {
        designer_a: DesignerARuntime::spawn_with_callbacks(Some(review_fn), Some(doc_fn)),
        executor_b: ExecutorBRuntime::spawn_with_executor(Some(exec_fn)),
        has_executor: true,
        has_a_callbacks: true,
    }
}
