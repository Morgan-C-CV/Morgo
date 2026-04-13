use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppStateStoreUpdate<T> {
    pub generation: u64,
    pub previous: T,
    pub current: T,
}

type StoreSubscriber<T> = Arc<dyn Fn(AppStateStoreUpdate<T>) + Send + Sync>;

#[derive(Clone)]
pub struct AppStateStore<T> {
    inner: Arc<RwLock<T>>,
    generation: Arc<AtomicU64>,
    next_subscriber_id: Arc<AtomicU64>,
    subscribers: Arc<RwLock<BTreeMap<u64, StoreSubscriber<T>>>>,
}

impl<T: Clone> AppStateStore<T> {
    pub fn new(state: T) -> Self {
        Self {
            inner: Arc::new(RwLock::new(state)),
            generation: Arc::new(AtomicU64::new(0)),
            next_subscriber_id: Arc::new(AtomicU64::new(1)),
            subscribers: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    pub fn get(&self) -> T {
        self.inner.read().expect("store poisoned").clone()
    }

    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }

    pub fn subscribe(
        &self,
        subscriber: impl Fn(AppStateStoreUpdate<T>) + Send + Sync + 'static,
    ) -> u64 {
        let subscriber_id = self.next_subscriber_id.fetch_add(1, Ordering::SeqCst);
        self.subscribers
            .write()
            .expect("store poisoned")
            .insert(subscriber_id, Arc::new(subscriber));
        subscriber_id
    }

    pub fn unsubscribe(&self, subscriber_id: u64) -> bool {
        self.subscribers
            .write()
            .expect("store poisoned")
            .remove(&subscriber_id)
            .is_some()
    }

    pub fn update(&self, mutator: impl FnOnce(&mut T)) -> AppStateStoreUpdate<T> {
        let (previous, current) = {
            let mut guard = self.inner.write().expect("store poisoned");
            let previous = guard.clone();
            mutator(&mut guard);
            let current = guard.clone();
            (previous, current)
        };

        let update = AppStateStoreUpdate {
            generation: self.generation.fetch_add(1, Ordering::SeqCst) + 1,
            previous,
            current,
        };
        let subscribers = self
            .subscribers
            .read()
            .expect("store poisoned")
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for subscriber in subscribers {
            subscriber(update.clone());
        }
        update
    }
}

impl<T> std::fmt::Debug for AppStateStore<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppStateStore")
            .field("generation", &self.generation.load(Ordering::SeqCst))
            .finish_non_exhaustive()
    }
}
