use std::sync::{Arc, RwLock};

#[derive(Debug, Clone)]
pub struct AppStateStore<T> {
    inner: Arc<RwLock<T>>,
}

impl<T: Clone> AppStateStore<T> {
    pub fn new(state: T) -> Self {
        Self {
            inner: Arc::new(RwLock::new(state)),
        }
    }

    pub fn get(&self) -> T {
        self.inner.read().expect("store poisoned").clone()
    }

    pub fn update(&self, mutator: impl FnOnce(&mut T)) {
        let mut guard = self.inner.write().expect("store poisoned");
        mutator(&mut guard);
    }
}
