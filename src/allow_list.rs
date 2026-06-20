//! Pluggable source for the per-platform "allow-list" of user IDs.
//!
//! The default in-tree implementation is [`StaticAllowList`], which wraps the
//! static set produced from a platform's `allowed_users` config field.
//! Downstream forks may provide alternative implementations (e.g. a
//! file-watching impl that mirrors an IdP group) without modifying the
//! gate-check call sites in the adapters.

use std::collections::HashSet;
use std::sync::Arc;

/// Provides the current set of user IDs allowed to interact with the bot.
///
/// Implementations must be cheap to call repeatedly: the dispatch path calls
/// [`AllowListSource::allowed_users`] once per inbound message. Returning an
/// `Arc<HashSet<String>>` lets implementations that hot-swap the underlying
/// set (e.g. via `arc_swap`) hand out a consistent snapshot to each caller
/// without taking a lock on the read path.
pub trait AllowListSource: Send + Sync {
    /// Returns a snapshot of the currently-allowed user IDs.
    fn allowed_users(&self) -> Arc<HashSet<String>>;
}

/// In-tree default implementation: wraps a fixed set loaded once at startup
/// from configuration. Snapshots share a single `Arc`-backed allocation, so
/// the read path is allocation-free.
pub struct StaticAllowList {
    users: Arc<HashSet<String>>,
}

impl StaticAllowList {
    pub fn new(users: HashSet<String>) -> Self {
        Self {
            users: Arc::new(users),
        }
    }
}

impl AllowListSource for StaticAllowList {
    fn allowed_users(&self) -> Arc<HashSet<String>> {
        Arc::clone(&self.users)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn static_round_trips_input() {
        let input: HashSet<String> = ["U1", "U2"].iter().map(|s| s.to_string()).collect();
        let source = StaticAllowList::new(input.clone());
        assert_eq!(*source.allowed_users(), input);
    }

    #[test]
    fn static_snapshots_share_allocation() {
        let input: HashSet<String> = ["U1"].iter().map(|s| s.to_string()).collect();
        let source = StaticAllowList::new(input);
        let a = source.allowed_users();
        let b = source.allowed_users();
        assert!(Arc::ptr_eq(&a, &b));
    }

    /// Mock impl proving the seam supports downstream impls that swap the
    /// underlying set at runtime. Mutex-guarded for test simplicity; a real
    /// hot-reload impl would use `arc_swap::ArcSwap`.
    struct SwappableSource {
        inner: std::sync::Mutex<Arc<HashSet<String>>>,
    }

    impl SwappableSource {
        fn new(initial: HashSet<String>) -> Self {
            Self {
                inner: std::sync::Mutex::new(Arc::new(initial)),
            }
        }

        fn swap(&self, next: HashSet<String>) {
            *self.inner.lock().unwrap() = Arc::new(next);
        }
    }

    impl AllowListSource for SwappableSource {
        fn allowed_users(&self) -> Arc<HashSet<String>> {
            Arc::clone(&self.inner.lock().unwrap())
        }
    }

    #[test]
    fn custom_source_can_hot_swap_through_trait_object() {
        let initial: HashSet<String> = ["U1"].iter().map(|s| s.to_string()).collect();
        let typed = Arc::new(SwappableSource::new(initial));
        let dyn_source: Arc<dyn AllowListSource> = typed.clone();

        let before = dyn_source.allowed_users();
        assert!(before.contains("U1"));

        let next: HashSet<String> = ["U2"].iter().map(|s| s.to_string()).collect();
        typed.swap(next);

        let after = dyn_source.allowed_users();
        assert!(after.contains("U2"));
        assert!(!after.contains("U1"));
    }
}
