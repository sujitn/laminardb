//! Rate limiting for connector throughput control.
//!
//! Provides token bucket and leaky bucket algorithms for controlling the rate
//! of source polling or sink writes.

// Rate limiters use f64 internally for token/level tracking with fractional precision.
// The precision loss from usize->f64 is acceptable for rate limiting purposes where
// exact counts are not critical. Sign loss on usize->f64 is impossible (always positive).
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use parking_lot::Mutex;

/// Trait for rate limiters.
#[async_trait]
pub trait RateLimiter: Send + Sync {
    /// Acquires permission to proceed, blocking until a token is available.
    async fn acquire(&self);

    /// Acquires multiple permits at once.
    async fn acquire_many(&self, permits: usize) {
        for _ in 0..permits {
            self.acquire().await;
        }
    }

    /// Tries to acquire permission without blocking.
    ///
    /// Returns `true` if permission was granted, `false` otherwise.
    fn try_acquire(&self) -> bool;

    /// Tries to acquire multiple permits without blocking.
    ///
    /// Note: This is not atomic - some permits may be consumed even if the
    /// function returns `false`.
    fn try_acquire_many(&self, permits: usize) -> bool {
        (0..permits).all(|_| self.try_acquire())
    }

    /// Returns the current available permits (approximate).
    fn available(&self) -> usize;
}

/// Token bucket rate limiter.
///
/// Tokens are added at a fixed rate up to a maximum capacity. Operations
/// consume tokens and block when none are available.
#[derive(Debug)]
pub struct TokenBucket {
    /// Tokens added per second.
    rate: f64,
    /// Maximum tokens that can accumulate.
    capacity: usize,
    /// Internal state.
    state: Arc<Mutex<TokenBucketState>>,
}

#[derive(Debug)]
struct TokenBucketState {
    tokens: f64,
    last_update: Instant,
}

impl TokenBucket {
    /// Creates a new token bucket.
    ///
    /// # Arguments
    ///
    /// * `rate` - Tokens added per second
    /// * `capacity` - Maximum tokens (burst size)
    #[must_use]
    pub fn new(rate: f64, capacity: usize) -> Self {
        Self {
            rate,
            capacity,
            state: Arc::new(Mutex::new(TokenBucketState {
                tokens: capacity as f64,
                last_update: Instant::now(),
            })),
        }
    }

    /// Creates a token bucket from a rate limit (e.g., "1000/s" = 1000 per second).
    #[must_use]
    pub fn from_rate_per_second(rate_per_second: usize) -> Self {
        // Use 10% of rate as burst capacity, minimum 1
        let capacity = (rate_per_second / 10).max(1);
        Self::new(rate_per_second as f64, capacity)
    }

    /// Creates a token bucket with custom burst capacity.
    #[must_use]
    pub fn with_burst(rate_per_second: usize, burst: usize) -> Self {
        Self::new(rate_per_second as f64, burst)
    }

    fn refill(&self, state: &mut TokenBucketState) {
        let now = Instant::now();
        let elapsed = now.duration_since(state.last_update).as_secs_f64();
        let new_tokens = elapsed * self.rate;
        state.tokens = (state.tokens + new_tokens).min(self.capacity as f64);
        state.last_update = now;
    }
}

impl Clone for TokenBucket {
    fn clone(&self) -> Self {
        Self {
            rate: self.rate,
            capacity: self.capacity,
            state: self.state.clone(),
        }
    }
}

#[async_trait]
impl RateLimiter for TokenBucket {
    async fn acquire(&self) {
        loop {
            {
                let mut state = self.state.lock();
                self.refill(&mut state);

                if state.tokens >= 1.0 {
                    state.tokens -= 1.0;
                    return;
                }
            }

            // Wait for approximately one token to accumulate
            let wait_time = Duration::from_secs_f64(1.0 / self.rate);
            tokio::time::sleep(wait_time).await;
        }
    }

    fn try_acquire(&self) -> bool {
        let mut state = self.state.lock();
        self.refill(&mut state);

        if state.tokens >= 1.0 {
            state.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    fn available(&self) -> usize {
        let mut state = self.state.lock();
        self.refill(&mut state);
        state.tokens as usize
    }
}

/// Leaky bucket rate limiter.
///
/// Requests "drip" out at a constant rate. New requests are added to the bucket
/// up to a maximum capacity; excess requests are rejected.
#[derive(Debug)]
pub struct LeakyBucket {
    /// Requests processed per second.
    rate: f64,
    /// Maximum pending requests.
    capacity: usize,
    /// Internal state.
    state: Arc<Mutex<LeakyBucketState>>,
}

#[derive(Debug)]
struct LeakyBucketState {
    level: f64,
    last_update: Instant,
}

impl LeakyBucket {
    /// Creates a new leaky bucket.
    ///
    /// # Arguments
    ///
    /// * `rate` - Requests processed per second
    /// * `capacity` - Maximum pending requests (queue depth)
    #[must_use]
    pub fn new(rate: f64, capacity: usize) -> Self {
        Self {
            rate,
            capacity,
            state: Arc::new(Mutex::new(LeakyBucketState {
                level: 0.0,
                last_update: Instant::now(),
            })),
        }
    }

    /// Creates a leaky bucket for a given rate per second.
    #[must_use]
    pub fn from_rate_per_second(rate_per_second: usize) -> Self {
        // Default capacity is rate/10 or 10, whichever is larger
        let capacity = (rate_per_second / 10).max(10);
        Self::new(rate_per_second as f64, capacity)
    }

    fn drain(&self, state: &mut LeakyBucketState) {
        let now = Instant::now();
        let elapsed = now.duration_since(state.last_update).as_secs_f64();
        let drained = elapsed * self.rate;
        state.level = (state.level - drained).max(0.0);
        state.last_update = now;
    }

    /// Returns the current fill level (approximate).
    #[must_use]
    pub fn level(&self) -> usize {
        let mut state = self.state.lock();
        self.drain(&mut state);
        state.level.ceil() as usize
    }
}

impl Clone for LeakyBucket {
    fn clone(&self) -> Self {
        Self {
            rate: self.rate,
            capacity: self.capacity,
            state: self.state.clone(),
        }
    }
}

#[async_trait]
impl RateLimiter for LeakyBucket {
    async fn acquire(&self) {
        loop {
            let wait_time = {
                let mut state = self.state.lock();
                self.drain(&mut state);

                // Use ceiling to ensure we don't exceed capacity due to float precision
                if state.level.ceil() < self.capacity as f64 {
                    state.level += 1.0;
                    return;
                }

                // Calculate how long until a slot opens (need to drain at least 1 slot)
                let excess = (state.level - (self.capacity as f64 - 1.0)).max(1.0);
                Duration::from_secs_f64(excess / self.rate)
            };

            tokio::time::sleep(wait_time).await;
        }
    }

    fn try_acquire(&self) -> bool {
        let mut state = self.state.lock();
        self.drain(&mut state);

        // Use ceiling to ensure we don't exceed capacity due to float precision
        if state.level.ceil() < self.capacity as f64 {
            state.level += 1.0;
            true
        } else {
            false
        }
    }

    fn available(&self) -> usize {
        let mut state = self.state.lock();
        self.drain(&mut state);
        (self.capacity as f64 - state.level).max(0.0) as usize
    }
}

/// A no-op rate limiter that never blocks.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopRateLimiter;

#[async_trait]
impl RateLimiter for NoopRateLimiter {
    async fn acquire(&self) {}

    fn try_acquire(&self) -> bool {
        true
    }

    fn available(&self) -> usize {
        usize::MAX
    }
}

#[cfg(test)]
#[allow(clippy::uninlined_format_args)]
mod tests {
    use super::*;

    #[test]
    fn test_token_bucket_creation() {
        let tb = TokenBucket::new(100.0, 10);
        assert_eq!(tb.available(), 10);
    }

    #[test]
    fn test_token_bucket_from_rate() {
        let tb = TokenBucket::from_rate_per_second(1000);
        assert_eq!(tb.capacity, 100);
    }

    #[test]
    fn test_token_bucket_try_acquire() {
        let tb = TokenBucket::new(100.0, 5);

        // Should be able to acquire up to capacity
        assert!(tb.try_acquire());
        assert!(tb.try_acquire());
        assert!(tb.try_acquire());
        assert!(tb.try_acquire());
        assert!(tb.try_acquire());

        // Next should fail
        assert!(!tb.try_acquire());
    }

    #[tokio::test]
    async fn test_token_bucket_acquire() {
        let tb = TokenBucket::new(1000.0, 2);

        // Acquire all tokens
        tb.acquire().await;
        tb.acquire().await;

        // Third should block briefly then succeed
        let start = Instant::now();
        tb.acquire().await;
        let elapsed = start.elapsed();

        // Should have waited approximately 1ms (1/1000 seconds)
        assert!(elapsed >= Duration::from_micros(500));
    }

    #[test]
    fn test_token_bucket_refill() {
        let tb = TokenBucket::new(1000.0, 5);

        // Drain all tokens
        for _ in 0..5 {
            assert!(tb.try_acquire());
        }
        assert_eq!(tb.available(), 0);

        // Wait a bit for refill
        std::thread::sleep(Duration::from_millis(10));

        // Should have some tokens now
        assert!(tb.available() > 0);
    }

    #[test]
    fn test_leaky_bucket_creation() {
        let lb = LeakyBucket::new(100.0, 10);
        assert_eq!(lb.available(), 10);
        assert_eq!(lb.level(), 0);
    }

    #[test]
    fn test_leaky_bucket_try_acquire() {
        // Use very low rate to prevent draining during test
        let lb = LeakyBucket::new(0.001, 3);

        // Fill the bucket
        assert!(lb.try_acquire());
        assert!(lb.try_acquire());
        assert!(lb.try_acquire());

        // Bucket is full
        assert!(!lb.try_acquire());
        assert_eq!(lb.level(), 3);
    }

    #[test]
    fn test_leaky_bucket_drains() {
        let lb = LeakyBucket::new(1000.0, 10);

        // Fill partially
        lb.try_acquire();
        lb.try_acquire();
        assert_eq!(lb.level(), 2);

        // Wait for drain
        std::thread::sleep(Duration::from_millis(10));

        // Should have drained some
        assert!(lb.level() < 2);
    }

    #[tokio::test]
    async fn test_leaky_bucket_acquire() {
        // Use lower rate to make wait time more measurable
        let lb = LeakyBucket::new(100.0, 2);

        // Fill the bucket
        lb.acquire().await;
        lb.acquire().await;

        // Third should block until a slot drains
        let start = Instant::now();
        lb.acquire().await;
        let elapsed = start.elapsed();

        // Should have waited for at least 10ms (1/100 seconds)
        // Use a more lenient check due to timer resolution on Windows
        assert!(
            elapsed >= Duration::from_millis(5),
            "expected wait of at least 5ms, got {:?}",
            elapsed
        );
    }

    #[test]
    fn test_noop_rate_limiter() {
        let limiter = NoopRateLimiter;
        assert!(limiter.try_acquire());
        assert!(limiter.try_acquire());
        assert_eq!(limiter.available(), usize::MAX);
    }

    #[tokio::test]
    async fn test_noop_rate_limiter_async() {
        let limiter = NoopRateLimiter;
        limiter.acquire().await;
        limiter.acquire().await;
        // Should not block
    }

    #[test]
    fn test_token_bucket_clone_shares_state() {
        let tb1 = TokenBucket::new(100.0, 5);
        let tb2 = tb1.clone();

        tb1.try_acquire();
        tb1.try_acquire();

        // Both should see the reduced count
        assert_eq!(tb1.available(), 3);
        assert_eq!(tb2.available(), 3);
    }

    #[test]
    fn test_leaky_bucket_clone_shares_state() {
        let lb1 = LeakyBucket::new(100.0, 5);
        let lb2 = lb1.clone();

        lb1.try_acquire();
        lb1.try_acquire();

        // Both should see the same level
        assert_eq!(lb1.level(), 2);
        assert_eq!(lb2.level(), 2);
    }
}
