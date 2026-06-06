use bytes::Bytes;
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::broadcast;

/// Global pub/sub hub shared across all connections.
/// Uses Tokio broadcast channels for fan-out.
#[derive(Debug)]
pub struct PubSubHub {
    /// channel_name -> broadcast sender
    channels: DashMap<Bytes, broadcast::Sender<Bytes>>,
    /// pattern -> broadcast sender (carries (channel, message))
    patterns: DashMap<Bytes, broadcast::Sender<(Bytes, Bytes)>>,
    /// shard_channel_name -> broadcast sender (carries (channel, message))
    shard_channels: DashMap<Bytes, broadcast::Sender<(Bytes, Bytes)>>,
    /// Tracks number of active pattern subscriptions (across all clients)
    pattern_sub_count: std::sync::atomic::AtomicUsize,
}

impl PubSubHub {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            channels: DashMap::new(),
            patterns: DashMap::new(),
            shard_channels: DashMap::new(),
            pattern_sub_count: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    /// Publish a message to a channel. Returns the number of subscribers
    /// (channel subscribers + matching pattern subscribers).
    pub fn publish(&self, channel: &Bytes, message: Bytes) -> i64 {
        let mut count: i64 = 0;

        // Send to channel subscribers
        if let Some(tx) = self.channels.get(channel) {
            count += tx.receiver_count() as i64;
            // Ignore send errors (no receivers is fine)
            let _ = tx.send(message.clone());
        }

        // Send to pattern subscribers
        for entry in self.patterns.iter() {
            let (pattern, tx) = entry.pair();
            if Self::matches_pattern(pattern, channel) {
                count += tx.receiver_count() as i64;
                let _ = tx.send((channel.clone(), message.clone()));
            }
        }

        count
    }

    /// Subscribe to a channel. Returns a receiver that gets all messages sent to that channel.
    pub fn subscribe(&self, channel: Bytes) -> broadcast::Receiver<Bytes> {
        self.channels
            .entry(channel)
            .or_insert_with(|| broadcast::channel(1024).0)
            .subscribe()
    }

    /// Subscribe to a pattern. Returns a receiver that gets (channel, message) for matching channels.
    pub fn psubscribe(&self, pattern: Bytes) -> broadcast::Receiver<(Bytes, Bytes)> {
        self.pattern_sub_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.patterns
            .entry(pattern)
            .or_insert_with(|| broadcast::channel(1024).0)
            .subscribe()
    }

    /// Unsubscribe from a pattern. Decrements the pattern subscription count.
    /// The receiver is dropped when the caller drops the returned receiver from psubscribe.
    pub fn punsubscribe(&self) {
        self.pattern_sub_count
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }

    /// Get the number of active pattern subscriptions.
    pub fn numpat(&self) -> usize {
        self.pattern_sub_count
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// List active channels (those with at least one subscriber), optionally filtered by pattern.
    pub fn channels(&self, pattern: Option<&Bytes>) -> Vec<Bytes> {
        let mut result = Vec::new();
        for entry in self.channels.iter() {
            let (ch, tx) = entry.pair();
            if tx.receiver_count() > 0 {
                if let Some(pat) = pattern {
                    if Self::matches_pattern(pat, ch) {
                        result.push(ch.clone());
                    }
                } else {
                    result.push(ch.clone());
                }
            }
        }
        result
    }

    /// Get subscriber counts for the given channels.
    pub fn numsub(&self, channels: &[Bytes]) -> Vec<(Bytes, i64)> {
        channels
            .iter()
            .map(|ch| {
                let count = self
                    .channels
                    .get(ch)
                    .map(|tx| tx.receiver_count() as i64)
                    .unwrap_or(0);
                (ch.clone(), count)
            })
            .collect()
    }

    /// Publish a message to a shard channel. Returns the number of subscribers.
    pub fn spublish(&self, channel: &Bytes, message: Bytes) -> i64 {
        let mut count: i64 = 0;
        if let Some(tx) = self.shard_channels.get(channel) {
            count = tx.receiver_count() as i64;
            let _ = tx.send((channel.clone(), message));
        }
        count
    }

    /// Subscribe to a shard channel. Returns a receiver that gets (channel, message) tuples.
    pub fn ssubscribe(&self, channel: Bytes) -> broadcast::Receiver<(Bytes, Bytes)> {
        self.shard_channels
            .entry(channel)
            .or_insert_with(|| broadcast::channel(1024).0)
            .subscribe()
    }

    /// List active shard channels (those with at least one subscriber), optionally filtered by pattern.
    pub fn shard_channels(&self, pattern: Option<&Bytes>) -> Vec<Bytes> {
        let mut result = Vec::new();
        for entry in self.shard_channels.iter() {
            let (ch, tx) = entry.pair();
            if tx.receiver_count() > 0 {
                if let Some(pat) = pattern {
                    if Self::matches_pattern(pat, ch) {
                        result.push(ch.clone());
                    }
                } else {
                    result.push(ch.clone());
                }
            }
        }
        result
    }

    /// Get subscriber counts for the given shard channels.
    pub fn shard_numsub(&self, channels: &[Bytes]) -> Vec<(Bytes, i64)> {
        channels
            .iter()
            .map(|ch| {
                let count = self
                    .shard_channels
                    .get(ch)
                    .map(|tx| tx.receiver_count() as i64)
                    .unwrap_or(0);
                (ch.clone(), count)
            })
            .collect()
    }

    /// Glob-style pattern matching: supports `*` (any sequence), `?` (single char),
    /// `[abc]` (character class), and `[^abc]` (negated class).
    pub fn matches_pattern(pattern: &[u8], channel: &[u8]) -> bool {
        Self::glob_match(pattern, channel, 0, 0)
    }

    fn glob_match(pattern: &[u8], text: &[u8], pi: usize, ti: usize) -> bool {
        let plen = pattern.len();
        let tlen = text.len();

        // Use iterative approach with a stack to avoid deep recursion
        let mut stack: Vec<(usize, usize)> = vec![(pi, ti)];

        while let Some((pi, ti)) = stack.pop() {
            if pi == plen {
                if ti == tlen {
                    return true;
                }
                continue;
            }
            if ti > tlen {
                continue;
            }

            match pattern[pi] {
                b'*' => {
                    // Try matching 0..remaining chars
                    // Push longest match first so shortest is tried first (stack = LIFO)
                    for skip in (0..=(tlen - ti)).rev() {
                        stack.push((pi + 1, ti + skip));
                    }
                }
                b'?' => {
                    if ti < tlen {
                        stack.push((pi + 1, ti + 1));
                    }
                }
                b'[' => {
                    // Find closing bracket
                    if let Some(close) = pattern[pi + 1..].iter().position(|&b| b == b']') {
                        let close = pi + 1 + close;
                        let class = &pattern[pi + 1..close];
                        let negated = class.first() == Some(&b'^');
                        let class = if negated { &class[1..] } else { class };

                        if ti < tlen {
                            let ch = text[ti];
                            let found = Self::char_class_matches(class, ch);
                            if found != negated {
                                stack.push((close + 1, ti + 1));
                            }
                        }
                    } else {
                        // No closing bracket, treat '[' as literal
                        if ti < tlen && text[ti] == b'[' {
                            stack.push((pi + 1, ti + 1));
                        }
                    }
                }
                c => {
                    if ti < tlen && text[ti] == c {
                        stack.push((pi + 1, ti + 1));
                    }
                }
            }
        }

        false
    }

    /// Check if a character matches a glob character class (e.g., [abc], [a-z], [^0-9]).
    fn char_class_matches(class: &[u8], ch: u8) -> bool {
        let mut i = 0;
        while i < class.len() {
            // Check for range: x-y
            if i + 2 < class.len() && class[i + 1] == b'-' {
                let start = class[i];
                let end = class[i + 2];
                if ch >= start && ch <= end {
                    return true;
                }
                i += 3;
            } else {
                if class[i] == ch {
                    return true;
                }
                i += 1;
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_glob_star() {
        assert!(PubSubHub::matches_pattern(b"foo*", b"foobar"));
        assert!(PubSubHub::matches_pattern(b"*bar", b"foobar"));
        assert!(PubSubHub::matches_pattern(b"foo*bar", b"fooxyzbar"));
        assert!(PubSubHub::matches_pattern(b"*", b"anything"));
        assert!(PubSubHub::matches_pattern(b"***", b"abc"));
    }

    #[test]
    fn test_glob_question() {
        assert!(PubSubHub::matches_pattern(b"fo?", b"foo"));
        assert!(PubSubHub::matches_pattern(b"?oo", b"foo"));
        assert!(!PubSubHub::matches_pattern(b"fo?", b"fo"));
        assert!(!PubSubHub::matches_pattern(b"fo?", b"fooo"));
    }

    #[test]
    fn test_glob_char_class() {
        assert!(PubSubHub::matches_pattern(b"[abc]", b"a"));
        assert!(PubSubHub::matches_pattern(b"[abc]", b"b"));
        assert!(!PubSubHub::matches_pattern(b"[abc]", b"d"));
        assert!(PubSubHub::matches_pattern(b"[^abc]", b"d"));
        assert!(!PubSubHub::matches_pattern(b"[^abc]", b"a"));
    }

    #[test]
    fn test_glob_combined() {
        assert!(PubSubHub::matches_pattern(b"chan*_[0-9]", b"chan_foo_1"));
        assert!(PubSubHub::matches_pattern(
            b"*test*",
            b"this_is_a_test_channel"
        ));
    }

    #[test]
    fn test_publish_subscribe() {
        let hub = PubSubHub::new();
        let mut rx = hub.subscribe(Bytes::from("ch1"));
        let count = hub.publish(&Bytes::from("ch1"), Bytes::from("hello"));
        assert_eq!(count, 1);
        let msg = rx.try_recv().unwrap();
        assert_eq!(msg, Bytes::from("hello"));
    }

    #[test]
    fn test_publish_no_subscribers() {
        let hub = PubSubHub::new();
        let count = hub.publish(&Bytes::from("ch1"), Bytes::from("hello"));
        assert_eq!(count, 0);
    }

    #[test]
    fn test_psubscribe() {
        let hub = PubSubHub::new();
        let mut rx = hub.psubscribe(Bytes::from("chan*"));
        let count = hub.publish(&Bytes::from("chan1"), Bytes::from("msg1"));
        assert!(count >= 1);
        let (ch, msg) = rx.try_recv().unwrap();
        assert_eq!(ch, Bytes::from("chan1"));
        assert_eq!(msg, Bytes::from("msg1"));
    }

    #[test]
    fn test_numpat() {
        let hub = PubSubHub::new();
        assert_eq!(hub.numpat(), 0);
        let _rx1 = hub.psubscribe(Bytes::from("a*"));
        assert_eq!(hub.numpat(), 1);
        let _rx2 = hub.psubscribe(Bytes::from("b*"));
        assert_eq!(hub.numpat(), 2);
    }

    #[test]
    fn test_channels() {
        let hub = PubSubHub::new();
        let _rx1 = hub.subscribe(Bytes::from("ch1"));
        let _rx2 = hub.subscribe(Bytes::from("ch2"));
        let _rx3 = hub.subscribe(Bytes::from("other"));

        let mut chans = hub.channels(None);
        chans.sort();
        assert_eq!(chans.len(), 3);

        let filtered = hub.channels(Some(&Bytes::from("ch*")));
        assert_eq!(filtered.len(), 2);
    }

    #[test]
    fn test_numsub() {
        let hub = PubSubHub::new();
        let _rx1 = hub.subscribe(Bytes::from("ch1"));
        let _rx2 = hub.subscribe(Bytes::from("ch1"));
        let _rx3 = hub.subscribe(Bytes::from("ch2"));

        let counts = hub.numsub(&[Bytes::from("ch1"), Bytes::from("ch2"), Bytes::from("ch3")]);
        assert_eq!(counts[0].1, 2); // ch1 has 2 subscribers
        assert_eq!(counts[1].1, 1); // ch2 has 1 subscriber
        assert_eq!(counts[2].1, 0); // ch3 has 0 subscribers
    }
}
