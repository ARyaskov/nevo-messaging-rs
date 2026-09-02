//! NATS subject layout shared with the TypeScript transport:
//! `<service>-events` for requests, `<service>-events.sub.<method>` for pub/sub,
//! `__broadcast.<method>` for broadcasts.

use crate::wire::{BROADCAST_TOPIC, EVENTS_SUFFIX, Method, SUBSCRIPTION_SUFFIX};

pub fn request_subject(service: &str) -> String {
    format!("{service}{EVENTS_SUFFIX}")
}

/// Method-scoped subject under `prefix`. In subscribe mode NATS wildcards pass through.
pub fn method_subject(prefix: &str, method: &str, subscribe: bool) -> String {
    if method.is_empty() {
        return format!("{prefix}.>");
    }
    let base = Method::parse(method).name;
    let tokens: Vec<String> = base
        .split('.')
        .map(|token| {
            if subscribe && (token == "*" || token == ">") {
                return token.to_owned();
            }
            let cleaned: String = token
                .chars()
                .map(|c| {
                    if c.is_whitespace() || c == '*' || c == '>' {
                        '_'
                    } else {
                        c
                    }
                })
                .collect();
            if cleaned.is_empty() {
                "_".to_owned()
            } else {
                cleaned
            }
        })
        .collect();
    format!("{prefix}.{}", tokens.join("."))
}

pub fn publish_subject(service: &str, method: &str) -> String {
    method_subject(&format!("{service}{SUBSCRIPTION_SUFFIX}"), method, false)
}

pub fn subscribe_subject(service: &str, method: &str) -> String {
    if service == BROADCAST_TOPIC {
        method_subject(BROADCAST_TOPIC, method, true)
    } else {
        method_subject(&format!("{service}{SUBSCRIPTION_SUFFIX}"), method, true)
    }
}

pub fn broadcast_subject(method: &str) -> String {
    method_subject(BROADCAST_TOPIC, method, false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subjects_match_the_typescript_layout() {
        assert_eq!(request_subject("user"), "user-events");
        assert_eq!(
            publish_subject("user", "user.updated@v2"),
            "user-events.sub.user.updated"
        );
        assert_eq!(
            subscribe_subject("user", "user.*"),
            "user-events.sub.user.*"
        );
        assert_eq!(subscribe_subject("user", ""), "user-events.sub.>");
        assert_eq!(publish_subject("user", "a b*c"), "user-events.sub.a_b_c");
        assert_eq!(publish_subject("user", "x..y"), "user-events.sub.x._.y");
        assert_eq!(
            broadcast_subject("system.status"),
            "__broadcast.system.status"
        );
        assert_eq!(
            subscribe_subject("__broadcast", "system.>"),
            "__broadcast.system.>"
        );
        assert_eq!(publish_subject("user", "a.>"), "user-events.sub.a._");
    }
}
