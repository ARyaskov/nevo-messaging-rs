//! Secret redaction for logs and dead letters.

use serde_json::Value;

use crate::wire::{Auth, Meta};

pub const REDACTED: &str = "[REDACTED]";

const EXACT_KEYS: &[&str] = &[
    "password",
    "passwd",
    "pwd",
    "secret",
    "client_secret",
    "token",
    "access_token",
    "refresh_token",
    "id_token",
    "authorization",
    "auth",
    "bearer",
    "apikey",
    "api_key",
    "x-api-key",
    "accesskey",
    "access_key",
    "privatekey",
    "private_key",
    "cookie",
    "set-cookie",
    "credential",
    "credentials",
    "ssn",
    "cvv",
];

const SUBSTRINGS: &[&str] = &[
    "password",
    "passwd",
    "secret",
    "token",
    "cookie",
    "credential",
    "bearer",
    "apikey",
    "api-key",
    "api_key",
    "privatekey",
    "private-key",
    "private_key",
    "accesskey",
    "access-key",
    "access_key",
    "signingkey",
    "signing-key",
    "signing_key",
    "encryptionkey",
    "encryption-key",
    "encryption_key",
];

const MAX_DEPTH: usize = 512;

pub fn is_sensitive_key(key: &str, extra_keys: &[String]) -> bool {
    let lower = key.to_ascii_lowercase();
    EXACT_KEYS.contains(&lower.as_str())
        || extra_keys.iter().any(|k| k.eq_ignore_ascii_case(&lower))
        || SUBSTRINGS.iter().any(|s| lower.contains(s))
}

/// Replaces values under sensitive keys with [`REDACTED`], in place.
pub fn redact_value(value: &mut Value, extra_keys: &[String]) {
    walk(value, extra_keys, 0);
}

pub fn redacted(mut value: Value, extra_keys: &[String]) -> Value {
    redact_value(&mut value, extra_keys);
    value
}

/// Redacts the bearer token, sensitive headers and sensitive extra keys of an envelope's metadata.
pub fn redact_meta(meta: &mut Meta, extra_keys: &[String]) {
    if meta.auth.as_ref().is_some_and(|a| a.token.is_some()) {
        meta.auth = Some(Auth::token(REDACTED));
    }
    if let Some(headers) = &mut meta.headers {
        for (name, value) in headers.iter_mut() {
            if is_sensitive_key(name, extra_keys) {
                *value = REDACTED.to_owned();
            }
        }
    }
    for (key, value) in meta.extra.iter_mut() {
        if is_sensitive_key(key, extra_keys) {
            *value = Value::String(REDACTED.into());
        } else {
            walk(value, extra_keys, 1);
        }
    }
}

fn walk(value: &mut Value, extra_keys: &[String], depth: usize) {
    if depth >= MAX_DEPTH {
        *value = Value::String("[TooDeep]".into());
        return;
    }
    match value {
        Value::Object(map) => {
            for (key, child) in map.iter_mut() {
                if is_sensitive_key(key, extra_keys) {
                    *child = Value::String(REDACTED.into());
                } else {
                    walk(child, extra_keys, depth + 1);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                walk(item, extra_keys, depth + 1);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn redacts_known_and_custom_keys() {
        let value = json!({
            "auth": {"token": "t"},
            "user": {"name": "n", "sessionCookie": "c", "sortKey": "k"},
            "list": [{"api_key": "x"}],
            "custom": "v"
        });
        let out = redacted(value, &["custom".to_owned()]);
        assert_eq!(out["auth"], REDACTED);
        assert_eq!(out["user"]["sessionCookie"], REDACTED);
        assert_eq!(out["user"]["sortKey"], "k");
        assert_eq!(out["list"][0]["api_key"], REDACTED);
        assert_eq!(out["custom"], REDACTED);
    }

    #[test]
    fn redacts_meta_in_place() {
        let mut meta = Meta::new();
        meta.auth = Some(Auth::token("t"));
        meta.set_header("authorization", "Bearer x");
        meta.set_header("region", "eu");
        meta.extra.insert("apiKey".into(), json!("k"));
        redact_meta(&mut meta, &[]);
        assert_eq!(meta.auth_token(), Some(REDACTED));
        assert_eq!(meta.header("authorization"), Some(REDACTED));
        assert_eq!(meta.header("region"), Some("eu"));
        assert_eq!(meta.extra["apiKey"], REDACTED);
    }
}
