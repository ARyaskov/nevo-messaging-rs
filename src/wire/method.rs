//! Method names with an optional `@version` suffix (`user.getById@v2`).

pub const DEFAULT_VERSION: &str = "v1";

/// A method name split into its base name and optional version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Method<'a> {
    pub name: &'a str,
    pub version: Option<&'a str>,
}

impl<'a> Method<'a> {
    pub fn parse(method: &'a str) -> Self {
        match method.rfind('@') {
            Some(at) => Self {
                name: &method[..at],
                version: Some(&method[at + 1..]),
            },
            None => Self {
                name: method,
                version: None,
            },
        }
    }

    /// The version to dispatch on when the caller left it unspecified.
    pub fn version_or_default(&self) -> &'a str {
        self.version.unwrap_or(DEFAULT_VERSION)
    }
}

pub fn format(name: &str, version: Option<&str>) -> String {
    match version {
        Some(v) if !v.is_empty() => format!("{name}@{v}"),
        _ => name.to_owned(),
    }
}

/// Appends `version` unless the method already carries one.
pub fn with_version(method: &str, version: &str) -> String {
    if method.contains('@') {
        method.to_owned()
    } else {
        format(method, Some(version))
    }
}

pub fn is_compatible(want: Option<&str>, have: Option<&str>) -> bool {
    match (want, have) {
        (None, _) => true,
        (Some(w), None) => w == DEFAULT_VERSION,
        (Some(w), Some(h)) => w == h,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_version_suffix() {
        let m = Method::parse("user.getById@v2");
        assert_eq!(m.name, "user.getById");
        assert_eq!(m.version, Some("v2"));
        let m = Method::parse("user.getById");
        assert_eq!(m.version, None);
        assert_eq!(m.version_or_default(), "v1");
    }

    #[test]
    fn formats_and_keeps_existing_version() {
        assert_eq!(with_version("a.b", "v1"), "a.b@v1");
        assert_eq!(with_version("a.b@v3", "v1"), "a.b@v3");
        assert_eq!(format("a.b", None), "a.b");
    }

    #[test]
    fn compatibility_rules() {
        assert!(is_compatible(None, Some("v9")));
        assert!(is_compatible(Some("v1"), None));
        assert!(!is_compatible(Some("v2"), None));
        assert!(is_compatible(Some("v2"), Some("v2")));
        assert!(!is_compatible(Some("v2"), Some("v1")));
    }
}
