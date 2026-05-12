use std::fmt;

const REDACTED: &str = "[REDACTED]";

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum SecretFieldErrorKind {
    Empty,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct SecretFieldError {
    field: &'static str,
    kind: SecretFieldErrorKind,
}

impl SecretFieldError {
    pub const fn empty(field: &'static str) -> Self {
        Self {
            field,
            kind: SecretFieldErrorKind::Empty,
        }
    }

    pub const fn field(&self) -> &'static str {
        self.field
    }

    pub const fn kind(&self) -> SecretFieldErrorKind {
        self.kind
    }
}

impl fmt::Display for SecretFieldError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind {
            SecretFieldErrorKind::Empty => {
                write!(formatter, "secret field `{}` must not be empty", self.field)
            }
        }
    }
}

impl std::error::Error for SecretFieldError {}

#[derive(Clone, Eq, PartialEq, Hash)]
pub struct SecretString(String);

impl SecretString {
    pub fn new(field: &'static str, value: impl Into<String>) -> Result<Self, SecretFieldError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(SecretFieldError::empty(field));
        }

        Ok(Self(value))
    }

    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("SecretString")
            .field(&REDACTED)
            .finish()
    }
}

impl fmt::Display for SecretString {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(REDACTED)
    }
}

macro_rules! secret_newtype {
    ($name:ident, $field:literal) => {
        #[derive(Clone, Eq, PartialEq, Hash)]
        pub struct $name(SecretString);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, SecretFieldError> {
                SecretString::new($field, value).map(Self)
            }

            pub fn expose_secret(&self) -> &str {
                self.0.expose_secret()
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter
                    .debug_tuple(stringify!($name))
                    .field(&REDACTED)
                    .finish()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(REDACTED)
            }
        }
    };
}

secret_newtype!(ApiKey, "api_key");
secret_newtype!(CookieSecret, "cookie");
secret_newtype!(NotificationToken, "notification_token");
secret_newtype!(Passkey, "passkey");
secret_newtype!(Password, "password");

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct SanitizedUrl(String);

impl SanitizedUrl {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SanitizedUrl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

pub fn sanitize_url_for_logging(value: impl AsRef<str>) -> SanitizedUrl {
    let value = value.as_ref();
    let (without_fragment, fragment) = split_once(value, '#');
    let (before_query, query) = split_once(without_fragment, '?');

    let mut sanitized = redact_userinfo(before_query);

    if let Some(query) = query {
        sanitized.push('?');
        sanitized.push_str(&sanitize_query(query));
    }

    if fragment.is_some() {
        sanitized.push('#');
        sanitized.push_str(REDACTED);
    }

    SanitizedUrl(sanitized)
}

fn split_once(value: &str, delimiter: char) -> (&str, Option<&str>) {
    match value.split_once(delimiter) {
        Some((head, tail)) => (head, Some(tail)),
        None => (value, None),
    }
}

fn redact_userinfo(value: &str) -> String {
    let Some((scheme, rest)) = value.split_once("://") else {
        return value.to_owned();
    };

    let (authority, path) = split_once(rest, '/');

    if !authority.contains('@') {
        return value.to_owned();
    }

    let mut sanitized = String::with_capacity(value.len() + REDACTED.len());
    sanitized.push_str(scheme);
    sanitized.push_str("://");
    sanitized.push_str(REDACTED);
    sanitized.push('@');
    sanitized.push_str(
        authority
            .rsplit_once('@')
            .map_or(authority, |parts| parts.1),
    );
    if let Some(path) = path {
        sanitized.push('/');
        sanitized.push_str(path);
    }
    sanitized
}

fn sanitize_query(query: &str) -> String {
    query
        .split('&')
        .map(sanitize_query_pair)
        .collect::<Vec<_>>()
        .join("&")
}

fn sanitize_query_pair(pair: &str) -> String {
    let (key, value) = split_once(pair, '=');
    if is_sensitive_query_key(key) {
        format!("{key}={REDACTED}")
    } else if value.is_some() {
        pair.to_owned()
    } else {
        key.to_owned()
    }
}

fn is_sensitive_query_key(key: &str) -> bool {
    let normalized = key
        .bytes()
        .filter(|byte| byte.is_ascii_alphanumeric())
        .map(|byte| byte.to_ascii_lowercase())
        .collect::<Vec<_>>();

    matches!(
        normalized.as_slice(),
        b"apikey"
            | b"api_token"
            | b"apitoken"
            | b"authorization"
            | b"auth"
            | b"cookie"
            | b"key"
            | b"passkey"
            | b"password"
            | b"passwd"
            | b"secret"
            | b"token"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_string_redacts_debug_and_display() {
        let secret = SecretString::new("api_key", "super-secret").unwrap();

        assert_eq!("super-secret", secret.expose_secret());
        assert_eq!(REDACTED, secret.to_string());
        assert_eq!("SecretString(\"[REDACTED]\")", format!("{secret:?}"));
    }

    #[test]
    fn typed_secrets_redact_common_secret_shapes() {
        let api_key = ApiKey::new("abc123").unwrap();
        let cookie = CookieSecret::new("sid=abc123").unwrap();
        let token = NotificationToken::new("token-123").unwrap();
        let passkey = Passkey::new("passkey-123").unwrap();
        let password = Password::new("password-123").unwrap();

        assert_eq!(REDACTED, api_key.to_string());
        assert_eq!(REDACTED, cookie.to_string());
        assert_eq!(REDACTED, token.to_string());
        assert_eq!(REDACTED, passkey.to_string());
        assert_eq!(REDACTED, password.to_string());
        assert_eq!("ApiKey(\"[REDACTED]\")", format!("{api_key:?}"));
        assert_eq!("CookieSecret(\"[REDACTED]\")", format!("{cookie:?}"));
    }

    #[test]
    fn validation_errors_identify_field_without_secret_value() {
        let error = ApiKey::new("   ").unwrap_err();

        assert_eq!("api_key", error.field());
        assert_eq!(SecretFieldErrorKind::Empty, error.kind());
        assert_eq!(
            "secret field `api_key` must not be empty",
            error.to_string()
        );
        assert!(!error.to_string().contains("super-secret"));
    }

    #[test]
    fn url_sanitizer_redacts_userinfo_query_secrets_and_fragments() {
        let sanitized = sanitize_url_for_logging(
            "https://user:password@indexer.example/api?apikey=abc&passkey=def&t=search#token",
        );

        assert_eq!(
            "https://[REDACTED]@indexer.example/api?apikey=[REDACTED]&passkey=[REDACTED]&t=search#[REDACTED]",
            sanitized.as_str()
        );
    }

    #[test]
    fn url_sanitizer_handles_bare_query_tokens() {
        let sanitized = sanitize_url_for_logging("https://example.invalid/hook?token&ok=true");

        assert_eq!(
            "https://example.invalid/hook?token=[REDACTED]&ok=true",
            sanitized.as_str()
        );
    }
}
