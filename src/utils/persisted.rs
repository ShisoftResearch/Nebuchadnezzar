//! Present persisted bytes must decode. Absent state starts empty.
//! Recovery hooks halt with a typed error because the raft trait returns `()`.

use bifrost::utils;
use serde::Deserialize;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedDecodeError {
    /// Which persisted state failed, e.g. `"id allocator snapshot"`.
    pub what: &'static str,
    pub type_name: &'static str,
    pub bytes: usize,
}

impl fmt::Display for PersistedDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "persisted {} ({} bytes) cannot be decoded as {}; rebuild the store",
            self.what, self.bytes, self.type_name
        )
    }
}

impl std::error::Error for PersistedDecodeError {}

impl From<PersistedDecodeError> for std::io::Error {
    fn from(e: PersistedDecodeError) -> Self {
        std::io::Error::new(std::io::ErrorKind::InvalidData, e)
    }
}

/// Decodes present persisted bytes. `None` from the codec becomes an error.
pub fn decode_persisted<'a, T>(
    what: &'static str,
    data: &'a [u8],
) -> Result<T, PersistedDecodeError>
where
    T: Deserialize<'a>,
{
    utils::serde::deserialize::<T>(data).ok_or(PersistedDecodeError {
        what,
        type_name: std::any::type_name::<T>(),
        bytes: data.len(),
    })
}

/// For state machine `recover` hooks, whose signature cannot return an error
/// (it belongs to the raft trait): halts recovery with the typed error as the
/// panic payload. Callers can inspect its concrete error type.
pub fn decode_persisted_or_halt<'a, T>(what: &'static str, data: &'a [u8]) -> T
where
    T: Deserialize<'a>,
{
    match decode_persisted(what, data) {
        Ok(value) => value,
        Err(e) => {
            error!("{}", e);
            std::panic::panic_any(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn undecodable_bytes_report_what_type_and_length() {
        let err = decode_persisted::<Vec<u64>>("test state", b"not cbor at all").unwrap_err();
        assert_eq!(err.what, "test state");
        assert_eq!(err.bytes, 15);
        assert!(err.type_name.contains("Vec<u64>"));
        let text = err.to_string();
        assert!(text.contains("15 bytes") && text.contains("test state"));
    }

    #[test]
    fn valid_bytes_decode() {
        let bytes = utils::serde::serialize(&vec![1u64, 2, 3]);
        assert_eq!(
            decode_persisted::<Vec<u64>>("test state", &bytes).unwrap(),
            vec![1, 2, 3]
        );
    }
}
