use serde::{Deserialize, Serialize};

macro_rules! wire_enum {
    ($name:ident { $($variant:ident => $wire:literal),+ $(,)? }) => {
        #[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
        pub enum $name { $(#[serde(rename = $wire)] $variant),+ }
        impl $name { pub const fn as_str(self) -> &'static str { match self { $(Self::$variant => $wire),+ } } }
        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str(self.as_str()) }
        }
        impl PartialEq<&str> for $name { fn eq(&self, other: &&str) -> bool { self.as_str() == *other } }
    };
}
wire_enum!(ErrorCode {
    InvalidRequest => "invalid_request", ProtocolMismatch => "protocol_mismatch",
    Unauthorized => "unauthorized", Forbidden => "forbidden", NotFound => "not_found",
    MethodNotAllowed => "method_not_allowed", Gone => "gone", Conflict => "conflict",
    StaleGeneration => "stale_generation", Gap => "gap", CapabilityUnavailable => "capability_unavailable",
    CapacityExceeded => "capacity_exceeded", PayloadTooLarge => "payload_too_large",
    UpstreamUnavailable => "upstream_unavailable", ResultUnknown => "result_unknown",
    Rejected => "rejected", InternalError => "internal_error", PreconditionFailed => "precondition_failed",
    NotCanonical => "not_canonical",
});
wire_enum!(RetryAction {
    None => "none", SameRequest => "same-request", QueryStatus => "query-status",
    Rebind => "rebind", Refresh => "refresh", Rediscover => "rediscover",
});
