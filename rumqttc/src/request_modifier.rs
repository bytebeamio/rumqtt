//! Request modifier types for websocket connections.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// A boxed future returned by the request modifier function.
type ModifierFuture = Pin<
    Box<
        dyn Future<Output = Result<http::Request<()>, Box<dyn std::error::Error + Send + Sync>>>
            + Send,
    >,
>;

/// The stored request modifier closure.
pub(crate) type RequestModifierFn = Arc<dyn Fn(http::Request<()>) -> ModifierFuture + Send + Sync>;

/// Trait to convert request modifier output to Result, enabling backwards compatibility.
/// Accepts both `http::Request<()>` (infallible) and `Result<http::Request<()>, E>` (fallible).
pub trait IntoModifierResult {
    type Error: std::error::Error + Send + Sync + 'static;
    fn into_modifier_result(self) -> Result<http::Request<()>, Self::Error>;
}

impl IntoModifierResult for http::Request<()> {
    type Error = std::convert::Infallible;
    fn into_modifier_result(self) -> Result<http::Request<()>, Self::Error> {
        Ok(self)
    }
}

impl<E: std::error::Error + Send + Sync + 'static> IntoModifierResult
    for Result<http::Request<()>, E>
{
    type Error = E;
    fn into_modifier_result(self) -> Result<http::Request<()>, Self::Error> {
        self
    }
}
