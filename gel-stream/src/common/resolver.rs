use std::borrow::Cow;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::{future::Future, str::FromStr, task::Poll};

use crate::{MaybeResolvedTarget, ResolvedTarget, TargetName, TcpResolve};

#[cfg(feature = "hickory")]
fn net_error(err: hickory_resolver::net::NetError) -> std::io::Error {
    std::io::Error::other(err)
}

#[cfg(feature = "hickory")]
fn build_hickory_resolver() -> Result<hickory_resolver::TokioResolver, std::io::Error> {
    let mut builder = hickory_resolver::Resolver::builder_tokio().map_err(net_error)?;
    // hickory 0.26 changed the default lookup strategy to `Ipv6AndIpv4`, which
    // returns IPv6 addresses first. We only use the first resolved address and do
    // not fall back to the next, so preserve the pre-0.26 `Ipv4thenIpv6` behavior
    // to avoid breaking IPv4-only listeners (e.g. a local server bound to 127.0.0.1).
    builder.options_mut().ip_strategy = hickory_resolver::config::LookupIpStrategy::Ipv4thenIpv6;
    builder.build().map_err(net_error)
}

#[cfg(feature = "tokio")]
fn select_preferred_socket_addr(
    addrs: impl Iterator<Item = SocketAddr>,
) -> std::io::Result<ResolvedTarget> {
    let mut first = None;
    for addr in addrs {
        if addr.is_ipv4() {
            return Ok(ResolvedTarget::SocketAddr(addr));
        }
        if first.is_none() {
            first = Some(addr);
        }
    }
    first
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "No address found"))
        .map(ResolvedTarget::SocketAddr)
}

#[cfg(feature = "tokio")]
async fn resolve_host_to_socket_addrs(host: String, port: u16) -> std::io::Result<ResolvedTarget> {
    let res = tokio::task::spawn_blocking(move || (host.as_str(), port).to_socket_addrs())
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Interrupted, e.to_string()))??;
    select_preferred_socket_addr(res)
}

/// An async resolver for hostnames to IP addresses.
#[derive(Clone, Default)]
pub struct Resolver {
    #[cfg(feature = "hickory")]
    resolver: std::sync::Arc<std::sync::OnceLock<Option<hickory_resolver::TokioResolver>>>,
}

impl Resolver {
    /// Create a new resolver.
    pub fn new() -> Result<Self, std::io::Error> {
        Ok(Self {
            #[cfg(feature = "hickory")]
            resolver: std::sync::Arc::new(std::sync::OnceLock::new()),
        })
    }

    pub(crate) fn resolve_remote(
        &self,
        host: &MaybeResolvedTarget,
    ) -> ResolveResult<ResolvedTarget> {
        match host {
            MaybeResolvedTarget::Resolved(resolved) => {
                ResolveResult::new_sync(Ok(resolved.clone()))
            }
            MaybeResolvedTarget::Unresolved(host, port, _) => {
                if let Ok(ip) = IpAddr::from_str(host) {
                    ResolveResult::new_sync(Ok(ResolvedTarget::SocketAddr(SocketAddr::from((
                        ip, *port,
                    )))))
                } else {
                    #[cfg(feature = "hickory")]
                    {
                        let resolver = self.resolver.clone();
                        let host = host.to_string();
                        let port = *port;
                        ResolveResult::new_async(async move {
                            if let Some(resolver) =
                                resolver.get_or_init(|| build_hickory_resolver().ok())
                            {
                                if let Ok(lookup) = resolver.lookup_ip(&host).await {
                                    if let Some(addr) = lookup.iter().next() {
                                        return Ok(ResolvedTarget::SocketAddr(SocketAddr::new(
                                            addr, port,
                                        )));
                                    }
                                }
                            }
                            resolve_host_to_socket_addrs(host, port).await
                        })
                    }
                    #[cfg(all(feature = "tokio", not(feature = "hickory")))]
                    {
                        ResolveResult::new_async(resolve_host_to_socket_addrs(
                            host.to_string(),
                            *port,
                        ))
                    }
                    #[cfg(not(feature = "tokio"))]
                    {
                        ResolveResult::new_sync(Err(std::io::Error::new(
                            std::io::ErrorKind::Unsupported,
                            "No resolver available",
                        )))
                    }
                }
            }
        }
    }
}

/// The result of a resolution. It may be synchronous or asynchronous, but you
/// can always call `.await` on it.
pub struct ResolveResult<T> {
    inner: ResolveResultInner<T>,
}

impl<T> ResolveResult<T> {
    fn new_sync(result: Result<T, std::io::Error>) -> Self {
        Self {
            inner: ResolveResultInner::Sync(result),
        }
    }

    fn new_async(future: impl Future<Output = std::io::Result<T>> + Send + 'static) -> Self {
        Self {
            inner: ResolveResultInner::Async(Box::pin(future)),
        }
    }

    pub fn sync(&mut self) -> Result<Option<T>, std::io::Error> {
        if let ResolveResultInner::Sync(_) = &mut self.inner {
            let this = std::mem::replace(&mut self.inner, ResolveResultInner::Fused);
            let ResolveResultInner::Sync(result) = this else {
                unreachable!()
            };
            result.map(Some)
        } else {
            Ok(None)
        }
    }

    pub fn map<U>(self, f: impl (FnOnce(T) -> U) + Send + 'static) -> ResolveResult<U>
    where
        T: 'static,
    {
        match self.inner {
            ResolveResultInner::Sync(Ok(t)) => ResolveResult::new_sync(Ok(f(t))),
            ResolveResultInner::Sync(Err(e)) => ResolveResult::new_sync(Err(e)),
            ResolveResultInner::Async(future) => {
                ResolveResult::new_async(async move { Ok(f(future.await?)) })
            }
            ResolveResultInner::Fused => ResolveResult::new_sync(Err(std::io::Error::other(
                "Polled a previously awaited result",
            ))),
        }
    }
}

enum ResolveResultInner<T> {
    Sync(Result<T, std::io::Error>),
    Async(std::pin::Pin<Box<dyn Future<Output = std::io::Result<T>> + Send>>),
    Fused,
}

impl<T> Future for ResolveResult<T>
where
    Self: Unpin,
{
    type Output = std::io::Result<T>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.get_mut();
        match &mut this.inner {
            ResolveResultInner::Sync(_) => {
                let this = std::mem::replace(&mut this.inner, ResolveResultInner::Fused);
                let ResolveResultInner::Sync(result) = this else {
                    unreachable!()
                };
                Poll::Ready(result)
            }
            ResolveResultInner::Async(future) => future.as_mut().poll(cx),
            ResolveResultInner::Fused => {
                panic!("Polled a previously awaited result")
            }
        }
    }
}

/// A trait for types that can be resolved to a target.
pub trait Resolvable {
    type Target;

    fn resolve(&self, resolver: &Resolver) -> ResolveResult<Self::Target>;
}

impl Resolvable for String {
    type Target = IpAddr;

    fn resolve(&self, resolver: &Resolver) -> ResolveResult<Self::Target> {
        resolver
            .resolve_remote(&MaybeResolvedTarget::Unresolved(
                Cow::Owned(self.clone()),
                0,
                None,
            ))
            .map(|target| match target {
                ResolvedTarget::SocketAddr(addr) => addr.ip(),
                _ => unreachable!(),
            })
    }
}

impl<T: TcpResolve + Clone> Resolvable for T {
    type Target = ResolvedTarget;

    fn resolve(&self, resolver: &Resolver) -> ResolveResult<Self::Target> {
        resolver.resolve_remote(&self.clone().into())
    }
}

impl Resolvable for TargetName {
    type Target = ResolvedTarget;

    fn resolve(&self, resolver: &Resolver) -> ResolveResult<Self::Target> {
        resolver.resolve_remote(self.maybe_resolved())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::*;

    #[tokio::test]
    async fn test_resolve_remote() {
        let resolver = Resolver::new().unwrap();
        let target = TargetName::new_tcp(("localhost", 8080));
        let result = target.resolve(&resolver).await.unwrap();
        assert_eq!(
            result,
            ResolvedTarget::SocketAddr(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                8080
            ))
        );
    }

    #[tokio::test]
    async fn test_resolve_lazy_for_ip_and_unix() {
        let resolver = Resolver::new().unwrap();
        #[cfg(feature = "hickory")]
        assert!(resolver.resolver.get().is_none());

        let ip_target = TargetName::new_tcp(("127.0.0.1", 8080));
        let result = ip_target.resolve(&resolver).await.unwrap();
        assert_eq!(
            result,
            ResolvedTarget::SocketAddr(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                8080
            ))
        );
        #[cfg(feature = "hickory")]
        assert!(resolver.resolver.get().is_none());

        #[cfg(unix)]
        {
            let unix_target = TargetName::new_unix_path("/tmp/test.sock").unwrap();
            let _ = unix_target.resolve(&resolver).await.unwrap();
            #[cfg(feature = "hickory")]
            assert!(resolver.resolver.get().is_none());
        }
    }

    #[tokio::test]
    async fn test_resolve_fallback_when_hickory_unavailable() {
        let resolver = Resolver::new().unwrap();
        #[cfg(feature = "hickory")]
        {
            // Simulate missing/invalid /etc/resolv.conf by pre-populating OnceLock with None
            let _ = resolver.resolver.set(None);
        }
        let target = TargetName::new_tcp(("localhost", 8080));
        let result = target.resolve(&resolver).await.unwrap();
        assert_eq!(
            result,
            ResolvedTarget::SocketAddr(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                8080
            ))
        );
    }

    #[cfg(feature = "__manual_tests")]
    #[tokio::test]
    async fn test_resolve_real_domain() {
        let resolver = Resolver::new().unwrap();
        let target = TargetName::new_tcp(("www.google.com", 443));
        let result = target.resolve(&resolver).await.unwrap();
        println!("{result:?}");
    }
}
