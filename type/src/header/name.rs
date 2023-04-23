use serde::{Deserialize, Serialize};

macro_rules! std_header_names {
    ($(($i:ident, $upper:ident, $lower:literal, $name:literal)),+) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
        pub enum StandardHeader {
            $(#[serde(rename = $name)] $i,)+
        }
        impl StandardHeader {
            fn from_lower(value: &str) -> Option<Self> {
                match value {
                    $($lower => Some(Self::$i),)+
                    _ => None
                }
            }
        }
        $(pub const $upper: HeaderName = HeaderName::Standard(StandardHeader::$i);)+
    };
}
std_header_names!(
    (Accept, ACCEPT, "accept", "Accept"),
    (AcceptCH, ACCEPT_CH, "accept-ch", "Accept-CH"),
    (
        AcceptCHLifetime,
        ACCEPT_CH_LIFETIME,
        "accept-ch-lifetime",
        "Accept-CH-Lifetime"
    ),
    (
        AcceptEncoding,
        ACCEPT_ENCODING,
        "accept-encoding",
        "Accept-Encoding"
    ),
    (
        AcceptLanguage,
        ACCEPT_LANGUAGE,
        "accept-language",
        "Accept-Language"
    ),
    (
        AcceptPushPolicy,
        ACCEPT_PUSH_POLICY,
        "accept-push-policy",
        "Accept-Push-Policy"
    ),
    (
        AcceptRanges,
        ACCEPT_RANGES,
        "accept-ranges",
        "Accept-Ranges"
    ),
    (
        AcceptSignature,
        ACCEPT_SIGNATURE,
        "accept-signature",
        "Accept-Signature"
    ),
    (
        AccessControlAllowCredentials,
        ACCESS_CONTROL_ALLOW_CREDENTIALS,
        "access-control-allow-credentials",
        "Access-Control-Allow-Credentials"
    ),
    (
        AccessControlAllowHeaders,
        ACCESS_CONTROL_ALLOW_HEADERS,
        "access-control-allow-headers",
        "Access-Control-Allow-Headers"
    ),
    (
        AccessControlAllowMethods,
        ACCESS_CONTROL_ALLOW_METHODS,
        "access-control-allow-methods",
        "Access-Control-Allow-Methods"
    ),
    (
        AccessControlAllowOrigin,
        ACCESS_CONTROL_ALLOW_ORIGIN,
        "access-control-allow-origin",
        "Access-Control-Allow-Origin"
    ),
    (
        AccessControlExposeHeaders,
        ACCESS_CONTROL_EXPOSE_HEADERS,
        "access-control-expose-headers",
        "Access-Control-Expose-Headers"
    ),
    (
        AccessControlMaxAge,
        ACCESS_CONTROL_MAX_AGE,
        "access-control-max-age",
        "Access-Control-Max-Age"
    ),
    (
        AccessControlRequestHeaders,
        ACCESS_CONTROL_REQUEST_HEADERS,
        "access-control-request-headers",
        "Access-Control-Request-Headers"
    ),
    (
        AccessControlRequestMethod,
        ACCESS_CONTROL_REQUEST_METHOD,
        "access-control-request-method",
        "Access-Control-Request-Method"
    ),
    (Age, AGE, "age", "Age"),
    (Allow, ALLOW, "allow", "Allow"),
    (AltSvc, ALT_SVC, "alt-svc", "Alt-Svc"),
    (
        Authorization,
        AUTHORIZATION,
        "authorization",
        "Authorization"
    ),
    (
        CacheControl,
        CACHE_CONTROL,
        "cache-control",
        "Cache-Control"
    ),
    (
        ClearSiteData,
        CLEAR_SITE_DATA,
        "clear-site-data",
        "Clear-Site-Data"
    ),
    (Connection, CONNECTION, "connection", "Connection"),
    (ContentDPR, CONTENT_DPR, "content-dpr", "Content-DPR"),
    (
        ContentDisposition,
        CONTENT_DISPOSITION,
        "content-disposition",
        "Content-Disposition"
    ),
    (
        ContentEncoding,
        CONTENT_ENCODING,
        "content-encoding",
        "Content-Encoding"
    ),
    (
        ContentLanguage,
        CONTENT_LANGUAGE,
        "content-language",
        "Content-Language"
    ),
    (
        ContentLength,
        CONTENT_LENGTH,
        "content-length",
        "Content-Length"
    ),
    (
        ContentLocation,
        CONTENT_LOCATION,
        "content-location",
        "Content-Location"
    ),
    (
        ContentRange,
        CONTENT_RANGE,
        "content-range",
        "Content-Range"
    ),
    (
        ContentSecurityPolicy,
        CONTENT_SECURITY_POLICY,
        "content-security-policy",
        "Content-Security-Policy"
    ),
    (
        ContentSecurityPolicyReportOnly,
        CONTENT_SECURITY_POLICY_REPORT_ONLY,
        "content-security-policy-report-only",
        "Content-Security-Policy-Report-Only"
    ),
    (ContentType, CONTENT_TYPE, "content-type", "Content-Type"),
    (Cookie, COOKIE, "cookie", "Cookie"),
    (CriticalCH, CRITICAL_CH, "critical-ch", "Critical-CH"),
    (
        CrossOriginEmbedderPolicy,
        CROSS_ORIGIN_EMBEDDER_POLICY,
        "cross-origin-embedder-policy",
        "Cross-Origin-Embedder-Policy"
    ),
    (
        CrossOriginOpenerPolicy,
        CROSS_ORIGIN_OPENER_POLICY,
        "cross-origin-opener-policy",
        "Cross-Origin-Opener-Policy"
    ),
    (
        CrossOriginResourcePolicy,
        CROSS_ORIGIN_RESOURCE_POLICY,
        "cross-origin-resource-policy",
        "Cross-Origin-Resource-Policy"
    ),
    (DPR, DPR, "dpr", "DPR"),
    (Date, DATE, "date", "Date"),
    (
        DeviceMemory,
        DEVICE_MEMORY,
        "device-memory",
        "Device-Memory"
    ),
    (Downlink, DOWNLINK, "downlink", "Downlink"),
    (ECT, ECT, "ect", "ECT"),
    (ETag, ETAG, "etag", "ETag"),
    (EarlyData, EARLY_DATA, "early-data", "Early-Data"),
    (Expect, EXPECT, "expect", "Expect"),
    (ExpectCT, EXPECT_CT, "expect-ct", "Expect-CT"),
    (Expires, EXPIRES, "expires", "Expires"),
    (Forwarded, FORWARDED, "forwarded", "Forwarded"),
    (From, FROM, "from", "From"),
    (Host, HOST, "host", "Host"),
    (IfMatch, IF_MATCH, "if-match", "If-Match"),
    (
        IfModifiedSince,
        IF_MODIFIED_SINCE,
        "if-modified-since",
        "If-Modified-Since"
    ),
    (IfNoneMatch, IF_NONE_MATCH, "if-none-match", "If-None-Match"),
    (IfRange, IF_RANGE, "if-range", "If-Range"),
    (
        IfUnmodifiedSince,
        IF_UNMODIFIED_SINCE,
        "if-unmodified-since",
        "If-Unmodified-Since"
    ),
    (KeepAlive, KEEP_ALIVE, "keep-alive", "Keep-Alive"),
    (
        LargeAllocation,
        LARGE_ALLOCATION,
        "large-allocation",
        "Large-Allocation"
    ),
    (LastEventID, LAST_EVENT_ID, "last-event-id", "Last-Event-ID"),
    (
        LastModified,
        LAST_MODIFIED,
        "last-modified",
        "Last-Modified"
    ),
    (Link, LINK, "link", "Link"),
    (Location, LOCATION, "location", "Location"),
    (MaxForwards, MAX_FORWARDS, "max-forwards", "Max-Forwards"),
    (NEL, NEL, "nel", "NEL"),
    (Origin, ORIGIN, "origin", "Origin"),
    (
        OriginIsolation,
        ORIGIN_ISOLATION,
        "origin-isolation",
        "Origin-Isolation"
    ),
    (
        PermissionsPolicy,
        PERMISSIONS_POLICY,
        "permissions-policy",
        "Permissions-Policy"
    ),
    (PingFrom, PING_FROM, "ping-from", "Ping-From"),
    (PingTo, PING_TO, "ping-to", "Ping-To"),
    (Pragma, PRAGMA, "pragma", "Pragma"),
    (
        ProxyAuthenticate,
        PROXY_AUTHENTICATE,
        "proxy-authenticate",
        "Proxy-Authenticate"
    ),
    (
        ProxyAuthorization,
        PROXY_AUTHORIZATION,
        "proxy-authorization",
        "Proxy-Authorization"
    ),
    (PushPolicy, PUSH_POLICY, "push-policy", "Push-Policy"),
    (RTT, RTT, "rtt", "RTT"),
    (Range, RANGE, "range", "Range"),
    (Referer, REFERER, "referer", "Referer"),
    (
        ReferrerPolicy,
        REFERRER_POLICY,
        "referrer-policy",
        "Referrer-Policy"
    ),
    (Refresh, REFRESH, "refresh", "Refresh"),
    (ReportTo, REPORT_TO, "report-to", "Report-To"),
    (RetryAfter, RETRY_AFTER, "retry-after", "Retry-After"),
    (SaveData, SAVE_DATA, "save-data", "Save-Data"),
    (
        SecCHPrefersReducedMotion,
        SEC_CH_PREFERS_REDUCED_MOTION,
        "sec-ch-prefers-reduced-motion",
        "Sec-CH-Prefers-Reduced-Motion"
    ),
    (SecCHUA, SEC_CH_UA, "sec-ch-ua", "Sec-CH-UA"),
    (
        SecCHUAArch,
        SEC_CH_UA_ARCH,
        "sec-ch-ua-arch",
        "Sec-CH-UA-Arch"
    ),
    (
        SecCHUABitness,
        SEC_CH_UA_BITNESS,
        "sec-ch-ua-bitness",
        "Sec-CH-UA-Bitness"
    ),
    (
        SecCHUAFullVersion,
        SEC_CH_UA_FULL_VERSION,
        "sec-ch-ua-full-version",
        "Sec-CH-UA-Full-Version"
    ),
    (
        SecCHUAFullVersionList,
        SEC_CH_UA_FULL_VERSION_LIST,
        "sec-ch-ua-full-version-list",
        "Sec-CH-UA-Full-Version-List"
    ),
    (
        SecCHUAMobile,
        SEC_CH_UA_MOBILE,
        "sec-ch-ua-mobile",
        "Sec-CH-UA-Mobile"
    ),
    (
        SecCHUAModel,
        SEC_CH_UA_MODEL,
        "sec-ch-ua-model",
        "Sec-CH-UA-Model"
    ),
    (
        SecCHUAPlatform,
        SEC_CH_UA_PLATFORM,
        "sec-ch-ua-platform",
        "Sec-CH-UA-Platform"
    ),
    (
        SecCHUAPlatformVersion,
        SEC_CH_UA_PLATFORM_VERSION,
        "sec-ch-ua-platform-version",
        "Sec-CH-UA-Platform-Version"
    ),
    (
        SecFetchDest,
        SEC_FETCH_DEST,
        "sec-fetch-dest",
        "Sec-Fetch-Dest"
    ),
    (
        SecFetchMode,
        SEC_FETCH_MODE,
        "sec-fetch-mode",
        "Sec-Fetch-Mode"
    ),
    (
        SecFetchSite,
        SEC_FETCH_SITE,
        "sec-fetch-site",
        "Sec-Fetch-Site"
    ),
    (
        SecFetchUser,
        SEC_FETCH_USER,
        "sec-fetch-user",
        "Sec-Fetch-User"
    ),
    (
        SecWebSocketAccept,
        SEC_WEBSOCKET_ACCEPT,
        "sec-websocket-accept",
        "Sec-WebSocket-Accept"
    ),
    (
        SecWebSocketExtensions,
        SEC_WEBSOCKET_EXTENSIONS,
        "sec-websocket-extensions",
        "Sec-WebSocket-Extensions"
    ),
    (
        SecWebSocketKey,
        SEC_WEBSOCKET_KEY,
        "sec-websocket-key",
        "Sec-WebSocket-Key"
    ),
    (
        SecWebSocketProtocol,
        SEC_WEBSOCKET_PROTOCOL,
        "sec-websocket-protocol",
        "Sec-WebSocket-Protocol"
    ),
    (
        SecWebSocketVersion,
        SEC_WEBSOCKET_VERSION,
        "sec-websocket-version",
        "Sec-WebSocket-Version"
    ),
    (Server, SERVER, "server", "Server"),
    (
        ServerTiming,
        SERVER_TIMING,
        "server-timing",
        "Server-Timing"
    ),
    (
        ServiceWorkerAllowed,
        SERVICE_WORKER_ALLOWED,
        "service-worker-allowed",
        "Service-Worker-Allowed"
    ),
    (
        ServiceWorkerNavigationPreload,
        SERVICE_WORKER_NAVIGATION_PRELOAD,
        "service-worker-navigation-preload",
        "Service-Worker-Navigation-Preload"
    ),
    (SetCookie, SET_COOKIE, "set-cookie", "Set-Cookie"),
    (Signature, SIGNATURE, "signature", "Signature"),
    (
        SignedHeaders,
        SIGNED_HEADERS,
        "signed-headers",
        "Signed-Headers"
    ),
    (SourceMap, SOURCEMAP, "sourcemap", "SourceMap"),
    (
        StrictTransportSecurity,
        STRICT_TRANSPORT_SECURITY,
        "strict-transport-security",
        "Strict-Transport-Security"
    ),
    (TE, TE, "te", "TE"),
    (
        TimingAllowOrigin,
        TIMING_ALLOW_ORIGIN,
        "timing-allow-origin",
        "Timing-Allow-Origin"
    ),
    (Trailer, TRAILER, "trailer", "Trailer"),
    (
        TransferEncoding,
        TRANSFER_ENCODING,
        "transfer-encoding",
        "Transfer-Encoding"
    ),
    (Upgrade, UPGRADE, "upgrade", "Upgrade"),
    (
        UpgradeInsecureRequests,
        UPGRADE_INSECURE_REQUESTS,
        "upgrade-insecure-requests",
        "Upgrade-Insecure-Requests"
    ),
    (UserAgent, USER_AGENT, "user-agent", "User-Agent"),
    (Vary, VARY, "vary", "Vary"),
    (Via, VIA, "via", "Via"),
    (
        ViewportWidth,
        VIEWPORT_WIDTH,
        "viewport-width",
        "Viewport-Width"
    ),
    (
        WWWAuthenticate,
        WWW_AUTHENTICATE,
        "www-authenticate",
        "WWW-Authenticate"
    ),
    (Warning, WARNING, "warning", "Warning"),
    (Width, WIDTH, "width", "Width"),
    (
        XContentTypeOptions,
        X_CONTENT_TYPE_OPTIONS,
        "x-content-type-options",
        "X-Content-Type-Options"
    ),
    (
        XDNSPrefetchControl,
        X_DNS_PREFETCH_CONTROL,
        "x-dns-prefetch-control",
        "X-DNS-Prefetch-Control"
    ),
    (
        XFirefoxSpdy,
        X_FIREFOX_SPDY,
        "x-firefox-spdy",
        "X-Firefox-Spdy"
    ),
    (
        XForwardedFor,
        X_FORWARDED_FOR,
        "x-forwarded-for",
        "X-Forwarded-For"
    ),
    (
        XForwardedHost,
        X_FORWARDED_HOST,
        "x-forwarded-host",
        "X-Forwarded-Host"
    ),
    (
        XForwardedProto,
        X_FORWARDED_PROTO,
        "x-forwarded-proto",
        "X-Forwarded-Proto"
    ),
    (
        XFrameOptions,
        X_FRAME_OPTIONS,
        "x-frame-options",
        "X-Frame-Options"
    ),
    (
        XPermittedCrossDomainPolicies,
        X_PERMITTED_CROSS_DOMAIN_POLICIES,
        "x-permitted-cross-domain-policies",
        "X-Permitted-Cross-Domain-Policies"
    ),
    (XPingback, X_PINGBACK, "x-pingback", "X-Pingback"),
    (XPoweredBy, X_POWERED_BY, "x-powered-by", "X-Powered-By"),
    (
        XRequestedWith,
        X_REQUESTED_WITH,
        "x-requested-with",
        "X-Requested-With"
    ),
    (XRobotsTag, X_ROBOTS_TAG, "x-robots-tag", "X-Robots-Tag"),
    (
        XXSSProtection,
        X_XSS_PROTECTION,
        "x-xss-protection",
        "X-XSS-Protection"
    )
);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum HeaderName {
    Standard(StandardHeader),
    Custom(Box<str>),
}
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct InvalidHeaderName(http::header::InvalidHeaderName);

impl HeaderName {
    pub fn parse(name: &[u8]) -> Result<Self, InvalidHeaderName> {
        Ok(Self::from_lower(
            http::HeaderName::from_bytes(name)
                .map_err(InvalidHeaderName)?
                .as_str(),
        ))
    }
    pub fn from_lower(data: &str) -> Self {
        match StandardHeader::from_lower(data) {
            Some(v) => Self::Standard(v),
            None => Self::Custom(data.to_string().into_boxed_str()),
        }
    }
}
