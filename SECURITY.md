# Security policy

Report suspected vulnerabilities privately to the repository owner at
assurrussa@yandex.ru. Avoid posting live credentials, personal data or working
exploits in public issues. Include the affected commit, configuration and a
minimal redacted reproduction. No response-time SLA is promised.

The library is not an authentication or authorization system. Applications must
verify a principal before upgrade, authorize each domain operation, manage
session revocation, terminate idle/revoked connections and use TLS in production.
Origin checks complement browser session protection; they do not authenticate
native clients. Do not use an all-origins wildcard for authenticated endpoints.

Set connection and byte budgets appropriate to available memory. Queue byte
counters cover serialized payload, not total heap. Application codecs, domain
callbacks and injected transports remain trusted code and must be bounded and
cooperative. Timeout cancellation cannot forcibly terminate arbitrary Go code.

Invalid client messages are rejected without logging their bodies. Do not add
payloads, cookies, panic values or high-cardinality user IDs to telemetry.
Security fixes are reviewed against the current development branch; production
users must pin a reviewed release/commit and track its compatibility notes.
