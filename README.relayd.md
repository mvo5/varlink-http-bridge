# varlink-relayd

Plan for a relay that makes a `varlink-httpd` reachable when it runs behind
NAT or a firewall that only lets HTTPS out.

## Terminology

- **node**: the machine behind the NAT/firewall. It runs `varlink-httpd`
  and dials out to the relay to keep the tunnel open.
- **relay**: the publicly reachable `varlink-relayd`. One listener accepts
  node dial-outs, the other accepts caller `CONNECT`s; it splices the two
  together and forwards opaque bytes.
- **caller**: whatever talks to the relay to make varlink calls on a node:
  `curl`, `varlinkctl-http`, a dashboard backend. A browser is never a
  caller directly (it cannot issue `CONNECT`); it talks to a backend that
  is one.

## Overview

The node's `varlink-httpd` dials out to the relay over a single WebSocket
and keeps it open. A caller reaches the node by sending
`CONNECT <id>` to the relay, which splices the caller onto that node's
connection. From then on the relay forwards opaque bytes.

```
[caller] ================ one end-to-end TLS session ================┐
   │ CONNECT <id>                                                  ▼
[relay] -- opaque bytes over the node's WebSocket --> [node: varlink-httpd]
  routes by name,                                       terminates TLS,
  never terminates the inner TLS                        does auth
```

Because the caller's TLS session terminates in the node's bridge, the relay
is content-blind: every bridge auth method (mTLS, ssh signatures with
channel binding, bearer tokens) works unchanged, and the relay can neither
read nor forge calls.

```console
$ curl --proxytunnel -x http://relay:8444 \
    -H 'Authorization: Bearer ...' \
    -X POST https://<id>/call/io.systemd.Hostname.Describe \
    -H 'content-type: application/json' -d '{}'
```

`<id>` is always the `app_machine_id`, i.e. the tunnel app specific
hash of the machine_id of the node (see "Node id"). Friendly names are
caller-side sugar and never reach the relay.

## Scope

Kept deliberately small: one process, no revocation beyond deleting a
key file, one tunnel per bridge instance: a host may run several
bridges, each dialing out under its own per-instance id (see "Node
id").
The relay holds no shared state beyond its node key store, so N
processes fronted by an L7 load balancer that redirects on the node id
could be used to shard (see "Node id"); ids are uniformly distributed,
so even plain modulo works.

## Security model

The end-to-end TLS session caps both untrusted parties. A malicious
**relay** never sees plaintext: it can refuse or misroute a connection,
but not read or forge calls. A malicious **node** claiming another node's
id receives the caller's TLS handshake, which it cannot complete
because it does not hold that node's key, so the caller fails instead of
talking to an impostor. Node authentication on the relay therefore
protects **availability** (no id squatting or hijack), not
confidentiality.

This argument holds only if:

1. the bridge behind the tunnel runs TLS (plaintext through the relay is
   readable and forgeable by any node), and
2. the caller validates the node's TLS identity (CA chain or pinning).

All caller authentication happens through the regular `varlink-httpd`
mechanisms, end-to-end inside the TLS session the relay cannot see; the
relay never authenticates callers. What the relay authenticates is which
machine owns a tunnel, in one of two ways:

**`--insecure`:** the node just asserts its id. Requires the explicit
flag, warns at startup, binds the caller listener to loopback by
default. For testing and trusted networks; anyone who can reach the node
listener can squat any id.

**Signed node ids (device key, the default):** each machine holds a
public/private keypair and proves possession on every connect. The
proof rides in the dial-out request: alongside its id the node sends its
public key and a signature over the id, the key, and the TLS channel
binding of the connection it is opening. Binding to the session keeps
registration a single request and still means no clocks, no freshness
window and no replay cache.

```http
GET /v1/tunnel?node_id=0f8cb1e4a6d24f7c9b3e5a1d7c0b4e26
              &key=<node public key>
              &sig=<signature over id, key and channel binding>
```

Without `key` and `sig` it is the same request, so there is nothing to
negotiate: a relay that wants signatures answers `401` when they are
missing, an `--insecure` one ignores them, and a node that always signs
works against either.

The relay accepts only if `sig` verifies against `key`, `key` is the one
bound to `id` (pinned or first use), and the binding matches its own
side of the session; anything else closes the connection. Both ends
export the binding from their own side of the TLS 1.3 session and it
never goes on the wire, so a signature lifted from a log is useless on
any other connection. Signing therefore needs `wss://`; plain `ws://`
has nothing to bind to and is for `--insecure` only. The suite and
encoding are that step's to fix.

The proof is an application-level signature rather than a TLS client
certificate on purpose: a client certificate would wire the key into the
TLS handshake (an openssl provider on the node), which is heavy for a
TPM-resident key, while signing one value is an operation a TPM does
natively. The signer is therefore a trait: a software implementation
(PEM key file) now, a TPM-backed one slots in later without touching
the protocol.

What binds an id to a key (an id in both stores with different keys is
a hard error, see "Reconnects and collisions"):

- **pinned keys** (`/etc/varlink-relayd/nodes.d/<id>.pem`): the key
  must match the listed one. Guarantee: this machine is on my list.
  A control plane that already knows the binding can populate this.
- **first use** (`/var/lib/varlink-relayd/nodes.d/<id>.pem`,
  written automatically): the first connection records the key, later
  connections must present the same one. Zero config. Guarantee: this is
  the same machine that first claimed the id. Re-keying or revoking is
  deleting the file. First use is a convenience for small manual
  deployments and off by default.

## Node key

The bridge only consumes the key, resolved as `--device-key` > systemd
credential > a default path under `/var/lib`. Generating one is a
documented one-liner (or a small helper we ship); an OS image can create
it at first boot and hand it in as a credential.

The signature algorithm is one fixed suite, no negotiation, picked from
what TPMs implement natively; the exact suite and encoding are an
implementation detail of the signed-node-id step.

## Node id

The id is always the **application-specific machine ID** (the
`app_machine_id` from the Overview); there is no override. The raw
`/etc/machine-id` is confidential per `machine-id(5)` and must not go on
the wire. The derivation matches `sd_id128_get_machine_app_specific()`:

1. `HMAC-SHA256(key = machine id, msg = app id)`, both 16 bytes
2. take the first 16 bytes
3. `bytes[6] = (bytes[6] & 0x0F) | 0x40`, `bytes[8] = (bytes[8] & 0x3F) | 0x80`

Being wire-compatible means `systemd-id128 machine-id -a <app-id>` prints
a node's id. Needs one fixed app-id UUID for the bridge, stable forever.

A host can run more than one bridge with different policies, e.g. an
additional unauthenticated one exposing only a few harmless sockets (an
update trigger, basic host information). Each instance dials out itself
and registers under its own id: the default instance uses the fixed app
id, a named instance (`--instance update`) derives its app id from the
fixed one plus the label, then applies the same machine-specific
derivation. The relay knows nothing about instances, only ids; an
unauthenticated instance relaxes only who may call, not the transport,
so the security model applies unchanged.

The id appears in the `CONNECT` authority and as a SAN in the node's
certificate; 32 hex chars is a valid DNS label, so ordinary hostname
verification works.

Friendly names are pure sugar, resolved caller-side before connecting (a
small alias file for CLI use, or the dashboard's database), so TLS
verification stays anchored on the id. The relay may use aliases for log
output, but never for routing; a relay-side routing alias would break
TLS verification.

The node also sends its id in the dial-out query string so an L7 load
balancer can hash on it without parsing the tunnel.

Registering is therefore nothing but the WebSocket upgrade, carrying the
node id (and its signature when one is wanted, see "Security model"):

```http
GET /v1/tunnel?node_id=0f8cb1e4a6d24f7c9b3e5a1d7c0b4e26 HTTP/1.1
Host: relay.example
Upgrade: websocket
```

A `101` means the id is reserved and the tunnel is up; the h2 session
runs inside that WebSocket from then on. Otherwise the relay answers
`404` (not the tunnel path), `400` (no `node_id=`, or not 32 hex chars) or
`409` (another live connection already holds that id, see "Reconnects
and collisions"), `401` (a signature was required) or `302` (this is a
balancer, the shard that owns the id is elsewhere).

The `302` is for sharding. Tunnels are long-lived, so a balancer that
proxied them would hold two connections per node for as long as each
one lasts; redirecting costs it only the upgrade, which scales with the
reconnect rate rather than the fleet size.

```
node -> lb       GET /v1/tunnel?node_id=<id>&key=<key>&sig=<over lb session>
lb   -> node     302 Location: wss://shard3.relay.example/v1/tunnel?node_id=<id>
node -> shard3   GET /v1/tunnel?node_id=<id>&key=<key>&sig=<over shard3 session>
shard3 -> node   101
```

Shards are therefore reachable directly and verify signatures on a session they
terminated themselves; the node signs again for the second connection
and follows one hop, `wss://` only. Caching the shard to skip the front
door is an optimization: a stale one turns the node away, the front door
redirects it again, and a reshard heals itself that way.

## Shape

The node side is a new listener in `varlink-httpd`, not a separate
process. `AsyncTlsListener<L>` is generic over the inner listener, so
`--relay` composes as `AsyncTlsListener<DialOutListener>` and all
existing auth paths apply untouched. No local TCP listener is exposed.

`varlink-relayd` is a separate binary with its own size gate.

## Milestones

| # | Status | Step |
|---|---|---|
| 1 | [x] | `varlink-relayd`: `--bind`, `--connect-bind`, CONNECT demux, node registry, h2 PING heartbeat, TLS, `--insecure` guard rails, tested against a stub node |
| 2 | [x] | `varlink-httpd --relay <url>`: dial-out with redial and backoff, `--instance <label>`, end-to-end test against `varlink-relayd` |
| 3 | [x] | fairness: a connection window sized for `MAX_TUNNEL_STREAMS`, a slot timeout and a `503` rather than one caller starving a tunnel |
| 4 | [x] | operations: per-tunnel load tiers, one `debug` line per caller, `--auth=none` for a relay-only instance serving named sockets |
| 5 | [ ] | `varlinkctl-http` as a caller through the relay (`VARLINK_RELAY_URL`) |
| 6 | [ ] | signed node ids: dial-out signature bound to the channel binding, signer trait + software key, pinned-keys dir, first-use store, key generation |
| 7 | [ ] | packaging: `varlink-relayd.service`/`.socket`, spec file |
| 8 | [ ] | caller-side alias file |

Steps 1 and 2 are the minimum for a working, tested feature with curl as
the caller; 5 adds `varlinkctl` as a caller; 6 is wanted before running
on a real network.

Dial-out plus `h2` did not fit the old 4MB size gate (~1MB added); the
gate is raised to 4.5MB. A `dialout` cargo feature remains the fallback
if a size-sensitive target ever needs the smaller binary.

## Reconnects and collisions

A reconnect is normal operation, not a collision: with signed node ids a
new connection
that proves the **same key** as the registered one replaces it. Same key
means same machine, and replacing a dead or half-dead connection gives
instant recovery. Drops are expected even on healthy networks, since
middleboxes commonly enforce hard connection lifetimes (L7 load
balancers reap WebSockets after a fixed time regardless of activity), so
the node redials with jitter on any drop or PING timeout.

The actual conflicts:

- **A different key for a registered id:** hard reject, an auth
  failure. Within one key store duplicate ids are structurally
  impossible (one `<id>.pem` file per id), but the same id can appear in
  both the pinned and the first-use store with different keys (a machine
  first-use-registered, later re-imaged and pinned). That is a hard
  error for that id: the relay refuses its connections and logs until
  the admin deletes one of the two files. Refusing beats silently
  preferring the pinned key, which would leave `/var/lib` disagreeing
  with `/etc` unnoticed.
- **Unsigned (`--insecure`):** first-wins among live connections. A claim
  is rejected while
  the existing connection still answers h2 PINGs and replaces it once it
  does not. Plain first-wins would let a node's own stale connection
  wedge it out; plain last-wins would let anyone hijack a registration.

This needs the heartbeat from day one, so it ships with the relay
rather than waiting for packaging.

## Relay load

One long-lived CONNECT per node covers sequential `POST /call/...` via
HTTP keep-alive. Each live stream (`/ws/sockets/{socket}`) needs its own
CONNECT, so relay fds scale with nodes x watched sockets, plus one.

The relay never buffers unboundedly: it reads from a caller socket only
while the inner h2 stream has window capacity, and opens h2 window only
as bytes drain to the other side, so backpressure propagates end to end
in both directions. This lives in the `tunnel` module's primitives and
is tested there.

That backpressure has to stay *per stream*, though. All callers of one
node share a single h2 connection, and h2's default is one 64KiB window
for the whole connection, i.e. shared by all of its streams: a single
caller whose local service hangs -- or who stopped reading its socket --
then holds the entire connection window and every other caller on that
tunnel starves. Both tunnel ends therefore size the connection window as
`MAX_TUNNEL_STREAMS` (256) stream windows of 32KiB each, i.e. 8MiB, and
the node advertises that same stream limit, so no stream can hold more
than its own share and a caller beyond the limit waits for a slot (and
gets a `503` if none frees up in time) instead of slowing everybody
down. The relay keeps that line itself, in front of h2: a caller takes
one of the node's slots before it opens a stream, so one that gives up
waiting never reaches the node -- left to h2's own queue, an abandoned
request would still open and reset a stream on the node once a slot
frees, one per waiter, ahead of the live callers. The window is a
promise, not an allocation: an idle tunnel costs the same with 8MiB as
with h2's 64KiB default, and only a tunnel whose callers all wedge at
once holds that much.

The 32KiB stream window is what bounds a single caller's throughput over
a long fat pipe (window/RTT, so ~650KB/s at 50ms, measured 0.60MB/s) --
ample for varlink call and reply, and the price for serving 256 callers
per node out of one connection window. Bulk data is what would suffer:
256KiB gets 4.89MB/s and 1MiB gets 17.65MB/s over the same 50ms link, so
carrying file transfers through the tunnel means revisiting the window
(see the `TODO` on `STREAM_WINDOW`).

## Logging

Networks are weird, so the log is the only way to tell "the relay is
down" from "this node is misconfigured" from "that caller is slow".
What makes that work is not more lines, it is knowing which level a
line belongs at -- `info` has to stay readable on a busy relay, or it
stops being read at all:

| level | what belongs there | volume |
| ----- | ------------------ | ------ |
| `error` | the process cannot do its job any more | never, in practice |
| `warn` | someone has to act: a tunnel is down, a node id is claimed twice, a tunnel is out of stream slots, a stream is wedged, the listener is out of file descriptors | one per event, not per attempt |
| `info` | lifecycle worth tracking: listeners bound, a node connected or disconnected (with how long it lasted and how many streams went with it), a tunnel established or recovered, callers starting to queue on a full tunnel and that queue draining (how long, how many served from it, how many gave up), a caller asking for a node nobody has | per node, per tunnel |
| `debug` | one line per caller and per retry, with the numbers: bytes each way, how long, why it ended | per stream |

Three rules keep the volume proportional to the trouble rather than to
the retrying:

- **A run of failures is one event.** A relay outage says so once, then
  goes quiet, then reminds every 10 minutes while it lasts, and says how
  long it took when it comes back. A cause that changes mid-outage is
  loud again, because it is news.
- **What a public port sees all day is `debug`.** Scanners, half-open
  connections, TLS mismatches, malformed `CONNECT`s: routine, and it
  must not bury the rest.
- **Both ends name a caller by its h2 stream id.** It is the one
  identifier the relay and the node both see, so a caller's line on the
  relay leads to its lines on the node.
