# varlink-relayd

Plan for a relay that makes a `varlink-httpd` reachable when it runs behind
NAT or a firewall that only lets HTTPS out.

## Terminology

- **node**: the machine behind the NAT/firewall. It runs `varlink-httpd`
  and dials out to the relay to keep the tunnel open.
- **relay**: `varlink-relayd`. One listener accepts node dial-outs and
  is publicly reachable, the other accepts caller `CONNECT` and should
  sit on a trusted network (see "Deployment"). It splices the two
  together and forwards opaque bytes (the TLS encrypted varlink session).
- **caller**: whatever talks to the relay to make varlink calls on a node:
  `curl`, `varlinkctl-http` or some other backend. Note that a browser is
  never directly a caller (it cannot issue `CONNECT`) but it can talk
  to e.g. a tiny shim backend.

## Overview

The node's `varlink-httpd` dials out to the relay over a single WebSocket
and keeps it open. A caller reaches the node by sending
`CONNECT <node_id>` to the relay, which splices the caller onto that node's
connection. From then on the relay forwards opaque bytes. curl sends
`CONNECT <node_id>:443`; the port is accepted and ignored, since a node id
names one service, and anything that is not `<node_id>[:port]` gets a `400`.

```
caller ---CONNECT <node_id>--> relay ---node's WebSocket--> node (varlink-httpd)
  \____________________ one TLS session, end to end _____________________/

                          routes by node id,           the caller's TLS peer,
                          splices opaque bytes         authenticates the caller
```

The caller's TLS session is end to end with the node's bridge, so the
relay is content-blind: every bridge auth method (mTLS, ssh signatures
with channel binding, bearer tokens) works unchanged, and the relay can
neither read nor forge calls.

```console
$ curl --proxytunnel -x http://relay:8444 \
    -H 'Authorization: Bearer ...' \
    --json '{}' https://<node_id>/call/io.systemd.Hostname.Describe
```

The caller talks to the relay over plain `http://` on purpose: that hop
carries nothing but the `CONNECT` line and the caller's own TLS bytes,
so there is nothing for a second TLS layer to protect, and a plain
`CONNECT` proxy is what every HTTP client's proxy setting already
understands. This holds only while the `CONNECT` carries no secret:
caller authentication on the relay (milestone 10) puts a token in
`Proxy-Authorization`, which over plain `http://` is readable by anyone
on the path. The caller listener then takes the same TLS wrapper the
node listener has, without changing the protocol, and callers use an
`https://` proxy (`curl -x https://relay:8444`); the relay refuses to
enable caller authentication on a plaintext caller listener. The same
wrapper serves callers behind egress that only passes TLS.

### Deployment

Only the node listener needs to be on the internet, since that is what
nodes behind NAT dial. The caller listener should sit on a trusted
network: localhost, a private network or VPN shared with the control
plane, or behind a firewall that admits only known callers. Callers are
typically a few backends, not the whole internet, so this is cheap, and
it is what keeps an anonymous caller from reaching a node at all (see
"Relay load"). A caller listener open to the internet needs caller
authentication (milestone 10).

`<node_id>` is always the `app_machine_id`, i.e. the tunnel app specific
hash of the machine_id of the node (see "Node id"). Friendly names are
caller-side sugar and never reach the relay.

## Scope

Kept deliberately small: one process, no revocation beyond a token
expiring or a pin file being deleted, one tunnel per bridge instance: a
host may run several bridges, each dialing out under its own
per-instance id (see "Node id"). The relay holds no shared state beyond
its pin directory, if it has one, so N
processes can split a fleet between them if we ever run into scaling
issues (see "Scaling"); ids are uniformly distributed, so even plain
modulo works for sharding.

## Node id

The id is always the **application-specific machine ID** (the
`app_machine_id` from the Overview); there is no override. The raw
`/etc/machine-id` is confidential per `machine-id(5)` and must not go on
the wire. The id is what `systemd-id128 machine-id -a <app-id>` derives
from it with one fixed app-id UUID for the bridge (stable forever).

A host can run more than one bridge with different policies, e.g. an
additional unauthenticated one exposing a single harmless socket such
as a ping or a health check. That instance is `--auth=none` over TLS,
which the bridge allows only when the one socket to serve is given
explicitly on the commandline (see README.md). Each instance dials out
itself and registers under its own id: the default instance uses the
fixed app id, a named instance (`--instance ping`) derives its app id
from the fixed one plus the label, then applies the same
machine-specific derivation. The relay knows nothing about instances,
only ids.

Through the relay such an instance is reachable by anyone who can reach
the caller listener and knows its id: node ids are not secret (below),
so anyone who learned one can `CONNECT` to it and call every method on
its socket. TLS still keeps the relay from reading or forging
those calls, but nothing restricts who makes them, so only expose
sockets designed for anonymous callers, with their own rate limiting.

The id appears in the `CONNECT` authority and as a DNS SAN in the node's
certificate: the bridge adds it to the self-signed certificate it
generates, and `--relay` refuses a certificate without it. 32 hex chars
is a valid DNS label, so ordinary hostname verification works.

Node ids are not secret, but not discoverable either: 128 random-looking
bits per machine and app, not enumerable and not published anywhere.
They do travel in the clear in the `CONNECT` line and end up in caller
configs and logs, so whoever sees those learns them; nothing may depend
on an id staying unknown.

The SAN alone proves nothing: anyone who knows an id can self-sign a
certificate naming it. It authenticates the node only
when the certificate chains to a CA the caller trusts, and that CA
issues an id SAN only to the machine owning the id. For a self-signed
certificate the proof is a pin the caller already holds, keyed by node
id rather than by `host:port` (every node behind a relay shares the
relay's). The per-id pin is what stops one node's valid key from
answering for another id; with a pin the SAN check only catches
mix-ups. The pin is on the key (see README.md), so re-issuing an older
certificate from the same key adds the SAN and keeps every pin.

The `--relay` refusal is therefore a configuration check, not a
security property: it makes a CA-issued certificate without the id SAN
fail at startup on the node instead of at every caller's handshake.

Registering is the WebSocket upgrade, with the node id in the URL:

```http
GET /v1/tunnel?node_id=0f8cb1e4a6d24f7c9b3e5a1d7c0b4e26 HTTP/1.1
Host: relay.example
Upgrade: websocket
```

A `101` means the id is claimed; the h2 session runs inside the
WebSocket from then on, and the relay verifies the claim through it
before any caller can reach the id (see "Security model"). Before the
`101` the relay answers `404` (not the tunnel path), `400` (no
`node_id=`, or not 32 hex chars) or `409` (another *registered*
connection already holds that id, see "Reconnects and collisions"); a
claim that fails verification closes the WebSocket instead. A claim
that is still unverified is invisible to everyone else: it never
causes a `409`, callers see the id as unknown (`502`), and any number of
claims for one id may be pending at once.

The node_ids are not names a human wants to type. Friendly names can
come later similar to how DNS names work: the caller resolves the name
(e.g. an alias file) and connects to the id, so TLS verification stays
anchored on the id. The relay never sees a name and must never route
on one.

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
2. the caller validates the node's TLS identity (CA chain or
   pre-existing pin, never TOFU).

The first is enforced: `varlink-httpd --relay` refuses to start when
it would serve plaintext, whatever mode the relay runs in.

All caller authentication happens through the regular `varlink-httpd`
mechanisms, end-to-end inside the TLS session the relay cannot see. The
relay never authenticates callers. The relay only authenticates which
machine owns a tunnel, in one of two ways, with nothing in between:

**`--insecure`:** the node just asserts its id. This relaxes only the
relay's check of the claim; the bridge still refuses to serve plaintext
over `--relay`. Requires the explicit flag and warns at startup.
For testing and trusted networks; anyone who knows a machine_id can
squat an id. Anyone who can sniff the traffic of a relay can
learn the node_id and also squat.

**Verified node ids (default):** requires TLS on the node's bridge,
since the proof is a TLS handshake; a bridge serving plaintext never
registers. The node already holds a key that callers verify: the TLS key
of its `varlink-httpd`, whose certificate names the node id as a SAN
(see "Node id"). The relay checks that key the way a caller does. Once
the tunnel is up it opens one h2 stream through it, performs an ordinary
TLS handshake to the node as a client, checks that the id is a SAN of
the certificate and that the public key is the one bound to the id
(below), closes the stream, and only then registers the id. The probe
passes once the node's `Finished` verifies: in TLS 1.3 the node proves
its key before it learns that the relay has no client certificate, so a
bridge that requires mTLS still registers and merely rejects the probe
afterwards.

```
node  -> GET /v1/tunnel?node_id=0f8cb1e4a6d24f7c9b3e5a1d7c0b4e26 HTTP/1.1
relay -> 101 Switching Protocols, h2 preface ...
relay -> opens one stream: TLS ClientHello, SNI relay.example
node  -> checks SNI is the relay it dialed,
         certificate (SAN 0f8cb1e4...), CertificateVerify
relay -> checks SAN and public key, closes the stream, registers the id
```

The handshake is a challenge-response by construction: the node's
`CertificateVerify` signs the transcript, which includes the relay's
fresh random, so it proves possession of the private key for this
handshake alone. Nothing can be replayed, on this connection, on another
shard, after a restart or from a log; there is no clock, no nonce, no
cache, and nothing signed in a request line. Balancers are irrelevant,
since the probe is TLS inside the WebSocket and nothing on the path can
read or alter it. It also enforces what the security argument otherwise
assumes: a bridge without TLS, or with a certificate that does not name
the id, never registers.

There is no signing protocol and no second key; the only thing new on
the node is the SNI check below. An `--insecure` relay skips the key
check and registers on the `101`.

Until the probe succeeds a claim holds nothing. It does not block other
claims for the same id, so a node's own stale connection or a squatter
who dials and never answers the `ClientHello` cannot keep the real node
out; concurrent claims race and the first verified one registers. The
probe has a deadline of a few seconds, after which the claim is closed;
until then a claim is one more connection on the node listener, and
flooding that port is the same operations problem as flooding the caller
port (see "Relay load").

The handshake alone does not say which relay is asking, so every relay
the node dials could relay another relay's probe. A malicious relay A
claims the node's id at relay B and forwards B's probe through the
node's tunnel to A; the node answers, B registers A's connection, and
B's callers are routed to A. In `jwt` mode A even holds the token B
wants, since the node sent it to A. The probe therefore carries the
relay's own hostname as SNI, and the node refuses a probe whose SNI is
not the host of the `--relay` URL it dialed. Callers use the node id as
SNI, so the bridge accepts exactly two names over `--relay`: its id and
the relay it dialed. This is mandatory: without it any relay a node
dials can hijack that node's id on any other relay that trusts its key.
Sharded relays behind one hostname share that name, which is fine
since they are one operator.

What the probe checks the node's key against comes from one of two
places, chosen on the relay; the probe itself is the same either way.

**A pin directory** (`--node-auth=pin`,
`/etc/varlink-relayd/nodes.d/<node_id>.pem`): the node's public key must
match the listed one, and an id with no entry is refused. Guarantee:
this machine is on my list. The bridge prints its public key pin at
startup; an operator puts it here before the node first dials. An entry
is PEM and may hold a public key or a certificate: the probe accepts an
exact public key match or a chain ending at a stored certificate, which
is what lets an issuer be pinned instead of a leaf (see "Node key").
Re-keying or revoking is editing or deleting the file. Zero
infrastructure, for a handful of machines.

**A workload identity token** (`--node-auth=jwt`): a WIMSE Workload
Identity Token (WIT), a JWT the fleet's identity provider issued when
the machine enrolled, sent as a bearer token on the upgrade request.
The relay verifies it against the issuer's JWKS and expiry, and
requires `aud` to name this relay, so a token handed to one relay is
refused by every other one (with the SNI check above this is defence in
depth). It takes the node id from a claim (it must be the one in the query) and the
key from the `cnf` claim, and the probe then checks the node's
certificate against that key exactly as it would against a pin: the
leaf's key is `cnf`, or the chain ends at a certificate for `cnf`, which
is the shape for a TPM-resident key that issues the TLS certificate
(see "Node key"). The relay keeps no per-node state: the enrollment the
machine already went through is the registry. The tunnel is dropped
when the token expires and the node redials with a fresh one. This is
the mode for a fleet with an identity provider, and the same verifier
can later gate callers on the `CONNECT` face.

The token is the credential that says which machine this is; the probe
is what proves the machine holds the key the token names. Neither is
enough alone: a stolen token cannot pass the probe, and a pinned or
`cnf` key alone does not say who is allowed to claim the id.

There is no trust-on-first-use. It would let whoever dials first with a
certificate naming an id own that id, and it needs a second store that
can disagree with the pins; copying one printed pin per machine is the
same motion as an SSH host key, and a fleet too large for that has an
identity provider and uses tokens. It is a few lines on top of the probe
if a deployment ever wants it.

## Node key and token

The identity is the TLS key the bridge already generates and callers
already pin; there is no separate device key. Should a TPM-resident key
be wanted later, the clean shape is for it to issue the TLS certificate
rather than be the TLS key, so it signs once at issuance and never in a
handshake, and the relay pins the issuer instead of the leaf. That is
also how a token fits: the `cnf` key an identity provider bound to the
machine at enrollment is typically that device key, and the probe
accepts a certificate chain ending at it.

In `jwt` mode the bridge only carries the token: `--relay-token=PATH`,
a systemd credential, or a default path under `/var/lib`. Issuing and
renewing it is the enrollment agent's job; the bridge re-reads it on
every dial, so a rotated token is picked up without a restart.

## Scaling

This design should scale fine to many thousands of nodes per relayd: a
connected node costs a TLS session, an h2 connection and a heartbeat,
and the connection window is a promise, not an allocation (see "Relay
load"), so an idle tunnel is cheap and the limit is memory and file
descriptors rather than CPU. But if scaling becomes a problem we can
shard the relayd processes. One constraint is that the caller needs to
work with `curl --proxytunnel`.

Sharding needs nothing from the relay itself. It holds no state two
processes would have to agree on: a node belongs to whichever process it
dialed and an unknown id is a `502`, so N independent processes behind
one L7 load balancer are N shards with nothing to coordinate. The pin
directory is configuration and is replicated to every shard like the
rest of it. The balancer hashes on the node id, consistently so a
reshard moves as few nodes as possible, and never round-robin: a node
has to land on the same shard every time it redials or its callers find
a stale registration.

Both connections already carry the id where such a balancer can read
it: the node's upgrade in `?node_id=`, the caller's `CONNECT` as the
authority, and the same hash on both sends a node and its callers to the
same shard. That is what keeps `curl --proxytunnel` working
unchanged: it connects to the one balancer address and issues `CONNECT`
as it always did, and the routing happens behind it. It is also why the
relay verifies a node inside the tunnel rather than on the connection
that carries it (see "Security model"): the balancer has to decrypt the
node's connection to read the id, so anything bound to that TLS session
would not survive it.

None of this is built or exercised; it is what the protocol leaves room
for. A caller that knows the shard layout, a control plane rather than
`curl`, may compute the shard from the id and skip the balancer, but
nothing requires it to.

## Shape

The node side will be a new listener in `varlink-httpd`, not a separate
process. `AsyncTlsListener<L>` is already generic over the inner
listener, so `--relay` can compose as `AsyncTlsListener<DialOutListener>`
and all existing auth paths apply untouched; the listener, its transport
variant and its connect-info are milestone 2. With `--bind=none` no
local TCP listener is exposed.

`varlink-relayd` is a separate binary with its own size gate.

## Milestones

| # | Status | Step |
|---|---|---|
| 1 | [ ] | `varlink-relayd`: `--bind`, `--connect-bind`, CONNECT demux, node registry, h2 PING heartbeat, TLS, `--insecure` guard rails, tested against a stub node |
| 2 | [ ] | `varlink-httpd --relay <url>`: `DialOutListener` with its transport variant and connect-info, dial-out with redial and backoff, `--instance <label>`, `--bind=none`, the id as a SAN of the generated certificate, the size gate raised to 4608 KiB, end-to-end test against `varlink-relayd` |
| 3 | [ ] | fairness: a connection window sized for `MAX_TUNNEL_STREAMS`, a slot timeout and a `503` rather than one caller starving a tunnel |
| 4 | [ ] | operations: per-tunnel load tiers, one `debug` line per caller, `--auth=none` for a relay-only instance serving named sockets |
| 5 | [ ] | `varlinkctl-http` as a caller through the relay (`VARLINK_RELAY_URL`) |
| 6 | [ ] | verified node ids: the registration probe with the relay name as SNI and the node's check of it, the pin directory, the `4401` close and the node's backoff on it |
| 7 | [ ] | packaging: `varlink-relayd.service`/`.socket`, spec file |
| 8 | [ ] | caller-side alias file |
| 9 | [ ] | workload identity tokens: `--node-auth=jwt` verifying a WIT against a JWKS as the source of the key the probe checks, `--relay-token` on the bridge |
| 10 | [ ] | caller authentication: TLS on the caller listener, a WIT in `Proxy-Authorization` on the `CONNECT`, same verifier as 9 |

Steps 1 and 2 are the minimum for a working, tested feature with curl as
the caller; 5 adds `varlinkctl` as a caller; 6 is wanted before running
on a real network. Until 6 lands the relay only implements `--insecure`
and refuses to start without the flag, so an early build cannot be
deployed looking secure: claims register on the `101`, and the `409`
and pending-claim rules do not yet apply. Step 6 makes verified node ids
the default.

## Reconnects and collisions

A reconnect is normal operation, not a collision: with verified node ids
a new connection that proves the **same key** as the registered one
replaces it, once its probe has succeeded, so a claim that cannot prove
the key never evicts a live one, and one that has not yet proved it
neither evicts nor blocks anything (see "Security model"). Same key
means same machine, and replacing a dead or half-dead connection gives
instant recovery. Drops are expected even on healthy networks, since
middleboxes commonly enforce hard connection lifetimes (L7 load
balancers reap WebSockets after a fixed time regardless of activity), so
the node redials with jitter on any drop or PING timeout. A refused
claim is different from a drop: the relay closes with the private code
`4401` for a key that does not match its pin or an id that has none, and
the node treats that as an operator problem, warns once and redials at
the slowest backoff rather than as if the network had blinked.

The actual conflicts:

- **A key that does not match the pin, or an id with no pin:** refused
  with `4401`, an auth failure, and nothing about the id changes. There
  is one store, so two entries cannot disagree; a re-keyed machine is an
  edit to its file. In `jwt` mode the same applies with the token in
  place of the pin: a token that does not verify, or names another id,
  or a key the probe cannot confirm, is `4401`, and a re-keyed machine
  is a fresh token.
- **Unsigned (`--insecure`):** there is no probe, so a claim registers
  on the `101` and the rule is first-wins among live connections. A
  claim is rejected while the existing connection still answers h2 PINGs
  and replaces it once it does not. Plain first-wins would let a node's
  own stale connection wedge it out; plain last-wins would let anyone
  hijack a registration. Squatting an id by dialing first stays
  possible here; that is the weakness `--insecure` accepts.

This needs the heartbeat from day one, so it ships with the relay
rather than waiting for packaging.

## Relay load

One long-lived CONNECT per node covers sequential `POST /call/...` via
HTTP keep-alive. Each live stream (`/ws/sockets/{socket}`) needs its own
CONNECT, so relay fds scale with nodes x watched sockets, plus one.

The relay never buffers unboundedly: it reads from a caller socket only
while the inner h2 stream has window capacity, and opens h2 window only
as bytes drain to the other side, so backpressure propagates end to end
in both directions. This belongs in the `tunnel` module's primitives,
tested there.

That backpressure has to stay *per stream*, though. All callers of one
node share a single h2 connection, and h2's default is one 64KiB window
for the whole connection, i.e. shared by all of its streams: a single
caller whose local service hangs, or who stopped reading its socket,
then holds the entire connection window and every other caller on that
tunnel starves. Both tunnel ends will therefore size the connection
window as `MAX_TUNNEL_STREAMS` (256) stream windows of 32KiB each, i.e.
8MiB, and the node advertises that same stream limit, so no stream can
hold more than its own share and a caller beyond the limit waits for a
slot (and gets a `503` if none frees up in time) instead of slowing
everybody down. The relay keeps that line itself, in front of h2: a
caller takes one of the node's slots before it opens a stream, so one
that gives up waiting never reaches the node; left to h2's own queue, an
abandoned request would still open and reset a stream on the node once a
slot frees, one per waiter, ahead of the live callers. The window is a
promise, not an allocation: an idle tunnel costs the same with 8MiB as
with h2's 64KiB default, and only a tunnel whose callers all wedge at
once holds that much.

A rogue caller that stalls inside the TLS handshake holds a node slot
until the node's handshake timeout drops the stream, so
`MAX_TUNNEL_STREAMS` such connections block every caller of that node.
It needs to reach the caller listener and know the node's id; ids are
not secret (see "Node id"), so the real barrier is the first. Waiting
for the caller's first byte, or even
the whole `ClientHello`, before taking a slot does not help: the
attacker sends it and stalls on the next flight, and the relay cannot
tell a stalled handshake from a slow one since everything after the
`ServerHello` is encrypted. A shorter handshake timeout on relay streams
only makes the attacker reconnect more often.

So this is a deployment question, not something the relay can solve in
code. With the caller listener on a trusted network (see "Deployment")
only trusted callers can take slots, and the attack is gone. A caller
listener open to the internet needs caller authentication (milestone
10): a slot is taken only after the `CONNECT` carried a valid token, so
an anonymous caller costs the node nothing. Without either, the best
the relay can do is bound it: a cap on concurrent `CONNECT`s per source
and node id (per /64 for IPv6, or `nft ... ct count over N` in front),
so one source cannot take all of a node's slots, though enough sources
still can. The end-to-end TLS protects what callers send, not whether
they get through.

Whatever the cause, the relay limits the damage rather than trying to
stop a flood: the slot timeout and `503` fail that node's callers fast
and leave every other node's untouched, and the load tiers make the
saturated node visible in the log.

The 32KiB stream window is what bounds a single caller's throughput over
a long fat pipe (window/RTT, so ~650KB/s at 50ms; the prototype measured
0.60MB/s), ample for varlink call and reply, and the price for serving
256 callers per node out of one connection window. Bulk data is what
would suffer: 256KiB gets 4.89MB/s and 1MiB gets 17.65MB/s over the same
50ms link, so carrying file transfers through the tunnel means
revisiting the window.

## Logging

Networks are weird, so the log is the only way to tell "the relay is
down" from "this node is misconfigured" from "that caller is slow". What
makes that work is not more lines, it is knowing which level a line
belongs at: `info` has to stay readable on a busy relay, or it stops
being read at all:

| level | what belongs there | volume |
| ----- | ------------------ | ------ |
| `error` | the process cannot do its job any more | never, in practice |
| `warn` | someone has to act: a tunnel is down, a pinned id is claimed with the wrong key, a tunnel is out of stream slots, a stream is wedged, the listener is out of file descriptors | one per event, not per attempt |
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
