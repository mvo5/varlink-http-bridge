# varlink-http-bridge

This is a http bridge to make local varlink services available via
http. The main use case is systemd, so only the subset of varlink that
systemd needs is supported right now.

It takes a directory with varlink sockets (or symlinks to varlink
sockets) like /run/systemd/registry as the argument and will server
whatever it find in there. Sockets can be added or removed dynamically
in the dir as needed.

## URL Schema

```
POST /call/{method}                    → invoke method (c.f. varlink call, supports ?socket=)
GET  /sockets                          → list available sockets (c.f. valinkctl list-registry)
GET  /sockets/{socket}                 → socket info (c.f. varlinkctl info)
GET  /sockets/{socket}/{interface}     → interface details, including method names (c.f. varlinkctl list-methods)

GET  /health                           → health check
```

For `/call`, the socket is derived from the method name by stripping
the last `.Component` (e.g. `io.systemd.Hostname.Describe` connects
to socket `io.systemd.Hostname`). The `?socket=` query parameter
overrides this for cross-interface calls, e.g. to call
`io.systemd.service.SetLogLevel` on the `io.systemd.Hostname` socket.

For `/call` the parameters are POSTed as regular JSON.

### Websocket support

```
GET  /ws/sockets/{socket}              → transparent varlink-over-websocket proxy
```

The websocket endpoint is a transparent proxy that forwards raw bytes
between the websocket and the varlink unix socket in both directions.
Clients are expected to speak raw varlink wire protocol.

This makes the bridge compatible with libvarlink `varlink --brige`
via `websocat --binary`, enabling full varlink features (including
`--more`) over the network.

## Examples (curl)

Using curl for direct calls is usually more convenient/ergonimic than
using the websocket endpoint.

```console
$ systemd-run --user ./target/debug/varlink-http-bridge

$ curl -s http://localhost:8080/sockets | jq
{
  "sockets": [
    "io.systemd.Login",
    "io.systemd.Hostname",
    "io.systemd.sysext",
    "io.systemd.BootControl",
    "io.systemd.Import",
    "io.systemd.Repart",
    "io.systemd.MuteConsole",
    "io.systemd.FactoryReset",
    "io.systemd.Credentials",
    "io.systemd.AskPassword",
    "io.systemd.Manager",
    "io.systemd.ManagedOOM"
  ]
}

$ curl -s http://localhost:8080/sockets/io.systemd.Hostname | jq
{
  "interfaces": [
    "io.systemd",
    "io.systemd.Hostname",
    "io.systemd.service",
    "org.varlink.service"
  ],
  "product": "systemd (systemd-hostnamed)",
  "url": "https://systemd.io/",
  "vendor": "The systemd Project",
  "version": "259 (259-1)"
}

$ curl -s http://localhost:8080/sockets/io.systemd.Hostname/io.systemd.Hostname | jq
{
  "method_names": [
    "Describe"
  ]
}

$ curl -s -X POST http://localhost:8080/call/io.systemd.Hostname.Describe -d '{}' -H "Content-Type: application/json" | jq .StaticHostname
"top"

$ curl -s -X POST http://localhost:8080/call/org.varlink.service.GetInfo?socket=io.systemd.Hostname -d '{}' -H "Content-Type: application/json" | jq
{
  "interfaces": [
    "io.systemd",
    "io.systemd.Hostname",
    "io.systemd.service",
    "org.varlink.service"
  ],
  "product": "systemd (systemd-hostnamed)",
  "url": "https://systemd.io/",
  "vendor": "The systemd Project",
  "version": "259 (259-1)"
}

```

### Examples (websocket)

The examples use websocat because curl for websockets support is relatively new and
still a bit cumbersome to use.

```console
$ cargo install websocat
...

# call via websocat: note that this is the raw procotol so the result is wrapped in "paramters"
# note that the reply also contains the raw \0 so we filter them
$ printf '{"method":"io.systemd.Hostname.Describe","parameters":{}}\0' | websocat ws://localhost:8080/ws/sockets/io.systemd.Hostname | tr -d '\0' | jq
{
  "parameters": {
    "Hostname": "top",
...

# io.systemd.Unit.List streams the output
$ printf '{"method":"io.systemd.Unit.List","parameters":{}, "more": true}\0' | websocat  --no-close  ws://localhost:8080/ws/sockets/io.systemd.Manager| tr -d '\0' | jq
{
  "parameters": {
    "context": {
      "Type": "device",
...

# and user records come via "continues": true
$ printf '{"method":"io.systemd.UserDatabase.GetUserRecord", "parameters": {"service":"io.systemd.Multiplexer"}, "more": true}\0' | websocat --no-close ws://localhost:8080/ws/sockets/io.systemd.Multiplexer | tr '\0' '\n'|jq
{
  "parameters": {
    "record": {
      "userName": "root",
      "uid": 0,
      "gid": 0,
...

# varlinkctl is supported via our varlinkctl-helper
$ VARLINK_BRIDGE_URL=http://localhost:8080/ws/sockets/io.systemd.Multiplexer \
    varlinkctl call --more /usr/libexec/varlinkctl-helper \
	io.systemd.UserDatabase.GetUserRecord '{"service":"io.systemd.Multiplexer"}'


# libvarlink bridge mode gives full varlink CLI support over the network
$ varlink --bridge "websocat --binary ws://localhost:8080/ws/sockets/io.systemd.Hostname" info
Vendor: The systemd Project
Product: systemd (systemd-hostnamed)
...

$ varlink --bridge "websocat --binary ws://localhost:8080/ws/sockets/io.systemd.Hostname" \
    call io.systemd.Hostname.Describe
{
  "Hostname": "top",
  "StaticHostname": "top",
  ...
}

```

## TLS / mTLS

TLS flag names follow the Kubernetes API server convention.

```
--tls-cert-file        path to TLS certificate PEM file
--tls-private-key-file path to TLS private key PEM file
--client-ca-file       path to CA certificate PEM for client verification (mTLS)
```

Providing `--client-ca-file` implicitly enables mTLS: the server will
require clients to present a certificate signed by that CA.

### systemd credentials

When running as a systemd service, the bridge automatically discovers
TLS material from `$CREDENTIALS_DIRECTORY` (see `systemd.exec(5)`).
The credential file names match the CLI flag names:

```ini
[Service]
LoadCredential=tls-cert-file:/etc/ssl/certs/bridge.pem
LoadCredential=tls-private-key-file:/etc/ssl/private/bridge.pem
LoadCredential=client-ca-file:/etc/ssl/ca/client-ca.pem
```

Explicit CLI flags take priority over credentials directory files.

### Client (varlinkctl-helper)

The `varlinkctl-helper` binary acts as a bridge between `varlinkctl`
and `varlink-http-bridge`, supporting TLS and mTLS. It looks for
client credentials in the first existing directory:

* `$XDG_CONFIG_HOME/varlink-http-bridge/`
* `~/.config/varlink-http-bridge/`
* `$CREDENTIALS_DIRECTORY`

The credential file names are:

| File                   | Purpose                                   |
|------------------------|-------------------------------------------|
| `client-cert-file`     | Client certificate PEM (for mTLS)         |
| `client-key-file`      | Client private key PEM (for mTLS)         |
| `server-ca-file`       | CA certificate PEM (for private/self-signed server CAs) |

The system CAs are used automatically. For mTLS, drop the client cert
and key into the config directory:

```console
$ mkdir -p ~/.config/varlink-http-bridge
$ cp client-cert.pem ~/.config/varlink-http-bridge/client-cert-file
$ cp client-key.pem  ~/.config/varlink-http-bridge/client-key-file
$ cp ca.pem          ~/.config/varlink-http-bridge/server-ca-file

$ VARLINK_BRIDGE_URL=https://myhost:8080/ws/sockets/io.systemd.Hostname \
    varlinkctl call exec:/usr/libexec/varlinkctl-helper \
    io.systemd.Hostname.Describe '{}'
```

## SSH key authentication

The bridge can authenticate requests using SSH public keys. If you
have an SSH agent running clients authenticate automatically with zero
extra configuration. Note that RSA keys are *not* supported, just
Ed25519 and ECDSA keys.

### Server setup

Pass `--authorized-keys` with a path to an openssh `authorized_keys`
file, e.g.:

```console
$ varlink-http-bridge --authorized-keys ~/.ssh/authorized_keys
```

When running as a systemd service, the bridge also checks
`$CREDENTIALS_DIRECTORY/authorized-keys` automatically. So
a systemd config like below automatically works.

```ini
[Service]
LoadCredential=authorized-keys:/etc/ssh/authorized_keys
```

### Client setup (key selection)

The varlinkctl-helper binary will automatically connect to
`SSH_AUTH_SOCK` and sign the request with a short lived bearer token
using the first keys. No setup required.

When the SSH agent has multiple keys, the client uses the first
Ed25519 or ECDSA key it finds. To select a specific key, set
`VARLINK_SSH_KEY` to the path of the corresponding public key file:

```console
$ export VARLINK_SSH_KEY=~/.ssh/id_ed25519.pub
```

The client reads the public key, matches it by fingerprint against the
agent's loaded keys, and signs with that key. This also works with
systemd credentials:

```ini
[Service]
Environment=VARLINK_SSH_KEY=/etc/ssh/bridge_key.pub
```


### Combining with TLS

SSH key auth and TLS/mTLS are independent and can be combined. For
example, use TLS for transport encryption and SSH keys for user
authentication:

```console
$ varlink-http-bridge \
    --tls-cert-file server.pem \
    --tls-private-key-file server-key.pem \
    --authorized-keys ~/.ssh/authorized_keys
```
