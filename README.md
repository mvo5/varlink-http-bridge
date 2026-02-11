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
