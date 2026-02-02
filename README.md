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


## Examples

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
