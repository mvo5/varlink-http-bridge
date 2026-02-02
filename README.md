# varlink-proxy-rs 

This is a http proxy to make local varlink services available via
http. The main use case is systemd, so only the subset of varlink that
systemd needs is supported right now.

The URL schema looks like this:
```
/info
/info/{address}
/info/{address}/{interface}
/call/{address}/{method}
```
and the paramters are POSTed as regular json.

It takes a directory with varlink sockets (or symlinks to varlink sockets)
as the argument and will server whatever it find in there. Sockets can be
added or removed dynamically in the dir as needed.

Example:
```console
$ systemd-run --user ./target/debug/varlink-proxy-rs /run/systemd/

$ curl -s http://localhost:8080/info | jq
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
    "io.systemd.ManagedOOM",
  ]
}

$ curl -s http://localhost:8080/info/io.systemd.Hostname | jq
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

$ curl -s http://localhost:8080/info/io.systemd.Hostname/io.systemd.Hostname | jq
{
  "method_names": [
    "Describe"
  ]
}

$ curl -s -X POST http://localhost:8080/call/io.systemd.Hostname/io.systemd.Hostname.Describe -d {} | jq .StaticHostname
"top"
```


