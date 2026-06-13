# connect/

The Frontier worker daemon bundle, published so a machine can join a host with
just a pairing code (the native, no-Docker path — see `connect.sh` at the site root).

`daemon.bundle.js` is the built worker daemon. `connect.sh` downloads it and runs
`node daemon.bundle.js connect <code>`, which resolves the host via the Link service,
pairs, and supervises the connection. The daemon negotiates a protocol version with
the host (one version of skew tolerated), so this bundle only needs to be within one
release of the host it joins; it is refreshed on each channel cut.
