# mess

An early prototype of a process manager, service mesh, and event bus.

The main goal is to provide a simple and reliable foundation for running services without containers on bare-metal
servers, while having some basics out of the box.

Those basics are currently:

- Node manager with an API (certificate rotation, gossip, logging)
- Process manager with an API (control, deployment, logging)
- Service mesh with TLS and automatic routing of HTTP traffic
- Event bus, durable and ephemeral messaging

> None of the above is production ready

TODO:
- Metrics collection

Maybe:
- Pass additional streams to the binary (on Linux)
