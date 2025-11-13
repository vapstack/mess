# mess

An early prototype of a process manager, service mesh, and event bus.

The main goal is to provide a simple and reliable foundation for running services without containers on bare-metal
servers, while having some basics out of the box.

Those basics are currently:

1. Process manager with an API for management and deployment
2. Service mesh with automatic routing of HTTP traffic

TODO:

3. Event bus, durable and ephemeral messaging
4. Metrics collection
