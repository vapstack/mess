package main

var commandUsage = map[string][]string{
	"new": {
		"Generate files required for a new node and store them under the current dir",
		"$ mess new <region>.<country>.<datacenter> [cert_lifetime_days]",
	},
	"gen": {
		"Generate new key and certificate files for a node and store them under the current dir",
		"$ mess gen <node_id> [<lifetime_days>]",
	},

	"sync": {
		"Fetch the mess map from known nodes or from the specified one",
		"$ mess sync [<ip_addr>]",
	},
	"map": {
		"Print mess map",
		"$ mess map [json]",
	},
	"rec": {
		"Print example service configuration as json",
		"$ mess rec",
	},

	"add": {
		"Add a node to the mess",
		"$ mess add <ip_addr>",
	},
	// "forget": {
	// 	"Remove a node from the mess",
	// 	"$ mess forget <ip_addr|node_id>",
	// },

	"rotate": {
		"Rotate node certificates",
		"$ mess rotate [<lifetime_days>] [force]",
	},
	"upgrade": {
		"Upgrade a node binary using the provided file",
		"$ mess node-upgrade <ip_addr|node_id> <filename>",
	},
	"shutdown": {
		"Gracefully shut down a node (requires systemd unit to start again)",
		"$ mess shutdown <ip_addr|node_id>",
	},

	"put": {
		"Update or create a service configuration from the provided file on a specified node",
		"$ mess put <filename> <ip_addr|node_id>",
	},
	"start": {
		"Start a service on a node or on all nodes where the service exists",
		"$ mess start <service[@realm]> <ip_addr|node_id|all>",
	},
	"stop": {
		"Stop a service on a node or on all nodes where the service exists",
		"$ mess stop <service[@realm]> <ip_addr|node_id|all>",
	},
	"restart": {
		"Restart a service on a node node or on all nodes where the service exists",
		"$ mess restart <service[@realm]> <ip_addr|node_id|all>",
	},
	"delete": {
		"Delete a service from a node",
		"$ mess delete <service[@realm]> <ip_addr|node_id>",
	},
	"store": {
		"Upload the provided binary or archive as a service with the given name",
		"on the specified node or on all nodes where the service exists",
		"$ mess store <filename> <service[@realm]> <ip_addr|node_id|all>",
	},
	"deploy": {
		"Upload the provided binary or archive as a service with the given name",
		"on the specified node or on all nodes where the service exists",
		"and restart the service",
		"$ mess deploy <filename> <service[@realm]> <ip_addr|node_id|all>",
	},

	// "find": {
	// 	"Find nodes having a specified service (supports <*suffix> and <prefix*>)",
	// 	"$ mess find <service[@realm]>",
	// },
	// "wipe": {
	// 	"Completely erase a node, including all services, configs, logs, and data",
	// 	"$ mess wipe <ip_addr|node_id>",
	// },
	// "logs": {
	// 	"Show logs from services or nodes",
	// 	"$ mess logs <service> <ip_addr|node_id> [stream]",
	// },
	// "signals" {
	// 	"Write signals from the topic to stdout",
	// 	"$ mess signals <topic> [limit]",
	// },
	// "metrics": {
	// 	"Show metrics from services or nodes",
	// 	"$ mess metrics <service> <ip_addr|node_id>",
	// },
}
