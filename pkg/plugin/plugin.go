// plugin.go defines the plugin interface for gomq extensions.
// Plugins live outside the core server and are registered at startup.
package plugin

// ServerLike is the minimal interface a plugin needs from the broker.
// The concrete type passed to Init is always *server.Server.
type ServerLike interface {
	// intentionally minimal — plugins type-assert to *server.Server
	// when they need full capabilities.
}

// Plugin is the interface implemented by all gomq plugins.
type Plugin interface {
	// Name returns the unique plugin identifier.
	Name() string

	// Init is called once after the server is fully constructed but
	// before it starts accepting connections. The srv argument is
	// the concrete *server.Server value wrapped as ServerLike.
	Init(srv ServerLike) error
}
