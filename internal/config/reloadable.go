// reloadable.go determines which configuration sections can be changed
// at runtime without restarting the server.
package config

// IsReloadable compares old and new configuration and returns true if the
// differences are limited to reloadable sections. It also returns a slice of
// ignored keys that require a restart to take effect.
func IsReloadable(oldCfg, newCfg *Config) (bool, []string) {
	var ignored []string

	if !slicesEqual(oldCfg.Network.Listeners, newCfg.Network.Listeners) {
		ignored = append(ignored, "network.listeners")
	}
	if oldCfg.Network.Heartbeat != newCfg.Network.Heartbeat {
		ignored = append(ignored, "network.heartbeat")
	}
	if oldCfg.Network.FrameMax != newCfg.Network.FrameMax {
		ignored = append(ignored, "network.frame_max")
	}
	if oldCfg.Network.ChannelMax != newCfg.Network.ChannelMax {
		ignored = append(ignored, "network.channel_max")
	}
	if oldCfg.Cluster.NodeID != newCfg.Cluster.NodeID {
		ignored = append(ignored, "cluster.node_id")
	}
	if oldCfg.Cluster.Discovery != newCfg.Cluster.Discovery {
		ignored = append(ignored, "cluster.discovery")
	}
	if !slicesEqual(oldCfg.Cluster.Nodes, newCfg.Cluster.Nodes) {
		ignored = append(ignored, "cluster.nodes")
	}
	if !slicesEqual(oldCfg.Cluster.EtcdEndpoints, newCfg.Cluster.EtcdEndpoints) {
		ignored = append(ignored, "cluster.etcd_endpoints")
	}
	if oldCfg.Web.Enabled != newCfg.Web.Enabled {
		ignored = append(ignored, "web.enabled")
	}
	if oldCfg.Web.Listen != newCfg.Web.Listen {
		ignored = append(ignored, "web.listen")
	}
	if oldCfg.Web.PathPrefix != newCfg.Web.PathPrefix {
		ignored = append(ignored, "web.path_prefix")
	}
	if oldCfg.Metrics.Enabled != newCfg.Metrics.Enabled {
		ignored = append(ignored, "metrics.enabled")
	}
	if oldCfg.Metrics.Listen != newCfg.Metrics.Listen {
		ignored = append(ignored, "metrics.listen")
	}

	return len(ignored) == 0, ignored
}

func slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
