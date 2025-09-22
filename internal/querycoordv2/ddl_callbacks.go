package querycoordv2

import "github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"

// RegisterDDLCallbacks registers the ddl callbacks.
func RegisterDDLCallbacks(s *Server) {
	ddlCallback := &DDLCallbacks{
		Server: s,
	}
	ddlCallback.registerLoadConfigCallbacks()
	ddlCallback.registerResourceGroupCallbacks()
}

type DDLCallbacks struct {
	*Server
}

// registerLoadConfigCallbacks registers the load config callbacks.
func (c *DDLCallbacks) registerLoadConfigCallbacks() {
	registry.RegisterAlterLoadConfigV2AckCallback(c.alterLoadConfigV2AckCallback)
	registry.RegisterDropLoadConfigV2AckCallback(c.dropLoadConfigV2AckCallback)
}

func (c *DDLCallbacks) registerResourceGroupCallbacks() {
	registry.RegisterAlterResourceGroupV2AckCallback(c.putResourceGroupV2AckCallback)
	registry.RegisterDropResourceGroupV2AckCallback(c.dropResourceGroupV2AckCallback)
}
