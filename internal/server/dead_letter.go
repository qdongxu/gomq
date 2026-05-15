// dead_letter.go implements dead-letter exchange routing.
package server

import (
	"fmt"
)

// DeadLetterConfig holds DLX settings extracted from queue arguments.
type DeadLetterConfig struct {
	Exchange   string
	RoutingKey string
}

// GetDeadLetterConfig extracts DLX config from queue arguments.
// Returns nil when no DLX is configured.
func GetDeadLetterConfig(
	args map[string]interface{},
) *DeadLetterConfig {
	if args == nil {
		return nil
	}
	ex, ok := args["x-dead-letter-exchange"]
	if !ok {
		return nil
	}
	exName, ok := ex.(string)
	if !ok || exName == "" {
		return nil
	}
	cfg := &DeadLetterConfig{Exchange: exName}
	if rk, ok := args["x-dead-letter-routing-key"]; ok {
		if rks, ok := rk.(string); ok {
			cfg.RoutingKey = rks
		}
	}
	return cfg
}

// MaxLength extracts the x-max-length value from queue arguments.
// Returns 0 when no limit is set.
func MaxLength(args map[string]interface{}) int {
	if args == nil {
		return 0
	}
	v, ok := args["x-max-length"]
	if !ok {
		return 0
	}
	switch n := v.(type) {
	case int:
		return n
	case int8:
		return int(n)
	case int16:
		return int(n)
	case int32:
		return int(n)
	case int64:
		return int(n)
	case uint:
		return int(n)
	case uint8:
		return int(n)
	case uint16:
		return int(n)
	case uint32:
		return int(n)
	case uint64:
		return int(n)
	case float32:
		return int(n)
	case float64:
		return int(n)
	}
	return 0
}

// ShouldDeadLetter reports whether a rejected/expired message should be
// routed to a DLX.  A DLX must be configured for this to return true.
func ShouldDeadLetter(
	args map[string]interface{},
	reason string, // "rejected" | "expired" | "maxlen"
) bool {
	cfg := GetDeadLetterConfig(args)
	return cfg != nil && reason != ""
}

// RouteDeadLetter publishes a dead-lettered message to the configured DLX.
// It mutates the message with the DLX routing key and publishes via the
// provided publisher.
func RouteDeadLetter(
	msg *Message,
	args map[string]interface{},
	publishFunc func(exchange, routingKey string, msg *Message) error,
) error {
	cfg := GetDeadLetterConfig(args)
	if cfg == nil {
		return fmt.Errorf("no dead-letter exchange configured")
	}

	// Preserve original routing key as header.
	if msg.properties.Headers == nil {
		msg.properties.Headers = make(map[string]interface{})
	}
	msg.properties.Headers["x-original-routing-key"] = msg.RoutingKey()

	// Use DLX routing key if configured, otherwise keep original.
	rk := cfg.RoutingKey
	if rk == "" {
		rk = msg.RoutingKey()
	}

	return publishFunc(cfg.Exchange, rk, msg)
}
