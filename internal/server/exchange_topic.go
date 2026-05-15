// exchange_topic.go implements topic (pattern-based) routing.
package server

import (
	"strings"
)

// TopicExchange routes messages by routing-key pattern match.
type TopicExchange struct{}

// Route returns queue names whose binding pattern matches msgRoutingKey.
func (e *TopicExchange) Route(
	msgRoutingKey string,
	_ map[string]interface{},
	bindings []*Binding,
) []string {
	var queues []string
	seen := make(map[string]struct{})
	msgParts := strings.Split(msgRoutingKey, ".")
	for _, b := range bindings {
		if matchTopicPattern(msgParts, b.RoutingKey) {
			if _, ok := seen[b.QueueName]; !ok {
				queues = append(queues, b.QueueName)
				seen[b.QueueName] = struct{}{}
			}
		}
	}
	return queues
}

// matchTopicPattern reports whether msgParts matches the pattern.
func matchTopicPattern(msgParts []string, pattern string) bool {
	if pattern == "#" {
		return true
	}
	patParts := strings.Split(pattern, ".")
	return matchParts(msgParts, patParts)
}

// matchParts recursively matches message parts against pattern parts.
func matchParts(msg, pat []string) bool {
	for len(pat) > 0 {
		switch pat[0] {
		case "#":
			pat = pat[1:]
			if len(pat) == 0 {
				return true // # at end swallows everything
			}
			// Try every possible split point for #
			for i := 0; i <= len(msg); i++ {
				if matchParts(msg[i:], pat) {
					return true
				}
			}
			return false
		case "*":
			if len(msg) == 0 {
				return false
			}
			msg, pat = msg[1:], pat[1:]
		default:
			if len(msg) == 0 || msg[0] != pat[0] {
				return false
			}
			msg, pat = msg[1:], pat[1:]
		}
	}
	return len(msg) == 0
}
