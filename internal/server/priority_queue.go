// priority_queue.go implements priority-aware message ordering.
package server

// MaxPriority extracts the x-max-priority value from queue arguments.
// Returns 0 when the queue is not a priority queue.
func MaxPriority(args map[string]interface{}) uint8 {
	if args == nil {
		return 0
	}
	v, ok := args["x-max-priority"]
	if !ok {
		return 0
	}
	switch n := v.(type) {
	case int:
		if n > 0 && n <= 255 {
			return uint8(n)
		}
	case int8:
		if n > 0 {
			return uint8(n)
		}
	case int16:
		if n > 0 && n <= 255 {
			return uint8(n)
		}
	case int32:
		if n > 0 && n <= 255 {
			return uint8(n)
		}
	case int64:
		if n > 0 && n <= 255 {
			return uint8(n)
		}
	case uint:
		if n > 0 && n <= 255 {
			return uint8(n)
		}
	case uint8:
		return n
	case uint16:
		if n > 0 && n <= 255 {
			return uint8(n)
		}
	case uint32:
		if n > 0 && n <= 255 {
			return uint8(n)
		}
	case uint64:
		if n > 0 && n <= 255 {
			return uint8(n)
		}
	case float32:
		if n > 0 && n <= 255 {
			return uint8(n)
		}
	case float64:
		if n > 0 && n <= 255 {
			return uint8(n)
		}
	}
	return 0
}

// insertByPriority inserts msg into queue preserving descending priority.
// Higher Priority values are delivered first.  O(n) for simplicity.
func insertByPriority(queue []*Message, msg *Message) []*Message {
	pri := msg.properties.Priority
	for i, m := range queue {
		if m.properties.Priority < pri {
			// Insert before the first lower-priority message.
			out := make([]*Message, 0, len(queue)+1)
			out = append(out, queue[:i]...)
			out = append(out, msg)
			out = append(out, queue[i:]...)
			return out
		}
	}
	return append(queue, msg)
}
