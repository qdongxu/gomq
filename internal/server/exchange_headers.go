// exchange_headers.go implements header-based routing.
package server

// HeadersExchange routes messages by matching header key-value pairs.
type HeadersExchange struct{}

// Route returns queue names whose binding arguments match msg headers.
// The binding argument "x-match" controls match mode:
//   - "all" (default): every specified header must exist and match exactly
//   - "any": at least one specified header must exist and match
func (e *HeadersExchange) Route(
	_ string,
	msgHeaders map[string]interface{},
	bindings []*Binding,
) []string {
	var queues []string
	seen := make(map[string]struct{})
	for _, b := range bindings {
		if matchHeaders(msgHeaders, b.Args) {
			if _, ok := seen[b.QueueName]; !ok {
				queues = append(queues, b.QueueName)
				seen[b.QueueName] = struct{}{}
			}
		}
	}
	return queues
}

// matchHeaders reports whether msgHeaders satisfy the binding arguments.
func matchHeaders(
	msgHeaders map[string]interface{},
	args map[string]interface{},
) bool {
	if len(args) == 0 {
		return false
	}

	// Determine match mode; default to "all".
	mode := "all"
	if m, ok := args["x-match"]; ok {
		if s, ok := m.(string); ok {
			mode = s
		}
	}

	if mode == "any" {
		// At least one non-x-match key must exist and match.
		for k, v := range args {
			if k == "x-match" {
				continue
			}
			mv, ok := msgHeaders[k]
			if ok && valuesEqual(mv, v) {
				return true
			}
		}
		return false
	}

	// "all" mode: every non-x-match key must exist and match.
	for k, v := range args {
		if k == "x-match" {
			continue
		}
		mv, ok := msgHeaders[k]
		if !ok || !valuesEqual(mv, v) {
			return false
		}
	}
	return true
}

// valuesEqual compares two values for equality.
// Supported types: string, bool, numeric kinds (converted to float64).
func valuesEqual(a, b interface{}) bool {
	switch av := a.(type) {
	case string:
		bv, ok := b.(string)
		return ok && av == bv
	case bool:
		bv, ok := b.(bool)
		return ok && av == bv
	case int:
		return int64(av) == toInt64(b)
	case int8:
		return int64(av) == toInt64(b)
	case int16:
		return int64(av) == toInt64(b)
	case int32:
		return int64(av) == toInt64(b)
	case int64:
		return int64(av) == toInt64(b)
	case uint:
		return uint64(av) == toUint64(b)
	case uint8:
		return uint64(av) == toUint64(b)
	case uint16:
		return uint64(av) == toUint64(b)
	case uint32:
		return uint64(av) == toUint64(b)
	case uint64:
		return uint64(av) == toUint64(b)
	case float32:
		return float64(av) == toFloat64(b)
	case float64:
		return float64(av) == toFloat64(b)
	}
	return false
}

// toInt64 converts supported numeric types to int64.
func toInt64(v interface{}) int64 {
	switch x := v.(type) {
	case int:
		return int64(x)
	case int8:
		return int64(x)
	case int16:
		return int64(x)
	case int32:
		return int64(x)
	case int64:
		return x
	}
	return 0
}

// toUint64 converts supported numeric types to uint64.
func toUint64(v interface{}) uint64 {
	switch x := v.(type) {
	case uint:
		return uint64(x)
	case uint8:
		return uint64(x)
	case uint16:
		return uint64(x)
	case uint32:
		return uint64(x)
	case uint64:
		return x
	}
	return 0
}

// toFloat64 converts supported numeric types to float64.
func toFloat64(v interface{}) float64 {
	switch x := v.(type) {
	case float32:
		return float64(x)
	case float64:
		return x
	case int:
		return float64(x)
	case int8:
		return float64(x)
	case int16:
		return float64(x)
	case int32:
		return float64(x)
	case int64:
		return float64(x)
	case uint:
		return float64(x)
	case uint8:
		return float64(x)
	case uint16:
		return float64(x)
	case uint32:
		return float64(x)
	case uint64:
		return float64(x)
	}
	return 0
}
