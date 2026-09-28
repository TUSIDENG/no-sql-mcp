package mcp

// intParam reads an integer parameter, falling back to defaultVal when it is
// missing or not a number.
func intParam(params map[string]any, name string, defaultVal int) int {
	switch v := params[name].(type) {
	case float64:
		return int(v)
	case int:
		return v
	case int64:
		return int(v)
	default:
		return defaultVal
	}
}

// stringSliceParam reads a string array parameter.
func stringSliceParam(params map[string]any, name string) []string {
	raw, ok := params[name].([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		if s, ok := item.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// toAnySlice converts a string slice to an argument slice.
func toAnySlice(in []string) []any {
	out := make([]any, len(in))
	for i, v := range in {
		out[i] = v
	}
	return out
}
