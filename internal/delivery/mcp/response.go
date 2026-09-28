// Package mcp is the delivery layer: MCP tool registration, request handling
// and validation, authentication and HTTP transports.
package mcp

// TextContent builds a successful MCP response containing a single text block.
func TextContent(text string) map[string]any {
	return map[string]any{
		"content": []map[string]any{
			{
				"type": "text",
				"text": text,
			},
		},
	}
}

// ErrorContent builds the unified error response structure. It must never echo
// credentials or connection strings.
func ErrorContent(source, operation, reason string) map[string]any {
	return map[string]any{
		"content": []map[string]any{
			{
				"type": "text",
				"text": "error: " + reason,
			},
		},
		"isError": true,
		"error": map[string]any{
			"source":    source,
			"operation": operation,
			"reason":    reason,
		},
	}
}
