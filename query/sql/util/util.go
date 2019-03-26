package util

import (
	"errors"
	"strings"
)

// RequireNonNull validate a point is not null
func RequireNonNull(pointer interface{}, errMsg string) {
	if pointer == nil {
		panic(errors.New(errMsg))
	}
}

// GetSubstring returns substring embeded in a node's value (substring)
func GetSubstring(str string) string {
	front := strings.Index(str, "(")
	return str[front+1 : len(str)-1]
}

// TrimQuote remove leading and tail quote
func TrimQuote(str string) string {
	if strings.HasPrefix(str, "\"") {
		return strings.Trim(str, "\"")
	} else if strings.HasPrefix(str, "'") {
		return strings.Trim(str, "'")
	}
	return str
}
