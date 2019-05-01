package utils

import "fmt"

// GetTopicFromTable get the topic name for namespace and table name
func GetTopicFromTable(namespace, table string) string {
	return fmt.Sprintf("%s-%s", namespace, table)
}
