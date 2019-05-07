package utils

import "fmt"

// ares redolog kafka topic prefix
const aresRedologKafkaTopicPrefix = "ares-redolog"

// GetTopicFromTable get the topic name for namespace and table name
func GetTopicFromTable(namespace, table string) string {
	return fmt.Sprintf("%s-%s-%s", aresRedologKafkaTopicPrefix, namespace, table)
}
