package utils

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
)

const (
	assetFileName = "asset-manifest.json"
)

// ResourceMapper maps path to a static resource built by react
type ResourceMapper struct {
	assetMap map[string]string
}

// NewResourceMapper creates a new resource mapper
func NewResourceMapper(buildPath string) (mapper ResourceMapper) {
	filePath := path.Join(buildPath, assetFileName)
	jsonFile, err := os.Open(filePath)
	if err != nil {
		// keep quiet for now
		fmt.Printf("failed to read asset-manifest for resource mapper from %s\n", filePath)
		return
	}

	byteValue, _ := ioutil.ReadAll(jsonFile)

	err = jsonFile.Close()
	if err != nil {
		// keep quiet for now
		return
	}
	var assetMap map[string]string
	err = json.Unmarshal([]byte(byteValue), &assetMap)
	if err != nil {
		// keep quiet for now
		return
	}

	return ResourceMapper{
		assetMap: assetMap,
	}
}

// Map returns physical path of resource
func (m ResourceMapper) Map(assetName string) (string, error) {
	if v, ok := m.assetMap[assetName]; ok {
		return v, nil
	}
	return "", fmt.Errorf("failed to get resource path for %s", assetName)
}
