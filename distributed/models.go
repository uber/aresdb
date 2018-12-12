package distributed

// Instance models an aresdb instance
type Instance struct {
	Name string `json:"name"`
	Host string `json:"host"`
	Port int    `json:"port"`
}
