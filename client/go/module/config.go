package module

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// LoadConfig loads configuration from a YAML file
func LoadConfig(configPath string) (*Config, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Set default secret key for nodes that don't have one
	for i := range config.Nodes {
		if config.Nodes[i].SecretKey == "" {
			config.Nodes[i].SecretKey = config.DefaultSecretKey
		}
		// Initialize unavailableUntil to zero time
		config.Nodes[i].UnavailableUntil = time.Time{}
		// Initialize datasource unavailableUntil
		for j := range config.Nodes[i].Datasources {
			config.Nodes[i].Datasources[j].UnavailableUntil = time.Time{}
		}
	}

	return &config, nil
}
