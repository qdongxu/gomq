package config

// Config holds server configuration.
// Fields will be populated in Issue #2 (TOML parser).
type Config struct {
	ConfigPath string
}

// Load reads configuration from the given file path.
// Currently a stub; full TOML parsing comes in Issue #2.
func Load(path string) (*Config, error) {
	return &Config{ConfigPath: path}, nil
}
