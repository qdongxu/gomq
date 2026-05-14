// loader.go reads TOML configuration files and overlays environment
// variables on top of the parsed values.
package config

import (
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"

	"github.com/pelletier/go-toml/v2"
)

// Load reads a TOML file at path, applies defaults, overlays environment
// variables, and returns the resulting Config. If path is empty, only
// defaults and environment variables are used.
func Load(path string) (*Config, error) {
	cfg := Default()

	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read config: %w", err)
		}
		if err := toml.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("parse config: %w", err)
		}
	}

	if err := applyEnv(cfg); err != nil {
		return nil, fmt.Errorf("apply env: %w", err)
	}

	return cfg, nil
}

// applyEnv walks cfg recursively and overrides fields with matching
// environment variables. Variable names follow GOMQ_<SECTION>_<FIELD>
// or GOMQ_<FIELD> for top-level fields, using the toml tag name.
func applyEnv(cfg *Config) error {
	return applyEnvStruct(reflect.ValueOf(cfg).Elem(), "GOMQ")
}

// applyEnvStruct performs the recursive env-var overlay.
func applyEnvStruct(v reflect.Value, prefix string) error {
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		fv := v.Field(i)

		if !fv.CanSet() {
			continue
		}

		name := field.Tag.Get("toml")
		if name == "" {
			name = strings.ToLower(field.Name)
		}
		name = strings.ToUpper(name)

		if fv.Kind() == reflect.Struct {
			nextPrefix := prefix + "_" + name
			if err := applyEnvStruct(fv, nextPrefix); err != nil {
				return err
			}
			continue
		}

		key := prefix + "_" + name
		val, ok := os.LookupEnv(key)
		if !ok {
			continue
		}

		if err := setField(fv, val); err != nil {
			return fmt.Errorf("env %s: %w", key, err)
		}
	}
	return nil
}

// setField converts a string value and assigns it to a reflect.Value.
func setField(v reflect.Value, s string) error {
	switch v.Kind() {
	case reflect.String:
		v.SetString(s)
	case reflect.Int, reflect.Int8, reflect.Int16,
		reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		v.SetInt(n)
	case reflect.Bool:
		b, err := strconv.ParseBool(s)
		if err != nil {
			return err
		}
		v.SetBool(b)
	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.String {
			parts := strings.Split(s, ",")
			for i := range parts {
				parts[i] = strings.TrimSpace(parts[i])
			}
			v.Set(reflect.ValueOf(parts))
			return nil
		}
		return fmt.Errorf("unsupported slice type %v", v.Type())
	default:
		return fmt.Errorf("unsupported type %v", v.Kind())
	}
	return nil
}
