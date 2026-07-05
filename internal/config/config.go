// Package config reads service configuration from the environment — the only
// configuration source in a container platform.
package config

import "os"

// EnvOr returns the value of key, or def when unset/empty.
func EnvOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
