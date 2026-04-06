package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadDefaultsWhenFileMissing(t *testing.T) {
	cfg, err := Load(filepath.Join(t.TempDir(), "missing.yaml"))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Server.Port != "19190" || cfg.Search.Mode != "exact" {
		t.Fatal("expected default config values")
	}
}

func TestLoadFromFileAndEnv(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(cfgPath, []byte(`
server:
  port: 20000
  read_timeout: 1s
  write_timeout: 2s
  api_key: "from-file"
  rate_limit_rps: 20
database:
  snapshot_path: "./snap.json"
  wal_path: "./wal.log"
  snapshot_every: 12
limits:
  max_body_bytes: 123
  max_vector_dim: 88
  max_k: 9
search:
  mode: "ann"
`), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("VECTOR_DB_PORT", "30000")
	t.Setenv("VECTOR_DB_API_KEY", "from-env")

	cfg, err := Load(cfgPath)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Server.Port != "30000" || cfg.Server.APIKey != "from-env" || cfg.Search.Mode != "ann" {
		t.Fatal("expected yaml + env overrides")
	}
}

func TestParseDuration(t *testing.T) {
	if got := ParseDuration("5s", time.Second); got != 5*time.Second {
		t.Fatalf("ParseDuration() = %v", got)
	}
	if got := ParseDuration("bad", time.Second); got != time.Second {
		t.Fatalf("ParseDuration() fallback = %v", got)
	}
}

func TestLoadInvalidYAML(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(cfgPath, []byte("server: ["), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(cfgPath); err == nil {
		t.Fatal("expected yaml error")
	}
}

func TestOverrideFromEnvIgnoresInvalidNumericValues(t *testing.T) {
	cfg := defaultConfig()
	t.Setenv("VECTOR_DB_RATE_LIMIT_RPS", "bad")
	t.Setenv("VECTOR_DB_SNAPSHOT_EVERY", "-1")
	t.Setenv("VECTOR_DB_MAX_BODY_BYTES", "bad")
	t.Setenv("VECTOR_DB_MAX_VECTOR_DIM", "bad")
	t.Setenv("VECTOR_DB_MAX_K", "bad")
	overrideFromEnv(&cfg)
	if cfg.Server.RateLimitRPS != 100 || cfg.Database.SnapshotEvery != 25 || cfg.Limits.MaxK != 100 {
		t.Fatal("expected invalid env values to be ignored")
	}
}

func TestOverrideFromEnvValidValues(t *testing.T) {
	cfg := defaultConfig()
	t.Setenv("VECTOR_DB_READ_TIMEOUT", "9s")
	t.Setenv("VECTOR_DB_WRITE_TIMEOUT", "11s")
	t.Setenv("VECTOR_DB_RATE_LIMIT_RPS", "50")
	t.Setenv("VECTOR_DB_SNAPSHOT_PATH", "/tmp/snap")
	t.Setenv("VECTOR_DB_WAL_PATH", "/tmp/wal")
	t.Setenv("VECTOR_DB_SNAPSHOT_EVERY", "30")
	t.Setenv("VECTOR_DB_MAX_BODY_BYTES", "2048")
	t.Setenv("VECTOR_DB_MAX_VECTOR_DIM", "256")
	t.Setenv("VECTOR_DB_MAX_K", "20")
	t.Setenv("VECTOR_DB_SEARCH_MODE", "ann")
	overrideFromEnv(&cfg)

	if cfg.Server.ReadTimeout != "9s" || cfg.Server.WriteTimeout != "11s" {
		t.Fatal("expected timeout overrides")
	}
	if cfg.Server.RateLimitRPS != 50 || cfg.Database.SnapshotEvery != 30 {
		t.Fatal("expected numeric overrides")
	}
	if cfg.Database.SnapshotPath != "/tmp/snap" || cfg.Database.WALPath != "/tmp/wal" {
		t.Fatal("expected path overrides")
	}
	if cfg.Limits.MaxBodyBytes != 2048 || cfg.Limits.MaxVectorDim != 256 || cfg.Limits.MaxK != 20 {
		t.Fatal("expected limit overrides")
	}
	if cfg.Search.Mode != "ann" {
		t.Fatal("expected search mode override")
	}
}
