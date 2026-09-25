package objectstore

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	content := `{
		"root_dir": "./store",
		"min_part_size": 1024,
		"max_parts": 100,
		"gc_interval": 5000000000,
		"gc_unref_age": 1000000000
	}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MinPartSize != 1024 || cfg.MaxParts != 100 {
		t.Fatalf("limits not loaded: %+v", cfg)
	}
	if cfg.GCInterval != 5*time.Second || cfg.GCUnrefAge != time.Second {
		t.Fatalf("durations not loaded: %+v", cfg)
	}
	if cfg.RootDir != "./store" {
		t.Fatalf("root=%q", cfg.RootDir)
	}
}
