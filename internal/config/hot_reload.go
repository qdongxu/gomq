// hot_reload.go implements configuration hot-reloading via file watching
// and SIGHUP signal handling.
package config

import (
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
)

// Reloader handles configuration file changes and applies them to the
// running server.
type Reloader struct {
	path      string
	loader    func(string) (*Config, error)
	applier   func(*Config) error
	watcher   *fsnotify.Watcher
	mu        sync.Mutex
	lastMod   time.Time
	debounce  time.Duration
	stopCh    chan struct{}
	stopped   bool
}

// NewReloader creates a configuration reloader.
// loader loads a Config from a file path.
// applier applies a new Config to the running system.
func NewReloader(path string, loader func(string) (*Config, error), applier func(*Config) error) (*Reloader, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	if err := watcher.Add(path); err != nil {
		_ = watcher.Close()
		return nil, err
	}
	// Also watch the parent directory so we catch atomic renames
	// (mv config.toml.tmp config.toml).
	dir := filepath.Dir(path)
	_ = watcher.Add(dir)

	return &Reloader{
		path:     path,
		loader:   loader,
		applier:  applier,
		watcher:  watcher,
		debounce: 500 * time.Millisecond,
		stopCh:   make(chan struct{}),
	}, nil
}

// Start begins watching the configuration file for changes.
func (r *Reloader) Start() {
	go r.loop()
}

// Stop shuts down the file watcher.
func (r *Reloader) Stop() {
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return
	}
	r.stopped = true
	r.mu.Unlock()
	close(r.stopCh)
	_ = r.watcher.Close()
}

// Reload forces an immediate configuration reload.
func (r *Reloader) Reload() error {
	return r.doReload()
}

func (r *Reloader) loop() {
	var timer *time.Timer
	for {
		select {
		case <-r.stopCh:
			if timer != nil {
				timer.Stop()
			}
			return
		case event, ok := <-r.watcher.Events:
			if !ok {
				return
			}
			if !r.isConfigEvent(event) {
				continue
			}
			if timer != nil {
				timer.Stop()
			}
			timer = time.AfterFunc(r.debounce, func() {
				if err := r.doReload(); err != nil {
					log.Printf("config reload failed: %v", err)
				}
			})
		case err, ok := <-r.watcher.Errors:
			if !ok {
				return
			}
			log.Printf("config watcher error: %v", err)
		}
	}
}

func (r *Reloader) isConfigEvent(event fsnotify.Event) bool {
	// Match on the exact file, or on the parent directory when the file
	// is written via rename (atomic replacement).
	if event.Name == r.path {
		return event.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Rename) != 0
	}
	// When watching the directory, the new file appears as a Create.
	if filepath.Base(event.Name) == filepath.Base(r.path) {
		return event.Op&(fsnotify.Create|fsnotify.Write) != 0
	}
	return false
}

func (r *Reloader) doReload() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Avoid reloading if the file hasn't actually changed (mtime).
	fi, err := os.Stat(r.path)
	if err != nil {
		return err
	}
	if fi.ModTime().Equal(r.lastMod) {
		return nil
	}
	r.lastMod = fi.ModTime()

	cfg, err := r.loader(r.path)
	if err != nil {
		return err
	}
	if err := Validate(cfg); err != nil {
		return err
	}
	if err := r.applier(cfg); err != nil {
		return err
	}
	log.Printf("config reloaded from %s", r.path)
	return nil
}
