// i18n.go provides multi-language support for the web UI.
package web

import (
	"encoding/json"
	"net/http"
	"strings"
	"sync"
)

// I18n loads and serves translated strings.
type I18n struct {
	mu    sync.RWMutex
	langs map[string]map[string]string
}

// NewI18n creates an empty i18n manager.
func NewI18n() *I18n {
	return &I18n{langs: make(map[string]map[string]string)}
}

// Load registers a language pack from raw JSON bytes.
func (i *I18n) Load(lang string, data []byte) error {
	var pack map[string]string
	if err := json.Unmarshal(data, &pack); err != nil {
		return err
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	i.langs[lang] = pack
	return nil
}

// T returns the translated string for key in lang. Falls back to
// the key itself if the translation is missing.
func (i *I18n) T(lang, key string) string {
	i.mu.RLock()
	defer i.mu.RUnlock()
	if pack, ok := i.langs[lang]; ok {
		if t, ok := pack[key]; ok {
			return t
		}
	}
	return key
}

// Pack returns the full language pack for lang, or nil.
func (i *I18n) Pack(lang string) map[string]string {
	i.mu.RLock()
	defer i.mu.RUnlock()
	out := make(map[string]string, len(i.langs[lang]))
	for k, v := range i.langs[lang] {
		out[k] = v
	}
	return out
}

// Supported returns the list of loaded language codes.
func (i *I18n) Supported() []string {
	i.mu.RLock()
	defer i.mu.RUnlock()
	out := make([]string, 0, len(i.langs))
	for k := range i.langs {
		out = append(out, k)
	}
	return out
}

// DetectLang picks the best language for the request. Cookie
// `lang` takes priority; otherwise Accept-Language header is
// parsed. Defaults to "en".
func (i *I18n) DetectLang(r *http.Request) string {
	if c, err := r.Cookie("lang"); err == nil && c.Value != "" {
		if i.Has(c.Value) {
			return c.Value
		}
	}
	al := r.Header.Get("Accept-Language")
	if al != "" {
		for _, seg := range strings.Split(al, ",") {
			seg = strings.TrimSpace(seg)
			if idx := strings.Index(seg, ";"); idx != -1 {
				seg = seg[:idx]
			}
			seg = strings.ToLower(seg)
			if i.Has(seg) {
				return seg
			}
			// "zh-CN" → "zh"
			if idx := strings.Index(seg, "-"); idx != -1 {
				base := seg[:idx]
				if i.Has(base) {
					return base
				}
			}
		}
	}
	return "en"
}

func (i *I18n) Has(lang string) bool {
	i.mu.RLock()
	defer i.mu.RUnlock()
	_, ok := i.langs[lang]
	return ok
}
