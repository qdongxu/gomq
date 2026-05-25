// message_paging.go manages on-disk page files for queue overflow.
// When a queue exceeds an in-memory message limit, older messages are
// flushed to a page file and loaded back on demand.
package server

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// PageFile is a single on-disk page containing serialized messages.
type PageFile struct {
	path   string
	mu     sync.Mutex
	count  int   // number of messages in the page
	closed bool
}

// PageManager handles creation, reading, and deletion of page files.
type PageManager struct {
	dir      string
	seq      uint64
	mu       sync.Mutex
	pages    map[string]*PageFile
}

// NewPageManager creates a page manager in the given directory.
func NewPageManager(dir string) *PageManager {
	return &PageManager{
		dir:   dir,
		pages: make(map[string]*PageFile),
	}
}

// Flush writes messages to a new page file and returns its path.
func (pm *PageManager) Flush(queueName string, msgs []*Message) (string, error) {
	pm.mu.Lock()
	pm.seq++
	seq := pm.seq
	pm.mu.Unlock()

	name := fmt.Sprintf("%s_%d.page", queueName, seq)
	path := filepath.Join(pm.dir, name)

	if err := os.MkdirAll(pm.dir, 0750); err != nil {
		return "", fmt.Errorf("mkdir: %w", err)
	}
	f, err := os.Create(path)
	if err != nil {
		return "", fmt.Errorf("create page: %w", err)
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	for _, m := range msgs {
		if err := writeMessage(w, m); err != nil {
			return "", err
		}
	}
	if err := w.Flush(); err != nil {
		return "", fmt.Errorf("flush page: %w", err)
	}

	page := &PageFile{path: path, count: len(msgs)}
	pm.mu.Lock()
	pm.pages[path] = page
	pm.mu.Unlock()
	return path, nil
}

// Load reads all messages from a page file and deletes it.
func (pm *PageManager) Load(path string) ([]*Message, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open page: %w", err)
	}
	defer f.Close()

	var msgs []*Message
	r := bufio.NewReader(f)
	for {
		m, err := readMessage(r)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, m)
	}

	_ = f.Close()
	_ = os.Remove(path)
	pm.mu.Lock()
	delete(pm.pages, path)
	pm.mu.Unlock()
	return msgs, nil
}

// writeMessage serializes a message to the writer.
// Format: [4:payload_len][payload][4:props_len][json_props][8:delivery_tag][8:enqueued_at_unix_ns]
func writeMessage(w *bufio.Writer, m *Message) error {
	payload := m.Payload()
	if err := binary.Write(w, binary.BigEndian, uint32(len(payload))); err != nil {
		return err
	}
	if _, err := w.Write(payload); err != nil {
		return err
	}
	// Serialize properties as simple text for now.
	props := []byte(m.Properties().ContentType)
	if err := binary.Write(w, binary.BigEndian, uint32(len(props))); err != nil {
		return err
	}
	if _, err := w.Write(props); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, m.DeliveryTag()); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, m.EnqueuedAt().UnixNano()); err != nil {
		return err
	}
	return nil
}

// readMessage deserializes a message from the reader.
func readMessage(r *bufio.Reader) (*Message, error) {
	var payloadLen uint32
	if err := binary.Read(r, binary.BigEndian, &payloadLen); err != nil {
		return nil, err
	}
	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	var propsLen uint32
	if err := binary.Read(r, binary.BigEndian, &propsLen); err != nil {
		return nil, err
	}
	propsBytes := make([]byte, propsLen)
	if _, err := io.ReadFull(r, propsBytes); err != nil {
		return nil, err
	}
	var deliveryTag uint64
	if err := binary.Read(r, binary.BigEndian, &deliveryTag); err != nil {
		return nil, err
	}
	var enqueuedAtNS int64
	if err := binary.Read(r, binary.BigEndian, &enqueuedAtNS); err != nil {
		return nil, err
	}

	msg := NewMessage(payload, Properties{ContentType: string(propsBytes)})
	msg.SetDeliveryTag(deliveryTag)
	msg.SetEnqueuedAt(time.Unix(0, enqueuedAtNS))
	return msg, nil
}