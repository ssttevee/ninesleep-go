package controller

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fxamacker/cbor/v2"
)

const (
	// DefaultSocketReadTimeout limits how long we wait for a single response read.
	DefaultSocketReadTimeout = 50 * time.Millisecond
	// DefaultMaxLogEntries bounds the in‑memory log buffer.
	DefaultMaxLogEntries = 200
)

// AlarmInput models an alarm configuration command.
type AlarmInput struct {
	Side    string // logical side ("left"/"right")
	PL      int    // intensity percentage (0-100)
	DU      int    // duration seconds
	TT      int64  // unix time
	Pattern string // pattern string (e.g. "double", "rise")
}

// SettingsInput models simple settings commands (currently LED brightness).
type SettingsInput struct {
	LB int // LED brightness percent
}

// PodVariables is a structured representation of the variables response.
type PodVariables struct {
	TgHeatLevelR int    `json:"tgHeatLevelR"`
	TgHeatLevelL int    `json:"tgHeatLevelL"`
	HeatTimeL    int    `json:"heatTimeL"`
	HeatLevelL   int    `json:"heatLevelL"`
	HeatTimeR    int    `json:"heatTimeR"`
	HeatLevelR   int    `json:"heatLevelR"`
	SensorLabel  string `json:"sensorLabel"`
	WaterLevel   bool   `json:"waterLevel"`
	Priming      bool   `json:"priming"`
	SettingsRaw  string `json:"settingsRaw"`

	Unknown    map[string]string `json:"unknown,omitempty"`
	ParseError string            `json:"parseError,omitempty"`
	Raw        string            `json:"raw"`
}

// LogEntry represents a single command interaction.
type LogEntry struct {
	Time       time.Time
	Command    string
	PayloadHex string
	Response   string
	Err        string
}

// VariablesParser converts raw variable output into a structured model.
type VariablesParser func(raw string) *PodVariables

// VariablesCallback is invoked after parsing a variables response.
type VariablesCallback func(pv *PodVariables)

// Option mutates controller configuration.
type Option func(*PodController)

// WithReadTimeout overrides the socket read timeout.
func WithReadTimeout(d time.Duration) Option {
	return func(p *PodController) { p.readTimeout = d }
}

// WithMaxLogEntries overrides the log buffer size.
func WithMaxLogEntries(n int) Option {
	return func(p *PodController) {
		if n > 0 {
			p.maxLogEntries = n
		}
	}
}

// WithVariablesParser sets a custom parser for variables responses.
func WithVariablesParser(vp VariablesParser) Option {
	return func(p *PodController) { p.varParser = vp }
}

// WithVariablesCallback registers a callback invoked after each successful variables parse.
func WithVariablesCallback(cb VariablesCallback) Option {
	return func(p *PodController) {
		if cb != nil {
			p.varCallbacks = append(p.varCallbacks, cb)
		}
	}
}

// WithVariablesCommandIDs sets which command IDs should be treated as "variables" responses.
func WithVariablesCommandIDs(ids ...int) Option {
	return func(p *PodController) {
		p.variablesCmdIDs = map[int]struct{}{}
		for _, id := range ids {
			p.variablesCmdIDs[id] = struct{}{}
		}
	}
}

// PodController manages a single active pod connection and related state.
type PodController struct {
	mu sync.RWMutex

	conn          net.Conn
	connected     bool
	logBuf        []*LogEntry
	maxLogEntries int

	// configuration
	readTimeout     time.Duration
	varParser       VariablesParser
	varCallbacks    []VariablesCallback
	variablesCmdIDs map[int]struct{}

	// last parsed variables
	lastVariablesRaw string
	lastParsed       *PodVariables
}

// New creates a PodController with provided options.
func New(opts ...Option) *PodController {
	p := &PodController{
		logBuf:          make([]*LogEntry, 0, DefaultMaxLogEntries),
		readTimeout:     DefaultSocketReadTimeout,
		maxLogEntries:   DefaultMaxLogEntries,
		varParser:       defaultVariablesParser,
		varCallbacks:    nil,
		variablesCmdIDs: map[int]struct{}{14: {}}, // default mapping
	}
	for _, o := range opts {
		o(p)
	}
	return p
}

// SetConnection sets (and replaces) the active connection.
func (p *PodController) SetConnection(c net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.conn != nil {
		_ = p.conn.Close()
	}
	p.conn = c
	p.connected = true
	p.appendLogLocked(&LogEntry{
		Time:    time.Now(),
		Command: "connection",
		Response: fmt.Sprintf("new connection from %s",
			c.RemoteAddr().String()),
	})
}

// ConnAlive reports whether there is an active connection.
func (p *PodController) ConnAlive() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.connected && p.conn != nil
}

// LogSnapshot returns a copy of the recent log entries.
func (p *PodController) LogSnapshot() []*LogEntry {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]*LogEntry, len(p.logBuf))
	copy(out, p.logBuf)
	return out
}

// Execute sends a raw command with optional hex payload and returns the response.
func (p *PodController) Execute(commandID int, payloadHex string) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.conn == nil {
		err := errors.New("no active connection")
		p.appendLogLocked(&LogEntry{
			Command: strconv.Itoa(commandID),
			Err:     err.Error(),
		})
		return "", err
	}

	var buf bytes.Buffer
	if payloadHex != "" {
		fmt.Fprintf(&buf, "%d\n%s\n\n", commandID, payloadHex)
	} else {
		fmt.Fprintf(&buf, "%d\n\n", commandID)
	}

	_ = p.conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if _, err := p.conn.Write(buf.Bytes()); err != nil {
		p.appendLogLocked(&LogEntry{
			Command:    strconv.Itoa(commandID),
			PayloadHex: payloadHex,
			Err:        "write failed: " + err.Error(),
		})
		return "", err
	}

	_ = p.conn.SetReadDeadline(time.Now().Add(p.readTimeout))
	readBuf := make([]byte, 8192)
	n, rerr := p.conn.Read(readBuf)
	var resp string
	if rerr != nil {
		if ne, ok := rerr.(net.Error); ok && ne.Timeout() {
			resp = ""
		} else if errors.Is(rerr, io.EOF) {
			resp = ""
			p.connected = false
		} else {
			resp = ""
		}
	} else {
		resp = string(readBuf[:n])
	}

	p.appendLogLocked(&LogEntry{
		Command:    strconv.Itoa(commandID),
		PayloadHex: payloadHex,
		Response:   resp,
		Err:        errorString(rerr),
	})

	if _, isVarCmd := p.variablesCmdIDs[commandID]; isVarCmd {
		p.lastVariablesRaw = resp
		if p.varParser != nil {
			p.lastParsed = p.varParser(resp)
			for _, cb := range p.varCallbacks {
				cb(p.lastParsed)
			}
		}
	}

	return resp, nil
}

// ExecuteAlarm builds and sends an alarm command for a side.
func (p *PodController) ExecuteAlarm(a AlarmInput) (string, error) {
	cmdID := map[string]int{"left": 5, "right": 6}[a.Side]
	if cmdID == 0 {
		return "", fmt.Errorf("invalid side %q", a.Side)
	}
	data := map[string]any{
		"pl": uint8(a.PL),
		"du": uint16(a.DU),
		"pi": a.Pattern,
		"tt": uint64(a.TT),
	}
	payloadHex, err := toCBORHex(data)
	if err != nil {
		return "", err
	}
	return p.Execute(cmdID, payloadHex)
}

// ExecuteSettings sends a settings command (currently LED brightness only).
func (p *PodController) ExecuteSettings(s SettingsInput) (string, error) {
	data := map[string]any{
		"lb": uint8(s.LB),
	}
	payloadHex, err := toCBORHex(data)
	if err != nil {
		return "", err
	}
	return p.Execute(8, payloadHex)
}

// ParsedVariables returns a deep copy of the last parsed variables (or nil).
func (p *PodController) ParsedVariables() *PodVariables {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.lastParsed == nil {
		return nil
	}
	cp := *p.lastParsed
	if p.lastParsed.Unknown != nil {
		cp.Unknown = make(map[string]string, len(p.lastParsed.Unknown))
		for k, v := range p.lastParsed.Unknown {
			cp.Unknown[k] = v
		}
	}
	return &cp
}

// appendLogLocked appends a log entry. Caller must hold write lock.
func (p *PodController) appendLogLocked(le *LogEntry) {
	if le == nil {
		return
	}
	if le.Time.IsZero() {
		le.Time = time.Now()
	}
	p.logBuf = append([]*LogEntry{le}, p.logBuf...)
	if len(p.logBuf) > p.maxLogEntries {
		p.logBuf = p.logBuf[:p.maxLogEntries]
	}
}

// toCBORHex marshals a value to CBOR and hex-encodes it.
func toCBORHex(v any) (string, error) {
	enc, err := cbor.Marshal(v)
	if err != nil {
		return "", fmt.Errorf("cbor marshal: %w", err)
	}
	return hex.EncodeToString(enc), nil
}

// defaultVariablesParser is a basic parser suitable for the raw key=value lines format.
func defaultVariablesParser(raw string) *PodVariables {
	pv := &PodVariables{
		Unknown: map[string]string{},
		Raw:     raw,
	}
	lines := strings.Split(raw, "\n")
	for _, ln := range lines {
		ln = strings.TrimSpace(ln)
		if ln == "" {
			continue
		}
		parts := strings.SplitN(ln, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])

		stripQuotes := func(s string) string {
			if len(s) >= 2 && ((s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '\'' && s[len(s)-1] == '\'')) {
				return s[1 : len(s)-1]
			}
			return s
		}
		parseInt := func(s string) (int, error) { return strconv.Atoi(strings.TrimSpace(s)) }

		switch key {
		case "tgHeatLevelR":
			if i, e := parseInt(val); e == nil {
				pv.TgHeatLevelR = i
			} else {
				pv.ParseError += "tgHeatLevelR:" + e.Error() + ";"
			}
		case "tgHeatLevelL":
			if i, e := parseInt(val); e == nil {
				pv.TgHeatLevelL = i
			} else {
				pv.ParseError += "tgHeatLevelL:" + e.Error() + ";"
			}
		case "heatTimeL":
			if i, e := parseInt(val); e == nil {
				pv.HeatTimeL = i
			} else {
				pv.ParseError += "heatTimeL:" + e.Error() + ";"
			}
		case "heatLevelL":
			if i, e := parseInt(val); e == nil {
				pv.HeatLevelL = i
			} else {
				pv.ParseError += "heatLevelL:" + e.Error() + ";"
			}
		case "heatTimeR":
			if i, e := parseInt(val); e == nil {
				pv.HeatTimeR = i
			} else {
				pv.ParseError += "heatTimeR:" + e.Error() + ";"
			}
		case "heatLevelR":
			if i, e := parseInt(val); e == nil {
				pv.HeatLevelR = i
			} else {
				pv.ParseError += "heatLevelR:" + e.Error() + ";"
			}
		case "sensorLabel":
			pv.SensorLabel = stripQuotes(val)
		case "waterLevel":
			pv.WaterLevel = strings.EqualFold(val, "true")
		case "priming":
			pv.Priming = strings.EqualFold(val, "true")
		case "settings":
			pv.SettingsRaw = stripQuotes(val)
		default:
			pv.Unknown[key] = val
		}
	}
	if len(pv.Unknown) == 0 {
		pv.Unknown = nil
	}
	return pv
}

// errorString produces a short classification for an error (not exported).
func errorString(err error) string {
	if err == nil {
		return ""
	}
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		return "timeout"
	}
	return err.Error()
}
