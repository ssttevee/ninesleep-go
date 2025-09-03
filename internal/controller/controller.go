package controller

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
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

// FrankenCommand mirrors the command nomenclature (utils.ts) from the original implementation.
type FrankenCommand int

const (
	FrankenCommandHello               FrankenCommand = 0
	FrankenCommandSetTemp             FrankenCommand = 1
	FrankenCommandSetAlarm            FrankenCommand = 2
	FrankenCommandReset               FrankenCommand = 3
	FrankenCommandForceReset          FrankenCommand = 4
	FrankenCommandAlarmLeft           FrankenCommand = 5
	FrankenCommandAlarmRight          FrankenCommand = 6
	FrankenCommandFormat              FrankenCommand = 7
	FrankenCommandSetSettings         FrankenCommand = 8
	FrankenCommandHeatLeft            FrankenCommand = 9
	FrankenCommandHeatRight           FrankenCommand = 10
	FrankenCommandLevelLeft           FrankenCommand = 11
	FrankenCommandLevelRight          FrankenCommand = 12
	FrankenCommandPrime               FrankenCommand = 13
	FrankenCommandPleaseSendVariables FrankenCommand = 14
)

// String returns the symbolic name of the command (useful for logs / diagnostics).
func (c FrankenCommand) String() string {
	switch c {
	case FrankenCommandHello:
		return "HELLO"
	case FrankenCommandSetTemp:
		return "SET_TEMP"
	case FrankenCommandSetAlarm:
		return "SET_ALARM"
	case FrankenCommandReset:
		return "RESET"
	case FrankenCommandForceReset:
		return "FORCE_RESET"
	case FrankenCommandAlarmLeft:
		return "ALARM_LEFT"
	case FrankenCommandAlarmRight:
		return "ALARM_RIGHT"
	case FrankenCommandFormat:
		return "FORMAT"
	case FrankenCommandSetSettings:
		return "SET_SETTINGS"
	case FrankenCommandHeatLeft:
		return "HEAT_LEFT"
	case FrankenCommandHeatRight:
		return "HEAT_RIGHT"
	case FrankenCommandLevelLeft:
		return "LEVEL_LEFT"
	case FrankenCommandLevelRight:
		return "LEVEL_RIGHT"
	case FrankenCommandPrime:
		return "PRIME"
	case FrankenCommandPleaseSendVariables:
		return "PLEASE_SEND_VARIABLES"
	default:
		return fmt.Sprintf("UNKNOWN_COMMAND_%d", int(c))
	}
}

// CommandID returns the raw integer ID used on the wire.
func (c FrankenCommand) CommandID() int { return int(c) }

// funcNameToFrankenCommand parallels utils.ts mapping allowing textual function names to resolve.
var funcNameToFrankenCommand = map[string]FrankenCommand{
	"reset":       FrankenCommandReset,
	"force-reset": FrankenCommandForceReset,
	"format":      FrankenCommandFormat,
	"alarmR":      FrankenCommandAlarmRight,
	"alarmL":      FrankenCommandAlarmLeft,
	"setsettings": FrankenCommandSetSettings,
	"prime":       FrankenCommandPrime,
	"leftHeat":    FrankenCommandHeatLeft,
	"leftLevel":   FrankenCommandLevelLeft,
	"rightHeat":   FrankenCommandHeatRight,
	"rightLevel":  FrankenCommandLevelRight,
}

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
	TargetHeatLevelR int    `json:"targetHeatLevelR"`
	TargetHeatLevelL int    `json:"targetHeatLevelL"`
	HeatTimeL        int    `json:"heatTimeL"`
	HeatLevelL       int    `json:"heatLevelL"`
	HeatTimeR        int    `json:"heatTimeR"`
	HeatLevelR       int    `json:"heatLevelR"`
	SensorLabel      string `json:"sensorLabel"`
	WaterLevel       bool   `json:"waterLevel"`
	Priming          bool   `json:"priming"`
	SettingsRaw      string `json:"settingsRaw"`

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

// WithVariablesCallback registers a callback invoked after each successful variables parse.
func WithVariablesCallback(cb VariablesCallback) Option {
	return func(p *PodController) {
		if cb != nil {
			p.varCallbacks = append(p.varCallbacks, cb)
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
	readTimeout  time.Duration
	varCallbacks []VariablesCallback

	// last parsed variables
	lastVariablesRaw string
	lastParsed       *PodVariables
}

// New creates a PodController with provided options.
func New(opts ...Option) *PodController {
	p := &PodController{
		logBuf:        make([]*LogEntry, 0, DefaultMaxLogEntries),
		readTimeout:   DefaultSocketReadTimeout,
		maxLogEntries: DefaultMaxLogEntries,
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

// WaitForFranken waits for an inbound connection on the provided listener, sets it,
// and returns the net.Conn (mirrors TS FrankenServer.waitForFranken).
func (p *PodController) WaitForFranken(ln net.Listener) (net.Conn, error) {
	conn, err := ln.Accept()
	if err != nil {
		return nil, err
	}
	p.SetConnection(conn)
	return conn, nil
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

// ExecuteFranken issues a FrankenCommand with an optional hex payload.
func (p *PodController) ExecuteFranken(cmd FrankenCommand, payloadHex string) (string, error) {
	return p.Execute(cmd.CommandID(), payloadHex)
}

// GetVariables sends PLEASE_SEND_VARIABLES and returns a raw key/value map.
// Caching, structured parsing, and callbacks are performed inside Execute
// when the variables command is issued.
func (p *PodController) GetVariables() (map[string]string, error) {
	resp, err := p.ExecuteFranken(FrankenCommandPleaseSendVariables, "")
	if err != nil {
		return nil, err
	}
	vars := map[string]string{}
	lines := strings.Split(resp, "\n")
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if l == "" {
			continue
		}
		parts := strings.SplitN(l, "=", 2)
		if len(parts) != 2 {
			continue
		}
		k := strings.TrimSpace(parts[0])
		v := strings.TrimSpace(parts[1])
		vars[k] = v
	}
	return vars, nil
}

// Execute sends a raw command with optional hex payload and returns the response.
// If the command is PLEASE_SEND_VARIABLES the variables are parsed, cached, and
// callbacks are invoked (callbacks are executed after the lock is released).
func (p *PodController) Execute(commandID int, payloadHex string) (string, error) {
	resp, parsed, callbacks, err := p.executeLocked(commandID, payloadHex)
	if err != nil {
		return "", err
	}
	if parsed != nil {
		for _, cb := range callbacks {
			cb(parsed)
		}
	}
	return resp, nil
}

// ExecuteAlarm builds and sends an alarm command for a side.
func (p *PodController) ExecuteAlarm(a AlarmInput) (string, error) {
	cmdID := map[string]int{"left": int(FrankenCommandAlarmLeft), "right": int(FrankenCommandAlarmRight)}[a.Side]
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
	return p.Execute(int(FrankenCommandSetSettings), payloadHex)
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

// executeLocked performs the core send/receive while holding the lock.
// It returns: response string, parsed variables (if any), callbacks snapshot and error.
func (p *PodController) executeLocked(commandID int, payloadHex string) (string, *PodVariables, []VariablesCallback, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.conn == nil {
		err := errors.New("no active connection")
		p.appendLogLocked(&LogEntry{
			Command: strconv.Itoa(commandID),
			Err:     err.Error(),
		})
		return "", nil, nil, err
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
		return "", nil, nil, err
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

	if commandID == int(FrankenCommandPleaseSendVariables) {
		parsed := parseVariables(resp)
		p.lastVariablesRaw = resp
		p.lastParsed = parsed
		callbacks := append([]VariablesCallback(nil), p.varCallbacks...)
		return resp, parsed, callbacks, nil
	}

	return resp, nil, nil, nil
}

// parseVariables converts raw key=value lines into a PodVariables structure.
func parseVariables(raw string) *PodVariables {
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
				pv.TargetHeatLevelR = i
			} else {
				pv.ParseError += "tgHeatLevelR:" + e.Error() + ";"
			}
		case "tgHeatLevelL":
			if i, e := parseInt(val); e == nil {
				pv.TargetHeatLevelL = i
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

// TryCleanupUnixSocket removes a pre-existing unix domain socket file.
//
// Mirrors the tryCleanup behavior in the original FrankenServer.start (TS) but is
// exposed so callers can explicitly manage lifecycle when embedding the controller.
func (p *PodController) CleanupUnixSocket(path string) error {
	if err := os.Remove(path); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	return nil
}

// StartFrankenUnixSocket creates and starts a unix domain socket listener used to
// accept Franken (firmware) connections. This is analogous to FrankenServer.start
// in the TypeScript reference. The caller is responsible for closing the returned
// listener. If the underlying *net.UnixListener is obtained, SetUnlinkOnClose(true)
// is invoked to ensure cleanup on close.
//
// Typical usage:
//
//	if err := TryCleanupUnixSocket(sockPath); err != nil { ... }
//	ln, err := StartFrankenUnixSocket(sockPath)
//	conn, err := controller.WaitForFranken(ln)
func (p *PodController) StartUnixSocket(path string) (net.Listener, error) {
	if err := p.CleanupUnixSocket(path); err != nil {
		return nil, fmt.Errorf("cleanup socket: %w", err)
	}
	l, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if ul, ok := l.(*net.UnixListener); ok {
		ul.SetUnlinkOnClose(true)
	}
	return l, nil
}

// RunUnixSocketLoop ensures the directory for the socket exists, starts (or
// reuses) a unix socket listener and continuously waits for incoming firmware
// (franken) connections until the context is canceled. Each accepted
// connection is set as the active connection. Returns when context is done
// or when a non‑context related accept error occurs.
func (p *PodController) RunUnixSocketLoop(ctx context.Context, sockPath string) error {
	dir := filepath.Dir(sockPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}

	ln, err := p.StartUnixSocket(sockPath)
	if err != nil {
		return err
	}
	defer ln.Close()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		_, err := p.WaitForFranken(ln)
		if err != nil {
			// If context canceled, treat as graceful exit.
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
	}
}
