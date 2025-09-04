package controller

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"maps"
	"net"
	"os"
	"os/exec"
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

	// DacSocketPath is the unix domain socket path used by the firmware and first‑party service.
	DacSocketPath = "/deviceinfo/dac.sock"
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

// VariablesCallback is invoked after parsing a variables response.
type VariablesCallback func(pv *PodVariables)

// Option mutates controller configuration.
type Option func(*PodController)

// WithReadTimeout overrides the socket read timeout.
func WithReadTimeout(d time.Duration) Option {
	return func(p *PodController) { p.readTimeout = d }
}

// WithVariablesCallback registers a callback invoked after each successful variables parse.
func WithVariablesCallback(cb VariablesCallback) Option {
	return func(p *PodController) {
		if cb != nil {
			p.varCallbacks = append(p.varCallbacks, cb)
		}
	}
}

// WithMITM enables or disables man-in-the-middle mode.
func WithMITM(enabled bool) Option {
	return func(p *PodController) {
		p.mitmMode = enabled
	}
}

// PodController manages a single active pod connection and related state.
type PodController struct {
	mu sync.RWMutex

	conn      net.Conn
	connected bool
	mitmMode  bool

	// reconnectCh is a length-1 buffered channel used to signal a transition
	// from connected -> disconnected.
	reconnectCh chan struct{}

	// active (stored) unix listener for franken firmware
	frankenLn net.Listener

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
		readTimeout: DefaultSocketReadTimeout,
	}
	for _, o := range opts {
		o(p)
	}
	return p
}

// setConnection sets (and replaces) the active connection.
func (p *PodController) setConnection(c net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.conn != nil {
		_ = p.conn.Close()
	}
	p.conn = c
	p.connected = true
	log.Printf("[controller] new connection from %s", c.RemoteAddr().String())
}

// waitForFranken waits for an inbound connection on the provided listener, sets it,
// and returns the net.Conn (mirrors TS FrankenServer.waitForFranken).
func (p *PodController) waitForFranken() (net.Conn, error) {
	p.mu.RLock()
	connected := p.connected
	conn := p.conn
	p.mu.RUnlock()
	if connected && conn != nil {
		return conn, nil
	}

	conn, err := p.frankenLn.Accept()
	if err != nil {
		return nil, err
	}
	p.setConnection(conn)
	return conn, nil
}

// ConnAlive reports whether there is an active connection.
func (p *PodController) ConnAlive() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.connected && p.conn != nil
}

// ExecuteFranken issues a FrankenCommand with an optional hex payload.
func (p *PodController) ExecuteFranken(cmd FrankenCommand, payloadHex string) (string, error) {
	return p.ExecuteRaw(cmd.CommandID(), payloadHex)
}

// ExecuteRaw sends a raw command with optional hex payload and returns the response.
// If the command is PLEASE_SEND_VARIABLES the variables are parsed, cached, and
// callbacks are invoked (callbacks are executed after the lock is released).
func (p *PodController) ExecuteRaw(commandID int, payloadHex string) (string, error) {
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
	return p.ExecuteRaw(cmdID, payloadHex)
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
	return p.ExecuteRaw(int(FrankenCommandSetSettings), payloadHex)
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
		maps.Copy(cp.Unknown, p.lastParsed.Unknown)
	}
	return &cp
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
		log.Printf("[controller] execute command=%d error=no active connection", commandID)
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
		log.Printf("[controller] write failed cmd=%d payload=%s err=%v", commandID, payloadHex, err)
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
			select {
			case p.reconnectCh <- struct{}{}:
			default:
			}
		} else {
			resp = ""
		}
	} else {
		resp = string(readBuf[:n])
	}

	if commandID == int(FrankenCommandPleaseSendVariables) {
		parsed := parseVariables(resp)
		p.lastVariablesRaw = resp
		p.lastParsed = parsed
		callbacks := append([]VariablesCallback(nil), p.varCallbacks...)
		return resp, parsed, callbacks, nil
	} else {
		log.Printf("[controller] cmd=%d payload=%s resp_len=%d err=%s", commandID, payloadHex, len(resp), errorString(rerr))
	}

	return resp, nil, nil, nil
}

// parseVariables converts raw key=value lines into a PodVariables structure.
func parseVariables(raw string) *PodVariables {
	pv := &PodVariables{
		Unknown: map[string]string{},
		Raw:     raw,
	}
	for ln := range strings.SplitSeq(raw, "\n") {
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

// cleanupUnixSocket removes a pre-existing unix domain socket file.
//
// Mirrors the tryCleanup behavior in the original FrankenServer.start (TS) but is
// exposed so callers can explicitly manage lifecycle when embedding the controller.
func (p *PodController) cleanupUnixSocket(path string) error {
	if err := os.Remove(path); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	return nil
}

// startUnixSocket creates (or recreates) the primary unix domain socket listener at DacSocketPath
// and stores it in the controller. If a previous stored listener exists it is closed first.
// The stored listener is used by RunUnixSocketLoop unless overridden during MITM injection.
//
// Concurrency:
//   - Caller should NOT hold p.mu; this function acquires it internally only after
//     the listener is successfully created.
//   - Safe to call repeatedly; an existing different listener will be closed.
//
// Idempotency:
//   - If the existing listener already listens on DacSocketPath it is replaced
//     without error (the old one is closed).
func (p *PodController) startUnixSocket() error {
	if err := p.cleanupUnixSocket(DacSocketPath); err != nil {
		return fmt.Errorf("cleanup socket: %w", err)
	}
	l, err := net.Listen("unix", DacSocketPath)
	if err != nil {
		return err
	}
	if ul, ok := l.(*net.UnixListener); ok {
		ul.SetUnlinkOnClose(true)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.frankenLn != nil && p.frankenLn != l {
		_ = p.frankenLn.Close()
	}
	p.frankenLn = l
	return nil
}

// stopUnixSocket closes the currently stored unix listener (if present) and
// clears the reference so a later startUnixSocket or InjectMITM can recreate it.
func (p *PodController) stopUnixSocket() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.frankenLn == nil {
		return nil
	}
	err := p.frankenLn.Close()
	p.frankenLn = nil
	return err
}

// RunUnixSocketLoop ensures a unix socket listener exists and continuously
// accepts firmware connections until the context is canceled.
func (p *PodController) RunUnixSocketLoop(ctx context.Context) error {
	dir := filepath.Dir(DacSocketPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		// If a connection is already active, wait efficiently for it to end.
		if p.ConnAlive() {
			select {
			case <-ctx.Done():
				return nil
			case <-p.reconnectCh:
			}
			continue
		}

		if p.mitmMode {
			if err := p.reconnectFrankenMitm(ctx); err != nil {
				if ctx.Err() != nil {
					return nil
				}

				return err
			}

		} else {
			if err := p.reconnectFrankenNormal(true); err != nil {
				if ctx.Err() != nil {
					return nil
				}

				return err
			}
		}
	}
}

func (p *PodController) reconnectFrankenNormal(reuse bool) error {
	p.mu.RLock()
	ln := p.frankenLn
	p.mu.RUnlock()

	if ln == nil || !reuse {
		if err := p.startUnixSocket(); err != nil {
			return fmt.Errorf("start unix socket: %w", err)
		}
	}

	// Accept firmware connection.
	if _, err := p.waitForFranken(); err != nil {
		return fmt.Errorf("wait for franken: %w", err)
	}

	return nil
}

// mitmConnectAndForward connects to the original (renamed) dac socket and
// forwards commands bi‑directionally between the first‑party process and the
// active firmware connection managed by this controller.
func (p *PodController) mitmConnectAndForward(ctx context.Context) error {
	upConn, err := net.Dial("unix", DacSocketPath)
	if err != nil {
		return fmt.Errorf("dial upstream: %w", err)
	}
	defer upConn.Close()

	reader := bufio.NewReader(upConn)

	for {
		// Honor context cancellation.
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		_ = upConn.SetReadDeadline(time.Now().Add(5 * time.Second))
		cmdLine, err := reader.ReadString('\n')
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("read command line: %w", err)
		}
		cmdLine = strings.TrimSpace(cmdLine)
		if cmdLine == "" {
			// Skip stray blank lines.
			continue
		}
		commandID, err := strconv.Atoi(cmdLine)
		if err != nil {
			// Malformed line; log and continue.
			log.Printf("[mitm] invalid command id line=%q", cmdLine)
			continue
		}

		_ = upConn.SetReadDeadline(time.Now().Add(2 * time.Second))
		payloadLine, err := reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("read payload line: %w", err)
		}
		payloadLineTrimmed := strings.TrimSpace(payloadLine)

		payloadHex := ""
		if payloadLineTrimmed != "" {
			payloadHex = payloadLineTrimmed
			// Consume the required terminating blank line if present (ignore errors/timeouts).
			_ = upConn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
			if term, terr := reader.ReadString('\n'); terr == nil {
				_ = term
			}
		} else {
			// payloadLine itself was the blank terminator for no-payload command;
			// nothing more to consume.
		}

		resp, execErr := p.ExecuteRaw(commandID, payloadHex)

		// Write response back (best effort).
		_ = upConn.SetWriteDeadline(time.Now().Add(2 * time.Second))
		if resp == "" {
			// Maintain protocol expectation of something being sent; at least a newline.
			_, _ = upConn.Write([]byte("\n"))
		} else {
			_, _ = upConn.Write([]byte(resp))
		}

		// Log the MITM interaction.
		if commandID != int(FrankenCommandPleaseSendVariables) {
			log.Printf("[mitm] cmd=%d payload=%s resp_len=%d err=%s", commandID, payloadHex, len(resp), errorString(execErr))
		}
	}
}

// reconnectFrankenMitm performs ephemeral MITM setup. After completion the controller
// proxies commands between the firmware (captured via a temporary intercept socket) and
// the first‑party process which continues to own dac.sock. We do NOT keep dac.sock bound;
// if this process exits the system keeps working normally.
//
// Ephemeral flow (crash‑safe):
//  1. systemctl stop dac
//  2. Create a temporary intercept socket and accept a firmware (franken) connection
//  3. Close (unbind) the temporary intercept socket so dac can recreate dac.sock
//  4. systemctl start dac
//  5. Once dac.sock is recreated, begin MITM forwarding (background goroutine)
//
// Re‑injection:
//   - If the firmware disconnects, this can be invoked again.
//   - Because dac.sock is not held long‑term, crashes revert cleanly to native behavior.
//
// Returns after initial setup; forwarding continues in a background goroutine.
func (p *PodController) reconnectFrankenMitm(ctx context.Context) error {
	logStep := func(step, msg string, err error) {
		if err != nil {
			log.Printf("[mitm-inject] step=%s msg=%s err=%s", step, msg, errorString(err))
		} else {
			log.Printf("[mitm-inject] step=%s %s", step, msg)
		}
	}

	// Step 1: stop dac service.
	if err := exec.Command("systemctl", "stop", "dac").Run(); err != nil {
		logStep("stop", "systemctl stop dac", err)
		return fmt.Errorf("stop dac: %w", err)
	}
	logStep("stop", "dac stopped", nil)

	// Step 2: start temporary intercept socket to capture firmware connection.
	if err := p.reconnectFrankenNormal(false); err != nil {
		logStep("start-listener-initial", "start intercept socket", err)
		return fmt.Errorf("start initial socket: %w", err)
	}
	logStep("start-listener-initial", "intercept listener started", nil)

	// Step 3: unbind (close) our temporary listener (SetUnlinkOnClose will remove the path).
	if err := p.stopUnixSocket(); err != nil {
		logStep("unbind-initial", "close initial listener", err)
		return fmt.Errorf("close initial listener: %w", err)
	}
	logStep("unbind-initial", "initial listener closed", nil)

	// Step 4: restart dac service so it recreates dac.sock.
	if err := exec.Command("systemctl", "start", "dac").Run(); err != nil {
		logStep("start-dac", "systemctl start dac", err)
		return fmt.Errorf("start dac: %w", err)
	}
	logStep("start-dac", "dac started", nil)

	// Step 5: start forwarding loop (connect to dac.sock as a client) in background.
	go p.runMitmLoop(ctx)

	logStep("complete", "MITM injection complete", nil)
	return nil
}

func (p *PodController) runMitmLoop(ctx context.Context) {
	for {
		if err := p.mitmConnectAndForward(ctx); err != nil {
			if ctx.Err() != nil {
				log.Printf("[mitm-loop] context canceled, stopping")
				return
			}

			log.Printf("[mitm-loop] error: %v", err)
			time.Sleep(5 * time.Second)
		}
	}
}
