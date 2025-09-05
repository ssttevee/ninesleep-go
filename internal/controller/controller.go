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
	"log/slog"
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

var (
	errNoFranken = errors.New("no active connection")
)

const (
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

type Side int

const (
	SideLeft  Side = 0
	SideRight Side = 1
)

func SideFromString(s string) (Side, error) {
	switch strings.ToLower(s) {
	case "left":
		return SideLeft, nil
	case "right":
		return SideRight, nil
	default:
		return 0, fmt.Errorf("invalid side: %s", s)
	}
}

func (s Side) String() string {
	switch s {
	case SideLeft:
		return "left"
	case SideRight:
		return "right"
	default:
		return fmt.Sprintf("UNKNOWN_SIDE_%d", int(s))
	}
}

func (s Side) Opposite() Side {
	switch s {
	case SideLeft:
		return SideRight
	default:
		return SideLeft
	}
}

// AlarmInput models an alarm configuration command.
type AlarmInput struct {
	Side    Side   // logical side ("left"/"right")
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
	LedBrightness    int    `json:"ledBrightness"`

	Unknown    map[string]string `json:"unknown,omitempty"`
	ParseError string            `json:"parseError,omitempty"`
	Raw        string            `json:"raw"`
}

// Option mutates controller configuration.
type Option func(*PodController)

// WithMITM enables or disables man-in-the-middle mode.
func WithMITM(enabled bool) Option {
	return func(p *PodController) {
		p.mitmMode = enabled
	}
}

// PodController manages a single active pod connection and related state.
type PodController struct {
	mu sync.RWMutex

	mitmConnected bool
	killMitmLoop  func()

	onMitmConnected    func(connected bool)
	onMitmRequest      func(command FrankenCommand, payload string)
	onFrankenConnected func(connected bool)
	onVariables        func(pv *PodVariables)

	connMutex   sync.RWMutex
	conn        net.Conn
	mitmMode    bool
	connectedCh chan struct{}

	// reconnectCh is a length-1 buffered channel used to signal a transition
	// from connected -> disconnected.
	reconnectCh chan struct{}

	// last parsed variables
	lastVariablesRaw string
	lastParsed       *PodVariables
}

// New creates a PodController with provided options.
func New(opts ...Option) *PodController {
	p := &PodController{
		connectedCh: make(chan struct{}),
		reconnectCh: make(chan struct{}),
	}
	for _, o := range opts {
		o(p)
	}
	return p
}

func (p *PodController) GetMitmMode() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.mitmMode
}
func (p *PodController) SetMitmMode(enabled bool) {
	p.mu.RLock()
	if enabled == p.mitmMode {
		p.mu.RUnlock()
		return
	}
	p.mu.RUnlock()

	p.mu.Lock()
	p.mitmMode = enabled
	kill := p.killMitmLoop
	p.mu.Unlock()

	if enabled {
		if kill == nil {
			go p.runMitmLoop()
		}
	} else if kill != nil {
		kill()
	}
}

func (p *PodController) SetOnMitmConnected(f func(connected bool)) {
	p.mu.RLock()
	p.onMitmConnected = f
	connected := p.mitmConnected
	p.mu.RUnlock()
	f(connected)
}

func (p *PodController) SetOnMitmRequest(f func(command FrankenCommand, payload string)) {
	p.onMitmRequest = f
}

func (p *PodController) SetOnFrankenConnected(f func(connected bool)) {
	p.mu.RLock()
	p.onFrankenConnected = f
	p.mu.RUnlock()
	p.connMutex.RLock()
	connected := p.conn != nil
	p.connMutex.RUnlock()
	f(connected)
}

func (p *PodController) SetOnVariables(f func(pv *PodVariables)) {
	p.mu.RLock()
	p.onVariables = f
	p.mu.RUnlock()
	if p.lastParsed != nil {
		f(p.lastParsed)
	}
}

func (p *PodController) WaitForConn() <-chan struct{} {
	p.connMutex.RLock()
	ch := p.connectedCh
	p.connMutex.RUnlock()

	return ch
}

// setConnection sets (and replaces) the active connection.
func (p *PodController) setConnection(c net.Conn) {
	var connected bool
	defer func() {
		p.mu.RLock()
		f := p.onFrankenConnected
		p.mu.RUnlock()

		if f != nil {
			go f(connected)
		}
	}()

	p.connMutex.Lock()
	defer p.connMutex.Unlock()
	if p.conn != nil {
		_ = p.conn.Close()
	}
	p.conn = c
	connected = p.conn != nil
	if connected {
		close(p.connectedCh)

		log.Printf("[controller] new connection from %s", c.RemoteAddr().String())
	} else {
		select {
		case p.reconnectCh <- struct{}{}:
		default:
		}

		p.connectedCh = make(chan struct{})

		log.Printf("[controller] connection lost")
	}
}

// waitForFranken waits for an inbound connection on the provided listener, sets it,
// and returns the net.Conn (mirrors TS FrankenServer.waitForFranken).
func (p *PodController) waitForFranken() (net.Conn, error) {
	p.connMutex.RLock()
	conn := p.conn
	p.connMutex.RUnlock()
	if conn != nil {
		return conn, nil
	}

	if err := os.Remove(DacSocketPath); err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
	}

	l, err := net.Listen("unix", DacSocketPath)
	if err != nil {
		return nil, err
	}

	if ul, ok := l.(*net.UnixListener); ok {
		ul.SetUnlinkOnClose(true)
	}

	conn, err = l.Accept()
	if err != nil {
		return nil, err
	}

	p.setConnection(conn)

	return conn, nil
}

// ConnAlive reports whether there is an active connection.
func (p *PodController) ConnAlive() bool {
	p.connMutex.RLock()
	defer p.connMutex.RUnlock()
	return p.conn != nil
}

// ExecuteFranken issues a FrankenCommand with an optional hex payload.
func (p *PodController) ExecuteFranken(ctx context.Context, cmd FrankenCommand, payloadHex string) (string, error) {
	return p.ExecuteRaw(ctx, cmd.CommandID(), payloadHex)
}

// ExecuteRaw sends a raw command with optional hex payload and returns the response.
// If the command is PLEASE_SEND_VARIABLES the variables are parsed, cached, and
// callbacks are invoked (callbacks are executed after the lock is released).
func (p *PodController) ExecuteRaw(ctx context.Context, commandID int, payloadHex string) (string, error) {
	resp, err := p.executeLocked(ctx, commandID, payloadHex)
	if err != nil {
		p.setConnection(nil)
		return "", err
	}
	if commandID == int(FrankenCommandPleaseSendVariables) && p.onVariables != nil && p.lastParsed != nil {
		p.onVariables(p.lastParsed)
	}
	return resp, nil
}

func (p *PodController) ExecuteHeatLevel(ctx context.Context, side Side, level int) error {
	_, err := p.ExecuteRaw(ctx, int(FrankenCommandLevelLeft)+int(side), strconv.Itoa(level))
	return err
}

func (p *PodController) ExecuteHeatDuration(ctx context.Context, side Side, duration int) error {
	_, err := p.ExecuteRaw(ctx, int(FrankenCommandHeatLeft)+int(side), strconv.Itoa(duration))
	return err
}

func (p *PodController) ExecutePrime(ctx context.Context) error {
	_, err := p.ExecuteRaw(ctx, int(FrankenCommandPrime), "")
	return err
}

// ExecuteAlarm builds and sends an alarm command for a side.
func (p *PodController) ExecuteAlarm(ctx context.Context, a AlarmInput) (string, error) {
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
	return p.ExecuteRaw(ctx, int(FrankenCommandAlarmLeft)+int(a.Side), payloadHex)
}

// ExecuteSettings sends a settings command (currently LED brightness only).
func (p *PodController) ExecuteSettings(ctx context.Context, s SettingsInput) (string, error) {
	data := map[string]any{
		"lb": uint8(s.LB),
	}
	payloadHex, err := toCBORHex(data)
	if err != nil {
		return "", err
	}
	return p.ExecuteRaw(ctx, int(FrankenCommandSetSettings), payloadHex)
}

func (p *PodController) ExecuteVariables(ctx context.Context) error {
	_, err := p.ExecuteRaw(ctx, int(FrankenCommandPleaseSendVariables), "")
	return err
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
func (p *PodController) executeLocked(ctx context.Context, commandID int, payloadHex string) (string, error) {
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	p.connMutex.Lock()
	defer p.connMutex.Unlock()

	if p.conn == nil {
		log.Printf("[controller] execute command=%d error=no active connection", commandID)
		return "", errors.New("no active connection")
	}

	go func() {
		<-cctx.Done()

		if cctx.Err() != ctx.Err() {
			// `executeLocked` has returned, do nothing
			return
		}

		// canceled from outside, abort pending connections
		p.conn.SetDeadline(time.Unix(1, 0))
	}()

	p.conn.SetDeadline(time.Time{})

	var buf bytes.Buffer
	if payloadHex != "" {
		fmt.Fprintf(&buf, "%d\n%s\n\n", commandID, payloadHex)
	} else {
		fmt.Fprintf(&buf, "%d\n\n", commandID)
	}

	if _, err := buf.WriteTo(p.conn); err != nil {
		log.Printf("[controller] write failed cmd=%d payload=%s err=%v", commandID, payloadHex, err)
		return "", err
	}

	readBuf := make([]byte, 8192)
	n, err := p.conn.Read(readBuf)
	var resp string
	if err != nil {
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			resp = ""
		} else {
			return "", err
		}
	} else {
		resp = string(readBuf[:n])
	}

	if commandID == int(FrankenCommandPleaseSendVariables) {
		parsed := parseVariables(resp)
		p.lastVariablesRaw = resp
		p.lastParsed = parsed
		return resp, nil
	} else {
		log.Printf("[controller] cmd=%d payload=%s resp_len=%d err=%s", commandID, payloadHex, len(resp), errorString(err))
	}

	return resp, nil
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

			if raw, err := hex.DecodeString(pv.SettingsRaw); err == nil {
				var decoded map[string]any
				if err := cbor.Unmarshal(raw, &decoded); err == nil {
					if v, ok := decoded["lb"]; ok {
						if lb, ok := v.(uint64); ok {
							pv.LedBrightness = int(lb)
						}
					}
				}
			}
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
		}

		if err := exec.Command("systemctl", "stop", "dac").Run(); err != nil {
			log.Println("failed to stop dac service", err)
		}

		if _, err := p.waitForFranken(); err != nil {
			return fmt.Errorf("wait for franken: %w", err)
		}

		p.mu.RLock()
		mitm := p.mitmMode
		p.mu.RUnlock()

		if mitm {
			go p.runMitmLoop()
		} else {
			p.mu.RLock()
			kill := p.killMitmLoop
			p.mu.RUnlock()

			if kill != nil {
				kill()
			}
		}

		if err := exec.Command("systemctl", "start", "dac").Run(); err != nil {
			log.Println("failed to start dac service", err)
		}
	}
}

// mitmConnectAndForward connects to the original (renamed) dac socket and
// forwards commands bi‑directionally between the first‑party process and the
// active firmware connection managed by this controller.
func (p *PodController) mitmConnectAndForward(ctx context.Context) error {
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	upConn, err := net.Dial("unix", DacSocketPath)
	if err != nil {
		return fmt.Errorf("dial upstream: %w", err)
	}
	defer upConn.Close()

	go func() {
		<-cctx.Done()
		upConn.SetDeadline(time.Unix(1, 0))
	}()

	upConn.SetDeadline(time.Time{})

	{
		p.mu.Lock()
		p.mitmConnected = true
		f := p.onMitmConnected
		p.mu.Unlock()

		if f != nil {
			f(true)
		}
	}

	defer func() {
		p.mu.Lock()
		p.mitmConnected = false
		f := p.onMitmConnected
		p.mu.Unlock()

		if f != nil {
			f(false)
		}
	}()

	reader := bufio.NewReader(upConn)

	for {
		// Honor context cancellation.
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		cmdLine, err := reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("read command line: %w", err)
		}

		commandID, err := strconv.Atoi(cmdLine)
		if err != nil {
			slog.Warn("[mitm] invalid command id", "line", cmdLine)
			continue
		}

		payloadLine, err := reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("read payload line: %w", err)
		}

		payloadHex := ""
		if payloadLine != "" {
			payloadHex = payloadLine
			// Consume the required terminating blank line if present (ignore errors/timeouts).
			if term, terr := reader.ReadString('\n'); terr == nil {
				_ = term
			}
		} else {
			// payloadLine itself was the blank terminator for no-payload command;
			// nothing more to consume.
		}

		resp, err := p.ExecuteRaw(ctx, commandID, payloadHex)
		if err != nil {
			return err
		}

		if _, err := upConn.Write([]byte(resp)); err != nil {
			slog.Warn("[mitm] write response", "err", err)
		}

		// Log the MITM interaction.
		if commandID != int(FrankenCommandPleaseSendVariables) {
			slog.Info("[mitm] cmd", "cmd", commandID, "payload", payloadHex, "resp_len", len(resp))
		}

		if p.onMitmRequest != nil {
			p.onMitmRequest(FrankenCommand(commandID), payloadHex)
		}
	}
}

func (p *PodController) runMitmLoop() {
	p.mu.RLock()
	kill := p.killMitmLoop
	p.mu.RUnlock()
	if kill != nil {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p.mu.Lock()
	p.killMitmLoop = cancel
	p.mu.Unlock()

	defer func() {
		p.mu.Lock()
		p.killMitmLoop = nil
		p.mu.Unlock()
	}()

	for {
		if err := p.mitmConnectAndForward(ctx); err != nil {
			if ctx.Err() != nil {
				log.Printf("[mitm-loop] context canceled, stopping")
				return
			}

			if err == errNoFranken {
				<-p.WaitForConn()
				continue
			}

			log.Printf("[mitm-loop] error: %v", err)
			time.Sleep(5 * time.Second)
		}
	}
}
