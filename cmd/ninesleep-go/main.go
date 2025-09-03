package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/grandcat/zeroconf"

	// Gosthome (ESPHome native API) integration
	_ "github.com/gosthome/gosthome/components" // register default components (api, sensor, textsensor, etc.)
	"github.com/gosthome/gosthome/components/api"
	"github.com/gosthome/gosthome/core"
	"github.com/gosthome/gosthome/core/bus"
	"github.com/gosthome/gosthome/core/component"
	"github.com/gosthome/gosthome/core/config"
	"github.com/gosthome/gosthome/core/entity"
	"github.com/gosthome/gosthome/core/registry"
	"github.com/gosthome/gosthome/core/state"
)

/*
Enhancement: adds parsing of the /variables output into a structured model.

Original behavior summary:
  - UNIX domain socket listener: /deviceinfo/dac.sock
  - HTTP test UI: 0.0.0.0:8080
  - Commands per original Rust prototype.

Enhancements in this revision:
  - Parse variables response (command 14) into structured fields (podVariables).
  - Store most recent parsed variables + raw.
  - Show structured JSON block on UI when available.
*/

const (
	socketPath     = "/deviceinfo/dac.sock"
	httpListenAddr = "0.0.0.0:8080"
	readTimeout    = 50 * time.Millisecond
	maxLogEntries  = 200
)

var (
	uiTmpl = template.Must(template.New("ui").Funcs(template.FuncMap{
		"trim": strings.TrimSpace,
	}).Parse(htmlPage))
)

// ---- Data Models ----

type alarmInput struct {
	Side    string
	PL      int
	DU      int
	TT      int64
	Pattern string
}

type settingsInput struct {
	LB int
}

// Parsed variables structure
type podVariables struct {
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

// ---- Command Execution Core ----

type podController struct {
	mu sync.RWMutex

	active    net.Conn
	connected bool

	logBuf []*logEntry

	lastVariablesRaw string
	lastParsed       *podVariables
}

type logEntry struct {
	Time       time.Time
	Command    string
	PayloadHex string
	Response   string
	Err        string
}

func newPodController() *podController {
	return &podController{
		logBuf: make([]*logEntry, 0, maxLogEntries),
	}
}

func (p *podController) setConn(c net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.active != nil {
		_ = p.active.Close()
	}
	p.active = c
	p.connected = true
	p.appendLog(&logEntry{
		Time:    time.Now(),
		Command: "connection",
		Response: fmt.Sprintf("New connection from %s",
			c.RemoteAddr().String()),
	})
}

func (p *podController) appendLog(le *logEntry) {
	if le == nil {
		return
	}
	if le.Time.IsZero() {
		le.Time = time.Now()
	}
	p.logBuf = append([]*logEntry{le}, p.logBuf...)
	if len(p.logBuf) > maxLogEntries {
		p.logBuf = p.logBuf[:maxLogEntries]
	}
}

func (p *podController) logSnapshot() []*logEntry {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]*logEntry, len(p.logBuf))
	copy(out, p.logBuf)
	return out
}

func (p *podController) connAlive() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.connected && p.active != nil
}

func (p *podController) execute(commandID int, payloadHex string) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.active == nil {
		err := errors.New("no active pod connection")
		p.appendLog(&logEntry{
			Command: fmt.Sprintf("%d", commandID),
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

	_ = p.active.SetWriteDeadline(time.Now().Add(2 * time.Second))
	_, err := p.active.Write(buf.Bytes())
	if err != nil {
		p.appendLog(&logEntry{
			Command: fmt.Sprintf("%d", commandID),
			Err:     "write failed: " + err.Error(),
		})
		return "", err
	}

	_ = p.active.SetReadDeadline(time.Now().Add(readTimeout))
	readBuf := make([]byte, 8192)
	n, rerr := p.active.Read(readBuf)
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

	p.appendLog(&logEntry{
		Command:    fmt.Sprintf("%d", commandID),
		PayloadHex: payloadHex,
		Response:   resp,
		Err:        errorString(rerr),
	})

	// Parse variables if that's the command
	if commandID == 14 {
		p.lastVariablesRaw = resp
		p.lastParsed = parseVariables(resp)
		// Propagate to gosthome sensors if integration initialized
		updateGosthomeFromParsed(p.lastParsed)
	}

	return resp, nil
}

func (p *podController) executeAlarm(side string, a alarmInput) (string, error) {
	cmdID := map[string]int{
		"left":  5,
		"right": 6,
	}[side]
	if cmdID == 0 {
		return "", fmt.Errorf("invalid side %q", side)
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
	return p.execute(cmdID, payloadHex)
}

func (p *podController) executeSettings(s settingsInput) (string, error) {
	data := map[string]any{
		"lb": uint8(s.LB),
	}
	payloadHex, err := toCBORHex(data)
	if err != nil {
		return "", err
	}
	return p.execute(8, payloadHex)
}

func (p *podController) getParsedVariables() *podVariables {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.lastParsed == nil {
		return nil
	}
	// Shallow copy (maps copy)
	cp := *p.lastParsed
	if p.lastParsed.Unknown != nil {
		cp.Unknown = make(map[string]string, len(p.lastParsed.Unknown))
		for k, v := range p.lastParsed.Unknown {
			cp.Unknown[k] = v
		}
	}
	return &cp
}

// ---- Variables Parser ----

func parseVariables(raw string) *podVariables {
	pv := &podVariables{
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

		parseInt := func(s string) (int, error) {
			return strconv.Atoi(strings.TrimSpace(s))
		}

		switch key {
		case "tgHeatLevelR":
			i, e := parseInt(val)
			if e == nil {
				pv.TgHeatLevelR = i
			} else {
				pv.ParseError += "tgHeatLevelR:" + e.Error() + ";"
			}
		case "tgHeatLevelL":
			i, e := parseInt(val)
			if e == nil {
				pv.TgHeatLevelL = i
			} else {
				pv.ParseError += "tgHeatLevelL:" + e.Error() + ";"
			}
		case "heatTimeL":
			i, e := parseInt(val)
			if e == nil {
				pv.HeatTimeL = i
			} else {
				pv.ParseError += "heatTimeL:" + e.Error() + ";"
			}
		case "heatLevelL":
			i, e := parseInt(val)
			if e == nil {
				pv.HeatLevelL = i
			} else {
				pv.ParseError += "heatLevelL:" + e.Error() + ";"
			}
		case "heatTimeR":
			i, e := parseInt(val)
			if e == nil {
				pv.HeatTimeR = i
			} else {
				pv.ParseError += "heatTimeR:" + e.Error() + ";"
			}
		case "heatLevelR":
			i, e := parseInt(val)
			if e == nil {
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

// ---- CBOR Helpers ----

func toCBORHex(v any) (string, error) {
	enc, err := cbor.Marshal(v)
	if err != nil {
		return "", fmt.Errorf("cbor marshal: %w", err)
	}
	return hex.EncodeToString(enc), nil
}

// ---- HTTP Layer ----

type server struct {
	pod *podController
	mux *http.ServeMux
}

func newServer(pod *podController) *server {
	s := &server{
		pod: pod,
		mux: http.NewServeMux(),
	}
	s.routes()
	return s
}

func (s *server) routes() {
	s.mux.HandleFunc("/", s.handleUI)
	s.mux.HandleFunc("/action", s.handleAction)
	s.mux.HandleFunc("/logs.json", s.handleLogsJSON)
	s.mux.HandleFunc("/variables.json", s.handleVarsJSON)
}

func (s *server) handleUI(w http.ResponseWriter, r *http.Request) {
	vars := s.pod.getParsedVariables()
	var varsJSON string
	if vars != nil {
		if b, err := json.MarshalIndent(vars, "", "  "); err == nil {
			varsJSON = string(b)
		}
	}
	data := struct {
		Connected bool
		Logs      []*logEntry
		NowUnix   int64
		VarsJSON  string
	}{
		Connected: s.pod.connAlive(),
		Logs:      s.pod.logSnapshot(),
		NowUnix:   time.Now().Unix(),
		VarsJSON:  varsJSON,
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := uiTmpl.Execute(w, data); err != nil {
		http.Error(w, err.Error(), 500)
	}
}

func (s *server) handleLogsJSON(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(s.pod.logSnapshot())
}

func (s *server) handleVarsJSON(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	v := s.pod.getParsedVariables()
	if v == nil {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"no variables parsed yet"}`))
		return
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func (s *server) handleAction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	_ = r.ParseForm()
	action := r.FormValue("action")
	var err error
	var resp string

	switch action {

	case "hello":
		resp, err = s.pod.execute(0, "")

	case "variables":
		resp, err = s.pod.execute(14, "")

	case "alarm":
		side := r.FormValue("side")
		pl, _ := strconv.Atoi(r.FormValue("pl"))
		du, _ := strconv.Atoi(r.FormValue("du"))
		tt, _ := strconv.ParseInt(r.FormValue("tt"), 10, 64)
		pattern := r.FormValue("pi")
		if pattern == "" {
			pattern = "double"
		}
		resp, err = s.pod.executeAlarm(side, alarmInput{
			Side: side, PL: pl, DU: du, TT: tt, Pattern: pattern,
		})

	case "alarm-clear":
		resp, err = s.pod.execute(16, "")

	case "settings":
		lb, _ := strconv.Atoi(r.FormValue("lb"))
		resp, err = s.pod.executeSettings(settingsInput{LB: lb})

	case "temperature":
		side := r.FormValue("side")
		val, _ := strconv.Atoi(r.FormValue("value"))
		cmd := map[string]int{"left": 11, "right": 12}[side]
		if cmd == 0 {
			err = fmt.Errorf("invalid side")
			break
		}
		resp, err = s.pod.execute(cmd, strconv.Itoa(val))

	case "temperature-duration":
		side := r.FormValue("side")
		val, _ := strconv.Atoi(r.FormValue("value"))
		cmd := map[string]int{"left": 9, "right": 10}[side]
		if cmd == 0 {
			err = fmt.Errorf("invalid side")
			break
		}
		resp, err = s.pod.execute(cmd, strconv.Itoa(val))

	case "prime":
		resp, err = s.pod.execute(13, "")

	default:
		err = fmt.Errorf("unknown action %q", action)
	}

	if err != nil {
		http.Error(w, "Action error: "+err.Error(), 500)
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprintf(w, "OK action=%s\nResponse:\n%s\n\nBack: /\n", action, resp)
}

// ---- UNIX Socket Listener ----

func runUnixListener(ctx context.Context, pod *podController) error {
	dir := filepath.Dir(socketPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}
	if st, err := os.Stat(socketPath); err == nil && (st.Mode()&os.ModeSocket) != 0 {
		_ = os.Remove(socketPath)
	}

	l, err := net.Listen("unix", socketPath)
	if err != nil {
		return fmt.Errorf("listen unix: %w", err)
	}
	log.Printf("[unix] listening on %s", socketPath)

	go func() {
		<-ctx.Done()
		_ = l.Close()
	}()

	for {
		c, err := l.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
			}
			if !errors.Is(err, net.ErrClosed) {
				log.Printf("[unix] accept error: %v", err)
			}
			return err
		}
		log.Printf("[unix] accepted connection")
		pod.setConn(c)
	}
}

// ---- Gosthome Integration (dynamic entities) ----
//
// This section now unconditionally initializes a gosthome (ESPHome native API)
// node and dynamically registers sensors/text sensors/binary sensors that
// reflect parsed Pod variables plus a heartbeat & availability indicator.

var (
	gosthomeOnce       sync.Once
	gosthomeNode       *core.Node
	gosthomeInitErr    error
	gosthomeCtxCancel  context.CancelFunc
	gosthomeSensorLock sync.Mutex

	// base context used for creating dynamic gosthome entity states (must carry bus)
	gosthomeBaseCtx context.Context
	lastPollSuccess time.Time
)

func gosthomeCtx() context.Context {
	// Returns a non-nil context for creating dynamic entity states.
	// Falls back to Background() if gosthome not fully initialized yet.
	if gosthomeBaseCtx != nil {
		return gosthomeBaseCtx
	}
	return context.Background()
}

// updateGosthomeFromParsed is currently a placeholder that just logs.
// Later it will push each parsed field into a corresponding gosthome sensor.
func updateGosthomeFromParsed(pv *podVariables) {
	if pv == nil || gosthomeNode == nil {
		return
	}

	ensureGosthomeEntities()

	// Numeric sensors
	setFloatSensor("tg_heat_level_left", float32(pv.TgHeatLevelL), "lvl")
	setFloatSensor("tg_heat_level_right", float32(pv.TgHeatLevelR), "lvl")
	setFloatSensor("heat_level_left", float32(pv.HeatLevelL), "lvl")
	setFloatSensor("heat_level_right", float32(pv.HeatLevelR), "lvl")
	setFloatSensor("heat_time_left_seconds", float32(pv.HeatTimeL), "s")
	setFloatSensor("heat_time_right_seconds", float32(pv.HeatTimeR), "s")

	// Binary sensors (represented as float 0/1 if binary domain not yet wired)
	setBinarySensor("water_level_ok", pv.WaterLevel)
	setBinarySensor("priming_active", pv.Priming)

	// Text sensors
	setTextSensor("sensor_label", pv.SensorLabel)
	setTextSensor("settings_raw", pv.SettingsRaw)

	// Heartbeat (epoch seconds) - updated here as well (fast path)
	setFloatSensor("pod_heartbeat_epoch", float32(time.Now().Unix()), "s")
}

// initGosthome initializes a gosthome node with only the API component active.
func initGosthome(parent context.Context, pod *podController) {
	gosthomeOnce.Do(func() {
		ctx, cancel := context.WithCancel(parent)
		gosthomeCtxCancel = cancel
		// Do not set gosthomeBaseCtx yet; bus not attached until node is constructed

		// Build minimal config programmatically
		apiPort := uint16(6053)
		if ps := os.Getenv("GOSTHOME_API_PORT"); ps != "" {
			if v, err := strconv.Atoi(ps); err == nil && v > 0 && v < 65536 {
				apiPort = uint16(v)
			}
		}

		// Prepare MAC (ignore error for deterministic demo MAC; in real code handle it)
		mac, _ := config.ParseMAC("02:00:00:00:00:01")

		// Build minimal config using proper gosthome types
		apiCfg := api.NewConfig()
		// Leaving API component ID at default (no explicit ID set)
		apiCfg.Address = "0.0.0.0"
		apiCfg.Port = apiPort

		cfg := &config.Config{
			Registry: registry.DefaultRegistry(),
			Gosthome: config.GosthomeConfig{
				Name: "pod3",
				MAC:  mac,
			},
			Components: config.Configs{
				"api": component.NewConfigDecoder(apiCfg),
			},
		}

		node, err := core.NewNode(ctx, cfg)
		if err != nil {
			gosthomeInitErr = fmt.Errorf("gosthome init node: %w", err)
			slog.Error("Failed to initialize gosthome", "err", err)
			return
		}
		gosthomeNode = node

		// Manually create required domains if not present.
		for _, d := range []entity.DomainDefinition{
			entity.PublicDomain(&entity.SensorDomain{}),
			entity.PublicDomain(&entity.BinarySensorDomain{}),
			entity.PublicDomain(&entity.TextSensorDomain{}),
		} {
			if err := gosthomeNode.Registry.CreateDomain(d); err != nil {
				// Ignore duplicate registration; only log if it is not that case.
				if _, ok := err.(entity.ErrAlreadyRegistered); !ok {
					slog.Warn("Failed to create domain", "err", err)
				}
			}
		}

		// Attach bus to a base context for dynamic state objects
		gosthomeBaseCtx = bus.Context(context.Background(), gosthomeNode.Bus)
		go node.Start()
		slog.Info("Gosthome (ESPHome API) started",
			"port", apiPort,
			"name", cfg.Gosthome.Name,
		)

		// mDNS / Zeroconf advertisement for Home Assistant ESPHome discovery
		go func() {
			host := cfg.Gosthome.Name
			txt := []string{
				// Minimal useful TXT records (ESPHome typically publishes more,
				// but these are enough for discovery).
				"version=1.0",
				"address=" + apiCfg.Address,
			}
			svc, err := zeroconf.Register(
				host, "_esphomelib._tcp", "local.",
				int(apiPort), txt, nil,
			)
			if err != nil {
				slog.Error("mDNS register failed", "err", err)
				return
			}
			slog.Info("mDNS advertisement started",
				"service", "_esphomelib._tcp",
				"port", apiPort,
				"host", host,
			)
			<-ctx.Done()
			svc.Shutdown()
			slog.Info("mDNS advertisement stopped")
		}()

		// Start polling loop (variables command 14) with diff-based updates
		go pollingLoop(ctx, pod)
	})
}

// ---------- Dynamic gosthome entities & polling ----------

type floatSensor struct {
	ent  *entity.BaseEntity
	st   state.State_[entity.SensorState]
	unit string
}

// component.Component methods
func (f *floatSensor) Setup()       {}
func (f *floatSensor) Close() error { return nil }
func (f *floatSensor) InitializationPriority() component.InitializationPriority {
	return component.InitializationPriorityBus
}

func (f *floatSensor) AccuracyDecimals() int32             { return 2 }
func (f *floatSensor) ForceUpdate() bool                   { return false }
func (f *floatSensor) StateClass() entity.SensorStateClass { return entity.SensorStateClassMeasurement }
func (f *floatSensor) LastResetType() entity.SensorLastResetType {
	return entity.SensorLastResetTypeNone
}
func (f *floatSensor) UnitOfMeasurement() string             { return f.unit }
func (f *floatSensor) DeviceClass() entity.SensorDeviceClass { return "" }
func (f *floatSensor) Icon() string                          { return "" }
func (f *floatSensor) State() entity.SensorState             { return f.st.State() }
func (f *floatSensor) ID() string                            { return f.ent.ID() }
func (f *floatSensor) HashID() uint32                        { return f.ent.HashID() }
func (f *floatSensor) Name() string                          { return f.ent.Name() }
func (f *floatSensor) Internal() bool                        { return f.ent.Internal() }
func (f *floatSensor) DisabledByDefault() bool               { return f.ent.DisabledByDefault() }
func (f *floatSensor) EntityCategory() entity.Category       { return f.ent.EntityCategory() }

type binSensor struct {
	ent *entity.BaseEntity
	st  state.State_[entity.BinarySensorState]
}

// component.Component methods
func (b *binSensor) Setup()       {}
func (b *binSensor) Close() error { return nil }
func (b *binSensor) InitializationPriority() component.InitializationPriority {
	return component.InitializationPriorityBus
}

func (b *binSensor) IsStatusBinarySensor() bool                  { return false }
func (b *binSensor) DeviceClass() entity.BinarySensorDeviceClass { return "" }
func (b *binSensor) Icon() string                                { return "" }
func (b *binSensor) State() entity.BinarySensorState             { return b.st.State() }
func (b *binSensor) ID() string                                  { return b.ent.ID() }
func (b *binSensor) HashID() uint32                              { return b.ent.HashID() }
func (b *binSensor) Name() string                                { return b.ent.Name() }
func (b *binSensor) Internal() bool                              { return b.ent.Internal() }
func (b *binSensor) DisabledByDefault() bool                     { return b.ent.DisabledByDefault() }
func (b *binSensor) EntityCategory() entity.Category             { return b.ent.EntityCategory() }

type textSensor struct {
	ent *entity.BaseEntity
	st  state.State_[entity.TextSensorState]
}

// component.Component methods
func (t *textSensor) Setup()       {}
func (t *textSensor) Close() error { return nil }
func (t *textSensor) InitializationPriority() component.InitializationPriority {
	return component.InitializationPriorityBus
}

func (t *textSensor) DeviceClass() entity.SensorDeviceClass { return "" }
func (t *textSensor) Icon() string                          { return "" }
func (t *textSensor) State() entity.TextSensorState         { return t.st.State() }
func (t *textSensor) ID() string                            { return t.ent.ID() }
func (t *textSensor) HashID() uint32                        { return t.ent.HashID() }
func (t *textSensor) Name() string                          { return t.ent.Name() }
func (t *textSensor) Internal() bool                        { return t.ent.Internal() }
func (t *textSensor) DisabledByDefault() bool               { return t.ent.DisabledByDefault() }
func (t *textSensor) EntityCategory() entity.Category       { return t.ent.EntityCategory() }

var (
	ghEntitiesOnce sync.Once
	floatSensors   = map[string]*floatSensor{}
	binarySensors  = map[string]*binSensor{}
	textSensors    = map[string]*textSensor{}
	sensorMu       sync.Mutex
)

func ensureGosthomeEntities() {
	ghEntitiesOnce.Do(func() {
		if gosthomeNode == nil {
			return
		}
	})
}

func setFloatSensor(id string, val float32, unit string) {
	if gosthomeNode == nil {
		return
	}
	sensorMu.Lock()
	defer sensorMu.Unlock()
	fs, ok := floatSensors[id]
	if !ok {
		fs = newFloatSensor(id, unit)
		if err := gosthomeNode.Registry.RegisterSensor(fs); err != nil {
			slog.Error("register sensor failed", "id", id, "err", err)
			return
		}
		floatSensors[id] = fs
	}
	cur := fs.State()
	if cur.State != val || cur.MissingState {
		cur.State = val
		cur.MissingState = false
		fs.st.SetState(cur)
	}
}

func newFloatSensor(id, unit string) *floatSensor {
	cfg := &entity.EntityConfig{
		ID:   id,
		Name: prettyName(id),
	}
	beVal := entity.NewBaseEntity(entity.DomainTypeSensor, cfg)
	be := &beVal
	st, _ := state.NewState(gosthomeCtx(), be, entity.SensorState{State: 0, MissingState: true})
	return &floatSensor{ent: be, st: st, unit: unit}
}

func setBinarySensor(id string, on bool) {
	if gosthomeNode == nil {
		return
	}
	sensorMu.Lock()
	defer sensorMu.Unlock()
	bs, ok := binarySensors[id]
	if !ok {
		bs = newBinarySensor(id)
		if err := gosthomeNode.Registry.RegisterBinarySensor(bs); err != nil {
			slog.Error("register binary sensor failed", "id", id, "err", err)
			return
		}
		binarySensors[id] = bs
	}
	cur := bs.State()
	if cur.State != on || cur.Missing {
		cur.State = on
		cur.Missing = false
		bs.st.SetState(cur)
	}
}

func newBinarySensor(id string) *binSensor {
	cfg := &entity.EntityConfig{
		ID:   id,
		Name: prettyName(id),
	}
	beVal := entity.NewBaseEntity(entity.DomainTypeBinarySensor, cfg)
	be := &beVal
	st, _ := state.NewState(gosthomeCtx(), be, entity.BinarySensorState{State: false, Missing: true})
	return &binSensor{ent: be, st: st}
}

func setTextSensor(id, val string) {
	if gosthomeNode == nil {
		return
	}
	sensorMu.Lock()
	defer sensorMu.Unlock()
	ts, ok := textSensors[id]
	if !ok {
		ts = newTextSensor(id)
		if err := gosthomeNode.Registry.RegisterTextSensor(ts); err != nil {
			slog.Error("register text sensor failed", "id", id, "err", err)
			return
		}
		textSensors[id] = ts
	}
	cur := ts.State()
	if cur.State != val || cur.MissingState {
		cur.State = val
		cur.MissingState = false
		ts.st.SetState(cur)
	}
}

func newTextSensor(id string) *textSensor {
	cfg := &entity.EntityConfig{
		ID:   id,
		Name: prettyName(id),
	}
	beVal := entity.NewBaseEntity(entity.DomainTypeTextSensor, cfg)
	be := &beVal
	st, _ := state.NewState(gosthomeCtx(), be, entity.TextSensorState{State: "", MissingState: true})
	return &textSensor{ent: be, st: st}
}

func updateAvailability() {
	// Consider pod available if last successful poll was within 2*interval + small grace
	grace := 5 * time.Second
	ok := false
	if !lastPollSuccess.IsZero() {
		ok = time.Since(lastPollSuccess) <= 2*pollInterval+grace
	}
	setBinarySensor("pod_available", ok)
}

func prettyName(id string) string {
	parts := strings.Split(id, "_")
	for i, p := range parts {
		if p == "" {
			continue
		}
		parts[i] = strings.ToUpper(p[:1]) + p[1:]
	}
	return strings.Join(parts, " ")
}

const pollInterval = 15 * time.Second

func pollingLoop(ctx context.Context, pod *podController) {
	t := time.NewTicker(pollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			// Always request variables to keep time-based changes visible.
			_, err := pod.execute(14, "")
			now := time.Now()
			if err != nil {
				// Failure path: still bump heartbeat, update availability (likely false)
				setFloatSensor("pod_heartbeat_epoch", float32(now.Unix()), "s")
				updateAvailability()
				continue
			}
			// Success
			lastPollSuccess = now
			updateAvailability()
			// pod.execute already parsed & called updateGosthomeFromParsed (which set heartbeat)
		}
	}
}

// ---- Main ----

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	pod := newPodController()

	// Optionally start gosthome (ESPHome native API server) for HA integration
	// Always enable gosthome integration (env gating removed)
	initGosthome(ctx, pod)
	if gosthomeInitErr != nil {
		log.Printf("[gosthome] initialization error: %v", gosthomeInitErr)
	} else {
		log.Printf("[gosthome] integration enabled")
	}

	go func() {
		if err := runUnixListener(ctx, pod); err != nil {
			log.Printf("unix listener exited: %v", err)
			cancel()
		}
	}()

	s := newServer(pod)
	httpSrv := &http.Server{
		Addr:              httpListenAddr,
		Handler:           logRequestMiddleware(s.mux),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Printf("[http] listening on %s", httpListenAddr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("http server error: %v", err)
			cancel()
		}
	}()

	<-ctx.Done()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = httpSrv.Shutdown(shutdownCtx)
	log.Println("Exiting.")
}

// ---- Helpers ----

func errorString(err error) string {
	if err == nil {
		return ""
	}
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		return "timeout"
	}
	return err.Error()
}

func logRequestMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := &respWriter{ResponseWriter: w, status: 200}
		next.ServeHTTP(ww, r)
		log.Printf("[http] %s %s %d %s", r.Method, r.URL.Path, ww.status, time.Since(start))
	})
}

type respWriter struct {
	http.ResponseWriter
	status int
}

func (rw *respWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}

// ---- HTML Template ----

const htmlPage = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Pod Control Test UI (Go)</title>
<style>
body { font-family: system-ui, Arial, sans-serif; margin: 20px; line-height:1.4; }
fieldset { margin-bottom: 1.5em; }
legend { font-weight: bold; }
button { padding: 0.4em 0.9em; margin:0.2em; }
input[type=number], input[type=text] { width: 7em; }
.status { padding:0.4em 0.8em; border-radius:4px; display:inline-block;
  background:#eee; font-weight:bold; }
.status.ok { background:#c8f7c5; }
.status.no { background:#f9d0d0; }
.logs { font-family: monospace; white-space: pre-wrap; background:#111; color:#ddd; padding:10px; border-radius:6px; max-height:400px; overflow:auto; }
.log-entry { margin-bottom: 0.8em; border-bottom:1px solid #333; padding-bottom:0.4em; }
small { color:#888; }
form.inline { display:inline; }
code { background:#f4f4f4; padding:1px 3px; border-radius:3px; }
pre.vars { background:#222; color:#9f9; padding:10px; border-radius:6px; overflow:auto; max-height:350px; }
</style>
</head>
<body>
<h1>Pod Control Test UI (Go)</h1>
<p>Connection status:
  {{if .Connected}}<span class="status ok">CONNECTED</span>{{else}}<span class="status no">NO CONNECTION</span>{{end}}
</p>

<section>
  <fieldset>
    <legend>Basic Queries</legend>
    <form method="post" action="/action" class="inline">
      <input type="hidden" name="action" value="hello">
      <button type="submit">Hello (0)</button>
    </form>
    <form method="post" action="/action" class="inline">
      <input type="hidden" name="action" value="variables">
      <button type="submit">Variables (14)</button>
    </form>
  </fieldset>

  <fieldset>
    <legend>Temperature Target (Tenths °C)</legend>
    <form method="post" action="/action">
      <input type="hidden" name="action" value="temperature">
      <label>Side:
        <select name="side">
          <option value="left">left</option>
          <option value="right">right</option>
        </select>
      </label>
      <label>Value (e.g. -40 = -4°C): <input type="number" name="value" value="0"></label>
      <button type="submit">Set Temperature</button>
    </form>
  </fieldset>

  <fieldset>
    <legend>Temperature Duration (seconds)</legend>
    <form method="post" action="/action">
      <input type="hidden" name="action" value="temperature-duration">
      <label>Side:
        <select name="side">
          <option value="left">left</option>
          <option value="right">right</option>
        </select>
      </label>
      <label>Seconds: <input type="number" name="value" value="7200"></label>
      <button type="submit">Set Duration</button>
    </form>
  </fieldset>

  <fieldset>
    <legend>Alarm</legend>
    <form method="post" action="/action">
      <input type="hidden" name="action" value="alarm">
      <label>Side:
        <select name="side">
          <option value="left">left</option>
          <option value="right">right</option>
        </select>
      </label>
      <label>Intensity % (pl): <input type="number" name="pl" value="50" min="0" max="100"></label>
      <label>Duration s (du): <input type="number" name="du" value="600" min="0"></label>
      <label>Unix Time (tt): <input type="number" name="tt" value="{{.NowUnix}}"></label>
      <label>Pattern (pi):
        <select name="pi">
          <option value="double">double</option>
          <option value="rise">rise</option>
        </select>
      </label>
      <button type="submit">Set Alarm (5/6)</button>
    </form>
    <form method="post" action="/action" class="inline">
      <input type="hidden" name="action" value="alarm-clear">
      <button type="submit">Alarm Clear (16)</button>
    </form>
  </fieldset>

  <fieldset>
    <legend>Settings (LED Brightness 'lb')</legend>
    <form method="post" action="/action">
      <input type="hidden" name="action" value="settings">
      <label>LED Brightness %: <input type="number" name="lb" min="0" max="100" value="20"></label>
      <button type="submit">Apply Settings (8)</button>
    </form>
  </fieldset>

  <fieldset>
    <legend>Prime</legend>
    <form method="post" action="/action">
      <input type="hidden" name="action" value="prime">
      <button type="submit">Prime (13)</button>
    </form>
  </fieldset>

  {{if .VarsJSON}}
  <fieldset>
    <legend>Parsed Variables</legend>
    <pre class="vars">{{.VarsJSON}}</pre>
    <p><small>Raw & parsed representation of the last Variables command (14).</small></p>
  </fieldset>
  {{end}}
</section>

<h2>Recent Log</h2>
<div class="logs">
  {{range .Logs}}
    <div class="log-entry">
      <div><strong>{{.Time.Format "15:04:05.000"}}</strong> cmd=<code>{{.Command}}</code>
      {{if .PayloadHex}} payload=<code>{{.PayloadHex}}</code>{{end}}</div>
      {{if .Err}}<div style="color:#ff8080;">err: {{.Err}}</div>{{end}}
      {{if .Response}}<div style="color:#9cdcfe; white-space:pre-wrap;">{{trim .Response}}</div>{{end}}
    </div>
  {{end}}
  {{if not .Logs}}<em>No log entries yet.</em>{{end}}
</div>

<p><small>Endpoints: <code>/logs.json</code>, <code>/variables.json</code>. Refresh page to update UI.</small></p>

</body>
</html>
`
