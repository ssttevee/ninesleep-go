package gost

import (
	"context"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"eightsleep-esphome/internal/controller"

	"github.com/grandcat/zeroconf"

	"github.com/gosthome/gosthome/components/api"
	"github.com/gosthome/gosthome/core"
	"github.com/gosthome/gosthome/core/bus"
	"github.com/gosthome/gosthome/core/component"
	"github.com/gosthome/gosthome/core/config"
	"github.com/gosthome/gosthome/core/entity"
	"github.com/gosthome/gosthome/core/registry"
	"github.com/gosthome/gosthome/core/state"

	_ "github.com/gosthome/gosthome/components" // register default components
)

// Manager owns the gosthome (ESPHome native API) node and dynamic entities.
type Manager struct {
	mu sync.RWMutex

	node      *core.Node
	initErr   error
	cancel    context.CancelFunc
	baseCtx   context.Context
	pod       *controller.PodController
	onceStart sync.Once

	// configuration
	name        string
	apiPort     uint16
	pollEvery   time.Duration
	enableMDNS  bool
	varsCommand int // which command ID triggers variable refresh (mirrors controller default 14)

	// last success tracking
	lastPollSuccess time.Time

	// entity maps
	floatSensors  map[string]*floatSensor
	binarySensors map[string]*binSensor
	textSensors   map[string]*textSensor
}

// Option configures a Manager.
type Option func(*Manager)

// WithName overrides the node name (default "pod3").
func WithName(n string) Option {
	return func(m *Manager) {
		if n != "" {
			m.name = n
		}
	}
}

// WithAPIPort sets the API TCP port (default 6053).
func WithAPIPort(port uint16) Option {
	return func(m *Manager) {
		if port > 0 {
			m.apiPort = port
		}
	}
}

// WithPollingInterval sets the interval between variable polling.
func WithPollingInterval(d time.Duration) Option {
	return func(m *Manager) {
		if d > 0 {
			m.pollEvery = d
		}
	}
}

// WithMDNS enables or disables zeroconf advertisement (default true).
func WithMDNS(v bool) Option {
	return func(m *Manager) { m.enableMDNS = v }
}

// WithVariablesCommand overrides the command ID used to request variables.
func WithVariablesCommand(id int) Option {
	return func(m *Manager) {
		if id > 0 {
			m.varsCommand = id
		}
	}
}

// NewManager creates a Manager; call Start to initialize the node.
func NewManager(pod *controller.PodController, opts ...Option) *Manager {
	m := &Manager{
		pod:           pod,
		name:          "pod3",
		apiPort:       6053,
		pollEvery:     15 * time.Second,
		enableMDNS:    true,
		varsCommand:   14,
		floatSensors:  map[string]*floatSensor{},
		binarySensors: map[string]*binSensor{},
		textSensors:   map[string]*textSensor{},
	}
	for _, o := range opts {
		o(m)
	}
	if envPort := os.Getenv("GOSTHOME_API_PORT"); envPort != "" {
		if v, err := strconv.Atoi(envPort); err == nil && v > 0 && v < 65536 {
			m.apiPort = uint16(v)
		}
	}
	return m
}

// Start initializes the gosthome node (only once) and begins polling.
func (m *Manager) Start(parent context.Context) {
	m.onceStart.Do(func() {
		ctx, cancel := context.WithCancel(parent)
		m.cancel = cancel

		m.initNode(ctx)
		if m.node == nil {
			return
		}
		go m.pollLoop(ctx)
	})
}

// Stop shuts down the manager (idempotent).
func (m *Manager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cancel != nil {
		m.cancel()
		m.cancel = nil
	}
}

// Node returns the underlying gosthome node (may be nil if init failed).
func (m *Manager) Node() *core.Node {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.node
}

// InitError returns any initialization error.
func (m *Manager) InitError() error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.initErr
}

func (m *Manager) initNode(ctx context.Context) {
	// Minimal API config
	apiCfg := api.NewConfig()
	apiCfg.Address = "0.0.0.0"
	apiCfg.Port = m.apiPort

	mac, _ := config.ParseMAC("02:00:00:00:00:01") // demo MAC

	cfg := &config.Config{
		Registry: registry.DefaultRegistry(),
		Gosthome: config.GosthomeConfig{
			Name: m.name,
			MAC:  mac,
		},
		Components: config.Configs{
			"api": component.NewConfigDecoder(apiCfg),
		},
	}

	node, err := core.NewNode(ctx, cfg)
	if err != nil {
		m.initErr = err
		slog.Error("gosthome init failed", "err", err)
		return
	}
	m.node = node

	// Domains needed for dynamic sensors
	for _, d := range []entity.DomainDefinition{
		entity.PublicDomain(&entity.SensorDomain{}),
		entity.PublicDomain(&entity.BinarySensorDomain{}),
		entity.PublicDomain(&entity.TextSensorDomain{}),
	} {
		if e := m.node.Registry.CreateDomain(d); e != nil {
			if _, ok := e.(entity.ErrAlreadyRegistered); !ok {
				slog.Warn("domain create", "err", e)
			}
		}
	}

	// Base context that carries bus
	m.baseCtx = bus.Context(context.Background(), m.node.Bus)

	go node.Start()
	slog.Info("gosthome API started", "name", m.name, "port", m.apiPort)

	if m.enableMDNS {
		go m.runMDNS(ctx, apiCfg)
	}
}

func (m *Manager) runMDNS(ctx context.Context, apiCfg *api.Config) {
	txt := []string{
		"version=1.0",
		"address=" + apiCfg.Address,
	}
	svc, err := zeroconf.Register(m.name, "_esphomelib._tcp", "local.", int(m.apiPort), txt, nil)
	if err != nil {
		slog.Error("mdns register failed", "err", err)
		return
	}
	slog.Info("mdns advertisement started", "service", "_esphomelib._tcp", "port", m.apiPort)
	<-ctx.Done()
	svc.Shutdown()
	slog.Info("mdns advertisement stopped")
}

func (m *Manager) pollLoop(ctx context.Context) {
	t := time.NewTicker(m.pollEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.pollOnce()
		}
	}
}

func (m *Manager) pollOnce() {
	if m.pod == nil {
		return
	}
	_, err := m.pod.Execute(m.varsCommand, "")
	now := time.Now()
	if err != nil {
		// heartbeat sensor still updated
		m.setFloat("pod_heartbeat_epoch", float32(now.Unix()), "s")
		m.updateAvailability()
		return
	}
	m.lastPollSuccess = now
	m.updateFromParsed()
	m.updateAvailability()
}

func (m *Manager) updateFromParsed() {
	pv := m.pod.ParsedVariables()
	if pv == nil {
		return
	}
	// Numeric
	m.setFloat("tg_heat_level_left", float32(pv.TgHeatLevelL), "lvl")
	m.setFloat("tg_heat_level_right", float32(pv.TgHeatLevelR), "lvl")
	m.setFloat("heat_level_left", float32(pv.HeatLevelL), "lvl")
	m.setFloat("heat_level_right", float32(pv.HeatLevelR), "lvl")
	m.setFloat("heat_time_left_seconds", float32(pv.HeatTimeL), "s")
	m.setFloat("heat_time_right_seconds", float32(pv.HeatTimeR), "s")

	// Binary
	m.setBinary("water_level_ok", pv.WaterLevel)
	m.setBinary("priming_active", pv.Priming)

	// Text
	m.setText("sensor_label", pv.SensorLabel)
	m.setText("settings_raw", pv.SettingsRaw)

	// Heartbeat (fast path)
	m.setFloat("pod_heartbeat_epoch", float32(time.Now().Unix()), "s")
}

func (m *Manager) updateAvailability() {
	grace := 5 * time.Second
	ok := false
	if !m.lastPollSuccess.IsZero() {
		ok = time.Since(m.lastPollSuccess) <= 2*m.pollEvery+grace
	}
	m.setBinary("pod_available", ok)
}

// -------- Dynamic entity implementations --------

type floatSensor struct {
	ent  *entity.BaseEntity
	st   state.State_[entity.SensorState]
	unit string
}

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

// -------- Registration helpers --------

func (m *Manager) setFloat(id string, val float32, unit string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.node == nil {
		return
	}
	fs, ok := m.floatSensors[id]
	if !ok {
		fs = m.newFloatSensor(id, unit)
		if err := m.node.Registry.RegisterSensor(fs); err != nil {
			slog.Error("register sensor", "id", id, "err", err)
			return
		}
		m.floatSensors[id] = fs
	}
	cur := fs.State()
	if cur.MissingState || cur.State != val {
		cur.State = val
		cur.MissingState = false
		fs.st.SetState(cur)
	}
}

func (m *Manager) newFloatSensor(id, unit string) *floatSensor {
	cfg := &entity.EntityConfig{
		ID:   id,
		Name: prettyName(id),
	}
	beVal := entity.NewBaseEntity(entity.DomainTypeSensor, cfg)
	be := &beVal
	st, _ := state.NewState(m.baseCtxOrBG(), be, entity.SensorState{State: 0, MissingState: true})
	return &floatSensor{ent: be, st: st, unit: unit}
}

func (m *Manager) setBinary(id string, on bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.node == nil {
		return
	}
	bs, ok := m.binarySensors[id]
	if !ok {
		bs = m.newBinarySensor(id)
		if err := m.node.Registry.RegisterBinarySensor(bs); err != nil {
			slog.Error("register binary sensor", "id", id, "err", err)
			return
		}
		m.binarySensors[id] = bs
	}
	cur := bs.State()
	if cur.Missing || cur.State != on {
		cur.State = on
		cur.Missing = false
		bs.st.SetState(cur)
	}
}

func (m *Manager) newBinarySensor(id string) *binSensor {
	cfg := &entity.EntityConfig{
		ID:   id,
		Name: prettyName(id),
	}
	beVal := entity.NewBaseEntity(entity.DomainTypeBinarySensor, cfg)
	be := &beVal
	st, _ := state.NewState(m.baseCtxOrBG(), be, entity.BinarySensorState{State: false, Missing: true})
	return &binSensor{ent: be, st: st}
}

func (m *Manager) setText(id, val string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.node == nil {
		return
	}
	ts, ok := m.textSensors[id]
	if !ok {
		ts = m.newTextSensor(id)
		if err := m.node.Registry.RegisterTextSensor(ts); err != nil {
			slog.Error("register text sensor", "id", id, "err", err)
			return
		}
		m.textSensors[id] = ts
	}
	cur := ts.State()
	if cur.MissingState || cur.State != val {
		cur.State = val
		cur.MissingState = false
		ts.st.SetState(cur)
	}
}

func (m *Manager) newTextSensor(id string) *textSensor {
	cfg := &entity.EntityConfig{
		ID:   id,
		Name: prettyName(id),
	}
	beVal := entity.NewBaseEntity(entity.DomainTypeTextSensor, cfg)
	be := &beVal
	st, _ := state.NewState(m.baseCtxOrBG(), be, entity.TextSensorState{State: "", MissingState: true})
	return &textSensor{ent: be, st: st}
}

func (m *Manager) baseCtxOrBG() context.Context {
	if m.baseCtx != nil {
		return m.baseCtx
	}
	return context.Background()
}

// prettyName converts an identifier_with_underscores to "Identifier With Underscores".
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

// LocalAddress attempts to retrieve a non-loopback IP for logging (best-effort).
func LocalAddress() string {
	ifaces, err := net.Interfaces()
	if err != nil {
		return ""
	}
	for _, iface := range ifaces {
		if (iface.Flags & net.FlagUp) == 0 {
			continue
		}
		addrs, _ := iface.Addrs()
		for _, a := range addrs {
			if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
				if ip4 := ipnet.IP.To4(); ip4 != nil {
					return ip4.String()
				}
			}
		}
	}
	return ""
}
