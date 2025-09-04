package gost

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"math"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"eightsleep-esphome/internal/controller"

	"github.com/fxamacker/cbor/v2"
	"github.com/grandcat/zeroconf"

	"github.com/gosthome/gosthome/components/api"
	"github.com/gosthome/gosthome/components/button"
	"github.com/gosthome/gosthome/components/climate"
	"github.com/gosthome/gosthome/components/number"
	"github.com/gosthome/gosthome/components/switchcomp"
	"github.com/gosthome/gosthome/core"
	"github.com/gosthome/gosthome/core/bus"
	"github.com/gosthome/gosthome/core/component"
	"github.com/gosthome/gosthome/core/config"
	"github.com/gosthome/gosthome/core/entity"
	"github.com/gosthome/gosthome/core/registry"
	"github.com/gosthome/gosthome/core/state"

	_ "github.com/gosthome/gosthome/components" // register default components
)

// Entity & Sensor ID constants
const (
	EntityIDPodAvailable         = "pod_available"
	EntityIDCloudConnected       = "cloud_connected"
	EntityIDEnableCloud          = "enable_cloud"
	EntityIDHeatLeft             = "heat_left"
	EntityIDHeatRight            = "heat_right"
	EntityIDHeatLevelLeft        = "heat_level_left"
	EntityIDHeatLevelRight       = "heat_level_right"
	EntityIDHeatTimeLeftSeconds  = "heat_time_left_seconds"
	EntityIDHeatTimeRightSeconds = "heat_time_right_seconds"
	EntityIDTargetHeatLevelLeft  = "target_heat_level_left"
	EntityIDTargetHeatLevelRight = "target_heat_level_right"
	EntityIDLedBrightness        = "led_brightness"
	EntityIDWaterLevelOK         = "water_level_ok"
	EntityIDPrimingActive        = "priming_active"
	EntityIDSensorLabel          = "sensor_label"
	EntityIDSettingsRaw          = "settings_raw"
	EntityIDPodHeartbeatEpoch    = "pod_heartbeat_epoch"
	EntityIDPrimeButton          = "prime"
	EntityIDFirmwareResetButton  = "firmware_reset"
	EntityIDPowerResetButton     = "power_reset"
	EntityIDFactoryResetButton   = "factory_reset"
	EntityIDTemperatureOffset    = "climate_temperature_offset"
	EntityIDClimateLeft          = "climate_left"
	EntityIDClimateRight         = "climate_right"
)

// Manager owns the gosthome (ESPHome native API) node and registered entities.
type Manager struct {
	mu sync.RWMutex

	node      *core.Node
	initErr   error
	cancel    context.CancelFunc
	baseCtx   context.Context
	pod       *controller.PodController
	onceStart sync.Once

	// entity abstraction
	stopLeftHeating  func() error
	stopRightHeating func() error

	// configuration
	name        string
	apiPort     uint16
	pollEvery   time.Duration
	enableMDNS  bool
	varsCommand int // which command ID triggers variable refresh (mirrors controller default 14)

	// last success tracking
	lastPollSuccess time.Time

	// entity maps
	floatSensors      map[string]*floatSensor
	binarySensors     map[string]*binSensor
	textSensors       map[string]*textSensor
	numberEntities    map[string]*numberEntity
	switchEntities    map[string]*switchEntity
	climateEntities   map[string]*climateEntity
	buttonEntities    map[string]*buttonEntity
	disabledByDefault map[string]struct{}
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

// WithDisabledSensors marks specified sensor IDs to be disabled by default
// when first registered (Home Assistant will show them as entities that must
// be manually enabled).
func WithDisabledSensors(ids ...string) Option {
	return func(m *Manager) {
		if m.disabledByDefault == nil {
			m.disabledByDefault = map[string]struct{}{}
		}
		for _, id := range ids {
			if id == "" {
				continue
			}
			m.disabledByDefault[id] = struct{}{}
		}
	}
}

// NewManager creates a Manager; call Start to initialize the node.
func NewManager(pod *controller.PodController, opts ...Option) *Manager {
	m := &Manager{
		pod:               pod,
		name:              "pod3",
		apiPort:           6053,
		pollEvery:         15 * time.Second,
		enableMDNS:        true,
		varsCommand:       14,
		floatSensors:      map[string]*floatSensor{},
		binarySensors:     map[string]*binSensor{},
		textSensors:       map[string]*textSensor{},
		numberEntities:    map[string]*numberEntity{},
		switchEntities:    map[string]*switchEntity{},
		climateEntities:   map[string]*climateEntity{},
		buttonEntities:    map[string]*buttonEntity{},
		disabledByDefault: map[string]struct{}{},
	}
	for _, o := range opts {
		o(m)
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

	// Domains needed for sensor domains
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

	var numberDomain entity.NumberDomain
	if err := m.node.Registry.CreateDomain(entity.PublicDomain(&numberDomain)); err != nil {
		if _, ok := err.(entity.ErrAlreadyRegistered); !ok {
			slog.Warn("domain create", "err", err)
		}
	}
	number.RegisterServiceCallHandlers(ctx, &numberDomain, node.Bus)

	var switchDomain entity.SwitchDomain
	if err := m.node.Registry.CreateDomain(entity.PublicDomain(&switchDomain)); err != nil {
		if _, ok := err.(entity.ErrAlreadyRegistered); !ok {
			slog.Warn("domain create", "err", err)
		}
	}
	switchcomp.RegisterServiceCallHandlers(ctx, &switchDomain, node.Bus)

	var climateDomain entity.ClimateDomain
	if err := m.node.Registry.CreateDomain(entity.PublicDomain(&climateDomain)); err != nil {
		if _, ok := err.(entity.ErrAlreadyRegistered); !ok {
			slog.Warn("domain create", "err", err)
		}
	}
	climate.RegisterServiceCallHandlers(ctx, &climateDomain, node.Bus)

	var buttonDomain entity.ButtonDomain
	if err := m.node.Registry.CreateDomain(entity.PublicDomain(&buttonDomain)); err != nil {
		if _, ok := err.(entity.ErrAlreadyRegistered); !ok {
			slog.Warn("domain create", "err", err)
		}
	}
	button.RegisterServiceCallHandlers(ctx, &buttonDomain, node.Bus)

	// Base context that carries bus
	m.baseCtx = bus.Context(context.Background(), m.node.Bus)

	go node.Start()
	slog.Info("gosthome API started", "name", m.name, "port", m.apiPort)
	// Pre-register known sensors (some disabled by default)
	m.preRegisterEntities()

	if m.enableMDNS {
		go m.runMDNS(ctx, apiCfg)
	}

	m.setBinary(EntityIDPodAvailable, false)
	m.pod.SetOnMitmRequest(func(command controller.FrankenCommand, payload string) {
		switch command {
		case controller.FrankenCommandSetSettings:
			if payload != "" {
				if raw, err := hex.DecodeString(payload); err == nil {
					var decoded map[string]any
					if err := cbor.Unmarshal(raw, &decoded); err == nil {
						if v, ok := decoded["lb"]; ok {
							if lb, ok := v.(uint64); ok {
								m.setNumber(EntityIDLedBrightness, float32(lb))
							}
						}
					}
				}
			}

		case controller.FrankenCommandHeatLeft:
			if n, err := strconv.Atoi(payload); err == nil {
				m.setNumber(EntityIDHeatTimeLeftSeconds, float32(n))
			}

		case controller.FrankenCommandHeatRight:
			if n, err := strconv.Atoi(payload); err == nil {
				m.setNumber(EntityIDHeatTimeRightSeconds, float32(n))
			}

		case controller.FrankenCommandLevelLeft:
			if n, err := strconv.Atoi(payload); err == nil {
				m.setNumber(EntityIDTargetHeatLevelLeft, float32(n))
			}

		case controller.FrankenCommandLevelRight:
			if n, err := strconv.Atoi(payload); err == nil {
				m.setNumber(EntityIDTargetHeatLevelRight, float32(n))
			}
		}
	})
	m.pod.SetOnMitmConnected(func(connected bool) {
		m.setBinary(EntityIDCloudConnected, connected)
	})
	m.setSwitch(EntityIDEnableCloud, m.pod.GetMitmMode())
	m.setNumber(EntityIDTemperatureOffset, 25)
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
	t := time.NewTimer(m.pollEvery)
	defer t.Stop()
	for {
		if m.pod.ConnAlive() {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		} else {
			t.Stop()

			select {
			case <-ctx.Done():
				return
			case <-m.pod.WaitForConn():
			}
		}

		m.pollOnce()

		t.Reset(m.pollEvery)
	}
}

func (m *Manager) pollOnce() {
	if m.pod == nil {
		return
	}
	_, err := m.pod.ExecuteRaw(m.varsCommand, "")
	now := time.Now()
	if err != nil {
		// heartbeat sensor still updated
		m.setFloat(EntityIDPodHeartbeatEpoch, float32(now.Unix()))
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
	m.setNumber(EntityIDLedBrightness, float32(pv.LedBrightness))
	m.setNumber(EntityIDTargetHeatLevelLeft, float32(pv.TargetHeatLevelL))
	m.setNumber(EntityIDTargetHeatLevelRight, float32(pv.TargetHeatLevelR))
	m.setFloat(EntityIDHeatLevelLeft, float32(pv.HeatLevelL))
	m.setFloat(EntityIDHeatLevelRight, float32(pv.HeatLevelR))
	m.setFloat(EntityIDHeatTimeLeftSeconds, float32(pv.HeatTimeL))
	m.setFloat(EntityIDHeatTimeRightSeconds, float32(pv.HeatTimeR))
	m.setSwitch(EntityIDHeatLeft, pv.HeatTimeL > 0)
	m.setSwitch(EntityIDHeatRight, pv.HeatTimeR > 0)

	// Binary
	m.setBinary(EntityIDWaterLevelOK, pv.WaterLevel)
	m.setBinary(EntityIDPrimingActive, pv.Priming)

	// Text
	m.setText(EntityIDSensorLabel, pv.SensorLabel)
	m.setText(EntityIDSettingsRaw, pv.SettingsRaw)

	// Heartbeat (fast path)
	m.setFloat(EntityIDPodHeartbeatEpoch, float32(time.Now().Unix()))

	offset := m.numberEntities[EntityIDTemperatureOffset].State().State
	m.updateClimate(EntityIDClimateLeft, func(cs *entity.ClimateState) {
		cs.TargetTemperature = float32(pv.TargetHeatLevelL)/10 + m.numberEntities[EntityIDTemperatureOffset].State().State
		if pv.HeatTimeL > 0 {
			cs.Mode = entity.ClimateModeHeatCool
		} else {
			cs.Mode = entity.ClimateModeOff
		}
		cs.Action = climateAction(
			pv.HeatTimeL > 0,
			float32(pv.HeatLevelL),
			float32(pv.TargetHeatLevelL),
		)
		cs.CurrentTemperature = offset + float32(pv.HeatLevelL)/10
	})

	m.updateClimate(EntityIDClimateRight, func(cs *entity.ClimateState) {
		cs.TargetTemperature = float32(pv.TargetHeatLevelR) / 10
		if pv.HeatTimeR > 0 {
			cs.Mode = entity.ClimateModeHeatCool
		} else {
			cs.Mode = entity.ClimateModeOff
		}
		cs.Action = climateAction(
			pv.HeatTimeR > 0,
			float32(pv.HeatLevelR),
			float32(pv.TargetHeatLevelR),
		)
		cs.CurrentTemperature = offset + float32(pv.HeatLevelR)/10
	})
}

func (m *Manager) updateAvailability() {
	grace := 5 * time.Second
	ok := false
	if !m.lastPollSuccess.IsZero() {
		ok = time.Since(m.lastPollSuccess) <= 2*m.pollEvery+grace
	}
	m.setBinary(EntityIDPodAvailable, ok)
}

const heatLoopSeconds = 21600

func (m *Manager) startHeatLoop(ctx context.Context, side controller.Side) (func() error, error) {
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Hour):
			}

			if err := m.pod.ExecuteHeatDuration(side, heatLoopSeconds); err != nil {
				slog.Error("failed to execute heat duration: %v", "err", err)
			}

			m.setFloat(fmt.Sprintf("heat_time_%s_seconds", side), heatLoopSeconds)
		}
	}()

	// do first one synchronously so we can check for errors
	return func() error {
		cancel()

		return m.pod.ExecuteHeatDuration(side, 0)
	}, m.pod.ExecuteHeatDuration(side, heatLoopSeconds)
}

// -------- Entity implementations --------

type floatSensor struct {
	ent        *entity.BaseEntity
	st         state.State_[entity.SensorState]
	unit       string
	stateclass entity.SensorStateClass
	devclass   entity.SensorDeviceClass
	decimals   int32
}

func (f *floatSensor) Setup()       {}
func (f *floatSensor) Close() error { return nil }
func (f *floatSensor) InitializationPriority() component.InitializationPriority {
	return component.InitializationPriorityBus
}
func (f *floatSensor) AccuracyDecimals() int32             { return f.decimals }
func (f *floatSensor) ForceUpdate() bool                   { return false }
func (f *floatSensor) StateClass() entity.SensorStateClass { return f.stateclass }
func (f *floatSensor) LastResetType() entity.SensorLastResetType {
	return entity.SensorLastResetTypeNone
}
func (f *floatSensor) UnitOfMeasurement() string             { return f.unit }
func (f *floatSensor) DeviceClass() entity.SensorDeviceClass { return f.devclass }
func (f *floatSensor) Icon() string                          { return "" }
func (f *floatSensor) State() entity.SensorState             { return f.st.State() }
func (f *floatSensor) ID() string                            { return f.ent.ID() }
func (f *floatSensor) HashID() uint32                        { return f.ent.HashID() }
func (f *floatSensor) Name() string                          { return f.ent.Name() }
func (f *floatSensor) Internal() bool                        { return f.ent.Internal() }
func (f *floatSensor) DisabledByDefault() bool               { return f.ent.DisabledByDefault() }
func (f *floatSensor) EntityCategory() entity.Category       { return f.ent.EntityCategory() }

type binSensor struct {
	ent   *entity.BaseEntity
	st    state.State_[entity.BinarySensorState]
	class entity.BinarySensorDeviceClass
}

func (b *binSensor) Setup()       {}
func (b *binSensor) Close() error { return nil }
func (b *binSensor) InitializationPriority() component.InitializationPriority {
	return component.InitializationPriorityBus
}
func (b *binSensor) IsStatusBinarySensor() bool                  { return false }
func (b *binSensor) DeviceClass() entity.BinarySensorDeviceClass { return b.class }
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

// -------- Number, switch & climate entity implementations --------

// Number entity
type numberEntity struct {
	ent      *entity.BaseEntity
	st       state.State_[entity.NumberState]
	unit     string
	mode     entity.NumberMode
	class    entity.NumberDeviceClass
	icon     string
	minValue float32
	maxValue float32
	step     float32

	// onSet is an optional callback invoked when SetValue is called via a service
	// request. If it returns an error the internal state is not updated.
	onSet func(ctx context.Context, newValue float32, current entity.NumberState) error
}

// OnSetValue registers a callback for SetValue service calls.
func (n *numberEntity) OnSetValue(cb func(ctx context.Context, newValue float32, current entity.NumberState) error) {
	n.onSet = cb
}

func (n *numberEntity) Setup()       {}
func (n *numberEntity) Close() error { return nil }
func (n *numberEntity) InitializationPriority() component.InitializationPriority {
	return component.InitializationPriorityBus
}
func (n *numberEntity) UnitOfMeasurement() string             { return n.unit }
func (n *numberEntity) DeviceClass() entity.NumberDeviceClass { return n.class }
func (n *numberEntity) Icon() string                          { return n.icon }
func (n *numberEntity) State() entity.NumberState             { return n.st.State() }
func (n *numberEntity) SetValue(ctx context.Context, v float32) error {
	cur := n.st.State()
	if n.onSet != nil {
		if err := n.onSet(ctx, v, cur); err != nil {
			return err
		}
	}
	cur.State = v
	cur.MissingState = false
	n.st.SetState(cur)
	return nil
}
func (n *numberEntity) NumberMode() entity.NumberMode   { return n.mode }
func (n *numberEntity) ID() string                      { return n.ent.ID() }
func (n *numberEntity) HashID() uint32                  { return n.ent.HashID() }
func (n *numberEntity) Name() string                    { return n.ent.Name() }
func (n *numberEntity) Internal() bool                  { return n.ent.Internal() }
func (n *numberEntity) DisabledByDefault() bool         { return n.ent.DisabledByDefault() }
func (n *numberEntity) EntityCategory() entity.Category { return n.ent.EntityCategory() }
func (n *numberEntity) MinValue() float32               { return n.minValue }
func (n *numberEntity) MaxValue() float32               { return n.maxValue }
func (n *numberEntity) Step() float32                   { return n.step }

// Switch entity
type switchEntity struct {
	ent   *entity.BaseEntity
	st    state.State_[entity.SwitchState]
	class entity.SwitchDeviceClass

	// onSet is an optional callback invoked when SetState is called via a service
	// request. If it returns an error the internal state is not updated.
	onSet func(ctx context.Context, newState bool, current entity.SwitchState) error
}

// OnSetState registers a callback for switch SetState service calls.
func (s *switchEntity) OnSetState(cb func(ctx context.Context, newState bool, current entity.SwitchState) error) {
	s.onSet = cb
}

func (s *switchEntity) Setup()       {}
func (s *switchEntity) Close() error { return nil }
func (s *switchEntity) InitializationPriority() component.InitializationPriority {
	return component.InitializationPriorityBus
}
func (s *switchEntity) DeviceClass() entity.SwitchDeviceClass { return s.class }
func (s *switchEntity) Icon() string                          { return "" }
func (s *switchEntity) State() entity.SwitchState             { return s.st.State() }
func (s *switchEntity) SetState(ctx context.Context, state bool) error {
	cur := s.st.State()
	if s.onSet != nil {
		if err := s.onSet(ctx, state, cur); err != nil {
			return err
		}
	}
	if cur.State != state {
		cur.State = state
		s.st.SetState(cur)
	}
	return nil
}
func (s *switchEntity) ID() string                      { return s.ent.ID() }
func (s *switchEntity) HashID() uint32                  { return s.ent.HashID() }
func (s *switchEntity) Name() string                    { return s.ent.Name() }
func (s *switchEntity) Internal() bool                  { return s.ent.Internal() }
func (s *switchEntity) DisabledByDefault() bool         { return s.ent.DisabledByDefault() }
func (s *switchEntity) EntityCategory() entity.Category { return s.ent.EntityCategory() }

// Climate entity (mutable implementation with callback)
type climateEntity struct {
	ent *entity.BaseEntity
	st  state.State_[entity.ClimateState]

	minTemp float32
	maxTemp float32

	// onSet is an optional callback invoked when SetState is called via a service
	// request. It can mutate the desired state before it is stored by returning
	// a modified copy. If it returns an error the internal state is not updated.
	onSet func(ctx context.Context, desired entity.ClimateState, current entity.ClimateState) (entity.ClimateState, error)
}

// OnSetState registers a callback for climate SetState service calls.
func (c *climateEntity) OnSetState(cb func(ctx context.Context, desired entity.ClimateState, current entity.ClimateState) (entity.ClimateState, error)) {
	c.onSet = cb
}

func (c *climateEntity) Setup()       {}
func (c *climateEntity) Close() error { return nil }
func (c *climateEntity) InitializationPriority() component.InitializationPriority {
	return component.InitializationPriorityBus
}
func (c *climateEntity) Icon() string               { return "" }
func (c *climateEntity) State() entity.ClimateState { return c.st.State() }
func (c *climateEntity) SetState(ctx context.Context, st entity.ClimateState) error {
	cur := c.st.State()
	if c.onSet != nil {
		mod, err := c.onSet(ctx, st, cur)
		if err != nil {
			return err
		}
		st = mod
	}
	c.st.SetState(st)
	return nil
}
func (c *climateEntity) SupportsCurrentTemperature() bool { return true }
func (c *climateEntity) SupportedModes() []entity.ClimateMode {
	return []entity.ClimateMode{entity.ClimateModeOff, entity.ClimateModeHeatCool}
}
func (c *climateEntity) VisualMinTemperature() float32         { return c.minTemp }
func (c *climateEntity) VisualMaxTemperature() float32         { return c.maxTemp }
func (c *climateEntity) VisualTargetTemperatureStep() float32  { return 0.1 }
func (c *climateEntity) VisualCurrentTemperatureStep() float32 { return 0.1 }
func (c *climateEntity) SupportsAction() bool                  { return true }
func (c *climateEntity) ID() string                            { return c.ent.ID() }
func (c *climateEntity) HashID() uint32                        { return c.ent.HashID() }
func (c *climateEntity) Name() string                          { return c.ent.Name() }
func (c *climateEntity) Internal() bool                        { return c.ent.Internal() }
func (c *climateEntity) DisabledByDefault() bool               { return c.ent.DisabledByDefault() }
func (c *climateEntity) EntityCategory() entity.Category       { return c.ent.EntityCategory() }

// Button entity (press callback)
type buttonEntity struct {
	ent     *entity.BaseEntity
	onPress func() error
}

// OnPress registers a callback for button press service calls.
func (b *buttonEntity) OnPress(cb func() error) { b.onPress = cb }

func (b *buttonEntity) Setup()       {}
func (b *buttonEntity) Close() error { return nil }
func (b *buttonEntity) InitializationPriority() component.InitializationPriority {
	return component.InitializationPriorityBus
}
func (b *buttonEntity) Icon() string                          { return "" }
func (b *buttonEntity) DeviceClass() entity.ButtonDeviceClass { return "" }
func (b *buttonEntity) Press(ctx context.Context) error {
	if b.onPress != nil {
		return b.onPress()
	}
	return nil
}
func (b *buttonEntity) ID() string                      { return b.ent.ID() }
func (b *buttonEntity) HashID() uint32                  { return b.ent.HashID() }
func (b *buttonEntity) Name() string                    { return b.ent.Name() }
func (b *buttonEntity) Internal() bool                  { return b.ent.Internal() }
func (b *buttonEntity) DisabledByDefault() bool         { return b.ent.DisabledByDefault() }
func (b *buttonEntity) EntityCategory() entity.Category { return b.ent.EntityCategory() }

// -------- Registration helpers --------

func (m *Manager) setFloat(id string, val float32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.node == nil {
		return
	}
	fs, ok := m.floatSensors[id]
	if !ok {
		slog.Warn("float sensor not found", "id", id)
		return
	}
	cur := fs.State()
	if cur.MissingState || cur.State != val {
		cur.State = val
		cur.MissingState = false
		fs.st.SetState(cur)
	}
}

func (m *Manager) registerFloatSensor(id string, disabled bool, f *floatSensor) {
	be := entity.NewBaseEntity(entity.DomainTypeSensor, &entity.EntityConfig{
		ID:                id,
		Name:              prettyName(id),
		DisabledByDefault: disabled,
	})
	st, _ := state.NewState(m.baseCtxOrBG(), &be, entity.SensorState{State: 0, MissingState: true})
	f.ent = &be
	f.st = st
	if err := m.node.Registry.RegisterSensor(f); err != nil {
		slog.Error("register sensor", "id", id, "err", err)
		return
	}
	m.floatSensors[id] = f
}

func (m *Manager) setBinary(id string, on bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.node == nil {
		return
	}
	bs, ok := m.binarySensors[id]
	if !ok {
		slog.Warn("binary sensor not found", "id", id)
		return
	}
	cur := bs.State()
	if cur.Missing || cur.State != on {
		cur.State = on
		cur.Missing = false
		bs.st.SetState(cur)
	}
}

func (m *Manager) registerBinarySensor(id string, disabled bool, b *binSensor) {
	be := entity.NewBaseEntity(entity.DomainTypeBinarySensor, &entity.EntityConfig{
		ID:                id,
		Name:              prettyName(id),
		DisabledByDefault: disabled,
	})
	st, _ := state.NewState(m.baseCtxOrBG(), &be, entity.BinarySensorState{State: false, Missing: true})
	b.ent = &be
	b.st = st
	if err := m.node.Registry.RegisterBinarySensor(b); err != nil {
		slog.Error("register binary sensor", "id", id, "err", err)
		return
	}
	m.binarySensors[id] = b
}

func (m *Manager) setText(id, val string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.node == nil {
		return
	}
	ts, ok := m.textSensors[id]
	if !ok {
		slog.Warn("text sensor not found", "id", id)
		return
	}
	cur := ts.State()
	if cur.MissingState || cur.State != val {
		cur.State = val
		cur.MissingState = false
		ts.st.SetState(cur)
	}
}

func (m *Manager) registerTextSensor(id string, disabled bool, t *textSensor) {
	be := entity.NewBaseEntity(entity.DomainTypeTextSensor, &entity.EntityConfig{
		ID:                id,
		Name:              prettyName(id),
		DisabledByDefault: disabled,
	})
	st, _ := state.NewState(m.baseCtxOrBG(), &be, entity.TextSensorState{State: "", MissingState: true})
	t.ent = &be
	t.st = st
	if err := m.node.Registry.RegisterTextSensor(t); err != nil {
		slog.Error("register text sensor", "id", id, "err", err)
		return
	}
	m.textSensors[id] = t
}

func (m *Manager) setNumber(id string, val float32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.node == nil {
		return
	}
	num, ok := m.numberEntities[id]
	if !ok {
		slog.Warn("number entity not found", "id", id)
		return
	}
	cur := num.State()
	cur.MissingState = false
	cur.State = val
	num.st.SetState(cur)
}

func (m *Manager) registerNumberEntity(id string, disabled bool, num *numberEntity) {
	beVal := entity.NewBaseEntity(entity.DomainTypeNumber, &entity.EntityConfig{
		ID:                id,
		Name:              prettyName(id),
		DisabledByDefault: disabled,
	})
	st, _ := state.NewState(m.baseCtxOrBG(), &beVal, entity.NumberState{State: 0, MissingState: true})
	st.SetState(entity.NumberState{MissingState: true})
	num.ent = &beVal
	num.st = st
	if err := m.node.Registry.RegisterNumber(num); err != nil {
		slog.Error("register number", "id", id, "err", err)
		return
	}
	m.numberEntities[id] = num
}

// registerSwitchEntity registers a switch entity similar to registerNumberEntity.
func (m *Manager) registerSwitchEntity(id string, disabled bool, sw *switchEntity) {
	beVal := entity.NewBaseEntity(entity.DomainTypeSwitch, &entity.EntityConfig{
		ID:                id,
		Name:              prettyName(id),
		DisabledByDefault: disabled,
	})
	st, _ := state.NewState(m.baseCtxOrBG(), &beVal, entity.SwitchState{State: false})
	sw.ent = &beVal
	sw.st = st
	if err := m.node.Registry.RegisterSwitch(sw); err != nil {
		slog.Error("register switch", "id", id, "err", err)
		return
	}
	m.switchEntities[id] = sw
}

// registerClimateEntity registers a climate entity similar to registerNumberEntity.
func (m *Manager) registerClimateEntity(id string, disabled bool, cl *climateEntity) {
	be := entity.NewBaseEntity(entity.DomainTypeClimate, &entity.EntityConfig{
		ID:                id,
		Name:              prettyName(id),
		DisabledByDefault: disabled,
	})
	st, _ := state.NewState(m.baseCtxOrBG(), &be, entity.ClimateState{})
	cl.ent = &be
	cl.st = st
	if err := m.node.Registry.RegisterClimate(cl); err != nil {
		slog.Error("register climate", "id", id, "err", err)
		return
	}
	m.climateEntities[id] = cl
}

func (m *Manager) registerButtonEntity(id string, disabled bool, b *buttonEntity) {
	be := entity.NewBaseEntity(entity.DomainTypeButton, &entity.EntityConfig{
		ID:                id,
		Name:              prettyName(id),
		DisabledByDefault: disabled,
	})
	b.ent = &be
	if err := m.node.Registry.RegisterButton(b); err != nil {
		slog.Error("register button", "id", id, "err", err)
		return
	}
	m.buttonEntities[id] = b
}

func (m *Manager) setSwitch(id string, on bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.node == nil {
		return
	}
	sw, ok := m.switchEntities[id]
	if !ok {
		slog.Warn("switch not found", "id", id)
		return
	}
	cur := sw.State()
	cur.State = on
	sw.st.SetState(cur)
}

func (m *Manager) updateClimate(id string, mutate func(*entity.ClimateState)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.node == nil {
		return
	}
	cl, ok := m.climateEntities[id]
	if !ok {
		slog.Warn("climate not found", "id", id)
		return
	}
	cur := cl.State()
	mutate(&cur)
	cl.st.SetState(cur)
}

func (m *Manager) baseCtxOrBG() context.Context {
	if m.baseCtx != nil {
		return m.baseCtx
	}
	return context.Background()
}

func (m *Manager) doHeatSwitch(ctx context.Context, side controller.Side, state bool) error {
	var stopFuncPtr *func() error
	if side == controller.SideLeft {
		stopFuncPtr = &m.stopLeftHeating
	} else {
		stopFuncPtr = &m.stopRightHeating
	}

	if state {
		stopFn, err := m.startHeatLoop(ctx, side)
		if err != nil {
			return err
		}

		*stopFuncPtr = stopFn
	} else if *stopFuncPtr != nil {
		if err := (*stopFuncPtr)(); err != nil {
			return err
		}

		*stopFuncPtr = nil
	}

	return nil
}

func (m *Manager) heatSwitchHandler(side controller.Side) func(ctx context.Context, newState bool, current entity.SwitchState) error {
	return func(ctx context.Context, newState bool, current entity.SwitchState) error {
		if current.State == newState {
			return nil
		}

		if err := m.doHeatSwitch(ctx, side, newState); err != nil {
			return err
		}

		m.updateClimate(fmt.Sprintf("climate_%s", side), func(cs *entity.ClimateState) {
			if newState {
				cs.Mode = entity.ClimateModeHeatCool
			} else {
				cs.Mode = entity.ClimateModeOff
			}
			cs.Action = climateAction(
				newState,
				m.floatSensors[fmt.Sprintf("heat_level_%s", side)].State().State,
				m.numberEntities[fmt.Sprintf("target_heat_level_%s", side)].State().State,
			)
		})

		return nil
	}
}

func (m *Manager) heatLevelHandler(side controller.Side) func(ctx context.Context, newState float32, current entity.NumberState) error {
	return func(ctx context.Context, newState float32, current entity.NumberState) error {
		if current.State == newState {
			return nil
		}

		newState = max(-100, min(100, float32(math.Floor(float64(newState)))))

		m.updateClimate(fmt.Sprintf("climate_%s", side), func(cs *entity.ClimateState) {
			cs.TargetTemperature = m.numberEntities[EntityIDTemperatureOffset].State().State + newState/10
			cs.Action = climateAction(
				m.switchEntities[fmt.Sprintf("heat_%s", side)].State().State,
				m.floatSensors[fmt.Sprintf("heat_level_%s", side)].State().State,
				newState,
			)
		})

		return m.pod.ExecuteHeatLevel(side, int(newState))
	}
}

func climateAction(on bool, currentLevel, targetLevel float32) entity.ClimateAction {
	if on {
		if currentLevel == targetLevel {
			return entity.ClimateActionIdle
		}

		if currentLevel > targetLevel {
			return entity.ClimateActionCooling
		}

		return entity.ClimateActionHeating
	}

	return entity.ClimateActionOff
}

func (m *Manager) climateHandler(side controller.Side) func(ctx context.Context, desired entity.ClimateState, current entity.ClimateState) (entity.ClimateState, error) {
	return func(ctx context.Context, desired entity.ClimateState, current entity.ClimateState) (entity.ClimateState, error) {
		if desired.TargetTemperature != current.TargetTemperature {
			newLevel := max(-100, min(100, (desired.TargetTemperature-m.numberEntities[EntityIDTemperatureOffset].State().State)*10))
			if err := m.pod.ExecuteHeatLevel(side, int(newLevel)); err != nil {
				return entity.ClimateState{}, err
			}

			m.setNumber(fmt.Sprintf("target_heat_level_%s", side), newLevel)
			current.TargetTemperature = desired.TargetTemperature
		}

		if desired.Mode != current.Mode {
			var badMode bool
			switch desired.Mode {
			case entity.ClimateModeOff, entity.ClimateModeHeatCool:
				current.Mode = desired.Mode
			default:
				badMode = true
			}

			if !badMode {
				state := current.Mode == entity.ClimateModeHeatCool
				if err := m.doHeatSwitch(ctx, side, state); err != nil {
					return entity.ClimateState{}, err
				}

				m.setSwitch(fmt.Sprintf("heat_%s", side), state)
				current.Action = climateAction(
					state,
					m.floatSensors[fmt.Sprintf("heat_level_%s", side)].State().State,
					m.numberEntities[fmt.Sprintf("target_heat_level_%s", side)].State().State,
				)
			}
		}

		return current, nil
	}
}

// Pre-register core sensors so they appear immediately in HA.
// Disabled-by-default: sensor_label, settings_raw, pod_heartbeat_epoch.
func (m *Manager) preRegisterEntities() {
	if m.disabledByDefault == nil {
		m.disabledByDefault = map[string]struct{}{}
	}
	// Mark disabled sensors
	for _, id := range []string{EntityIDSensorLabel, EntityIDSettingsRaw, EntityIDPodHeartbeatEpoch} {
		m.disabledByDefault[id] = struct{}{}
	}
	// Float sensors
	m.registerFloatSensor(EntityIDHeatLevelLeft, false, &floatSensor{unit: "lvl", stateclass: entity.SensorStateClassMeasurement})
	m.registerFloatSensor(EntityIDHeatLevelRight, false, &floatSensor{unit: "lvl", stateclass: entity.SensorStateClassMeasurement})
	m.registerFloatSensor(EntityIDHeatTimeLeftSeconds, false, &floatSensor{unit: "s", stateclass: entity.SensorStateClassMeasurement, devclass: entity.SensorDeviceClassDuration})
	m.registerFloatSensor(EntityIDHeatTimeRightSeconds, false, &floatSensor{unit: "s", stateclass: entity.SensorStateClassMeasurement, devclass: entity.SensorDeviceClassDuration})
	m.registerFloatSensor(EntityIDPodHeartbeatEpoch, true, &floatSensor{unit: "s", stateclass: entity.SensorStateClassTotalIncreasing, devclass: entity.SensorDeviceClassTimestamp})
	// Binary sensors
	m.registerBinarySensor(EntityIDWaterLevelOK, false, &binSensor{})
	m.registerBinarySensor(EntityIDPrimingActive, false, &binSensor{class: entity.BinarySensorDeviceClassRunning})
	m.registerBinarySensor(EntityIDPodAvailable, false, &binSensor{})
	m.registerBinarySensor(EntityIDCloudConnected, false, &binSensor{class: entity.BinarySensorDeviceClassRunning})
	// Text sensors
	m.registerTextSensor(EntityIDSensorLabel, true, &textSensor{})
	m.registerTextSensor(EntityIDSettingsRaw, true, &textSensor{})
	// Switches
	m.registerSwitchEntity(EntityIDHeatLeft, false, &switchEntity{onSet: m.heatSwitchHandler(controller.SideLeft)})
	m.registerSwitchEntity(EntityIDHeatRight, false, &switchEntity{onSet: m.heatSwitchHandler(controller.SideRight)})
	m.registerSwitchEntity(EntityIDEnableCloud, false, &switchEntity{onSet: func(ctx context.Context, newState bool, current entity.SwitchState) error {
		m.pod.SetMitmMode(newState)
		return nil
	}})
	// Numbers
	m.registerNumberEntity(EntityIDTargetHeatLevelLeft, false, &numberEntity{
		mode:     entity.NumberModeSlider,
		unit:     "lvl",
		minValue: -100,
		maxValue: 100,
		step:     1,
		onSet:    m.heatLevelHandler(controller.SideLeft),
	})
	m.registerNumberEntity(EntityIDTargetHeatLevelRight, false, &numberEntity{
		mode:     entity.NumberModeSlider,
		unit:     "lvl",
		minValue: -100,
		maxValue: 100,
		step:     1,
		onSet:    m.heatLevelHandler(controller.SideRight),
	})
	m.registerNumberEntity(EntityIDLedBrightness, false, &numberEntity{
		mode:     entity.NumberModeSlider,
		unit:     "%",
		minValue: 0,
		maxValue: 100,
		step:     1,
		onSet: func(ctx context.Context, newValue float32, current entity.NumberState) error {
			_, err := m.pod.ExecuteSettings(controller.SettingsInput{LB: max(0, min(100, int(newValue)))})
			return err
		},
	})
	m.registerNumberEntity(EntityIDTemperatureOffset, true, &numberEntity{
		mode:     entity.NumberModeBox,
		unit:     "ºC",
		minValue: 25,
		maxValue: 25,
		step:     1,
		onSet: func(ctx context.Context, newValue float32, current entity.NumberState) error {
			// m.updateClimate(EntityIDClimateLeft, func(cs *entity.ClimateState) {
			// 	cs.
			// })
			return nil
		},
	})
	// Buttons
	m.registerButtonEntity(EntityIDPrimeButton, false, &buttonEntity{onPress: func() error {
		_, err := m.pod.ExecuteFranken(controller.FrankenCommandPrime, "")
		return err
	}})
	m.registerButtonEntity(EntityIDFirmwareResetButton, true, &buttonEntity{onPress: func() error {
		_, err := m.pod.ExecuteFranken(controller.FrankenCommandReset, "")
		return err
	}})
	m.registerButtonEntity(EntityIDPowerResetButton, true, &buttonEntity{onPress: func() error {
		_, err := m.pod.ExecuteFranken(controller.FrankenCommandForceReset, "")
		return err
	}})
	m.registerButtonEntity(EntityIDFactoryResetButton, true, &buttonEntity{onPress: func() error {
		_, err := m.pod.ExecuteFranken(controller.FrankenCommandFormat, "")
		return err
	}})
	// Climate
	m.registerClimateEntity(EntityIDClimateLeft, false, &climateEntity{
		minTemp: 15,
		maxTemp: 35,
		onSet:   m.climateHandler(controller.SideLeft),
	})
	m.registerClimateEntity(EntityIDClimateRight, false, &climateEntity{
		minTemp: 15,
		maxTemp: 35,
		onSet:   m.climateHandler(controller.SideRight),
	})
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
