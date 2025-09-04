module github.com/ssttevee/ninesleep-go

go 1.24

toolchain go1.24.3

require (
	github.com/fxamacker/cbor/v2 v2.6.0
	github.com/gosthome/gosthome v0.1.0
	github.com/grandcat/zeroconf v1.0.0
)

require github.com/x448/float16 v0.8.4 // indirect

// Ensure we use the same fork of go-yaml that gosthome expects (it defines
// additional interfaces like yaml.NodeUnmarshalerContext used in their code).
replace github.com/goccy/go-yaml => github.com/gosthome/go-yaml v0.0.0-20250218092000-3492985ee5ed

replace github.com/gosthome/gosthome => github.com/ssttevee/gosthome v0.0.0-20250904155447-dae257fe2874

require (
	github.com/cenkalti/backoff v2.2.1+incompatible // indirect
	github.com/coder/websocket v1.8.12 // indirect
	github.com/creack/goselect v0.1.2 // indirect
	github.com/ebitengine/purego v0.8.2 // indirect
	github.com/flynn/noise v1.1.0 // indirect
	github.com/fsnotify/fsnotify v1.8.0 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/go-ozzo/ozzo-validation/v4 v4.3.0 // indirect
	github.com/goccy/go-yaml v1.15.22 // indirect
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/majfault/signal v1.0.0 // indirect
	github.com/miekg/dns v1.1.27 // indirect
	github.com/mozillazg/go-unidecode v0.2.0 // indirect
	github.com/oklog/ulid/v2 v2.1.0 // indirect
	github.com/power-devops/perfstat v0.0.0-20210106213030-5aafc221ea8c // indirect
	github.com/shirou/gopsutil/v4 v4.25.1 // indirect
	github.com/tklauser/go-sysconf v0.3.12 // indirect
	github.com/tklauser/numcpus v0.6.1 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	go.bug.st/serial v1.6.2 // indirect
	golang.org/x/crypto v0.31.0 // indirect
	golang.org/x/net v0.21.0 // indirect
	golang.org/x/sys v0.28.0 // indirect
	google.golang.org/protobuf v1.36.5 // indirect
)
