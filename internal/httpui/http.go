package httpui

import (
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"strings"
	"time"

	"eightsleep-esphome/internal/controller"
)

type Server struct {
	pod  *controller.PodController
	mux  *http.ServeMux
	tmpl *template.Template
}

func NewServer(pod *controller.PodController) *Server {
	s := &Server{
		pod: pod,
		mux: http.NewServeMux(),
		tmpl: template.Must(template.New("ui").Funcs(template.FuncMap{
			"trim": strings.TrimSpace,
		}).Parse(pageTemplate)),
	}
	s.routes()
	return s
}

func (s *Server) Handler() http.Handler { return s.mux }

func (s *Server) routes() {
	s.mux.HandleFunc("/", s.handleUI)
	s.mux.HandleFunc("/action", s.handleAction)
	s.mux.HandleFunc("/logs.json", s.handleLogsJSON)
	s.mux.HandleFunc("/variables.json", s.handleVarsJSON)
}

type uiData struct {
	Connected bool
	Logs      []*controller.LogEntry
	NowUnix   int64
	VarsJSON  string
}

func (s *Server) handleUI(w http.ResponseWriter, r *http.Request) {
	vars := s.pod.ParsedVariables()
	var varsJSON string
	if vars != nil {
		if b, err := json.MarshalIndent(vars, "", "  "); err == nil {
			varsJSON = string(b)
		}
	}
	data := uiData{
		Connected: s.pod.ConnAlive(),
		Logs:      s.pod.LogSnapshot(),
		NowUnix:   time.Now().Unix(),
		VarsJSON:  varsJSON,
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := s.tmpl.Execute(w, data); err != nil {
		http.Error(w, err.Error(), 500)
	}
}

func (s *Server) handleLogsJSON(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(s.pod.LogSnapshot())
}

func (s *Server) handleVarsJSON(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	v := s.pod.ParsedVariables()
	if v == nil {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"no variables parsed yet"}`))
		return
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func (s *Server) handleAction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	_ = r.ParseForm()
	action := r.FormValue("action")
	var (
		resp string
		err  error
	)
	switch action {
	case "hello":
		resp, err = s.pod.Execute(0, "")
	case "variables":
		resp, err = s.pod.Execute(14, "")
	case "alarm":
		side := r.FormValue("side")
		pl, _ := strconv.Atoi(r.FormValue("pl"))
		du, _ := strconv.Atoi(r.FormValue("du"))
		tt, _ := strconv.ParseInt(r.FormValue("tt"), 10, 64)
		pattern := r.FormValue("pi")
		if pattern == "" {
			pattern = "double"
		}
		resp, err = s.pod.ExecuteAlarm(controller.AlarmInput{
			Side: side, PL: pl, DU: du, TT: tt, Pattern: pattern,
		})
	case "alarm-clear":
		resp, err = s.pod.Execute(16, "")
	case "settings":
		lb, _ := strconv.Atoi(r.FormValue("lb"))
		resp, err = s.pod.ExecuteSettings(controller.SettingsInput{LB: lb})
	case "temperature":
		side := r.FormValue("side")
		val, _ := strconv.Atoi(r.FormValue("value"))
		cmd := map[string]int{"left": 11, "right": 12}[side]
		if cmd == 0 {
			err = fmt.Errorf("invalid side")
			break
		}
		resp, err = s.pod.Execute(cmd, strconv.Itoa(val))
	case "temperature-duration":
		side := r.FormValue("side")
		val, _ := strconv.Atoi(r.FormValue("value"))
		cmd := map[string]int{"left": 9, "right": 10}[side]
		if cmd == 0 {
			err = fmt.Errorf("invalid side")
			break
		}
		resp, err = s.pod.Execute(cmd, strconv.Itoa(val))
	case "prime":
		resp, err = s.pod.Execute(13, "")
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

const pageTemplate = `<!DOCTYPE html>
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
