package httpui

import (
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ssttevee/ninesleep-go/internal/controller"
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
	s.mux.HandleFunc("/variables.json", s.handleVarsJSON)
}

type uiData struct {
	Connected bool
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
		NowUnix:   time.Now().Unix(),
		VarsJSON:  varsJSON,
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := s.tmpl.Execute(w, data); err != nil {
		http.Error(w, err.Error(), 500)
	}
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
		resp, err = s.pod.ExecuteFranken(controller.FrankenCommandHello, "")
	case "variables":
		resp, err = s.pod.ExecuteFranken(controller.FrankenCommandPleaseSendVariables, "")
	case "alarm":
		side, err := controller.SideFromString(r.FormValue("side"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
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
		resp, err = s.pod.ExecuteRaw(16, "")
	case "settings":
		lb, _ := strconv.Atoi(r.FormValue("lb"))
		resp, err = s.pod.ExecuteSettings(controller.SettingsInput{LB: lb})
	case "temperature":
		side, err := controller.SideFromString(r.FormValue("side"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		val, _ := strconv.Atoi(r.FormValue("value"))
		err = s.pod.ExecuteHeatLevel(side, val)
	case "temperature-duration":
		side, err := controller.SideFromString(r.FormValue("side"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		val, _ := strconv.Atoi(r.FormValue("value"))
		err = s.pod.ExecuteHeatDuration(side, val)
	case "prime":
		err = s.pod.ExecutePrime()
	case "raw":
		cmdID, _ := strconv.Atoi(r.FormValue("cmd"))
		payload := strings.TrimSpace(r.FormValue("payload"))
		resp, err = s.pod.ExecuteRaw(cmdID, payload)
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

  <fieldset>
    <legend>Arbitrary Command</legend>
    <form method="post" action="/action">
      <input type="hidden" name="action" value="raw">
      <label>Command ID:
        <input type="number" name="cmd" min="0" required>
      </label>
      <label>Payload Hex (optional):
        <input type="text" name="payload" placeholder="e.g. a1b203">
      </label>
      <button type="submit">Send Command</button>
    </form>
    <p><small>Payload (if provided) is sent as raw hex (no spaces). Leave empty for commands with no payload.</small></p>
    </fieldset>

  <fieldset>
    <legend>Command Legend</legend>
    <ul style="margin-top:0;">
      <li><strong>0</strong>: Hello / ping</li>
      <li><strong>5</strong>: Set alarm (left side)</li>
      <li><strong>6</strong>: Set alarm (right side)</li>
      <li><strong>8</strong>: Apply settings (LED brightness)</li>
      <li><strong>9</strong>: Heat duration (left)</li>
      <li><strong>10</strong>: Heat duration (right)</li>
      <li><strong>11</strong>: Target heat level (left)</li>
      <li><strong>12</strong>: Target heat level (right)</li>
      <li><strong>13</strong>: Prime</li>
      <li><strong>14</strong>: Variables snapshot</li>
      <li><strong>16</strong>: Alarm clear</li>
    </ul>
    <p><small>Use the "Arbitrary Command" section above to send any command ID with an optional hex payload.</small></p>
  </fieldset>

  {{if .VarsJSON}}
  <fieldset>
    <legend>Parsed Variables</legend>
    <pre class="vars">{{.VarsJSON}}</pre>
    <p><small>Raw & parsed representation of the last Variables command (14).</small></p>
  </fieldset>
  {{end}}
</section>

<p><small>Endpoint: <code>/variables.json</code>. Refresh page to update UI.</small></p>

</body>
</html>
`
