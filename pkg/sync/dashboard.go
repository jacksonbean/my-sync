package sync

import (
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"time"
)

type syncProgress struct {
	Copied  int64 `json:"copied"`
	Skipped int64 `json:"skipped"`
	Failed  int64 `json:"failed"`
	Deleted int64 `json:"deleted"`
	Checked int64 `json:"checked"`
	Extra   int64 `json:"extra"`
	Handled int64 `json:"handled"`
	Bytes   int64 `json:"bytes"`
	Time    string `json:"time"`
}

func getProgress() syncProgress {
	p := syncProgress{
		Time: time.Now().Format("2006-01-02 15:04:05"),
	}
	if copied != nil {
		p.Copied = copied.Current()
	}
	if skipped != nil {
		p.Skipped = skipped.Current()
	}
	if failed != nil {
		p.Failed = failed.Current()
	}
	if deleted != nil {
		p.Deleted = deleted.Current()
	}
	if checked != nil {
		p.Checked = checked.Current()
	}
	if extra != nil {
		p.Extra = extra.Current()
	}
	if copiedBytes != nil {
		p.Bytes = copiedBytes.Current()
	}
	if handled != nil {
		p.Handled = handled.Current()
	}
	return p
}

const dashboardHTML = `<!DOCTYPE html>
<html><head><title>JuiceFS Sync</title>
<meta charset="utf-8">
<meta http-equiv="refresh" content="5">
<style>
body{font-family:system-ui,monospace;background:#0d1117;color:#c9d1d9;margin:40px auto;max-width:700px}
h1{color:#58a6ff;font-size:24px}
.grid{display:grid;grid-template-columns:repeat(3,1fr);gap:16px}
.card{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:20px;text-align:center}
.card .num{font-size:36px;font-weight:bold;color:#58a6ff}
.card .label{margin-top:8px;color:#8b949e;font-size:14px}
.bar{height:8px;background:#30363d;border-radius:4px;margin-top:10px}
.bar-fill{height:100%;background:#238636;border-radius:4px;transition:width 0.5s}
.time{text-align:center;color:#8b949e;margin-top:24px}
</style></head><body>
<h1>⚡ JuiceFS Sync Progress</h1>
<div class="grid">
<div class="card"><div class="num">{{.Copied}}</div><div class="label">Copied</div></div>
<div class="card"><div class="num">{{.Skipped}}</div><div class="label">Skipped</div></div>
<div class="card"><div class="num">{{.Failed}}</div><div class="label">Failed</div></div>
<div class="card"><div class="num">{{.Deleted}}</div><div class="label">Deleted</div></div>
<div class="card"><div class="num">{{.Checked}}</div><div class="label">Checked</div></div>
<div class="card"><div class="num">{{.Extra}}</div><div class="label">Extra</div></div>
</div>
<div style="margin-top:24px">
<div class="bar"><div class="bar-fill" style="width:{{printf "%.0f" .Percent}}%"></div></div>
<div style="text-align:center;color:#8b949e;margin-top:4px;font-size:12px">{{.PercentHtml}}%</div>
</div>
<div class="time">Updated: {{.Time}} | Total: {{.Handled}} objects, {{.Bytes}} bytes</div>
<script>
setTimeout(function(){location.reload()},3000);
</script>
</body></html>`

func (p syncProgress) Percent() float64 {
	if p.Handled == 0 {
		return 0
	}
	done := p.Copied + p.Skipped
	pct := float64(done) / float64(p.Handled) * 100
	if pct > 100 {
		pct = 100
	}
	return pct
}

func (p syncProgress) PercentHtml() string {
	return template.HTMLEscapeString(fmt.Sprintf("%.1f", p.Percent()))
}

func startDashboard(addr string) {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/progress", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		json.NewEncoder(w).Encode(getProgress())
	})

	tmpl := template.Must(template.New("dashboard").Parse(dashboardHTML))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		p := getProgress()
		tmpl.Execute(w, p)
	})

	go func() {
		if err := http.ListenAndServe(addr, mux); err != nil {
			logger.Errorf("Dashboard error: %v", err)
		}
	}()
	logger.Infof("Dashboard listening on http://localhost%s", addr)
}
