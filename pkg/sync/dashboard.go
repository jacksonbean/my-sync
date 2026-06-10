package sync

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type syncProgress struct {
	Copied    int64  `json:"copied"`
	Skipped   int64  `json:"skipped"`
	Failed    int64  `json:"failed"`
	Deleted   int64  `json:"deleted"`
	Checked   int64  `json:"checked"`
	Extra     int64  `json:"extra"`
	Total     int64  `json:"total"`
	Bytes     int64  `json:"bytes"`
	BytesFmt  string `json:"bytes_fmt"`
	Percent   int    `json:"percent"`
	Time      string `json:"time"`
}

func getProgress() syncProgress {
	p := syncProgress{Time: time.Now().Format("15:04:05")}
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
		p.Total = handled.Current()
	}
	p.BytesFmt = formatSize(p.Bytes)
	done := p.Copied + p.Skipped + p.Deleted
	if p.Total > 0 {
		p.Percent = int(float64(done) / float64(p.Total) * 100)
		if p.Percent > 100 {
			p.Percent = 100
		}
	}
	return p
}

const dashboardHTML = `<!DOCTYPE html>
<html lang="zh" class="dark">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>JuiceFS Sync</title>
<link rel="icon" href="data:image/svg+xml,<svg xmlns=%22http://www.w3.org/2000/svg%22 viewBox=%220 0 32 32%22><rect x=%222%22 y=%226%22 width=%2210%22 height=%2220%22 rx=%222%22 fill=%22%23818cf8%22 opacity=%220.4%22/><rect x=%2220%22 y=%226%22 width=%2210%22 height=%2220%22 rx=%222%22 fill=%22%23818cf8%22 opacity=%220.2%22/><path d=%22M14 16L18 16%22 stroke=%22%23818cf8%22 stroke-width=%222%22 stroke-linecap=%22round%22/><path d=%22M16 13L20 16L16 19%22 stroke=%22%23818cf8%22 stroke-width=%222%22 stroke-linecap=%22round%22 stroke-linejoin=%22round%22/></svg>">
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{font-family:system-ui,-apple-system,sans-serif;background:#09090b;color:#e4e4e7;min-height:100vh}
.bg{position:fixed;inset:0;z-index:0;background:radial-gradient(ellipse 80% 50% at 50% -20%,rgba(120,119,198,0.15),transparent)}
.container{position:relative;z-index:1;max-width:900px;margin:0 auto;padding:40px 24px}
.header{text-align:center;margin-bottom:48px}
.header h1{font-size:28px;font-weight:700;background:linear-gradient(135deg,#818cf8,#a78bfa,#f472b6);-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.header p{color:#71717a;margin-top:8px;font-size:14px}
.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin-bottom:32px}
@media(max-width:640px){.stats{grid-template-columns:repeat(2,1fr)}}
.card{background:#18181b;border:1px solid #27272a;border-radius:12px;padding:24px;text-align:center;transition:transform .2s,border-color .2s}
.card:hover{transform:scale(1.02);border-color:#3f3f46}
.card .num{font-size:32px;font-weight:700;font-variant-numeric:tabular-nums}
.card .label{margin-top:8px;color:#a1a1aa;font-size:13px;text-transform:uppercase;letter-spacing:.5px}
.card-copied .num{color:#22c55e}.card-skipped .num{color:#eab308}
.card-failed .num{color:#ef4444}.card-bytes .num{color:#3b82f6}
.progress-section{background:#18181b;border:1px solid #27272a;border-radius:12px;padding:24px;margin-bottom:24px}
.progress-label{display:flex;justify-content:space-between;margin-bottom:12px;font-size:14px;color:#a1a1aa}
.progress-label span:last-child{color:#e4e4e7;font-weight:600}
.progress-bar{height:10px;background:#27272a;border-radius:8px;overflow:hidden;position:relative}
.progress-fill{height:100%;border-radius:8px;transition:width .5s ease;background:linear-gradient(90deg,#6366f1,#8b5cf6,#d946ef);position:relative}
.progress-fill::after{content:'';position:absolute;top:0;left:0;right:0;height:50%;background:linear-gradient(180deg,rgba(255,255,255,.15),transparent);border-radius:8px 8px 0 0}
.bar-label{display:flex;justify-content:center;gap:32px;margin-top:12px;font-size:12px;color:#71717a}
.bar-label span{display:flex;align-items:center;gap:6px}
.bar-label .dot{width:8px;height:8px;border-radius:50%}
.dot-copied{background:#22c55e}.dot-skipped{background:#eab308}.dot-failed{background:#ef4444}.dot-deleted{background:#f97316}
.status-bar{display:flex;align-items:center;gap:8px;font-size:13px;color:#a1a1aa;margin-bottom:16px;padding:8px 16px;background:#18181b;border:1px solid #27272a;border-radius:8px}
.status-dot{width:8px;height:8px;border-radius:50%;background:#22c55e;animation:pulse 2s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.5}}
.time{text-align:center;color:#52525b;font-size:12px;margin-top:32px}
</style></head><body>
<div class="bg"></div>
<div class="container">
<div class="header"><h1>&#9879; JuiceFS Sync</h1><p>Real-time migration progress</p></div>
<div class="status-bar"><div class="status-dot"></div><span>Live &middot; updated <span id="time">--</span></span></div>
<div class="stats">
<div class="card card-copied"><div class="num" id="copied">0</div><div class="label">Copied</div></div>
<div class="card card-skipped"><div class="num" id="skipped">0</div><div class="label">Skipped</div></div>
<div class="card card-failed"><div class="num" id="failed">0</div><div class="label">Failed</div></div>
<div class="card card-bytes"><div class="num" id="bytes">0</div><div class="label">Transferred</div></div>
</div>
<div class="progress-section">
<div class="progress-label"><span>Progress</span><span id="percent">0%</span></div>
<div class="progress-bar"><div class="progress-fill" id="bar" style="width:0%"></div></div>
<div class="bar-label">
<span><span class="dot dot-copied"></span>Copied <b id="lbl-copied">0</b></span>
<span><span class="dot dot-skipped"></span>Skipped <b id="lbl-skipped">0</b></span>
<span><span class="dot dot-failed"></span>Failed <b id="lbl-failed">0</b></span>
<span><span class="dot dot-deleted"></span>Deleted <b id="lbl-deleted">0</b></span>
</div>
</div>
<div class="time" id="footer">Total: 0 objects</div>
</div>
<script>
function fetchProgress(){fetch('/api/progress').then(r=>r.json()).then(d=>{
document.getElementById('copied').textContent=d.copied||0;
document.getElementById('skipped').textContent=d.skipped||0;
document.getElementById('failed').textContent=d.failed||0;
document.getElementById('bytes').textContent=d.bytes_fmt||'0';
document.getElementById('percent').textContent=(d.percent||0)+'%';
document.getElementById('bar').style.width=(d.percent||0)+'%';
document.getElementById('time').textContent=d.time||'--';
document.getElementById('lbl-copied').textContent=d.copied||0;
document.getElementById('lbl-skipped').textContent=d.skipped||0;
document.getElementById('lbl-failed').textContent=d.failed||0;
document.getElementById('lbl-deleted').textContent=d.deleted||0;
document.getElementById('footer').textContent='Total: '+(d.total||0)+' objects | '+ (d.bytes_fmt||'0');
})}
fetchProgress();setInterval(fetchProgress,2000);
</script></body></html>`

func startDashboard(addr string) {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/progress", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		json.NewEncoder(w).Encode(getProgress())
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprint(w, dashboardHTML)
	})

	go func() {
		if err := http.ListenAndServe(addr, mux); err != nil {
			logger.Errorf("Dashboard error: %v", err)
		}
	}()
	logger.Infof("Dashboard listening on http://localhost%s", addr)
}
