package cmd

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	sync_db "github.com/juicedata/juicefs/pkg/sync/db"
	"github.com/urfave/cli/v2"
)

func cmdDashboard() *cli.Command {
	return &cli.Command{
		Name:     "dashboard",
		Usage:    "Start web dashboard to view sync progress and history",
		Category: "TOOL",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "db",
				Usage: "MySQL connection string (e.g. mysql://user:pass@host:3306)",
			},
			&cli.StringFlag{
				Name:  "port",
				Value: ":8080",
				Usage: "dashboard listen address",
			},
		},
		Action: func(c *cli.Context) error {
			addr := c.String("port")
			if addr[0] != ':' {
				addr = ":" + addr
			}

			dbURL := c.String("db")
			if dbURL == "" {
				return fmt.Errorf("--db is required")
			}

			cfg, err := sync_db.ParseDbDSN(dbURL)
			if err != nil {
				return err
			}

			dsn := fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&loc=Local", cfg.User, cfg.Pass, cfg.Host)
			db, err := sql.Open("mysql", dsn)
			if err != nil {
				return err
			}
			defer db.Close()

			startDBDashboard(db, addr)
			return nil
		},
	}
}

type dbDashboard struct {
	db *sql.DB
}

func startDBDashboard(db *sql.DB, addr string) {
	d := &dbDashboard{db: db}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/jobs", d.apiJobs)
	mux.HandleFunc("/api/job/", d.apiJobDetail)
	mux.HandleFunc("/", d.index)

	logger.Infof("Dashboard listening on http://localhost%s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		logger.Fatalf("Dashboard: %v", err)
	}
}

type jobSummary struct {
	ID             string    `json:"id"`
	Source         string    `json:"source"`
	Dest           string    `json:"dest"`
	Status         string    `json:"status"`
	StartTime      time.Time `json:"start_time"`
	EndTime        time.Time `json:"end_time"`
	TotalObjects   int64     `json:"total"`
	CopiedObjects  int64     `json:"copied"`
	SkippedObjects int64     `json:"skipped"`
	FailedObjects  int64     `json:"failed"`
	DeletedObjects int64     `json:"deleted"`
	TotalBytes     int64     `json:"bytes"`
	BytesFmt       string    `json:"bytes_fmt"`
	Percent        int       `json:"percent"`
	DBName         string    `json:"db_name"`
}

func (d *dbDashboard) queryJobs() ([]jobSummary, error) {
	var jobs []jobSummary
	for _, pair := range [][2]string{
		{"sync_jobs", "juicefs_sync"},
		{"scan_jobs", "scan_sync"},
	} {
		rows, err := d.db.Query(
			fmt.Sprintf("SELECT id, src_url, dst_url, status, start_time, end_time, total_objects, copied_objects, skipped_objects, failed_objects, deleted_objects, total_bytes FROM `%s`.`sync_jobs` ORDER BY start_time DESC LIMIT 50", pair[0]))
		if err != nil {
			continue
		}
		for rows.Next() {
			var j jobSummary
			var start, end sql.NullTime
			rows.Scan(&j.ID, &j.Source, &j.Dest, &j.Status, &start, &end, &j.TotalObjects, &j.CopiedObjects, &j.SkippedObjects, &j.FailedObjects, &j.DeletedObjects, &j.TotalBytes)
			if start.Valid {
				j.StartTime = start.Time
			}
			if end.Valid {
				j.EndTime = end.Time
			}
			j.BytesFmt = formatBytesStr(j.TotalBytes)
			done := j.CopiedObjects + j.SkippedObjects + j.DeletedObjects
			if j.TotalObjects > 0 {
				j.Percent = int(float64(done) / float64(j.TotalObjects) * 100)
				if j.Percent > 100 {
					j.Percent = 100
				}
			}
			j.DBName = pair[0]
			jobs = append(jobs, j)
		}
		rows.Close()
	}
	return jobs, nil
}

func (d *dbDashboard) apiJobs(w http.ResponseWriter, r *http.Request) {
	jobs, _ := d.queryJobs()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode(jobs)
}

func (d *dbDashboard) apiJobDetail(w http.ResponseWriter, r *http.Request) {
	jobID := r.URL.Path[len("/api/job/"):]
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	for _, dbName := range []string{"juicefs_sync", "scan_sync", "single_scan"} {
		tableName := strings.ReplaceAll(strings.ReplaceAll(jobID, "-", "_"), ".", "_")
		var count int
		d.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s`", dbName, tableName)).Scan(&count)
		if count > 0 {
			rows, _ := d.db.Query(fmt.Sprintf("SELECT status, COUNT(*), SUM(size) FROM `%s`.`%s` GROUP BY status", dbName, tableName))
			var stats []map[string]interface{}
			for rows.Next() {
				var status string
				var cnt, totalSize int64
				rows.Scan(&status, &cnt, &totalSize)
				stats = append(stats, map[string]interface{}{
					"status": status, "count": cnt, "bytes": totalSize,
				})
			}
			rows.Close()

			// Also get sample objects
			objRows, _ := d.db.Query(fmt.Sprintf("SELECT source_key, size, content_type, status FROM `%s`.`%s` LIMIT 100", dbName, tableName))
			var objects []map[string]interface{}
			for objRows.Next() {
				var key, ct, st string
				var sz int64
				objRows.Scan(&key, &sz, &ct, &st)
				objects = append(objects, map[string]interface{}{
					"key": key, "size": sz, "content_type": ct, "status": st,
				})
			}
			objRows.Close()

			json.NewEncoder(w).Encode(map[string]interface{}{
				"job_id": jobID, "db": dbName, "stats": stats, "objects": objects,
			})
			return
		}
	}
	json.NewEncoder(w).Encode(map[string]string{"error": "not found"})
}

func formatBytesStr(b int64) string {
	if b < 1024 {
		return fmt.Sprintf("%d B", b)
	}
	v := float64(b)
	units := []string{"B", "KiB", "MiB", "GiB", "TiB"}
	z := 0
	for v >= 1024 && z < len(units)-1 {
		v /= 1024
		z++
	}
	return fmt.Sprintf("%.1f %s", v, units[z])
}

func shortURL(s string) string {
	u, err := url.Parse(s)
	if err != nil {
		if len(s) > 40 {
			return s[:40] + "..."
		}
		return s
	}
	return u.Host + u.Path
}

const indexHTML = `<!DOCTYPE html>
<html lang="zh" class="dark">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>JuiceFS Dashboard</title>
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{font-family:system-ui,-apple-system,sans-serif;background:#09090b;color:#e4e4e7;min-height:100vh}
.bg{position:fixed;inset:0;z-index:0;background:radial-gradient(ellipse 80% 50% at 50% -20%,rgba(120,119,198,0.12),transparent)}
.container{position:relative;z-index:1;max-width:1100px;margin:0 auto;padding:40px 24px}
.header{text-align:center;margin-bottom:48px}
.header h1{font-size:28px;font-weight:700;background:linear-gradient(135deg,#818cf8,#a78bfa,#f472b6);-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.header p{color:#71717a;margin-top:8px;font-size:14px}
.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin-bottom:32px}
@media(max-width:640px){.stats{grid-template-columns:repeat(2,1fr)}}
.stat-card{background:#18181b;border:1px solid #27272a;border-radius:12px;padding:20px;text-align:center}
.stat-card .num{font-size:28px;font-weight:700;font-variant-numeric:tabular-nums}
.stat-card .label{margin-top:6px;color:#a1a1aa;font-size:12px;text-transform:uppercase;letter-spacing:.5px}
.c-green .num{color:#22c55e}.c-yellow .num{color:#eab308}.c-red .num{color:#ef4444}.c-blue .num{color:#3b82f6}
.job-card{background:#18181b;border:1px solid #27272a;border-radius:12px;padding:20px;margin-bottom:16px;cursor:pointer;transition:border-color .2s,transform .2s}
.job-card:hover{border-color:#3f3f46;transform:translateY(-1px)}
.job-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:12px}
.job-id{font-size:14px;font-weight:600;color:#a78bfa}
.job-status{padding:4px 12px;border-radius:20px;font-size:12px;font-weight:600;text-transform:uppercase}
.status-completed{background:#052e16;color:#22c55e}
.status-running{background:#1e3a5f;color:#3b82f6}
.status-failed{background:#450a0a;color:#ef4444}
.job-paths{font-size:12px;color:#71717a;margin-bottom:12px;display:flex;gap:12px}
.job-paths span{color:#a1a1aa}
.progress{height:6px;background:#27272a;border-radius:4px;overflow:hidden;margin-bottom:12px}
.progress-fill{height:100%;border-radius:4px;transition:width .5s}
.progress-ok{background:linear-gradient(90deg,#6366f1,#8b5cf6)}.progress-fail{background:linear-gradient(90deg,#ef4444,#f97316)}
.job-meta{display:flex;gap:24px;font-size:12px;color:#71717a}
.job-meta b{color:#e4e4e7}
.time{text-align:center;color:#52525b;font-size:12px;margin-top:32px}
.detail-overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,.7);z-index:10;justify-content:center;align-items:center}
.detail-overlay.show{display:flex}
.detail-panel{background:#18181b;border:1px solid #27272a;border-radius:16px;padding:32px;max-width:700px;width:90%;max-height:80vh;overflow-y:auto}
.detail-panel h2{color:#a78bfa;margin-bottom:20px}
.detail-close{float:right;background:none;border:none;color:#71717a;font-size:24px;cursor:pointer}
.detail-table{width:100%;border-collapse:collapse;font-size:13px}
.detail-table th,.detail-table td{border:1px solid #27272a;padding:8px 12px;text-align:left}
.detail-table th{background:#09090b;color:#a1a1aa}
</style></head><body>
<div class="bg"></div>
<div class="container">
<div class="header"><h1>&#128202; JuiceFS Sync Dashboard</h1><p>Migration history & analytics</p></div>
<div class="stats">
<div class="stat-card c-green"><div class="num" id="stat-total">0</div><div class="label">Total Jobs</div></div>
<div class="stat-card c-blue"><div class="num" id="stat-copied">0</div><div class="label">Copied</div></div>
<div class="stat-card c-yellow"><div class="num" id="stat-skipped">0</div><div class="label">Skipped</div></div>
<div class="stat-card c-red"><div class="num" id="stat-failed">0</div><div class="label">Failed</div></div>
</div>
<div id="job-list"></div>
<div class="time" id="footer"></div>
</div>
<div class="detail-overlay" id="detail-overlay" onclick="if(event.target==this)closeDetail()">
<div class="detail-panel" id="detail-panel"><button class="detail-close" onclick="closeDetail()">&times;</button><div id="detail-content"></div></div>
</div>
<script>
function openDetail(id){document.getElementById('detail-overlay').classList.add('show');document.getElementById('detail-content').innerHTML='Loading...';
fetch('/api/job/'+id).then(r=>r.json()).then(d=>{
var h='<h2>'+id+'</h2>';
if(d.error){h+=d.error}else{
h+='<table class="detail-table"><tr><th>Status</th><th>Count</th><th>Bytes</th></tr>';
(d.stats||[]).forEach(s=>{h+='<tr><td>'+s.status+'</td><td>'+s.count+'</td><td>'+s.bytes+'</td></tr>'});
h+='</table>';
if(d.objects){
h+='<h3 style="margin-top:16px;color:#a1a1aa">Sample Objects</h3><table class="detail-table"><tr><th>Key</th><th>Size</th><th>Content-Type</th></tr>';
d.objects.slice(0,50).forEach(o=>{h+='<tr><td>'+o.key+'</td><td>'+o.size+'</td><td>'+(o.content_type||'')+'</td></tr>'});
h+='</table>'}
}
document.getElementById('detail-content').innerHTML=h})}
function closeDetail(){document.getElementById('detail-overlay').classList.remove('show')}
function loadJobs(){fetch('/api/jobs').then(r=>r.json()).then(data=>{
var total=0,copied=0,skipped=0,failed=0;
var html='';
data.forEach(j=>{total++;copied+=j.copied;skipped+=j.skipped;failed+=j.failed;
var pct=j.percent||0;
html+='<div class="job-card" onclick="openDetail(\''+j.id+'\')"><div class="job-header"><span class="job-id">'+j.id+'</span><span class="job-status status-'+j.status+'">'+j.status+'</span></div><div class="job-paths"><span>'+short(j.source)+'</span> &#10142; <span>'+short(j.dest)+'</span></div><div class="progress"><div class="progress-fill '+(j.status=='failed'?'progress-fail':'progress-ok')+'" style="width:'+pct+'%"></div></div><div class="job-meta"><span>Total: <b>'+j.total+'</b></span><span>Copied: <b>'+j.copied+'</b></span><span>Skipped: <b>'+j.skipped+'</b></span><span>Failed: <b>'+j.failed+'</b></span><span>Bytes: <b>'+j.bytes_fmt+'</b></span><span>'+timeFmt(j.start_time)+'</span></div></div>'});
document.getElementById('job-list').innerHTML=html||'<div style="text-align:center;color:#71717a;padding:40px">No jobs yet. Run a sync with --db to record.</div>';
document.getElementById('stat-total').textContent=total;
document.getElementById('stat-copied').textContent=copied;
document.getElementById('stat-skipped').textContent=skipped;
document.getElementById('stat-failed').textContent=failed;
document.getElementById('footer').textContent='Updated: '+new Date().toLocaleTimeString()})}
function short(s){if(!s)return'';var i=s.indexOf('://');var t=i>0?s.substring(i+3):s;return t.length>40?t.substring(0,40)+'...':t}
function timeFmt(t){if(!t)return'';return new Date(t).toLocaleString()}
loadJobs();setInterval(loadJobs,10000);
</script></body></html>`

func (d *dbDashboard) index(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprint(w, indexHTML)
}
