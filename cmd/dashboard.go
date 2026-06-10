package cmd

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
	sync_db "github.com/juicedata/juicefs/pkg/sync/db"
	"github.com/urfave/cli/v2"
)

type runningJob struct {
	ID        string    `json:"id"`
	SrcURL    string    `json:"src_url"`
	DstURL    string    `json:"dst_url"`
	PID       int       `json:"pid"`
	StartTime time.Time `json:"start_time"`
	Status    string    `json:"status"` // running, completed, failed
	cmd       *exec.Cmd
}

type jobRegistry struct {
	mu   sync.RWMutex
	jobs map[int]*runningJob
}

var registry = &jobRegistry{jobs: make(map[int]*runningJob)}

func (r *jobRegistry) add(job *runningJob) {
	r.mu.Lock()
	r.jobs[job.PID] = job
	r.mu.Unlock()
}

func (r *jobRegistry) list() []*runningJob {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var jobs []*runningJob
	for _, j := range r.jobs {
		if j.alive() {
			jobs = append(jobs, j)
		} else {
			j.Status = "completed"
		}
	}
	return jobs
}

func (j *runningJob) alive() bool {
	if j.cmd == nil || j.cmd.Process == nil {
		return false
	}
	err := j.cmd.Process.Signal(syscall.Signal(0))
	return err == nil
}

func cmdDashboard() *cli.Command {
	return &cli.Command{
		Name:     "dashboard",
		Usage:    "Start web dashboard to view sync progress and launch migrations",
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

			startDBDashboard(db, addr, dbURL)
			return nil
		},
	}
}

type dbDashboard struct {
	db      *sql.DB
	dbURL   string
	binPath string
}

func startDBDashboard(db *sql.DB, addr string, dbURL string) {
	binPath, _ := os.Executable()
	d := &dbDashboard{db: db, dbURL: dbURL, binPath: binPath}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/jobs", d.apiJobs)
	mux.HandleFunc("/api/job/", d.apiJobRoute)
	mux.HandleFunc("/api/running", d.apiRunning)
	mux.HandleFunc("/api/sync", d.apiSync)
	mux.HandleFunc("/", d.index)

	logger.Infof("Dashboard listening on http://localhost%s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		logger.Fatalf("Dashboard: %v", err)
	}
}

// POST /api/sync - start a new sync job
func (d *dbDashboard) apiSync(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "POST required", 405)
		return
	}
	body, _ := io.ReadAll(r.Body)
	var req struct {
		Src           string `json:"src"`
		Dst           string `json:"dst"`
		Mode          string `json:"mode"`
		Threads       int    `json:"threads"`
		PreserveMeta  bool   `json:"preserve_meta"`
		NoHttps       bool   `json:"no_https"`
		ForceUpdate   bool   `json:"force_update"`
		DeleteDst     bool   `json:"delete_dst"`
	}
	json.Unmarshal(body, &req)
	if req.Mode == "" {
		req.Mode = "sync"
	}
	if req.Src == "" {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"error": "src required"})
		return
	}

	// Build command (all modes use the "sync" subcommand with mode flags)
	args := []string{"sync"}
	if req.Mode == "scan" {
		args = append(args, "--scan")
	} else if req.Mode == "scan-single" {
		args = append(args, "--scan-single")
	}
	if req.NoHttps {
		args = append(args, "--no-https")
	}
	if req.PreserveMeta {
		args = append(args, "--preserve-meta")
	}
	if req.ForceUpdate {
		args = append(args, "--force-update")
	}
	if req.DeleteDst {
		args = append(args, "--delete-dst")
	}
	if req.Threads > 0 {
		args = append(args, fmt.Sprintf("--threads=%d", req.Threads))
	}
	if d.dbURL != "" {
		args = append(args, "--db", d.dbURL)
	}
	if req.Mode == "scan-single" {
		args = append(args, req.Src)
	} else {
		args = append(args, req.Src, req.Dst)
	}

	cmd := exec.Command(d.binPath, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	job := &runningJob{
		ID:        fmt.Sprintf("sync_%d", cmd.Process.Pid),
		SrcURL:    req.Src,
		DstURL:    req.Dst,
		PID:       cmd.Process.Pid,
		StartTime: time.Now(),
		Status:    "running",
		cmd:       cmd,
	}
	registry.add(job)

	// Wait in background
	go func() {
		cmd.Wait()
		job.Status = "completed"
	}()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"ok": true, "id": job.ID, "pid": job.PID,
	})
}

// GET /api/running - list running sync jobs
func (d *dbDashboard) apiRunning(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode(registry.list())
}

type jobSummary struct {
	ID             string    `json:"id"`
	Source         string    `json:"source"`
	Dest           string    `json:"dest"`
	Type           string    `json:"type"` // "sync", "scan", "scan-single"
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
}

func (d *dbDashboard) queryJobs() ([]jobSummary, error) {
	var jobs []jobSummary
	for _, pair := range [][3]string{
		{"sync_jobs", "juicefs_sync", "sync"},
		{"scan_jobs", "scan_sync", "scan"},
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
			j.Type = pair[2]
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

func (d *dbDashboard) apiJobRoute(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	if strings.HasSuffix(path, "/csv") {
		d.apiJobCSV(w, r)
		return
	}
	d.apiJobDetail(w, r)
}

func (d *dbDashboard) apiJobCSV(w http.ResponseWriter, r *http.Request) {
	jobID := strings.TrimSuffix(r.URL.Path[len("/api/job/"):], "/csv")
	for _, dbName := range []string{"juicefs_sync", "scan_sync", "single_scan"} {
		tableName := "objects_" + strings.ReplaceAll(strings.ReplaceAll(jobID, "-", "_"), ".", "_")
		var count int
		d.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s`", dbName, tableName)).Scan(&count)
		if count > 0 {
			rows, _ := d.db.Query(fmt.Sprintf("SELECT source_key, size, content_type, status, error_msg, start_time, end_time FROM `%s`.`%s`", dbName, tableName))
			w.Header().Set("Content-Type", "text/csv; charset=utf-8")
			w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s.csv", tableName))
			fmt.Fprintln(w, "source_key,size,content_type,status,error_msg,start_time,end_time")
			for rows.Next() {
				var key, ct, st, msg string
				var sz int64
				var start, end sql.NullTime
				rows.Scan(&key, &sz, &ct, &st, &msg, &start, &end)
				fmt.Fprintf(w, "%q,%d,%q,%q,%q,%q,%q\n", key, sz, ct, st, msg, fmtTime(start), fmtTime(end))
			}
			rows.Close()
			return
		}
	}
	http.Error(w, "not found", 404)
}

func fmtTime(t sql.NullTime) string {
	if t.Valid {
		return t.Time.Format("2006-01-02 15:04:05")
	}
	return ""
}

func (d *dbDashboard) apiJobDetail(w http.ResponseWriter, r *http.Request) {
	jobID := r.URL.Path[len("/api/job/"):]
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	for _, dbName := range []string{"juicefs_sync", "scan_sync", "single_scan"} {
		tableName := "objects_" + strings.ReplaceAll(strings.ReplaceAll(jobID, "-", "_"), ".", "_")
		var count int
		d.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s`", dbName, tableName)).Scan(&count)
		if count > 0 {
			rows, _ := d.db.Query(fmt.Sprintf("SELECT status, COUNT(*), SUM(size) FROM `%s`.`%s` GROUP BY status", dbName, tableName))
			var stats []map[string]interface{}
			for rows.Next() {
				var st string
				var cnt, totalSize int64
				rows.Scan(&st, &cnt, &totalSize)
				stats = append(stats, map[string]interface{}{
					"status": st, "count": cnt, "bytes": totalSize,
				})
			}
			rows.Close()

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
				"job_id": jobID, "db": dbName, "stats": stats, "objects": objects, "total_objects": count,
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

const indexHTML = `<!DOCTYPE html>
<html lang="zh" class="dark">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>JuiceFS Dashboard</title>
<link rel="icon" href="data:image/svg+xml,<svg xmlns=%22http://www.w3.org/2000/svg%22 viewBox=%220 0 32 32%22><rect x=%222%22 y=%226%22 width=%2210%22 height=%2220%22 rx=%222%22 fill=%22%23818cf8%22 opacity=%220.4%22/><rect x=%2220%22 y=%226%22 width=%2210%22 height=%2220%22 rx=%222%22 fill=%22%23818cf8%22 opacity=%220.2%22/><path d=%22M14 16L18 16%22 stroke=%22%23818cf8%22 stroke-width=%222%22 stroke-linecap=%22round%22/><path d=%22M16 13L20 16L16 19%22 stroke=%22%23818cf8%22 stroke-width=%222%22 stroke-linecap=%22round%22 stroke-linejoin=%22round%22/></svg>">
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{font-family:system-ui,-apple-system,sans-serif;background:var(--bg);color:var(--fg);min-height:100vh;--bg:#09090b;--fg:#e4e4e7;--card:#18181b;--border:#27272a;--muted:#71717a;--accent:#a78bfa}body.light{--bg:#fafafa;--fg:#18181b;--card:#fff;--border:#e5e5e5;--muted:#737373;--accent:#7c3aed}}
.bg{position:fixed;inset:0;z-index:0;background:radial-gradient(ellipse 80% 50% at 50% -20%,var(--glow),transparent)}body{--glow:rgba(120,119,198,0.12)}body.light{--glow:rgba(120,119,198,0.04)}
.container{position:relative;z-index:1;max-width:1100px;margin:0 auto;padding:40px 24px}
.header{text-align:center;margin-bottom:48px}
.logo{margin-bottom:16px}.header h1{font-size:28px;font-weight:700;background:linear-gradient(135deg,#818cf8,#a78bfa,#f472b6);-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.header p{color:var(--muted);margin-top:8px;font-size:14px}
.tabs{display:flex;gap:8px;margin-bottom:32px;border-bottom:1px solid #27272a;padding-bottom:16px}
.tab{padding:8px 20px;border-radius:8px 8px 0 0;cursor:pointer;font-size:14px;color:var(--muted);background:none;border:none;transition:color .2s}
.tab.active{color:var(--accent);background:var(--card);border:1px solid var(--border);border-bottom:1px solid #18181b;margin-bottom:-17px}
.tab:hover{color:var(--fg)}
.panel{display:none}.panel.active{display:block}
.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin-bottom:32px}
@media(max-width:640px){.stats{grid-template-columns:repeat(2,1fr)}}
.stat-card{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:20px;text-align:center}
.stat-card .num{font-size:28px;font-weight:700}
.stat-card .label{margin-top:6px;color:var(--muted);font-size:12px;text-transform:uppercase;letter-spacing:.5px}
.c-green .num{color:#22c55e}.c-yellow .num{color:#eab308}.c-red .num{color:#ef4444}.c-blue .num{color:#3b82f6}
.job-card{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:20px;margin-bottom:16px;cursor:pointer;transition:border-color .2s,transform .2s}
.job-card:hover{border-color:#3f3f46;transform:translateY(-1px)}
.job-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:12px}
.job-id{font-size:14px;font-weight:600;color:var(--accent)}
.job-status{padding:4px 12px;border-radius:20px;font-size:12px;font-weight:600;text-transform:uppercase}
.status-completed{background:#052e16;color:#22c55e}.status-running{background:#1e3a5f;color:#3b82f6;animation:pulse 2s infinite}
.status-failed{background:#450a0a;color:#ef4444}
.type-badge{padding:2px 8px;border-radius:4px;font-size:11px;font-weight:600;text-transform:uppercase;margin-right:8px}.type-sync{background:#1e3a5f;color:#3b82f6}.type-scan{background:#1e2a1e;color:#22c55e}.type-scan-single{background:#2a1e3a;color:var(--accent)}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.6}}
.job-paths{font-size:12px;color:var(--muted);margin-bottom:12px;display:flex;gap:12px}
.job-paths span{color:var(--muted)}
.progress{height:6px;background:#27272a;border-radius:4px;overflow:hidden;margin-bottom:12px}
.progress-fill{height:100%;border-radius:4px;transition:width .5s}
.progress-ok{background:linear-gradient(90deg,#6366f1,#8b5cf6)}.progress-fail{background:linear-gradient(90deg,#ef4444,#f97316)}
.job-meta{display:flex;gap:24px;font-size:12px;color:var(--muted)}
.job-meta b{color:var(--fg)}
.time{text-align:center;color:var(--muted);font-size:12px;margin-top:32px}
.form-group{margin-bottom:16px}
.form-group label{display:block;font-size:13px;color:var(--muted);margin-bottom:6px}
.form-group input{width:100%;padding:10px 14px;background:var(--card);border:1px solid var(--border);border-radius:8px;color:var(--fg);font-size:14px;outline:none;transition:border-color .2s}
.form-group input:focus{border-color:#6366f1}
.form-section{margin-bottom:24px}.form-label{font-size:13px;color:var(--muted);margin-bottom:10px;text-transform:uppercase;letter-spacing:.5px}
.mode-selector{display:flex;gap:12px}.mode-option{flex:1;display:flex;flex-direction:column;align-items:center;gap:4px;padding:16px 12px;background:var(--card);border:1px solid var(--border);border-radius:10px;cursor:pointer;transition:border-color .2s}.mode-option:hover{border-color:#3f3f46}.mode-option input[type=radio]{display:none}.mode-option:has(input:checked){border-color:var(--accent);background:var(--accent)}.mode-option:has(input:checked) .mode-text,.mode-option:has(input:checked) .mode-desc{color:#fff}.mode-text{font-size:15px;font-weight:600;color:var(--fg)}.mode-desc{font-size:11px;color:var(--muted)}.checkbox-grid{display:flex;gap:20px}.check-label{display:flex;align-items:center;gap:6px;font-size:14px;color:var(--muted);cursor:pointer}.check-label input[type=checkbox]{accent-color:#6366f1;width:16px;height:16px}
.row{display:grid;grid-template-columns:1fr 1fr;gap:16px}
.btn{display:inline-flex;align-items:center;gap:8px;padding:12px 24px;background:linear-gradient(135deg,#6366f1,#8b5cf6);border:none;border-radius:8px;color:#fff;font-size:15px;font-weight:600;cursor:pointer;transition:opacity .2s}
.btn:hover{opacity:.9}
.btn:disabled{opacity:.5;cursor:not-allowed}
.toast{position:fixed;top:16px;right:16px;padding:12px 20px;border-radius:8px;font-size:14px;z-index:20;animation:fade-in .3s}
.toast-ok{background:#052e16;color:#22c55e;border:1px solid #166534}
.toast-err{background:#450a0a;color:#ef4444;border:1px solid #991b1b}
@keyframes fade-in{from{opacity:0;transform:translateY(-8px)}to{opacity:1;transform:translateY(0)}}
.running-dot{width:8px;height:8px;border-radius:50%;background:#22c55e;display:inline-block;margin-right:6px}
.detail-overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,.7);z-index:10;justify-content:center;align-items:center}
.detail-overlay.show{display:flex}
.detail-panel{background:var(--card);border:1px solid var(--border);border-radius:16px;padding:32px;max-width:700px;width:90%;max-height:80vh;overflow-y:auto}
.detail-panel h2{color:var(--accent);margin-bottom:20px}
.detail-close{float:right;background:none;border:none;color:var(--muted);font-size:24px;cursor:pointer}
.detail-table{width:100%;border-collapse:collapse;font-size:13px}
.detail-table th,.detail-table td{border:1px solid #27272a;padding:8px 12px;text-align:left}
.detail-table th{background:#09090b;color:var(--muted)}
</style></head><body>
<div class="bg"></div>
<div class="container">
<div class="header"><div class="logo"><svg width="48" height="48" viewBox="0 0 48 48" fill="none"><rect x="2" y="8" width="16" height="32" rx="3" fill="var(--accent)" opacity="0.3" stroke="var(--accent)" stroke-width="2"/><rect x="30" y="8" width="16" height="32" rx="3" fill="var(--accent)" opacity="0.15" stroke="var(--accent)" stroke-width="2"/><path d="M20 24L28 24" stroke="var(--accent)" stroke-width="2.5" stroke-linecap="round"/><path d="M25 20L29 24L25 28" stroke="var(--accent)" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/><circle cx="12" cy="18" r="3" fill="var(--accent)" opacity="0.5"/><circle cx="12" cy="30" r="3" fill="var(--accent)" opacity="0.5"/><circle cx="38" cy="18" r="3" fill="var(--accent)" opacity="0.5"/><circle cx="38" cy="30" r="3" fill="var(--accent)" opacity="0.5"/></svg></div><h1>JuiceFS Sync Dashboard</h1><p>Migration control center</p><button onclick="toggleTheme()" class="theme-btn" title="Toggle theme">&#9788;&#65039;</button></div>
<div class="tabs">
<button class="tab active" onclick="switchTab('jobs')">History</button>
<button class="tab" onclick="switchTab('new-task')">New Task</button>
<button class="tab" onclick="switchTab('running')">Running</button>
</div>

<div class="panel active" id="panel-jobs">
<div class="stats">
<div class="stat-card c-green"><div class="num" id="stat-total">0</div><div class="label">Total Jobs</div></div>
<div class="stat-card c-blue"><div class="num" id="stat-copied">0</div><div class="label">Copied</div></div>
<div class="stat-card c-yellow"><div class="num" id="stat-skipped">0</div><div class="label">Skipped</div></div>
<div class="stat-card c-red"><div class="num" id="stat-failed">0</div><div class="label">Failed</div></div>
</div>
<div id="job-list"></div>
</div>

<div class="panel" id="panel-new-task">
<form id="task-form" onsubmit="startSync(event)" style="max-width:600px;margin:0 auto">
<div class="form-section"><div class="form-label">Mode</div><div class="mode-selector"><label class="mode-option"><input type="radio" name="mode" value="sync" checked onchange="toggleDst()"><span class="mode-text">Sync</span><span class="mode-desc">Copy data</span></label><label class="mode-option"><input type="radio" name="mode" value="scan" onchange="toggleDst()"><span class="mode-text">Scan</span><span class="mode-desc">Compare only</span></label><label class="mode-option"><input type="radio" name="mode" value="scan-single" onchange="toggleDst()"><span class="mode-text">Scan Single</span><span class="mode-desc">List one bucket</span></label></div></div>
<div class="row"><div class="form-group"><label>Source (SRC)</label><input id="src" placeholder="s3://ak:sk@bucket.endpoint/"></div>
<div class="form-group" id="dst-group"><label>Destination (DST)</label><input id="dst" placeholder="s3://ak:sk@bucket.endpoint/"></div></div>
<div class="row">
<div class="form-group"><label>Threads</label><input id="threads" type="number" value="10"></div>
<div class="form-section"><div class="form-label">Options</div><div class="checkbox-grid"><label class="check-label"><input type="checkbox" id="no-https" checked> No HTTPS</label><label class="check-label"><input type="checkbox" id="preserve-meta"> Preserve Meta</label><label class="check-label"><input type="checkbox" id="force-update"> Force Update</label></div></div></div>
<button type="submit" class="btn" id="sync-btn">&#9654; Start Sync</button>
</form>
<div id="toast"></div>
</div>

<div class="panel" id="panel-running">
<div id="running-list"><div style="text-align:center;color:var(--muted);padding:40px">No running sync jobs</div></div>
</div>

<div class="time" id="footer"></div>
</div>
<div class="detail-overlay" id="detail-overlay" onclick="if(event.target==this)closeDetail()">
<div class="detail-panel" id="detail-panel"><button class="detail-close" onclick="closeDetail()">&times;</button><div id="detail-content"></div></div>
</div>
<script>
function switchTab(name){
document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
document.querySelectorAll('.panel').forEach(p=>p.classList.remove('active'));
document.querySelector('.tab:nth-child('+({'jobs':1,'new-task':2,'running':3}[name])+')').classList.add('active');
document.getElementById('panel-'+name).classList.add('active');
if(name=='running')loadRunning();
}
function toast(msg,ok){var d=document.createElement('div');d.className='toast toast-'+(ok?'ok':'err');d.textContent=msg;document.body.appendChild(d);setTimeout(function(){d.remove()},3000)}
function startSync(e){e.preventDefault();
var btn=document.getElementById('sync-btn');btn.disabled=true;btn.textContent='Starting...';
fetch('/api/sync',{method:'POST',body:JSON.stringify({
src:document.getElementById('src').value,dst:document.getElementById('dst').value,mode:document.querySelector('input[name="mode"]:checked').value,
threads:parseInt(document.getElementById('threads').value)||10,
no_https:document.getElementById('no-https').checked,
preserve_meta:document.getElementById('preserve-meta').checked,
force_update:document.getElementById('force-update').checked
})}).then(r=>r.json()).then(d=>{
btn.disabled=false;btn.textContent='▶ Start Sync';
if(d.ok){toast('Sync started! PID: '+d.pid,true);switchTab('running')}
else{toast(d.error,false)}
}).catch(e=>{btn.disabled=false;btn.textContent='▶ Start Sync';toast(e.toString(),false)})}
function loadRunning(){fetch('/api/running').then(r=>r.json()).then(data=>{
var h='';
data.forEach(j=>{h+='<div class="job-card"><div class="job-header"><span class="job-id"><span class="running-dot"></span>'+j.id+'</span><span class="job-status status-running">running</span></div><div class="job-paths"><span>'+j.src_url+'</span> → <span>'+j.dst_url+'</span></div><div class="job-meta"><span>PID: <b>'+j.pid+'</b></span><span>Started: <b>'+new Date(j.start_time).toLocaleString()+'</b></span></div></div>'});
document.getElementById('running-list').innerHTML=h||'<div style="text-align:center;color:var(--muted);padding:40px">No running sync jobs</div>'})}
function openDetail(id){document.getElementById('detail-overlay').classList.add('show');document.getElementById('detail-content').innerHTML='Loading...';
fetch('/api/job/'+id).then(r=>r.json()).then(d=>{
var h='<h2>'+id+'</h2>';
if(d.error){h+=d.error}else{
h+='<table class="detail-table"><tr><th>Status</th><th>Count</th><th>Bytes</th></tr>';
(d.stats||[]).forEach(s=>{h+='<tr><td>'+s.status+'</td><td>'+s.count+'</td><td>'+s.bytes+'</td></tr>'});
h+='</table>';
h+='<div style="display:flex;justify-content:space-between;align-items:center;margin-top:16px"><h3 style="color:var(--muted);margin:0">Objects '+(d.total_objects>100?'(showing 100 of '+d.total_objects+')':'('+d.total_objects+' total)')+'</h3><a href="/api/job/'+id+'/csv" class="btn" style="padding:6px 12px;font-size:12px;text-decoration:none">Download CSV</a></div>';if(d.objects){h+='<table class="detail-table"><tr><th>Key</th><th>Size</th><th>Content-Type</th></tr>';
d.objects.slice(0,50).forEach(o=>{h+='<tr><td>'+o.key+'</td><td>'+o.size+'</td><td>'+(o.content_type||'')+'</td></tr>'});
h+='</table>'}}
document.getElementById('detail-content').innerHTML=h})}
function closeDetail(){document.getElementById('detail-overlay').classList.remove('show')}
function loadJobs(){fetch('/api/jobs').then(r=>r.json()).then(data=>{
var total=0,copied=0,skipped=0,failed=0,html='';
data.forEach(j=>{total++;copied+=j.copied;skipped+=j.skipped;failed+=j.failed;
html+='<div class="job-card" onclick="openDetail(\''+j.id+'\')"><div class="job-header"><span class="job-id">'+j.id+'</span><span class="job-status status-'+j.status+'">'+j.status+'</span></div><div class="job-paths"><span>'+short(j.source)+'</span> &#10142; <span>'+short(j.dest)+'</span></div><div class="progress"><div class="progress-fill '+(j.status=='failed'?'progress-fail':'progress-ok')+'" style="width:'+(j.percent||0)+'%"></div></div><div class="job-meta"><span>Total: <b>'+j.total+'</b></span><span>Copied: <b>'+j.copied+'</b></span><span>Skipped: <b>'+j.skipped+'</b></span><span>Failed: <b>'+j.failed+'</b></span><span>Bytes: <b>'+j.bytes_fmt+'</b></span><span>'+timeFmt(j.start_time)+'</span></div></div>'});
document.getElementById('job-list').innerHTML=html||'<div style="text-align:center;color:var(--muted);padding:40px">No jobs yet</div>';
document.getElementById('stat-total').textContent=total;
document.getElementById('stat-copied').textContent=copied;
document.getElementById('stat-skipped').textContent=skipped;
document.getElementById('stat-failed').textContent=failed;
document.getElementById('footer').textContent='Updated: '+new Date().toLocaleTimeString()})}
function toggleDst(){var m=document.querySelector('input[name="mode"]:checked').value;document.getElementById('dst-group').style.display=m=='scan-single'?'none':'block'}
function toggleTheme(){document.body.classList.toggle("light");localStorage.setItem("theme",document.body.classList.contains("light")?"light":"dark")}if(localStorage.getItem("theme")==="light")document.body.classList.add("light")
function short(s){if(!s)return'';var i=s.indexOf('://');var t=i>0?s.substring(i+3):s;return t.length>40?t.substring(0,40)+'...':t}
function timeFmt(t){if(!t)return'';return new Date(t).toLocaleString()}
loadJobs();setInterval(loadJobs,10000);setInterval(loadRunning,5000);
</script></body></html>`

func (d *dbDashboard) index(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprint(w, indexHTML)
}
