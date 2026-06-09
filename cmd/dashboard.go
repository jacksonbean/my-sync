package cmd

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	_ "github.com/go-sql-driver/mysql"
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

			cfg, err := parseDashboardDSN(dbURL)
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

type dbConfig struct {
	Host string
	User string
	Pass string
}

func parseDashboardDSN(raw string) (*dbConfig, error) {
	cfg, err := parseRawDSN(raw)
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

// parseRawDSN parses mysql://user:pass@host:port
func parseRawDSN(raw string) (*dbConfig, error) {
	u, err := parseURL(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid db url: %w", err)
	}
	pass := u.Password
	host := u.Host
	if host == "" {
		return nil, fmt.Errorf("host required")
	}
	return &dbConfig{Host: host, User: u.User, Pass: pass}, nil
}

type parsedURL struct {
	Scheme   string
	Host     string
	User     string
	Password string
}

func parseURL(raw string) (*parsedURL, error) {
	// Simple manual parse for mysql://user:pass@host:port
	p := &parsedURL{}
	rest := raw

	// Scheme
	for i, ch := range rest {
		if ch == ':' && rest[i+1] == '/' && rest[i+2] == '/' {
			p.Scheme = rest[:i]
			rest = rest[i+3:]
			break
		}
	}
	if p.Scheme == "" {
		return nil, fmt.Errorf("invalid url: %s", raw)
	}

	// User:Pass@Host
	atIdx := -1
	for i := len(rest) - 1; i >= 0; i-- {
		if rest[i] == '@' {
			atIdx = i
			break
		}
	}
	if atIdx > 0 {
		userInfo := rest[:atIdx]
		for j, ch := range userInfo {
			if ch == ':' {
				p.User = userInfo[:j]
				p.Password = userInfo[j+1:]
				break
			}
		}
		if p.User == "" {
			p.User = userInfo
		}
		rest = rest[atIdx+1:]
	}

	// Host:Port
	p.Host = rest
	return p, nil
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
	TotalBytes     int64     `json:"bytes"`
	DBName         string    `json:"db_name"`
}

func (d *dbDashboard) queryJobs() ([]jobSummary, error) {
	var jobs []jobSummary
	for _, pair := range [][2]string{
		{"sync_jobs", "juicefs_sync"},
		{"scan_jobs", "scan_sync"},
	} {
		rows, err := d.db.Query(
			fmt.Sprintf("SELECT id, src_url, dst_url, status, start_time, end_time, total_objects, copied_objects, skipped_objects, failed_objects, total_bytes FROM `%s`.`sync_jobs` ORDER BY start_time DESC LIMIT 50", pair[0]))
		if err != nil {
			continue
		}
		for rows.Next() {
			var j jobSummary
			var start, end sql.NullTime
			rows.Scan(&j.ID, &j.Source, &j.Dest, &j.Status, &start, &end, &j.TotalObjects, &j.CopiedObjects, &j.SkippedObjects, &j.FailedObjects, &j.TotalBytes)
			if start.Valid {
				j.StartTime = start.Time
			}
			if end.Valid {
				j.EndTime = end.Time
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

	// Try to find in all data DBs
	for _, dbName := range []string{"juicefs_sync", "scan_sync"} {
		tableName := "objects_" + sanitizeJobID(jobID)
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
			json.NewEncoder(w).Encode(map[string]interface{}{
				"job_id": jobID, "db": dbName, "stats": stats,
			})
			return
		}
	}
	json.NewEncoder(w).Encode(map[string]string{"error": "not found"})
}

func sanitizeJobID(id string) string {
	id = replaceAll(id, "-", "_")
	id = replaceAll(id, ".", "_")
	return id
}

func replaceAll(s, old, new string) string {
	for {
		t := ""
		for i := 0; i < len(s); i++ {
			if i+len(old) <= len(s) && s[i:i+len(old)] == old {
				t += new
				i += len(old) - 1
			} else {
				t += string(s[i])
			}
		}
		if t == s {
			break
		}
		s = t
	}
	return s
}

const indexHTML = `<!DOCTYPE html>
<html><head><title>JuiceFS Dashboard</title>
<meta charset="utf-8">
<meta http-equiv="refresh" content="10">
<style>
body{font-family:system-ui;background:#0d1117;color:#c9d1d9;margin:40px auto;max-width:1000px}
h1{color:#58a6ff}
table{width:100%;border-collapse:collapse;margin-top:16px}
th,td{border:1px solid #30363d;padding:10px;text-align:left}
th{background:#161b22}
tr:hover{background:#161b22}
.status-ok{color:#238636}.status-fail{color:#da3633}.status-run{color:#58a6ff}
</style></head><body>
<h1>📊 JuiceFS Sync Dashboard</h1>
<table><thead><tr>
<th>Job ID</th><th>Source</th><th>Dest</th><th>Status</th><th>Total</th><th>Copied</th><th>Skipped</th><th>Failed</th><th>Time</th>
</tr></thead><tbody id="jobs"></tbody></table>
<script>
fetch('/api/jobs').then(r=>r.json()).then(data=>{
  document.getElementById('jobs').innerHTML = data.map(j=>` + "`" + `
    <tr onclick="location.href='/job/${j.id}'" style="cursor:pointer">
      <td>${j.id}</td>
      <td>${j.source||''}</td>
      <td>${j.dest||''}</td>
      <td class="status-${j.status=='completed'?'ok':j.status=='failed'?'fail':'run'}">${j.status}</td>
      <td>${j.total}</td><td>${j.copied}</td><td>${j.skipped}</td><td>${j.failed}</td>
      <td>${j.start_time}</td></tr>` + "`" + `).join('');
});
</script></body></html>`

func (d *dbDashboard) index(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		// Job detail page
		if len(r.URL.Path) > 5 && r.URL.Path[:5] == "/job/" {
			d.jobPage(w, r)
			return
		}
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html")
	fmt.Fprint(w, indexHTML)
}

func (d *dbDashboard) jobPage(w http.ResponseWriter, r *http.Request) {
	jobID := r.URL.Path[5:]
	fmt.Fprintf(w, `<!DOCTYPE html><html><head><title>Job %s</title>
<meta charset="utf-8"><style>body{font-family:system-ui;background:#0d1117;color:#c9d1d9;margin:40px}
h1{color:#58a6ff}table{width:100%%;border-collapse:collapse}th,td{border:1px solid #30363d;padding:8px}th{background:#161b22}
</style></head><body><h1>Job: %s</h1><div id="detail">Loading...</div>
<script>
fetch('/api/job/%s').then(r=>r.json()).then(d=>{
  if(d.error){document.getElementById('detail').innerHTML=d.error;return}
  let html='<table><tr><th>Status</th><th>Count</th><th>Bytes</th></tr>';
  (d.stats||[]).forEach(s=>{html+=` + "`" + `<tr><td>${s.status}</td><td>${s.count}</td><td>${s.bytes}</td></tr>` + "`" + `});
  html+='</table>';
  document.getElementById('detail').innerHTML=html;
});
</script></body></html>`, jobID, jobID, jobID)
}
