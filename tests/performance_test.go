//go:build perf

package tests

import (
	"bufio"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nickyhof/CommitDB"
	"github.com/nickyhof/CommitDB/core"
	"github.com/nickyhof/CommitDB/ps"
)

// =============================================================================
// CONFIGURATION
// =============================================================================

// PerfConfig holds configurable test parameters
type PerfConfig struct {
	// Thresholds (can be overridden via environment variables)
	P99ThresholdMs     int           // COMMITDB_PERF_P99_MS
	TLSOverheadPercent int           // COMMITDB_PERF_TLS_OVERHEAD_PCT
	MaxErrorRate       float64       // COMMITDB_PERF_MAX_ERROR_RATE
	TestDuration       time.Duration // COMMITDB_PERF_DURATION_SEC
}

func loadPerfConfig() PerfConfig {
	cfg := PerfConfig{
		P99ThresholdMs:     50,
		TLSOverheadPercent: 15,
		MaxErrorRate:       0.001, // 0.1%
		TestDuration:       10 * time.Second,
	}

	if v := os.Getenv("COMMITDB_PERF_P99_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.P99ThresholdMs = n
		}
	}
	if v := os.Getenv("COMMITDB_PERF_TLS_OVERHEAD_PCT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.TLSOverheadPercent = n
		}
	}
	if v := os.Getenv("COMMITDB_PERF_MAX_ERROR_RATE"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			cfg.MaxErrorRate = f
		}
	}
	if v := os.Getenv("COMMITDB_PERF_DURATION_SEC"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.TestDuration = time.Duration(n) * time.Second
		}
	}

	return cfg
}

// =============================================================================
// METRICS
// =============================================================================

// PerfMetrics collects performance measurements
type PerfMetrics struct {
	mu            sync.Mutex
	Latencies     []time.Duration
	Errors        int64
	TotalRequests int64
	StartTime     time.Time
	EndTime       time.Time
}

func NewPerfMetrics() *PerfMetrics {
	return &PerfMetrics{
		Latencies: make([]time.Duration, 0, 10000),
		StartTime: time.Now(),
	}
}

func (m *PerfMetrics) Record(latency time.Duration, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.TotalRequests++
	if err != nil {
		m.Errors++
	} else {
		m.Latencies = append(m.Latencies, latency)
	}
}

func (m *PerfMetrics) Finalize() {
	m.EndTime = time.Now()
}

func (m *PerfMetrics) P50() time.Duration {
	return m.percentile(50)
}

func (m *PerfMetrics) P95() time.Duration {
	return m.percentile(95)
}

func (m *PerfMetrics) P99() time.Duration {
	return m.percentile(99)
}

func (m *PerfMetrics) percentile(p int) time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.Latencies) == 0 {
		return 0
	}

	sorted := make([]time.Duration, len(m.Latencies))
	copy(sorted, m.Latencies)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	idx := (p * len(sorted)) / 100
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func (m *PerfMetrics) Throughput() float64 {
	duration := m.EndTime.Sub(m.StartTime).Seconds()
	if duration == 0 {
		return 0
	}
	return float64(m.TotalRequests) / duration
}

func (m *PerfMetrics) ErrorRate() float64 {
	if m.TotalRequests == 0 {
		return 0
	}
	return float64(m.Errors) / float64(m.TotalRequests)
}

func (m *PerfMetrics) Print(t *testing.T, clientCount int, duration time.Duration) {
	t.Logf("Performance Results:")
	t.Logf("├── Clients:     %d", clientCount)
	t.Logf("├── Duration:    %s", duration)
	t.Logf("├── Requests:    %d", m.TotalRequests)
	t.Logf("├── Throughput:  %.1f req/s", m.Throughput())
	t.Logf("├── Latency:")
	t.Logf("│   ├── p50:     %s", m.P50())
	t.Logf("│   ├── p95:     %s", m.P95())
	t.Logf("│   └── p99:     %s", m.P99())
	t.Logf("└── Errors:      %d (%.2f%%)", m.Errors, m.ErrorRate()*100)
}

// =============================================================================
// TEST SERVER
// =============================================================================

// perfServer wraps a CommitDB server for performance testing
type perfServer struct {
	instance *CommitDB.Instance
	listener net.Listener
	addr     string
	done     chan struct{}
	wg       sync.WaitGroup
	stopOnce sync.Once

	// TLS support
	tlsConfig *tls.Config
}

func newPerfServer(t *testing.T) *perfServer {
	persistence, err := ps.NewMemoryPersistence()
	if err != nil {
		t.Fatalf("Failed to create persistence: %v", err)
	}
	instance := CommitDB.Open(&persistence)

	// Setup test database
	engine := instance.Engine(core.Identity{Name: "perf", Email: "perf@test.com"})
	engine.Execute("CREATE DATABASE perf")
	engine.Execute("CREATE TABLE perf.users (id INT PRIMARY KEY, name STRING, age INT)")

	// Insert seed data
	for i := 1; i <= 100; i++ {
		engine.Execute(fmt.Sprintf("INSERT INTO perf.users (id, name, age) VALUES (%d, 'User%d', %d)", i, i, 20+i%50))
	}

	s := &perfServer{
		instance: instance,
		done:     make(chan struct{}),
	}
	return s
}

func (s *perfServer) Start(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	s.listener = listener
	s.addr = listener.Addr().String()

	go s.acceptLoop(t)
}

func (s *perfServer) StartTLS(t *testing.T, certFile, keyFile string) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatalf("Failed to load TLS cert: %v", err)
	}

	s.tlsConfig = &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

	listener, err := tls.Listen("tcp", ":0", s.tlsConfig)
	if err != nil {
		t.Fatalf("Failed to start TLS server: %v", err)
	}
	s.listener = listener
	s.addr = listener.Addr().String()

	go s.acceptLoop(t)
}

func (s *perfServer) Stop() {
	s.stopOnce.Do(func() {
		close(s.done)
		s.listener.Close()
		s.wg.Wait()
	})
}

func (s *perfServer) acceptLoop(t *testing.T) {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.done:
				return
			default:
				continue
			}
		}
		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

func (s *perfServer) handleConnection(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	// Each connection gets its own engine - thread-safety handled at persistence layer
	engine := s.instance.Engine(core.Identity{Name: "perf", Email: "perf@test.com"})
	reader := bufio.NewReader(conn)

	for {
		select {
		case <-s.done:
			return
		default:
		}

		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}

		query := line[:len(line)-1] // trim newline
		if query == "quit" {
			return
		}

		result, execErr := engine.Execute(query)
		resp := s.buildResponse(result, execErr)

		data, _ := json.Marshal(resp)
		conn.Write(append(data, '\n'))
	}
}

type perfResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

func (s *perfServer) buildResponse(result interface{}, err error) perfResponse {
	if err != nil {
		return perfResponse{Success: false, Error: err.Error()}
	}
	return perfResponse{Success: true}
}

// =============================================================================
// TEST CLIENT
// =============================================================================

// PerfClient is a simple client for performance testing
type PerfClient struct {
	addr      string
	tlsConfig *tls.Config
}

func NewPerfClient(addr string) *PerfClient {
	return &PerfClient{addr: addr}
}

func NewPerfClientTLS(addr string, tlsConfig *tls.Config) *PerfClient {
	return &PerfClient{addr: addr, tlsConfig: tlsConfig}
}

func (c *PerfClient) Execute(query string) (time.Duration, error) {
	start := time.Now()

	var conn net.Conn
	var err error

	if c.tlsConfig != nil {
		conn, err = tls.Dial("tcp", c.addr, c.tlsConfig)
	} else {
		conn, err = net.DialTimeout("tcp", c.addr, 5*time.Second)
	}
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	_, err = conn.Write([]byte(query + "\n"))
	if err != nil {
		return 0, err
	}

	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		return 0, err
	}

	var resp perfResponse
	if err := json.Unmarshal([]byte(line), &resp); err != nil {
		return 0, err
	}

	if !resp.Success {
		return 0, fmt.Errorf("query failed: %s", resp.Error)
	}

	return time.Since(start), nil
}

// =============================================================================
// PERFORMANCE TESTS
// =============================================================================

// TestPerfConcurrentReads tests concurrent SELECT query performance
func TestPerfConcurrentReads(t *testing.T) {
	cfg := loadPerfConfig()
	server := newPerfServer(t)
	server.Start(t)
	defer server.Stop()

	const numClients = 50
	metrics := NewPerfMetrics()
	var wg sync.WaitGroup

	// Run for configured duration
	done := make(chan struct{})
	go func() {
		time.Sleep(cfg.TestDuration)
		close(done)
	}()

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			client := NewPerfClient(server.addr)

			for {
				select {
				case <-done:
					return
				default:
				}

				query := "SELECT * FROM perf.users WHERE age > 30"
				latency, err := client.Execute(query)
				metrics.Record(latency, err)
			}
		}(i)
	}

	wg.Wait()
	metrics.Finalize()
	metrics.Print(t, numClients, cfg.TestDuration)

	// Validate thresholds
	p99Ms := float64(metrics.P99()) / float64(time.Millisecond)
	if p99Ms > float64(cfg.P99ThresholdMs) {
		t.Errorf("p99 latency %.1fms exceeds threshold %dms", p99Ms, cfg.P99ThresholdMs)
	}
	if metrics.ErrorRate() > cfg.MaxErrorRate {
		t.Errorf("error rate %.2f%% exceeds threshold %.2f%%", metrics.ErrorRate()*100, cfg.MaxErrorRate*100)
	}
}

// TestPerfConcurrentWrites tests concurrent INSERT/UPDATE performance
func TestPerfConcurrentWrites(t *testing.T) {
	cfg := loadPerfConfig()
	server := newPerfServer(t)
	server.Start(t)
	defer server.Stop()

	const numClients = 25
	metrics := NewPerfMetrics()
	var wg sync.WaitGroup
	var counter int64

	done := make(chan struct{})
	go func() {
		time.Sleep(cfg.TestDuration)
		close(done)
	}()

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			client := NewPerfClient(server.addr)

			for {
				select {
				case <-done:
					return
				default:
				}

				id := atomic.AddInt64(&counter, 1)
				query := fmt.Sprintf("INSERT INTO perf.users (id, name, age) VALUES (%d, 'NewUser%d', %d)",
					1000+id, id, 25)
				latency, err := client.Execute(query)
				metrics.Record(latency, err)
			}
		}(i)
	}

	wg.Wait()
	metrics.Finalize()
	metrics.Print(t, numClients, cfg.TestDuration)

	// Write threshold is more lenient
	writeThreshold := cfg.P99ThresholdMs * 2
	p99Ms := float64(metrics.P99()) / float64(time.Millisecond)
	if p99Ms > float64(writeThreshold) {
		t.Errorf("p99 latency %.1fms exceeds threshold %dms", p99Ms, writeThreshold)
	}
	if metrics.ErrorRate() > cfg.MaxErrorRate {
		t.Errorf("error rate %.2f%% exceeds threshold %.2f%%", metrics.ErrorRate()*100, cfg.MaxErrorRate*100)
	}
}

// TestPerfMixedWorkload tests a realistic mixed read/write workload
func TestPerfMixedWorkload(t *testing.T) {
	cfg := loadPerfConfig()
	server := newPerfServer(t)
	server.Start(t)
	defer server.Stop()

	const numClients = 50
	const readPct = 70
	metrics := NewPerfMetrics()
	var wg sync.WaitGroup
	var counter int64

	done := make(chan struct{})
	go func() {
		time.Sleep(cfg.TestDuration)
		close(done)
	}()

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			client := NewPerfClient(server.addr)

			for {
				select {
				case <-done:
					return
				default:
				}

				var query string
				if clientID%100 < readPct {
					query = "SELECT * FROM perf.users WHERE age > 30 LIMIT 10"
				} else {
					id := atomic.AddInt64(&counter, 1)
					query = fmt.Sprintf("INSERT INTO perf.users (id, name, age) VALUES (%d, 'User%d', %d)",
						10000+id, id, 30)
				}

				latency, err := client.Execute(query)
				metrics.Record(latency, err)
			}
		}(i)
	}

	wg.Wait()
	metrics.Finalize()
	metrics.Print(t, numClients, cfg.TestDuration)

	// Mixed threshold: between read and write
	mixedThreshold := int(float64(cfg.P99ThresholdMs) * 1.5)
	p99Ms := float64(metrics.P99()) / float64(time.Millisecond)
	if p99Ms > float64(mixedThreshold) {
		t.Errorf("p99 latency %.1fms exceeds threshold %dms", p99Ms, mixedThreshold)
	}
	if metrics.ErrorRate() > cfg.MaxErrorRate {
		t.Errorf("error rate %.2f%% exceeds threshold %.2f%%", metrics.ErrorRate()*100, cfg.MaxErrorRate*100)
	}
}

// TestPerfConnectionChurn tests rapid connect/disconnect cycles
func TestPerfConnectionChurn(t *testing.T) {
	cfg := loadPerfConfig()
	server := newPerfServer(t)
	server.Start(t)
	defer server.Stop()

	const numClients = 20
	metrics := NewPerfMetrics()
	var wg sync.WaitGroup

	// Track goroutines before and after
	goroutinesBefore := runtime.NumGoroutine()

	done := make(chan struct{})
	go func() {
		time.Sleep(cfg.TestDuration)
		close(done)
	}()

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				select {
				case <-done:
					return
				default:
				}

				// Each iteration creates a new connection
				client := NewPerfClient(server.addr)
				latency, err := client.Execute("SELECT COUNT(*) FROM perf.users")
				metrics.Record(latency, err)
			}
		}()
	}

	wg.Wait()
	metrics.Finalize()

	// Give goroutines time to clean up
	time.Sleep(100 * time.Millisecond)
	// Note: server.Stop() will be called by defer

	goroutinesAfter := runtime.NumGoroutine()

	t.Logf("Connection Churn Results:")
	t.Logf("├── Connections:     %d", metrics.TotalRequests)
	t.Logf("├── Throughput:      %.1f conn/s", metrics.Throughput())
	t.Logf("├── Goroutines:")
	t.Logf("│   ├── Before:      %d", goroutinesBefore)
	t.Logf("│   └── After:       %d", goroutinesAfter)
	t.Logf("└── Errors:          %d", metrics.Errors)

	// Check for goroutine leaks (allow some buffer for test infrastructure)
	if goroutinesAfter > goroutinesBefore+10 {
		t.Errorf("possible goroutine leak: before=%d, after=%d", goroutinesBefore, goroutinesAfter)
	}
}

// TestPerfTLSOverhead compares TLS vs non-TLS latency
func TestPerfTLSOverhead(t *testing.T) {
	cfg := loadPerfConfig()

	// Generate test certificates
	certFile, keyFile, cleanup := generateTestCerts(t)
	defer cleanup()

	// Run non-TLS test
	serverPlain := newPerfServer(t)
	serverPlain.Start(t)
	metricsPlain := runLatencyTest(t, serverPlain.addr, nil, cfg.TestDuration, 10)
	serverPlain.Stop()

	// Run TLS test
	serverTLS := newPerfServer(t)
	serverTLS.StartTLS(t, certFile, keyFile)
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	metricsTLS := runLatencyTest(t, serverTLS.addr, tlsConfig, cfg.TestDuration, 10)
	serverTLS.Stop()

	plainP99 := metricsPlain.P99()
	tlsP99 := metricsTLS.P99()

	var overhead float64
	if plainP99 > 0 {
		overhead = float64(tlsP99-plainP99) / float64(plainP99) * 100
	}

	t.Logf("TLS Overhead Comparison:")
	t.Logf("├── Non-TLS p99:    %s", plainP99)
	t.Logf("├── TLS p99:        %s", tlsP99)
	t.Logf("└── Overhead:       %.1f%%", overhead)

	// Only fail if overhead is significantly positive (TLS much slower)
	// Negative overhead (TLS faster) can happen due to measurement variance
	if overhead > float64(cfg.TLSOverheadPercent) {
		// In CI, log as warning but don't fail - latency variance is high
		if os.Getenv("CI") != "" {
			t.Logf("WARNING: TLS overhead %.1f%% exceeds threshold %d%% (not failing in CI due to variance)", overhead, cfg.TLSOverheadPercent)
		} else {
			t.Errorf("TLS overhead %.1f%% exceeds threshold %d%%", overhead, cfg.TLSOverheadPercent)
		}
	}
}

// TestPerfSustainedLoad runs a long-duration soak test
func TestPerfSustainedLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping soak test in short mode")
	}

	cfg := loadPerfConfig()
	// Override for soak test: 10 minutes
	soakDuration := 10 * time.Minute
	if v := os.Getenv("COMMITDB_PERF_SOAK_DURATION_MIN"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			soakDuration = time.Duration(n) * time.Minute
		}
	}

	server := newPerfServer(t)
	server.Start(t)
	defer server.Stop()

	const numClients = 20
	metrics := NewPerfMetrics()
	var wg sync.WaitGroup
	var counter int64

	// Sample memory periodically
	var memSamples []uint64
	memTicker := time.NewTicker(30 * time.Second)
	defer memTicker.Stop()

	go func() {
		for range memTicker.C {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			memSamples = append(memSamples, m.HeapAlloc)
		}
	}()

	done := make(chan struct{})
	go func() {
		time.Sleep(soakDuration)
		close(done)
	}()

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			client := NewPerfClient(server.addr)

			for {
				select {
				case <-done:
					return
				default:
				}

				var query string
				if clientID%2 == 0 {
					query = "SELECT * FROM perf.users WHERE age > 30 LIMIT 10"
				} else {
					id := atomic.AddInt64(&counter, 1)
					query = fmt.Sprintf("INSERT INTO perf.users (id, name, age) VALUES (%d, 'User%d', 30)",
						100000+id, id)
				}

				latency, err := client.Execute(query)
				metrics.Record(latency, err)
			}
		}(i)
	}

	wg.Wait()
	metrics.Finalize()

	t.Logf("Soak Test Results:")
	t.Logf("├── Duration:       %s", soakDuration)
	t.Logf("├── Requests:       %d", metrics.TotalRequests)
	t.Logf("├── Throughput:     %.1f req/s", metrics.Throughput())
	t.Logf("├── Latency p99:    %s", metrics.P99())
	t.Logf("└── Errors:         %d (%.4f%%)", metrics.Errors, metrics.ErrorRate()*100)

	if len(memSamples) >= 2 {
		first := memSamples[0]
		last := memSamples[len(memSamples)-1]
		growth := float64(last-first) / float64(first) * 100

		t.Logf("Memory:")
		t.Logf("├── Start:          %.1f MB", float64(first)/1024/1024)
		t.Logf("├── End:            %.1f MB", float64(last)/1024/1024)
		t.Logf("└── Growth:         %.1f%%", growth)

		// Warn if memory grew more than 50%
		if growth > 50 {
			t.Errorf("memory grew %.1f%% during soak test", growth)
		}
	}

	if metrics.ErrorRate() > cfg.MaxErrorRate {
		t.Errorf("error rate %.4f%% exceeds threshold %.4f%%", metrics.ErrorRate()*100, cfg.MaxErrorRate*100)
	}
}

// =============================================================================
// HELPERS
// =============================================================================

func runLatencyTest(t *testing.T, addr string, tlsConfig *tls.Config, duration time.Duration, numClients int) *PerfMetrics {
	metrics := NewPerfMetrics()
	var wg sync.WaitGroup

	done := make(chan struct{})
	go func() {
		time.Sleep(duration)
		close(done)
	}()

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			var client *PerfClient
			if tlsConfig != nil {
				client = NewPerfClientTLS(addr, tlsConfig)
			} else {
				client = NewPerfClient(addr)
			}

			for {
				select {
				case <-done:
					return
				default:
				}

				latency, err := client.Execute("SELECT COUNT(*) FROM perf.users")
				metrics.Record(latency, err)
			}
		}()
	}

	wg.Wait()
	metrics.Finalize()
	return metrics
}

func generateTestCerts(t *testing.T) (certFile, keyFile string, cleanup func()) {
	tmpDir := t.TempDir()
	certFile = tmpDir + "/cert.pem"
	keyFile = tmpDir + "/key.pem"

	// Generate private key
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("Failed to generate key: %v", err)
	}

	// Create certificate template
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"CommitDB Test"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:              []string{"localhost"},
	}

	// Create certificate
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("Failed to create certificate: %v", err)
	}

	// Write cert file
	certOut, _ := os.Create(certFile)
	pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	certOut.Close()

	// Write key file
	keyOut, _ := os.Create(keyFile)
	pem.Encode(keyOut, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})
	keyOut.Close()

	return certFile, keyFile, func() {}
}
