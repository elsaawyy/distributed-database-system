package main

import (
	"database/sql"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"time"

	"distributed-db/master/api"
	"distributed-db/master/coordinator"
	"distributed-db/master/election"
	"distributed-db/master/health"
	"distributed-db/master/metadata"
	"distributed-db/master/replication"
	"distributed-db/master/sharding"
	"distributed-db/master/ui"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/mux"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Server struct {
		Port string `yaml:"port"`
	} `yaml:"server"`
	MetadataDB struct {
		Host     string `yaml:"host"`
		Port     int    `yaml:"port"`
		User     string `yaml:"user"`
		Password string `yaml:"password"`
		DBName   string `yaml:"dbname"`
	} `yaml:"metadata_db"`
	Workers []struct {
		ID   string `yaml:"id"`
		URL  string `yaml:"url"`
		Tech string `yaml:"tech"`
	} `yaml:"workers"`
	Reducer struct {
		URL string `yaml:"url"`
	} `yaml:"reducer"`
	Auth struct {
		APIKey string `yaml:"api_key"`
	} `yaml:"auth"`
	HeartbeatInterval int `yaml:"heartbeat_interval"`
	ReplicationFactor int `yaml:"replication_factor"`
	ShardCount        int `yaml:"shard_count"`
	ChunkSizeKB       int `yaml:"chunk_size_kb"`
}

func main() {
	// Read config
	data, err := os.ReadFile("config.yaml")
	if err != nil {
		log.Fatal("Cannot read config:", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		log.Fatal("Cannot parse config:", err)
	}

	// Check if running as backup (via environment variable)
	if os.Getenv("PORT") != "" {
		cfg.Server.Port = ":" + os.Getenv("PORT")
		log.Printf("Running on custom port: %s", cfg.Server.Port)
	}

	// Connect to metadata DB
	dsn := cfg.MetadataDB.User + ":" + cfg.MetadataDB.Password +
		"@tcp(" + cfg.MetadataDB.Host + ":" + strconv.Itoa(cfg.MetadataDB.Port) +
		")/" + cfg.MetadataDB.DBName + "?parseTime=true"

	log.Printf("Connecting to metadata DB with DSN: %s:***@tcp(%s:%d)/%s",
		cfg.MetadataDB.User, cfg.MetadataDB.Host, cfg.MetadataDB.Port, cfg.MetadataDB.DBName)

	metaDB, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal("Metadata DB connection error:", err)
	}
	defer metaDB.Close()
	metaDB.SetMaxOpenConns(10)
	metaDB.SetMaxIdleConns(5)

	// Test connection
	if err := metaDB.Ping(); err != nil {
		log.Fatal("Cannot ping metadata DB:", err)
	}
	log.Println("Connected to metadata database successfully")

	// Leader election
	leader := election.NewLeaderElection(metaDB)
	if !leader.TryAcquire() {
		log.Println("Standby mode – another master is active")
		go leader.AcquireLoop()
	} else {
		log.Println("Active master")
		go leader.KeepAliveLoop()
	}

	// Wait until we become active (or stay standby forever)
	for !leader.IsActive() {
		time.Sleep(1 * time.Second)
	}
	log.Println("This instance is now the active master")

	// Metadata manager
	metaManager := metadata.NewManager(metaDB)

	// Worker map
	workerMap := make(map[string]health.WorkerInfo)
	for _, w := range cfg.Workers {
		workerMap[w.ID] = health.WorkerInfo{
			ID:    w.ID,
			URL:   w.URL,
			Tech:  w.Tech,
			Alive: true,
		}
	}

	// Health monitor
	healthMon := health.NewMonitor(workerMap, time.Duration(cfg.HeartbeatInterval)*time.Second)
	go healthMon.Start()

	// Sharding utilities
	consistentHash := sharding.NewConsistentHash(cfg.ShardCount)
	chunkSplitter := sharding.NewChunkSplitter(cfg.ChunkSizeKB * 1024)

	// Replica manager
	replicaMgr := replication.NewReplicaManager(metaManager, healthMon, cfg.ReplicationFactor)

	// Coordinator
	coord := coordinator.NewCoordinator(metaManager, healthMon, replicaMgr, consistentHash,
		cfg.Reducer.URL, cfg.ReplicationFactor)

	// HTTP router
	r := mux.NewRouter()

	// API routes
	apiHandler := api.NewHandler(coord, metaManager, healthMon, chunkSplitter, consistentHash,
		cfg.Auth.APIKey, replicaMgr)
	apiHandler.RegisterRoutes(r)

	// UI routes
	uiHandler := ui.NewUIHandler("http://localhost"+cfg.Server.Port, cfg.Auth.APIKey)
	r.HandleFunc("/ui", uiHandler.ServeIndex)
	r.HandleFunc("/ui/api/workers", uiHandler.GetWorkers)
	r.HandleFunc("/ui/api/stats", uiHandler.GetStats)
	r.HandleFunc("/ui/api/create-db", uiHandler.CreateDatabase)
	r.HandleFunc("/ui/api/create-table", uiHandler.CreateTable)
	r.HandleFunc("/ui/api/insert", uiHandler.InsertData)
	r.HandleFunc("/ui/api/select", uiHandler.ExecuteQuery)
	r.HandleFunc("/ui/api/mapreduce", uiHandler.RunMapReduce)

	// Start server
	srv := &http.Server{
		Addr:    cfg.Server.Port,
		Handler: r,
	}
	go func() {
		log.Printf("Master API listening on %s", cfg.Server.Port)
		log.Printf("UI available at http://localhost%s/ui", cfg.Server.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit
	log.Println("Shutting down master...")
	healthMon.Stop()
	leader.Stop()
	srv.Close()
}
