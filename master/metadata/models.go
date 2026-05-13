package metadata

type Table struct {
	DBName      string
	TableName   string
	SchemaSQL   string
	ShardKey    string
	ShardCount  int
	ReplicaFact int
}

type Shard struct {
	DBName    string
	TableName string
	ShardID   int
	WorkerID  string
	IsPrimary bool
}

type Worker struct {
	ID       string
	URL      string
	Tech     string
	LastSeen int64
}
