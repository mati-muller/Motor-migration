# SQL Server to PostgreSQL Migration Engine

A high-performance database migration tool that transfers data from SQL Server to PostgreSQL with automatic JSON column detection.

## Features

- **Two execution modes**: GUI (desktop) and CLI (headless/container)
- **Automatic JSON detection**: Analyzes text columns and converts to PostgreSQL JSONB
- **Parallel processing**: Concurrent table analysis and migration
- **Batch inserts**: High-throughput data transfer with configurable batch sizes
- **BCP support**: Optional bulk export via Microsoft BCP tools
- **S3 integration**: Export mode for very large tables (SQL Server → S3 → PostgreSQL)
- **Kubernetes ready**: Docker image and Job templates included

## Requirements

- **Rust** 1.80+ (https://rustup.rs/)
- **SQL Server** accessible via network
- **PostgreSQL** accessible via network

### System Dependencies

**macOS:**
```bash
brew install openssl
```

**Ubuntu/Debian:**
```bash
sudo apt-get install libssl-dev pkg-config
```

## Installation

```bash
git clone https://github.com/your-org/sql-migration-engine.git
cd sql-migration-engine
cargo build --release
```

## Usage

### Option 1: GUI Mode (Desktop)

```bash
# Run with GUI
cargo run --release

# Or run the compiled binary
./target/release/sql_migration_engine
```

The GUI provides:
- Connection configuration forms
- Table selection with checkboxes
- JSON column analysis preview
- Real-time migration progress

### Option 2: CLI Mode (Headless)

Perfect for automation, containers, and Kubernetes jobs.

```bash
# Using environment variables
export SQLSERVER_HOST=localhost
export SQLSERVER_DATABASE=mydb
export SQLSERVER_USER=sa
export SQLSERVER_PASSWORD=secret
export POSTGRES_HOST=localhost
export POSTGRES_DATABASE=mydb
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=secret

./target/release/sql_migration_engine --cli

# Or using .env file
cp .env.example .env
# Edit .env with your values
./target/release/sql_migration_engine --cli
```

### Option 3: Docker

```bash
# Build the image
docker build -t sql-migration-engine:latest .

# Run migration
docker run --rm \
  -e SQLSERVER_HOST=host.docker.internal \
  -e SQLSERVER_DATABASE=mydb \
  -e SQLSERVER_USER=sa \
  -e SQLSERVER_PASSWORD=secret \
  -e POSTGRES_HOST=host.docker.internal \
  -e POSTGRES_DATABASE=mydb \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=secret \
  sql-migration-engine:latest
```

### Option 4: Kubernetes Job

See the [Kubernetes Deployment](#kubernetes-deployment) section below.

## Configuration

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SQLSERVER_HOST` | Yes | - | SQL Server hostname |
| `SQLSERVER_PORT` | No | 1433 | SQL Server port |
| `SQLSERVER_DATABASE` | Yes | - | Source database name |
| `SQLSERVER_USER` | Yes | - | SQL Server username |
| `SQLSERVER_PASSWORD` | Yes | - | SQL Server password |
| `SQLSERVER_TRUST_CERT` | No | true | Trust self-signed certificates |
| `POSTGRES_HOST` | Yes | - | PostgreSQL hostname |
| `POSTGRES_PORT` | No | 5432 | PostgreSQL port |
| `POSTGRES_DATABASE` | Yes | - | Target database name |
| `POSTGRES_USER` | Yes | - | PostgreSQL username |
| `POSTGRES_PASSWORD` | Yes | - | PostgreSQL password |
| `MODE` | No | direct | Migration mode: `direct` or `export` |
| `PARALLEL_ANALYSIS` | No | 8 | Concurrent table analysis |
| `PARALLEL_MIGRATION` | No | 4 | Concurrent table migration |
| `BATCH_SIZE` | No | 5000 | Rows per INSERT batch |
| `JSON_THRESHOLD` | No | 73 | JSON detection threshold (0-100) |
| `TABLES` | No | (all) | Comma-separated table filter |

### Migration Modes

#### Direct Mode (default)
Streams data directly from SQL Server to PostgreSQL. Best for:
- Small to medium tables
- Good network connectivity
- Real-time migration

```bash
MODE=direct
```

#### Export Mode
Exports via BCP to S3, then imports to PostgreSQL. Best for:
- Very large tables (millions of rows)
- Unreliable network
- Minimizing source database load

```bash
MODE=export
S3_ENABLED=true
S3_BUCKET=my-bucket
S3_REGION=us-east-1
```

## JSON Detection

The engine automatically detects JSON content in text columns (VARCHAR, NVARCHAR, TEXT) and converts them to PostgreSQL JSONB.

### Scoring System (0-100)

| Factor | Max Points | Description |
|--------|------------|-------------|
| Data type | 40 | Longer text types score higher |
| Content | 50 | Valid JSON detected in samples |
| Consistency | 20 | Percentage of values that are valid JSON |
| Benefit | 20 | Advantage of using JSONB |

Columns scoring >= `JSON_THRESHOLD` are converted to JSONB.

## Kubernetes Deployment

### Prerequisites

1. EKS cluster (or any Kubernetes cluster)
2. Container registry (ECR, Docker Hub, etc.)
3. Network connectivity to both databases

### Build and Push Image

```bash
# Build
docker build -t your-registry/sql-migration-engine:latest .

# Push (example for ECR)
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin YOUR_ACCOUNT.dkr.ecr.us-east-1.amazonaws.com
docker tag sql-migration-engine:latest YOUR_ACCOUNT.dkr.ecr.us-east-1.amazonaws.com/sql-migration-engine:latest
docker push YOUR_ACCOUNT.dkr.ecr.us-east-1.amazonaws.com/sql-migration-engine:latest
```

### Create Secrets

```bash
kubectl create secret generic db-migration-secrets \
  --from-literal=sqlserver-user=YOUR_USER \
  --from-literal=sqlserver-password=YOUR_PASSWORD \
  --from-literal=postgres-user=YOUR_USER \
  --from-literal=postgres-password=YOUR_PASSWORD
```

### Deploy Job

```bash
# Copy and customize the example
cp k8s/migration-job.yaml.example k8s/migration-job.yaml
# Edit k8s/migration-job.yaml with your values

# Apply
kubectl apply -f k8s/migration-job.yaml

# Watch logs
kubectl logs -f job/db-migration
```

### EKS Fargate

To run on Fargate, create a Fargate profile with selector:
```yaml
selector:
  - namespace: your-namespace
    labels:
      app: db-migration
```

## Project Structure

```
.
├── src/
│   ├── main.rs              # Entry point (GUI/CLI detection)
│   ├── cli/
│   │   ├── mod.rs           # CLI module
│   │   ├── config.rs        # Environment variable parsing
│   │   └── runner.rs        # Headless migration execution
│   ├── gui/
│   │   └── app.rs           # Desktop GUI (egui)
│   ├── db/
│   │   ├── mod.rs           # Database module
│   │   ├── sqlserver.rs     # SQL Server connection
│   │   ├── postgres.rs      # PostgreSQL connection
│   │   └── types.rs         # Shared types
│   ├── migration/
│   │   └── engine.rs        # Migration engine
│   └── analysis/
│       └── json_analyzer.rs # JSON column detection
├── k8s/
│   ├── migration-job.yaml.example  # Kubernetes Job template
│   └── secrets.yaml.example        # Secrets template
├── Dockerfile               # Multi-stage build with BCP tools
├── Cargo.toml               # Rust dependencies
├── .env.example             # Environment variables template
└── README.md
```

## Performance Tips

1. **Increase batch size** for faster migration (if memory allows):
   ```bash
   BATCH_SIZE=10000
   ```

2. **Adjust parallelism** based on your hardware:
   ```bash
   PARALLEL_ANALYSIS=16
   PARALLEL_MIGRATION=8
   ```

3. **Use export mode** for very large tables to avoid timeout issues

4. **Filter tables** to migrate in batches:
   ```bash
   TABLES=dbo.Table1,dbo.Table2
   ```

## Troubleshooting

### Connection Issues

- Ensure network connectivity (firewall rules, security groups)
- For SQL Server, set `SQLSERVER_TRUST_CERT=true` if using self-signed certs
- Check credentials and database permissions

### Memory Issues

- Reduce `BATCH_SIZE` if running out of memory
- Reduce `PARALLEL_MIGRATION` for fewer concurrent operations

### Timeout Issues

- For Kubernetes, increase `activeDeadlineSeconds` in the Job spec
- Use export mode for very large tables
- Migrate large tables separately with `TABLES` filter

### JSON Conversion Errors

- PostgreSQL JSONB doesn't support `\u0000` (null character)
- Some rows may fail if they contain invalid Unicode in JSON columns
- Check logs for specific row failures

## License

MIT License - see LICENSE file for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests: `cargo test`
5. Submit a pull request
