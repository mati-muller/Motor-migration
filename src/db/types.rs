//! Tipos de datos compartidos para la migración

use serde::{Deserialize, Serialize};

/// Información de una tabla en la base de datos
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableInfo {
    pub schema: String,
    pub name: String,
    pub full_name: String,
    pub row_count: i64,
    pub columns: Vec<ColumnInfo>,
    pub primary_keys: Vec<String>,
    pub foreign_keys: Vec<ForeignKeyInfo>,
}

/// Información de una columna
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub max_length: Option<i32>,
    pub precision: Option<i32>,
    pub scale: Option<i32>,
    pub is_nullable: bool,
    pub is_identity: bool,
    pub default_value: Option<String>,
    /// Indica si esta columna debería convertirse a JSON
    pub should_convert_to_json: bool,
    /// Puntuación de evaluación JSON (0-100)
    pub json_score: u8,
    /// Razón de la recomendación JSON
    pub json_reason: String,
    /// Tipo PostgreSQL mapeado
    pub pg_type: String,
}

/// Información de clave foránea
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKeyInfo {
    pub name: String,
    pub column: String,
    pub referenced_table: String,
    pub referenced_column: String,
}

/// Resultado del análisis de una columna para JSON
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonAnalysisResult {
    pub column_name: String,
    pub should_convert: bool,
    pub score: u8,
    pub reason: String,
    pub sample_valid_json: u32,
    pub sample_invalid_json: u32,
    pub sample_null: u32,
    pub sample_total: u32,
}

/// Estado de migración de una tabla
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationStatus {
    Pending,
    Analyzing,
    Analyzed,
    Migrating,
    Completed,
    Failed(String),
    Skipped,
}

/// Información de progreso de migración
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationProgress {
    pub table_name: String,
    pub status: MigrationStatus,
    pub total_rows: i64,
    pub migrated_rows: i64,
    pub json_conversions: u32,
    pub json_nulled: u32,
    pub errors: Vec<String>,
    pub start_time: Option<chrono::DateTime<chrono::Utc>>,
    pub end_time: Option<chrono::DateTime<chrono::Utc>>,
}

impl MigrationProgress {
    pub fn new(table_name: String, total_rows: i64) -> Self {
        Self {
            table_name,
            status: MigrationStatus::Pending,
            total_rows,
            migrated_rows: 0,
            json_conversions: 0,
            json_nulled: 0,
            errors: Vec::new(),
            start_time: None,
            end_time: None,
        }
    }

    pub fn percentage(&self) -> f32 {
        if self.total_rows == 0 {
            100.0
        } else {
            (self.migrated_rows as f32 / self.total_rows as f32) * 100.0
        }
    }

    pub fn elapsed(&self) -> Option<chrono::Duration> {
        match (self.start_time, self.end_time) {
            (Some(start), Some(end)) => Some(end - start),
            (Some(start), None) => Some(chrono::Utc::now() - start),
            _ => None,
        }
    }
}

/// Mapeo de tipos SQL Server a PostgreSQL
pub fn map_sqlserver_to_postgres(sql_type: &str, max_length: Option<i32>) -> String {
    let sql_type_lower = sql_type.to_lowercase();

    match sql_type_lower.as_str() {
        // Numéricos exactos
        "bigint" => "BIGINT".to_string(),
        "int" | "integer" => "INTEGER".to_string(),
        "smallint" => "SMALLINT".to_string(),
        "tinyint" => "SMALLINT".to_string(), // PostgreSQL no tiene TINYINT
        "bit" => "BOOLEAN".to_string(),
        "decimal" | "numeric" => "NUMERIC".to_string(),
        "money" => "NUMERIC(19,4)".to_string(),
        "smallmoney" => "NUMERIC(10,4)".to_string(),

        // Numéricos aproximados
        "float" => "DOUBLE PRECISION".to_string(),
        "real" => "REAL".to_string(),

        // Fecha y hora
        "date" => "DATE".to_string(),
        "time" => "TIME".to_string(),
        "datetime" | "datetime2" => "TIMESTAMP".to_string(),
        "smalldatetime" => "TIMESTAMP".to_string(),
        "datetimeoffset" => "TIMESTAMPTZ".to_string(),

        // Caracteres
        "char" | "nchar" => {
            match max_length {
                Some(len) if len > 0 => format!("CHAR({})", len),
                _ => "CHAR(1)".to_string(),
            }
        }
        "varchar" | "nvarchar" => {
            match max_length {
                Some(-1) => "TEXT".to_string(), // MAX
                Some(len) if len > 0 => format!("VARCHAR({})", len),
                _ => "TEXT".to_string(),
            }
        }
        "text" | "ntext" => "TEXT".to_string(),

        // Binarios
        "binary" => {
            match max_length {
                Some(len) if len > 0 => format!("BYTEA"), // PostgreSQL usa BYTEA para todos
                _ => "BYTEA".to_string(),
            }
        }
        "varbinary" | "image" => "BYTEA".to_string(),

        // XML
        "xml" => "XML".to_string(),

        // UUID
        "uniqueidentifier" => "UUID".to_string(),

        // Otros
        "sql_variant" => "TEXT".to_string(),
        "hierarchyid" => "TEXT".to_string(),
        "geography" | "geometry" => "TEXT".to_string(), // Requiere PostGIS para soporte real

        // Por defecto
        _ => "TEXT".to_string(),
    }
}

/// Configuración de conexión
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ConnectionConfig {
    pub sqlserver_host: String,
    pub sqlserver_port: u16,
    pub sqlserver_database: String,
    pub sqlserver_user: String,
    pub sqlserver_password: String,
    pub sqlserver_trust_cert: bool,

    pub postgres_host: String,
    pub postgres_port: u16,
    pub postgres_database: String,
    pub postgres_user: String,
    pub postgres_password: String,

    // S3 Configuration (optional)
    pub s3_enabled: bool,
    pub s3_bucket: String,
    pub s3_region: String,
    pub s3_access_key: String,
    pub s3_secret_key: String,
    pub s3_prefix: String,

    // BCP Configuration
    pub bcp_use_docker: bool,
    pub bcp_docker_image: String,
}

impl ConnectionConfig {
    pub fn sqlserver_connection_string(&self) -> String {
        format!(
            "Server={},{};Database={};User Id={};Password={};TrustServerCertificate={}",
            self.sqlserver_host,
            self.sqlserver_port,
            self.sqlserver_database,
            self.sqlserver_user,
            self.sqlserver_password,
            if self.sqlserver_trust_cert { "true" } else { "false" }
        )
    }

    pub fn postgres_connection_string(&self) -> String {
        format!(
            "host={} port={} dbname={} user={} password={}",
            self.postgres_host,
            self.postgres_port,
            self.postgres_database,
            self.postgres_user,
            self.postgres_password
        )
    }
}
