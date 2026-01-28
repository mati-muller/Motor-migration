//! Conexión y operaciones con PostgreSQL

use anyhow::{Context, Result};
use tokio_postgres::{Client, NoTls, types::ToSql};
use std::sync::Arc;
use tokio::sync::Mutex;
use serde_json::Value as JsonValue;

use super::types::{TableInfo, ColumnInfo, ConnectionConfig};

pub struct PostgresConnection {
    client: Arc<Mutex<Client>>,
}

impl PostgresConnection {
    pub async fn connect(config: &ConnectionConfig) -> Result<Self> {
        let conn_string = format!(
            "host={} port={} dbname={} user={} password={}",
            config.postgres_host,
            config.postgres_port,
            config.postgres_database,
            config.postgres_user,
            config.postgres_password
        );

        let (client, connection) = tokio_postgres::connect(&conn_string, NoTls)
            .await
            .context("Error conectando a PostgreSQL")?;

        // Spawn la conexión en background
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("Error en conexión PostgreSQL: {}", e);
            }
        });

        Ok(Self {
            client: Arc::new(Mutex::new(client)),
        })
    }

    /// Crea una tabla en PostgreSQL
    pub async fn create_table(&self, table: &TableInfo) -> Result<()> {
        let mut columns_sql = Vec::new();

        for col in &table.columns {
            let mut col_def = format!(
                "\"{}\" {}",
                col.name,
                if col.should_convert_to_json { "JSONB" } else { &col.pg_type }
            );

            if !col.is_nullable && !col.should_convert_to_json {
                col_def.push_str(" NOT NULL");
            }

            if col.is_identity {
                col_def = format!("\"{}\" SERIAL", col.name);
                if !col.is_nullable {
                    col_def.push_str(" NOT NULL");
                }
            }

            columns_sql.push(col_def);
        }

        // Agregar clave primaria
        if !table.primary_keys.is_empty() {
            let pk_cols: Vec<String> = table.primary_keys.iter()
                .map(|pk| format!("\"{}\"", pk))
                .collect();
            columns_sql.push(format!("PRIMARY KEY ({})", pk_cols.join(", ")));
        }

        // Crear esquema si no existe
        let create_schema = format!(
            "CREATE SCHEMA IF NOT EXISTS \"{}\"",
            table.schema
        );

        let create_table = format!(
            "CREATE TABLE IF NOT EXISTS \"{}\".\"{}\" (\n    {}\n)",
            table.schema,
            table.name,
            columns_sql.join(",\n    ")
        );

        let client = self.client.lock().await;
        client.execute(&create_schema, &[]).await?;
        client.execute(&create_table, &[]).await?;

        Ok(())
    }

    /// Inserta filas en una tabla
    pub async fn insert_rows(
        &self,
        schema: &str,
        table: &str,
        columns: &[ColumnInfo],
        rows: Vec<Vec<DynamicValue>>,
    ) -> Result<InsertResult> {
        let client = self.client.lock().await;

        let col_names: Vec<String> = columns.iter()
            .map(|c| format!("\"{}\"", c.name))
            .collect();

        let placeholders: Vec<String> = (1..=columns.len())
            .map(|i| format!("${}", i))
            .collect();

        let insert_sql = format!(
            "INSERT INTO \"{}\".\"{}\" ({}) VALUES ({})",
            schema,
            table,
            col_names.join(", "),
            placeholders.join(", ")
        );

        let mut result = InsertResult::default();

        for row in rows {
            let params: Vec<&(dyn ToSql + Sync)> = row.iter()
                .map(|v| v as &(dyn ToSql + Sync))
                .collect();

            match client.execute(&insert_sql, &params).await {
                Ok(_) => result.inserted += 1,
                Err(e) => {
                    result.errors.push(format!("Error insertando fila: {}", e));
                    result.failed += 1;
                }
            }
        }

        Ok(result)
    }

    /// Inserta filas usando COPY para mayor velocidad
    pub async fn bulk_insert(
        &self,
        schema: &str,
        table: &str,
        columns: &[ColumnInfo],
        rows: &[Vec<String>],
    ) -> Result<u64> {
        if rows.is_empty() {
            return Ok(0);
        }

        let client = self.client.lock().await;

        let col_names: Vec<String> = columns.iter()
            .map(|c| format!("\"{}\"", c.name))
            .collect();

        // Construir INSERT masivo
        let mut values_parts = Vec::new();
        let mut param_idx = 1;
        let mut all_params: Vec<String> = Vec::new();

        for row in rows {
            let row_placeholders: Vec<String> = row.iter()
                .map(|_| {
                    let p = format!("${}", param_idx);
                    param_idx += 1;
                    p
                })
                .collect();
            values_parts.push(format!("({})", row_placeholders.join(", ")));
            all_params.extend(row.iter().cloned());
        }

        let insert_sql = format!(
            "INSERT INTO \"{}\".\"{}\" ({}) VALUES {}",
            schema,
            table,
            col_names.join(", "),
            values_parts.join(", ")
        );

        let params: Vec<&(dyn ToSql + Sync)> = all_params.iter()
            .map(|s| s as &(dyn ToSql + Sync))
            .collect();

        let affected = client.execute(&insert_sql, &params).await?;
        Ok(affected)
    }

    /// Verifica la conexión
    pub async fn test_connection(&self) -> Result<()> {
        let client = self.client.lock().await;
        client.query("SELECT 1", &[]).await?;
        Ok(())
    }

    /// Verifica si una tabla existe
    pub async fn table_exists(&self, schema: &str, table: &str) -> Result<bool> {
        let client = self.client.lock().await;
        let query = "SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_schema = $1 AND table_name = $2
        )";
        let row = client.query_one(query, &[&schema, &table]).await?;
        let exists: bool = row.get(0);
        Ok(exists)
    }

    /// Obtiene el conteo de filas de una tabla
    pub async fn get_row_count(&self, schema: &str, table: &str) -> Result<i64> {
        let client = self.client.lock().await;
        let query = format!("SELECT COUNT(*) FROM \"{}\".\"{}\"", schema, table);
        let row = client.query_one(&query, &[]).await?;
        let count: i64 = row.get(0);
        Ok(count)
    }

    /// Trunca una tabla (solo si el usuario lo solicita explícitamente)
    #[allow(dead_code)]
    pub async fn truncate_table(&self, schema: &str, table: &str) -> Result<()> {
        let client = self.client.lock().await;
        let query = format!("TRUNCATE TABLE \"{}\".\"{}\";", schema, table);
        client.execute(&query, &[]).await?;
        Ok(())
    }
}

/// Valor dinámico para inserción
#[derive(Debug, Clone)]
pub enum DynamicValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Json(JsonValue),
    Bytes(Vec<u8>),
    Uuid(uuid::Uuid),
    Timestamp(chrono::NaiveDateTime),
    TimestampTz(chrono::DateTime<chrono::Utc>),
    Date(chrono::NaiveDate),
    Time(chrono::NaiveTime),
}

impl ToSql for DynamicValue {
    fn to_sql(
        &self,
        ty: &tokio_postgres::types::Type,
        out: &mut bytes::BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match self {
            DynamicValue::Null => Ok(tokio_postgres::types::IsNull::Yes),
            DynamicValue::Bool(v) => v.to_sql(ty, out),
            DynamicValue::Int(v) => v.to_sql(ty, out),
            DynamicValue::Float(v) => v.to_sql(ty, out),
            DynamicValue::Text(v) => v.to_sql(ty, out),
            DynamicValue::Json(v) => v.to_sql(ty, out),
            DynamicValue::Bytes(v) => v.to_sql(ty, out),
            DynamicValue::Uuid(v) => v.to_sql(ty, out),
            DynamicValue::Timestamp(v) => v.to_sql(ty, out),
            DynamicValue::TimestampTz(v) => v.to_sql(ty, out),
            DynamicValue::Date(v) => v.to_sql(ty, out),
            DynamicValue::Time(v) => v.to_sql(ty, out),
        }
    }

    fn accepts(_ty: &tokio_postgres::types::Type) -> bool {
        true
    }

    tokio_postgres::types::to_sql_checked!();
}

#[derive(Debug, Default)]
pub struct InsertResult {
    pub inserted: u64,
    pub failed: u64,
    pub errors: Vec<String>,
}
