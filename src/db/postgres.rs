//! Conexión y operaciones con PostgreSQL

use anyhow::{Context, Result};
use tokio_postgres::{Client, NoTls, types::ToSql};
use std::sync::Arc;
use tokio::sync::Mutex;
use serde_json::Value as JsonValue;

use super::types::{TableInfo, ColumnInfo};

pub struct PostgresConnection {
    client: Arc<Mutex<Client>>,
}

impl PostgresConnection {
    pub async fn connect(config: &super::ConnectionConfig) -> Result<Self> {
        // Primero intentar crear la base de datos si no existe
        Self::ensure_database_exists(config).await?;

        // Conectar a la base de datos destino
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

    /// Verifica si la base de datos existe y la crea si no
    async fn ensure_database_exists(config: &super::ConnectionConfig) -> Result<()> {
        // Conectar a la base de datos 'postgres' (siempre existe)
        let admin_conn_string = format!(
            "host={} port={} dbname=postgres user={} password={}",
            config.postgres_host,
            config.postgres_port,
            config.postgres_user,
            config.postgres_password
        );

        let (client, connection) = tokio_postgres::connect(&admin_conn_string, NoTls)
            .await
            .context("Error conectando a PostgreSQL (admin)")?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("Error en conexión admin PostgreSQL: {}", e);
            }
        });

        // Verificar si la base de datos existe
        let db_name = &config.postgres_database;
        let check_query = "SELECT 1 FROM pg_database WHERE datname = $1";
        let rows = client.query(check_query, &[db_name]).await?;

        if rows.is_empty() {
            // La base de datos no existe, crearla
            // Nota: CREATE DATABASE no acepta parámetros, hay que usar formato directo
            // pero escapamos el nombre para seguridad
            let safe_db_name = db_name.replace('"', "\"\"");
            let create_query = format!("CREATE DATABASE \"{}\"", safe_db_name);

            client.execute(&create_query, &[]).await
                .context(format!("Error creando base de datos '{}'", db_name))?;

            tracing::info!("Base de datos '{}' creada exitosamente", db_name);
        } else {
            tracing::info!("Base de datos '{}' ya existe", db_name);
        }

        Ok(())
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

            // No poner NOT NULL para evitar problemas con datos faltantes
            if col.is_identity {
                col_def = format!("\"{}\" SERIAL", col.name);
            }

            columns_sql.push(col_def);
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

        tracing::info!("Tabla creada: {}.{}", table.schema, table.name);

        Ok(())
    }

    /// Inserción optimizada por lotes con fallback a filas individuales
    /// Usa sub-batches para evitar límites de tamaño SQL
    pub async fn insert_rows(
        &self,
        schema: &str,
        table: &str,
        columns: &[ColumnInfo],
        rows: Vec<Vec<DynamicValue>>,
    ) -> Result<InsertResult> {
        if rows.is_empty() {
            return Ok(InsertResult::default());
        }

        if columns.is_empty() {
            tracing::error!("insert_rows: No hay columnas definidas para {}.{}", schema, table);
            return Ok(InsertResult::default());
        }

        let client = self.client.lock().await;

        let col_names: Vec<String> = columns.iter()
            .map(|c| format!("\"{}\"", c.name))
            .collect();
        let col_names_str = col_names.join(", ");

        let mut result = InsertResult::default();

        // Tamaño de sub-batch (100 filas por INSERT para balance entre velocidad y tamaño SQL)
        const SUB_BATCH_SIZE: usize = 100;

        // Procesar en sub-batches
        for chunk in rows.chunks(SUB_BATCH_SIZE) {
            // Construir INSERT con múltiples VALUES
            let mut all_values: Vec<String> = Vec::with_capacity(chunk.len());

            for row in chunk {
                let row_values: Vec<String> = row.iter()
                    .enumerate()
                    .map(|(i, val)| val.to_sql_string(columns[i].should_convert_to_json))
                    .collect();
                all_values.push(format!("({})", row_values.join(",")));
            }

            let insert_sql = format!(
                "INSERT INTO \"{}\".\"{}\" ({}) VALUES {}",
                schema, table, col_names_str, all_values.join(",")
            );

            // Intentar batch insert
            match client.execute(&insert_sql, &[]).await {
                Ok(count) => {
                    result.inserted += count;
                }
                Err(batch_err) => {
                    // Si falla el batch, intentar fila por fila para identificar el problema
                    tracing::warn!("Batch insert falló: {:?}", batch_err);
                    tracing::warn!("SQL (primeros 500 chars): {}", &insert_sql[..insert_sql.len().min(500)]);

                    for row in chunk {
                        let row_values: Vec<String> = row.iter()
                            .enumerate()
                            .map(|(i, val)| val.to_sql_string(columns[i].should_convert_to_json))
                            .collect();

                        let single_sql = format!(
                            "INSERT INTO \"{}\".\"{}\" ({}) VALUES ({})",
                            schema, table, col_names_str, row_values.join(",")
                        );

                        match client.execute(&single_sql, &[]).await {
                            Ok(_) => result.inserted += 1,
                            Err(e) => {
                                if result.errors.len() < 5 {
                                    result.errors.push(format!("{}", e));
                                }
                                result.failed += 1;
                            }
                        }
                    }
                }
            }
        }

        Ok(result)
    }

    /// Verifica la conexión
    pub async fn test_connection(&self) -> Result<()> {
        let client = self.client.lock().await;
        client.query("SELECT 1", &[]).await?;
        Ok(())
    }

    /// Verifica si una tabla existe
    #[allow(dead_code)]
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
    #[allow(dead_code)]
    pub async fn get_row_count(&self, schema: &str, table: &str) -> Result<i64> {
        let client = self.client.lock().await;
        let query = format!("SELECT COUNT(*) FROM \"{}\".\"{}\"", schema, table);
        let row = client.query_one(&query, &[]).await?;
        let count: i64 = row.get(0);
        Ok(count)
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
}

impl DynamicValue {
    /// Convierte el valor a string SQL escapado
    pub fn to_sql_string(&self, is_json_column: bool) -> String {
        match self {
            DynamicValue::Null => "NULL".to_string(),
            DynamicValue::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
            DynamicValue::Int(i) => i.to_string(),
            DynamicValue::Float(f) => {
                if f.is_nan() || f.is_infinite() {
                    "NULL".to_string()
                } else {
                    f.to_string()
                }
            }
            DynamicValue::Text(s) => {
                // Remover bytes nulos y escapar comillas simples
                let sanitized = s.replace('\0', "");
                let escaped = sanitized.replace('\'', "''");
                format!("'{}'", escaped)
            }
            DynamicValue::Json(j) => {
                // Remover bytes nulos del JSON serializado
                let json_str = j.to_string().replace('\0', "");
                if is_json_column {
                    let escaped = json_str.replace('\'', "''");
                    format!("'{}'::jsonb", escaped)
                } else {
                    let escaped = json_str.replace('\'', "''");
                    format!("'{}'", escaped)
                }
            }
        }
    }
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
