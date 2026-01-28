//! Conexión y operaciones con SQL Server

use anyhow::{Context, Result};
use tiberius::{Client, Config, AuthMethod};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;
use std::sync::Arc;
use tokio::sync::Mutex;

use super::types::{TableInfo, ColumnInfo, ForeignKeyInfo, ConnectionConfig, map_sqlserver_to_postgres};

pub struct SqlServerConnection {
    client: Arc<Mutex<Client<tokio_util::compat::Compat<TcpStream>>>>,
}

impl SqlServerConnection {
    pub async fn connect(config: &ConnectionConfig) -> Result<Self> {
        let mut tiberius_config = Config::new();
        tiberius_config.host(&config.sqlserver_host);
        tiberius_config.port(config.sqlserver_port);
        tiberius_config.database(&config.sqlserver_database);
        tiberius_config.authentication(AuthMethod::sql_server(
            &config.sqlserver_user,
            &config.sqlserver_password,
        ));

        if config.sqlserver_trust_cert {
            tiberius_config.trust_cert();
        }

        let tcp = TcpStream::connect(format!("{}:{}", config.sqlserver_host, config.sqlserver_port))
            .await
            .context("Error conectando TCP a SQL Server")?;

        tcp.set_nodelay(true)?;

        let client = Client::connect(tiberius_config, tcp.compat_write())
            .await
            .context("Error autenticando con SQL Server")?;

        Ok(Self {
            client: Arc::new(Mutex::new(client)),
        })
    }

    /// Obtiene la lista de todas las tablas
    pub async fn get_tables(&self) -> Result<Vec<TableInfo>> {
        let query = r#"
            SELECT
                s.name AS schema_name,
                t.name AS table_name,
                p.rows AS row_count
            FROM sys.tables t
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            INNER JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0, 1)
            WHERE t.is_ms_shipped = 0
            ORDER BY s.name, t.name
        "#;

        let mut client = self.client.lock().await;
        let stream = client.query(query, &[]).await?;
        let rows = stream.into_first_result().await?;

        let mut tables = Vec::new();
        for row in rows {
            let schema: &str = row.get(0).unwrap_or("dbo");
            let name: &str = row.get(1).unwrap_or("");
            let row_count: i64 = row.get::<i64, _>(2).unwrap_or(0);

            tables.push(TableInfo {
                schema: schema.to_string(),
                name: name.to_string(),
                full_name: format!("{}.{}", schema, name),
                row_count,
                columns: Vec::new(),
                primary_keys: Vec::new(),
                foreign_keys: Vec::new(),
            });
        }

        Ok(tables)
    }

    /// Obtiene información detallada de las columnas de una tabla
    pub async fn get_columns(&self, schema: &str, table: &str) -> Result<Vec<ColumnInfo>> {
        let query = r#"
            SELECT
                c.name AS column_name,
                t.name AS data_type,
                c.max_length,
                c.precision,
                c.scale,
                c.is_nullable,
                c.is_identity,
                dc.definition AS default_value
            FROM sys.columns c
            INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
            INNER JOIN sys.tables tb ON c.object_id = tb.object_id
            INNER JOIN sys.schemas s ON tb.schema_id = s.schema_id
            LEFT JOIN sys.default_constraints dc ON c.default_object_id = dc.object_id
            WHERE s.name = @P1 AND tb.name = @P2
            ORDER BY c.column_id
        "#;

        let mut client = self.client.lock().await;
        let stream = client.query(query, &[&schema, &table]).await?;
        let rows = stream.into_first_result().await?;

        let mut columns = Vec::new();
        for row in rows {
            let name: &str = row.get(0).unwrap_or("");
            let data_type: &str = row.get(1).unwrap_or("");
            let max_length: i16 = row.get(2).unwrap_or(0);
            let precision: u8 = row.get(3).unwrap_or(0);
            let scale: u8 = row.get(4).unwrap_or(0);
            let is_nullable: bool = row.get(5).unwrap_or(false);
            let is_identity: bool = row.get(6).unwrap_or(false);
            let default_value: Option<&str> = row.get(7);

            let pg_type = map_sqlserver_to_postgres(data_type, Some(max_length as i32));

            columns.push(ColumnInfo {
                name: name.to_string(),
                data_type: data_type.to_string(),
                max_length: Some(max_length as i32),
                precision: Some(precision as i32),
                scale: Some(scale as i32),
                is_nullable,
                is_identity,
                default_value: default_value.map(|s| s.to_string()),
                should_convert_to_json: false,
                json_score: 0,
                json_reason: String::new(),
                pg_type,
            });
        }

        Ok(columns)
    }

    /// Obtiene las claves primarias de una tabla
    pub async fn get_primary_keys(&self, schema: &str, table: &str) -> Result<Vec<String>> {
        let query = r#"
            SELECT c.name
            FROM sys.indexes i
            INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
            INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            INNER JOIN sys.tables t ON i.object_id = t.object_id
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE i.is_primary_key = 1 AND s.name = @P1 AND t.name = @P2
            ORDER BY ic.key_ordinal
        "#;

        let mut client = self.client.lock().await;
        let stream = client.query(query, &[&schema, &table]).await?;
        let rows = stream.into_first_result().await?;

        let pks: Vec<String> = rows
            .iter()
            .filter_map(|row| row.get::<&str, _>(0).map(|s| s.to_string()))
            .collect();

        Ok(pks)
    }

    /// Obtiene las claves foráneas de una tabla
    pub async fn get_foreign_keys(&self, schema: &str, table: &str) -> Result<Vec<ForeignKeyInfo>> {
        let query = r#"
            SELECT
                fk.name AS fk_name,
                c.name AS column_name,
                OBJECT_SCHEMA_NAME(fk.referenced_object_id) + '.' + OBJECT_NAME(fk.referenced_object_id) AS referenced_table,
                rc.name AS referenced_column
            FROM sys.foreign_keys fk
            INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
            INNER JOIN sys.columns c ON fkc.parent_object_id = c.object_id AND fkc.parent_column_id = c.column_id
            INNER JOIN sys.columns rc ON fkc.referenced_object_id = rc.object_id AND fkc.referenced_column_id = rc.column_id
            INNER JOIN sys.tables t ON fk.parent_object_id = t.object_id
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = @P1 AND t.name = @P2
        "#;

        let mut client = self.client.lock().await;
        let stream = client.query(query, &[&schema, &table]).await?;
        let rows = stream.into_first_result().await?;

        let fks: Vec<ForeignKeyInfo> = rows
            .iter()
            .map(|row| ForeignKeyInfo {
                name: row.get::<&str, _>(0).unwrap_or("").to_string(),
                column: row.get::<&str, _>(1).unwrap_or("").to_string(),
                referenced_table: row.get::<&str, _>(2).unwrap_or("").to_string(),
                referenced_column: row.get::<&str, _>(3).unwrap_or("").to_string(),
            })
            .collect();

        Ok(fks)
    }

    /// Obtiene una muestra de datos de una columna para análisis JSON
    /// Solo trae datos NO NULL para análisis más preciso
    pub async fn sample_column_for_json(&self, schema: &str, table: &str, column: &str, sample_size: u32) -> Result<Vec<Option<String>>> {
        let query = format!(
            "SELECT TOP {} CAST([{}] AS NVARCHAR(MAX)) FROM [{}].[{}] WITH (NOLOCK) WHERE [{}] IS NOT NULL AND LEN(CAST([{}] AS NVARCHAR(MAX))) > 0",
            sample_size, column, schema, table, column, column
        );

        let mut client = self.client.lock().await;
        let stream = client.query(&query, &[]).await?;
        let rows = stream.into_first_result().await?;

        let samples: Vec<Option<String>> = rows
            .iter()
            .map(|row| {
                match row.get::<&str, _>(0) {
                    Some(s) if !s.is_empty() => Some(s.to_string()),
                    _ => None,
                }
            })
            .collect();

        Ok(samples)
    }

    /// Lee filas de una tabla con paginación rápida
    /// Usa NOLOCK para lecturas sin bloqueo
    pub async fn read_rows(&self, schema: &str, table: &str, columns: &[String], offset: i64, limit: i64) -> Result<Vec<Vec<Option<String>>>> {
        if columns.is_empty() {
            return Ok(Vec::new());
        }

        let columns_str = columns.iter()
            .map(|c| format!("CAST([{}] AS NVARCHAR(MAX))", c))
            .collect::<Vec<_>>()
            .join(", ");

        // Query con NOLOCK para lectura rápida sin bloqueos
        let query = format!(
            "SELECT {} FROM [{}].[{}] WITH (NOLOCK) ORDER BY (SELECT NULL) OFFSET {} ROWS FETCH NEXT {} ROWS ONLY",
            columns_str, schema, table, offset, limit
        );

        let mut client = self.client.lock().await;
        let stream = client.query(&query, &[]).await?;
        let rows = stream.into_first_result().await?;

        let result: Vec<Vec<Option<String>>> = rows
            .into_iter()
            .map(|row| {
                (0..columns.len())
                    .map(|i| row.get::<&str, _>(i).map(|s| s.to_string()))
                    .collect()
            })
            .collect();

        Ok(result)
    }

    /// Verifica la conexión
    pub async fn test_connection(&self) -> Result<()> {
        let mut client = self.client.lock().await;
        let stream = client.query("SELECT 1", &[]).await?;
        let _ = stream.into_first_result().await?;
        Ok(())
    }
}
