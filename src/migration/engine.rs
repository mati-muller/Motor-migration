//! Motor de migración principal
//!
//! Maneja la migración de tablas de SQL Server a PostgreSQL
//! con análisis paralelo y conversión inteligente a JSON.

use anyhow::{Context, Result};
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use std::collections::HashMap;
use futures::future::join_all;
use chrono::Utc;

use crate::db::{SqlServerConnection, PostgresConnection, ConnectionConfig};
use crate::db::types::{TableInfo, ColumnInfo, MigrationProgress, MigrationStatus};
use crate::db::postgres::DynamicValue;
use crate::analysis::JsonAnalyzer;

/// Configuración del motor de migración
#[derive(Debug, Clone)]
pub struct MigrationConfig {
    /// Número de tablas a analizar en paralelo
    pub parallel_analysis: usize,
    /// Número de tablas a migrar en paralelo
    pub parallel_migration: usize,
    /// Tamaño de lote para inserción
    pub batch_size: i64,
    /// Tamaño de muestra para análisis JSON
    pub json_sample_size: u32,
    /// Umbral para recomendar JSON (0-100)
    pub json_threshold: u8,
}

impl Default for MigrationConfig {
    fn default() -> Self {
        Self {
            parallel_analysis: 8,   // 8 tablas analizando en paralelo
            parallel_migration: 4,  // 4 tablas migrando en paralelo
            batch_size: 5000,       // 5000 filas por lote (más rápido)
            json_sample_size: 100,
            json_threshold: 73,     // Umbral para convertir a JSONB
        }
    }
}

/// Evento de progreso para la GUI
#[derive(Debug, Clone)]
pub enum MigrationEvent {
    /// Conexión exitosa, envía lista de tablas
    ConnectionSuccess(Vec<TableInfo>),
    AnalysisStarted(String),
    AnalysisComplete(String, Vec<ColumnInfo>),
    MigrationStarted(String),
    MigrationProgress(String, i64, i64),
    MigrationComplete(String),
    MigrationError(String, String),
    AllComplete,
}

/// Motor de migración
pub struct MigrationEngine {
    config: MigrationConfig,
    conn_config: ConnectionConfig,
    sqlserver: Option<Arc<SqlServerConnection>>,
    postgres: Option<Arc<PostgresConnection>>,
    tables: Arc<RwLock<Vec<TableInfo>>>,
    progress: Arc<RwLock<HashMap<String, MigrationProgress>>>,
    event_sender: Option<mpsc::UnboundedSender<MigrationEvent>>,
}

impl MigrationEngine {
    pub fn new(conn_config: ConnectionConfig, config: MigrationConfig) -> Self {
        Self {
            config,
            conn_config,
            sqlserver: None,
            postgres: None,
            tables: Arc::new(RwLock::new(Vec::new())),
            progress: Arc::new(RwLock::new(HashMap::new())),
            event_sender: None,
        }
    }

    /// Establece el canal de eventos para la GUI
    pub fn set_event_sender(&mut self, sender: mpsc::UnboundedSender<MigrationEvent>) {
        self.event_sender = Some(sender);
    }

    fn send_event(&self, event: MigrationEvent) {
        if let Some(sender) = &self.event_sender {
            let _ = sender.send(event);
        }
    }

    /// Conecta a ambas bases de datos
    pub async fn connect(&mut self) -> Result<()> {
        let sqlserver = SqlServerConnection::connect(&self.conn_config)
            .await
            .context("Error conectando a SQL Server")?;

        let postgres = PostgresConnection::connect(&self.conn_config)
            .await
            .context("Error conectando a PostgreSQL")?;

        self.sqlserver = Some(Arc::new(sqlserver));
        self.postgres = Some(Arc::new(postgres));

        Ok(())
    }

    /// Prueba las conexiones
    #[allow(dead_code)]
    pub async fn test_connections(&self) -> Result<(bool, bool)> {
        let sql_ok = if let Some(ref conn) = self.sqlserver {
            conn.test_connection().await.is_ok()
        } else {
            false
        };

        let pg_ok = if let Some(ref conn) = self.postgres {
            conn.test_connection().await.is_ok()
        } else {
            false
        };

        Ok((sql_ok, pg_ok))
    }

    /// Obtiene la lista de tablas de SQL Server
    pub async fn fetch_tables(&self) -> Result<Vec<TableInfo>> {
        let sqlserver = self.sqlserver.as_ref()
            .context("No hay conexión a SQL Server")?;

        let tables = sqlserver.get_tables().await?;

        let mut tables_lock = self.tables.write().await;
        *tables_lock = tables.clone();

        Ok(tables)
    }

    /// Analiza tablas en paralelo para detectar columnas JSON
    pub async fn analyze_tables(&self, table_names: Vec<String>) -> Result<HashMap<String, Vec<ColumnInfo>>> {
        let sqlserver = self.sqlserver.as_ref()
            .context("No hay conexión a SQL Server")?;

        let mut results = HashMap::new();
        let semaphore = Arc::new(tokio::sync::Semaphore::new(self.config.parallel_analysis));

        let futures: Vec<_> = table_names.into_iter().map(|table_name| {
            let sqlserver = Arc::clone(sqlserver);
            let semaphore = Arc::clone(&semaphore);
            let analyzer = JsonAnalyzer::new(self.config.json_sample_size, self.config.json_threshold);
            let event_sender = self.event_sender.clone();

            async move {
                let _permit = semaphore.acquire().await.unwrap();

                if let Some(sender) = &event_sender {
                    let _ = sender.send(MigrationEvent::AnalysisStarted(table_name.clone()));
                }

                let parts: Vec<&str> = table_name.split('.').collect();
                if parts.len() != 2 {
                    return (table_name, Vec::new());
                }

                let (schema, table) = (parts[0], parts[1]);

                // Obtener columnas
                let mut columns = match sqlserver.get_columns(schema, table).await {
                    Ok(cols) => cols,
                    Err(_) => return (table_name, Vec::new()),
                };

                // Analizar cada columna para JSON
                for col in &mut columns {
                    // Solo analizar columnas de texto largas
                    if is_text_column(&col.data_type) {
                        if let Ok(samples) = sqlserver.sample_column_for_json(
                            schema,
                            table,
                            &col.name,
                            analyzer.sample_size()
                        ).await {
                            // Contar cuántos valores no son null
                            let non_null_count = samples.iter()
                                .filter(|s| s.as_ref().map(|v| !v.is_empty()).unwrap_or(false))
                                .count();

                            // Solo analizar si hay datos reales (no todo null)
                            if non_null_count == 0 {
                                col.json_reason = "Columna sin datos (todos NULL)".to_string();
                                col.json_score = 0;
                                col.should_convert_to_json = false;
                                continue;
                            }

                            let analysis = analyzer.analyze_column(col, &samples);
                            col.should_convert_to_json = analysis.should_convert;
                            col.json_score = analysis.score;
                            col.json_reason = analysis.reason;

                            if analysis.should_convert {
                                col.pg_type = "JSONB".to_string();
                            }
                        }
                    }
                }

                if let Some(sender) = &event_sender {
                    let _ = sender.send(MigrationEvent::AnalysisComplete(table_name.clone(), columns.clone()));
                }

                (table_name, columns)
            }
        }).collect();

        let analyzed = join_all(futures).await;

        for (table_name, columns) in analyzed {
            results.insert(table_name, columns);
        }

        Ok(results)
    }

    /// Migra las tablas seleccionadas
    pub async fn migrate_tables(&self, tables_to_migrate: Vec<TableInfo>) -> Result<()> {
        let sqlserver = self.sqlserver.as_ref()
            .context("No hay conexión a SQL Server")?;
        let postgres = self.postgres.as_ref()
            .context("No hay conexión a PostgreSQL")?;

        let semaphore = Arc::new(tokio::sync::Semaphore::new(self.config.parallel_migration));

        let futures: Vec<_> = tables_to_migrate.into_iter().map(|table| {
            let sqlserver = Arc::clone(sqlserver);
            let postgres = Arc::clone(postgres);
            let semaphore = Arc::clone(&semaphore);
            let batch_size = self.config.batch_size;
            let progress = Arc::clone(&self.progress);
            let event_sender = self.event_sender.clone();

            async move {
                let _permit = semaphore.acquire().await.unwrap();

                let table_name = table.full_name.clone();

                // Inicializar progreso
                {
                    let mut progress_lock = progress.write().await;
                    let mut prog = MigrationProgress::new(table_name.clone(), table.row_count);
                    prog.status = MigrationStatus::Migrating;
                    prog.start_time = Some(Utc::now());
                    progress_lock.insert(table_name.clone(), prog);
                }

                if let Some(sender) = &event_sender {
                    let _ = sender.send(MigrationEvent::MigrationStarted(table_name.clone()));
                }

                // Crear tabla en PostgreSQL
                if let Err(e) = postgres.create_table(&table).await {
                    let error_msg = format!("Error creando tabla: {}", e);
                    if let Some(sender) = &event_sender {
                        let _ = sender.send(MigrationEvent::MigrationError(table_name.clone(), error_msg.clone()));
                    }
                    let mut progress_lock = progress.write().await;
                    if let Some(prog) = progress_lock.get_mut(&table_name) {
                        prog.status = MigrationStatus::Failed(error_msg);
                        prog.end_time = Some(Utc::now());
                    }
                    return;
                }

                // Migrar datos en lotes
                let column_names: Vec<String> = table.columns.iter().map(|c| c.name.clone()).collect();
                let mut offset = 0i64;
                let mut total_migrated = 0i64;
                let mut json_nulled = 0u32;

                if table.columns.is_empty() {
                    tracing::error!("La tabla {} no tiene columnas definidas", table_name);
                    return;
                }

                loop {
                    let rows = match sqlserver.read_rows(
                        &table.schema,
                        &table.name,
                        &column_names,
                        offset,
                        batch_size
                    ).await {
                        Ok(r) => r,
                        Err(e) => {
                            let error_msg = format!("Error leyendo filas: {}", e);
                            if let Some(sender) = &event_sender {
                                let _ = sender.send(MigrationEvent::MigrationError(table_name.clone(), error_msg));
                            }
                            break;
                        }
                    };

                    if rows.is_empty() {
                        tracing::info!("No hay más filas para leer de {}", table_name);
                        break;
                    }

                    // Convertir filas a valores dinámicos
                    let mut converted_rows = Vec::new();
                    for row in &rows {
                        let mut converted_row = Vec::new();
                        for (i, col_data) in row.iter().enumerate() {
                            let column = &table.columns[i];
                            let value = convert_string_data(col_data, column, &mut json_nulled);
                            converted_row.push(value);
                        }
                        converted_rows.push(converted_row);
                    }

                    // Insertar en PostgreSQL
                    let result = postgres.insert_rows(
                        &table.schema,
                        &table.name,
                        &table.columns,
                        converted_rows
                    ).await;

                    match result {
                        Ok(r) => {
                            total_migrated += r.inserted as i64;
                            if r.failed > 0 {
                                tracing::warn!("{}: {} insertadas, {} fallidas", table_name, r.inserted, r.failed);
                            }
                        }
                        Err(e) => {
                            let error_msg = format!("Error insertando: {}", e);
                            tracing::error!("{}", error_msg);
                            if let Some(sender) = &event_sender {
                                let _ = sender.send(MigrationEvent::MigrationError(table_name.clone(), error_msg));
                            }
                        }
                    }

                    // Actualizar progreso
                    {
                        let mut progress_lock = progress.write().await;
                        if let Some(prog) = progress_lock.get_mut(&table_name) {
                            prog.migrated_rows = total_migrated;
                            prog.json_nulled = json_nulled;
                        }
                    }

                    if let Some(sender) = &event_sender {
                        let _ = sender.send(MigrationEvent::MigrationProgress(
                            table_name.clone(),
                            total_migrated,
                            table.row_count
                        ));
                    }

                    offset += batch_size;

                    if rows.len() < batch_size as usize {
                        break;
                    }
                }

                // Marcar como completado
                {
                    let mut progress_lock = progress.write().await;
                    if let Some(prog) = progress_lock.get_mut(&table_name) {
                        prog.status = MigrationStatus::Completed;
                        prog.end_time = Some(Utc::now());
                    }
                }

                if let Some(sender) = &event_sender {
                    let _ = sender.send(MigrationEvent::MigrationComplete(table_name));
                }
            }
        }).collect();

        join_all(futures).await;

        self.send_event(MigrationEvent::AllComplete);

        Ok(())
    }

    /// Obtiene el progreso actual de todas las migraciones
    #[allow(dead_code)]
    pub async fn get_progress(&self) -> HashMap<String, MigrationProgress> {
        self.progress.read().await.clone()
    }

    /// Obtiene información detallada de una tabla
    #[allow(dead_code)]
    pub async fn get_table_details(&self, schema: &str, table: &str) -> Result<TableInfo> {
        let sqlserver = self.sqlserver.as_ref()
            .context("No hay conexión a SQL Server")?;

        let columns = sqlserver.get_columns(schema, table).await?;
        let primary_keys = sqlserver.get_primary_keys(schema, table).await?;
        let foreign_keys = sqlserver.get_foreign_keys(schema, table).await?;

        Ok(TableInfo {
            schema: schema.to_string(),
            name: table.to_string(),
            full_name: format!("{}.{}", schema, table),
            row_count: 0,
            columns,
            primary_keys,
            foreign_keys,
        })
    }
}

/// Verifica si es una columna de texto
fn is_text_column(data_type: &str) -> bool {
    let dt = data_type.to_lowercase();
    matches!(dt.as_str(),
        "varchar" | "nvarchar" | "text" | "ntext" | "xml" | "char" | "nchar"
    )
}

/// Convierte datos de string a DynamicValue para PostgreSQL
fn convert_string_data(data: &Option<String>, column: &ColumnInfo, _json_nulled: &mut u32) -> DynamicValue {
    // Si la columna debe convertirse a JSON
    if column.should_convert_to_json {
        // safe_convert_to_json SIEMPRE retorna valor para strings no vacíos
        // Si no puede parsear como JSON estructurado, lo guarda como string JSON
        match JsonAnalyzer::safe_convert_to_json(data) {
            Some(json) => DynamicValue::Json(json),
            None => DynamicValue::Null, // Solo para NULL o vacío
        }
    } else {
        // Conversión normal basada en el tipo de destino PostgreSQL
        match data {
            None => DynamicValue::Null,
            Some(s) if s.is_empty() => DynamicValue::Null,
            Some(s) => {
                // Intentar convertir según el tipo de PostgreSQL destino
                let pg_type = column.pg_type.to_uppercase();

                if pg_type.contains("INT") || pg_type == "BIGINT" || pg_type == "SMALLINT" {
                    s.parse::<i64>()
                        .map(DynamicValue::Int)
                        .unwrap_or(DynamicValue::Text(s.clone()))
                } else if pg_type.contains("NUMERIC") || pg_type == "DOUBLE PRECISION" || pg_type == "REAL" {
                    s.parse::<f64>()
                        .map(DynamicValue::Float)
                        .unwrap_or(DynamicValue::Text(s.clone()))
                } else if pg_type == "BOOLEAN" {
                    match s.to_lowercase().as_str() {
                        "true" | "1" | "yes" => DynamicValue::Bool(true),
                        "false" | "0" | "no" => DynamicValue::Bool(false),
                        _ => DynamicValue::Text(s.clone()),
                    }
                } else if pg_type == "UUID" {
                    // Keep as text, PostgreSQL will handle the UUID conversion
                    DynamicValue::Text(s.clone())
                } else {
                    DynamicValue::Text(s.clone())
                }
            }
        }
    }
}
