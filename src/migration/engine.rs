//! Motor de migración principal
//!
//! Maneja la migración de tablas de SQL Server a PostgreSQL
//! con análisis paralelo y conversión inteligente a JSON.

use anyhow::{Context, Result};
use std::sync::Arc;
use std::path::PathBuf;
use tokio::sync::{mpsc, RwLock};
use std::collections::HashMap;
use futures::future::join_all;
use chrono::Utc;

use crate::db::{SqlServerConnection, PostgresConnection, ConnectionConfig};
use crate::db::types::{TableInfo, ColumnInfo, MigrationProgress, MigrationStatus};
use crate::db::postgres::DynamicValue;
use crate::analysis::JsonAnalyzer;
use crate::storage::{BcpExporter, BcpMode, S3Client, FileDataSource};

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
    /// Export events (BCP + S3)
    ExportStarted(String),
    ExportProgress(String, i64, i64), // table, current, total tables
    ExportComplete(String),
    ExportError(String, String),
    AllExportsComplete,
    /// Analysis events
    AnalysisStarted(String),
    AnalysisComplete(String, Vec<ColumnInfo>),
    /// Migration events
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
    /// Cada tabla usa su propia conexión para paralelismo real
    pub async fn analyze_tables(&self, table_names: Vec<String>) -> Result<HashMap<String, Vec<ColumnInfo>>> {
        let mut results = HashMap::new();
        let semaphore = Arc::new(tokio::sync::Semaphore::new(self.config.parallel_analysis));
        let conn_config = self.conn_config.clone();

        let futures: Vec<_> = table_names.into_iter().map(|table_name| {
            let semaphore = Arc::clone(&semaphore);
            let analyzer = JsonAnalyzer::new(self.config.json_sample_size, self.config.json_threshold);
            let event_sender = self.event_sender.clone();
            let conn_config = conn_config.clone();

            async move {
                let _permit = semaphore.acquire().await.unwrap();

                if let Some(sender) = &event_sender {
                    let _ = sender.send(MigrationEvent::AnalysisStarted(table_name.clone()));
                }

                // Crear conexión propia para esta tabla
                let sqlserver = match SqlServerConnection::connect(&conn_config).await {
                    Ok(conn) => conn,
                    Err(_) => return (table_name, Vec::new()),
                };

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
    /// Cada tabla usa su propia conexión para paralelismo real
    pub async fn migrate_tables(&self, tables_to_migrate: Vec<TableInfo>) -> Result<()> {
        let postgres = self.postgres.as_ref()
            .context("No hay conexión a PostgreSQL")?;

        let semaphore = Arc::new(tokio::sync::Semaphore::new(self.config.parallel_migration));
        let conn_config = self.conn_config.clone();

        let futures: Vec<_> = tables_to_migrate.into_iter().map(|table| {
            let postgres = Arc::clone(postgres);
            let semaphore = Arc::clone(&semaphore);
            let batch_size = self.config.batch_size;
            let progress = Arc::clone(&self.progress);
            let event_sender = self.event_sender.clone();
            let conn_config = conn_config.clone();

            async move {
                let _permit = semaphore.acquire().await.unwrap();

                let table_name = table.full_name.clone();

                // Crear conexión propia a SQL Server para esta tabla
                let sqlserver = match SqlServerConnection::connect(&conn_config).await {
                    Ok(conn) => Arc::new(conn),
                    Err(e) => {
                        let error_msg = format!("Error conectando SQL Server: {}", e);
                        if let Some(sender) = &event_sender {
                            let _ = sender.send(MigrationEvent::MigrationError(table_name.clone(), error_msg));
                        }
                        return;
                    }
                };

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

                tracing::info!("{}: Iniciando migración de {} filas", table_name, table.row_count);

                loop {
                    let read_start = std::time::Instant::now();
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
                            tracing::error!("{}: {}", table_name, error_msg);
                            if let Some(sender) = &event_sender {
                                let _ = sender.send(MigrationEvent::MigrationError(table_name.clone(), error_msg));
                            }
                            break;
                        }
                    };
                    let read_time = read_start.elapsed();

                    if rows.is_empty() {
                        tracing::info!("{}: Migración completada", table_name);
                        break;
                    }

                    tracing::info!("{}: Leídas {} filas en {:?}", table_name, rows.len(), read_time);

                    // Convertir filas a valores dinámicos
                    let convert_start = std::time::Instant::now();
                    let mut converted_rows = Vec::new();
                    let num_columns = table.columns.len();
                    for row in &rows {
                        let mut converted_row = Vec::new();
                        // Solo procesar hasta el número de columnas definidas
                        for i in 0..num_columns {
                            let col_data = row.get(i).cloned().flatten();
                            let column = &table.columns[i];
                            let value = convert_string_data(&col_data, column, &mut json_nulled);
                            converted_row.push(value);
                        }
                        converted_rows.push(converted_row);
                    }
                    let convert_time = convert_start.elapsed();

                    // Insertar en PostgreSQL
                    let insert_start = std::time::Instant::now();
                    let result = postgres.insert_rows(
                        &table.schema,
                        &table.name,
                        &table.columns,
                        converted_rows
                    ).await;
                    let insert_time = insert_start.elapsed();

                    tracing::info!("{}: Convertido en {:?}, Insertado en {:?}", table_name, convert_time, insert_time);

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

    /// Export tables to S3 using BCP
    pub async fn export_to_s3(&self, tables: &[TableInfo]) -> Result<PathBuf> {
        // Determine BCP mode
        let bcp_mode = if self.conn_config.bcp_use_docker {
            BcpMode::Docker {
                image: self.conn_config.bcp_docker_image.clone(),
            }
        } else {
            BcpMode::Local
        };

        // Check if BCP is available
        if !BcpExporter::is_available(&bcp_mode) {
            let instructions = if self.conn_config.bcp_use_docker {
                "Docker no esta instalado. Instala Docker o desactiva 'Usar BCP via Docker'.".to_string()
            } else {
                BcpExporter::get_install_instructions()
            };
            self.send_event(MigrationEvent::ExportError(
                "bcp".to_string(),
                instructions.clone()
            ));
            return Err(anyhow::anyhow!(instructions));
        }

        // Create temporary directory
        let temp_dir = tempfile::tempdir()
            .context("Error creando directorio temporal")?;
        let temp_path = temp_dir.path().to_path_buf();

        tracing::info!("Directorio temporal para export: {}", temp_path.display());
        tracing::info!("Modo BCP: {:?}", bcp_mode);

        let total_tables = tables.len() as i64;
        let exported_count = Arc::new(std::sync::atomic::AtomicI64::new(0));

        // Export tables in parallel using BCP
        let semaphore = Arc::new(tokio::sync::Semaphore::new(self.config.parallel_analysis));
        let conn_config = self.conn_config.clone();

        let export_futures: Vec<_> = tables.iter().map(|table| {
            let semaphore = Arc::clone(&semaphore);
            let event_sender = self.event_sender.clone();
            let temp_path = temp_path.clone();
            let table = table.clone();
            let bcp_mode = bcp_mode.clone();
            let conn_config = conn_config.clone();
            let exported_count = Arc::clone(&exported_count);

            async move {
                let _permit = semaphore.acquire().await.unwrap();

                if let Some(sender) = &event_sender {
                    let _ = sender.send(MigrationEvent::ExportStarted(table.full_name.clone()));
                }

                let output_path = temp_path.join(format!("{}.{}.csv", table.schema, table.name));

                // Create own SQL Server connection for column info
                let sqlserver = match SqlServerConnection::connect(&conn_config).await {
                    Ok(conn) => conn,
                    Err(e) => {
                        if let Some(sender) = &event_sender {
                            let _ = sender.send(MigrationEvent::ExportError(
                                table.full_name.clone(),
                                format!("Error conectando: {}", e)
                            ));
                        }
                        return Err(anyhow::anyhow!("Error conectando: {}", e));
                    }
                };

                let columns = match sqlserver.get_columns(&table.schema, &table.name).await {
                    Ok(cols) => cols,
                    Err(e) => {
                        if let Some(sender) = &event_sender {
                            let _ = sender.send(MigrationEvent::ExportError(
                                table.full_name.clone(),
                                format!("Error obteniendo columnas: {}", e)
                            ));
                        }
                        return Err(anyhow::anyhow!("Error obteniendo columnas: {}", e));
                    }
                };
                let column_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();

                // Create BCP exporter for this table
                let bcp = BcpExporter::new(
                    conn_config.sqlserver_host.clone(),
                    conn_config.sqlserver_port,
                    conn_config.sqlserver_database.clone(),
                    conn_config.sqlserver_user.clone(),
                    conn_config.sqlserver_password.clone(),
                    bcp_mode,
                );

                match bcp.export_table_with_header(&table.schema, &table.name, &column_names, &output_path).await {
                    Ok(result) => {
                        let count = exported_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                        tracing::info!("Exportado {}: {} filas ({}/{})", result.table_name, result.rows_exported, count, total_tables);

                        if let Some(sender) = &event_sender {
                            let _ = sender.send(MigrationEvent::ExportProgress(table.full_name.clone(), count, total_tables));
                            let _ = sender.send(MigrationEvent::ExportComplete(table.full_name.clone()));
                        }
                        Ok((table.full_name.clone(), output_path))
                    }
                    Err(e) => {
                        let error_msg = format!("Error exportando {}: {}", table.full_name, e);
                        tracing::error!("{}", error_msg);
                        if let Some(sender) = &event_sender {
                            let _ = sender.send(MigrationEvent::ExportError(table.full_name.clone(), error_msg.clone()));
                        }
                        Err(anyhow::anyhow!(error_msg))
                    }
                }
            }
        }).collect();

        let export_results = join_all(export_futures).await;

        // Check for any errors
        let mut exported_files: Vec<(String, PathBuf)> = Vec::new();
        for result in export_results {
            match result {
                Ok((name, path)) => exported_files.push((name, path)),
                Err(e) => {
                    tracing::error!("Export failed: {}", e);
                    // Continue with other exports, don't fail completely
                }
            }
        }

        // Upload to S3 if enabled (also in parallel)
        if self.conn_config.s3_enabled && !self.conn_config.s3_bucket.is_empty() {
            tracing::info!("Subiendo {} archivos a S3...", exported_files.len());

            let s3 = Arc::new(S3Client::new(
                self.conn_config.s3_bucket.clone(),
                self.conn_config.s3_region.clone(),
                self.conn_config.s3_access_key.clone(),
                self.conn_config.s3_secret_key.clone(),
                self.conn_config.s3_prefix.clone(),
            ).await.context("Error creando cliente S3")?);

            let upload_semaphore = Arc::new(tokio::sync::Semaphore::new(4)); // 4 uploads in parallel

            let upload_futures: Vec<_> = exported_files.iter().map(|(table_name, local_path)| {
                let s3 = Arc::clone(&s3);
                let upload_semaphore = Arc::clone(&upload_semaphore);
                let event_sender = self.event_sender.clone();
                let table_name = table_name.clone();
                let local_path = local_path.clone();

                async move {
                    let _permit = upload_semaphore.acquire().await.unwrap();

                    let key = local_path.file_name()
                        .map(|f| f.to_string_lossy().to_string())
                        .unwrap_or_default();

                    match s3.upload_file(&local_path, &key).await {
                        Ok(_) => {
                            tracing::info!("Subido a S3: {}", key);
                            Ok(())
                        }
                        Err(e) => {
                            let error_msg = format!("Error subiendo {} a S3: {}", key, e);
                            tracing::error!("{}", error_msg);
                            if let Some(sender) = &event_sender {
                                let _ = sender.send(MigrationEvent::ExportError(table_name, error_msg.clone()));
                            }
                            Err(anyhow::anyhow!(error_msg))
                        }
                    }
                }
            }).collect();

            let upload_results = join_all(upload_futures).await;

            // Check for upload errors
            for result in upload_results {
                if let Err(e) = result {
                    tracing::error!("Upload failed: {}", e);
                }
            }
        }

        // Keep the temp directory by leaking it (we'll use it for migration)
        // The files will be cleaned up when the process exits
        let persistent_path = temp_dir.keep();

        // Note: AllExportsComplete will be sent by the GUI after receiving the path
        Ok(persistent_path)
    }

    /// Analyze tables from exported files
    pub async fn analyze_tables_from_files(
        &self,
        table_names: Vec<String>,
        files_path: &PathBuf,
    ) -> Result<HashMap<String, Vec<ColumnInfo>>> {
        let mut results = HashMap::new();
        let semaphore = Arc::new(tokio::sync::Semaphore::new(self.config.parallel_analysis));

        let futures: Vec<_> = table_names.into_iter().map(|table_name| {
            let semaphore = Arc::clone(&semaphore);
            let analyzer = JsonAnalyzer::new(self.config.json_sample_size, self.config.json_threshold);
            let event_sender = self.event_sender.clone();
            let files_path = files_path.clone();

            async move {
                let _permit = semaphore.acquire().await.unwrap();

                if let Some(sender) = &event_sender {
                    let _ = sender.send(MigrationEvent::AnalysisStarted(table_name.clone()));
                }

                let file_source = FileDataSource::new(files_path);

                let parts: Vec<&str> = table_name.split('.').collect();
                if parts.len() != 2 {
                    return (table_name, Vec::new());
                }

                let (schema, table) = (parts[0], parts[1]);

                // Check if file exists
                if !file_source.table_file_exists(schema, table) {
                    tracing::warn!("Archivo no encontrado para {}.{}", schema, table);
                    return (table_name, Vec::new());
                }

                // Read columns from CSV header
                let column_names = match file_source.read_columns(schema, table) {
                    Ok(cols) => cols,
                    Err(e) => {
                        tracing::error!("Error leyendo columnas de {}: {}", table_name, e);
                        return (table_name, Vec::new());
                    }
                };

                // Create ColumnInfo for each column (we need to infer types from data)
                let mut columns: Vec<ColumnInfo> = column_names.iter().map(|name| {
                    ColumnInfo {
                        name: name.clone(),
                        data_type: "nvarchar".to_string(), // Assume text from CSV
                        max_length: Some(-1), // MAX
                        precision: None,
                        scale: None,
                        is_nullable: true,
                        is_identity: false,
                        default_value: None,
                        should_convert_to_json: false,
                        json_score: 0,
                        json_reason: String::new(),
                        pg_type: "TEXT".to_string(),
                    }
                }).collect();

                // Analyze each column for JSON
                for col in &mut columns {
                    match file_source.sample_column_for_json_async(
                        schema,
                        table,
                        &col.name,
                        analyzer.sample_size()
                    ).await {
                        Ok(samples) => {
                            let non_null_count = samples.iter()
                                .filter(|s| s.is_some())
                                .count();

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
                        Err(e) => {
                            tracing::warn!("Error muestreando columna {}: {}", col.name, e);
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

    /// Migrate tables reading from exported files
    pub async fn migrate_tables_from_files(
        &self,
        tables_to_migrate: Vec<TableInfo>,
        files_path: &PathBuf,
    ) -> Result<()> {
        let postgres = self.postgres.as_ref()
            .context("No hay conexion a PostgreSQL")?;

        let semaphore = Arc::new(tokio::sync::Semaphore::new(self.config.parallel_migration));

        let futures: Vec<_> = tables_to_migrate.into_iter().map(|table| {
            let postgres = Arc::clone(postgres);
            let semaphore = Arc::clone(&semaphore);
            let batch_size = self.config.batch_size;
            let progress = Arc::clone(&self.progress);
            let event_sender = self.event_sender.clone();
            let files_path = files_path.clone();

            async move {
                let _permit = semaphore.acquire().await.unwrap();

                let table_name = table.full_name.clone();
                let file_source = FileDataSource::new(files_path);

                // Get row count from file
                let row_count = match file_source.get_row_count(&table.schema, &table.name) {
                    Ok(count) => count,
                    Err(e) => {
                        tracing::error!("Error obteniendo row count de {}: {}", table_name, e);
                        if let Some(sender) = &event_sender {
                            let _ = sender.send(MigrationEvent::MigrationError(
                                table_name.clone(),
                                e.to_string()
                            ));
                        }
                        return;
                    }
                };

                // Initialize progress
                {
                    let mut progress_lock = progress.write().await;
                    let mut prog = MigrationProgress::new(table_name.clone(), row_count);
                    prog.status = MigrationStatus::Migrating;
                    prog.start_time = Some(Utc::now());
                    progress_lock.insert(table_name.clone(), prog);
                }

                if let Some(sender) = &event_sender {
                    let _ = sender.send(MigrationEvent::MigrationStarted(table_name.clone()));
                }

                // Create table in PostgreSQL
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

                // Migrate data in batches
                let column_names: Vec<String> = table.columns.iter().map(|c| c.name.clone()).collect();
                let mut offset = 0i64;
                let mut total_migrated = 0i64;
                let mut json_nulled = 0u32;

                if table.columns.is_empty() {
                    tracing::error!("La tabla {} no tiene columnas definidas", table_name);
                    return;
                }

                tracing::info!("{}: Iniciando migracion desde archivo ({} filas)", table_name, row_count);

                loop {
                    let read_start = std::time::Instant::now();
                    let rows = match file_source.read_rows_async(
                        &table.schema,
                        &table.name,
                        &column_names,
                        offset,
                        batch_size
                    ).await {
                        Ok(r) => r,
                        Err(e) => {
                            let error_msg = format!("Error leyendo filas de archivo: {}", e);
                            tracing::error!("{}: {}", table_name, error_msg);
                            if let Some(sender) = &event_sender {
                                let _ = sender.send(MigrationEvent::MigrationError(table_name.clone(), error_msg));
                            }
                            break;
                        }
                    };
                    let read_time = read_start.elapsed();

                    if rows.is_empty() {
                        tracing::info!("{}: Migracion completada", table_name);
                        break;
                    }

                    tracing::info!("{}: Leidas {} filas de archivo en {:?}", table_name, rows.len(), read_time);

                    // Convert rows to dynamic values
                    let convert_start = std::time::Instant::now();
                    let mut converted_rows = Vec::new();
                    let num_columns = table.columns.len();
                    for row in &rows {
                        let mut converted_row = Vec::new();
                        // Solo procesar hasta el número de columnas definidas
                        for i in 0..num_columns {
                            let col_data = row.get(i).cloned().flatten();
                            let column = &table.columns[i];
                            let value = convert_string_data(&col_data, column, &mut json_nulled);
                            converted_row.push(value);
                        }
                        converted_rows.push(converted_row);
                    }
                    let convert_time = convert_start.elapsed();

                    // Insert into PostgreSQL
                    let insert_start = std::time::Instant::now();
                    let result = postgres.insert_rows(
                        &table.schema,
                        &table.name,
                        &table.columns,
                        converted_rows
                    ).await;
                    let insert_time = insert_start.elapsed();

                    tracing::info!("{}: Convertido en {:?}, Insertado en {:?}", table_name, convert_time, insert_time);

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

                    // Update progress
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
                            row_count
                        ));
                    }

                    offset += batch_size;

                    if rows.len() < batch_size as usize {
                        break;
                    }
                }

                // Mark as complete
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
