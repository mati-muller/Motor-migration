//! Interfaz gráfica del motor de migración

use eframe::egui;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::runtime::Runtime;
use chrono::Utc;

use crate::db::types::{ConnectionConfig, TableInfo, ColumnInfo, MigrationProgress, MigrationStatus};
use crate::migration::{MigrationEngine, MigrationConfig, MigrationEvent};

/// Estados de la aplicación
#[derive(Debug, Clone, PartialEq)]
enum AppState {
    Config,
    Connecting,
    TableSelection,
    Analyzing,
    ColumnConfig,
    Migrating,
    Complete,
}

/// Aplicación principal
pub struct MigrationApp {
    // Estado
    state: AppState,

    // Configuración de conexión
    conn_config: ConnectionConfig,

    // Configuración de migración
    migration_config: MigrationConfig,

    // Tablas disponibles
    tables: Vec<TableInfo>,

    // Tablas seleccionadas para migrar
    selected_tables: HashSet<String>,

    // Columnas analizadas por tabla
    analyzed_columns: HashMap<String, Vec<ColumnInfo>>,

    // Progreso de migración
    progress: HashMap<String, MigrationProgress>,

    // Mensajes de log
    logs: Vec<String>,

    // Error actual
    error: Option<String>,

    // Runtime de tokio
    runtime: Arc<Runtime>,

    // Canal de eventos
    event_receiver: Option<mpsc::UnboundedReceiver<MigrationEvent>>,

    // Tiempo de inicio
    start_time: Option<chrono::DateTime<chrono::Utc>>,

    // Búsqueda de tablas
    table_search: String,

    // Estadísticas
    total_rows_to_migrate: i64,
    total_rows_migrated: i64,
    json_columns_found: usize,
    json_nulled_count: u32,
}

impl Default for MigrationApp {
    fn default() -> Self {
        Self {
            state: AppState::Config,
            conn_config: ConnectionConfig {
                sqlserver_host: "localhost".to_string(),
                sqlserver_port: 1433,
                sqlserver_database: String::new(),
                sqlserver_user: String::new(),
                sqlserver_password: String::new(),
                sqlserver_trust_cert: true,
                postgres_host: "localhost".to_string(),
                postgres_port: 5432,
                postgres_database: String::new(),
                postgres_user: "postgres".to_string(),
                postgres_password: String::new(),
            },
            migration_config: MigrationConfig::default(),
            tables: Vec::new(),
            selected_tables: HashSet::new(),
            analyzed_columns: HashMap::new(),
            progress: HashMap::new(),
            logs: Vec::new(),
            error: None,
            runtime: Arc::new(Runtime::new().unwrap()),
            event_receiver: None,
            start_time: None,
            table_search: String::new(),
            total_rows_to_migrate: 0,
            total_rows_migrated: 0,
            json_columns_found: 0,
            json_nulled_count: 0,
        }
    }
}

impl MigrationApp {
    pub fn new(_cc: &eframe::CreationContext<'_>) -> Self {
        Self::default()
    }

    fn log(&mut self, msg: &str) {
        let timestamp = Utc::now().format("%H:%M:%S");
        self.logs.push(format!("[{}] {}", timestamp, msg));
    }

    fn process_events(&mut self) {
        // Coleccionar eventos primero para evitar doble borrow
        let events: Vec<MigrationEvent> = if let Some(receiver) = &mut self.event_receiver {
            let mut collected = Vec::new();
            while let Ok(event) = receiver.try_recv() {
                collected.push(event);
            }
            collected
        } else {
            return;
        };

        // Procesar eventos
        for event in events {
            match event {
                MigrationEvent::AnalysisStarted(table) => {
                    let timestamp = Utc::now().format("%H:%M:%S");
                    self.logs.push(format!("[{}] Analizando: {}", timestamp, table));
                }
                MigrationEvent::AnalysisComplete(table, columns) => {
                    let json_cols_count = columns.iter()
                        .filter(|c| c.should_convert_to_json)
                        .count();
                    if json_cols_count > 0 {
                        let timestamp = Utc::now().format("%H:%M:%S");
                        self.logs.push(format!("[{}]   {} columnas JSON detectadas en {}", timestamp, json_cols_count, table));
                        self.json_columns_found += json_cols_count;
                    }
                    self.analyzed_columns.insert(table, columns);
                }
                MigrationEvent::MigrationStarted(table) => {
                    let timestamp = Utc::now().format("%H:%M:%S");
                    self.logs.push(format!("[{}] Migrando: {}", timestamp, table));
                }
                MigrationEvent::MigrationProgress(table, current, total) => {
                    if let Some(prog) = self.progress.get_mut(&table) {
                        prog.migrated_rows = current;
                    } else {
                        let mut p = MigrationProgress::new(table.clone(), total);
                        p.migrated_rows = current;
                        p.status = MigrationStatus::Migrating;
                        self.progress.insert(table, p);
                    }
                    // Actualizar totales inline
                    self.total_rows_migrated = self.progress.values()
                        .map(|p| p.migrated_rows)
                        .sum();
                    self.json_nulled_count = self.progress.values()
                        .map(|p| p.json_nulled)
                        .sum();
                }
                MigrationEvent::MigrationComplete(table) => {
                    let timestamp = Utc::now().format("%H:%M:%S");
                    self.logs.push(format!("[{}] Completado: {}", timestamp, table));
                    if let Some(prog) = self.progress.get_mut(&table) {
                        prog.status = MigrationStatus::Completed;
                        prog.end_time = Some(Utc::now());
                    }
                }
                MigrationEvent::MigrationError(table, error) => {
                    let timestamp = Utc::now().format("%H:%M:%S");
                    self.logs.push(format!("[{}] ERROR en {}: {}", timestamp, table, error));
                    if let Some(prog) = self.progress.get_mut(&table) {
                        prog.status = MigrationStatus::Failed(error.clone());
                        prog.errors.push(error);
                    }
                }
                MigrationEvent::AllComplete => {
                    let timestamp = Utc::now().format("%H:%M:%S");
                    self.logs.push(format!("[{}] Migracion completada!", timestamp));
                    self.state = AppState::Complete;
                }
            }
        }
    }

    fn update_totals(&mut self) {
        self.total_rows_migrated = self.progress.values()
            .map(|p| p.migrated_rows)
            .sum();

        self.json_nulled_count = self.progress.values()
            .map(|p| p.json_nulled)
            .sum();
    }

    fn render_config(&mut self, ui: &mut egui::Ui) {
        ui.heading("Configuración de Conexiones");
        ui.add_space(10.0);

        ui.horizontal(|ui| {
            // SQL Server
            ui.vertical(|ui| {
                ui.group(|ui| {
                    ui.heading("SQL Server (Origen)");
                    ui.add_space(5.0);

                    ui.horizontal(|ui| {
                        ui.label("Host:");
                        ui.text_edit_singleline(&mut self.conn_config.sqlserver_host);
                    });

                    ui.horizontal(|ui| {
                        ui.label("Puerto:");
                        let mut port_str = self.conn_config.sqlserver_port.to_string();
                        if ui.text_edit_singleline(&mut port_str).changed() {
                            if let Ok(p) = port_str.parse() {
                                self.conn_config.sqlserver_port = p;
                            }
                        }
                    });

                    ui.horizontal(|ui| {
                        ui.label("Base de datos:");
                        ui.text_edit_singleline(&mut self.conn_config.sqlserver_database);
                    });

                    ui.horizontal(|ui| {
                        ui.label("Usuario:");
                        ui.text_edit_singleline(&mut self.conn_config.sqlserver_user);
                    });

                    ui.horizontal(|ui| {
                        ui.label("Contrasena:");
                        ui.add(egui::TextEdit::singleline(&mut self.conn_config.sqlserver_password).password(true));
                    });

                    ui.checkbox(&mut self.conn_config.sqlserver_trust_cert, "Trust Server Certificate");
                });
            });

            ui.add_space(20.0);

            // PostgreSQL
            ui.vertical(|ui| {
                ui.group(|ui| {
                    ui.heading("PostgreSQL (Destino)");
                    ui.add_space(5.0);

                    ui.horizontal(|ui| {
                        ui.label("Host:");
                        ui.text_edit_singleline(&mut self.conn_config.postgres_host);
                    });

                    ui.horizontal(|ui| {
                        ui.label("Puerto:");
                        let mut port_str = self.conn_config.postgres_port.to_string();
                        if ui.text_edit_singleline(&mut port_str).changed() {
                            if let Ok(p) = port_str.parse() {
                                self.conn_config.postgres_port = p;
                            }
                        }
                    });

                    ui.horizontal(|ui| {
                        ui.label("Base de datos:");
                        ui.text_edit_singleline(&mut self.conn_config.postgres_database);
                    });

                    ui.horizontal(|ui| {
                        ui.label("Usuario:");
                        ui.text_edit_singleline(&mut self.conn_config.postgres_user);
                    });

                    ui.horizontal(|ui| {
                        ui.label("Contrasena:");
                        ui.add(egui::TextEdit::singleline(&mut self.conn_config.postgres_password).password(true));
                    });
                });
            });
        });

        ui.add_space(20.0);

        // Configuración de migración
        ui.group(|ui| {
            ui.heading("Configuracion de Migracion");
            ui.add_space(5.0);

            ui.horizontal(|ui| {
                ui.label("Tablas en paralelo (analisis):");
                ui.add(egui::Slider::new(&mut self.migration_config.parallel_analysis, 1..=16));
                ui.label("(mas = mas rapido)");
            });

            ui.horizontal(|ui| {
                ui.label("Tablas en paralelo (migracion):");
                ui.add(egui::Slider::new(&mut self.migration_config.parallel_migration, 1..=10));
                ui.label("(cuidado con la carga)");
            });

            ui.horizontal(|ui| {
                ui.label("Tamano de lote (filas):");
                ui.add(egui::Slider::new(&mut self.migration_config.batch_size, 100..=50000).logarithmic(true));
            });

            ui.horizontal(|ui| {
                ui.label("Muestra JSON (filas):");
                ui.add(egui::Slider::new(&mut self.migration_config.json_sample_size, 10..=1000));
            });

            ui.horizontal(|ui| {
                ui.label("Umbral JSON (0-100):");
                ui.add(egui::Slider::new(&mut self.migration_config.json_threshold, 0..=100));
            });
        });

        ui.add_space(20.0);

        if ui.button("Conectar").clicked() {
            self.connect();
        }

        if let Some(error) = &self.error {
            ui.colored_label(egui::Color32::RED, error);
        }
    }

    fn render_table_selection(&mut self, ui: &mut egui::Ui) {
        ui.heading("Seleccion de Tablas a Migrar");
        ui.add_space(5.0);
        ui.label("Selecciona SOLO las tablas que quieres migrar:");
        ui.add_space(10.0);

        // Barra de búsqueda y botones
        ui.horizontal(|ui| {
            ui.label("Buscar:");
            ui.add(egui::TextEdit::singleline(&mut self.table_search).desired_width(200.0));
        });

        ui.add_space(5.0);

        ui.horizontal(|ui| {
            if ui.button("Seleccionar Todas").clicked() {
                for table in &self.tables {
                    self.selected_tables.insert(table.full_name.clone());
                }
            }

            if ui.button("Deseleccionar Todas").clicked() {
                self.selected_tables.clear();
            }

            if ui.button("Invertir Seleccion").clicked() {
                let all_tables: std::collections::HashSet<String> = self.tables.iter()
                    .map(|t| t.full_name.clone())
                    .collect();
                let new_selection: std::collections::HashSet<String> = all_tables
                    .difference(&self.selected_tables)
                    .cloned()
                    .collect();
                self.selected_tables = new_selection;
            }

            // Seleccionar solo las que coinciden con búsqueda
            if !self.table_search.is_empty() {
                if ui.button("Seleccionar Filtradas").clicked() {
                    let search_lower = self.table_search.to_lowercase();
                    for table in &self.tables {
                        if table.full_name.to_lowercase().contains(&search_lower) {
                            self.selected_tables.insert(table.full_name.clone());
                        }
                    }
                }
            }
        });

        ui.add_space(10.0);

        // Estadísticas
        let total_tables = self.tables.len();
        let selected_count = self.selected_tables.len();
        let selected_rows: i64 = self.tables.iter()
            .filter(|t| self.selected_tables.contains(&t.full_name))
            .map(|t| t.row_count)
            .sum();
        let total_rows: i64 = self.tables.iter().map(|t| t.row_count).sum();

        ui.group(|ui| {
            ui.horizontal(|ui| {
                ui.label(format!("Tablas: {}/{}", selected_count, total_tables));
                ui.separator();
                ui.label(format!("Filas a migrar: {} de {}",
                    format_number(selected_rows),
                    format_number(total_rows)));
            });
        });

        ui.add_space(10.0);

        // Lista de tablas
        let search_lower = self.table_search.to_lowercase();

        egui::ScrollArea::vertical()
            .max_height(350.0)
            .show(ui, |ui| {
                // Agrupar por esquema
                let mut current_schema = String::new();

                for table in &self.tables {
                    if !search_lower.is_empty() && !table.full_name.to_lowercase().contains(&search_lower) {
                        continue;
                    }

                    // Mostrar encabezado de esquema
                    if table.schema != current_schema {
                        current_schema = table.schema.clone();
                        ui.add_space(5.0);
                        ui.horizontal(|ui| {
                            ui.strong(format!("[{}]", current_schema));
                            // Botón para seleccionar todo el esquema
                            if ui.small_button("+ todo").clicked() {
                                for t in &self.tables {
                                    if t.schema == current_schema {
                                        self.selected_tables.insert(t.full_name.clone());
                                    }
                                }
                            }
                            if ui.small_button("- todo").clicked() {
                                for t in &self.tables {
                                    if t.schema == current_schema {
                                        self.selected_tables.remove(&t.full_name);
                                    }
                                }
                            }
                        });
                    }

                    let mut selected = self.selected_tables.contains(&table.full_name);
                    ui.horizontal(|ui| {
                        ui.add_space(20.0); // Indentación
                        if ui.checkbox(&mut selected, "").changed() {
                            if selected {
                                self.selected_tables.insert(table.full_name.clone());
                            } else {
                                self.selected_tables.remove(&table.full_name);
                            }
                        }
                        ui.label(&table.name);
                        ui.weak(format!("({} filas)", format_number(table.row_count)));
                    });
                }
            });

        ui.add_space(15.0);

        // Botón de acción
        ui.horizontal(|ui| {
            if selected_count == 0 {
                ui.label("Selecciona al menos una tabla para continuar");
            } else {
                let btn = ui.button(format!("Analizar {} Tablas Seleccionadas", selected_count));
                if btn.clicked() {
                    self.analyze_selected_tables();
                }
            }
        });
    }

    fn render_column_config(&mut self, ui: &mut egui::Ui) {
        ui.heading("Configuracion de Columnas");
        ui.add_space(5.0);

        ui.colored_label(egui::Color32::YELLOW,
            "Columnas marcadas para JSON seran convertidas a JSONB en PostgreSQL");
        ui.colored_label(egui::Color32::YELLOW,
            "Si un valor no puede convertirse a JSON, se insertara NULL");
        ui.add_space(10.0);

        egui::ScrollArea::vertical()
            .max_height(400.0)
            .show(ui, |ui| {
                let table_names: Vec<String> = self.analyzed_columns.keys().cloned().collect();

                for table_name in table_names {
                    if let Some(columns) = self.analyzed_columns.get_mut(&table_name) {
                        ui.collapsing(&table_name, |ui| {
                            for col in columns.iter_mut() {
                                ui.horizontal(|ui| {
                                    let mut convert = col.should_convert_to_json;
                                    if ui.checkbox(&mut convert, "JSON").changed() {
                                        col.should_convert_to_json = convert;
                                        col.pg_type = if convert { "JSONB".to_string() } else {
                                            crate::db::types::map_sqlserver_to_postgres(&col.data_type, col.max_length)
                                        };
                                    }

                                    ui.label(&col.name);
                                    ui.label(format!("({} -> {})", col.data_type, col.pg_type));

                                    if col.json_score > 0 {
                                        let color = if col.json_score >= 60 {
                                            egui::Color32::GREEN
                                        } else if col.json_score >= 40 {
                                            egui::Color32::YELLOW
                                        } else {
                                            egui::Color32::GRAY
                                        };
                                        ui.colored_label(color, format!("Score: {}", col.json_score));
                                    }
                                });

                                if !col.json_reason.is_empty() {
                                    ui.indent(col.name.as_str(), |ui| {
                                        ui.small(&col.json_reason);
                                    });
                                }
                            }
                        });
                    }
                }
            });

        ui.add_space(10.0);

        if ui.button("Iniciar Migracion").clicked() {
            self.start_migration();
        }
    }

    fn render_migration_progress(&mut self, ui: &mut egui::Ui) {
        ui.heading("Progreso de Migracion");
        ui.add_space(10.0);

        // Tiempo transcurrido
        if let Some(start) = self.start_time {
            let elapsed = Utc::now() - start;
            ui.label(format!("Tiempo transcurrido: {}:{:02}:{:02}",
                elapsed.num_hours(),
                elapsed.num_minutes() % 60,
                elapsed.num_seconds() % 60
            ));
        }

        ui.add_space(10.0);

        // Progreso global
        let global_progress = if self.total_rows_to_migrate > 0 {
            self.total_rows_migrated as f32 / self.total_rows_to_migrate as f32
        } else {
            0.0
        };

        ui.horizontal(|ui| {
            ui.label("Progreso global:");
            ui.add(egui::ProgressBar::new(global_progress)
                .text(format!("{:.1}%", global_progress * 100.0)));
        });

        ui.label(format!("Filas: {} / {}", self.total_rows_migrated, self.total_rows_to_migrate));

        if self.json_nulled_count > 0 {
            ui.colored_label(egui::Color32::YELLOW,
                format!("Valores JSON no convertibles (NULL): {}", self.json_nulled_count));
        }

        ui.add_space(10.0);

        // Progreso por tabla
        egui::ScrollArea::vertical()
            .max_height(300.0)
            .show(ui, |ui| {
                for (table, prog) in &self.progress {
                    ui.horizontal(|ui| {
                        let status_color = match &prog.status {
                            MigrationStatus::Completed => egui::Color32::GREEN,
                            MigrationStatus::Migrating => egui::Color32::YELLOW,
                            MigrationStatus::Failed(_) => egui::Color32::RED,
                            _ => egui::Color32::GRAY,
                        };

                        ui.colored_label(status_color, status_icon(&prog.status));
                        ui.label(table);

                        let progress = prog.percentage() / 100.0;
                        ui.add(egui::ProgressBar::new(progress)
                            .text(format!("{:.1}%", progress * 100.0))
                            .desired_width(150.0));

                        ui.label(format!("{}/{}", prog.migrated_rows, prog.total_rows));

                        if let Some(elapsed) = prog.elapsed() {
                            ui.label(format!("{}s", elapsed.num_seconds()));
                        }
                    });
                }
            });

        ui.add_space(10.0);

        // Logs
        ui.collapsing("Logs", |ui| {
            egui::ScrollArea::vertical()
                .max_height(150.0)
                .stick_to_bottom(true)
                .show(ui, |ui| {
                    for log in &self.logs {
                        ui.label(log);
                    }
                });
        });
    }

    fn render_complete(&mut self, ui: &mut egui::Ui) {
        ui.heading("Migracion Completada!");
        ui.add_space(20.0);

        if let Some(start) = self.start_time {
            let elapsed = Utc::now() - start;
            ui.label(format!("Tiempo total: {}:{:02}:{:02}",
                elapsed.num_hours(),
                elapsed.num_minutes() % 60,
                elapsed.num_seconds() % 60
            ));
        }

        ui.add_space(10.0);

        // Estadísticas finales
        ui.group(|ui| {
            ui.heading("Estadisticas");
            ui.label(format!("Tablas migradas: {}", self.progress.len()));
            ui.label(format!("Filas migradas: {}", self.total_rows_migrated));
            ui.label(format!("Columnas JSON detectadas: {}", self.json_columns_found));
            ui.label(format!("Valores no convertibles a JSON: {}", self.json_nulled_count));

            let completed = self.progress.values()
                .filter(|p| matches!(p.status, MigrationStatus::Completed))
                .count();
            let failed = self.progress.values()
                .filter(|p| matches!(p.status, MigrationStatus::Failed(_)))
                .count();

            ui.label(format!("Exitosas: {} | Fallidas: {}", completed, failed));
        });

        ui.add_space(20.0);

        if ui.button("Nueva Migracion").clicked() {
            self.reset();
        }
    }

    fn connect(&mut self) {
        self.state = AppState::Connecting;
        self.error = None;
        self.log("Conectando a las bases de datos...");

        let conn_config = self.conn_config.clone();
        let migration_config = self.migration_config.clone();

        let (tx, rx) = mpsc::unbounded_channel();
        self.event_receiver = Some(rx);

        let runtime = Arc::clone(&self.runtime);

        // Ejecutar conexión en background
        std::thread::spawn(move || {
            runtime.block_on(async {
                let mut engine = MigrationEngine::new(conn_config, migration_config);
                engine.set_event_sender(tx.clone());

                match engine.connect().await {
                    Ok(_) => {
                        match engine.fetch_tables().await {
                            Ok(tables) => {
                                // Enviar tablas de vuelta (simplificado)
                                for table in tables {
                                    let _ = tx.send(MigrationEvent::AnalysisComplete(
                                        format!("__TABLE__:{}", table.full_name),
                                        vec![]
                                    ));
                                }
                                let _ = tx.send(MigrationEvent::AllComplete);
                            }
                            Err(e) => {
                                let _ = tx.send(MigrationEvent::MigrationError(
                                    "connection".to_string(),
                                    e.to_string()
                                ));
                            }
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(MigrationEvent::MigrationError(
                            "connection".to_string(),
                            e.to_string()
                        ));
                    }
                }
            });
        });
    }

    fn analyze_selected_tables(&mut self) {
        self.state = AppState::Analyzing;
        self.log("Analizando tablas seleccionadas...");

        let conn_config = self.conn_config.clone();
        let migration_config = self.migration_config.clone();
        let selected: Vec<String> = self.selected_tables.iter().cloned().collect();

        let (tx, rx) = mpsc::unbounded_channel();
        self.event_receiver = Some(rx);

        let runtime = Arc::clone(&self.runtime);

        std::thread::spawn(move || {
            runtime.block_on(async {
                let mut engine = MigrationEngine::new(conn_config, migration_config);
                engine.set_event_sender(tx.clone());

                if let Err(e) = engine.connect().await {
                    let _ = tx.send(MigrationEvent::MigrationError("connect".to_string(), e.to_string()));
                    return;
                }

                match engine.analyze_tables(selected).await {
                    Ok(_) => {
                        let _ = tx.send(MigrationEvent::AllComplete);
                    }
                    Err(e) => {
                        let _ = tx.send(MigrationEvent::MigrationError("analysis".to_string(), e.to_string()));
                    }
                }
            });
        });
    }

    fn start_migration(&mut self) {
        self.state = AppState::Migrating;
        self.start_time = Some(Utc::now());
        self.log("Iniciando migracion...");

        // Preparar tablas con las columnas configuradas
        let mut tables_to_migrate: Vec<TableInfo> = Vec::new();

        for (table_name, columns) in &self.analyzed_columns {
            let parts: Vec<&str> = table_name.split('.').collect();
            if parts.len() == 2 {
                let table = TableInfo {
                    schema: parts[0].to_string(),
                    name: parts[1].to_string(),
                    full_name: table_name.clone(),
                    row_count: self.tables.iter()
                        .find(|t| &t.full_name == table_name)
                        .map(|t| t.row_count)
                        .unwrap_or(0),
                    columns: columns.clone(),
                    primary_keys: Vec::new(),
                    foreign_keys: Vec::new(),
                };
                tables_to_migrate.push(table);
            }
        }

        self.total_rows_to_migrate = tables_to_migrate.iter().map(|t| t.row_count).sum();

        // Inicializar progreso
        for table in &tables_to_migrate {
            self.progress.insert(
                table.full_name.clone(),
                MigrationProgress::new(table.full_name.clone(), table.row_count)
            );
        }

        let conn_config = self.conn_config.clone();
        let migration_config = self.migration_config.clone();

        let (tx, rx) = mpsc::unbounded_channel();
        self.event_receiver = Some(rx);

        let runtime = Arc::clone(&self.runtime);

        std::thread::spawn(move || {
            runtime.block_on(async {
                let mut engine = MigrationEngine::new(conn_config, migration_config);
                engine.set_event_sender(tx.clone());

                if let Err(e) = engine.connect().await {
                    let _ = tx.send(MigrationEvent::MigrationError("connect".to_string(), e.to_string()));
                    return;
                }

                if let Err(e) = engine.migrate_tables(tables_to_migrate).await {
                    let _ = tx.send(MigrationEvent::MigrationError("migration".to_string(), e.to_string()));
                }
            });
        });
    }

    fn reset(&mut self) {
        self.state = AppState::Config;
        self.tables.clear();
        self.selected_tables.clear();
        self.analyzed_columns.clear();
        self.progress.clear();
        self.logs.clear();
        self.error = None;
        self.start_time = None;
        self.total_rows_to_migrate = 0;
        self.total_rows_migrated = 0;
        self.json_columns_found = 0;
        self.json_nulled_count = 0;
    }
}

impl eframe::App for MigrationApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        // Procesar eventos pendientes
        self.process_events();

        // Manejar transiciones de estado basadas en eventos
        if self.state == AppState::Connecting {
            // Verificar si llegaron tablas
            let table_events: Vec<String> = self.analyzed_columns.keys()
                .filter(|k| k.starts_with("__TABLE__:"))
                .cloned()
                .collect();

            if !table_events.is_empty() {
                for key in table_events {
                    let table_name = key.replace("__TABLE__:", "");
                    let parts: Vec<&str> = table_name.split('.').collect();
                    if parts.len() == 2 {
                        // Esto es simplificado - en realidad necesitamos los datos reales
                    }
                    self.analyzed_columns.remove(&key);
                }
                // Si tenemos tablas, transicionar
                if !self.tables.is_empty() {
                    self.state = AppState::TableSelection;
                    self.log("Conexion exitosa!");
                }
            }
        }

        if self.state == AppState::Analyzing {
            // Si todas las tablas fueron analizadas, transicionar
            if self.analyzed_columns.len() >= self.selected_tables.len() && !self.selected_tables.is_empty() {
                self.state = AppState::ColumnConfig;
                self.log("Analisis completado!");
            }
        }

        egui::CentralPanel::default().show(ctx, |ui| {
            ui.heading("Motor de Migracion SQL Server -> PostgreSQL");
            ui.separator();

            // Indicador de estado
            ui.horizontal(|ui| {
                ui.label("Estado:");
                let (state_text, state_color) = match self.state {
                    AppState::Config => ("Configuracion", egui::Color32::GRAY),
                    AppState::Connecting => ("Conectando...", egui::Color32::YELLOW),
                    AppState::TableSelection => ("Seleccion de Tablas", egui::Color32::LIGHT_BLUE),
                    AppState::Analyzing => ("Analizando...", egui::Color32::YELLOW),
                    AppState::ColumnConfig => ("Configuracion de Columnas", egui::Color32::LIGHT_BLUE),
                    AppState::Migrating => ("Migrando...", egui::Color32::YELLOW),
                    AppState::Complete => ("Completado", egui::Color32::GREEN),
                };
                ui.colored_label(state_color, state_text);
            });

            ui.separator();
            ui.add_space(10.0);

            match self.state {
                AppState::Config => self.render_config(ui),
                AppState::Connecting => {
                    ui.spinner();
                    ui.label("Conectando a las bases de datos...");
                }
                AppState::TableSelection => self.render_table_selection(ui),
                AppState::Analyzing => {
                    ui.spinner();
                    ui.label("Analizando tablas para detectar columnas JSON...");
                    egui::ScrollArea::vertical().max_height(200.0).show(ui, |ui| {
                        for log in &self.logs {
                            ui.label(log);
                        }
                    });
                }
                AppState::ColumnConfig => self.render_column_config(ui),
                AppState::Migrating => self.render_migration_progress(ui),
                AppState::Complete => self.render_complete(ui),
            }
        });

        // Solicitar repintado si hay operaciones en curso
        if matches!(self.state, AppState::Connecting | AppState::Analyzing | AppState::Migrating) {
            ctx.request_repaint();
        }
    }
}

fn status_icon(status: &MigrationStatus) -> &'static str {
    match status {
        MigrationStatus::Pending => "[ ]",
        MigrationStatus::Analyzing => "[~]",
        MigrationStatus::Analyzed => "[A]",
        MigrationStatus::Migrating => "[>]",
        MigrationStatus::Completed => "[OK]",
        MigrationStatus::Failed(_) => "[X]",
        MigrationStatus::Skipped => "[-]",
    }
}

/// Formatea un número con separadores de miles
fn format_number(n: i64) -> String {
    if n < 1000 {
        return n.to_string();
    }

    let s = n.to_string();
    let mut result = String::new();
    let chars: Vec<char> = s.chars().collect();
    let len = chars.len();

    for (i, c) in chars.iter().enumerate() {
        if i > 0 && (len - i) % 3 == 0 {
            result.push(',');
        }
        result.push(*c);
    }

    result
}
