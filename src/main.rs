//! Motor de Migración SQL Server a PostgreSQL
//!
//! # Características:
//! - Interfaz gráfica para selección de tablas
//! - Análisis paralelo de tablas
//! - Detección inteligente de columnas JSON
//! - Migración segura (sin borrar datos)
//!
//! # Método de Evaluación JSON:
//!
//! El motor usa un sistema de puntuación (0-100) para decidir si convertir columnas a JSON:
//!
//! ## Criterios (total 100 puntos):
//!
//! 1. **Tipo de dato** (30 pts máx):
//!    - NVARCHAR(MAX) o >1000 chars: +20 pts
//!    - XML: +25 pts
//!    - TEXT/NTEXT: +15 pts
//!
//! 2. **Estructura del contenido** (40 pts máx):
//!    - Comienza con '{' o '[': +15 pts
//!    - Es JSON válido parseable: +25 pts
//!
//! 3. **Consistencia** (20 pts máx):
//!    - >80% JSON válido: +20 pts
//!    - 50-80% JSON válido: +10 pts
//!
//! 4. **Beneficio JSONB** (10 pts máx):
//!    - Datos estructurados indexables: +10 pts
//!
//! ## Decisión:
//! - Score >= 60: CONVERTIR a JSON
//! - Score 40-59: Revisar manualmente
//! - Score < 40: NO convertir
//!
//! ## Seguridad:
//! - Si un valor no se puede convertir a JSON -> NULL
//! - Los datos originales quedan intactos en SQL Server
//! - NUNCA se borran datos

mod db;
mod migration;
mod analysis;
mod storage;
mod cli;

#[cfg(feature = "gui")]
mod gui;

fn main() {
    // Inicializar logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into())
        )
        .init();

    // Check for CLI mode (or if GUI feature is disabled)
    #[cfg(feature = "gui")]
    let cli_mode = std::env::args().any(|arg| arg == "--cli");

    #[cfg(not(feature = "gui"))]
    let cli_mode = true;

    if cli_mode {
        tracing::info!("Starting in CLI mode (headless)");

        // Run in CLI mode
        let rt = tokio::runtime::Runtime::new()
            .expect("Failed to create Tokio runtime");

        if let Err(e) = rt.block_on(cli::run()) {
            tracing::error!("Migration failed: {:#}", e);
            std::process::exit(1);
        }
    } else {
        #[cfg(feature = "gui")]
        {
            use eframe::egui;
            use gui::MigrationApp;

            tracing::info!("Iniciando Motor de Migracion SQL Server -> PostgreSQL (GUI)");

            // Configuración de la ventana
            let options = eframe::NativeOptions {
                viewport: egui::ViewportBuilder::default()
                    .with_inner_size([1200.0, 800.0])
                    .with_min_inner_size([800.0, 600.0])
                    .with_title("Motor de Migracion SQL Server -> PostgreSQL"),
                ..Default::default()
            };

            // Ejecutar aplicación GUI
            if let Err(e) = eframe::run_native(
                "SQL Migration Engine",
                options,
                Box::new(|cc| Ok(Box::new(MigrationApp::new(cc)))),
            ) {
                tracing::error!("GUI error: {}", e);
                std::process::exit(1);
            }
        }
    }
}
