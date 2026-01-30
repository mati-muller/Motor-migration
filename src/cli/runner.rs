//! CLI runner - headless migration execution

use anyhow::{Context, Result};
use std::time::Instant;

use crate::migration::MigrationEngine;
use super::config::CliConfig;

/// Run migration in CLI mode
pub async fn run() -> Result<()> {
    let start_time = Instant::now();

    // Parse configuration from environment
    let config = CliConfig::from_env()
        .context("Error parsing configuration from environment")?;

    tracing::info!("===========================================");
    tracing::info!("SQL Server → PostgreSQL Migration (CLI Mode)");
    tracing::info!("===========================================");
    tracing::info!("");
    tracing::info!("Source: {}:{}/{}",
        config.sqlserver_host,
        config.sqlserver_port,
        config.sqlserver_database
    );
    tracing::info!("Target: {}:{}/{}",
        config.postgres_host,
        config.postgres_port,
        config.postgres_database
    );
    tracing::info!("Mode: {}", config.mode);
    tracing::info!("Parallelism: analysis={}, migration={}",
        config.parallel_analysis,
        config.parallel_migration
    );
    tracing::info!("Batch size: {}", config.batch_size);
    tracing::info!("JSON threshold: {}", config.json_threshold);
    if !config.tables.is_empty() {
        tracing::info!("Tables filter: {:?}", config.tables);
    }
    tracing::info!("");

    // Create migration engine
    let mut engine = MigrationEngine::new(
        config.connection_config(),
        config.migration_config(),
    );

    // Connect to databases
    tracing::info!("Connecting to databases...");
    engine.connect().await
        .context("Failed to connect to databases")?;
    tracing::info!("Connected successfully!");

    // Fetch tables
    tracing::info!("Fetching tables from SQL Server...");
    let all_tables = engine.fetch_tables().await
        .context("Failed to fetch tables")?;
    tracing::info!("Found {} tables", all_tables.len());

    // Filter tables if specified
    let tables_to_migrate: Vec<_> = if config.tables.is_empty() {
        all_tables
    } else {
        all_tables.into_iter()
            .filter(|t| {
                config.tables.iter().any(|filter| {
                    t.full_name.eq_ignore_ascii_case(filter) ||
                    t.name.eq_ignore_ascii_case(filter)
                })
            })
            .collect()
    };

    if tables_to_migrate.is_empty() {
        tracing::warn!("No tables to migrate!");
        return Ok(());
    }

    tracing::info!("Tables to migrate: {}", tables_to_migrate.len());
    for table in &tables_to_migrate {
        tracing::info!("  - {} ({} rows)", table.full_name, table.row_count);
    }
    tracing::info!("");

    // Run migration based on mode
    if config.mode == "export" {
        run_export_mode(&mut engine, tables_to_migrate).await?;
    } else {
        run_direct_mode(&mut engine, tables_to_migrate).await?;
    }

    let elapsed = start_time.elapsed();
    tracing::info!("");
    tracing::info!("===========================================");
    tracing::info!("Migration completed in {:.2?}", elapsed);
    tracing::info!("===========================================");

    Ok(())
}

/// Direct mode: SQL Server → PostgreSQL
async fn run_direct_mode(
    engine: &mut MigrationEngine,
    tables: Vec<crate::db::types::TableInfo>,
) -> Result<()> {
    tracing::info!("Running DIRECT mode migration...");
    tracing::info!("");

    // Analyze tables for JSON columns
    tracing::info!("Phase 1: Analyzing tables for JSON columns...");
    let table_names: Vec<String> = tables.iter()
        .map(|t| t.full_name.clone())
        .collect();

    let analysis_results = engine.analyze_tables(table_names.clone()).await
        .context("Failed to analyze tables")?;

    // Log analysis results
    for (table_name, columns) in &analysis_results {
        let json_cols: Vec<_> = columns.iter()
            .filter(|c| c.should_convert_to_json)
            .collect();
        if !json_cols.is_empty() {
            tracing::info!("  {} - {} JSON columns detected:", table_name, json_cols.len());
            for col in json_cols {
                tracing::info!("    - {} (score: {})", col.name, col.json_score);
            }
        }
    }
    tracing::info!("");

    // Update tables with analyzed columns
    let mut tables_with_columns = tables;
    for table in &mut tables_with_columns {
        if let Some(columns) = analysis_results.get(&table.full_name) {
            table.columns = columns.clone();
        }
    }

    // Migrate tables
    tracing::info!("Phase 2: Migrating tables...");
    engine.migrate_tables(tables_with_columns).await
        .context("Failed to migrate tables")?;

    Ok(())
}

/// Export mode: SQL Server → BCP → S3 → PostgreSQL
async fn run_export_mode(
    engine: &mut MigrationEngine,
    tables: Vec<crate::db::types::TableInfo>,
) -> Result<()> {
    tracing::info!("Running EXPORT mode migration (via S3)...");
    tracing::info!("");

    // Export to S3
    tracing::info!("Phase 1: Exporting tables with BCP to S3...");
    let export_path = engine.export_to_s3(&tables).await
        .context("Failed to export to S3")?;
    tracing::info!("Exported to: {:?}", export_path);
    tracing::info!("");

    // Analyze from files
    tracing::info!("Phase 2: Analyzing tables from exported files...");
    let table_names: Vec<String> = tables.iter()
        .map(|t| t.full_name.clone())
        .collect();

    let analysis_results = engine.analyze_tables_from_files(table_names.clone(), &export_path).await
        .context("Failed to analyze tables from files")?;

    // Log analysis results
    for (table_name, columns) in &analysis_results {
        let json_cols: Vec<_> = columns.iter()
            .filter(|c| c.should_convert_to_json)
            .collect();
        if !json_cols.is_empty() {
            tracing::info!("  {} - {} JSON columns detected", table_name, json_cols.len());
        }
    }
    tracing::info!("");

    // Update tables with analyzed columns
    let mut tables_with_columns = tables;
    for table in &mut tables_with_columns {
        if let Some(columns) = analysis_results.get(&table.full_name) {
            table.columns = columns.clone();
        }
    }

    // Migrate from files
    tracing::info!("Phase 3: Migrating tables from files...");
    engine.migrate_tables_from_files(tables_with_columns, &export_path).await
        .context("Failed to migrate tables from files")?;

    Ok(())
}
