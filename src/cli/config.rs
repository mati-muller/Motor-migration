//! CLI configuration from environment variables

use anyhow::{Context, Result};
use std::env;

use crate::db::ConnectionConfig;
use crate::migration::MigrationConfig;

/// CLI configuration parsed from environment variables
#[derive(Debug, Clone)]
pub struct CliConfig {
    // SQL Server
    pub sqlserver_host: String,
    pub sqlserver_port: u16,
    pub sqlserver_database: String,
    pub sqlserver_user: String,
    pub sqlserver_password: String,
    pub sqlserver_trust_cert: bool,

    // PostgreSQL
    pub postgres_host: String,
    pub postgres_port: u16,
    pub postgres_database: String,
    pub postgres_user: String,
    pub postgres_password: String,

    // S3 (optional)
    pub s3_enabled: bool,
    pub s3_bucket: String,
    pub s3_region: String,
    pub s3_access_key: String,
    pub s3_secret_key: String,
    pub s3_prefix: String,

    // BCP
    pub bcp_use_docker: bool,
    pub bcp_docker_image: String,

    // Migration
    pub parallel_analysis: usize,
    pub parallel_migration: usize,
    pub batch_size: i64,
    pub json_threshold: u8,

    // Tables filter (comma-separated, empty = all)
    pub tables: Vec<String>,

    // Mode: "direct" or "export" (via S3)
    pub mode: String,
}

impl CliConfig {
    /// Parse configuration from environment variables
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            // SQL Server (required)
            sqlserver_host: env::var("SQLSERVER_HOST")
                .context("SQLSERVER_HOST is required")?,
            sqlserver_port: env::var("SQLSERVER_PORT")
                .unwrap_or_else(|_| "1433".to_string())
                .parse()
                .context("SQLSERVER_PORT must be a number")?,
            sqlserver_database: env::var("SQLSERVER_DATABASE")
                .context("SQLSERVER_DATABASE is required")?,
            sqlserver_user: env::var("SQLSERVER_USER")
                .context("SQLSERVER_USER is required")?,
            sqlserver_password: env::var("SQLSERVER_PASSWORD")
                .context("SQLSERVER_PASSWORD is required")?,
            sqlserver_trust_cert: env::var("SQLSERVER_TRUST_CERT")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .unwrap_or(true),

            // PostgreSQL (required)
            postgres_host: env::var("POSTGRES_HOST")
                .context("POSTGRES_HOST is required")?,
            postgres_port: env::var("POSTGRES_PORT")
                .unwrap_or_else(|_| "5432".to_string())
                .parse()
                .context("POSTGRES_PORT must be a number")?,
            postgres_database: env::var("POSTGRES_DATABASE")
                .context("POSTGRES_DATABASE is required")?,
            postgres_user: env::var("POSTGRES_USER")
                .context("POSTGRES_USER is required")?,
            postgres_password: env::var("POSTGRES_PASSWORD")
                .context("POSTGRES_PASSWORD is required")?,

            // S3 (optional)
            s3_enabled: env::var("S3_ENABLED")
                .unwrap_or_else(|_| "false".to_string())
                .parse()
                .unwrap_or(false),
            s3_bucket: env::var("S3_BUCKET").unwrap_or_default(),
            s3_region: env::var("S3_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
            s3_access_key: env::var("S3_ACCESS_KEY").unwrap_or_default(),
            s3_secret_key: env::var("S3_SECRET_KEY").unwrap_or_default(),
            s3_prefix: env::var("S3_PREFIX").unwrap_or_default(),

            // BCP
            bcp_use_docker: env::var("BCP_USE_DOCKER")
                .unwrap_or_else(|_| "false".to_string())
                .parse()
                .unwrap_or(false),
            bcp_docker_image: env::var("BCP_DOCKER_IMAGE")
                .unwrap_or_else(|_| "mcr.microsoft.com/mssql-tools".to_string()),

            // Migration config
            parallel_analysis: env::var("PARALLEL_ANALYSIS")
                .unwrap_or_else(|_| "8".to_string())
                .parse()
                .unwrap_or(8),
            parallel_migration: env::var("PARALLEL_MIGRATION")
                .unwrap_or_else(|_| "4".to_string())
                .parse()
                .unwrap_or(4),
            batch_size: env::var("BATCH_SIZE")
                .unwrap_or_else(|_| "5000".to_string())
                .parse()
                .unwrap_or(5000),
            json_threshold: env::var("JSON_THRESHOLD")
                .unwrap_or_else(|_| "73".to_string())
                .parse()
                .unwrap_or(73),

            // Tables filter
            tables: env::var("TABLES")
                .unwrap_or_default()
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect(),

            // Mode
            mode: env::var("MODE").unwrap_or_else(|_| "direct".to_string()),
        })
    }

    /// Convert to ConnectionConfig for MigrationEngine
    pub fn connection_config(&self) -> ConnectionConfig {
        ConnectionConfig {
            sqlserver_host: self.sqlserver_host.clone(),
            sqlserver_port: self.sqlserver_port,
            sqlserver_database: self.sqlserver_database.clone(),
            sqlserver_user: self.sqlserver_user.clone(),
            sqlserver_password: self.sqlserver_password.clone(),
            sqlserver_trust_cert: self.sqlserver_trust_cert,

            postgres_host: self.postgres_host.clone(),
            postgres_port: self.postgres_port,
            postgres_database: self.postgres_database.clone(),
            postgres_user: self.postgres_user.clone(),
            postgres_password: self.postgres_password.clone(),

            s3_enabled: self.s3_enabled,
            s3_bucket: self.s3_bucket.clone(),
            s3_region: self.s3_region.clone(),
            s3_access_key: self.s3_access_key.clone(),
            s3_secret_key: self.s3_secret_key.clone(),
            s3_prefix: self.s3_prefix.clone(),

            bcp_use_docker: self.bcp_use_docker,
            bcp_docker_image: self.bcp_docker_image.clone(),
        }
    }

    /// Convert to MigrationConfig for MigrationEngine
    pub fn migration_config(&self) -> MigrationConfig {
        MigrationConfig {
            parallel_analysis: self.parallel_analysis,
            parallel_migration: self.parallel_migration,
            batch_size: self.batch_size,
            json_sample_size: 100,
            json_threshold: self.json_threshold,
        }
    }
}
