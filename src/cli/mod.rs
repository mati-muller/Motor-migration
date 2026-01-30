//! CLI module for headless migration
//!
//! Allows running migrations without GUI, configured via environment variables.

mod config;
mod runner;

pub use config::CliConfig;
pub use runner::run;
