//! BCP (Bulk Copy Program) wrapper for exporting SQL Server data

use anyhow::{Context, Result, anyhow};
use std::path::Path;
use std::process::Stdio;
use tokio::process::Command;

/// BCP execution mode
#[derive(Debug, Clone, PartialEq)]
pub enum BcpMode {
    /// Run BCP locally (must be installed on host)
    Local,
    /// Run BCP via Docker
    Docker {
        image: String,
    },
}

impl Default for BcpMode {
    fn default() -> Self {
        BcpMode::Docker {
            image: "mcr.microsoft.com/mssql-tools".to_string(),
        }
    }
}

/// BCP Exporter for SQL Server tables
pub struct BcpExporter {
    server: String,
    port: u16,
    database: String,
    user: String,
    password: String,
    mode: BcpMode,
}

impl BcpExporter {
    pub fn new(
        server: String,
        port: u16,
        database: String,
        user: String,
        password: String,
        mode: BcpMode,
    ) -> Self {
        Self {
            server,
            port,
            database,
            user,
            password,
            mode,
        }
    }

    /// Check if BCP is available (locally or via Docker)
    pub fn is_available(mode: &BcpMode) -> bool {
        match mode {
            BcpMode::Local => which::which("bcp").is_ok(),
            BcpMode::Docker { .. } => which::which("docker").is_ok(),
        }
    }

    /// Check if Docker is installed
    pub fn is_docker_available() -> bool {
        which::which("docker").is_ok()
    }

    /// Check if local BCP is installed
    pub fn is_local_installed() -> bool {
        which::which("bcp").is_ok()
    }

    /// Get installation instructions based on OS
    pub fn get_install_instructions() -> String {
        if cfg!(target_os = "macos") {
            r#"BCP no esta instalado. Para instalarlo en macOS:

1. Instalar Homebrew (si no lo tienes):
   /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

2. Agregar el tap de Microsoft:
   brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release

3. Instalar mssql-tools18:
   brew update
   HOMEBREW_ACCEPT_EULA=Y brew install mssql-tools18

4. Agregar al PATH (en ~/.zshrc o ~/.bashrc):
   export PATH="/opt/homebrew/opt/mssql-tools18/bin:$PATH"

5. Reiniciar terminal o ejecutar: source ~/.zshrc"#.to_string()
        } else if cfg!(target_os = "linux") {
            r#"BCP no esta instalado. Para instalarlo en Linux (Ubuntu/Debian):

1. Agregar repositorio de Microsoft:
   curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
   curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list | sudo tee /etc/apt/sources.list.d/msprod.list

2. Instalar mssql-tools:
   sudo apt-get update
   sudo ACCEPT_EULA=Y apt-get install -y mssql-tools unixodbc-dev

3. Agregar al PATH:
   echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc
   source ~/.bashrc"#.to_string()
        } else {
            "BCP no esta instalado. Descarga SQL Server Command Line Tools desde: https://docs.microsoft.com/en-us/sql/tools/bcp-utility".to_string()
        }
    }

    /// Export a table to CSV using BCP
    pub async fn export_table(
        &self,
        schema: &str,
        table: &str,
        output_path: &Path,
    ) -> Result<ExportResult> {
        if !Self::is_available(&self.mode) {
            return Err(anyhow!("BCP no esta disponible.\n\n{}", Self::get_install_instructions()));
        }

        let query = format!("SELECT * FROM [{}].[{}]", schema, table);

        // For Docker mode, replace localhost with host.docker.internal
        let effective_server = match &self.mode {
            BcpMode::Docker { .. } => {
                if self.server == "localhost" || self.server == "127.0.0.1" {
                    "host.docker.internal".to_string()
                } else {
                    self.server.clone()
                }
            }
            BcpMode::Local => self.server.clone(),
        };
        let server_with_port = format!("{},{}", effective_server, self.port);

        tracing::info!("Exportando {}.{} con BCP ({:?})...", schema, table, self.mode);
        tracing::info!("Conectando a: {}", server_with_port);

        let output = match &self.mode {
            BcpMode::Local => {
                Command::new("bcp")
                    .args([
                        &query,
                        "queryout",
                        output_path.to_str().unwrap(),
                        "-c",                    // Character mode (text)
                        "-t\t",                  // TAB delimiter (safer than comma)
                        "-r\n",                  // Row terminator
                        "-S", &server_with_port,
                        "-d", &self.database,
                        "-U", &self.user,
                        "-P", &self.password,
                    ])
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .output()
                    .await
                    .context("Error ejecutando BCP local")?
            }
            BcpMode::Docker { image } => {
                // Get the parent directory for volume mount
                let parent_dir = output_path.parent()
                    .ok_or_else(|| anyhow!("No se puede obtener directorio padre"))?;
                let filename = output_path.file_name()
                    .ok_or_else(|| anyhow!("No se puede obtener nombre de archivo"))?
                    .to_str()
                    .unwrap();

                let container_output = format!("/data/{}", filename);
                let volume_mount = format!("{}:/data", parent_dir.display());

                Command::new("docker")
                    .args([
                        "run",
                        "--rm",
                        "--platform", "linux/amd64",  // Force amd64 for ARM Macs
                        "-v", &volume_mount,
                        image,
                        "/opt/mssql-tools/bin/bcp",
                        &query,
                        "queryout",
                        &container_output,
                        "-c",                    // Character mode (text)
                        "-t\t",                  // TAB delimiter (safer than comma)
                        "-r\n",                  // Row terminator
                        "-S", &server_with_port,
                        "-d", &self.database,
                        "-U", &self.user,
                        "-P", &self.password,
                    ])
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .output()
                    .await
                    .context("Error ejecutando BCP via Docker")?
            }
        };

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        if !output.status.success() {
            return Err(anyhow!(
                "BCP fallo para {}.{}:\nstdout: {}\nstderr: {}",
                schema, table, stdout, stderr
            ));
        }

        // Parse row count from BCP output
        let rows_exported = Self::parse_row_count(&stdout);

        tracing::info!("Exportadas {} filas de {}.{}", rows_exported, schema, table);

        Ok(ExportResult {
            table_name: format!("{}.{}", schema, table),
            rows_exported,
            file_path: output_path.to_path_buf(),
        })
    }

    /// Export a table with a header row (column names)
    pub async fn export_table_with_header(
        &self,
        schema: &str,
        table: &str,
        columns: &[String],
        output_path: &Path,
    ) -> Result<ExportResult> {
        // First, create a temp file for the data
        let temp_data_path = output_path.with_extension("tmp");

        // Export data
        let result = self.export_table(schema, table, &temp_data_path).await?;

        // Read the exported data
        let data = tokio::fs::read_to_string(&temp_data_path).await
            .context("Error leyendo archivo temporal de BCP")?;

        // Create header (TAB-delimited to match BCP output)
        let header = columns.join("\t");

        // Write final file with header
        let final_content = format!("{}\n{}", header, data);
        tokio::fs::write(output_path, final_content).await
            .context("Error escribiendo archivo final con header")?;

        // Clean up temp file
        let _ = tokio::fs::remove_file(&temp_data_path).await;

        Ok(ExportResult {
            table_name: result.table_name,
            rows_exported: result.rows_exported,
            file_path: output_path.to_path_buf(),
        })
    }

    fn parse_row_count(output: &str) -> i64 {
        // BCP outputs something like "1000 rows copied."
        for line in output.lines() {
            if line.contains("rows copied") {
                if let Some(num_str) = line.split_whitespace().next() {
                    if let Ok(num) = num_str.parse::<i64>() {
                        return num;
                    }
                }
            }
        }
        0
    }
}

/// Result of a BCP export operation
#[derive(Debug, Clone)]
pub struct ExportResult {
    pub table_name: String,
    pub rows_exported: i64,
    pub file_path: std::path::PathBuf,
}

/// Escape a field for CSV format
fn escape_csv_field(field: &str) -> String {
    if field.contains(',') || field.contains('"') || field.contains('\n') {
        format!("\"{}\"", field.replace('"', "\"\""))
    } else {
        field.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_escape_csv_field() {
        assert_eq!(escape_csv_field("simple"), "simple");
        assert_eq!(escape_csv_field("with,comma"), "\"with,comma\"");
        assert_eq!(escape_csv_field("with\"quote"), "\"with\"\"quote\"");
    }

    #[test]
    fn test_parse_row_count() {
        assert_eq!(BcpExporter::parse_row_count("1000 rows copied."), 1000);
        assert_eq!(BcpExporter::parse_row_count("Starting copy...\n500 rows copied.\nDone."), 500);
        assert_eq!(BcpExporter::parse_row_count("No rows"), 0);
    }
}
