//! File-based data source for reading exported CSV files

use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::fs::File;
use std::io::{BufRead, BufReader};

/// Data source that reads from CSV files exported by BCP
pub struct FileDataSource {
    base_path: PathBuf,
}

impl FileDataSource {
    pub fn new(base_path: PathBuf) -> Self {
        Self { base_path }
    }

    /// Get the file path for a table
    pub fn get_file_path(&self, schema: &str, table: &str) -> PathBuf {
        self.base_path.join(format!("{}.{}.csv", schema, table))
    }

    /// Check if the file for a table exists
    pub fn table_file_exists(&self, schema: &str, table: &str) -> bool {
        self.get_file_path(schema, table).exists()
    }

    /// Get the row count from a CSV file (excluding header)
    pub fn get_row_count(&self, schema: &str, table: &str) -> Result<i64> {
        let file_path = self.get_file_path(schema, table);
        let file = File::open(&file_path)
            .context(format!("Error abriendo archivo: {}", file_path.display()))?;

        let reader = BufReader::new(file);
        let count = reader.lines().count();

        // Subtract 1 for header if file has content
        Ok(if count > 0 { (count - 1) as i64 } else { 0 })
    }

    /// Read column names from CSV header (TAB-delimited from BCP)
    pub fn read_columns(&self, schema: &str, table: &str) -> Result<Vec<String>> {
        let file_path = self.get_file_path(schema, table);
        let file = File::open(&file_path)
            .context(format!("Error abriendo archivo: {}", file_path.display()))?;

        let mut reader = csv::ReaderBuilder::new()
            .delimiter(b'\t')  // BCP uses TAB delimiter
            .has_headers(true)
            .flexible(true)    // Allow variable number of fields
            .from_reader(file);

        let headers = reader.headers()
            .context("Error leyendo headers del CSV")?;

        Ok(headers.iter().map(|s| s.to_string()).collect())
    }

    /// Read rows from a CSV file with pagination (TAB-delimited from BCP)
    pub fn read_rows(
        &self,
        schema: &str,
        table: &str,
        _columns: &[String],
        offset: i64,
        limit: i64,
    ) -> Result<Vec<Vec<Option<String>>>> {
        let file_path = self.get_file_path(schema, table);
        let file = File::open(&file_path)
            .context(format!("Error abriendo archivo: {}", file_path.display()))?;

        let mut reader = csv::ReaderBuilder::new()
            .delimiter(b'\t')  // BCP uses TAB delimiter
            .has_headers(true)
            .flexible(true)    // Allow variable number of fields
            .from_reader(file);

        let mut rows = Vec::new();

        for (i, result) in reader.records().enumerate() {
            let idx = i as i64;

            // Skip rows before offset
            if idx < offset {
                continue;
            }

            // Stop after limit
            if idx >= offset + limit {
                break;
            }

            let record = match result {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!("Error leyendo fila {}: {}", i, e);
                    continue;  // Skip problematic rows
                }
            };

            let row: Vec<Option<String>> = record.iter()
                .map(|field| {
                    let trimmed = field.trim();
                    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("NULL") {
                        None
                    } else {
                        Some(trimmed.to_string())
                    }
                })
                .collect();

            rows.push(row);
        }

        Ok(rows)
    }

    /// Read rows asynchronously (wrapper for sync read)
    pub async fn read_rows_async(
        &self,
        schema: &str,
        table: &str,
        columns: &[String],
        offset: i64,
        limit: i64,
    ) -> Result<Vec<Vec<Option<String>>>> {
        let schema = schema.to_string();
        let table = table.to_string();
        let columns = columns.to_vec();
        let base_path = self.base_path.clone();

        // Run blocking IO in a separate thread
        tokio::task::spawn_blocking(move || {
            let source = FileDataSource::new(base_path);
            source.read_rows(&schema, &table, &columns, offset, limit)
        })
        .await
        .context("Error en task de lectura")?
    }

    /// Sample column data for JSON analysis (TAB-delimited from BCP)
    pub fn sample_column_for_json(
        &self,
        schema: &str,
        table: &str,
        column_name: &str,
        sample_size: u32,
    ) -> Result<Vec<Option<String>>> {
        let file_path = self.get_file_path(schema, table);
        let file = File::open(&file_path)
            .context(format!("Error abriendo archivo: {}", file_path.display()))?;

        let mut reader = csv::ReaderBuilder::new()
            .delimiter(b'\t')  // BCP uses TAB delimiter
            .has_headers(true)
            .flexible(true)    // Allow variable number of fields
            .from_reader(file);

        // Find column index
        let headers = reader.headers()
            .context("Error leyendo headers")?;

        let col_index = headers.iter()
            .position(|h| h == column_name);

        let col_index = match col_index {
            Some(idx) => idx,
            None => {
                tracing::warn!("Columna '{}' no encontrada en CSV, columnas disponibles: {:?}",
                    column_name, headers.iter().collect::<Vec<_>>());
                return Ok(Vec::new());
            }
        };

        let mut samples = Vec::new();
        let mut count = 0u32;

        for result in reader.records() {
            if count >= sample_size {
                break;
            }

            let record = match result {
                Ok(r) => r,
                Err(_) => continue,  // Skip problematic rows
            };

            if let Some(field) = record.get(col_index) {
                let trimmed = field.trim();
                if !trimmed.is_empty() && !trimmed.eq_ignore_ascii_case("NULL") {
                    samples.push(Some(trimmed.to_string()));
                    count += 1;
                }
            }
        }

        Ok(samples)
    }

    /// Sample column data asynchronously
    pub async fn sample_column_for_json_async(
        &self,
        schema: &str,
        table: &str,
        column_name: &str,
        sample_size: u32,
    ) -> Result<Vec<Option<String>>> {
        let schema = schema.to_string();
        let table = table.to_string();
        let column_name = column_name.to_string();
        let base_path = self.base_path.clone();

        tokio::task::spawn_blocking(move || {
            let source = FileDataSource::new(base_path);
            source.sample_column_for_json(&schema, &table, &column_name, sample_size)
        })
        .await
        .context("Error en task de muestreo")?
    }

    /// Get list of available table files
    pub fn list_tables(&self) -> Result<Vec<(String, String)>> {
        let mut tables = Vec::new();

        let entries = std::fs::read_dir(&self.base_path)
            .context("Error leyendo directorio")?;

        for entry in entries {
            let entry = entry?;
            let path = entry.path();

            if path.extension().map(|e| e == "csv").unwrap_or(false) {
                if let Some(filename) = path.file_stem() {
                    let name = filename.to_string_lossy();
                    // Parse "schema.table" format
                    let parts: Vec<&str> = name.split('.').collect();
                    if parts.len() >= 2 {
                        tables.push((parts[0].to_string(), parts[1..].join(".")));
                    }
                }
            }
        }

        Ok(tables)
    }

    /// Get base path
    pub fn base_path(&self) -> &Path {
        &self.base_path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn test_read_csv() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("dbo.test_table.csv");

        // Create test CSV
        let mut file = File::create(&file_path).unwrap();
        writeln!(file, "id,name,data").unwrap();
        writeln!(file, "1,John,{{\"key\":\"value\"}}").unwrap();
        writeln!(file, "2,Jane,NULL").unwrap();
        writeln!(file, "3,Bob,").unwrap();

        let source = FileDataSource::new(dir.path().to_path_buf());

        // Test column reading
        let columns = source.read_columns("dbo", "test_table").unwrap();
        assert_eq!(columns, vec!["id", "name", "data"]);

        // Test row reading
        let rows = source.read_rows("dbo", "test_table", &columns, 0, 10).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0], Some("1".to_string()));
        assert_eq!(rows[0][1], Some("John".to_string()));
        assert_eq!(rows[1][2], None); // NULL
        assert_eq!(rows[2][2], None); // empty

        // Test row count
        let count = source.get_row_count("dbo", "test_table").unwrap();
        assert_eq!(count, 3);
    }

    #[test]
    fn test_pagination() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("dbo.paged.csv");

        let mut file = File::create(&file_path).unwrap();
        writeln!(file, "id").unwrap();
        for i in 1..=10 {
            writeln!(file, "{}", i).unwrap();
        }

        let source = FileDataSource::new(dir.path().to_path_buf());

        // Read first 3 rows
        let rows = source.read_rows("dbo", "paged", &["id".to_string()], 0, 3).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0], Some("1".to_string()));

        // Read rows 4-6
        let rows = source.read_rows("dbo", "paged", &["id".to_string()], 3, 3).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0], Some("4".to_string()));

        // Read beyond end
        let rows = source.read_rows("dbo", "paged", &["id".to_string()], 8, 5).unwrap();
        assert_eq!(rows.len(), 2); // Only 2 rows left
    }
}
