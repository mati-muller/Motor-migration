//! File-based data source for reading exported CSV files

use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::fs::File;
use std::io::Read as IoRead;

/// Row terminator used by BCP export (must match bcp.rs)
const ROW_TERMINATOR: &str = "|||ROW|||";

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

    /// Read file content and split into rows
    fn read_raw_rows(&self, schema: &str, table: &str) -> Result<Vec<String>> {
        let file_path = self.get_file_path(schema, table);
        let mut file = File::open(&file_path)
            .context(format!("Error abriendo archivo: {}", file_path.display()))?;

        let mut content = String::new();
        file.read_to_string(&mut content)
            .context("Error leyendo archivo")?;

        // Split by custom row terminator
        let rows: Vec<String> = content
            .split(ROW_TERMINATOR)
            .map(|s| s.to_string())
            .filter(|s| !s.trim().is_empty())
            .collect();

        Ok(rows)
    }

    /// Parse a row string into fields (TAB-delimited)
    fn parse_row(row: &str) -> Vec<Option<String>> {
        row.split('\t')
            .map(|field| {
                let trimmed = field.trim();
                if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("NULL") {
                    None
                } else {
                    Some(trimmed.to_string())
                }
            })
            .collect()
    }

    /// Get the row count from file (excluding header)
    pub fn get_row_count(&self, schema: &str, table: &str) -> Result<i64> {
        let rows = self.read_raw_rows(schema, table)?;
        // Subtract 1 for header if file has content
        Ok(if rows.len() > 1 { (rows.len() - 1) as i64 } else { 0 })
    }

    /// Read column names from header (first row, TAB-delimited)
    pub fn read_columns(&self, schema: &str, table: &str) -> Result<Vec<String>> {
        let rows = self.read_raw_rows(schema, table)?;

        if rows.is_empty() {
            return Ok(Vec::new());
        }

        // First row is the header
        let header = &rows[0];
        let columns: Vec<String> = header
            .split('\t')
            .map(|s| s.trim().to_string())
            .collect();

        Ok(columns)
    }

    /// Read rows from file with pagination
    pub fn read_rows(
        &self,
        schema: &str,
        table: &str,
        _columns: &[String],
        offset: i64,
        limit: i64,
    ) -> Result<Vec<Vec<Option<String>>>> {
        let raw_rows = self.read_raw_rows(schema, table)?;

        if raw_rows.len() <= 1 {
            return Ok(Vec::new()); // No data rows (only header or empty)
        }

        let mut rows = Vec::new();

        // Skip header (index 0), start from data rows (index 1)
        for (i, raw_row) in raw_rows.iter().skip(1).enumerate() {
            let idx = i as i64;

            // Skip rows before offset
            if idx < offset {
                continue;
            }

            // Stop after limit
            if idx >= offset + limit {
                break;
            }

            let row = Self::parse_row(raw_row);
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

    /// Sample column data for JSON analysis
    pub fn sample_column_for_json(
        &self,
        schema: &str,
        table: &str,
        column_name: &str,
        sample_size: u32,
    ) -> Result<Vec<Option<String>>> {
        let raw_rows = self.read_raw_rows(schema, table)?;

        if raw_rows.is_empty() {
            return Ok(Vec::new());
        }

        // Find column index from header
        let headers: Vec<&str> = raw_rows[0].split('\t').map(|s| s.trim()).collect();
        let col_index = headers.iter().position(|h| *h == column_name);

        let col_index = match col_index {
            Some(idx) => idx,
            None => {
                tracing::warn!("Columna '{}' no encontrada en CSV, columnas disponibles: {:?}",
                    column_name, headers);
                return Ok(Vec::new());
            }
        };

        let mut samples = Vec::new();
        let mut count = 0u32;

        // Skip header, iterate data rows
        for raw_row in raw_rows.iter().skip(1) {
            if count >= sample_size {
                break;
            }

            let fields: Vec<&str> = raw_row.split('\t').collect();
            if let Some(field) = fields.get(col_index) {
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
    fn test_read_with_custom_terminator() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("dbo.test_table.csv");

        // Create test file with custom row terminator
        let mut file = File::create(&file_path).unwrap();
        write!(file, "id\tname\tdata|||ROW|||").unwrap();
        write!(file, "1\tJohn\t{{\"key\":\"value\"}}|||ROW|||").unwrap();
        write!(file, "2\tJane\tNULL|||ROW|||").unwrap();
        write!(file, "3\tBob\t|||ROW|||").unwrap();

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
    fn test_multiline_fields() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("dbo.multiline.csv");

        // Create test file with multiline data
        let mut file = File::create(&file_path).unwrap();
        write!(file, "id\tdescription|||ROW|||").unwrap();
        write!(file, "1\tLine 1\nLine 2\nLine 3|||ROW|||").unwrap();
        write!(file, "2\tSimple|||ROW|||").unwrap();

        let source = FileDataSource::new(dir.path().to_path_buf());

        let rows = source.read_rows("dbo", "multiline", &["id".to_string(), "description".to_string()], 0, 10).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Some("1".to_string()));
        assert!(rows[0][1].as_ref().unwrap().contains("Line 1\nLine 2"));
    }

    #[test]
    fn test_pagination() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("dbo.paged.csv");

        let mut file = File::create(&file_path).unwrap();
        write!(file, "id|||ROW|||").unwrap();
        for i in 1..=10 {
            write!(file, "{}|||ROW|||", i).unwrap();
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
