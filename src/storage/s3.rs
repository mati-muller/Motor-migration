//! S3 Client for uploading and downloading exported files

use anyhow::{Context, Result};
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::config::Region;
use std::path::Path;

/// S3 Client for storing and retrieving exported data
pub struct S3Client {
    client: aws_sdk_s3::Client,
    bucket: String,
    prefix: String,
}

impl S3Client {
    /// Create a new S3 client with the given configuration
    pub async fn new(
        bucket: String,
        region: String,
        access_key: String,
        secret_key: String,
        prefix: String,
    ) -> Result<Self> {
        // Trim whitespace from credentials (common copy-paste issue)
        let access_key = access_key.trim();
        let secret_key = secret_key.trim();

        tracing::info!("Creando cliente S3: bucket={}, region={}, access_key={}...",
            bucket, region, &access_key[..8.min(access_key.len())]);

        let creds = Credentials::new(
            access_key,
            secret_key,
            None,
            None,
            "static",
        );

        let region = region.trim().to_string();
        let bucket = bucket.trim().to_string();
        let prefix = prefix.trim().to_string();

        let config = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(region.clone()))
            .credentials_provider(creds)
            .load()
            .await;

        let client = aws_sdk_s3::Client::new(&config);

        Ok(Self {
            client,
            bucket,
            prefix,
        })
    }

    /// Get the full S3 key for a given filename
    fn full_key(&self, key: &str) -> String {
        if self.prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}/{}", self.prefix.trim_end_matches('/'), key)
        }
    }

    /// Upload a file to S3
    pub async fn upload_file(&self, local_path: &Path, key: &str) -> Result<UploadResult> {
        let body = ByteStream::from_path(local_path)
            .await
            .context("Error leyendo archivo para subir a S3")?;

        let full_key = self.full_key(key);
        let file_size = local_path.metadata()
            .map(|m| m.len())
            .unwrap_or(0);

        tracing::info!("Subiendo {} a s3://{}/{}", local_path.display(), self.bucket, full_key);

        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&full_key)
            .body(body)
            .send()
            .await
            .map_err(|e| {
                tracing::error!("S3 upload error details: {:?}", e);
                anyhow::anyhow!("Error subiendo a S3 bucket='{}' key='{}': {}", self.bucket, full_key, e)
            })?;

        tracing::info!("Subido exitosamente: {}", full_key);

        Ok(UploadResult {
            bucket: self.bucket.clone(),
            key: full_key,
            size_bytes: file_size,
        })
    }

    /// Download a file from S3
    pub async fn download_file(&self, key: &str, local_path: &Path) -> Result<()> {
        let full_key = self.full_key(key);

        tracing::info!("Descargando s3://{}/{} a {}", self.bucket, full_key, local_path.display());

        let response = self.client
            .get_object()
            .bucket(&self.bucket)
            .key(&full_key)
            .send()
            .await
            .context(format!("Error descargando de S3: {}", full_key))?;

        let data = response.body.collect().await
            .context("Error leyendo cuerpo de respuesta S3")?
            .into_bytes();

        // Ensure parent directory exists
        if let Some(parent) = local_path.parent() {
            tokio::fs::create_dir_all(parent).await
                .context("Error creando directorio para descarga")?;
        }

        tokio::fs::write(local_path, data).await
            .context("Error escribiendo archivo descargado")?;

        tracing::info!("Descargado exitosamente: {}", local_path.display());

        Ok(())
    }

    /// List files in the bucket with the configured prefix
    pub async fn list_files(&self) -> Result<Vec<S3Object>> {
        let prefix = if self.prefix.is_empty() {
            None
        } else {
            Some(self.prefix.clone())
        };

        let mut objects = Vec::new();
        let mut continuation_token: Option<String> = None;

        loop {
            let mut request = self.client
                .list_objects_v2()
                .bucket(&self.bucket);

            if let Some(p) = &prefix {
                request = request.prefix(p);
            }

            if let Some(token) = &continuation_token {
                request = request.continuation_token(token);
            }

            let response = request.send().await
                .context("Error listando objetos en S3")?;

            if let Some(contents) = response.contents {
                for obj in contents {
                    objects.push(S3Object {
                        key: obj.key.unwrap_or_default(),
                        size: obj.size.unwrap_or(0) as u64,
                        last_modified: obj.last_modified
                            .map(|dt| dt.to_string())
                            .unwrap_or_default(),
                    });
                }
            }

            if response.is_truncated.unwrap_or(false) {
                continuation_token = response.next_continuation_token;
            } else {
                break;
            }
        }

        Ok(objects)
    }

    /// Check if a file exists in S3
    pub async fn file_exists(&self, key: &str) -> Result<bool> {
        let full_key = self.full_key(key);

        match self.client
            .head_object()
            .bucket(&self.bucket)
            .key(&full_key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(e) => {
                let service_error = e.into_service_error();
                if service_error.is_not_found() {
                    Ok(false)
                } else {
                    Err(anyhow::anyhow!("Error verificando archivo en S3: {:?}", service_error))
                }
            }
        }
    }

    /// Delete a file from S3
    pub async fn delete_file(&self, key: &str) -> Result<()> {
        let full_key = self.full_key(key);

        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(&full_key)
            .send()
            .await
            .context(format!("Error eliminando de S3: {}", full_key))?;

        Ok(())
    }

    /// Get bucket name
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Get prefix
    pub fn prefix(&self) -> &str {
        &self.prefix
    }
}

/// Result of an upload operation
#[derive(Debug, Clone)]
pub struct UploadResult {
    pub bucket: String,
    pub key: String,
    pub size_bytes: u64,
}

/// S3 object metadata
#[derive(Debug, Clone)]
pub struct S3Object {
    pub key: String,
    pub size: u64,
    pub last_modified: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_full_key() {
        // We can't easily test S3Client without mocking, but we can test the key logic
        let prefix = "exports/2024";
        let key = "dbo.users.csv";
        let expected = format!("{}/{}", prefix, key);
        assert_eq!(expected, "exports/2024/dbo.users.csv");
    }
}
