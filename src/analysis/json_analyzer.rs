//! Analizador de columnas para determinar si conviene convertir a JSON
//!
//! # Método de Evaluación para JSON
//!
//! El motor evalúa cada columna usando un sistema de puntuación (0-100) basado en:
//!
//! ## Criterios de Evaluación:
//!
//! 1. **Tipo de dato original (30 puntos máx)**:
//!    - NVARCHAR/VARCHAR con MAX o >1000 chars: +20 puntos
//!    - XML: +25 puntos (buena candidata para JSON)
//!    - TEXT/NTEXT: +15 puntos
//!
//! 2. **Estructura del contenido (40 puntos máx)**:
//!    - Empieza con '{' o '[': +15 puntos
//!    - Es JSON válido parseable: +25 puntos
//!    - Contiene patrones JSON (":": {): +10 puntos
//!
//! 3. **Consistencia de datos (20 puntos máx)**:
//!    - >80% de valores son JSON válido: +20 puntos
//!    - 50-80% de valores son JSON válido: +10 puntos
//!    - <50%: 0 puntos
//!
//! 4. **Beneficio de migración (10 puntos máx)**:
//!    - Datos estructurados que PostgreSQL puede indexar con JSONB: +10 puntos
//!
//! ## Umbral de decisión:
//! - Score >= 60: RECOMENDAR conversión a JSON
//! - Score 40-59: SUGERIR revisión manual
//! - Score < 40: NO convertir a JSON
//!
//! ## Comportamiento de seguridad:
//! - Si un valor no puede parsearse como JSON, se inserta NULL
//! - Nunca se pierden datos: el original queda en SQL Server

use serde_json::Value as JsonValue;
use crate::db::types::{ColumnInfo, JsonAnalysisResult};

/// Analizador de columnas para JSON
pub struct JsonAnalyzer {
    /// Tamaño de muestra para análisis
    sample_size: u32,
    /// Umbral para recomendar JSON (0-100)
    recommendation_threshold: u8,
}

impl Default for JsonAnalyzer {
    fn default() -> Self {
        Self {
            sample_size: 100,
            recommendation_threshold: 60,
        }
    }
}

impl JsonAnalyzer {
    pub fn new(sample_size: u32, threshold: u8) -> Self {
        Self {
            sample_size,
            recommendation_threshold: threshold,
        }
    }

    /// Analiza una columna y determina si debería convertirse a JSON
    pub fn analyze_column(&self, column: &ColumnInfo, samples: &[Option<String>]) -> JsonAnalysisResult {
        let mut score: u8 = 0;
        let mut reasons = Vec::new();

        // 1. Evaluar tipo de dato original (30 puntos máx)
        let type_score = self.evaluate_data_type(column);
        score = score.saturating_add(type_score);
        if type_score > 0 {
            reasons.push(format!("Tipo {} favorable (+{})", column.data_type, type_score));
        }

        // 2. Evaluar estructura del contenido (40 puntos máx)
        let (content_score, valid_json, invalid_json, null_count) = self.evaluate_content(samples);
        score = score.saturating_add(content_score);
        if content_score > 0 {
            reasons.push(format!("Contenido estructurado (+{})", content_score));
        }

        // 3. Evaluar consistencia (20 puntos máx)
        let total_non_null = valid_json + invalid_json;
        let consistency_score = if total_non_null > 0 {
            let ratio = valid_json as f32 / total_non_null as f32;
            if ratio > 0.8 {
                reasons.push(format!("{}% JSON válido (+20)", (ratio * 100.0) as u8));
                20
            } else if ratio > 0.5 {
                reasons.push(format!("{}% JSON válido (+10)", (ratio * 100.0) as u8));
                10
            } else {
                0
            }
        } else {
            0
        };
        score = score.saturating_add(consistency_score);

        // 4. Beneficio de migración (10 puntos máx)
        let benefit_score = self.evaluate_migration_benefit(column, valid_json, samples.len() as u32);
        score = score.saturating_add(benefit_score);
        if benefit_score > 0 {
            reasons.push(format!("Beneficio JSONB (+{})", benefit_score));
        }

        let should_convert = score >= self.recommendation_threshold;
        let reason = if reasons.is_empty() {
            "No se recomienda conversión a JSON".to_string()
        } else {
            reasons.join("; ")
        };

        JsonAnalysisResult {
            column_name: column.name.clone(),
            should_convert,
            score,
            reason,
            sample_valid_json: valid_json,
            sample_invalid_json: invalid_json,
            sample_null: null_count,
            sample_total: samples.len() as u32,
        }
    }

    /// Evalúa el tipo de dato original (máx 30 puntos)
    fn evaluate_data_type(&self, column: &ColumnInfo) -> u8 {
        let data_type = column.data_type.to_lowercase();
        let max_len = column.max_length.unwrap_or(0);

        match data_type.as_str() {
            "xml" => 25,
            "nvarchar" | "varchar" if max_len == -1 || max_len > 1000 => 20,
            "ntext" | "text" => 15,
            "nvarchar" | "varchar" if max_len > 255 => 10,
            _ => 0,
        }
    }

    /// Evalúa el contenido de las muestras (máx 40 puntos)
    fn evaluate_content(&self, samples: &[Option<String>]) -> (u8, u32, u32, u32) {
        let mut valid_json = 0u32;
        let mut invalid_json = 0u32;
        let mut null_count = 0u32;
        let mut looks_like_json = 0u32;

        for sample in samples {
            match sample {
                None => null_count += 1,
                Some(s) if s.is_empty() => null_count += 1,
                Some(s) => {
                    let trimmed = s.trim();

                    // Verificar si parece JSON (incluye formatos con comillas externas)
                    // Casos: {}, [], "{...}", "[...]"
                    if trimmed.starts_with('{') || trimmed.starts_with('[') ||
                       trimmed.starts_with("\"{") || trimmed.starts_with("\"[") ||
                       trimmed.starts_with("'{") || trimmed.starts_with("'[") {
                        looks_like_json += 1;
                    }

                    // Intentar parsear como JSON (varios formatos)
                    if try_parse_json(trimmed).is_some() {
                        valid_json += 1;
                    } else if trimmed.starts_with('<') {
                        // Para XML, contamos como potencial JSON
                        looks_like_json += 1;
                        invalid_json += 1;
                    } else {
                        invalid_json += 1;
                    }
                }
            }
        }

        let total_non_null = valid_json + invalid_json;
        let mut score: u8 = 0;

        // Puntos por estructura JSON detectada
        if total_non_null > 0 {
            let json_ratio = valid_json as f32 / total_non_null as f32;
            if json_ratio > 0.8 {
                score = score.saturating_add(25); // JSON válido mayoritario
            } else if json_ratio > 0.5 {
                score = score.saturating_add(15);
            }
        }

        // Puntos por patrones que parecen JSON
        if total_non_null > 0 {
            let looks_ratio = looks_like_json as f32 / total_non_null as f32;
            if looks_ratio > 0.7 {
                score = score.saturating_add(15);
            } else if looks_ratio > 0.3 {
                score = score.saturating_add(8);
            }
        }

        (score.min(40), valid_json, invalid_json, null_count)
    }

    /// Evalúa el beneficio de migrar a JSONB (máx 10 puntos)
    fn evaluate_migration_benefit(&self, column: &ColumnInfo, valid_json: u32, total: u32) -> u8 {
        if total == 0 {
            return 0;
        }

        let valid_ratio = valid_json as f32 / total as f32;

        // JSONB en PostgreSQL permite indexación y búsqueda eficiente
        // Solo vale la pena si hay datos estructurados reales
        if valid_ratio > 0.7 && column.max_length.unwrap_or(0) > 100 {
            10
        } else if valid_ratio > 0.5 {
            5
        } else {
            0
        }
    }

    /// Convierte un valor a JSON de forma segura
    /// Retorna None si no se puede convertir (se insertará NULL)
    /// Soporta formatos: {...}, [...], "{...}", "[...]", '{...}', '[...]'
    pub fn safe_convert_to_json(value: &Option<String>) -> Option<JsonValue> {
        match value {
            None => None,
            Some(s) if s.is_empty() => None,
            Some(s) => {
                let trimmed = s.trim();

                // Intentar parsear JSON en varios formatos
                if let Some(json) = try_parse_json(trimmed) {
                    return Some(json);
                }

                // Intentar convertir XML a JSON (simplificado)
                if trimmed.starts_with('<') {
                    if let Some(json) = xml_to_json_simple(trimmed) {
                        return Some(json);
                    }
                }

                // No se pudo convertir, retornar NULL
                None
            }
        }
    }

    /// Obtiene el tamaño de muestra configurado
    pub fn sample_size(&self) -> u32 {
        self.sample_size
    }

    /// Obtiene el umbral de recomendación
    pub fn threshold(&self) -> u8 {
        self.recommendation_threshold
    }

    /// Genera un resumen del análisis
    pub fn generate_summary(results: &[JsonAnalysisResult]) -> AnalysisSummary {
        let total = results.len();
        let recommended = results.iter().filter(|r| r.should_convert).count();
        let avg_score: f32 = if total > 0 {
            results.iter().map(|r| r.score as f32).sum::<f32>() / total as f32
        } else {
            0.0
        };

        AnalysisSummary {
            total_columns: total,
            recommended_for_json: recommended,
            average_score: avg_score,
        }
    }
}

/// Intenta parsear JSON en varios formatos comunes
/// Soporta: {...}, [...], "{...}", "[...]", '{...}', '[...]'
fn try_parse_json(s: &str) -> Option<JsonValue> {
    // 1. Intento directo (formato normal: {...} o [...])
    if let Ok(json) = serde_json::from_str::<JsonValue>(s) {
        return Some(json);
    }

    // 2. JSON con comillas dobles externas: "{...}" o "[...]"
    // Esto ocurre cuando SQL Server almacena JSON como string escapado
    if (s.starts_with("\"{") && s.ends_with("}\"")) ||
       (s.starts_with("\"[") && s.ends_with("]\"")) {
        // Remover comillas externas y desescapar
        let inner = &s[1..s.len()-1];
        // Desescapar las comillas internas \" -> "
        let unescaped = inner.replace("\\\"", "\"").replace("\\\\", "\\");
        if let Ok(json) = serde_json::from_str::<JsonValue>(&unescaped) {
            return Some(json);
        }
    }

    // 3. JSON con comillas simples externas: '{...}' o '[...]'
    if (s.starts_with("'{") && s.ends_with("}'")) ||
       (s.starts_with("'[") && s.ends_with("]'")) {
        let inner = &s[1..s.len()-1];
        if let Ok(json) = serde_json::from_str::<JsonValue>(inner) {
            return Some(json);
        }
    }

    // 4. Intentar parsear como string JSON (cuando viene como "\"...\"")
    // Esto es JSON doblemente escapado
    if s.starts_with('"') && s.ends_with('"') {
        if let Ok(JsonValue::String(inner_str)) = serde_json::from_str::<JsonValue>(s) {
            // El contenido interno podría ser JSON
            if let Ok(json) = serde_json::from_str::<JsonValue>(&inner_str) {
                return Some(json);
            }
        }
    }

    None
}

/// Conversión simplificada de XML a JSON
fn xml_to_json_simple(xml: &str) -> Option<JsonValue> {
    // Implementación básica: detectar tags y convertir a objeto
    // Para una implementación completa, usar una librería como quick-xml

    if !xml.starts_with('<') {
        return None;
    }

    // Intento básico de conversión
    // En producción, usar una librería de XML parsing
    let mut result = serde_json::Map::new();
    result.insert("_xml_content".to_string(), JsonValue::String(xml.to_string()));
    result.insert("_converted".to_string(), JsonValue::Bool(false));

    Some(JsonValue::Object(result))
}

/// Resumen del análisis de columnas
#[derive(Debug, Clone)]
pub struct AnalysisSummary {
    pub total_columns: usize,
    pub recommended_for_json: usize,
    pub average_score: f32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_json_detection() {
        let analyzer = JsonAnalyzer::default();

        let samples = vec![
            Some(r#"{"name": "test", "value": 123}"#.to_string()),
            Some(r#"{"items": [1, 2, 3]}"#.to_string()),
            Some(r#"{"nested": {"deep": true}}"#.to_string()),
        ];

        let column = ColumnInfo {
            name: "data".to_string(),
            data_type: "nvarchar".to_string(),
            max_length: Some(-1),
            precision: None,
            scale: None,
            is_nullable: true,
            is_identity: false,
            default_value: None,
            should_convert_to_json: false,
            json_score: 0,
            json_reason: String::new(),
            pg_type: "TEXT".to_string(),
        };

        let result = analyzer.analyze_column(&column, &samples);
        assert!(result.should_convert);
        assert!(result.score >= 60);
    }

    #[test]
    fn test_non_json_detection() {
        let analyzer = JsonAnalyzer::default();

        let samples = vec![
            Some("Simple text".to_string()),
            Some("Another text".to_string()),
            Some("12345".to_string()),
        ];

        let column = ColumnInfo {
            name: "description".to_string(),
            data_type: "varchar".to_string(),
            max_length: Some(100),
            precision: None,
            scale: None,
            is_nullable: true,
            is_identity: false,
            default_value: None,
            should_convert_to_json: false,
            json_score: 0,
            json_reason: String::new(),
            pg_type: "VARCHAR(100)".to_string(),
        };

        let result = analyzer.analyze_column(&column, &samples);
        assert!(!result.should_convert);
        assert!(result.score < 60);
    }

    #[test]
    fn test_safe_convert() {
        let valid = Some(r#"{"key": "value"}"#.to_string());
        let invalid = Some("not json".to_string());
        let empty = None;

        assert!(JsonAnalyzer::safe_convert_to_json(&valid).is_some());
        assert!(JsonAnalyzer::safe_convert_to_json(&invalid).is_none());
        assert!(JsonAnalyzer::safe_convert_to_json(&empty).is_none());
    }

    #[test]
    fn test_quoted_json_detection() {
        // JSON con comillas externas: "{...}"
        let quoted_json = Some(r#""{\"name\": \"test\", \"value\": 123}""#.to_string());
        let result = JsonAnalyzer::safe_convert_to_json(&quoted_json);
        assert!(result.is_some());

        // JSON normal
        let normal_json = Some(r#"{"name": "test"}"#.to_string());
        assert!(JsonAnalyzer::safe_convert_to_json(&normal_json).is_some());

        // Array JSON con comillas
        let quoted_array = Some(r#""[1, 2, 3]""#.to_string());
        let result = JsonAnalyzer::safe_convert_to_json(&quoted_array);
        assert!(result.is_some());
    }
}
