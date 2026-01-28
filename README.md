# Motor de Migración SQL Server a PostgreSQL

Herramienta de migración de bases de datos con GUI que permite migrar tablas de SQL Server a PostgreSQL con detección automática de columnas JSON.

## Requisitos

- **Rust** 1.70+ (https://rustup.rs/)
- **SQL Server** accesible por red
- **PostgreSQL** accesible por red

### Dependencias del sistema (macOS)

```bash
brew install openssl
```

### Dependencias del sistema (Ubuntu/Debian)

```bash
sudo apt-get install libssl-dev pkg-config
```

## Instalación

1. Clonar el repositorio:
```bash
git clone <repo-url>
cd Motor-migration
```

2. Compilar en modo release:
```bash
cargo build --release
```

El ejecutable se genera en `target/release/sql_migration_engine`

## Iniciar la aplicación

### Opción 1: Ejecutar directamente
```bash
cargo run --release
```

### Opción 2: Ejecutar el binario compilado
```bash
./target/release/sql_migration_engine
```

## Configuración

Al iniciar la aplicación, se muestra una pantalla de configuración con:

### Conexión SQL Server
- **Host**: Dirección del servidor (ej: `localhost` o `192.168.1.100`)
- **Puerto**: Puerto de SQL Server (default: `1433`)
- **Base de datos**: Nombre de la base de datos origen
- **Usuario**: Usuario de SQL Server
- **Contraseña**: Contraseña del usuario
- **Trust Certificate**: Activar si el servidor usa certificado auto-firmado

### Conexión PostgreSQL
- **Host**: Dirección del servidor PostgreSQL
- **Puerto**: Puerto de PostgreSQL (default: `5432`)
- **Base de datos**: Nombre de la base de datos destino
- **Usuario**: Usuario de PostgreSQL
- **Contraseña**: Contraseña del usuario

### Configuración de Migración
- **Análisis paralelo**: Número de tablas a analizar simultáneamente (1-16)
- **Migración paralela**: Número de tablas a migrar simultáneamente (1-10)
- **Tamaño de lote**: Filas por inserción (100-50000)
- **Muestra JSON**: Filas a analizar para detección JSON (10-1000)
- **Umbral JSON**: Score mínimo para convertir a JSONB (0-100)

## Uso

1. **Configurar conexiones**: Ingresar datos de SQL Server y PostgreSQL
2. **Conectar**: Click en "Conectar" para establecer conexiones
3. **Seleccionar tablas**: Marcar las tablas a migrar
4. **Analizar**: Click en "Analizar" para detectar columnas JSON
5. **Revisar columnas**: Ver y ajustar qué columnas se convierten a JSONB
6. **Migrar**: Click en "Migrar" para iniciar la transferencia

## Detección de JSON

El motor analiza columnas de texto (VARCHAR, NVARCHAR, TEXT) para detectar contenido JSON:

### Sistema de puntuación (0-100)
- **Tipo de dato** (máx 40 pts): Columnas de texto largo suman más puntos
- **Contenido** (máx 50 pts): JSON válido detectado en las muestras
- **Consistencia** (máx 20 pts): Porcentaje de valores que son JSON válido
- **Beneficio** (máx 20 pts): Ventaja de usar JSONB en PostgreSQL

Las columnas con score >= umbral configurado se convierten a JSONB.

## Características

- Migración paralela de múltiples tablas
- Detección automática de JSON en columnas de texto
- Conversión inteligente a JSONB de PostgreSQL
- Soporte para JSON escapado (comillas dobles, triple escape)
- Inserción por lotes para alto rendimiento
- Interfaz gráfica intuitiva
- Progreso en tiempo real

## Estructura del proyecto

```
src/
├── main.rs              # Punto de entrada
├── gui/
│   └── app.rs           # Interfaz gráfica (egui)
├── db/
│   ├── mod.rs           # Módulo de base de datos
│   ├── sqlserver.rs     # Conexión SQL Server
│   ├── postgres.rs      # Conexión PostgreSQL
│   └── types.rs         # Tipos compartidos
├── migration/
│   └── engine.rs        # Motor de migración
└── analysis/
    └── json_analyzer.rs # Análisis de columnas JSON
```

## Variables de entorno (opcional)

Crear archivo `.env` en la raíz:

```env
SQLSERVER_HOST=localhost
SQLSERVER_PORT=1433
SQLSERVER_DATABASE=midb
SQLSERVER_USER=sa
SQLSERVER_PASSWORD=password

POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DATABASE=midb
POSTGRES_USER=postgres
POSTGRES_PASSWORD=password
```
