# Cardiovascular Synthetic Data Generator

Sistema completo para generar datos sintéticos de enfermedades cardiovasculares usando Machine Learning (CTGAN), streaming con Kafka, y almacenamiento en Snowflake para análisis.

## Arquitectura

```
Dataset Kaggle (CSV)
 ↓
[FastAPI Service] → Entrena modelo CTGAN
 ↓
[API Endpoints] → Genera datos sintéticos
 ↓
[Kafka Producer] → Topic: cardiovascular-data
 ↓
[Kafka Consumer] → Lee mensajes en batch
 ↓
[Snowflake Writer] → Inserta en tablas
 ↓
Snowflake Data Warehouse (Analytics)
```

## Componentes

1. **FastAPI Service**: API REST para entrenar modelos y generar datos
2. **Synthetic Generator**: Módulo CTGAN para generar datos realistas
3. **Kafka Producer**: Streaming de datos sintéticos
4. **Kafka Consumer**: Consumidor que lee del topic
5. **Snowflake Writer**: Persiste datos en Snowflake
6. **Docker Compose**: Infraestructura completa (Kafka, Zookeeper, servicios)

## Inicio Rápido

### Prerrequisitos

- Python 3.11+
- Docker & Docker Compose
- Cuenta de Snowflake
- Kaggle API credentials (opcional, para descargar dataset)

### 1. Clonar y Configurar

```bash
# Navegar al directorio
cd synthetic_generator

# Ejecutar setup
chmod +x scripts/setup.sh
./scripts/setup.sh
```

### 2. Configurar Variables de Entorno

Editar `.env` con tus credenciales de Snowflake:

```bash
# Snowflake Configuration
SNOWFLAKE_ACCOUNT=tu_cuenta
SNOWFLAKE_USER=tu_usuario
SNOWFLAKE_PASSWORD=tu_password
SNOWFLAKE_WAREHOUSE=tu_warehouse
SNOWFLAKE_DATABASE=CARDIOVASCULAR_DB
SNOWFLAKE_SCHEMA=SYNTHETIC_DATA
SNOWFLAKE_TABLE=PATIENT_DATA
```

### 3. Descargar Dataset

```bash
# Opción 1: Usando script (requiere Kaggle API configurado)
python scripts/download_dataset.py

# Opción 2: Manual
# Descargar desde: https://www.kaggle.com/datasets/alphiree/cardiovascular-diseases-risk-prediction-dataset
# Colocar en: ./data/CVD_cleaned.csv
```

### 4. Iniciar Infraestructura

```bash
# Iniciar todos los servicios
docker-compose up -d

# Ver logs
docker-compose logs -f

# Verificar servicios
docker-compose ps
```

## Endpoints de la API

### Base URL: `http://localhost:8000`

#### Health Check
```bash
GET /health
```

#### Métodos de Generación Disponibles
```bash
GET /train/methods

# Response:
{
  "methods": [
    {
      "method": "ctgan",
      "name": "CTGANGenerator",
      "description": "Generates synthetic cardiovascular disease data using CTGAN"
    }
  ],
  "default_method": "ctgan"
}
```

#### Listar Datasets Disponibles
```bash
GET /datasets

# Response:
{
  "datasets": [
    {
      "name": "CVD_cleaned.csv",
      "path": "./data/CVD_cleaned.csv",
      "size_bytes": 45678901,
      "modified_at": "2025-11-11T10:00:00"
    }
  ],
  "total_count": 1
}
```

#### Entrenar Modelo
```bash
POST /train
Content-Type: application/json

{
 "dataset_path": "./data/CVD_cleaned.csv",
 "model_name": "cardiovascular_model",
 "method": "ctgan",
 "epochs": 300,
 "overwrite_existing": false
}

# Response: Retorna inmediatamente con task_id
{
 "status": "started",
 "message": "Training task initiated. Use task_id to check progress.",
 "task_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

#### Verificar Estado del Entrenamiento
```bash
GET /train/status/{task_id}

# Response mientras está entrenando:
{
 "task_id": "550e8400-e29b-41d4-a716-446655440000",
 "status": "running",
 "message": "Training in progress (45.0%)",
 "progress": 45.0,
 "started_at": "2025-11-11T10:30:00",
 "completed_at": null,
 "error": null,
 "result": null
}

# Response cuando termina:
{
 "task_id": "550e8400-e29b-41d4-a716-446655440000",
 "status": "completed",
 "message": "Training completed successfully",
 "progress": 100.0,
 "started_at": "2025-11-11T10:30:00",
 "completed_at": "2025-11-11T10:45:00",
 "error": null,
 "result": {
 "model_path": "./models/cardiovascular_model.pkl",
 "training_time": 900.5,
 "dataset_rows": 308854
 }
}
```

#### Listar Todas las Tareas de Entrenamiento
```bash
GET /train/tasks
```

#### Generar Datos Sintéticos
```bash
POST /generate
Content-Type: application/json

{
 "num_samples": 100,
 "send_to_kafka": true
}
```

#### Streaming a Kafka
```bash
POST /stream
Content-Type: application/json

{
 "num_samples": 1000,
 "batch_size": 50,
 "interval_seconds": 2.0
}
```

#### Información del Modelo
```bash
GET /model/info
```

## Uso

### 1. Entrenar el Modelo

```bash
# Iniciar entrenamiento (retorna inmediatamente)
curl -X POST http://localhost:8000/train \
 -H "Content-Type: application/json" \
 -d '{
 "dataset_path": "./data/CVD_cleaned.csv",
 "epochs": 300
 }'

# Response:
# {
# "status": "started",
# "task_id": "abc-123-xyz",
# "message": "Training task initiated..."
# }

# Verificar progreso
curl http://localhost:8000/train/status/abc-123-xyz

# Ver todas las tareas
curl http://localhost:8000/train/tasks
```

### 2. Generar Datos y Enviar a Kafka

```bash
curl -X POST http://localhost:8000/generate \
 -H "Content-Type: application/json" \
 -d '{
 "num_samples": 100,
 "send_to_kafka": true
 }'
```

### 3. Streaming Continuo

```bash
curl -X POST http://localhost:8000/stream \
 -H "Content-Type: application/json" \
 -d '{
 "num_samples": 10000,
 "batch_size": 100,
 "interval_seconds": 5.0
 }'
```

## Interfaces Web

- **API Docs (Swagger)**: http://localhost:8000/docs
- **Kafka UI**: http://localhost:8080
- **Alternative API Docs (ReDoc)**: http://localhost:8000/redoc

## Estructura del Dataset

El dataset de Kaggle contiene las siguientes columnas:

- `general_health`: Estado de salud general
- `checkup`: Última revisión médica
- `exercise`: Hábitos de ejercicio
- `heart_disease`: Presencia de enfermedad cardíaca
- `skin_cancer`, `other_cancer`: Historial de cáncer
- `depression`: Historial de depresión
- `diabetes`: Estado de diabetes
- `arthritis`: Presencia de artritis
- `sex`: Género
- `age_category`: Categoría de edad
- `height_cm`, `weight_kg`, `bmi`: Medidas físicas
- `smoking_history`: Historial de tabaquismo
- `alcohol_consumption`: Consumo de alcohol
- `fruit_consumption`: Consumo de frutas
- `green_vegetables_consumption`: Consumo de vegetales verdes
- `fried_potato_consumption`: Consumo de papas fritas

## Estructura del Proyecto

```
synthetic_generator/
 app/
 main.py # FastAPI application
 models/
 schemas.py # Pydantic models
 services/
 generator_factory.py # Factory pattern (NEW)
 synthetic_generator.py # CTGAN generator
 kafka_producer.py # Kafka producer
 utils/
 logger.py # Logging setup
 task_tracker.py # Training task tracking
 consumer/
 kafka_consumer.py # Kafka consumer
 snowflake_writer.py # Snowflake integration
 config/
 settings.py # Configuration
 scripts/
 download_dataset.py # Dataset downloader
 setup.sh # Setup script
 test_api.py # API tests
 test_factory.py # Factory pattern tests (NEW)
 monitor_training.py # Training monitor
 data/ # Dataset files
 models/ # Trained models
 logs/ # Application logs
 docker-compose.yml # Docker services
 Dockerfile.api # API container
 Dockerfile.consumer # Consumer container
 requirements.txt # Python dependencies
 .env.example # Environment variables template
```

## Factory Pattern - Múltiples Métodos de Generación

El sistema ahora soporta múltiples algoritmos de generación mediante un **Factory Pattern extensible**:

### Métodos Disponibles
- ✅ **CTGAN** (Conditional Tabular GAN) - Implementado
- 🔄 **TVAE** (Tabular Variational AutoEncoder) - Futuro
- 🔄 **Gaussian Copula** - Futuro

### Nuevos Endpoints

```bash
# Listar métodos disponibles
GET /train/methods

# Listar datasets disponibles
GET /datasets

# Entrenar con método específico
POST /train
{
  "method": "ctgan",
  "model_name": "my_model",
  "overwrite_existing": false
}
```

### Testing Factory

```bash
make test-factory
```

## Servicios Docker

- **zookeeper**: Coordinación de Kafka (puerto 2181)
- **kafka**: Message broker (puerto 9092)
- **kafka-ui**: Interfaz web para Kafka (puerto 8080)
- **api-producer**: FastAPI service (puerto 8000)
- **consumer**: Snowflake consumer

## Testing

```bash
# Test API endpoints
python scripts/test_api.py

# Test con curl
curl http://localhost:8000/health
```

## Análisis en Snowflake

Una vez que los datos están en Snowflake, puedes realizar análisis:

```sql
-- Ver registros recientes
SELECT * FROM CARDIOVASCULAR_DB.SYNTHETIC_DATA.PATIENT_DATA
ORDER BY ingested_at DESC
LIMIT 100;

-- Estadísticas por categoría de edad
SELECT 
 age_category,
 COUNT(*) as count,
 AVG(bmi) as avg_bmi,
 SUM(CASE WHEN heart_disease = 'Yes' THEN 1 ELSE 0 END) as heart_disease_count
FROM CARDIOVASCULAR_DB.SYNTHETIC_DATA.PATIENT_DATA
GROUP BY age_category;

-- Distribución de enfermedades cardíacas
SELECT 
 heart_disease,
 COUNT(*) as count,
 ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM CARDIOVASCULAR_DB.SYNTHETIC_DATA.PATIENT_DATA
GROUP BY heart_disease;
```

## Desarrollo

### Instalar dependencias localmente

```bash
python -m venv venv
source venv/bin/activate # En Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### Ejecutar servicios localmente (sin Docker)

```bash
# Terminal 1: Kafka (con Docker)
docker-compose up kafka zookeeper

# Terminal 2: FastAPI
python -m uvicorn app.main:app --reload

# Terminal 3: Consumer
python -m consumer.kafka_consumer
```

## Seguridad

- Nunca commitear el archivo `.env` con credenciales reales
- Usar variables de entorno para información sensible
- Rotar credenciales de Snowflake regularmente
- Limitar acceso a topics de Kafka en producción

## Notas Técnicas

### CTGAN
- Modelo generativo especializado en datos tabulares
- Captura distribuciones complejas y correlaciones
- Entrenamiento puede tomar 5-30 minutos dependiendo del dataset

### Kafka
- Topic auto-creado al enviar primer mensaje
- Retención de mensajes: 7 días (configurable)
- Consumer group permite procesamiento paralelo

### Snowflake
- Tabla creada automáticamente al iniciar consumer
- Índices automáticos en timestamp de ingesta
- Optimizado para queries analíticas

## Contribución

1. Fork el proyecto
2. Crear feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit cambios (`git commit -m 'Add some AmazingFeature'`)
4. Push a branch (`git push origin feature/AmazingFeature`)
5. Abrir Pull Request

## Licencia

Este proyecto es de código abierto y está disponible bajo la Licencia MIT.

## Créditos

- Dataset: [Cardiovascular Diseases Risk Prediction](https://www.kaggle.com/datasets/alphiree/cardiovascular-diseases-risk-prediction-dataset)
- SDV/CTGAN: [Synthetic Data Vault](https://sdv.dev/)
- FastAPI: [FastAPI Framework](https://fastapi.tiangolo.com/)

## Soporte

Para preguntas o problemas:
1. Revisar la documentación en `/docs`
2. Verificar logs: `docker-compose logs -f`
3. Abrir un issue en el repositorio

---

**Cardiovascular Synthetic Data Generator - Professional Data Analysis Tool**
