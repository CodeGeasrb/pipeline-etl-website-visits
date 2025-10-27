# 🚀 ETL de Procesamiento de Datos de Visitas Web

**Autor:** Rigoberto Rincón Ballesteros  
**Repositorio:** ETL de Visitas Web  
**Tecnologías:** Prefect · Python · Pandas · MySQL · Paramiko · SQLAlchemy  

---

## 🧩 Resumen General

Este proyecto implementa un **pipeline ETL distribuido y tolerante a fallos** para procesar datos de visitas a un sitio web provenientes de archivos de texto.  
El flujo está **orquestado con Prefect**, empleando una **arquitectura Dispatcher/Performer** donde:

- El **Dispatcher** (flujo principal) identifica y distribuye archivos a procesar.  
- Los **Performers** (workers) ejecutan el ETL completo por archivo, en paralelo.  

Este diseño permite:
- Escalabilidad horizontal (añadir más workers para procesar más archivos).  
- Aislamiento de errores (un archivo fallido no detiene el flujo).  
- Monitoreo y trazabilidad granular por archivo procesado.  

---

## 🏗️ Arquitectura de la Solución

### Infraestructura

| Componente | Función | Ubicación |
|-------------|----------|-----------|
| **Servidor de Origen (SFTP)** | Almacena archivos fuente | `/home/vinkOS/archivosVisitas/` |
| **Servidor ETL** | Orquestador central y ejecución ETL | `/home/etl/` |
| **Servidor de Destino (MySQL)** | Almacena resultados y bitácoras | Tablas `visitantes`, `estadisticas`, `errores`, `bitacora_control` |

### Stack Tecnológico

- **Orquestación:** Prefect (scheduler, retries, colas y monitoreo UI)
- **Procesamiento:** Python + Pandas
- **Conectividad:** Paramiko/pysftp
- **Carga:** SQLAlchemy (bulk insert/upsert)
- **Almacenamiento:** MySQL
- **Logging:** Logs detallados por tarea y archivo

### Ejecución Distribuida (Micro-Batches)

Cada archivo es un **micro-batch independiente**.  
Los workers de Prefect procesan varios archivos en paralelo sin colisiones.  
Se limita la concurrencia de base de datos (ej. 8 conexiones simultáneas).

---

## 🗃️ Esquema de Base de Datos

| Tabla | Propósito | Características |
|-------|------------|-----------------|
| **visitantes** | Registro consolidado por visitante (email) | Upsert mediante tabla staging temporal |
| **estadisticas** | Detalle de registros válidos | Inserción por append |
| **errores** | Registros con fallos de validación | Incluye tipo de error y fila original |
| **bitacora_control** | Trazabilidad de archivos procesados | Métricas y estatus de ejecución |

---

## ⚙️ Flujo del Proceso ETL

### 1️⃣ Dispatcher (Orquestador Principal)
- Se ejecuta diariamente (02:00 AM).  
- Lista archivos nuevos en el SFTP.  
- Filtra archivos ya procesados.  
- Encola un **work item** por archivo detectado.  
- Los workers escuchan la cola y procesan archivos en paralelo.

### 2️⃣ Performer (Flujo ETL por Archivo)

#### ETAPA 1: **Extract**
- Descarga el archivo del SFTP → `/home/etl/staging/`
- Verifica integridad y tamaño.
- Maneja reintentos automáticos en fallos de red.

#### ETAPA 2: **Transform**
- Lee el archivo con Pandas (`pd.read_csv`)
- Valida layout (columnas esperadas).
- Valida emails, fechas y tipos de datos.
- Separa registros válidos e inválidos.
- Prepara tres DataFrames:
  - `df_estadisticas`
  - `df_errores`
  - `df_visitantes_staging` (agregación por email)

#### ETAPA 3: **Load**
- Inicia transacción SQL (`BEGIN`)
- Inserta errores (`tabla errores`)
- Inserta estadísticas (`tabla estadisticas`)
- Realiza **Upsert** en `visitantes` mediante tabla temporal y `ON DUPLICATE KEY UPDATE`
- Commit o rollback según resultado.

#### ETAPA 4: **Post-Proceso**
- Registra resultado en `bitacora_control`
- Mueve archivo a `/home/etl/backup/`
- Elimina archivo del SFTP y limpia staging.

### 3️⃣ Cierre del Dispatcher
- Consolida backups → `backup_YYYYMMDD.zip`
- Mueve archivos con fallos de sistema a cuarentena.
- Limpia staging.
- Genera reporte global del día (resumen de bitácora).

---

## 🧱 Estrategia de Resiliencia y Manejo de Errores

### 🔧 Errores de Sistema (técnicos)
Ejemplos: fallos SFTP, MySQL, red o memoria.  
- **Nivel 1:** Reintentos automáticos por tarea (3 intentos, delay exponencial).  
- **Nivel 2:** Reintentos de flujo completo.  
- **Nivel 3:** Si persiste → se marca `FALLO_SISTEMA` y se mueve a cuarentena.  
- **Nivel 4:** Reintento automático al día siguiente (máx. 2 días).  

### ⚠️ Errores de Negocio - Validaciones
Ejemplos: emails inválidos, fechas erróneas, nulos.  
- Se separan registros inválidos sin detener el proceso.  
- Se insertan en `errores`.  
- Archivo marcado como `COMPLETADO_CON_ERRORES`.  

### ❌ Errores de Layout (fallos graves)
Ejemplos: columnas incorrectas o faltantes.  
- Se lanza excepción crítica.  
- Archivo marcado como `FALLO_LAYOUT`.  
- No se reintenta (requiere intervención manual).  

---

## 📂 Logs y Backups

**Logs**
- Ubicación: `/home/etl/logs/YYYYMMDD/`
- Un archivo por proceso (`report_XXX.log`)
- Log del orquestador (`orchestrator_YYYYMMDD.log`)
- Retención: 30 días (limpieza automática diaria)

**Backups**
- Carpeta: `/home/etl/backup/`
- Consolidado diario: `backup_YYYYMMDD.zip`
- Retención: 90 días
- Incluye archivos exitosos, con errores y fallidos

---

## 🧠 Configuración Prefect

### Flujos
| Flujo | Nombre | Tipo | Programación |
|-------|---------|------|---------------|
| Dispatcher | `etl-visitas-web-dispatcher` | Principal | Diario 02:00 AM |
| Worker | `etl-visitas-web-worker` | Secundario | Activado por cola |



## 🖥️ Monitoreo Prefect UI

- **Dashboard en tiempo real** para visualizar ejecuciones activas.  
- **Histórico de ejecuciones filtrable** por fecha, estado y tags.  
- **Logs integrados y métricas de performance** directamente en la UI.  
- **Alertas configurables** por email o webhook ante fallos o demoras.  

---

## 📊 Monitoreo y Operaciones

### Dashboard (Grafana / Power BI / Tableau)
Basado en la tabla `bitacora_control`, incluye vistas como:

- Total de archivos procesados por día  
- Tasa de éxito y archivos en cuarentena  
- Distribución de estados (`COMPLETADO`, `CON_ERRORES`, `FALLO`)  
- Tasa de errores de datos y *top* tipos de error  

### Manual de Operaciones
Incluye escenarios de falla, diagnóstico y acciones recomendadas (SFTP, MySQL, red, etc.).  

---

## 🧮 Escalamiento a Big Data (Versión Spark)

Si el volumen de datos crece significativamente, la solución puede migrarse a:

- **Apache Spark** como motor ETL (en lugar de Pandas)  
- **HDFS / Hive / Delta Lake** como destino  
- **Prefect** mantiene orquestación mediante `spark-submit`  

### Adaptaciones Clave
- Procesamiento distribuido en cluster Hadoop o Kubernetes  
- Validaciones y agregaciones con operaciones de DataFrame de Spark  
- Carga en formato Parquet (append o merge mediante Delta Lake o Hudi)  

---

## 💡 Notas Finales

El diseño fue asistido por **LLMs (Modelos de Lenguaje Grandes)**, que ayudaron en:

- Diseño conceptual del flujo y la arquitectura  
- Implementación de *flows* y tareas en Prefect  
- Creación del script SQL para el *upsert* de la tabla `visitantes`  

**Prefect** fue elegido sobre Airflow por su menor complejidad de configuración y su fácil integración con Python.  


