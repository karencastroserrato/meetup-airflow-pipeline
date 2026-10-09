# Meetup Data Pipeline

Pipeline de datos para el dataset de Meetup (Kaggle), usando Snowflake como almacén y Airflow como orquestador.

## Arquitectura

En Snowflake, la base `MEETUP_DB` tiene tres esquemas:

- `RAW`: datos tal cual vienen de los CSV originales.
- `STAGING`: datos limpios y con tipos correctos.
- `ANALYTICS`: modelo final (dimensiones + tabla de hechos `FACT_EVENT`).

Los scripts de carga y transformación (numerados 01 a 06) están en los worksheets de Snowflake.

## Pipeline en Airflow

El DAG `meetup_pipeline` corre cada 15 minutos:

1. `generar_eventos_simulados` — crea eventos falsos para simular datos nuevos.
2. `merge_staging_events` — hace `MERGE` (upsert) de esos eventos en `STG_EVENTS`.
3. `merge_fact_event` — propaga el `MERGE` a la tabla analítica `FACT_EVENT`.
4. `refrescar_resumen_analytics` — recalcula `SUMMARY_EVENTS_BY_STATUS` con `CREATE OR REPLACE`.
5. `notificar_slack` — manda un mensaje a Slack con el resultado de la corrida.

Si alguna tarea falla, se manda una notificación de error aparte a Slack.

## Cómo correrlo

Requisitos: Docker y Docker Compose.

1. Completa `.env` con las credenciales de Snowflake y el webhook de Slack (este archivo no se sube a git).
2. Levanta los contenedores:
   ```
   docker compose build
   docker compose up -d
   ```
3. Entra a `http://localhost:8080` (usuario/clave por default: airflow/airflow).
4. El DAG ya corre solo cada 15 min. Para probarlo manualmente:
   ```
   docker compose exec airflow-worker airflow dags test meetup_pipeline 2026-10-08
   ```

## Notas

- Las credenciales (Snowflake, Slack) van solo en `.env`, nunca en el código.
- La exportación a S3 quedó fuera de esta entrega; el foco fue Snowflake + Airflow + Slack.
- Los eventos simulados usan un `GROUP_ID` y `VENUE_ID` que ya existen en los datos reales, para mantener consistencia con el modelo.
