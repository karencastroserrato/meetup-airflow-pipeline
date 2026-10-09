import os
import random
import uuid
from datetime import datetime

import requests
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import dag, task

SNOWFLAKE_CONN_ID = "snowflake_default"
N_EVENTOS_SIMULADOS = 5


def notificar_fallo_slack(context):
    webhook_url = os.environ["SLACK_WEBHOOK_URL"]
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    mensaje = f":x: *Pipeline Meetup fallo*\nDAG: {dag_id}\nTarea: {task_id}"
    requests.post(webhook_url, json={"text": mensaje}, timeout=10)


@dag(
    dag_id="meetup_pipeline",
    description="Pipeline de datos Meetup con Snowflake",
    start_date=datetime(2026, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    tags=["meetup", "snowflake"],
    default_args={"on_failure_callback": notificar_fallo_slack},
)
def meetup_pipeline():

    @task
    def generar_eventos_simulados():
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        group_id, venue_id = hook.get_first(
            "SELECT GROUP_ID, VENUE_ID FROM MEETUP_DB.STAGING.STG_EVENTS "
            "WHERE GROUP_ID IS NOT NULL AND VENUE_ID IS NOT NULL "
            "ORDER BY RANDOM() LIMIT 1"
        )

        estados = ["upcoming", "past", "cancelled"]
        eventos = []
        for _ in range(N_EVENTOS_SIMULADOS):
            eventos.append(
                {
                    "event_id": f"SIM-{uuid.uuid4().hex[:10]}",
                    "event_name": f"Evento simulado {random.randint(1000, 9999)}",
                    "event_status": random.choice(estados),
                    "group_id": group_id,
                    "venue_id": venue_id,
                    "duration": random.randint(30, 180) * 60,
                    "headcount": random.randint(5, 200),
                    "yes_rsvp_count": random.randint(0, 150),
                    "maybe_rsvp_count": random.randint(0, 30),
                    "waitlist_count": random.randint(0, 20),
                    "rsvp_limit": random.randint(50, 300),
                }
            )
        print(f"Generados {len(eventos)} eventos simulados.")
        return eventos

    @task
    def merge_staging_events(eventos):
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        for e in eventos:
            hook.run(
                """
                MERGE INTO MEETUP_DB.STAGING.STG_EVENTS AS target
                USING (SELECT %(event_id)s AS EVENT_ID) AS source
                ON target.EVENT_ID = source.EVENT_ID
                WHEN MATCHED THEN UPDATE SET
                    EVENT_NAME = %(event_name)s,
                    EVENT_STATUS = %(event_status)s,
                    HEADCOUNT = %(headcount)s,
                    YES_RSVP_COUNT = %(yes_rsvp_count)s,
                    MAYBE_RSVP_COUNT = %(maybe_rsvp_count)s,
                    WAITLIST_COUNT = %(waitlist_count)s,
                    UPDATED = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT (
                    EVENT_ID, EVENT_NAME, EVENT_STATUS, GROUP_ID, VENUE_ID,
                    EVENT_TIME, CREATED, UPDATED, DURATION, HEADCOUNT,
                    YES_RSVP_COUNT, MAYBE_RSVP_COUNT, WAITLIST_COUNT, RSVP_LIMIT
                ) VALUES (
                    %(event_id)s, %(event_name)s, %(event_status)s, %(group_id)s, %(venue_id)s,
                    CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), %(duration)s, %(headcount)s,
                    %(yes_rsvp_count)s, %(maybe_rsvp_count)s, %(waitlist_count)s, %(rsvp_limit)s
                )
                """,
                parameters=e,
            )
        print(f"MERGE en STG_EVENTS completado para {len(eventos)} eventos.")
        return eventos

    @task
    def merge_fact_event(eventos):
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        event_ids = [e["event_id"] for e in eventos]
        ids_sql = ", ".join(f"'{eid}'" for eid in event_ids)
        hook.run(
            f"""
            MERGE INTO MEETUP_DB.ANALYTICS.FACT_EVENT AS target
            USING (
                SELECT EVENT_ID, GROUP_ID, VENUE_ID, EVENT_NAME, EVENT_STATUS,
                       CREATED, EVENT_TIME, UPDATED, DURATION, HEADCOUNT,
                       YES_RSVP_COUNT, MAYBE_RSVP_COUNT, WAITLIST_COUNT, RSVP_LIMIT
                FROM MEETUP_DB.STAGING.STG_EVENTS
                WHERE EVENT_ID IN ({ids_sql})
            ) AS source
            ON target.EVENT_ID = source.EVENT_ID
            WHEN MATCHED THEN UPDATE SET
                target.EVENT_NAME = source.EVENT_NAME,
                target.EVENT_STATUS = source.EVENT_STATUS,
                target.HEADCOUNT = source.HEADCOUNT,
                target.YES_RSVP_COUNT = source.YES_RSVP_COUNT,
                target.MAYBE_RSVP_COUNT = source.MAYBE_RSVP_COUNT,
                target.WAITLIST_COUNT = source.WAITLIST_COUNT,
                target.UPDATED = source.UPDATED
            WHEN NOT MATCHED THEN INSERT (
                EVENT_ID, GROUP_ID, VENUE_ID, EVENT_NAME, EVENT_STATUS,
                CREATED, EVENT_TIME, UPDATED, DURATION, HEADCOUNT,
                YES_RSVP_COUNT, MAYBE_RSVP_COUNT, WAITLIST_COUNT, RSVP_LIMIT
            ) VALUES (
                source.EVENT_ID, source.GROUP_ID, source.VENUE_ID, source.EVENT_NAME, source.EVENT_STATUS,
                source.CREATED, source.EVENT_TIME, source.UPDATED, source.DURATION, source.HEADCOUNT,
                source.YES_RSVP_COUNT, source.MAYBE_RSVP_COUNT, source.WAITLIST_COUNT, source.RSVP_LIMIT
            )
            """
        )
        print(f"MERGE en FACT_EVENT completado para {len(event_ids)} eventos.")
        return len(event_ids)

    @task
    def refrescar_resumen_analytics():
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        hook.run(
            """
            CREATE OR REPLACE TABLE MEETUP_DB.ANALYTICS.SUMMARY_EVENTS_BY_STATUS AS
            SELECT
                EVENT_STATUS,
                COUNT(*) AS TOTAL_EVENTS,
                AVG(HEADCOUNT) AS AVG_HEADCOUNT,
                SUM(YES_RSVP_COUNT) AS TOTAL_YES_RSVP
            FROM MEETUP_DB.ANALYTICS.FACT_EVENT
            GROUP BY EVENT_STATUS
            """
        )
        print("Tabla SUMMARY_EVENTS_BY_STATUS actualizada.")
        return True

    @task(trigger_rule="all_success")
    def notificar_slack(total_eventos, _resumen_listo):
        webhook_url = os.environ["SLACK_WEBHOOK_URL"]
        mensaje = (
            ":white_check_mark: *Pipeline Meetup completado*\n"
            f"Eventos simulados y cargados: {total_eventos}\n"
            "Tablas actualizadas: STG_EVENTS, FACT_EVENT, SUMMARY_EVENTS_BY_STATUS :white_check_mark:"
        )
        response = requests.post(webhook_url, json={"text": mensaje}, timeout=10)
        response.raise_for_status()
        print("Notificacion enviada a Slack.")

    eventos = generar_eventos_simulados()
    eventos_en_staging = merge_staging_events(eventos)
    total = merge_fact_event(eventos_en_staging)
    resumen = refrescar_resumen_analytics()
    total >> resumen
    notificar_slack(total, resumen)


meetup_pipeline()
