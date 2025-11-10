"""
Data access layer for Human Review Streamlit App
Handles all database connections and queries
"""

import json
from typing import Dict, Optional, List
import pandas as pd
from databricks import sql
from databricks.sdk.core import Config


def get_connection(http_path: str):
    """Create SQL connection to Databricks warehouse"""
    cfg = Config()
    return sql.connect(
        server_hostname=cfg.host,
        http_path=http_path,
        credentials_provider=lambda: cfg.authenticate,
    )


def ensure_tables_exist(conn, full_schema: str):
    """Ensure required tables exist - creates them if missing"""
    with conn.cursor() as cursor:
        # Review actions table
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {full_schema}.review_actions (
                email_id STRING,
                action STRING,
                actor_email STRING,
                old_values STRING,
                new_values STRING,
                reason STRING,
                created_at TIMESTAMP
            )
        """)
        
        # Approved changes table
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {full_schema}.approved_changes (
                email_id STRING,
                sap_id STRING,
                contact_name STRING,
                contact_email STRING,
                contact_phone STRING,
                source_email_body STRING,
                approved_by STRING,
                approved_at TIMESTAMP
            )
        """)
        
        # Outgoing emails table (for follow-ups)
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {full_schema}.outgoing_emails (
                email_id STRING,
                to_email STRING,
                subject STRING,
                body STRING,
                created_by STRING,
                created_at TIMESTAMP,
                status STRING
            )
        """)


def get_kpi_counts(conn, full_schema: str) -> Dict:
    """Get KPI counts for dashboard"""
    with conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT 
                validation_status,
                COUNT(*) as count
            FROM {full_schema}.review_queue q
            LEFT ANTI JOIN (
                SELECT DISTINCT email_id FROM {full_schema}.review_actions
            ) a ON q.email_id = a.email_id
            GROUP BY validation_status
        """)
        results = cursor.fetchall()
        
        kpis = {"total": 0, "pass": 0, "needs_review": 0, "fail": 0}
        
        for row in results:
            status = row[0]
            count = row[1]
            kpis["total"] += count
            if status == "PASS":
                kpis["pass"] = count
            elif status == "NEEDS_REVIEW":
                kpis["needs_review"] = count
            elif status == "FAIL":
                kpis["fail"] = count
        
        return kpis


def get_pending_items(conn, full_schema: str, search_query: str = "", limit: int = 100) -> pd.DataFrame:
    """Get all pending review items"""
    with conn.cursor() as cursor:
        base_query = f"""
            SELECT 
                q.email_id,
                q.sender,
                q.validation_status,
                q.sap_id,
                q.contact_name,
                q.contact_email,
                q.contact_phone,
                q.normalized_sap_id,
                q.normalized_name,
                q.normalized_phone,
                q.sap_id_valid,
                q.name_valid,
                q.email_valid,
                q.phone_valid,
                q.sap_exists
            FROM {full_schema}.review_queue q
            LEFT ANTI JOIN (
                SELECT DISTINCT email_id FROM {full_schema}.review_actions
            ) a ON q.email_id = a.email_id
        """
        
        if search_query:
            base_query += f"""
                WHERE (
                    q.email_id LIKE '%{search_query}%' OR
                    q.sender LIKE '%{search_query}%' OR
                    q.sap_id LIKE '%{search_query}%' OR
                    q.normalized_sap_id LIKE '%{search_query}%'
                )
            """
        
        base_query += f" ORDER BY q.validation_status DESC, q.email_id LIMIT {limit}"
        
        cursor.execute(base_query)
        result = cursor.fetchall_arrow().to_pandas()
        return result


def get_email_body(conn, full_schema: str, email_id: str) -> Optional[str]:
    """Get original email body"""
    with conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT body
            FROM {full_schema}.synthetic_emails
            WHERE email_id = '{email_id}'
        """)
        result = cursor.fetchone()
        return result[0] if result else None


def confirm_extraction(conn, full_schema: str, email_id: str, sap_id: str, name: str, 
                      email: str, phone: str, email_body: str, actor_email: str):
    """Confirm extracted data is correct and write to approved_changes"""
    with conn.cursor() as cursor:
        cursor.execute(f"""
            INSERT INTO {full_schema}.approved_changes 
            VALUES (?, ?, ?, ?, ?, ?, ?, current_timestamp())
        """, (email_id, sap_id, name, email, phone, email_body, actor_email))
        
        new_values = {
            "sap_id": sap_id,
            "contact_name": name,
            "contact_email": email,
            "contact_phone": phone
        }
        
        cursor.execute(f"""
            INSERT INTO {full_schema}.review_actions 
            VALUES (?, 'confirmed', ?, NULL, ?, NULL, current_timestamp())
        """, (email_id, actor_email, json.dumps(new_values)))


def send_followup_email(conn, full_schema: str, email_id: str, to_email: str, 
                       subject: str, body: str, missing_fields: List[Dict], actor_email: str):
    """Save follow-up email to outgoing queue"""
    with conn.cursor() as cursor:
        cursor.execute(f"""
            INSERT INTO {full_schema}.outgoing_emails 
            VALUES (?, ?, ?, ?, ?, current_timestamp(), 'pending')
        """, (email_id, to_email, subject, body, actor_email))
        
        cursor.execute(f"""
            INSERT INTO {full_schema}.review_actions 
            VALUES (?, 'followup_sent', ?, ?, NULL, 'Missing or invalid fields', current_timestamp())
        """, (email_id, actor_email, json.dumps({"missing_fields": missing_fields}), 
              json.dumps({"followup_email": {"to": to_email, "subject": subject}})))


def get_audit_log(conn, full_schema: str, limit: int = 50) -> pd.DataFrame:
    """Get recent audit log entries"""
    with conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT 
                email_id,
                action,
                actor_email,
                created_at,
                reason
            FROM {full_schema}.review_actions
            ORDER BY created_at DESC
            LIMIT {limit}
        """)
        result = cursor.fetchall_arrow().to_pandas()
        return result


def get_followup_emails(conn, full_schema: str, limit: int = 50) -> pd.DataFrame:
    """Get recent follow-up emails"""
    with conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT 
                email_id,
                to_email,
                subject,
                created_by,
                created_at,
                status
            FROM {full_schema}.outgoing_emails
            ORDER BY created_at DESC
            LIMIT {limit}
        """)
        result = cursor.fetchall_arrow().to_pandas()
        return result


def get_followup_email_detail(conn, full_schema: str, email_id: str) -> Optional[tuple]:
    """Get subject and body for a specific follow-up email"""
    with conn.cursor() as cursor:
        cursor.execute(f"""
            SELECT subject, body
            FROM {full_schema}.outgoing_emails
            WHERE email_id = '{email_id}'
            ORDER BY created_at DESC
            LIMIT 1
        """)
        return cursor.fetchone()

