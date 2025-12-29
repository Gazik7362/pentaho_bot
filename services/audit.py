import psycopg2
import logging
from config.settings import DB_CONF

class AuditService:
    def get_connection(self):
        return psycopg2.connect(**DB_CONF)

    def log(self, user_id, action, target, details=""):
        """Records a user action."""
        sql = """
        INSERT INTO BOT_AUDIT_LOG (USER_ID, ACTION_TYPE, TARGET_NAME, DETAILS)
        VALUES (%s, %s, %s, %s)
        """
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute(sql, (str(user_id), action, target, details))
            conn.commit()
            conn.close()
            logging.info(f"AUDIT: User {user_id} -> {action} on {target}")
        except Exception as e:
            logging.error(f"Audit Log Error: {e}")

    def get_recent_logs(self, limit=15):
        """Fetches recent activity for the Admin dashboard."""
        sql = """
        SELECT USER_ID, ACTION_TYPE, TARGET_NAME, LOGGED_AT 
        FROM BOT_AUDIT_LOG 
        ORDER BY LOGGED_AT DESC 
        LIMIT %s
        """
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute(sql, (limit,))
            rows = cur.fetchall()
            conn.close()
            
            logs = []
            for row in rows:
                logs.append({
                    'user': row[0],
                    'action': row[1],
                    'target': row[2],
                    'time': row[3].strftime('%m-%d %H:%M')
                })
            return logs
        except Exception as e:
            logging.error(f"Audit Fetch Error: {e}")
            return []

    def get_user_search_history(self, user_id, limit=5):
        """Returns the last N unique search terms for a user."""
        sql = """
        SELECT DISTINCT DETAILS
        FROM BOT_AUDIT_LOG
        WHERE USER_ID = %s AND ACTION_TYPE = 'SEARCH'
        ORDER BY MAX(LOGGED_AT) DESC
        LIMIT %s
        """
        # Note: DISTINCT on DETAILS usually requires aggregation on sorting column in standard SQL.
        # Better Query for Postgres:
        sql = """
        SELECT DETAILS
        FROM BOT_AUDIT_LOG
        WHERE USER_ID = %s AND ACTION_TYPE = 'SEARCH'
        GROUP BY DETAILS
        ORDER BY MAX(LOGGED_AT) DESC
        LIMIT %s
        """
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute(sql, (str(user_id), limit))
            rows = cur.fetchall()
            conn.close()
            return [row[0] for row in rows]
        except Exception as e:
            logging.error(f"Search History Error: {e}")
            return []

    # ... inside AuditService ...

    def get_user_logs(self, user_id, limit=10):
        """Fetches the last N actions for a specific user."""
        sql = """
        SELECT ACTION_TYPE, TARGET_NAME, LOGGED_AT, DETAILS 
        FROM BOT_AUDIT_LOG 
        WHERE USER_ID = %s
        ORDER BY LOGGED_AT DESC 
        LIMIT %s
        """
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute(sql, (str(user_id), limit))
            rows = cur.fetchall()
            conn.close()
            
            logs = []
            for row in rows:
                logs.append({
                    'action': row[0],
                    'target': row[1],
                    'time': row[2].strftime('%m-%d %H:%M'),
                    'details': row[3]
                })
            return logs
        except Exception as e:
            logging.error(f"User Log Error: {e}")
            return []            

audit_service = AuditService()