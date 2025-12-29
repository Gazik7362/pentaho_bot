import psycopg2
import logging
from config.settings import DB_CONF

class RepoService:
    def __init__(self):
        self.cache = {}

    def get_connection(self):
        return psycopg2.connect(**DB_CONF)

    def fetch_structure(self):
        """Scans the DB and builds the folder/job/trans tree."""
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            
            # 1. Fetch Dirs
            cur.execute("SELECT ID_DIRECTORY, ID_DIRECTORY_PARENT, DIRECTORY_NAME FROM R_DIRECTORY")
            dirs = cur.fetchall()
            
            # 2. Fetch Jobs (.kjb)
            cur.execute('SELECT ID_JOB, ID_DIRECTORY, "NAME" FROM R_JOB')
            jobs = cur.fetchall()

            # 3. Fetch Transformations (.ktr) - NEW!
            cur.execute('SELECT ID_TRANSFORMATION, ID_DIRECTORY, "NAME" FROM R_TRANSFORMATION')
            trans = cur.fetchall()

            conn.close()

            # Build Tree
            tree = {-1: {"name": "MAIN MENU", "parent": None, "subfolders": [], "jobs": [], "trans": []}}
            
            for d, p, n in dirs:
                if not n: n = "ROOT"
                pid = p if p != 0 else -1
                tree[d] = {"name": n, "parent": pid, "subfolders": [], "jobs": [], "trans": []}

            for d, node in tree.items():
                if d == -1: continue
                pid = node['parent']
                if pid in tree: 
                    tree[pid]["subfolders"].append({"id": d, "name": node['name']})
                elif pid == 0: 
                    tree[-1]["subfolders"].append({"id": d, "name": node['name']})

            # Populate Jobs
            for j, d, n in jobs:
                target = d if d in tree else -1
                tree[target]["jobs"].append({"name": n})

            # Populate Transformations
            for t, d, n in trans:
                target = d if d in tree else -1
                tree[target]["trans"].append({"name": n})
            
            self.cache = tree
            return tree
        except Exception as e:
            logging.error(f"RepoService Error: {e}")
            return None

    def get_full_path(self, dir_id):
        if not self.cache: self.fetch_structure()
        dir_id = int(dir_id)
        if dir_id not in self.cache or dir_id == -1: return "/"
        node = self.cache[dir_id]
        if node['parent'] == -1 or node['parent'] is None: return "/" + node['name']
        return f"{self.get_full_path(node['parent'])}/{node['name']}".replace("//", "/")

    def get_job_schedule_config(self, job_name):
        """(Existing logic kept same)"""
        sql = """
        SELECT rjea.code, rjea.value_str, rjea.value_num
        FROM r_jobentry rje
        JOIN r_job rj ON rje.id_job = rj.id_job
        JOIN r_jobentry_attribute rjea ON rje.id_jobentry = rjea.id_jobentry
        WHERE rj.name = %s AND rje.name = 'Start'
        """
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute(sql, (job_name,))
            rows = cur.fetchall()
            conn.close()
            config = {row[0]: (int(row[2]) if row[2] is not None else row[1]) for row in rows}
            sched_type = config.get('schedulerType', 0)
            if sched_type == 1:
                return {'type': 'INTERVAL', 'desc': f"Every {config.get('intervalMinutes',0)}m", 'm': config.get('intervalMinutes',0)}
            elif sched_type == 2:
                return {'type': 'DAILY', 'desc': f"Daily {config.get('hour',12):02}:{config.get('minutes',0):02}", 'h': config.get('hour',12), 'm': config.get('minutes',0)}
            return {'type': 'NONE', 'desc': 'No Schedule'}
        except:
            return {'type': 'ERROR', 'desc': 'DB Error'}

    # ... inside RepoService class ...

    def get_trans_sql(self, trans_name):
        """
        Fetches SQL queries from Table Input steps for a specific transformation.
        Uses the specific step-name allowlist provided by the user.
        """
        sql = """
        SELECT
            rs."NAME" AS step_name,
            rsa.VALUE_STR AS sql_query
        FROM R_STEP rs
        JOIN R_TRANSFORMATION rt ON rs.ID_TRANSFORMATION = rt.ID_TRANSFORMATION
        JOIN R_STEP_ATTRIBUTE rsa ON rs.ID_STEP = rsa.ID_STEP
        JOIN R_STEP_TYPE rst ON rs.ID_STEP_TYPE = rst.ID_STEP_TYPE
        WHERE
            rt."NAME" = %s  -- Filter by the specific Trans we are looking at
            AND rst.CODE = 'TableInput'
            AND rsa.CODE = 'sql'
            -- User-defined filters
            AND rs."NAME" !~ '(_test|_TEST)'
            AND rs."NAME" !~ '(_OLD|_COPY|_TEMP|_TMP|_BCKP)$'
            /*AND rs."NAME" IN (
                'DAMU_DWH', 'KE_1CB', 'KGK_1CB', 'KGK_1CS', 'FRP_1C', 
                'KAF_1C', 'AKK_1C', 'BAITEREK_1CUH', 'BRK_COLVIR',
                'H_CONTRACT', 'Table input', 'S_CONTRACT_INFO_AKK_LOAN', -- Added from your screenshots
                'AKK_1C_LOAN_CREDIT_LINE', 'AKK_1C_LOAN_CONTRACT'
            )*/
        ORDER BY rs."NAME"
        """
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute(sql, (trans_name,))
            rows = cur.fetchall()
            conn.close()
            
            # Return list of dicts: [{'step': 'Name', 'sql': 'SELECT...'}]
            return [{'step': row[0], 'sql': row[1]} for row in rows]
        except Exception as e:
            logging.error(f"SQL Fetch Error: {e}")
            return []            

    def search_repo(self, query):
        """Scans cache and returns results sorted by relevance (Exact > StartsWith > Contains)."""
        if not self.cache: self.fetch_structure()
        
        q = query.strip().lower()
        
        # Buckets for sorting relevance
        exact_matches = []
        starts_with_matches = []
        contains_matches = []
        
        # Helper to process items
        def classify(name, dir_id, item_type):
            name_lower = name.lower()
            obj = {'name': name, 'dir_id': dir_id, 'type': item_type}
            
            if name_lower == q:
                exact_matches.append(obj)
            elif name_lower.startswith(q):
                starts_with_matches.append(obj)
            elif q in name_lower:
                contains_matches.append(obj)

        # Iterate over every folder in the tree
        for dir_id, node in self.cache.items():
            if dir_id == -1: continue 
            
            for job in node['jobs']:
                classify(job['name'], dir_id, 'JOB')
            
            for trans in node['trans']:
                classify(trans['name'], dir_id, 'TRANS')
        
        # Sort buckets alphabetically to keep them tidy
        exact_matches.sort(key=lambda x: x['name'])
        starts_with_matches.sort(key=lambda x: x['name'])
        contains_matches.sort(key=lambda x: x['name'])
        
        # Combine: Exact first, then StartsWith, then loosely matched
        return exact_matches + starts_with_matches + contains_matches   

    # --- NEW: HISTORY FEATURE ---
    def get_history(self, name, is_job=True):
        """Fetches last 5 runs using user's specific SQL logic."""
        table_log = "R_JOB_LOG" if is_job else "R_TRANS_LOG"
        table_main = "R_JOB" if is_job else "R_TRANSFORMATION"
        col_name = "JOBNAME" if is_job else "TRANSNAME"
        col_main_name = "NAME" # Both tables use "NAME"

        # Adapted SQL: We filter by specific name instead of getting all broken ones
        sql = f"""
        SELECT 
            t.STATUS, 
            t.REPLAYDATE, 
            t.LOG_FIELD,
            COALESCE(main.modified_user, main.created_user, 'NO USER') as EXECUTING_USER
        FROM {table_log} t
        LEFT JOIN {table_main} main ON t.{col_name} = main."{col_main_name}"
        WHERE t.{col_name} = %s
        ORDER BY t.REPLAYDATE DESC
        LIMIT 5
        """
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute(sql, (name,))
            rows = cur.fetchall()
            conn.close()
            
            history = []
            for row in rows:
                history.append({
                    'status': row[0],
                    'date': row[1].strftime('%Y-%m-%d %H:%M') if row[1] else 'Unknown',
                    'log': str(row[2])[:50] + "..." if row[2] else "",
                    'user': row[3]
                })
            return history
        except Exception as e:
            logging.error(f"History Error: {e}")
            return []

    def backup_and_update_sql(self, trans_name, step_name, new_sql, user_id):
        """
        1. Fetches current SQL.
        2. Inserts it into BOT_SQL_HISTORY.
        3. Updates R_STEP_ATTRIBUTE with new SQL.
        """
        try:
            conn = self.get_connection()
            cur = conn.cursor()

            # 1. Find the Attribute ID and Current Value
            find_sql = """
            SELECT rsa.ID_STEP_ATTRIBUTE, rsa.VALUE_STR
            FROM R_STEP rs
            JOIN R_TRANSFORMATION rt ON rs.ID_TRANSFORMATION = rt.ID_TRANSFORMATION
            JOIN R_STEP_ATTRIBUTE rsa ON rs.ID_STEP = rsa.ID_STEP
            WHERE rt."NAME" = %s AND rs."NAME" = %s AND rsa.CODE = 'sql'
            """
            cur.execute(find_sql, (trans_name, step_name))
            row = cur.fetchone()
            
            if not row:
                conn.rollback()
                return False, "Step not found or no SQL attribute."

            attr_id, old_sql = row

            # 2. Insert into History (Versioning)
            hist_sql = """
            INSERT INTO BOT_SQL_HISTORY (TRANS_NAME, STEP_NAME, OLD_SQL, CHANGED_BY)
            VALUES (%s, %s, %s, %s)
            """
            cur.execute(hist_sql, (trans_name, step_name, old_sql, str(user_id)))

            # 3. Update Live Repo
            update_sql = "UPDATE R_STEP_ATTRIBUTE SET VALUE_STR = %s WHERE ID_STEP_ATTRIBUTE = %s"
            cur.execute(update_sql, (new_sql, attr_id))

            conn.commit()
            conn.close()
            return True, "Success"

        except Exception as e:
            logging.error(f"Update SQL Error: {e}")
            if conn: conn.rollback()
            return False, str(e)

    # ... inside RepoService ...

    def get_broken_processes(self):
        """
        1. Counts TOTAL unique processes run in last 24h.
        2. Lists specific processes that failed in their latest run.
        """
        sql_failures = """
        WITH TRANS_LOGS AS (
            SELECT TRANSNAME, STATUS, REPLAYDATE,
                   ROW_NUMBER() OVER(PARTITION BY TRANSNAME ORDER BY REPLAYDATE DESC) as rn
            FROM R_TRANS_LOG
            WHERE REPLAYDATE >= CURRENT_DATE - INTERVAL '24 HOURS'
        ),
        JOB_LOGS AS (
            SELECT JOBNAME, STATUS, REPLAYDATE,
                   ROW_NUMBER() OVER(PARTITION BY JOBNAME ORDER BY REPLAYDATE DESC) as rn
            FROM R_JOB_LOG
            WHERE REPLAYDATE >= CURRENT_DATE - INTERVAL '24 HOURS'
        )
        -- Get Failures
        SELECT 'Transformation' as type, TRANSNAME, STATUS, TO_CHAR(REPLAYDATE, 'HH24:MI')
        FROM TRANS_LOGS WHERE rn = 1 AND STATUS != 'end'
        UNION ALL
        SELECT 'Job', JOBNAME, STATUS, TO_CHAR(REPLAYDATE, 'HH24:MI')
        FROM JOB_LOGS WHERE rn = 1 AND STATUS != 'end'
        ORDER BY 4 DESC
        """

        sql_total_count = """
        SELECT 
            (SELECT COUNT(DISTINCT TRANSNAME) FROM R_TRANS_LOG WHERE REPLAYDATE >= CURRENT_DATE - INTERVAL '24 HOURS') 
            + 
            (SELECT COUNT(DISTINCT JOBNAME) FROM R_JOB_LOG WHERE REPLAYDATE >= CURRENT_DATE - INTERVAL '24 HOURS')
        """

        try:
            conn = self.get_connection()
            cur = conn.cursor()
            
            # 1. Get List of Failures
            cur.execute(sql_failures)
            rows = cur.fetchall()
            failures = [{'type': r[0], 'name': r[1], 'status': r[2], 'time': r[3]} for r in rows]
            
            # 2. Get Total Count
            cur.execute(sql_total_count)
            total_runs = cur.fetchone()[0] or 0
            
            conn.close()
            
            return {
                'total_runs': total_runs,
                'failures': failures
            }
        except Exception as e:
            logging.error(f"Broken Process Fetch Error: {e}")
            return None

    def get_sql_history_list(self, trans_name, step_name):
        """Fetches the last 10 versions of this step's SQL."""
        sql = """
        SELECT ID, CHANGED_AT, CHANGED_BY, OLD_SQL
        FROM BOT_SQL_HISTORY
        WHERE TRANS_NAME = %s AND STEP_NAME = %s
        ORDER BY CHANGED_AT DESC
        LIMIT 10
        """
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute(sql, (trans_name, step_name))
            rows = cur.fetchall()
            conn.close()
            
            history = []
            for row in rows:
                history.append({
                    'id': row[0],
                    'date': row[1].strftime('%Y-%m-%d %H:%M'),
                    'user': row[2],
                    'sql': row[3]
                })
            return history
        except Exception as e:
            logging.error(f"History Fetch Error: {e}")
            return []
            
    def get_archived_sql(self, history_id):
        """Fetches a specific archived SQL body."""
        sql = "SELECT OLD_SQL FROM BOT_SQL_HISTORY WHERE ID = %s"
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute(sql, (history_id,))
            row = cur.fetchone()
            conn.close()
            return row[0] if row else None
        except:
            return None            

    def validate_sql_syntax(self, sql_query):
        """
        Placeholder for 'Tests'. 
        Realistically, you'd need a connection to the SOURCE db to EXPLAIN PLAN.
        For now, we just check if it's not empty and basic keywords.
        """
        q = sql_query.strip().upper()
        if not q: return False, "Empty query"
        if not q.startswith("SELECT") and not q.startswith("WITH"):
            return False, "Query must start with SELECT or WITH"
        if "DROP " in q or "DELETE " in q or "TRUNCATE " in q:
             return False, "Destructive commands (DROP/DELETE) not allowed via Bot."
        return True, "Passed Syntax Check"       

    def find_sql_usage(self, search_term):
        """
        Scans all Table Input steps to find where a specific table/column is used.
        Case-insensitive search inside the raw SQL code.
        """
        sql = """
        SELECT DISTINCT 
            rt.ID_DIRECTORY,
            rt."NAME" as trans_name,
            'TRANS' as type,
            rs."NAME" as step_name
        FROM 
            R_TRANSFORMATION rt
            JOIN R_STEP rs ON rt.ID_TRANSFORMATION = rs.ID_TRANSFORMATION
            JOIN R_STEP_ATTRIBUTE rsa ON rs.ID_STEP = rsa.ID_STEP
        WHERE 
            rsa.CODE = 'sql' 
            AND rsa.VALUE_STR ILIKE %s
        ORDER BY 
            rt."NAME"
        LIMIT 40;
        """
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            # Wrap search term in % % for wildcard match
            cur.execute(sql, (f"%{search_term}%",))
            rows = cur.fetchall()
            conn.close()
            
            # Format results to be compatible with your existing 'search_results' keyboard
            results = []
            for row in rows:
                results.append({
                    'dir_id': row[0], 
                    'name': row[1], 
                    'type': row[2]
                    # We don't need 'step_name' for the main list, but good to have if you expand later
                })
            return results
        except Exception as e:
            logging.error(f"Usage Search Error: {e}")
            return []             

    # Add to RepoService class
    def get_log_tail(self, name, lines=20):
        """Reads the last N lines of a log file without loading the whole thing."""
        # Assuming logs are stored in /home/ac/etl_logs/ or similar
        # You might need to adjust the path logic to match where your jobs write logs
        log_path = f"/home/ac/etl_logs/{name}.log" 
        
        try:
            from collections import deque
            with open(log_path, 'r') as f:
                # deque(f, 20) efficiently grabs the last 20 lines
                tail = deque(f, lines) 
                return "".join(tail)
        except FileNotFoundError:
            return "⚠️ Log file not found."
        except Exception as e:
            return f"Error reading log: {e}"

repo_service = RepoService()