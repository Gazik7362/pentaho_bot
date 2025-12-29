import os
import logging

# ==========================
# 🤖 TELEGRAM CONFIG
# ==========================
# Replace with your actual token
TELEGRAM_TOKEN = "8550091070:AAFPqy82XMYp5qDDuEO1yGZgqWb_1FDYjSM"  

# ==========================
# 🔌 PENTAHO CARTE CONFIG
# ==========================
CARTE_URL  = "http://10.7.7.230:8081/kettle"
CARTE_AUTH = ('cluster', 'cluster') 

# Basic auth payload for URL generation
REPO_CONF = {
    'rep': 'PENTAHO_REPO_BAITEREK',
    'user': 'gmaylibay',
    'pass': 'N0CYmWFKGmSa'
}

# ==========================
# 🗄️ DATABASE CONFIG (REPO)
# ==========================
DB_CONF = {
    'host': "10.7.7.230",
    'port': "5432",
    'database': "pentaho_repo",
    'user': "g_maylibay",
    'password': "N0CYmWFKGmSa"
}

# ==========================
# ⚙️ APP SETTINGS
# ==========================
# Path to the users database file
USERS_FILE_PATH = os.path.join(os.path.dirname(__file__), 'users.json')

# Logging Configuration
LOG_LEVEL = logging.INFO
# Modules to silence (too noisy)
SILENCED_LOGGERS = ["httpx", "apscheduler"]
