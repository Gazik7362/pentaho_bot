# 🤖 Pentaho ETL Bot

A Data Engineering Command Center that runs inside Telegram. This bot allows engineers to monitor, run, and debug Pentaho Data Integration (PDI/Kettle) jobs remotely.

## 🚀 Features
* **Remote Execution:** Run Jobs and Transformations from Telegram.
* **Live Monitoring:** Real-time status of Carte server and ETL flows.
* **Server Health:** Monitor RAM, CPU, and Disk usage (`psutil`).
* **Role-Based Access:** Admin, Engineer, and Analyst roles.
* **Log Peeking:** View the last 20 lines of logs without downloading files.
* **Scheduling:** View and manage job schedules.

## 🛠️ Stack
* **Language:** Python 3.10+
* **ETL Engine:** Pentaho Data Integration (Carte Server)
* **Database:** Vertica & PostgreSQL
* **Libraries:** `python-telegram-bot`, `psutil`, `apscheduler`

## 📦 Installation
1.  Clone the repo.
2.  Copy `config/settings_template.py` to `config/settings.py` and add your tokens.
3.  Run `python3 main.py`.
