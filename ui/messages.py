from datetime import datetime

class Msg:
    @staticmethod
    def browser_status(path, user_role, is_frozen, page, total):
        status = "❄️ FROZEN" if is_frozen else "🟢 Online"
        return (
            f"📂 <b>Browser:</b> <code>{path}</code>\n"
            f"User: {user_role} | Status: {status}\n"
            f"Page {page+1}/{max(1, total)}"
        )

    @staticmethod
    def job_prep(job_name, schedule_info=None):
        msg = f"✴️ <b>Job:</b> <code>{job_name}</code>\n"
        if schedule_info:
            state = "⏸️ PAUSED" if schedule_info.get('paused') else "✅ ACTIVE"
            next_run = schedule_info.get('next_run', 'Unknown')
            msg += f"\n📅 <b>Schedule:</b> {state}\n"
            msg += f"⏳ <b>Next Run:</b> {next_run}"
        else:
            msg += "\n📅 <b>Schedule:</b> Not scheduled"
        return msg

    @staticmethod
    def scheduler_dashboard(jobs):
        if not jobs:
            return "📅 <b>Scheduler Dashboard</b>\n\nNo active schedules."
        
        msg = f"📅 <b>Scheduler Dashboard ({len(jobs)} jobs)</b>\n\n"
        for j in jobs:
            icon = "⏸️" if j['paused'] else "✅"
            msg += f"{icon} <b>{j['id']}</b>\n   🕒 {j['next_run']}\n"
        return msg

    @staticmethod
    def execution_start(job_name):
        return f"✴️ <b>Starting:</b> <code>{job_name}</code>"

    @staticmethod
    def execution_success(job_name, job_id):
        return (
            f"✅ <b>Started!</b>\n"
            f"Job: {job_name}\n"
            f"ID: <code>{job_id}</code>\n"
            f"⏳ Monitoring..."
        )

    @staticmethod
    def execution_failure(job_name, error):
        return f"❌ <b>Error:</b> {error}"

    @staticmethod
    def monitor_status(jobs):
        if not jobs:
            return "💤 <b>No jobs currently running.</b>"
        msg = f"<b>📊 Active Jobs ({len(jobs)}):</b>\n\n"
        for j in jobs:
            msg += f"✴️ <b>{j['name']}</b>\n🆔 <code>{j['id'][:6]}...</code>\n\n"
        return msg

    @staticmethod
    def history_view(name, history_data):
        if not history_data:
            return f"📜 <b>History: {name}</b>\n\nNo records found (or DB error)."
        
        msg = f"📜 <b>History: {name}</b>\n\n"
        for h in history_data:
            icon = "✅" if h['status'] == 'end' else "❌"
            msg += (
                f"{icon} <b>{h['date']}</b>\n"
                f"👤 {h['user']} | Status: {h['status']}\n"
                f"📝 <i>{h['log']}</i>\n\n"
            )
        return msg

    @staticmethod
    def manager_report(data):
        if not data: return "⚠️ Error fetching stats."
        
        failures = data['failures']
        total = data['total_runs']
        fail_count = len(failures)
        
        # Calculate Success Rate
        success_rate = 100.0
        if total > 0:
            success_rate = ((total - fail_count) / total) * 100
            
        # Icon Logic
        if success_rate >= 99: header = "🟢 Excellent"
        elif success_rate >= 95: header = "🟡 Good"
        else: header = "🔴 Critical"
        
        msg = (
            f"<b>Nightly Load Status:</b> {header}\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📊 <b>Success Rate:</b> {success_rate:.1f}%\n"
            f"🔢 <b>Volume:</b> {total} Total Processes\n"
            f"❌ <b>Issues:</b> {fail_count} Failed\n\n"
        )
        
        if fail_count > 0:
            msg += "<b>⚠️ Detailed Failures:</b>\n"
            # Show max 10 to avoid spamming
            for f in failures[:10]:
                icon = "✴️" if f['type'] == 'Job' else "⚙️"
                msg += f"{icon} <b>{f['name']}</b> ({f['time']})\n"
            
            if fail_count > 10:
                msg += f"<i>...and {fail_count - 10} more.</i>"
        else:
            msg += "✨ <i>Clean run. No active failures.</i>"
            
        return msg

    @staticmethod
    def browser_status(path, user_role, is_frozen, page, total):
        # Just updating this to be generic
        status = "❄️ FROZEN" if is_frozen else "🟢 Online"
        return (
            f"📂 <b>Browser:</b> <code>{path}</code>\n"
            f"User: {user_role} | Status: {status}\n"
            f"Page {page+1}/{max(1, total)}"
        )
    
    # Update job_prep to handle both types
    @staticmethod
    def job_prep(name, type_label, schedule_info=None):
        icon = "✴️" if type_label == "JOB" else "⚙️"
        msg = f"{icon} <b>{type_label}:</b> <code>{name}</code>\n"
        
        if type_label == "JOB":
            if schedule_info:
                state = "⏸️ PAUSED" if schedule_info.get('paused') else "✅ ACTIVE"
                next_run = schedule_info.get('next_run', 'Unknown')
                msg += f"\n📅 <b>Schedule:</b> {state}\n⏳ <b>Next Run:</b> {next_run}"
            else:
                msg += "\n📅 <b>Schedule:</b> Not scheduled"
        else:
            msg += "\n(Transformations cannot be scheduled directly by this bot)"
            
        return msg