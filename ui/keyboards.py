from telegram import InlineKeyboardButton, InlineKeyboardMarkup

class Keyboards:
    @staticmethod
    def main_menu(items, page, total_pages, dir_id, user_role, permissions, parent_id, filter_mode='ALL'):
        keyboard = []
        
        # 1. Filter Tabs
        btn_all = f"[{'All'}]" if filter_mode == 'ALL' else "All"
        btn_job = f"[{'Jobs'}]" if filter_mode == 'JOB' else "Jobs"
        btn_trans = f"[{'Trans'}]" if filter_mode == 'TRANS' else "Trans"
        
        keyboard.append([
            InlineKeyboardButton(f"📂 {btn_all}", callback_data=f"OPEN|{dir_id}|0|ALL"),
            InlineKeyboardButton(f"✴️ {btn_job}", callback_data=f"OPEN|{dir_id}|0|JOB"),
            InlineKeyboardButton(f"⚙️ {btn_trans}", callback_data=f"OPEN|{dir_id}|0|TRANS"),
        ])

        # 2. Content Buttons
        for i in range(0, len(items), 2):
            row = [InlineKeyboardButton(items[i]['name'], callback_data=items[i]['data'])]
            if i + 1 < len(items):
                row.append(InlineKeyboardButton(items[i+1]['name'], callback_data=items[i+1]['data']))
            keyboard.append(row)

        # 3. Pagination
        nav = []
        if page > 0: 
            nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"OPEN|{dir_id}|{page-1}|{filter_mode}"))
        
        nav.append(InlineKeyboardButton(f"🔢 {page+1}/{max(1, total_pages)}", callback_data=f"GOTO_PAGE_INIT|{dir_id}|{filter_mode}"))

        if page < total_pages - 1: 
            nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"OPEN|{dir_id}|{page+1}|{filter_mode}"))
        elif total_pages > 1:
            nav.append(InlineKeyboardButton("⏪ Start", callback_data=f"OPEN|{dir_id}|0|{filter_mode}"))
            
        keyboard.append(nav)

        # 4. Tools Row
        tools = []
        if dir_id != -1:
            target_up = parent_id if parent_id is not None else -1
            tools.append(InlineKeyboardButton("🔙 Up Level", callback_data=f"OPEN|{target_up}|0|ALL"))

        tools.append(InlineKeyboardButton("🔍 Search", callback_data="SEARCH_INIT"))
        
        if dir_id == -1:
            tools.append(InlineKeyboardButton("📜 My Activity", callback_data="MY_ACTIVITY"))

            if 'SCHED' in permissions or user_role == 'SUPER':
                tools.append(InlineKeyboardButton("📅 Schedules", callback_data="SCHED_DASHBOARD"))
            if user_role == "SUPER":
                tools.append(InlineKeyboardButton("🛠️ Admin", callback_data="ADMIN_MENU"))
                tools.append(InlineKeyboardButton("🖥️ Server Health", callback_data="SYS_HEALTH"))
            if user_role in ["SUPER", "MANAGER", "ANALYST", "SUPPORT"]:
                 tools.append(InlineKeyboardButton("📊 Dashboard", callback_data="DASHBOARD"))

        tools.append(InlineKeyboardButton("🖥️ Monitor", callback_data="MONITOR"))

        if len(tools) > 3:
             # Split into two rows if > 3 buttons
             keyboard.append(tools[:3])
             keyboard.append(tools[3:6])
             keyboard.append(tools[6:])
        elif tools:
             keyboard.append(tools)
        
        return InlineKeyboardMarkup(keyboard)

    @staticmethod
    def search_results(matches):
        kb = []
        for item in matches[:15]:
            icon = "✴️" if item['type'] == 'JOB' else "⚙️"
            kb.append([InlineKeyboardButton(
                f"{icon} {item['name']}", 
                callback_data=f"PREP|{item['dir_id']}|{item['name']}|{item['type']}"
            )])
            
        kb.append([InlineKeyboardButton("🔙 Main Menu", callback_data="OPEN|-1|0")])
        return InlineKeyboardMarkup(kb)        

    @staticmethod
    def monitor_menu(active_count):
        kb = []
        kb.append([InlineKeyboardButton("🔄 Refresh List", callback_data="MONITOR")])
        
        # ✅ NEW BUTTON
        kb.append([InlineKeyboardButton("🖥️ Server Health", callback_data="SYS_HEALTH")])
        
        if active_count > 0:
            kb.append([InlineKeyboardButton("🛑 Stop a Process...", callback_data="STOP_MENU")])
            
        kb.append([InlineKeyboardButton("🔙 Main Menu", callback_data="OPEN|-1|0")])
        return InlineKeyboardMarkup(kb)

    @staticmethod
    def job_prep(dir_id, job_name, permissions, is_scheduled, is_paused, default_schedule=None, is_job=True):
        kb = []
        row1 = []
        if 'RUN' in permissions:
            row1.append(InlineKeyboardButton("🚀 Run Now", callback_data=f"RUN|{dir_id}|{job_name}|{'JOB' if is_job else 'TRANS'}"))
        
        row1.append(InlineKeyboardButton("📜 History", callback_data=f"HISTORY|{dir_id}|{job_name}|{'JOB' if is_job else 'TRANS'}"))
        kb.append(row1)

        if not is_job:
            kb.append([InlineKeyboardButton("🔍 View Source SQL", callback_data=f"GET_SQL|{dir_id}|{job_name}")])

        if is_job and 'SCHED' in permissions:
            if is_scheduled:
                pause_btn = "▶️ Resume" if is_paused else "⏸️ Pause"
                action_pause = "SCHED_RESUME" if is_paused else "SCHED_PAUSE"
                kb.append([
                    InlineKeyboardButton(pause_btn, callback_data=f"{action_pause}|{dir_id}|{job_name}"),
                    InlineKeyboardButton("⚙️ Edit", callback_data=f"SCHED_MENU|{dir_id}|{job_name}")
                ])
                kb.append([InlineKeyboardButton("🔕 Delete Schedule", callback_data=f"SCHED_STOP|{dir_id}|{job_name}")])
            else:
                if default_schedule and default_schedule['type'] != 'NONE':
                    desc = default_schedule['desc']
                    kb.append([InlineKeyboardButton(f"⏰ Start Schedule ({desc})", callback_data=f"SCHED_DEFAULT|{dir_id}|{job_name}")])
                kb.append([InlineKeyboardButton("📅 Custom Schedule", callback_data=f"SCHED_MENU|{dir_id}|{job_name}")])

        kb.append([InlineKeyboardButton("🔙 Cancel", callback_data=f"OPEN|{dir_id}|0")])
        return InlineKeyboardMarkup(kb)

    @staticmethod
    def source_selector(dir_id, trans_name, sources):
        kb = []
        for src in sources:
            kb.append([InlineKeyboardButton(f"📥 {src['step']}", callback_data=f"SHOW_SQL|{dir_id}|{trans_name}|{src['step']}")])
            
        kb.append([InlineKeyboardButton("🔙 Back", callback_data=f"PREP|{dir_id}|{trans_name}|TRANS")])
        return InlineKeyboardMarkup(kb)    

    @staticmethod
    def scheduler_dashboard(permissions):
        kb = [[InlineKeyboardButton("🔄 Refresh", callback_data="SCHED_DASHBOARD")]]
        kb.append([InlineKeyboardButton("🔙 Main Menu", callback_data="OPEN|-1|0")])
        return InlineKeyboardMarkup(kb)

    @staticmethod
    def execution_controls(dir_id, job_name, is_failure=False):
        kb = []
        if is_failure:
             kb.append([InlineKeyboardButton("🔄 Restart", callback_data=f"RUN|{dir_id}|{job_name}")])
        kb.append([InlineKeyboardButton("🔙 Main Menu", callback_data="OPEN|-1|0")])
        return InlineKeyboardMarkup(kb)

    @staticmethod
    def admin_menu(is_frozen):
        toggle_txt = "🔥 Unfreeze System" if is_frozen else "❄️ Freeze System"
        kb = [
            [InlineKeyboardButton("👤 Add User", callback_data="ADMIN_ADD_USER")], 
            [InlineKeyboardButton(toggle_txt, callback_data="TOGGLE_FREEZE")],
            [InlineKeyboardButton("📅 Scheduled Jobs", callback_data="SCHED_DASHBOARD")],
            [InlineKeyboardButton("💀 Kill Bot Process", callback_data="KILL_CONFIRM")],
            [InlineKeyboardButton("🔙 Back to Menu", callback_data="OPEN|-1|0")]
        ]
        return InlineKeyboardMarkup(kb)

    @staticmethod
    def role_selector(user_id):
        kb = [
            [InlineKeyboardButton("👷 Engineer", callback_data=f"SAVE_USER|{user_id}|ENGINEER")],
            [InlineKeyboardButton("📊 Analyst", callback_data=f"SAVE_USER|{user_id}|ANALYST")],
            [InlineKeyboardButton("🆘 Support", callback_data=f"SAVE_USER|{user_id}|SUPPORT")],
            [InlineKeyboardButton("👨🏻‍💼 MANAGER", callback_data=f"SAVE_USER|{user_id}|MANAGER")],
            [InlineKeyboardButton("🔙 Cancel", callback_data="ADMIN_MENU")]
        ]
        return InlineKeyboardMarkup(kb)

    @staticmethod
    def kill_confirm():
        kb = [
            [InlineKeyboardButton("✅ YES, KILL IT", callback_data="KILL_EXECUTE")],
            [InlineKeyboardButton("🔙 No, Cancel", callback_data="ADMIN_MENU")]
        ]
        return InlineKeyboardMarkup(kb)