import asyncio
import io
import html
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import ContextTypes
from services.system import system_service
from services.audit import audit_service
from services.auth import auth_service
from services.repository import repo_service
from services.carte import carte_service
from services.scheduler import scheduler_service
from ui.keyboards import Keyboards
from ui.messages import Msg
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from telegram.error import BadRequest


USER_STATE = {}
BOT_FROZEN = False

# ==========================================
# 🛠️ GLOBAL WRAPPER
# ==========================================
def scheduled_job_wrapper(job_name, dir_id):
    path = repo_service.get_full_path(dir_id)
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(carte_service.trigger_job(job_name, path))
        loop.close()
    except Exception as e:
        print(f"Wrapper Error: {e}")

# ==========================================
# 🛡️ UI HELPERS
# ==========================================
# In handlers/core.py

async def safe_edit_message(query, text, reply_markup=None, parse_mode='HTML'):
    """
    Tries to edit the message. 
    If it's a File/Photo (which can't be edited into text), it deletes and sends a new one.
    """
    try:
        await query.edit_message_text(text=text, reply_markup=reply_markup, parse_mode=parse_mode)
    except Exception as e:
        err_str = str(e)
        
        # 1. Handle "Message Not Modified" (User clicked same button twice) -> Ignore
        if "Message is not modified" in err_str:
            return

        # 2. Handle "No text to edit" (Trying to edit a File/Photo) -> Delete & Send New
        if "There is no text in the message to edit" in err_str or "Button_data_invalid" in err_str:
            try:
                await query.delete_message()
            except:
                pass # Message might already be gone
            
            # Send as a fresh message
            if query.message:
                await query.message.reply_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
        else:
            # Real error? Re-raise it so we see it in logs
            raise e

async def render_admin_panel(query, user_id):
    if auth_service.get_role(user_id) != "SUPER": return
    
    # Fetch Audit Logs
    logs = audit_service.get_recent_logs(10)
    log_text = "\n".join([f"🔹 <b>{l['time']}</b>: {l['user']} {l['action']} <i>{l['target']}</i>" for l in logs])
    
    status_text = "❄️ <b>FROZEN</b>" if BOT_FROZEN else "🟢 <b>ACTIVE</b>"
    text = (f"🛠️ <b>Super Admin Panel</b>\n"
            f"Current Status: {status_text}\n\n"
            f"📜 <b>Recent Activity:</b>\n{log_text or 'No activity yet.'}")
            
    kb = Keyboards.admin_menu(BOT_FROZEN)
    await safe_edit_message(query, text, kb)

async def render_prep_screen(query, dir_id, name, user_id, is_job=True):
    role = auth_service.get_role(user_id)
    perms = auth_service.roles.get(role, [])
    
    sched_info = None
    default_cfg = {'type': 'NONE'}

    if is_job:
        # Scheduler Logic only for Jobs
        job_schedule = scheduler_service.get_job(name)
        if job_schedule:
            sched_info = {
                'paused': job_schedule.next_run_time is None,
                'next_run': job_schedule.next_run_time.strftime('%H:%M') if job_schedule.next_run_time else "PAUSED"
            }
        default_cfg = repo_service.get_job_schedule_config(name)
    
    # Build Text & Keyboard
    type_label = "JOB" if is_job else "TRANS"
    text = Msg.job_prep(name, type_label, sched_info)
    
    if is_job and default_cfg['type'] != 'NONE':
        text += f"\n📋 <b>Repo Plan:</b> {default_cfg['desc']}"

    kb = Keyboards.job_prep(
        dir_id, name, perms, 
        bool(sched_info), 
        sched_info['paused'] if sched_info else False,
        default_schedule=default_cfg,
        is_job=is_job
    )
    await safe_edit_message(query, text, kb)

async def check_access(update, user_id):
    if BOT_FROZEN and auth_service.get_role(user_id) != "SUPER":
        if update.callback_query: await update.callback_query.answer("❄️ System is Frozen.", show_alert=True)
        return False
    if not auth_service.get_role(user_id): return False
    return True

# ==========================================
# 🎮 HANDLERS
# ==========================================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not await check_access(update, user_id): return
    
    # --- NEW: PERSISTENT MENU ---
    # resize_keyboard=True makes it small and nice
    # is_persistent=True keeps it always visible (Telegram 5.0+)
    home_kb = ReplyKeyboardMarkup(
        [[KeyboardButton("🏠 Main Menu")]], 
        resize_keyboard=True, 
        is_persistent=True
    )
    
    # We send a welcome message to attach the keyboard
    await update.message.reply_text(
        "👋 Welcome! Use the menu below.", 
        reply_markup=home_kb
    )
    
    # Then show the directory as usual (Inline Buttons)
    await show_directory(update, context, -1, 0)

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    state = USER_STATE.get(user_id)
    if not state or state.get('mode') != 'AWAITING_NEW_SQL':
        return

    # 1. Download File
    file_id = update.message.document.file_id
    new_file = await context.bot.get_file(file_id)
    
    # Read content into memory
    with io.BytesIO() as f:
        await new_file.download_to_memory(out=f)
        f.seek(0)
        raw_content = f.read().decode('utf-8')

    # --- 🛡️ SANITIZATION LAYER FOR FILES ---
    sql_content = raw_content.replace('\xa0', ' ').replace('\u200b', '').strip()
    # ---------------------------------------

    # 2. Process exactly like text input
    await process_sql_update(update, context, sql_content, state, user_id)

# --- REFACTOR: Move Update Logic to Shared Function ---
async def process_sql_update(update, context, text, state, user_id):
    """Shared logic for Text and File inputs."""
    trans = state['trans']
    step = state['step']
    dir_id = state['dir_id']
    
    # 1. Validate
    is_valid, msg = repo_service.validate_sql_syntax(text)
    if not is_valid:
        await update.message.reply_text(f"❌ <b>Test Failed:</b> {msg}")
        return

    # 2. Update Repo
    success, db_msg = repo_service.backup_and_update_sql(trans, step, text, user_id)
    
    if success:
        await update.message.reply_text(f"✅ <b>Success!</b>\nRepo updated for <code>{step}</code>.")
        audit_service.log(user_id, "CODE_UPDATE", state['step'], f"Trans: {state['trans']}")
        kb = [[InlineKeyboardButton("🔙 View New SQL", callback_data=f"SHOW_SQL|{dir_id}|{trans}|{step}")]]
        await update.message.reply_text("Click below to verify:", reply_markup=InlineKeyboardMarkup(kb))
        USER_STATE[user_id] = None
    else:
        await update.message.reply_text(f"❌ <b>DB Error:</b> {db_msg}")    

async def show_directory(update, context, dir_id, page=0, filter_mode='ALL'):
    user_id = update.effective_user.id
    dir_id = int(dir_id)
    
    tree = repo_service.fetch_structure()
    node = tree.get(dir_id)
    if not node:
        try: await update.callback_query.message.reply_text("⚠️ Repo Changed.")
        except: pass
        return

    items = []
    
    # 1. Always show subfolders (unless you want to hide them in JOB/TRANS mode, but usually folders are Nav)
    # Let's keep folders visible in ALL mode, but maybe hide them in strict 'JOB'/'TRANS' modes to focus?
    # For now, let's keep folders in ALL and specific modes so you can navigate deeper.
    for sub in sorted(node['subfolders'], key=lambda x: x['name']):
        items.append({"name": f"📁 {sub['name']}", "data": f"OPEN|{sub['id']}|0|ALL"})

    # 2. Filter Jobs
    if filter_mode in ['ALL', 'JOB']:
        for job in sorted(node['jobs'], key=lambda x: x['name']):
            items.append({"name": f"✴️ {job['name']}", "data": f"PREP|{dir_id}|{job['name']}|JOB"})

    # 3. Filter Transformations
    if filter_mode in ['ALL', 'TRANS']:
        for trans in sorted(node['trans'], key=lambda x: x['name']):
            items.append({"name": f"⚙️ {trans['name']}", "data": f"PREP|{dir_id}|{trans['name']}|TRANS"})
    
    # Pagination Logic
    PER_PAGE = 10
    total_pages = (len(items) + PER_PAGE - 1) // PER_PAGE
    # Safety check if filter reduced pages
    if page >= total_pages: page = 0
    
    page_items = items[page * PER_PAGE : (page + 1) * PER_PAGE]

    role = auth_service.get_role(user_id)
    perms = auth_service.roles.get(role, [])
    path = repo_service.get_full_path(dir_id)
    
    # Pass filter_mode to UI
    text = Msg.browser_status(path, role, BOT_FROZEN, page, total_pages)
    kb = Keyboards.main_menu(page_items, page, total_pages, dir_id, role, perms, node['parent'], filter_mode)
    
    if update.callback_query:
        await safe_edit_message(update.callback_query, text, kb)
    else:
        await update.message.reply_text(text, reply_markup=kb, parse_mode='HTML')

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global BOT_FROZEN
    query = update.callback_query
    try: await query.answer()
    except BadRequest: pass 

    user_id = update.effective_user.id
    if not await check_access(update, user_id): return

    data = query.data.split("|")
    action = data[0]

    # --- NAVIGATION ---
    if action == "OPEN":
        # Data format: OPEN | dir_id | page | filter_mode
        d_id = data[1]
        pg = int(data[2]) if len(data) > 2 else 0
        
        # Default to ALL if old button clicked or param missing
        f_mode = data[3] if len(data) > 3 else 'ALL'
        
        await show_directory(update, context, d_id, pg, f_mode)

    elif action == "PREP":
        # Data: PREP | DirID | Name | Type
        is_job = (len(data) < 4) or (data[3] == 'JOB')
        await render_prep_screen(query, int(data[1]), data[2], user_id, is_job)

    # --- EXECUTION & HISTORY ---
    elif action == "RUN":
        dir_id, name = int(data[1]), data[2]
        is_job = (len(data) < 4) or (data[3] == 'JOB')
        path = repo_service.get_full_path(dir_id)
        await execute_process(update, context, name, path, dir_id, is_job)

    elif action == "HISTORY":
        dir_id, name = int(data[1]), data[2]
        is_job = (len(data) < 4) or (data[3] == 'JOB')
        
        # Fetch DB History
        history = repo_service.get_history(name, is_job)
        text = Msg.history_view(name, history)
        
        # Back button returns to Prep screen
        kb = [[InlineKeyboardButton("🔙 Back", callback_data=f"PREP|{dir_id}|{name}|{'JOB' if is_job else 'TRANS'}"),
               InlineKeyboardButton("🔄 Refresh", callback_data=f"HISTORY|{dir_id}|{name}|{'JOB' if is_job else 'TRANS'}")]]
        
        await safe_edit_message(query, text, InlineKeyboardMarkup(kb))

    # --- MONITOR DASHBOARD ---
    elif action == "MONITOR":
        active_jobs = carte_service.get_active_jobs()
        active_trans = carte_service.get_active_trans()
        all_active = active_jobs + active_trans
        
        if not all_active:
            text = "🖥️ <b>Monitor</b>\n\n✅ <i>No active processes running.</i>"
            kb = [[InlineKeyboardButton("🔄 Refresh", callback_data="MONITOR")],
                  [InlineKeyboardButton("🔙 Main Menu", callback_data="OPEN|-1|0")]]
        else:
            text = f"🖥️ <b>Monitor ({len(all_active)} Running)</b>\n━━━━━━━━━━━━━━━━━━\n"
            for p in all_active:
                icon = "✴️" if 'job_id' in p else "⚙️"
                p_name = p.get('name', 'Unknown')
                # Show only first 8 chars of ID in text
                short_id = p.get('id', '')[:8]
                text += f"{icon} <b>{p_name}</b>\n   └ 🆔 <code>{short_id}...</code>\n"
            
            kb = [[InlineKeyboardButton("🛑 Stop a Process...", callback_data="STOP_MENU")], 
                  [InlineKeyboardButton("🔄 Refresh", callback_data="MONITOR")],
                  [InlineKeyboardButton("🔙 Main Menu", callback_data="OPEN|-1|0")]]
            
        await safe_edit_message(query, text, InlineKeyboardMarkup(kb))

    # --- STOP MENU (Generates Buttons with Short IDs) ---
    elif action == "STOP_MENU":
        active_jobs = carte_service.get_active_jobs()
        active_trans = carte_service.get_active_trans()
        all_active = active_jobs + active_trans
        
        if not all_active:
            await query.answer("Nothing is running anymore!", show_alert=True)
            # Redirect back to Monitor
            kb = [[InlineKeyboardButton("🔄 Refresh", callback_data="MONITOR")]]
            await safe_edit_message(query, "🖥️ <b>Monitor</b>\n\nNo active processes.", InlineKeyboardMarkup(kb))
            return

        kb = []
        for p in all_active:
            p_name = p.get('name', 'Unknown')
            p_type = "Job" if p.get('type') == 'JOB' else "Trans"
            p_id = p.get('id', '')
            
            # KEY FIX: Use only first 8 chars of ID to stay under 64-byte limit
            # Data: STOP_EXEC | 35c22222
            short_id = p_id[:8]
            
            kb.append([InlineKeyboardButton(f"🔴 {p_name}", callback_data=f"STOP_EXEC|{short_id}")])
            
        kb.append([InlineKeyboardButton("🔙 Cancel", callback_data="MONITOR")])
        
        await safe_edit_message(query, "🛑 <b>Select Process to STOP:</b>", InlineKeyboardMarkup(kb))

   # ... inside handlers/core.py ...
    elif action == "SYS_HEALTH":
        # 1. Fetch Data
        stats = system_service.get_health_report()
        
        # 2. Determine Icons based on thresholds
        cpu_icon = "🟢" if stats['cpu'] < 70 else ("🟡" if stats['cpu'] < 90 else "🔴")
        mem_icon = "🟢" if stats['mem_percent'] < 75 else ("🟡" if stats['mem_percent'] < 90 else "🔴")
        disk_icon = "🟢" if stats['disk_percent'] < 80 else ("🟡" if stats['disk_percent'] < 95 else "🔴")

        # 3. Format Message
        text = (
            f"🖥️ <b>Server Health Report</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"{cpu_icon} <b>CPU:</b> {stats['cpu']}%\n"
            f"{mem_icon} <b>RAM:</b> {stats['mem_used']}GB / {stats['mem_total']}GB ({stats['mem_percent']}%)\n"
            f"{disk_icon} <b>Disk:</b> {stats['disk_free']}GB Free ({stats['disk_percent']}% Used)\n\n"
        )
        
        if stats['heavy_processes']:
            text += "<b>⚠️ Heavy Processes:</b>\n" + "\n".join(stats['heavy_processes'])
        else:
            text += "✅ No memory hogs detected."

        # 4. Add 'Back' button to return to Monitor
        kb = [[InlineKeyboardButton("🔙 Back to Monitor", callback_data="MONITOR")]]
        
        await safe_edit_message(query, text, InlineKeyboardMarkup(kb))

    elif action == "PEEK_LOG":
        job_name = data[1]

        await query.answer("Fetching logs...")
        # Fetch the tail
        log_content = repo_service.get_log_tail(job_name)
        

        # ✅ FIX: Cut the text if it's too long for Telegram
        # Telegram limit is 4096. We use 3000 to be safe with HTML tags.
        if len(log_content) > 3000:
            # Take the LAST 3000 characters
            log_content = "..." + log_content[-3000:]
            
            # Optional: Try to cut at the first newline to make it look clean
            first_newline = log_content.find('\n')
            if first_newline != -1:
                log_content = "..." + log_content[first_newline+1:]

        safe_log = html.escape(log_content)

        # Send as a fresh message (so it doesn't clutter the menu)
        # Using <pre> tag for code formatting
        text = f"📜 <b>Log Tail: {job_name}</b>\n<pre>{safe_log}</pre>"
        
        kb = [[InlineKeyboardButton("❌ Close Log", callback_data="DELETE_MSG")]]
        # Delete the menu to "clean up" or just send a new message
        await query.message.reply_text(text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(kb))

    elif action == "STOP_EXEC":
        short_id_target = data[1]
        
        # 1. Fetch live list to find the FULL ID
        active_jobs = carte_service.get_active_jobs()
        active_trans = carte_service.get_active_trans()
        all_active = active_jobs + active_trans
        
        target = next((p for p in all_active if p['id'].startswith(short_id_target)), None)
        
        if not target:
            await query.answer("⚠️ Process not found.", show_alert=True)
            kb = [[InlineKeyboardButton("🔄 Refresh", callback_data="MONITOR")]]
            await safe_edit_message(query, "⚠️ Process not found.\nIt might have finished already.", InlineKeyboardMarkup(kb))
            return

        name = target['name']
        full_id = target['id']
        
        # ✅ FIX 1: Detect Type (Job vs Trans)
        is_job = (target['type'] == 'JOB')
        
        # ✅ FIX 2: Pass 'is_job' to the service
        # This matches the new signature: stop_process(name, id, is_job)
        success, msg = carte_service.stop_process(name, full_id, is_job)
        
        if success:
            audit_service.log(user_id, "STOP", name, f"ID: {full_id} ({target['type']})")
            
            await query.answer(f"🛑 Stopping {name}...", show_alert=False)
            
            kb = [[InlineKeyboardButton("🔄 Refresh Monitor", callback_data="MONITOR")]]
            await safe_edit_message(query, f"✅ <b>Signal Sent:</b> {name}\n\nType: {target['type']}\nResponse: {msg}", InlineKeyboardMarkup(kb))
        else:
            await query.answer(f"⚠️ Failed: {msg}", show_alert=True)

    # --- SQL VIEWER FOR ANALYSTS ---
    elif action == "GET_SQL":
        dir_id, trans_name = int(data[1]), data[2]
        chat_id = update.effective_chat.id
        
        sources = repo_service.get_trans_sql(trans_name)
        
        if not sources:
            await query.answer("⚠️ No Table Inputs found.", show_alert=True)
            return
            
        if len(sources) == 1:
            # OPTIMIZATION: Skip menu if only 1 source exists
            src = sources[0]
            step_name = src['step']
            
            kb = [
                [InlineKeyboardButton("✏️ Propose Change", callback_data=f"EDIT_SQL_INIT|{dir_id}|{trans_name}|{step_name}")],
                [InlineKeyboardButton("📜 Previous Versions", callback_data=f"SQL_HIST_LIST|{dir_id}|{trans_name}|{step_name}")],
                [InlineKeyboardButton("🔙 Back", callback_data=f"PREP|{dir_id}|{trans_name}|TRANS")]
            ]
            
            await send_smart_content(
                context, chat_id, 
                f"📄 <b>Source: {step_name}</b>", 
                src['sql'], 
                filename=f"{step_name}.sql", 
                reply_markup=InlineKeyboardMarkup(kb)
            )
            try: await query.delete_message()
            except: pass

        else:
            # Show Menu
            await safe_edit_message(
                query, 
                f"🔀 <b>Select Source Step</b>\nFound {len(sources)} inputs in <code>{trans_name}</code>:", 
                Keyboards.source_selector(dir_id, trans_name, sources)
            )

    elif action == "SHOW_SQL":
        dir_id, trans_name, step_name = int(data[1]), data[2], data[3]
        chat_id = update.effective_chat.id
        
        sources = repo_service.get_trans_sql(trans_name)
        target = next((s for s in sources if s['step'] == step_name), None)
        
        if target:
            kb = [
                [InlineKeyboardButton("✏️ Propose Change", callback_data=f"EDIT_SQL_INIT|{dir_id}|{trans_name}|{step_name}")],
                [InlineKeyboardButton("📜 Previous Versions", callback_data=f"SQL_HIST_LIST|{dir_id}|{trans_name}|{step_name}")],
                [InlineKeyboardButton("🔙 Back", callback_data=f"GET_SQL|{dir_id}|{trans_name}")] # Back to list
            ]
            
            await send_smart_content(
                context, chat_id, 
                f"📄 <b>Source: {target['step']}</b>", 
                target['sql'], 
                filename=f"{step_name}.sql", 
                reply_markup=InlineKeyboardMarkup(kb)
            )
            try: await query.delete_message()
            except: pass
        else:
            await query.answer("Error finding step.", show_alert=True)

    elif action == "SQL_HIST_LIST":
        dir_id, trans_name, step_name = int(data[1]), data[2], data[3]
        
        # NOTE: You must implement get_sql_history_list in repo_service!
        history = repo_service.get_sql_history_list(trans_name, step_name)
        
        if not history:
            await query.answer("⚠️ No history versions found.", show_alert=True)
            return

        kb = []
        for h in history:
            # h needs keys: 'date', 'user', 'id'
            lbl = f"📅 {h.get('date', '?')} ({h.get('user', 'unk')})"
            kb.append([InlineKeyboardButton(lbl, callback_data=f"VIEW_OLD_SQL|{dir_id}|{h['id']}")])
            
        kb.append([InlineKeyboardButton("🔙 Back to Current", callback_data=f"SHOW_SQL|{dir_id}|{trans_name}|{step_name}")])
        
        await safe_edit_message(query, f"📜 <b>History: {step_name}</b>\nSelect version:", InlineKeyboardMarkup(kb))

    elif action == "VIEW_OLD_SQL":
        dir_id, hist_id = int(data[1]), data[2]
        chat_id = update.effective_chat.id
        
        # NOTE: You must implement get_archived_sql in repo_service!
        old_sql = repo_service.get_archived_sql(hist_id)
        
        if old_sql:
            kb = [[InlineKeyboardButton("🗑️ Close View", callback_data=f"OPEN|{dir_id}|0")]]
            await send_smart_content(
                context, chat_id, 
                f"📜 <b>Archived Version (ID: {hist_id})</b>", 
                old_sql, 
                filename=f"archive_{hist_id}.sql", 
                reply_markup=InlineKeyboardMarkup(kb)
            )
        else:
            await query.answer("Error fetching archive.", show_alert=True)

    elif action == "EDIT_SQL_INIT":
        dir_id, trans_name, step_name = int(data[1]), data[2], data[3]
        
        # Set User State to Capture Text Input
        USER_STATE[user_id] = {
            'mode': 'AWAITING_NEW_SQL',
            'dir_id': dir_id,
            'trans': trans_name,
            'step': step_name
        }
        
        kb = [[InlineKeyboardButton("🔙 Cancel", callback_data=f"SHOW_SQL|{dir_id}|{trans_name}|{step_name}")]]
        await safe_edit_message(query, f"✍️ <b>Proposing change for:</b> <code>{step_name}</code>\n\n⬇️ <b>Paste the new SQL query below:</b>", InlineKeyboardMarkup(kb))

    # ... inside handle_callback ...

    elif action == "SEARCH_INIT":
        # 1. Set Default State: Name Search
        # This allows the user to just start typing immediately if they want.
        USER_STATE[user_id] = {'mode': 'SEARCH', 'type': 'NAME'}
        
        # 2. Fetch History
        recent_searches = audit_service.get_user_search_history(user_id)
        
        kb = []
        # --- Mode Selectors ---
        kb.append([InlineKeyboardButton("🔤 Name Search (Active)", callback_data="SEARCH_MODE|NAME")])
        kb.append([InlineKeyboardButton("🕵️ Find Table Usage", callback_data="SEARCH_MODE|USAGE")])
        
        # --- Recent History ---
        if recent_searches:
            # Add a non-clickable separator for visual clarity
            kb.append([InlineKeyboardButton("--- 🕒 Recent ---", callback_data="IGNORE")])
            for term in recent_searches:
                # Truncate if term is super long
                label = term[:20] + "..." if len(term) > 20 else term
                kb.append([InlineKeyboardButton(f"🕒 {label}", callback_data=f"SEARCH_RUN|{term}")])
        
        kb.append([InlineKeyboardButton("🔙 Cancel", callback_data="OPEN|-1|0")])
        
        await safe_edit_message(query, "🔍 <b>Search Repo</b>\n\nType a name to search, or select an option:", InlineKeyboardMarkup(kb))

    elif action == "SEARCH_RUN":
        term = data[1]
        
        # 1. Log the re-run (updates timestamp in audit log)
        audit_service.log(user_id, "SEARCH", "REPO", term)
        
        # 2. Execute Name Search (History items default to Name search)
        matches = repo_service.search_repo(term)
        
        header = f"🔍 <b>Found {len(matches)} matches for '{term}':</b>"
        if len(matches) > 15: header += "\n<i>(Showing top 15)</i>"
        
        kb = Keyboards.search_results(matches)
        
        # We send a new message because results lists can be long
        await safe_edit_message(query, header, kb)
        
        # Clear state so they don't accidentally search again by typing
        USER_STATE[user_id] = None

    elif action == "SEARCH_MODE":
        # Data format: SEARCH_MODE | NAME  or  SEARCH_MODE | USAGE
        mode = data[1]
        
        # 1. Update State so handle_text knows what logic to use
        USER_STATE[user_id] = {
            'mode': 'SEARCH', 
            'type': mode  # 'NAME' or 'USAGE'
        }
        
        # 2. Update UI to guide the user
        if mode == 'NAME':
            msg = "🔤 <b>Name Search</b>\n\nType a Job or Transformation name to find it:"
        else:
            msg = "🕵️ <b>Dependency Search</b>\n\nEnter a <b>Table Name</b> (e.g. <code>AKK_LOAN</code>) to see where it is used:"
            
        kb = [[InlineKeyboardButton("🔙 Cancel", callback_data="SEARCH_INIT")]]
        await safe_edit_message(query, msg, InlineKeyboardMarkup(kb))

    elif action == "MY_ACTIVITY":
        # 1. Fetch Personal Logs
        logs = audit_service.get_user_logs(user_id)
        
        if not logs:
            text = "📜 <b>My Activity</b>\n\nYou haven't done anything yet!"
        else:
            # Format: 🔹 12-15 10:00: EXECUTE MAIN_JOB
            log_lines = []
            for l in logs:
                icon = "🔹"
                if l['action'] == "EXECUTE": icon = "🚀"
                elif l['action'] == "STOP": icon = "🛑"
                elif l['action'] == "CODE_UPDATE": icon = "✏️"
                
                log_lines.append(f"{icon} <b>{l['time']}</b>: {l['action']} <code>{l['target']}</code>")
            
            text = f"📜 <b>My Recent Activity</b>\n\n" + "\n".join(log_lines)

        # 2. Add Back Button
        kb = [[InlineKeyboardButton("🔙 Main Menu", callback_data="OPEN|-1|0")]]
        await safe_edit_message(query, text, InlineKeyboardMarkup(kb))        

    # --- ADMIN / SCHEDULER (Existing logic) ---
    elif action == "ADMIN_MENU": await render_admin_panel(query, user_id)
    elif action == "TOGGLE_FREEZE":
        if auth_service.get_role(user_id) == "SUPER":
            BOT_FROZEN = not BOT_FROZEN
            await render_admin_panel(query, user_id)
    elif action == "ADMIN_ADD_USER":
        if auth_service.get_role(user_id) == "SUPER":
            USER_STATE[user_id] = {'mode': 'ADD_USER_ID'}
            kb = [[InlineKeyboardButton("🔙 Cancel", callback_data="ADMIN_MENU")]]
            await safe_edit_message(query, "✍️ Enter Telegram ID:", InlineKeyboardMarkup(kb))
    elif action == "SAVE_USER":
        if auth_service.get_role(user_id) == "SUPER":
            if auth_service.add_user(data[1], data[2]):
                await query.answer("✅ Saved!")
                await render_admin_panel(query, user_id)
    
    elif action == "SCHED_DASHBOARD":
        jobs = scheduler_service.list_jobs()
        text = Msg.scheduler_dashboard(jobs)
        role = auth_service.get_role(user_id)
        perms = auth_service.roles.get(role, [])
        kb = Keyboards.scheduler_dashboard(perms)
        await safe_edit_message(query, text, kb)

    elif action == "SCHED_MENU":
        dir_id, name = int(data[1]), data[2]
        USER_STATE[user_id] = {'job': name, 'dir_id': dir_id} 
        kb = [[InlineKeyboardButton("🔙 Cancel", callback_data=f"PREP|{dir_id}|{name}|JOB")]]
        await safe_edit_message(query, f"✍️ <b>Time for {name}</b> (HH:MM):", InlineKeyboardMarkup(kb))

    elif action == "SCHED_DEFAULT":
        try:
            dir_id, name = int(data[1]), data[2]
            
            # This is likely where it crashes (DB Error)
            cfg = repo_service.get_job_schedule_config(name)
            
            if not cfg or cfg.get('type') == 'NONE':
                await query.answer("⚠️ No default schedule found in DB.", show_alert=True)
                return

            trigger = CronTrigger(hour=cfg['h'], minute=cfg['m']) if cfg['type'] == 'DAILY' else None
            if cfg['type'] == 'INTERVAL': trigger = IntervalTrigger(minutes=cfg['m'])
            
            if trigger:
                scheduler_service.add_job(scheduled_job_wrapper, trigger, [name, dir_id], name)
                audit_service.log(user_id, "SCHEDULE_ADD", name, f"Type: {cfg['type']}")
                await query.answer("✅ Schedule Activated!")
                # Refresh screen
                await render_prep_screen(query, dir_id, name, user_id, is_job=True)
            else:
                await query.answer("⚠️ Invalid Schedule Config.", show_alert=True)

        except Exception as e:
            # This will show you the ACTUAL error instead of doing nothing
            await query.answer(f"❌ DB Error: {str(e)}", show_alert=True)
            logging.error(f"Scheduler Error: {e}")

    elif action == "SCHED_STOP":
        scheduler_service.remove_job(data[2])
        audit_service.log(user_id, "SCHEDULE_DEL", data[2])
        await render_prep_screen(query, int(data[1]), data[2], user_id, is_job=True)

    # Add this small helper too if you don't have it
    elif action == "DELETE_MSG":
        await query.message.delete()

    elif action == "DASHBOARD":
        # 1. Fetch Stats (Renamed method)
        failures = repo_service.get_broken_processes()
        
        # 2. Render Text
        text = Msg.manager_report(failures)
        
        # 3. Back Button
        kb = [[InlineKeyboardButton("🔙 Main Menu", callback_data="OPEN|-1|0")]]
        await safe_edit_message(query, text, InlineKeyboardMarkup(kb))    

    elif action == "GOTO_PAGE_INIT":
        # Data: GOTO_PAGE_INIT | DirID | FilterMode
        dir_id = int(data[1])
        filter_mode = data[2] if len(data) > 2 else 'ALL'
        
        USER_STATE[user_id] = {
            'mode': 'AWAITING_PAGE_NUM',
            'dir_id': dir_id,
            'filter': filter_mode
        }
        
        kb = [[InlineKeyboardButton("🔙 Cancel", callback_data=f"OPEN|{dir_id}|0|{filter_mode}")]]
        await safe_edit_message(query, "🔢 <b>Jump to Page</b>\n\nEnter the page number:", InlineKeyboardMarkup(kb))        

    # ... inside handle_callback ...

    elif action == "KILL_CONFIRM":
        # ✅ FIX: Fetch the role first!
        user_role = auth_service.get_role(user_id)

        if user_role != 'SUPER':
            await query.answer("⛔ Access Denied", show_alert=True)
            return

        kb = [
            [InlineKeyboardButton("💣 YES, KILL IT", callback_data="KILL_BOT")],
            [InlineKeyboardButton("🔙 No, Cancel", callback_data="ADMIN_MENU")]
        ]
        await safe_edit_message(query, "⚠️ <b>DANGER ZONE</b> ⚠️\n\nAre you sure you want to shut down the bot process?\n(You will need SSH to start it again)", InlineKeyboardMarkup(kb))

    elif action == "KILL_BOT":
        # ✅ FIX: Fetch the role first!
        user_role = auth_service.get_role(user_id)
        
        if user_role != 'SUPER': 
            await query.answer("⛔ Access Denied", show_alert=True)
            return

        await query.answer("💀 Shutting down...", show_alert=True)
        await safe_edit_message(query, "🔌 <b>Bot is shutting down now.</b>\n\nBye bye! 👋")
        
        logging.critical(f"User {user_id} initiated remote shutdown.")
        
        # Give Telegram a second to send the message before dying
        await asyncio.sleep(1) 
        
        import sys
        sys.exit(0)

async def execute_process(update, context, name, path, dir_id, is_job):
    chat_id = update.effective_chat.id
    msg_func = update.callback_query.edit_message_text if update.callback_query else update.message.reply_text
    
    kb = [[InlineKeyboardButton("🔙 Main Menu", callback_data="OPEN|-1|0")]]
    await msg_func(Msg.execution_start(name), parse_mode='HTML', reply_markup=InlineKeyboardMarkup(kb))
    
    if is_job:
        success, res = await carte_service.trigger_job(name, path)
    else:
        success, res = await carte_service.trigger_trans(name, path)

    # --- NEW: AUDIT LOG ---
    if success:
        user_id = update.effective_user.id
        audit_service.log(user_id, "EXECUTE", name, f"Carte ID: {res}")
    
    if success:
        kb = Keyboards.execution_controls(dir_id, name)
        await context.bot.send_message(chat_id, Msg.execution_success(name, res), parse_mode='HTML', reply_markup=kb)
        asyncio.create_task(monitor_loop(context, chat_id, name, res, dir_id, is_job))
    else:
        kb = Keyboards.execution_controls(dir_id, name, is_failure=True)
        await context.bot.send_message(chat_id, Msg.execution_failure(name, res), parse_mode='HTML', reply_markup=kb)

async def monitor_loop(context, chat_id, name, job_id, dir_id, is_job):
    while True:
        await asyncio.sleep(3)
        status, root = carte_service.get_status(name, job_id, is_job)
        if status == "Finished":
            await context.bot.send_message(chat_id, f"🎉 {name} Completed!")
            break
        elif status in ["Stopped", "Failed", "Finished (with errors)"]:
            kb = Keyboards.execution_controls(dir_id, name, is_failure=True)
            log = root.find('logging_string').text or "No Log"
            safe_log = html.escape(str(log)[:3000]) 
            await context.bot.send_message(chat_id, f"⚠️ {name} Failed!\n<pre>{safe_log}</pre>", parse_mode='HTML', reply_markup=kb)
            break

async def send_smart_content(context, chat_id, text_header, long_content, filename="query.sql", reply_markup=None):
    """
    Intelligently sends content.
    - If < 3000 chars: Sends as message text.
    - If > 3000 chars: Sends as a downloadable file.
    """
    if len(long_content) < 3000:
        # Strategy A: Send as Text
        full_text = f"{text_header}\n\n<pre>{html.escape(long_content)}</pre>"
        await context.bot.send_message(chat_id, full_text, parse_mode='HTML', reply_markup=reply_markup)
    else:
        # Strategy B: Send as File
        file_obj = io.BytesIO(long_content.encode('utf-8'))
        file_obj.name = filename
        
        # Send header first
        await context.bot.send_message(chat_id, f"{text_header}\n\n(📁 Content too long for chat, sent as file below)", parse_mode='HTML')
        # Send file with buttons attached
        await context.bot.send_document(chat_id, document=file_obj, caption="📄 Full SQL Query", reply_markup=reply_markup)            

# Text Handler remains largely same, just standard boilerplate...
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    raw_text = update.message.text.strip() if update.message.text else ""
    text = raw_text.replace('\xa0', ' ').replace('\u200b', '').strip()


    # --- NEW: SUPER HOME BUTTON HANDLER ---
    if text == "🏠 Main Menu":
        # Reset state just in case they were doing something else
        USER_STATE[user_id] = None 
        # Show Root Directory
        await show_directory(update, context, -1, 0)
        return

    state = USER_STATE.get(user_id)
    if not state: return

    # --- NEW: KILL COMMAND HANDLER ---
    if text.startswith("/kill "):
        # Format: /kill <UUID> <TYPE>
        try:
            parts = text.split()
            c_id = parts[1]
            p_type = parts[2] # JOB or TRANS
            
            is_job = (p_type == "JOB")
            
            # We need the Name for the Carte API, but we only have ID.
            # We must fetch the name first by checking active list.
            active_jobs = carte_service.get_active_jobs()
            active_trans = carte_service.get_active_trans()
            all_active = active_jobs + active_trans
            
            target_name = None
            for p in all_active:
                if p['id'] == c_id:
                    target_name = p['name']
                    break
            
            if not target_name:
                await update.message.reply_text(f"⚠️ Process {c_id} not found. Already stopped?")
                return

            # Execute Stop
            success = carte_service.stop_process(target_name, c_id, is_job)
            
            if success:
                audit_service.log(user_id, "STOP_CMD", target_name, f"ID: {c_id}")
                await update.message.reply_text(f"✅ <b>Signal Sent:</b> {target_name}\nChecking status...", parse_mode='HTML')
            else:
                await update.message.reply_text("❌ Failed to send stop signal.")
                
        except Exception as e:
            await update.message.reply_text(f"❌ Bad Command: {e}")
        return

    if state.get('mode') == 'AWAITING_PAGE_NUM':
        if not text.isdigit():
            await update.message.reply_text("❌ Please enter a valid number.")
            return
            
        page_num = int(text)
        dir_id = state['dir_id']
        f_mode = state['filter']
        
        # Logic: User enters "1" -> Index 0. User enters "5" -> Index 4.
        # We rely on show_directory to handle out-of-bounds safety.
        target_page_index = max(0, page_num - 1)
        
        await show_directory(update, context, dir_id, target_page_index, f_mode)
        USER_STATE[user_id] = None
        return

    if state.get('mode') == 'AWAITING_NEW_SQL':
        await process_sql_update(update, context, text, state, user_id)
        return

    # # --- SQL UPDATE FLOW ---
    # if state.get('mode') == 'AWAITING_NEW_SQL':
    #     trans = state['trans']
    #     step = state['step']
    #     dir_id = state['dir_id']
        
    #     # 1. Run Tests (Syntax/Security)
    #     is_valid, msg = repo_service.validate_sql_syntax(text)
    #     if not is_valid:
    #         await update.message.reply_text(f"❌ <b>Test Failed:</b> {msg}\n\nPlease try again or click Cancel.")
    #         return # Keep state active, let them retry

    #     # 2. Update Repo & Versioning
    #     success, db_msg = repo_service.backup_and_update_sql(trans, step, text, user_id)
        
    #     if success:
    #         await update.message.reply_text(f"✅ <b>Success!</b>\nrepo updated for <code>{step}</code>.\nOld version archived.")
            
    #         # Reset UI to show the NEW SQL
    #         # We need to construct a fake query object or just send a new menu
    #         # Easiest is to clear state and offer a link back
    #         kb = [[InlineKeyboardButton("🔙 View New SQL", callback_data=f"SHOW_SQL|{dir_id}|{trans}|{step}")]]
    #         await update.message.reply_text("Click below to verify:", reply_markup=InlineKeyboardMarkup(kb))
    #         USER_STATE[user_id] = None
    #     else:
    #         await update.message.reply_text(f"❌ <b>DB Error:</b> {db_msg}")
    #     return
    
    # --- SEARCH FLOW ---
    if state and state.get('mode') == 'SEARCH':
        search_type = state.get('type', 'NAME')
        
        # ✅ FIX: Save the search term to history!
        # This matches the logging format used in SEARCH_RUN
        audit_service.log(user_id, "SEARCH", "REPO", text)

        if search_type == 'NAME':
            matches = repo_service.search_repo(text)
            header = f"🔍 <b>Name Matches for '{text}':</b>"
        else:
            matches = repo_service.find_sql_usage(text)
            header = f"🕵️ <b>Table Usage: '{text}':</b>\n<i>(Found in these Transformations)</i>"

        if not matches:
            await update.message.reply_text(f"❌ No matches found for '{text}'")
            return
            
        kb = Keyboards.search_results(matches)
        await update.message.reply_text(f"{header}\nFound {len(matches)} results:", reply_markup=kb, parse_mode='HTML')
        
        USER_STATE[user_id] = None 
        return

    if state.get('mode') == 'ADD_USER_ID':
        if not text.isdigit():
            await update.message.reply_text("Digits only.")
            return
        kb = Keyboards.role_selector(text)
        await update.message.reply_text(f"User: {text}. Role?", reply_markup=kb)
        USER_STATE[user_id] = None
        return

    try:
        h, m = map(int, text.split(':'))
        scheduler_service.add_job(scheduled_job_wrapper, CronTrigger(hour=h, minute=m), [state['job'], state['dir_id']], state['job'])
        await update.message.reply_text(f"✅ Scheduled {state['job']}")
        USER_STATE[user_id] = None 
    except:
        await update.message.reply_text("Error. Use HH:MM.")

