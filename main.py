import logging
import asyncio
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, MessageHandler, filters
from config.settings import TELEGRAM_TOKEN, LOG_LEVEL, SILENCED_LOGGERS
from services.scheduler import scheduler_service

# ✅ FIX: Added handle_document to the imports
from handlers.core import start, handle_callback, handle_text, handle_document

# Logging Setup
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=LOG_LEVEL)
for logger in SILENCED_LOGGERS:
    logging.getLogger(logger).setLevel(logging.WARNING)

async def post_init(app):
    scheduler_service.start()
    print("🚀 Services Started. Bot is Ready.")

if __name__ == '__main__':
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).post_init(post_init).build()
    
    app.add_handler(CommandHandler('start', start))
    app.add_handler(CallbackQueryHandler(handle_callback))
    
    # Text Handler
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_text))
    
    # ✅ File Handler (.sql only)
    app.add_handler(MessageHandler(filters.Document.FileExtension("sql"), handle_document))
    
    print("🤖 Orchestrator V2 Running...")
    app.run_polling()