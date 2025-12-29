from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
import logging

class SchedulerService:
    def __init__(self):
        self.scheduler = AsyncIOScheduler()
    
    def start(self):
        if not self.scheduler.running:
            self.scheduler.start()
            logging.info("SchedulerService: Started.")

    # --- BASIC CRUD ---
    def add_job(self, func, trigger, args, job_id, meta=None):
        """
        Adds a job with metadata (like dir_id) so we can link back to the menu.
        """
        self.scheduler.add_job(
            func, 
            trigger, 
            args=args, 
            id=job_id, 
            replace_existing=True,
            misfire_grace_time=60  # If bot is down for <60s, run job on restart
        )

    def remove_job(self, job_id):
        try:
            self.scheduler.remove_job(job_id)
            return True
        except:
            return False

    def get_job(self, job_id):
        return self.scheduler.get_job(job_id)

    # --- NEW: ADVANCED FEATURES ---
    def list_jobs(self):
        """Returns a list of all active jobs for the Dashboard."""
        jobs = []
        for j in self.scheduler.get_jobs():
            jobs.append({
                'id': j.id,
                'next_run': j.next_run_time.strftime('%Y-%m-%d %H:%M:%S') if j.next_run_time else 'PAUSED',
                'paused': j.next_run_time is None
            })
        return sorted(jobs, key=lambda x: x['next_run'])

    def pause_job(self, job_id):
        try:
            self.scheduler.pause_job(job_id)
            return True
        except: return False

    def resume_job(self, job_id):
        try:
            self.scheduler.resume_job(job_id)
            return True
        except: return False

    def reschedule_job(self, job_id, new_hour, new_minute):
        """Updates an existing Daily job to a new time."""
        try:
            self.scheduler.reschedule_job(
                job_id, 
                trigger=CronTrigger(hour=new_hour, minute=new_minute)
            )
            return True
        except: return False

# Singleton
scheduler_service = SchedulerService()
