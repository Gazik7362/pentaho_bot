import psutil
import shutil
import os

class SystemService:
    def get_health_report(self):
        """
        Returns a dictionary with vital system stats.
        """
        # 1. CPU Usage
        cpu_usage = psutil.cpu_percent(interval=1)
        
        # 2. Memory (RAM)
        mem = psutil.virtual_memory()
        mem_total_gb = round(mem.total / (1024**3), 1)
        mem_used_gb = round(mem.used / (1024**3), 1)
        mem_percent = mem.percent
        
        # 3. Disk Space (Root partition)
        disk = shutil.disk_usage("/")
        disk_total_gb = round(disk.total / (1024**3), 1)
        disk_free_gb = round(disk.free / (1024**3), 1)
        disk_percent = round((disk.used / disk.total) * 100, 1)
        
        # 4. Process Check (Find PDI/Java hogs)
        java_procs = []
        for proc in psutil.process_iter(['pid', 'name', 'memory_info']):
            try:
                if 'java' in proc.info['name'].lower():
                    mem_gb = round(proc.info['memory_info'].rss / (1024**3), 2)
                    if mem_gb > 0.5: # Only show heavy processes (>500MB)
                        java_procs.append(f"☕ Java (PID {proc.info['pid']}): {mem_gb} GB")
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass

        return {
            "cpu": cpu_usage,
            "mem_total": mem_total_gb,
            "mem_used": mem_used_gb,
            "mem_percent": mem_percent,
            "disk_total": disk_total_gb,
            "disk_free": disk_free_gb,
            "disk_percent": disk_percent,
            "heavy_processes": java_procs
        }

system_service = SystemService()