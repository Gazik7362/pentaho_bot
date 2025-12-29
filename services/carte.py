import requests
import urllib.parse
import logging
import xml.etree.ElementTree as ET
from config.settings import CARTE_URL, CARTE_AUTH, REPO_CONF

class CarteService:
    
    @staticmethod
    def _execute(endpoint, name, directory, params=None):
        """Helper to avoid code duplication between Job and Trans."""
        strategies = [
            {"params": {'dir': directory, 'name': name}}, 
            {"params": {'dir': '/', 'name': f"{directory}/{name}".replace('//', '/')}},
            {"params": {'dir': directory.lstrip('/'), 'name': name}}
        ]
        
        last_error = ""
        for strat in strategies:
            final_params = strat['params'].copy()
            if 'executeJob' in endpoint:
                final_params['job'] = final_params.pop('name')
            else:
                final_params['trans'] = final_params.pop('name')

            payload = {**REPO_CONF, **final_params, 'level': 'Basic'}
            if params: payload.update(params)
            
            query = urllib.parse.urlencode(payload, quote_via=urllib.parse.quote, safe='/')
            url = f"{CARTE_URL}/{endpoint}?{query}"
            
            try:
                response = requests.get(url, auth=CARTE_AUTH, timeout=10)
                if response.status_code == 200:
                    text = response.text
                    if 'OK' in text or '<result>OK</result>' in text:
                        try:
                            return True, ET.fromstring(text).find('id').text
                        except:
                            return True, "Started (ID Unknown)"
                    try:
                        last_error = ET.fromstring(text).find('message').text
                    except:
                        last_error = "Carte returned error without message"
                else:
                    last_error = f"HTTP {response.status_code}"
            except Exception as e:
                last_error = str(e)
        return False, last_error

    @staticmethod
    async def trigger_job(job_name, directory):
        return CarteService._execute('executeJob', job_name, directory)

    @staticmethod
    async def trigger_trans(trans_name, directory):
        return CarteService._execute('executeTrans', trans_name, directory)

    @staticmethod
    @staticmethod
    def stop_process(name, id, is_job=True):
        """Stops a running process."""
        endpoint = "stopJob" if is_job else "stopTrans"
        p_name = "name" if is_job else "trans"
        
        try:
            # Send Stop Signal
            params = {p_name: name, 'id': id, 'xml': 'Y'}
            response = requests.get(f"{CARTE_URL}/{endpoint}/", params=params, auth=CARTE_AUTH, timeout=5)
            
            if response.status_code == 200:
                return True, "🛑 Stop Signal Sent."
            else:
                return False, f"HTTP Error {response.status_code}"
                
        except Exception as e:
            return False, f"Connection Error: {str(e)}"

    @staticmethod
    def get_status(name, id, is_job=True):
        """Checks the status of a specific job/trans ID."""
        endpoint = "jobStatus" if is_job else "transStatus"
        p_name = "name" if is_job else "trans"
        
        try:
            params = {p_name: name, 'id': id, 'xml': 'Y'}
            # Checks status
            r = requests.get(f"{CARTE_URL}/kettle/{endpoint}/", params=params, auth=CARTE_AUTH, timeout=2)
            if r.status_code == 200:
                root = ET.fromstring(r.text)
                return root.find('status_desc').text, root
        except:
            pass
        return "Connection Error", None

    @staticmethod
    def get_active_jobs():
        """Fetches running jobs."""
        try:
            r = requests.get(f"{CARTE_URL}/status/", params={'xml': 'Y'}, auth=CARTE_AUTH, timeout=5)
            if r.status_code == 200:
                running = []
                root = ET.fromstring(r.text)
                lst = root.find('jobstatuslist')
                if lst:
                    for j in lst.findall('jobstatus'):
                        # Only show Running/Initializing jobs
                        if j.find('status_desc').text in ["Running", "Initializing"]:
                            running.append({'name': j.find('jobname').text, 'id': j.find('id').text, 'type': 'JOB', 'job_id': True})
                return running
        except:
            pass
        return []

    @staticmethod
    def get_active_trans():
        """Fetches running transformations."""
        url = f"{CARTE_URL}/kettle/status/?xml=Y"
        try:
            response = requests.get(url, auth=CARTE_AUTH, timeout=5)
            if response.status_code != 200: return []
            
            root = ET.fromstring(response.content)
            active = []
            
            for item in root.findall(".//transstatus"):
                status = item.find("status_desc").text
                if status not in ["Finished", "Stopped", "Stopped (with errors)", "Waiting"]:
                    active.append({
                        'id': item.find("id").text,
                        'name': item.find("transname").text,
                        'status': status,
                        'type': 'TRANS'
                    })
            return active
        except Exception as e:
            logging.error(f"Carte Active Trans Error: {e}")
            return []

carte_service = CarteService()