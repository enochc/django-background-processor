import logging
import os
import sys
import time
from django.db import connection
from django.db.models import Q
from multiprocessing import Process
from socket import gethostbyaddr, gethostname


from django.core.management.base import BaseCommand
from django_bg_processor.models import Job, get_host
from django_bg_processor.settings import JOB_REFRESH_TIME

logger = logging.getLogger('bo.cron.commands')
HOSTNAME = get_host()

class JobProcess(Process):
    """
    Each ``Job`` gets run in it's own ``Process``.
    """
    daemon = True
    
    def __init__(self, job, *args, **kwargs):
        self.job = job
        Process.__init__(self, *args, **kwargs)
    
    def run(self):
        """
        Django doesn't handle multiple processes and db connections well, this cleares any locks currently on the
        db and forces new ones to be created
        """
        connection.close()
        logger.info('starting %s'% self.job.name)
        self.job.run()


class Command(BaseCommand):
    help = 'Runs all jobs that are due.'
    procs = []
    
    def get_jobs(self):
        return Job.objects.filter(Q(hosts__name=HOSTNAME) | Q(hosts=None), disabled=False)
    
    def add_job(self, job):
        proc = JobProcess(job)
        proc.start()
        self.procs.append(proc)
        
    def stop_job(self, proc):
        proc.job.hault()
        proc.terminate()
        self.procs.remove(proc)       
    
    def handle(self, *args, **options):      
        try:
            for job in self.get_jobs():
                if not job.check_is_running():
                    # Only run the Job if it isn't already running
                    self.add_job(job)            
                    
            while True:
                """ Checks that procs are alive every 10 seconds and attempts to restart a job if it's dead.
                """
                for proc in self.procs:
                    if not proc.is_alive():
                        logger.warning('Job %s has stopped unexpectedly!'%proc.job.name)
                        logger.info('restarting %s'%proc.job.name)
                        self.stop_job(proc)
                        self.add_job(proc.job)
                        
                    connection.close()
                    job = Job.objects.get(pk = proc.job.pk)
                    if job.disabled:
                        logger.info('stopping %s'%proc.job.name)
                        self.stop_job(proc)
                        
                for job in self.get_jobs():
                    """ Check that any new jobs have come available.
                    """                    
                    if job.id not in [p.job.id for p in self.procs]:
                        self.add_job(job)
                    
                time.sleep(JOB_REFRESH_TIME)
                
        except KeyboardInterrupt:
            print 'stopping processes'
            for proc in self.procs:
                proc.terminate()
         