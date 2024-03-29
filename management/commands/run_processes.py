import logging
import os
import sys
import time
import atexit
import signal

from django.db import connection
from django.db.models import Q
from multiprocessing import Process
from socket import gethostbyaddr, gethostname


from django.core.management.base import BaseCommand
from bg_processor.models import Job, get_host
from bg_processor.settings import JOB_REFRESH_TIME

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

run_command = None

class Command(BaseCommand):
    help = 'Runs all jobs that are due.'
    procs = []
    
    def get_jobs(self):
        return Job.objects.filter(Q(hosts__name=HOSTNAME) | Q(hosts=None), disabled=False)
    
    def add_job(self, job):
        logger.info('add job %s'%job.name)
        proc = JobProcess(job)
        proc.start()
        self.procs.append(proc)
        
        def de_register(*args):
            if proc.is_alive():
                
                logger.debug('de_register %s'%proc.pid)      
                connection.close()   
                try:
                    proc.job.hault()
                except Exception, e:
                    logger.debug('de_register exception: %s'%e)
                    pass
                try:
                    proc.terminate()
                    os.wait()
                except Exception, e:
                    pass
                logger.debug('killed %s'%proc.pid)

        atexit.register(de_register)     

        
    def stop_job(self, proc, clear=True):
        connection.close()
        proc.job.hault()
        proc.terminate()
        try:
            os.wait()
        except:
            pass
        if clear:
            self.procs.remove(proc)       
        logger.debug('removed job')
        
              
    def handle(self, *args, **options):  
        run_command = self    
        try:
            for job in self.get_jobs():
                if not job.check_is_running():
                    
                    # Only run the Job if it isn't already running
                    self.add_job(job)            
                    
            while True:
                
                """ Checks that procs are alive every 10 seconds and attempts to restart a job if it's dead.
                """
                time.sleep(JOB_REFRESH_TIME)
                for proc in self.procs:
                    if not proc.is_alive():
                        logger.warning('Job %s has stopped unexpectedly!'%proc.job.name)
                        logger.info('restarting %s'%proc.job.name)
                        self.stop_job(proc)
                        self.add_job(proc.job)        
                        
                    try:
                        connection.close()
                        job = Job.objects.get(pk = proc.job.pk)
                    except Job.DoesNotExist:
                        # Job has been removed
                        logger.info('existing job no longer exists, stopping job %s'% proc.job.name)
                        self.stop_job(proc)
                        
                    if job.disabled:
                        logger.info('stopping %s'%proc.job.name)
                        self.stop_job(proc)
                
                jobs = self.get_jobs()
                proc_jobs = [p.job.id for p in self.procs]
                for job in jobs:
                    """ Check that any new jobs have come available.
                    """                    
                    if job.id not in proc_jobs and not job.check_is_running():
                        self.add_job(job)

        except KeyboardInterrupt:
            logger.info('stopping processes')
        

# This catches an os kill signal and does a natural shutdown which will call the atexit registered functions
# which allows the process to shutdown all it's child processes
def cleanup(*args):
    sys.exit()  
signal.signal(signal.SIGTERM, cleanup)

          