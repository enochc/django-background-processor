import datetime
import logging
import os
import sys
import time
import cStringIO

from os import getpid

from socket import gethostbyaddr, gethostname
from threading import Thread

from django.db import connection, transaction, models
from django.core.management import call_command
from bg_processor.settings import LOG_REFRESH_TIME


log= logging.getLogger('bo.bg_processes.models')
# Create your models here.

def get_host():
    try:
        hostname = gethostbyaddr(gethostname())[0]
    except gaierror:
        hostname = 'localhost'
    return hostname
    
class JobHost(models.Model):
   
    name = models.CharField("name", max_length=200, default=get_host, unique=True)
    
    def __unicode__(self):
        return self.name
    
class JobLoggingThread(Thread):
    """
    A very simple thread that updates a log while the process is running.
    """
    daemon = True
    halt = False
    
    def _write_log(self):        
        cursor = connection.cursor()
        out = self.job.stdout.getvalue()
        err = self.job.stderr.getvalue()
        
        self.job.stdout.truncate(0)
        self.job.stderr.truncate(0)
        
        now = datetime.datetime.now()
        if len(out) > 0: 
            out = "[%s] %s"% (now, out)
        if len(err) > 0: 
            err = "[%s] %s"% (now, err)

        if len(out) or len(err) > 0:
            query = """
                UPDATE bg_processor_log 
                set stdout = CONCAT(`stdout`, %s), stderr = CONCAT(`stderr`, %s)
                where id = %s
                """
            try:     
                cursor.execute(query, (out, err, self.log.id) )
                transaction.commit_unless_managed()
            except Exception, e:
                log.error('%s query: %s'% (e, query))   
                    
    def _new_log(self):
        self.log = Log(job=self.job)
        self.log.save()
        
    def __init__(self, job, *args, **kwargs):
        self.job = job
        self._new_log()
        Thread.__init__(self, *args, **kwargs)

    def run(self):
        """
        Do not call this directly; call ``start()`` instead.
        """
        while not self.halt:
            now = datetime.datetime.now()
            seconds = (now - self.log.run_date).seconds
            if seconds >= Job.objects.get(pk=self.job.pk).log_life:
                self._new_log()
                
            self._write_log()
            time.sleep(LOG_REFRESH_TIME)
    
    def stop(self):
        """
        Call this to stop logging.
        """
        self.halt = True
        self._write_log()
        raise KeyboardInterrupt

     
class Job(models.Model):
    """
    A recurring ``django-admin`` command to be run.
    """
    name = models.CharField("name", max_length=200)
    pid = models.IntegerField(null=True)
    hosts = models.ManyToManyField('jobhost', null=True, blank=True, related_name='hosts',
                                    help_text="Specify one or more hosts \
     f                               or this job to run on, or leave blank to run any any host.")
    
    command = models.CharField("command", max_length=200, blank=True,
                               help_text="A valid django-admin command to run.")
    args = models.CharField("args", max_length=200, blank=True,
        help_text="Space separated list; e.g: arg1 option1=True")
    
    disabled = models.BooleanField(default=False, help_text='If checked this '
                                                              'job will never run.')
    log_life = models.IntegerField(default=60*60*24, help_text='Number of seconds each log lives be'
                                                            'before a new log is created. (default 24 hours)')
    
    started = models.DateTimeField("last started", editable=False, blank=True, null=True)   
    is_running = models.BooleanField(default=False, editable=False)
    subscribers = models.CharField("Subscribers", help_text="List of email addresses to send notifications"
                                   " in the event of an error", max_length=255, null=True, blank=True)
    job_log = None
    stdout = cStringIO.StringIO()
    stderr = cStringIO.StringIO()
    
    def __unicode__(self):
        return self.name
                
    def run(self):
        if not self.disabled and not self.check_is_running():
            self.is_running = True
            self.started = datetime.datetime.now()
            self.pid = getpid()
            self.job_log = JobLoggingThread(self)
            try: 
                self.save()   
                self.job_log.start() 
            except Exception, e:
                log.error('start save error: %s'%e)          
            
            ostdout = sys.stdout
            ostderr = sys.stderr
            sys.stdout = self.stdout
            sys.stderr = self.stderr

            try:
                call = call_command(self.command, self.args)
            except Exception, e:
                log.error(str(e))              
            
            self.hault()
                
            sys.stdout = ostdout
            sys.stderr = ostderr
    
            return True
        return False
    
    def hault(self):
        # Make sure we don't undo any manual changes to this job in the DB
        db_self = None
        try:
            db_self = self.__class__.objects.get(pk=self.pk)
            self.disabled = db_self.disabled
            self.args = db_self.args
            self.name = db_self.name
            self.log_life = db_self.log_life
            self.command = db_self.command
            self.started = db_self.started
        except self.__class__.DoesNotExist:
            # Job no longer exists
            pass
        
        if self.job_log is not None:
            try:
                self.job_log.stop()
                self.job_log.join()
            except:
                pass
        
        if db_self is not None:   
            self.is_running = False
            self.pid = None
            self.save()         
    
    def check_is_running(self):
        if self.is_running:
            ret = os.path.exists('/proc/%s'%self.pid)
            return ret

        else:
            return False

class Log(models.Model):
    """
    A record of stdout and stderr of a ``Job``.
    """
    job = models.ForeignKey(Job)
    run_date = models.DateTimeField(auto_now_add=True)
    stdout = models.TextField(blank=True)
    stderr = models.TextField(blank=True)
        
    class Meta:
        ordering = ('-run_date',)
    
    def __unicode__(self):
        return u"%s - %s" % (self.job.name, self.run_date)       
    