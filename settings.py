from django.conf import settings

# The interval in seconds that the processor will 
# check in with the db for new, disabled, or dead jobs.
JOB_REFRESH_TIME = getattr(settings, 'BG_PROCESSOR_REFRESH_JOBS', 60)

#The interval in seconds between updating the log db for a running job
LOG_REFRESH_TIME = getattr(settings, 'BG_PROCESSOR_REFRESH_LOGS', 30)
