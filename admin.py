from django import forms
from django.contrib import admin
from models import Job, JobHost, Log
from socket import gethostbyaddr, gethostname

from django.core.management import get_commands
from django.utils.datastructures import MultiValueDict
import datetime

class LogAdmin(admin.ModelAdmin):
    list_filter = ('job',)

class JobHostAdmin(admin.ModelAdmin):
    #list_display = ('name', 'add_host')
    actions = ['add_this_host']
    
    def add_host(self, obj):
        return obj
    
    def add_this_host(self, request, queryset):
        
        try:
            hostname = gethostbyaddr(gethostname())[0]
        except gaierror:
            hostname = 'localhost'
        host, created = JobHost.objects.get_or_create(name=hostname)
        if created:
            self.message_user(request, "%s was successfully added"%hostname)
        else:
            self.message_user(request, "%s already exists"%hostname)
        
    add_this_host.short_description = "Add this Host"
    
class JobAdmin(admin.ModelAdmin):
    list_display = ('name','pid','disabled', 'log_duration', 'command', 'args', 
                    'started', 'is_running', 'subscribers', 'view_logs_button')
    exclude = ('pid',)
    list_filter = ('hosts',)
    
    def log_duration(self, obj):
        return str(datetime.timedelta(seconds=obj.log_life))
    
    def formfield_for_dbfield(self, db_field, **kwargs):
        request = kwargs.pop("request", None)
        
        # Add a select field of available commands
        if db_field.name == 'command':
            choices_dict = MultiValueDict()
            for command, app in get_commands().items():
                choices_dict.appendlist(app, command)
            
            choices = []
            for key in choices_dict.keys():
                #if str(key).startswith('<'):
                #    key = str(key)
                commands = choices_dict.getlist(key)
                commands.sort()
                choices.append([key, [[c,c] for c in commands]])
                
            kwargs['widget'] = forms.widgets.Select(choices=choices)
            return db_field.formfield(**kwargs)
        
        kwargs['request'] = request
        return super(JobAdmin, self).formfield_for_dbfield(db_field, **kwargs)
    
    def view_logs_button(self, obj):
        on_click = "window.location='../log/?job=%d';" % obj.id
        return '<input type="button" onclick="%s" value="View Logs" />' % on_click
    view_logs_button.allow_tags = True
    view_logs_button.short_description = 'Logs'


admin.site.register(Log, LogAdmin)
admin.site.register(Job, JobAdmin)    
admin.site.register(JobHost, JobHostAdmin) 