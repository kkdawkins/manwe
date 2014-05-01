from core.admin import SingletonAdmin
from django.contrib import admin

from cassandra.models import *

class CassandraServiceAdmin(SingletonAdmin):
    model = CassandraService
    verbose_name = "Cassandra Service"
    verbose_name_plutal = "Cassandra Service"

    fieldsets = [(None, {'fields': ['name', 'enabled', 'description']})]

class CassandraTenantAdmin(SingletonAdmin):
    model = CassandraTenant
    verbose_name = "Cassandra Tenant"
    verbose_name_plural = "Cassandra Tenant"

    fieldsets = [(None, {'fields': ['cassandra_service', 'user', 'comment']})]

admin.site.register(CassandraService, CassandraServiceAdmin)
admin.site.register(CassandraTenant, CassandraTenantAdmin)
