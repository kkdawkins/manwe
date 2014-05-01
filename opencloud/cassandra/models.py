from core.models import User, Service, SingletonModel
from django.db import models

class CassandraService(SingletonModel, Service):
    """Cassandra multi-tenant service for OpenCloud"""

    class Meta:
        app_label = "cassandra"
        verbose_name = "Cassandra Service"

    def __unicode__(self):
        return u"%s" % self.name

class CassandraTenant(models.Model):

    cassandra_service = models.ForeignKey(CassandraService)
    user = models.ForeignKey(User)
    comment = models.CharField(blank=True, help_text="Optional comment for tenant", max_length=256)

    def __unicode__(self):
        return self.cassandra_service.name + " - " + self.user.email
