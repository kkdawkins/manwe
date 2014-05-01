import logging

from cassandra.models import CassandraService
from observer.deleter import Deleter

logging.basicConfig(format="[%(levelname)s] [%(module)s:%(lineno)d] %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class CassandraServiceDeleter(Deleter):
    model = "CassandraService"

    def __init__(self, **args):
        Deleter.__init__(self, **args)

    def call(self, pk, model_dict):
        print "XXX CassandraServiceDeleter called!"
