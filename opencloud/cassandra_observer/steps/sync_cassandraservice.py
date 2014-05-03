import logging
import traceback

from cassandra.models import CassandraService
from observer.syncstep import SyncStep

logging.basicConfig(format="[%(levelname)s] [%(module)s:%(lineno)d] %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class SyncCassandraService(SyncStep):
    provides = [CassandraService]
    requested_interval = 0

    def __init__(self, **args):
        SyncStep.__init__(self, **args)

    def fetch_pending(self):
        try:
            ret = CassandraService.objects.filter(Q(enacted__lt=F("updated")) | Q(enacted=None))
            return ret
        except Exception, e:
            traceback.print_exc()
            logger.exception("fetch_pending() failed.")
            return None

    def sync_record(self, cs):
        print "\n\nCassandra sync!\n\n"

        return True
