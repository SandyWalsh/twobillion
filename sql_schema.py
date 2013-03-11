import itertools

import sqlalchemy
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

engine = sqlalchemy.create_engine('mysql://root:password@localhost/cm_meta', 
                                  use_ansiquotes=True, echo=True)

Base = declarative_base()


class Meta(Base):
    __tablename__ = 'metadata'

    id = Column(Integer, primary_key=True)
    key = Column(String(32), index=True)
    value = Column(String(32), index=True)

    def __init__(self, key, value):
        self.key = key
        self.value = value

    def __repr__(self):
        return "<Meta('%s','%s')>" % (self.key, self.value)


Base.metadata.create_all(engine) 
Session = sessionmaker(bind=engine)
session = Session()

keys = ['deployment', 'tenant', 'routing_key', 'state', 'old_state', 'task',
        'old_task', 'image_type', 'when', 'publisher', 'event', 'service', 
        'host', 'instance', 'request_id']

deploy = ['ord', 'lon', 'dfw']
cells = ['cell-%d' % (x + 1) for x in xrange(4)]
deployments = ["%s-%s" % (d, c) for d, c in itertools.product(deploy, cells)]

tenants = range(10000, 20000)

states = ['scheduling', 'building', 'active', 'deleting', 'error']
tasks = ['task_%d' % t for t in range(10)]

image_type = [0x1, 0x2]

service = ['api', 'scheduler', 'compute', 'conductor']

hosts = ["%s-%s-%d" % (d, s, n) for d, s, n in
                        itertools.product(deploy, service, range(3))]

compute_events = [
    'compute.instance.finish_resize.*',
    'compute.instance.power_off.*',
    'compute.instance.power_on.*',
    'compute.instance.reboot.*',
    'compute.instance.rebuild.*',
    'compute.instance.resize.confirm.*',
    'compute.instance.resize.prep.*',
    'compute.instance.resize.revert.*',
    'compute.instance.resize.*',
    'compute.instance.shutdown.*',
    'compute.instance.snapshot.*',
    'compute.instance.suspend',
    'compute.instance.resume',
    'compute.instance.exists',
    'compute.instance.update',
    'attach_volume',
    'change_instance_metadata',
    'detach_volume',
    'finish_resize',
    'finish_revert_resize',
    'get_vnc_console',
    'power_on_instance',
    'prep_resize',
    'reboot_instance',
    'rebuild_instance',
    'rescue_instance',
    'reserve_block_device_name',
    'resize_instance',
    'revert_resize',
    'run_instance',
    'set_admin_password',
    'snapshot_instance',
    'start_instance',
    'suspend_instance',
    'terminate_instance',
    'unrescue_instance']

#m = Meta("my_key", "my_value")
#session.add(m)
#session.commit()


