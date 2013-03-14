import datetime
import itertools
import random
import sys
import uuid

import schema

from sqlalchemy.sql import func


session = schema.get_session()

deploy = ['ord', 'lon', 'dfw']
cells = ['cell-%d' % (x + 1) for x in xrange(4)]
deployments = ["%s-%s" % (d, c) for d, c in itertools.product(deploy, cells)]

tenants = range(10000, 20000)

states = ['scheduling', 'building', 'active', 'deleting', 'error']
tasks = ['task_%d' % t for t in range(10)]

image_types = [0x1, 0x2]

services = ['api', 'scheduler', 'compute', 'conductor']

hosts = ["%s-%s-%d" % (d, s, n) for d, s, n in
                        itertools.product(deploy, services, range(3))]

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

publishers = ['pub_%d' % x for x in range(10)]
routing_keys = ['route_%d' % x for x in range(10)]

keys = [('deployment', deployments),
        ('tenant', tenants),
        ('routing_key', routing_keys),
        ('state', states),
        ('old_state', states),
        ('task', tasks),
        ('old_task', tasks),
        ('image_type', image_types),
        ('when', None),
        ('publisher', publishers),
        ('event', compute_events),
        ('service', services),
        ('host', hosts),
        ('instance', ["ins-%s" % uuid.uuid4() for x in range(50)]),
        ('request_id', [uuid.uuid4() for x in range(50)])]


def save(traits, data):
    start = datetime.datetime.utcnow()
    d = schema.RawData(data)
    session.add(d)
    session.commit()

    for k, v in traits.iteritems():
        t = schema.Trait(k, v, d)
        session.add(t)
    session.commit()
    return datetime.datetime.utcnow() - start


def create_data():
    schema.reset_db()

    for x in range(10000):
        traits = {}
        for k, values in keys:
            value = None
            if not values:
                value = datetime.datetime.utcnow()
            else:
                value = random.choice(values)
            traits[k] = value
        print x, save(traits, x)


def query_data():
    # Find the min and max date ranges.
    _max, _min = session.query(func.max(schema.Trait.value).label("max_when"), 
                               func.min(schema.Trait.value).label("min_when")) \
                        .filter(schema.Trait.key == 'when').all()[0]

    _max = datetime.datetime.strptime(_max, "%Y-%m-%d %H:%M:%S.%f")
    _min = datetime.datetime.strptime(_min, "%Y-%m-%d %H:%M:%S.%f")
    left = _min + datetime.timedelta(minutes = 2)
    right = _max - datetime.timedelta(minutes = 2)

    print  "Range:", _min, _max
    print  "Cut  :", left, right

    # Select all the distinct data ID's within this time range.
    # Note: This query is not run, but rather it's used in sub-selects.
    # This should force all the heavy work to be done on the db.
    requests = session.query(schema.Trait.rawdata_id)\
                      .filter(schema.Trait.key == 'when',
                              schema.Trait.value >= str(left),
                              schema.Trait.value <= str(right))\
                      .distinct()

    # This is a real query to bring back all the UUID's for data created
    # in that time frame.
    uuids = session.query(schema.Trait.value)\
                   .filter(schema.Trait.key == 'instance',
                           schema.Trait.rawdata_id.in_(requests))\
                   .distinct()

    uuid = random.choice(list(uuids))[0]
    print "UUID:", uuid

    requests2 = session.query(schema.Trait.rawdata_id)\
                      .filter(schema.Trait.key == 'when',
                              schema.Trait.value >= str(left),
                              schema.Trait.value <= str(right))\
                      .distinct()

    uuid_requests = session.query(schema.Trait.rawdata_id)\
                           .filter(schema.Trait.key == 'instance',
                                   schema.Trait.value == uuid,
                                   schema.Trait.rawdata_id.in_(requests2))\
                           .distinct()

    all_data = session.query(schema.Trait)\
                      .filter(schema.Trait.rawdata_id.in_(uuid_requests)).limit(10)

    for r in all_data.all():
        print "Rawdata: ", r


if __name__ == '__main__':
    #create_data()
    query_data()
