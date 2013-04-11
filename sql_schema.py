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

keys = [('deployment', deployments, schema.TEXT_TYPE),
        ('tenant', tenants, schema.INT_TYPE),
        ('routing_key', routing_keys, schema.TEXT_TYPE),
        ('state', states, schema.TEXT_TYPE),
        ('old_state', states, schema.TEXT_TYPE),
        ('task', tasks, schema.TEXT_TYPE),
        ('old_task', tasks, schema.TEXT_TYPE),
        ('image_type', image_types, schema.INT_TYPE),
        ('publisher', publishers, schema.TEXT_TYPE),
        ('service', services, schema.TEXT_TYPE),
        ('host', hosts, schema.TEXT_TYPE),
        ('instance', ["ins-%s" % uuid.uuid4() for x in range(50)], schema.TEXT_TYPE),
        ('request_id', [uuid.uuid4() for x in range(10)], schema.TEXT_TYPE)
       ]


all_unique_names = {}


def get_unique_name(name):
    unique = all_unique_names.get(name)
    if not unique:
        unique = schema.UniqueName(name)
        session.add(unique)
        session.commit()
        all_unique_names[name] = unique
    return unique.id


def save(event, when, traits):

    unique_id = get_unique_name(event)

    raw_data = schema.RawData(unique_id, when)
    session.add(raw_data)
    session.commit()

    for k, v in traits.iteritems():
        v, _type = v
        key_id = get_unique_name(k)
        value_map = {schema.TEXT_TYPE: 't_string',
                     schema.FLOAT_TYPE: 't_float',
                     schema.INT_TYPE: 't_int'}
        values = {'t_string': None, 't_float': None, 't_int': None}
        values[value_map[_type]] = v
        t = schema.Trait(key_id, raw_data, _type, **values)
        session.add(t)
    session.commit()


def create_data():
    schema.reset_db()

    for x in range(10000):
        traits = {}
        event = random.choice(compute_events)
        when = datetime.datetime.utcnow()
        for k, values, _type in keys:
            value = random.choice(values)
            traits[k] = (value, _type)
        print x, save(event, when, traits)


def query_data():
    # Find the min and max date ranges.
    _max, _min = session.query(func.max(schema.RawData.when).label("max_when"), 
                               func.min(schema.RawData.when).label("min_when"))\
                        .all()[0]

    left = _min + datetime.timedelta(minutes = 2)
    right = _max - datetime.timedelta(minutes = 2)

    print  "Range:", _min, _max
    print  "Cut  :", left, right

    # Select all the distinct data ID's within this time range.
    # Note: This query is not run, but rather it's used in sub-selects.
    # This should force all the heavy work to be done on the db.
    requests = session.query(schema.RawData.id)\
                      .filter(schema.RawData.when >= str(left),
                              schema.RawData.when <= str(right))\
                      .distinct()

    # This is a real query to bring back all the UUID's for data created
    # in that time frame.
    uuids = session.query(schema.Trait.t_string)\
                   .filter(schema.Trait.key_id == all_unique_names['instance'].id,
                           schema.Trait.rawdata_id.in_(requests))\
                   .distinct()
    _uuids = list(uuids)
    print "# UUIDS=%d" % len(_uuids)

    uuid = random.choice(_uuids)[0]
    print "UUID:", uuid


    all_data = None
    if False:
        # Subselect approach ... 2.5 sec
        uuid_requests = session.query(schema.Trait.rawdata_id)\
                               .filter(schema.Trait.key_id == all_unique_names['instance'].id,
                                       schema.Trait.t_string == uuid,
                                       schema.Trait.rawdata_id.in_(requests))\
                               .distinct()

        all_data = session.query(schema.Trait)\
                          .filter(schema.Trait.rawdata_id.in_(uuid_requests))\
                          .group_by(schema.Trait.rawdata_id)
    else:
        # Join ... 0.25 sec
        sub_query = session.query(schema.RawData.id)\
                        .join(schema.Trait, schema.Trait.rawdata_id == schema.RawData.id)\
                        .filter(schema.RawData.when >= str(left),
                                schema.RawData.when <= str(right),
                                schema.Trait.key_id == 10,
                                schema.Trait.t_string == 'ins-301fbb11-2669-4be2-8f36-d7dc')\
                        .subquery()

        all_data = session.query(schema.Trait)\
                          .join(sub_query, schema.Trait.rawdata_id == sub_query.c.id)\
                          .order_by(sub_query.c.id)

    print len(list(all_data.all()))
    return 

    last = None
    for t in all_data.all():
        if t.rawdata_id != last:
            print "----- %s -----" % t.raw_data.unique_name
            last = t.rawdata_id
        print t


if __name__ == '__main__':
    #schema.reset_db()
    for uniquename in session.query(schema.UniqueName).all():
        all_unique_names[uniquename.key] = uniquename
    start = datetime.datetime.utcnow()
    #create_data()
    query_data()
    print datetime.datetime.utcnow() - start
