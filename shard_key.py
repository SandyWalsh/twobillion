# This program attempts a variety of different Shard Key strategies to see what
# gives the best mix of distribution, write speed and queryability.
import datetime
import heapq
import random
import uuid as uuidlib
import sys
import time

import pymongo

import fixtures


def pkg(*args):
    new = {}
    for a in args:
        new.update(a)
    return new


def bump_time(now, low, high):
    """Create a random time in fractional seconds and move now ahead
    that amount."""
    secs = low + ((high - low) * random.random())
    return now + datetime.timedelta(seconds=secs)


def _event(now, base, node, event):
    results = []
    if event[-1] == '*':
        event = event[0:-1]
        extra = {'when': now, 'node': node}
        results.append(pkg(base, extra, {'event': event + "start"}))
        now = bump_time(now, 0.25, 60.0 * 15.0)  # In compute node
        extra = {'when': now, 'node': node}
        results.append(pkg(base, extra, {'event': event + "end"}))
    else:
        extra = {'when': now, 'node': node}
        results.append(pkg(base, extra, {'event': event}))
    return results


def mk_event(now, base, nodes, events):
    return _event(now, base, random.choice(nodes), random.choice(events))


def make_action(now, base, instances_in_use, instances, collection):
    """Start creating records that look like OpenStack events.

    api [-> scheduler] -> compute node.

    The first N operations will be create_instance, after that it's a
    1 in 10 chance to be create_instance (through scheduler)
    Otherwise it will be something on an existing instance where
    1 in 10 operations will delete an existing instance.

    In general, 1 in 10 operations will fail somewhere along the way.

    instances_in_use is different than instances.keys():
    instances.keys() is the list of all instances, even instances that
    don't exist yet, but will be created in the near future.
    instance_in_use are the instances in the current timeline.
    """
    event_chain = []

    is_create = False
    is_delete = False
    is_update = False

    uuid = str(uuidlib.uuid4())
    compute_node = random.choice(fixtures.compute_nodes)

    if len(instances) > 10:
        is_create = random.randrange(100) < 10

    if not is_create and not instances_in_use:
        is_create = True

    if not is_create:
        uuid = random.choice(list(instances_in_use))
        compute_node = instances[uuid]
        is_delete = random.randrange(100) < 10
        if not is_delete:
            is_update = True

    if not (is_create or is_delete or is_update):
        return []

    nbase = {'uuid': uuid, 'is_create': is_create, 'is_delete': is_delete,
             'is_update': is_update}
    nbase.update(base)

    # All operations start with an API call ...
    api = mk_event(now, nbase, fixtures.api_nodes, ['compute.instance.update'])
    event_chain.extend(api)

    if is_create:
        now = bump_time(now, 0.5, 3.0)  # From api to scheduler
        scheduler_node = random.choice(fixtures.schedulers)
        for e in fixtures.scheduler_events:
            event_chain.extend(_event(now, nbase, scheduler_node, e))
            now = bump_time(now, 0.1, 0.5)  # inside scheduler

        now = bump_time(now, 0.5, 3.0)  # In Compute node
        event_chain.extend(_event(now, nbase, compute_node,
                                  'compute.instance.create.*'))
        instances[uuid] = compute_node

    if is_delete:
        if is_delete:
            event_chain.extend(_event(now, nbase, compute_node,
                                      'compute.instance.delete.*'))
            del instances[uuid]

    if is_update:
        event = random.choice(fixtures.compute_events)
        event_chain.extend(_event(now, nbase, compute_node, event))

    return event_chain


def get_action(instances_in_use, instances, now):
    request_id = "req_" + str(uuidlib.uuid4())
    base = {'request_id': request_id}
    return make_action(now, base, instances_in_use, instances, collection)


if __name__=='__main__':
    connection = pymongo.MongoClient("sandy-mongos-1", 27017)
    db = connection['scrap']
    db.drop_collection('raw')

    # Loosely simulate the RawData table in StackTach
    collection = db['raw']

    # Our first experiment is a purely temporal shard key.
    instances = {}  # { uuid: compute_node }

    # Many actions can be performed concurrently.
    # We might start working on instance #1 and then, while that
    # effort is still underway, start doing something with instances
    # #2, 3 and 4. They need to interleave each other.
    #
    # An "action", below, is a list of each of the steps necessary
    # to perform that operation, but with a time component relative to
    # the starting time passed in.
    # It's our responsibility to fire off the events when sufficient "time"
    # has passed.
    #
    # The thing we don't want to have to deal with is overlapping commands
    # (like instance.delete starting while instance.create is still underway)
    # That's too much headache.

    operations_per_minute = 60
    operations_per_second = float(operations_per_minute) / 60.0
    millisecond_per_tick = 1000.0 / float(operations_per_second)
    next_events = []  # priority queue
    instances_in_use = set()

    now = datetime.datetime.utcnow()
    tick = now + datetime.timedelta(milliseconds=millisecond_per_tick)
    while True:
        if now >= tick:
            action = get_action(instances_in_use, instances, now)
            for idx, event in enumerate(action):
                when = event['when']
                heapq.heappush(next_events,
                                    (when, event, idx==0, idx==len(action)-1))
            tick = now + datetime.timedelta(milliseconds=millisecond_per_tick)

        if next_events:
            when, event, start, end = next_events[0]  # peek
            while when < now:
                when, event, start, end = heapq.heappop(next_events)
                uuid = event['uuid']
                if end:
                    if event['is_create']:
                        instances_in_use.add(uuid)
                    elif event['is_delete']:
                        instances_in_use.remove(uuid)
                print when, event['event'], uuid

                #collection.insert(event)

        time.sleep(.1)
        now = datetime.datetime.utcnow()

    if False:
        read_start = datetime.datetime.utcnow()
        c = 0
        for r in collection.find().sort("when"):
            # print r['when'], r['event'], r['node']
            x = r['when']
            y = r['event']
            z = r['node']
            c += 1
        read_end = datetime.datetime.utcnow()

        print c, write_end - write_start, read_end - read_start
