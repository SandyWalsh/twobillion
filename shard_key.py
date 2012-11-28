# This program attempts a variety of different Shard Key strategies to see what
# gives the best mix of distribution, write speed and queryability.
import datetime
import heapq
import random
import uuid as uuidlib

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


def make_action(now, base, instances, collection):
    """Start creating records that look like OpenStack events.

    api [-> scheduler] -> compute node.

    The first N operations will be create_instance, after that it's a
    1 in 10 chance to be create_instance (through scheduler)
    Otherwise it will be something on an existing instance where
    1 in 10 operations will delete an existing instance.

    In general, 1 in 10 operations will fail somewhere along the way.
    """
    event_chain = []

    is_create = True
    if len(instances) > 10:
        is_create = random.randrange(100) < 10

    uuid = str(uuidlib.uuid4())
    if not is_create:
        uuid = random.choice(instances.keys())

    nbase = {'uuid': uuid}
    nbase.update(base)

    api = mk_event(now, nbase, fixtures.api_nodes, ['compute.instance.update'])
    event_chain.extend(api)

    if is_create:
        now = bump_time(now, 0.5, 3.0)  # From api to scheduler
        scheduler_node = random.choice(fixtures.schedulers)
        for e in fixtures.scheduler_events:
            event_chain.extend(_event(now, nbase, scheduler_node, e))
            now = bump_time(now, 0.1, 0.5)  # inside scheduler

        compute_node = random.choice(fixtures.compute_nodes)
        now = bump_time(now, 0.5, 3.0)  # In Compute node
        event_chain.extend(_event(now, nbase, compute_node,
                                  'compute.instance.create.*'))

        instances[uuid] = compute_node
    else:
        compute_node = instances[uuid]
        is_delete = random.randrange(100) < 10
        if is_delete:
            event_chain.extend(_event(now, nbase, compute_node,
                                      'compute.instance.delete.*', uuid))
            del instances[uuid]
        else:
            event = random.choice(fixtures.compute_events)
            event_chain.extend(_event(now, nbase, compute_node, event, uuid))
    return event_chain


def get_action(now):
    request_id = "req_" + str(uuidlib.uuid4())
    base = {'request_id': request_id}
    action = make_action(now, base, instances, collection)
    return action


if __name__=='__main__':
    connection = pymongo.Connection("sandy-modgod-1", 27017)
    db = connection['scrap']

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
    # It's our reponsability to fire off the events when sufficient "time"
    # has passed.
    #
    # We don't operate in real-time. Time jumps ahead to the next task to
    # be performed (so we don't have to do sleep()'s in this code).
    #
    # The thing we don't want to have to deal with is overlapping commands
    # (like instance.delete starting while instance.create is still underway)
    # That's too much headache.
    now = datetime.datetime.utcnow()
    actions = []  # active actions
    next_events = []  # priority queue
    instances_in_use = set()

    action = get_action(now)
    ends_at = None
    for idx, event in enumerate(action):
        ends_at = event['when']
        heapq.heappush(next_events, (ends_at, event, idx==0, idx==len(action)-1))

    while next_events:
        when, event, start, end = heapq.heappop(next_events)
        if start or end:
            uuid = event['uuid']
            if start:
                instances_in_use.add(uuid)
            else:
                instances_in_use.remove(uuid)
        print when, event['event'], start, end
