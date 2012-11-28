# This program attempts a variety of different Shard Key strategies to see what
# gives the best mix of distribution, write speed and queryability.
import random
import uuid as uuidlib

import pymongo

import fixtures


def temporal_key():
    pass


def _event(req, node, event, uuid):
    results = []
    if event[-1] == '*':
        event = event[0:-1]
        for e in ['start', 'end']:
            results.append({'request_id': req, 'node': node,
                            'event': event + e, 'uuid': uuid})
    else:
        results.append({'request_id': req, 'node': node,
                        'event': event, 'uuid': uuid})
    return results


def mk_event(req, nodes, events, uuid):
    return _event(req, random.choice(nodes), random.choice(events), uuid)


def make_action(request_id, instances, collection, shard_key_function):
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

    api = mk_event(request_id, fixtures.api_nodes,
                   ['compute.instance.update'], uuid)
    event_chain.extend(api)

    if is_create:
        scheduler_node = random.choice(fixtures.schedulers)
        for e in fixtures.scheduler_events:
            event_chain.extend(_event(request_id, scheduler_node, e, uuid))

        compute_node = random.choice(fixtures.compute_nodes)
        event_chain.extend(_event(request_id, compute_node,
                                  'compute.instance.create.*', uuid))

        instances[uuid] = compute_node
    else:
        compute_node = instances[uuid]
        is_delete = random.randrange(100) < 10
        if is_delete:
            event_chain.extend(_event(request_id, compute_node,
                                      'compute.instance.delete.*', uuid))
            del instances[uuid]
        else:
            event = random.choice(fixtures.compute_events)
            event_chain.extend(_event(request_id, compute_node, event, uuid))
    return event_chain


if __name__=='__main__':
    connection = pymongo.Connection("sandy-modgod-1", 27017)
    db = connection['scrap']

    # Loosely simulate the RawData table in StackTach
    collection = db['raw']

    # Our first experiment is a purely temporal shard key.
    instances = {}  # { uuid: compute_node }

    request_id = "req_" + str(uuidlib.uuid4())
    action = make_action(request_id, instances, collection, temporal_key)
    for e in action:
        print e
