# This program attempts a variety of different Shard Key strategies to see what
# gives the best mix of distribution, write speed and queryability.
import mongodb

import fixtures


def temporal_key():
    pass


def write_records(collection, shard_key_function):
    """Start creating records that look like OpenStack events.

    api [-> scheduler] -> compute node.

    1 in 10 operations will be create_instance (through scheduler)
    8 out of 10 will be something on an existing instance.
    1 in 10 operations will delete an existing instance.

    1 in 10 operations will fail somewhere along the way.
    """


if __name__=='__main__':
    connection = mongodb.Connection("sandy-modgod-1", 27017)
    db = connection['scrap']

    # Loosely simulate the RawData table in StackTach
    collection = db['raw']

    # Our first experiment is a purely temporal shard key.
    write_records(collection, temporal_key)
