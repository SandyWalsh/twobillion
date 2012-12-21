import datetime

import pymongo


if __name__=='__main__':
    connection = pymongo.MongoClient("sandy-mongos-1", 27017)
    db = connection['scrap']
    collection = db['raw']

    start = datetime.datetime.utcnow()
    _min = collection.find().sort('when', 1).limit(1)[0]
    _max = collection.find().sort('when', -1).limit(1)[0]
    print "Min/Max When:", _min['when'], _max['when']
    last = datetime.datetime.utcnow()
    print "Time:", last - start

    start = last

    from_time = _min['when']
    uuids = None
    while from_time < _max['when']:
        to_time = from_time + datetime.timedelta(hours=1)

        print "Fetching", from_time, "to", to_time
        count = collection.find({"when": {"$lt": to_time, "$gt": from_time}}
                                ).count()
        uuids = collection.find({"when": {"$lt": to_time, "$gt": from_time}}
                                ).distinct('uuid')
        print "# Events", count, len(list(uuids))

        last = datetime.datetime.utcnow()
        print "Time:", last - start
        start = last
        from_time = to_time
        break

    for uuid in uuids:
        print "UUID", uuid
        for event in collection.find({"uuid": uuid}).sort('when'):
            print event['when'], event['event']
