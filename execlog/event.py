from collections import namedtuple


Event = namedtuple(
    'Event',
    ['endpoint', 'name', 'action'],
    defaults=[None, None, None],
)

FileEvent = namedtuple(
    'FileEvent',
    ['endpoint', 'name', 'action'],
    defaults=[None, None, None],
    # action is 32bit flag mask
)
