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
)
