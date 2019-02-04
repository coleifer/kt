class Queue(object):
    """
    Helper-class for working with the Kyoto Tycoon Lua queue functions.
    """
    def __init__(self, client, key, db=None):
        self._client = client
        self._key = key
        self._db = client._default_db if db is None else db

    def _lua(self, fn, **kwargs):
        kwargs.update(queue=self._key, db=self._db)
        return self._client.script(fn, kwargs)

    def add(self, item):
        return int(self._lua('queue_add', data=item)['id'])

    def extend(self, items):
        args = {str(i): item for i, item in enumerate(items)}
        return int(self._lua('queue_madd', **args)['num'])

    def _item_list(self, fn, n=1):
        items = self._lua(fn, n=n)
        if n == 1:
            return items['0'] if items else None

        accum = []
        if items:
            for key in sorted(items, key=int):
                accum.append(items[key])
        return accum

    def pop(self, n=1):
        return self._item_list('queue_pop', n)
    def rpop(self, n=1):
        return self._item_list('queue_rpop', n)

    def peek(self, n=1):
        return self._item_list('queue_peek', n)
    def rpeek(self, n=1):
        return self._item_list('queue_rpeek', n)

    def count(self):
        return int(self._lua('queue_size')['num'])
    __len__ = count

    def remove(self, data, n=None):
        if n is None:
            n = -1
        return int(self._lua('queue_remove', data=data, n=n)['num'])
    def rremove(self, data, n=None):
        if n is None:
            n = -1
        return int(self._lua('queue_rremove', data=data, n=n)['num'])

    def clear(self):
        return int(self._lua('queue_clear')['num'])
