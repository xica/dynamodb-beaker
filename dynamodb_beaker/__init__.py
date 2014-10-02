from beaker.container import OpenResourceNamespaceManager, Container
from beaker.exceptions import InvalidCacheBackendError, MissingCacheParameter
from beaker.synchronization import null_synchronizer

ddb = None
ItemNotFound = None


class DynamoDBNamespaceManager(OpenResourceNamespaceManager):

    @classmethod
    def _init_dependencies(cls):
        global ddb, ItemNotFound
        if ddb is not None:
            return
        try:
            import boto.dynamodb2 as ddb
            from boto.dynamodb2.exceptions import ItemNotFound
        except ImportError as e:
            raise InvalidCacheBackendError('DynamoDB cache backend requires boto.')

    def __init__(self, namespace, table_name, region=None, hash_key='id', **params):

        OpenResourceNamespaceManager.__init__(self, namespace)

        if table_name is None:
            raise MissingCacheParameter('DynamoDB table name required.')

        self._hash_key = hash_key
        self._table = ddb.table.Table(table_name, connection=ddb.connect_to_region(region))
        self._flags = None
        self._item = None

    @property
    def _key(self):
        return {self._hash_key: self.namespace}

    def get_creation_lock(self, key):
        return null_synchronizer()

    def get_access_lock(self):
        return null_synchronizer()

    def do_open(self, flags, replace):

        self._flags = flags

        if self._item is not None:
            return

        try:
            self._item = self._table.get_item(**self._key)
        except ItemNotFound:
            self._item = ddb.items.Item(self._table, data=self._key)

    def do_close(self):

        if self._flags == 'c' or self._flags == 'w':
            self._item.partial_save()

        self._flags = None
        self._item = None

    def do_remove(self):
        self._table.delete_item(**self._key)

    def __getitem__(self, key):

        if key in self._item:
            return self._item[key]

        return self._item._data if key == 'session' and len(self._item._data) > 1 else None

    def __setitem__(self, key, value):
        self.set_value(key, value)

    def set_value(self, key, value, expiretime=None):

        if key == 'session' and isinstance(value, dict):
            self._item._data.update(value)
        else:
            self._item[key] = value

    def __contains__(self, key):
        return True if key == 'session' else key in self._item

    def __delitem__(self, key):
        if key in self._item:
            del self._item[key]
        elif key == 'session':
            for k in self._item.keys():
                if k != self._hash_key:
                    del self._item[k]

    def keys(self):
        return self._item.keys()


class DynamoDBContainer(Container):
    namespace_class = DynamoDBNamespaceManager
