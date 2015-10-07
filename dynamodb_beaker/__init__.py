import types

from beaker.container import OpenResourceNamespaceManager, Container
from beaker.exceptions import InvalidCacheBackendError, MissingCacheParameter
from beaker.synchronization import null_synchronizer
from beaker.util import verify_rules

ddb = None
ItemNotFound = None


class DynamoDBNamespaceManager(OpenResourceNamespaceManager):

    _supported_options = [
        'host', 'aws_access_key_id', 'aws_secret_access_key', 'security_token',
        'is_secure', 'https_connection_factory', 'proxy', 'proxy_port',
        'proxy_user', 'proxy_pass', 'port', 'validate_certs', 'profile_name',
        'debug', 'path']

    _rules = [
        ('host', (str, types.NoneType), "host must be a string"),
        ('aws_access_key_id', (str, types.NoneType), "aws_access_key_id must be a "
         "string"),
        ('aws_secret_access_key', (str, types.NoneType), "aws_secret_access_key "
         "must be a string"),
        ('security_token', (str, types.NoneType), "security_token must be a "
         "string"),
        ('is_secure', (bool, types.NoneType), "is_secure must be true/false"),
        ('https_connection_factory', (list, tuple, types.NoneType),
         "https_connection_factory must comma seperated list of valid factories"),
        ('proxy', (str, types.NoneType), "proxy must be a string"),
        ('proxy_port', (int, types.NoneType), "proxy_port must be an integer"),
        ('proxy_user', (str, types.NoneType), "proxy_user must be a string"),
        ('proxy_pass', (str, types.NoneType), "proxy_pass must be a string"),
        ('port', (int, types.NoneType), "port must be an integer"),
        ('validate_certs', (bool, types.NoneType), "validate_certs must be "
         "true/false"),
        ('profile_name', (str, types.NoneType), "profile_name must be a string"),
        ('debug', (int, types.NoneType), "debug must be an integer"),
        ('path', (str, types.NoneType), "path must be a string"),
    ]

    @classmethod
    def _init_dependencies(cls):
        global ddb, ItemNotFound, ConditionalCheckFailedException
        if ddb is not None:
            return
        try:
            import boto.dynamodb2 as ddb
            import boto.dynamodb2.table  # necessary for calling ddb.table
            from boto.dynamodb2.exceptions import ItemNotFound, ConditionalCheckFailedException
        except ImportError as e:
            raise InvalidCacheBackendError('DynamoDB cache backend requires boto.')

    def __init__(self, namespace, table_name, region=None, hash_key='id', **params):

        OpenResourceNamespaceManager.__init__(self, namespace)

        if table_name is None:
            raise MissingCacheParameter('DynamoDB table name required.')

        options = verify_rules(
            dict([(k, v) for k, v in params.iteritems()
                  if k in self._supported_options]),
            self._rules)

        self._hash_key = hash_key
        self._table = ddb.table.Table(
            table_name, connection=ddb.connect_to_region(region, **options))
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

        data, fields = self._item.prepare_partial()

        if self._flags == 'c' or self._flags == 'w':
            try:
                self._item.partial_save()
            except ConditionalCheckFailedException as e:
                # Since `partial_save()` performs a conditional write, this operation
                # may fail due to concurrent requests. Here, we return gracefully if
                # the session data was NOT actually modified.
                if fields != {u'_accessed_time'}:
                    raise

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
