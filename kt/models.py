from collections import namedtuple
from copy import deepcopy
import io

from kt import constants as C
from kt.client import KT_NONE
from kt.client import QueryBuilder
from kt.client import TokyoTyrant
from kt.client import decode
from kt.client import encode


class TableDatabase(TokyoTyrant):
    def __init__(self, host='127.0.0.1', port=1978, decode_keys=True):
        # Initialize with no value serialization / deserialization -- that will
        # be handled in the model layer.
        super(TableDatabase, self).__init__(host, port, KT_NONE, decode_keys)


Expression = namedtuple('Expression', ('lhs', 'op', 'rhs'))
Ordering = namedtuple('Ordering', ('field', 'value'))


class Field(object):
    _index_type = C.INDEX_STR
    _order_asc = C.ORDER_STR_ASC
    _order_desc = C.ORDER_STR_DESC

    def __init__(self, index=False, default=None):
        self._index = index
        self._default = default
        self.name = None

    def deserialize(self, raw_value):
        return raw_value

    def serialize(self, value):
        return value

    def asc(self):
        return Ordering(self, self._order_asc)

    def desc(self):
        return Ordering(self, self._order_desc)

    def add_to_class(self, model, name):
        self.model = model
        self.name = name
        setattr(model, name, self)

    def __get__(self, instance, instance_type=None):
        if instance is not None:
            return instance.__data__.get(self.name)
        return self

    def __set__(self, instance, value):
        instance.__data__[self.name] = value


def _e(op):
    def inner(self, rhs):
        return Expression(self, op, rhs)
    return inner


class BytesField(Field):
    __eq__ = _e(C.OP_STR_EQ)
    __ne__ = _e(C.OP_STR_EQ | C.OP_NEGATE)
    contains = _e(C.OP_STR_CONTAINS)
    startswith = _e(C.OP_STR_STARTSWITH)
    endswith = _e(C.OP_STR_ENDSWITH)
    contains_all = _e(C.OP_STR_ALL)
    contains_any = _e(C.OP_STR_ANY)
    contains_any_exact = _e(C.OP_STR_ANYEXACT)
    regex = _e(C.OP_STR_REGEX)


class TextField(BytesField):
    def deserialize(self, raw_value):
        return decode(raw_value)

    def serialize(self, value):
        return encode(value)


class IntegerField(Field):
    _index_type = C.INDEX_NUM
    _order_asc = C.ORDER_NUM_ASC
    _order_desc = C.ORDER_NUM_DESC

    __eq__ = _e(C.OP_NUM_EQ)
    __ne__ = _e(C.OP_NUM_EQ | C.OP_NEGATE)
    __gt__ = _e(C.OP_NUM_GT)
    __ge__ = _e(C.OP_NUM_GE)
    __lt__ = _e(C.OP_NUM_LT)
    __le__ = _e(C.OP_NUM_LE)
    between = _e(C.OP_NUM_BETWEEN)
    matches_any = _e(C.OP_NUM_ANYEXACT)

    def deserialize(self, raw_value):
        return int(decode(raw_value))

    def serialize(self, value):
        return encode(str(value))


class FloatField(IntegerField):
    def deserialize(self, raw_value):
        return float(decode(raw_value))


class BaseModel(type):
    def __new__(cls, name, bases, attrs):
        if not bases:
            return super(BaseModel, cls).__new__(cls, name, bases, attrs)

        for base in bases:
            for key, value in base.__dict__.items():
                if key in attrs: continue
                if isinstance(value, Field):
                    attrs[key] = deepcopy(value)

        model_class = super(BaseModel, cls).__new__(cls, name, bases, attrs)
        model_class.__data__ = None

        defaults = {}
        fields = {}
        indexes = []
        for key, value in model_class.__dict__.items():
            if isinstance(value, Field):
                value.add_to_class(model_class, key)
                if value._index:
                    indexes.append(value)
                fields[key] = value
                if value._default is not None:
                    defaults[key] = value._default

        model_class.__defaults__ = defaults
        model_class.__fields__ = fields
        model_class.__indexes__ = indexes
        return model_class

    def __getitem__(self, key):
        if isinstance(key, (list, tuple, set)):
            return self.get_list(key)
        else:
            return self.get(key)

    def __setitem__(self, key, value):
        if isinstance(value, dict):
            value = self(**value)

        if value.key and value.key != key:
            raise ValueError('Data contains key which does not match key used '
                             'for setitem.')

        _, data = serialize_model(value)
        self.__database__.set(key, data)

    def __delitem__(self, key):
        if isinstance(key, (list, tuple, set)):
            self.__database__.remove_bulk(key)
        else:
            self.__database__.remove(key)


def _with_metaclass(meta, base=object):
    return meta("NewBase", (base,), {'__database__': None})


def serialize_model(model):
    buf = io.BytesIO()
    for name, field in model.__fields__.items():
        if name == 'key': continue
        value = getattr(model, name, None)
        if value is not None:
            buf.write(encode(name))
            buf.write(b'\x00')
            buf.write(field.serialize(value))
            buf.write(b'\x00')
    return model.key, buf.getvalue()


def deserialize_into_model(model_class, key, raw_data):
    data = {'key': key}
    items = raw_data.split(b'\x00')
    i, l = 0, len(items) - 1
    while i < l:
        key = decode(items[i])
        value = items[i + 1]
        field = model_class.__fields__.get(key)
        if field is not None:
            data[key] = field.deserialize(value)
        else:
            data[key] = decode(value)
        i += 2
    return model_class(**data)


class Model(_with_metaclass(BaseModel)):
    __database__ = None

    # Key is used to indicate the key in which the model data is stored.
    key = TextField()

    def __init__(self, **kwargs):
        self.__data__ = {}
        self._load_default_dict()
        for key, value in kwargs.items():
            setattr(self, key, value)

    def _load_default_dict(self):
        for field_name, default in self.__defaults__.items():
            if callable(default):
                default = default()
            setattr(self, field_name, default)

    def __getitem__(self, attr):
        return getattr(self, attr)

    def __setitem__(self, attr, value):
        setattr(self, attr, value)

    def __repr__(self):
        return '<%s: %s>' % (type(self).__name__, self.key)

    def save(self):
        if not self.key:
            raise ValueError('Cannot save model without specifying a key.')
        key, data = serialize_model(self)
        return self.__database__.set(key, data)

    def delete(self):
        if not self.key:
            raise ValueError('Cannot delete model without specifying a key.')
        return self.__database__.remove(self.key)

    @classmethod
    def create_indexes(cls, safe=True):
        for field in cls.__indexes__:
            cls.__database__.set_index(field.name, field._index_type, not safe)

    @classmethod
    def drop_indexes(cls):
        for field in cls.__indexes__:
            cls.__database__.delete_index(field.name)

    @classmethod
    def optimize_indexes(cls):
        for field in cls.__indexes__:
            cls.__database__.optimize_index(field.name)

    @classmethod
    def create(cls, key, **data):
        model = cls(key=key, **data)
        model.save()
        return model

    @classmethod
    def get(cls, key):
        data = cls.__database__.get(key)
        if data is None:
            raise KeyError(key)
        return deserialize_into_model(cls, key, data)

    @classmethod
    def get_list(cls, keys):
        data = cls.__database__.get_bulk(keys)
        return [deserialize_into_model(cls, key, data[key])
                for key in keys if key in data]

    @classmethod
    def create_list(cls, models):
        accum = {}
        for model in models:
            key, data = serialize_model(model)
            accum[key] = data
        return cls.__database__.set_bulk(accum)

    @classmethod
    def delete_list(cls, keys):
        return cls.__database__.remove_bulk(keys)

    @classmethod
    def all(cls):
        return cls.get_list(cls.__database__.keys())

    @classmethod
    def query(cls):
        return ModelSearch(cls)

    @classmethod
    def count(cls):
        return cls.query().count()


def clone_query(method):
    def inner(self, *args, **kwargs):
        clone = self.clone()
        method(clone, *args, **kwargs)
        return clone
    return inner


class ModelSearch(object):
    def __init__(self, model):
        self._model = model
        self._conditions = []
        self._order_by = []
        self._limit = self._offset = None

    def clone(self):
        obj = ModelSearch(self._model)
        obj._conditions = list(self._conditions)
        obj._order_by = list(self._order_by)
        obj._limit = self._limit
        obj._offset = self._offset
        return obj

    @clone_query
    def filter(self, *expressions):
        for (field, op, value) in expressions:
            if isinstance(value, (list, set, tuple)):
                items = [field.serialize(item) for item in value]
                value = b','.join(items)
            else:
                value = field.serialize(value)
            self._conditions.append((field.name, op, value))

    @clone_query
    def order_by(self, *ordering):
        for item in ordering:
            if isinstance(item, Field):
                item = item.asc()
            self._order_by.append((item.field.name, item.value))

    @clone_query
    def limit(self, limit=None):
        self._limit = limit

    @clone_query
    def offset(self, offset=None):
        self._offset = offset

    def _build_search(self, operation=None):
        cmd = [('addcond', col, op, val) for col, op, val in self._conditions]
        for col, order in self._order_by:
            cmd.append(('setorder', col, order))
        if self._limit is not None or self._offset is not None:
            cmd.append(('setlimit', self._limit or 1 << 31, self._offset or 0))
        if operation:
            cmd.append(operation)
        return cmd

    def execute(self):
        return self._model.__database__.search(self._build_search())

    def delete(self):
        return self._model.__database__.search(self._build_search(b'out'))

    def count(self):
        return self._model.__database__.search(self._build_search(b'count'))

    def __iter__(self):
        return iter(self.execute())
