# orm.py
# -*- coding: utf-8 -*-
# day3
# methods need to be modified
# https://github.com/michaelliao/awesome-python3-webapp/blob/day-03/www/orm.py


r''' choose MySQL as database.
     encapsulate SELECT INSERT UPDATE and DELETE with functions.
     Because Web Frame used aiohttp which is based on asyncio( a
     asynchronous model based on coroutine, normal synchronous IO
     operations cannot be called in this app). Async programming
     principle: once async, always async.
     aiomysql provides async io driver for MySQL database.
'''


# findAll, findNumber, update, remove..

import asyncio
import aiomysql
import logging  # for log
# from orm import Model, StringField, IntegerField    # for ORM


def log(sql, args=()):
    logging.info('SQL: %s' % sql)


# Create connection pool
# Create a global connetion pool. Every HTTP request can get direct
# connect to the database from this pool. No need to open and close
# database connection.
# Connection pool stored in global variable __pool, encoded in utf-8.
@asyncio.coroutine
def create_pool(loop, **kw):
    logging.info('create database connection pool...')
    global __pool   # global variable for storing connection pool
    __pool = yield from aiomysql.create_pool(
        host=kw.get('host', 'localhost'),
        port=kw.get('port', 3306),  # default port for MySQL
        user=kw['user'],
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset', 'utf-8'),
        autocommit=kw.get('autocommit', True),
        maxsize=kw.get('maxsize', 10),
        minsize=kw.get('minsize', 1),
        loop=loop
    )


# select funtion for SELECT in MySQL
# Always use SQL with arguments for injection defending
@asyncio.coroutine
def select(sql, args, size=None):
    log(sql, args)
    global __pool
    with (yield from __pool) as conn:
        cur = yield from conn.cursor(aiomysql.DictCursor)
        # placeholder in SQL is '?', in MySQL is '%s'
        yield from cur.execute(sql.replace('?', '%s'), args or ())
        if size:
            rs = yield from cur.fetchmany(size)  # call coroutine by yield from
        else:
            rs = yield from cur.fetchall()  # call coroutine by yield from
        yield from cur.close()
        logging.info('rows returned: %s' % len(rs))
        return rs


# INSERT UPDATE and DELETE combined into a common execute() function
# cause these 3 SQL needs same arguments and all returns a integer
# of rows influenced.
@asyncio.coroutine
def execute(sql, args):
    log(sql)
    with (yield from __pool) as conn:
        try:
            cur = yield from conn.cursor()
            yield from cur.execute(sql.replace('?', '%s'), args)
            affected = cur.rowcount
            yield from cur.close()
        except BaseException as e:
            raise
        return affected


# create_args_stirng(num)
def create_args_stirng(num):
    L = []
    for n in range(num):
        L.append('?')
    return ','.join(L)


# define an User object for data table users
class User(Model):
    # class property(not instance property)
    __table__ = 'users'

    id = IntegerField(primary_key=True)
    name = StringField()


# define Model for all ORM mapping
# super dict supports model[key]
class Model(dict, metaclass=ModelMetaclass):

    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    # __getattr__ supports model.key
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

    def getValue(self, key):
        return getattr(self, key, None)

    def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' %
                              (key, str(value)))
                setattr(self, key, value)
        return value

    # class method: all child class can use this method
    # user = yield from User.find('123')
    @classmethod
    @asyncio.coroutine
    def find(cls, pk):
        'find object by primarykey'
        rs = yield from select('%s where %s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])

    # findAll()
    @classmethod
    async def findAll(cls, where=None, args=None, **kw):
        sql = [cls.__select__]
        if where:
            sql.append('where')
            sql.append(where)
        if args is None:
            args = []
        # order by orderByValue, if orderByValue!=None
        orderBy = kw.get('orderBy', None)
        if orderBy is not None:
            sql.append('order by')
            sql.append(orderBy)
        # limit limitValue, if limitValue!=None
        limit = kw.get('limit', None)
        if limit is not None:
            sql.append('limit')
            if isinstance(limit, int):  # limited by int
                sql.append('?')
                args.append(limit)
            elif isinstance(limit, tuple) and len(limit) == 2:    # limited by tuple
                sql.append('?, ?')
                args.extend(limit)
            else:
                raise ValueError('Invalid limit value: %s' % str(limit))

        rs = await select(' '.join(sql), args)
        return [cls(**r) for r in rs]

    # findNumber()
    @classmethod
    async def findNumber(cls, selectField, where=None, args=None):
        sql = ['select %s _num_ from `%s`' % (selectField, cls.__table__)]
        if where is not None:
            sql.append('where')
            sql.append(where)

        rs = await select(' '.join(sql), args, 1)
        if len(rs) == 0:
            return None
        return rs[0]['_num_']

    # update()
    async def update(self):
        args = list(map(self.getValue, self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        rows = await execute(self.__update__, args)
        if rows != 1:
            logging.warn(
                'failed to update by primary key: affected rows: %s' % rows)

    # remove()
    async def remove(self):
        args = [self.getValue(self.__primary_key__)]
        rows = await execute(self.__delete__, args)
        if rows != 1:
            logging.warn(
                'failed to remove by primary key: affected rows: %s' % rows)

    # save method, save instance to database
    # user = User(id=123, name='Michael')
    # yield from user.save()
    # user.save() only create a coroutine
    # only "yield from user.save()" will execute it
    @asyncio.coroutine
    def save(self):
        args = list(map(self.getValueOrDefault, self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = yield from execute(self.__insert__, args)
        if rows != 1:
            logging.warn('failed to insert recode: affected rows: %s' % rows)


class Field(object):
    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    def __str__(self):
        return '<%s, %s: %s>' % (self.__class__.__name__, self.column_type, self.name)


# No Boolean type in MySQL, use tinyint(1) instead
# #tinyint(1)=Boolean: 1=True, 2=False
class BooleanField(Field):
    def __init__(self, name=None, default=False):
        super().__init__(name, 'boolean', False, default)

# IntegerField: how many bits for integer?


class IntegerField(Field):
    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'bigint', primary_key, default)


# FloatFiled
class FloatField(Field):
    def __init__(self, name=None, primary_key=False, default=0.0):
        super().__init__(name, 'real', primary_key, default)


# TextField
#
class TextField(Field):
    def __init__(self, name=None, default=None):
        super.__init__(name, 'text', False, default)


# StringField for varchar
class StringField(Field):
    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        super().__init__(name, ddl, primary_key, default)

# In object-oriented programming, a metaclass is a class whose instances are
# classes. Just as an ordinary class defines the behavior of certain objects,
# a metaclass defines the behavior of certain classes and their instances. Not
# all object-oriented programming languages support metaclasses. Among those
# that do, the extent to which metaclasses can override any given aspect of
# class behavior varies.


class ModelMetaclass(type):
    def __new__(cls, name, bases, attrs):
        # exclude Model Class itself
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)
        # get table name
        tableName = attrs.get('__table__', None) or name
        logging.info('found model: %s (table: %s' % (name, tableName))
        # get all Field and primary_key names
        mappings = dict()
        fields = []
        primaryKey = None
        for k, v in attrs.items():
            if isinstance(v, Field):
                logging.info('  found mapping: %s ==> %s' % (k, v))
                mappings[k] = v
                if v.primary_key:
                    # find primarykey
                    if primaryKey:
                        raise RuntimeError(
                            'Duplicate primary key for field: %s' % k)
                    primaryKey = k
                else:
                    fields.append(k)
        if not primaryKey:
            raise RuntimeError('PrimaryKey not found.')
        for k in mappings.keys():
            attrs.pop(k)
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))
        # mappings of attributes and columns
        attrs['__mappings__'] = mappings
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primaryKey   # attr of primarykey
        attrs['__fields__'] = fields    # attrs except primarykey
        # construct default SELECT INSERT UPDATE and DELETE sentences:
        attr['__select__'] = 'select `%s`, %s from `%s`' % (
            primaryKey, ','.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ','.join(
            escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ','.join(
            map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (
            tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)
