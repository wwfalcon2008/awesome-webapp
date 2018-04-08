# test.py

import orm
import asyncio
import logging

from models import User, Blog, Comment


# @asyncio.coroutine
def test():
    yield from orm.create_pool(loop=None, user='root', password='wangwei123', db='awesome')

    u = User(name='Test', email='test@test.com', passwd='123456', image='about:blank')
    yield from u.save()

loop = asyncio.get_event_loop()
for x in test():
    pass