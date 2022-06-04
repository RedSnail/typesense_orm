from pydantic import BaseModel, Field, AnyHttpUrl
from typing import Sequence, Optional, Callable, TypeVar, Awaitable, Dict, Any, Type, Generic, Union
import aiohttp
import asyncio
from asyncio import Task, gather, all_tasks
from datetime import datetime, timedelta
from .logging import logger
from .exceptions import NoHealthyNode, ApiResponseNotOk
from functools import wraps
from abc import ABC, abstractmethod, abstractproperty, abstractclassmethod, ABCMeta
from pydantic.main import ModelMetaclass
from gc import get_referrers
import inspect
import dis
import traceback
from itertools import islice
from dis import dis
from gc import get_referrers


class Node(BaseModel):
    url: AnyHttpUrl
    last_checked: datetime = Field(datetime(1970, 1, 1))


Cl = TypeVar("Cl", bound="ApiCaller")
T = TypeVar("T")
V = TypeVar("V")


def wrap_task(func):
    def wrapper(self: Cl, *args, schedule=True, name=None, **kwargs):
        task = self.loop.create_task(func(self, *args, **kwargs), name=name)
        if schedule:
            logger.debug("-------------------------------")
            logger.debug("task scheduled")
            self.tasks.append(task)

        return task

    return wrapper


def retry(do_after_retries: Callable[[Cl], None]):
    def decorator(func):
        @wraps(func)
        async def wrapper(self: Cl, *args, **kwargs):
            while True:
                for _ in range(self.num_retries):
                    try:
                        logger.info("trying")
                        res = func(self, *args, **kwargs)
                        if inspect.iscoroutine(res):

                            return await res
                        else:
                            return res
                    except aiohttp.ClientConnectionError:
                        logger.debug("excepted connection")

                    await asyncio.sleep(self.retry_interval.seconds)

                do_after_retries(self)

        return wrapper

    return decorator


class MethodAssigner(type):
    def __new__(mcs, *args, **kwargs):
        ret: Type[ApiCaller] = super().__new__(mcs, *args, **kwargs)
        sync = ret.sync()
        ret.get = request_factory(aiohttp.ClientSession.get, sync)
        ret.post = request_factory(aiohttp.ClientSession.post, sync)
        ret.put = request_factory(aiohttp.ClientSession.put, sync)
        ret.delete = request_factory(aiohttp.ClientSession.delete, sync)
        ret.__abstractmethods__ = frozenset(ret.__abstractmethods__ - {"delete", "post", "put", "get"})
        if sync:
            ret.ret_type = Dict[str, Any]
        else:
            ret.ret_type = Awaitable[T]
        return ret


class ApiCallerMetaclass(MethodAssigner, ModelMetaclass, Generic[V]):
    pass


def no_healthy_node(self: Cl):
    raise NoHealthyNode(f"No node has responded after {self.num_retries} retries")


def nearest_node_unhealthy(self: Cl):
    logger.info("current node is unhealthy")
    self.nearest_node = None
    self.loop.run_until_complete(self.session.close())
    self.loop.run_until_complete(self.setup_session())


def request_factory(method: Callable[..., Awaitable[aiohttp.ClientResponse]], sync: bool):
    async def do_async(self: Cl, url, handler: Callable[[Dict[str, Any]], T] = lambda a: a, name=None, **kwargs):
        async with await method(self.session, url, **kwargs) as r:
            json = await r.json()
            if r.status < 200 or r.status >= 300:
                raise ApiResponseNotOk(json, r.status)
            return handler(json)

    @wraps(method)
    @wrap_task
    @retry(nearest_node_unhealthy)
    async def make_request(self: Cl, url,
                           handler: Callable[[Dict[str, Any]], T] = lambda a: a,
                           **kwargs) \
            -> Awaitable[T]:
        async with await method(self.session, url, **kwargs) as r:
            json = await r.json()
            if r.status < 200 or r.status >= 300:
                raise ApiResponseNotOk(json, r.status)
            return handler(json)

    if sync:
        @wraps(make_request)
        def make_request_sync(self: Cl, url,
                              handler: Callable[[Dict[str, Any]], T] = lambda a: a,
                              schedule=False,
                              name=None,
                              **kwargs) -> T:
            return self.loop.run_until_complete(make_request(self, url,
                                                             handler=handler,
                                                             schedule=schedule,
                                                             name=name, **kwargs))

        return make_request_sync

    if not sync:
        logger.debug(inspect.iscoroutinefunction(make_request))
    return make_request


API_KEY_HEADER_NAME = 'X-TYPESENSE-API-KEY'


class ApiCaller(ABC, BaseModel):
    api_key: str
    nodes: Sequence[Node]
    connection_timeout: timedelta = Field(timedelta(seconds=3))
    num_retries: int = Field(3)
    retry_interval: timedelta = Field(timedelta(seconds=1))
    healthcheck_interval: timedelta = Field(timedelta(seconds=60))
    nearest_node: Optional[Node] = Field(None)

    def __init__(self, loop: Optional[asyncio.AbstractEventLoop] = None, **kwargs):
        super().__init__(**kwargs)
        if loop:
            self.loop = loop
        else:
            self.loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

        self.loop.set_debug(True)
        self.tasks = []

        self.session: Optional[aiohttp.ClientSession] = None
        self.loop.run_until_complete(self.setup_session())

    async def do_healthcheck(self, node: Node, session: aiohttp.ClientSession) -> Optional[timedelta]:
        now = datetime.now()
        if now - node.last_checked > self.healthcheck_interval:
            response = await session.get(node.url + "/health")
            if response.status == 200:
                return datetime.now() - now

    @retry(no_healthy_node)
    async def select_new_node(self, session: aiohttp.ClientSession) -> Optional[Node]:
        logger.info("selecting new node")
        best_time: Optional[timedelta] = None
        best_node: Optional[Node] = None
        for node in self.nodes:
            time = await self.do_healthcheck(node, session)
            logger.debug(time)
            if time:
                if best_time is None or time < best_time:
                    best_time = time
                    best_node = node

        return best_node

    async def setup_session(self):
        sel_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.connection_timeout.seconds))
        self.nearest_node = await self.select_new_node(sel_session)
        await sel_session.close()
        logger.info("establishing new session")
        self.session = aiohttp.ClientSession(self.nearest_node.url,
                                             timeout=aiohttp.ClientTimeout(total=self.connection_timeout.seconds),
                                             headers={API_KEY_HEADER_NAME: self.api_key})

    def wait_all(self):
        logger.debug("+++++++++++++++++++++++")
        logger.debug(self.tasks)
        names = list(map(lambda task: task.get_name(), self.tasks))
        res = self.loop.run_until_complete(gather(*self.tasks, loop=self.loop))
        self.tasks = []
        return dict(zip(names, res))

    @abstractmethod
    def close_session(self):
        pass

    @abstractmethod
    def get(self, url, *args, **kwargs) -> V:
        pass

    @abstractmethod
    def post(self, url, *args, **kwargs) -> V:
        pass

    @abstractmethod
    def put(self, url, *args, **kwargs) -> V:
        pass

    @abstractmethod
    def delete(self, url, *args, **kwargs) -> V:
        pass

    @classmethod
    def ret_type(cls, type: type):
        if cls.sync():
            return type
        else:
            return Awaitable[type]

    @classmethod
    @abstractmethod
    def sync(cls):
        pass

    class Config:
        extra = "allow"


class ApiCallerAsync(ApiCaller, metaclass=ApiCallerMetaclass[Awaitable[T]]):
    @classmethod
    def sync(cls):
        return False

    @wrap_task
    async def close_session(self):
        return await self.session.close()


class ApiCallerSync(ApiCaller, metaclass=ApiCallerMetaclass[Dict[str, Any]]):
    @classmethod
    def sync(cls):
        return True

    def close_session(self):
        return self.loop.run_until_complete(self.session.close())
