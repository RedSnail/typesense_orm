from pydantic import BaseModel, Field, AnyHttpUrl
from pydantic.generics import GenericModel
from typing import Sequence, Optional, Callable, TypeVar, Awaitable, Dict, Any, Type, Generic, Union, AsyncIterable, Iterable, ClassVar
import aiohttp
import asyncio
from asyncio import Task, gather, all_tasks
from datetime import datetime, timedelta
from .logging import logger
from .exceptions import NoHealthyNode, ApiResponseNotOk
from functools import wraps
from abc import ABC, abstractmethod
from pydantic.main import ModelMetaclass
import inspect
from .exception_dict import ExceptionDict
from json import loads


class Node(BaseModel):
    url: AnyHttpUrl
    last_checked: datetime = Field(datetime(1970, 1, 1))


Cl = TypeVar("Cl", bound="ApiCaller")
T = TypeVar("T")
V = TypeVar("V")

Wrapper = TypeVar("Wrapper")
Iterator = TypeVar("Iterator")


def wrap_task(func: Callable[..., Awaitable[Any]]):
    """
    A decorator for async functions that creates task from a coroutine.
    Args:
        func (): a callback function

    Returns:

    """
    @wraps(func)
    def wrapper(self: Cl, *args, schedule=True, name=None, **kwargs):
        """

        Args:
            self (ApiCaller): a caller instance *args (): other positional arguments passed to function
            schedule (bool): whether a caller instance should memorize a task so that it results could be retrieved with
            ApiCaller.wait_all()
            name (str): A name for the newly-created task
            **kwargs (): other kwargs passed to a  callback function

        Returns:
            asyncio.Task: a task in a caller loop which was created.

        """
        print(f"created task {name}")
        task = self.loop.create_task(func(self, *args, **kwargs), name=name)
        if schedule:
            self.tasks[task.get_name()] = task

        return task

    return wrapper


def retry(do_after_retries: Callable[[Cl], Any]):
    """
    A decorator to retry connection several times.
    Args:
        do_after_retries (): a callback that is applied when retries fail

    Returns:

    """
    def decorator(func):
        @wraps(func)
        async def wrapper(self: Cl, *args, **kwargs):
            while True:
                for _ in range(self.num_retries):
                    try:
                        logger.info("trying")
                        res = func(self, *args, **kwargs)
                        if inspect.iscoroutine(res):
                            ret = await res
                        else:
                            ret = res

                        if ret:
                            return ret

                    except aiohttp.ClientConnectionError:
                        logger.debug("excepted connection")

                    await asyncio.sleep(self.retry_interval.seconds)

                do_after_retries(self)

        return wrapper

    return decorator


class MethodAssigner(ModelMetaclass):
    """
    A metaclass for api callers, I have no wish to implement all request methods separately, so I've created a factory,
    and this metaclass implements it.
    """
    def __new__(mcs, *args, **kwargs):
        ret: Type[ApiCaller] = super().__new__(mcs, *args, **kwargs)
        sync = ret.sync()
        ret.get = request_factory(aiohttp.ClientSession.get, sync)
        ret.post = request_factory(aiohttp.ClientSession.post, sync)
        ret.put = request_factory(aiohttp.ClientSession.put, sync)
        ret.delete = request_factory(aiohttp.ClientSession.delete, sync)
        ret.__abstractmethods__ = frozenset(ret.__abstractmethods__ - {"delete", "post", "put", "get"})
        if sync:
            ret.WRAPPER = Union
            ret.ITERATOR = Iterable
        else:
            ret.WRAPPER = Task
            ret.ITERATOR = AsyncIterable
        return ret



def no_healthy_node(self: Cl):
    """
    A callback which is implemented when ho healthy node is found.
    Args:
        self (ApiCaller): a caller which implements a callback

    Raises:
        NoHealthyNode: an exception which occurs when there is no healthy nodes.

    """
    raise NoHealthyNode(f"No node has responded after {self.num_retries} retries")


def nearest_node_unhealthy(self: Cl):
    """
    A callback which is applied when the nearest node is unhealthy, and caller has to select a new one.
    Args:
        self (ApiCaller):

    """
    logger.info("current node is unhealthy")
    self.nearest_node = None
    self.loop.run_until_complete(self.session.close())
    self.loop.run_until_complete(self.setup_session())


def request_factory(method: Callable[..., Awaitable[aiohttp.ClientResponse]], sync: bool):
    """
    A factory for request functions
    Args:
        method (): a coroutine function which implements the call
        sync (bool): whether an output function return a task or a ready result

    Returns:

    """
    @wraps(method)
    @wrap_task
    @retry(nearest_node_unhealthy)
    async def make_request(self: Cl, url,
                           handler: Callable[[Dict[str, Any]], T] = lambda a: a,
                           multiline=False,
                           **kwargs) \
            -> Union[Awaitable[T], AsyncIterable[T]]:
        """
        Make a request and handle a response asynchronously
        Args:
            self (ApiCaller): a caller instance
            url (str): url endpoint to make request
            handler (): a callback function which is used to handle response as json
            multiline (bool): if the response is expected to be multiline. If so, the callback will be called with two
            parameters - json and line index.
            **kwargs (): additional keyword arguments passed to the request function.

        Returns:
            asyncio.Coroutine

        """
        r = await method(self.session, url, **kwargs)
        if r.status < 200 or r.status >= 300:
            raise ApiResponseNotOk(await r.json(), r.status)
        if not multiline:
            json = await r.json()
            r.close()
            return handler(json)
        else:
            async def async_gen(response: aiohttp.ClientResponse):
                i = 0
                while True:
                    line = await response.content.readline()
                    if line == b"":
                        break
                    yield handler(i, loads(line))
                    i += 1
                response.close()

            return async_gen(r)

    if sync:
        @wraps(make_request)
        def make_request_sync(self: Cl, url,
                              handler: Callable[[Dict[str, Any]], T] = lambda a: a,
                              schedule=False,
                              name=None,
                              multiline=False,
                              **kwargs) -> Union[T, Iterable[T]]:
            """
            Make the same as make_request in a synchronized manner

            Notes:
                you can still add a callback handler and make a caller to memorize the results.

            """
            ret = self.loop.run_until_complete(make_request(self, url,
                                                            handler=handler,
                                                            schedule=schedule,
                                                            name=name, multiline=multiline,
                                                            **kwargs))
            if isinstance(ret, AsyncIterable):
                ret: Iterable[T] = self.synchronise_iterator(ret)

            return ret

        return make_request_sync

    if not sync:
        logger.debug(inspect.iscoroutinefunction(make_request))
    return make_request


API_KEY_HEADER_NAME = 'X-TYPESENSE-API-KEY'


class ApiCaller(ABC, GenericModel, Generic[Wrapper, Iterator]):
    """
    A base class for api callers.
    Attributes:
        api_key (str): An api key to make the requests
        nodes (list of Node): a list of nodes that this caller can use.
        connection_timeout(timedelta): connection timeout
        num_retries (int): number of retries it makes before considers node unhealthy.
        retry_interval (timedelta): retry interval
        healthcheck_interval (timedelta): interval after unsuccessful healthcheck before the next one
        nearest_node: (Node): a nearest node which is used by caller.
        loop: (asyncio.AbstractEventLoop): an event loop which is used by caller to perform tasks (synchronous caller either uses it)
        tasks: (dict of Task): tasks which results can currently be retrieved by ApiCaller.wait_for_all()
        session: (aiohttp.ClientSession or None): aiohttp client session used by caller.
    """
    WRAPPER: ClassVar = None
    ITERATOR: ClassVar = None
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
        self.tasks = {}

        self.session: Optional[aiohttp.ClientSession] = None
        self.loop.run_until_complete(self.setup_session())

    async def do_healthcheck(self, node: Node, session: aiohttp.ClientSession) -> Optional[timedelta]:
        now = datetime.now()
        if now - node.last_checked > self.healthcheck_interval:
            response = await session.get(node.url + "/health")
            logger.debug(await response.json())
            logger.debug(response.status)
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
        sel_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.connection_timeout.seconds),
                                            headers={API_KEY_HEADER_NAME: self.api_key})
        self.nearest_node = await self.select_new_node(sel_session)
        await sel_session.close()
        self.session = aiohttp.ClientSession(self.nearest_node.url,
                                             timeout=aiohttp.ClientTimeout(total=self.connection_timeout.seconds),
                                             headers={API_KEY_HEADER_NAME: self.api_key})

    def wait_all(self) -> ExceptionDict:
        """
        Retrieve results of all scheduled tasks.
        Returns:

        """
        if len(self.tasks) == 0:
            return ExceptionDict({})

        done, pending = self.loop.run_until_complete(asyncio.wait(self.tasks.values(),
                                                                  loop=self.loop,
                                                                  return_when=asyncio.ALL_COMPLETED))
        self.tasks = {}

        return ExceptionDict(dict(map(lambda t: (t.get_name(), t), done)))

    def synchronise_iterator(self, ait: AsyncIterable[T]) -> Iterable[T]:
        async def get_next(aiterator: AsyncIterable):
            try:
                obj = await aiterator.__anext__()
                return False, obj
            except StopAsyncIteration:
                return True, None

        def iterator(aiterator: AsyncIterable):
            print("IN ITERATOR")
            while True:
                fin, obj = self.loop.run_until_complete(get_next(aiterator))
                if fin:
                    break
                yield obj

        ret: Iterable[T] = iterator(ait)
        return ret

    @wraps(aiohttp.ClientSession.close)
    @abstractmethod
    def close_session(self):
        pass

    @wraps(aiohttp.ClientSession.get)
    @abstractmethod
    def get(self, url, *args, **kwargs) -> Wrapper:
        pass

    @wraps(aiohttp.ClientSession.post)
    @abstractmethod
    def post(self, url, *args, **kwargs) -> Wrapper:
        pass

    @wraps(aiohttp.ClientSession.put)
    @abstractmethod
    def put(self, url, *args, **kwargs) -> Wrapper:
        pass

    @wraps(aiohttp.ClientSession.delete)
    @abstractmethod
    def delete(self, url, *args, **kwargs) -> Wrapper:
        pass

    @classmethod
    @abstractmethod
    def sync(cls):
        pass

    class Config:
        extra = "allow"


class ApiCallerAsync(ApiCaller[Task, AsyncIterable], metaclass=MethodAssigner):
    WRAPPER = Task
    ITERATOR = AsyncIterable
    @classmethod
    def sync(cls):
        return False

    @wrap_task
    async def close_session(self):
        return await self.session.close()


class ApiCallerSync(ApiCaller[Any, Iterable], metaclass=MethodAssigner):
    WRAPPER = Union
    ITERATOR = Iterable
    @classmethod
    def sync(cls):
        return True

    def close_session(self):
        return self.loop.run_until_complete(self.session.close())
