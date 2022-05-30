from pydantic import BaseModel, Field, AnyHttpUrl
from typing import Sequence, Optional, Callable, TypeVar, Awaitable
import aiohttp
import asyncio
from datetime import datetime, timedelta
from .logging import logger
from .exceptions import NoHealthyNode
from functools import wraps


class Node(BaseModel):
    url: AnyHttpUrl
    last_checked: datetime = Field(datetime(1970, 1, 1))


Cl = TypeVar("Cl", bound="ApiCaller")


def retry(do_after_retries: Callable[[Cl], None]):
    def decorator(func):
        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            while True:
                for _ in range(self.num_retries):
                    try:
                        logger.debug("trying")
                        res = await func(self, *args, **kwargs)
                        return res
                    except aiohttp.ClientConnectionError:
                        logger.debug("excepted connection")

                    await asyncio.sleep(self.retry_interval.seconds)

                do_after_retries(self)

        return wrapper

    return decorator


def no_healthy_node(self: Cl):
    raise NoHealthyNode(f"No node has responded after {self.num_retries} retries")


def nearest_node_unhealthy(self: Cl):
    logger.info("current node is unhealthy")
    self.nearest_node = None
    self.loop.run_until_complete(self.session.close())
    self.loop.run_until_complete(self.setup_session())


def request_factory(method: Callable[..., Awaitable[aiohttp.ClientResponse]]):
    @wraps(method)
    @retry(nearest_node_unhealthy)
    async def make_request(self: Cl, *args, **kwargs) -> Awaitable[aiohttp.ClientResponse]:
        return method(self, *args, **kwargs)

    return make_request


class ApiCaller(BaseModel):
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
        self.get_cor = request_factory(aiohttp.ClientSession.get)
        self.post_cor = request_factory(aiohttp.ClientSession.post)
        self.put_cor = request_factory(aiohttp.ClientSession.put)
        self.delete_cor = request_factory(aiohttp.ClientSession.delete)
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
                                             timeout=aiohttp.ClientTimeout(total=self.connection_timeout.seconds))

    def close_session(self):
        self.loop.run_until_complete(self.session.close())
        self.session = None

    async def close_cor(self):
        return self.session.close()

    class Config:
        extra = "allow"
