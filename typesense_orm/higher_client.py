from .lower_client import LowerClient
from typing import Sequence, Type, Dict, Callable, TypeVar, Union, Iterable, AsyncIterable, Any
from .logging import logger
from typing_extensions import Unpack
from .api_caller import Node, ApiCaller
from .base_model import BaseModel
from .search import SearchQuery, SearchRes, Hit, PaginatedQuery
from collections import defaultdict
from typing_inspect import get_bound
from functools import singledispatchmethod
from .exceptions import CollectionUnregistered
from asyncstdlib import groupby, chain
from asyncstdlib import map as as_map
import random
import string
from asyncio import Task
import itertools

ADD_ENDPOINT = "add/"
SEARCH_ENDPOINT = "/search"

EntryType = TypeVar("EntryType", bound=BaseModel)
HandlerRetType = TypeVar("HandlerRetType")

C = TypeVar("C")


class Client(LowerClient[C]):
    def add(self, entry: EntryType, schedule=False, name=None,
            on_added: Callable[[EntryType], HandlerRetType] = lambda a: a):
        def handler(resp: Dict[str, Any]):
            entry.id = resp["id"]
            return on_added(entry)

        return self.api_caller.post(f"{entry.__class__.endpoint_path}", data=entry.json(exclude_none=True),
                                    schedule=schedule, name=name, handler=handler)

    def upsert(self, entry: EntryType, schedule=False, name=None,
               on_upsert: Callable[[EntryType], HandlerRetType] = lambda a: a):
        def handler(resp: Dict[str, Any]):
            entry.id = resp["id"]
            return on_upsert(entry)

        return self.api_caller.post(f"{entry.__class__.endpoint_path}", data=entry.json(exclude_none=True),
                                    schedule=schedule, name=name, handler=handler, params={"action": "upsert"})

    def import_json(self, collection: Type[EntryType], data: Union[AsyncIterable[str], Iterable[str]],
                    schedule=False, name=None,
                    error_handler: Callable[[int, Dict[str, Any]], HandlerRetType] = lambda i, a: (i, a),
                    action: str = "create",
                    entry_handler: Callable[[int, EntryType], HandlerRetType] = lambda i, a: (i, a)):
        if isinstance(data, Iterable):
            async def as_gen(iterable: Iterable):
                for i in iterable:
                    yield i

            data: AsyncIterable[str] = as_gen(data)

        async def iter_byte(iter_json: AsyncIterable[str]):
            async for i in iter_json:
                print(i)
                yield (i + "\n").encode("utf-8")

        def handler(i: int, resp: Dict[str, Any]):
            if not resp["success"]:
                return error_handler(i, resp)
            else:
                return entry_handler(i, collection(**resp["document"]))

        return self.api_caller.post(f"{collection.endpoint_path}/import", data=iter_byte(data),
                                    schedule=schedule, name=name, handler=handler, multiline=True,
                                    params={"action": action, "return_res": "true", "return_id": "false"})

    def import_objects(self, data: Union[AsyncIterable[EntryType], Iterable[EntryType]], schedule=False, name=None,
                       error_handler: Callable[[int, Dict[str, Any]], HandlerRetType] = lambda i, a: (i, a),
                       action: str = "create",
                       entry_handler: Callable[[int, EntryType], HandlerRetType] = lambda i, a: (i, a)):
        if isinstance(data, Iterable):
            async def as_gen(iterable: Iterable):
                for i in iterable:
                    yield i

            data: AsyncIterable[EntryType] = as_gen(data)

            async def import_everything(as_generator: AsyncIterable[EntryType]):
                tasks_or_its: Sequence[Union[Task, Iterable]] = []
                async for k, g in groupby(as_generator, key=lambda e: type(e)):
                    res = self.import_json(k,
                                           as_map(lambda e: e.json(exclude_none=True), g),
                                           schedule=False, error_handler=error_handler, entry_handler=entry_handler,
                                           action=action)
                    tasks_or_its.append(res)
                    if self.api_caller.sync():
                        return itertools.chain(*tasks_or_its)
                    else:
                        return chain.from_iterable((await part_task for part_task in tasks_or_its))

            task = self.api_caller.loop.create_task(import_everything(data), name=name)
            if schedule:
                self.api_caller.tasks[task.get_name()] = task

            if self.api_caller.sync():
                return self.api_caller.loop.run_until_complete(task)
            else:
                return task

    def search(self, collection: Type[EntryType], query: SearchQuery, schedule=False, name=None):
        def handler(resp: Dict[str, Any]):
            print(resp)
            return SearchRes[collection].parse_obj(resp)

        first_res = self.api_caller.get(f"{collection.endpoint_path}/search",
                                        params=query.dict(exclude_none=True),
                                        schedule=schedule, name=name, handler=handler)
        yield first_res
        if self.api_caller.sync():
            pages = first_res.out_of
        else:
            pages = self.api_caller.loop.run_until_complete(first_res).out_of

        for i in range(1, pages):
            params = PaginatedQuery(page=i, **query.__dict__)
            yield self.api_caller.get(f"{collection.endpoint_path}/search",
                                      params=params.dict(exclude_none=True, exclude_defaults=True),
                                      schedule=schedule, name=name, handler=handler)
