from .api_caller import ApiCaller, Node, ApiCallerSync, ApiCallerAsync
from typing import Sequence, Dict, Any, Generic, TypeVar, Type, Optional, Iterable, AsyncIterable, Union
from .schema import Schema
from .exceptions import ApiResponseNotOk
from .logging import logger
from gc import get_referrers
from typing_extensions import Unpack
from asyncio import Task
from abc import abstractmethod, ABC

COLLECTIONS_PATH = "/collections"

C = TypeVar("C", bound=ApiCaller)


class LowerClient(Generic[C], ABC):
    def __init__(self, api_key: str, nodes: Sequence[Node]):
        self._api_key = api_key
        self._nodes = nodes
        self.api_caller: Optional[ApiCaller] = None

    def start(self):
        self.api_caller = self.__orig_class__.__args__[0](api_key=self._api_key, nodes=self._nodes)

    def __enter__(self):
        self.start()
        return self

    def create_collection(self, schema: Schema) -> Optional[Schema]:
        try:
            # print(schema.json(exclude_unset=True))
            task_name = f"create_collection_{schema.name}"
            resp = self.api_caller.post(COLLECTIONS_PATH,
                                        handler=lambda d: Schema.from_dict(d),
                                        data=schema.json(exclude_unset=True),
                                        schedule=False, name=task_name)
            if isinstance(resp, Task):
                return self.api_caller.loop.run_until_complete(resp)
            else:
                return resp
        except ApiResponseNotOk as e:
            if e.status_code == 409 and e.response.get("message", "") == \
                    f"A collection with name `{schema.name}` already exists.":
                logger.info(f"collection {schema.name} already exists")
                resp = None
            else:
                raise e

        return resp

    def delete_collection(self, name: str):
        resp = self.api_caller.delete(f"{COLLECTIONS_PATH}/{name}", schedule=False)
        if isinstance(resp, Task):
            return self.api_caller.loop.run_until_complete(resp)
        else:
            return resp

    async def update_collection(self, schema: Schema) -> Schema:
        # I'll implement it in several days, when the current version will support it.
        pass

    def close(self):
        self.wait_for_all()
        self.api_caller.close_session()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        if exc_val:
            raise exc_val

    def wait_for_all(self):
        return self.api_caller.wait_all()

