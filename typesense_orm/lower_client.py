from .api_caller import ApiCaller, Node, ApiCallerSync, ApiCallerAsync
from .collection import Collection
from typing import Sequence, Dict, Any, Generic, TypeVar, Type, Optional
from .schema import Schema
from .exceptions import ApiResponseNotOk
from .logging import logger
from gc import get_referrers
from asyncio import Task

COLLECTIONS_PATH = "/collections"

C = TypeVar("C", bound=ApiCaller)


class LowerClient(Generic[C]):
    def __init__(self, api_caller: C):
        self.api_caller = api_caller
        self.collections: Dict = {}

    def create_collection(self, schema: Schema) -> Schema:
        try:
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
        return self.api_caller.close_session()

    def wait_for_all(self):
        return self.api_caller.wait_all()

