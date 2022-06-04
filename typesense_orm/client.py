from .api_caller import ApiCaller, Node, ApiCallerSync, ApiCallerAsync
from .collection import Collection
from typing import Sequence, Dict, Any, Generic, TypeVar, Type
from .schema import Schema
from .exceptions import ApiResponseNotOk
from .logging import logger
from gc import get_referrers

COLLECTIONS_PATH = "/collections"

C = TypeVar("C", bound=ApiCaller)


class Client(Generic[C]):
    def __init__(self, api_key: str, nodes: Sequence[Node], caller_class: Type[ApiCaller]):
        self.api_caller = caller_class(nodes=nodes, api_key=api_key)
        self.collections: Dict = {}

    def __class_getitem__(cls, item):
        print(item)
        ret = super().__class_getitem__(item)
        print(ret.__args__)
        return ret

    def create_collection(self, schema: Schema) -> Schema:
        try:
            task_name = f"create_collection_{schema.name}"
            self.api_caller.post(COLLECTIONS_PATH,
                                 handler=lambda d: Schema.from_dict(d),
                                 data=schema.json(exclude_unset=True),
                                 schedule=True, name=f"create_collection_{schema.name}")
            resp = self.api_caller.wait_all()[task_name]
        except ApiResponseNotOk as e:
            if e.status_code == 409 and e.response.get("message", "") == \
                    f"A collection with name `{schema.name}` already exists.":
                logger.info(f"collection {schema.name} already exists")
                resp = None
            else:
                raise e

        return resp

    def delete_collection(self, schema: Schema):
        return self.api_caller.delete(f"{COLLECTIONS_PATH}/{schema.name}")

    async def update_collection(self, schema: Schema) -> Schema:
        # I'll implement it in several days, when the current version will support it.
        pass

    def update_collection_sync(self, schema: Schema) -> Schema:
        pass

    async def add(self):
        pass

    def close(self):
        return self.api_caller.close_session()

    def wait_for_all(self):
        return self.api_caller.wait_all()

