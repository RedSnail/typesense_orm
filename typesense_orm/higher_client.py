from .lower_client import LowerClient, C
from typing import Sequence, Type, Dict, Callable, TypeVar
from .api_caller import Node, Any
from .base_model import BaseModel

ADD_ENDPOINT = "add/"

EntryType = TypeVar("EntryType", bound=BaseModel)


class Client(LowerClient[C]):
    def __init__(self, api_key: str, nodes: Sequence[Node], caller_class: Type[C]):
        api_caller = caller_class(api_key=api_key, nodes=nodes)
        super().__init__(api_caller)

    def add(self, entry: BaseModel, schedule=False, name=None, on_added: Callable[[EntryType], Any] = lambda a: a):
        def handler(resp: Dict[str, Any]):
            entry.id = resp["id"]

        self.api_caller.post(f"{entry.__class__.endpoint_path}", data=entry.json(exclude_unset=True),
                             schedule=schedule, name=name, handler=handler)



