from typesense_orm.api_caller import ApiCallerAsync, Node
from typesense_orm.base_model import create_base_model
from typesense_orm.field import Field
from typesense_orm.types import int32
from typesense_orm.client import Client
from typing import List, get_args

node = Node(url="http://localhost:8108")
client = Client[ApiCallerAsync](api_key="abcd", nodes=[node], caller_class=ApiCallerAsync)
# print(client.__orig_bases__[0].__parameters__)

BaseModel = create_base_model(client)


class Books(BaseModel):
    title: str = Field(..., index=True)
    year: int32 = Field(2001, default_sorting_field=True)
    rating: float = Field(0)
    authors: List[str] = Field([])

    class Config:
        symbols_to_index = []
        token_separators = ["+"]


print(Books.to_schema().json(exclude_unset=True))

client.close()
