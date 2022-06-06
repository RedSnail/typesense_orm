from typesense_orm.api_caller import ApiCallerAsync, Node
from typesense_orm.base_model import create_base_model
from typesense_orm.field import Field
from typesense_orm.types import int32
from typesense_orm.higher_client import Client
from typing import List, get_args

node = Node(url="http://localhost:8108")
client = Client[ApiCallerAsync](api_key="abcd", nodes=[node], caller_class=ApiCallerAsync)
# print(client.__orig_bases__[0].__parameters__)

BaseModel = create_base_model(client)
client.delete_collection("books")


class Books(BaseModel):
    title: str = Field(..., index=True)
    year: int32 = Field(2001, default_sorting_field=True, optional=False, index=True)
    rating: float = Field(0)
    authors: List[str] = Field([])

    class Config:
        symbols_to_index = []
        token_separators = ["+"]


book1 = Books(title="harry potter", year=2001)
book2 = Books(title="hp 2", year=2002)
client.add(book1, schedule=True, name="adding_task1")
client.add(book2, schedule=True, name="adding_task2")
print(client.wait_for_all())
print(book2.id)
print(Books.to_schema().json(exclude_unset=True))

client.close()
