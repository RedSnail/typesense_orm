from typesense_orm.api_caller import ApiCallerAsync, Node, ApiCallerSync
from typesense_orm.base_model import create_base_model
from typesense_orm.field import Field
from typesense_orm.types import int32
from typesense_orm.higher_client import Client
from typing import List, get_args

node = Node(url="http://localhost:8108")
client = Client[ApiCallerSync](api_key="abcd", nodes=[node])
client.start()

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
# it = client.import_json(Books, [book1.json(exclude_unset=True), book2.json(exclude_unset=True)], schedule=True, name="import_task", action="upsert")
it = client.import_objects([book1, book2])
for i in it:
    print(i)

client.close()
