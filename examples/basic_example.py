from typesense_orm import ApiCallerAsync, Node, ApiCallerSync, create_base_model, Field, int32, Client, SearchQuery
from typing import List, get_args, Optional
from enum import IntEnum, Enum


class Genres(IntEnum):
    FANTASY = 1
    NOVEL = 2
    OTHER = 3


node = Node(url="http://localhost:8108")
client = Client[ApiCallerSync](api_key="abcd", nodes=[node])
client.start()

BaseModel = create_base_model(client)
try:
    client.delete_collection("books")
except Exception:
    pass


class Books(BaseModel):
    title: str = Field(..., index=True)
    year: int32 = Field(2001, default_sorting_field=True, optional=False, index=True)
    rating: float = Field(0)
    authors: List[str] = Field([])
    genre: Genres = Field(Genres.OTHER)
    age_rating: int32 = Field(0, facet=True)

    class Config:
        symbols_to_index = []
        token_separators = ["+"]


book1 = Books(title="harry potter", year=2001, genre=Genres.FANTASY, authors=["Joahn Rowling"])
book2 = Books(title="hp 2", year=2002, genre=Genres.FANTASY, authors=["Joahn Rowling"])
print(book1.json(exclude_unset=True))
it = client.import_objects([book1, book2])
for i in it:
    print(i)

q = SearchQuery(q="harry potter", query_by=[Books.title], filter_by=Books.year > 2000,
                split_join_tokens=True, facet_by=[Books.age_rating])
res = client.search(Books, q)
for r in res:
    print(r)

client.close()
