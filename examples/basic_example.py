from typesense_orm.api_caller import ApiCaller, Node
from typesense_orm.base_model import BaseModel
from typesense_orm.field import Field, int32


class Books(BaseModel):
    title: str = Field(..., index=True)
    year: int32 = Field(2001)


print(Books.__fields__)


# node = Node(url="http://localhost:8108")
# client = ApiCaller(api_key="abcd", nodes=[node])

# client.close_session()
