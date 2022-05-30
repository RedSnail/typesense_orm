from typesense_orm.client import Client, Node

node = Node(url="http://localhost:8108")
client = Client(api_key="abcd", nodes=[node])

