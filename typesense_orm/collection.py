from .api_caller import ApiCaller
from typing import Optional, Type
# from .base_model import ModelMetaclass


class Collection:
    def __init__(self, api_caller: ApiCaller, schema):
        self.api_caller = api_caller
        self.schema = schema
