import pydantic
from .field import ModelField
from typing import Dict, ClassVar, Optional, Type
from .logging import logger
from .client import Client
from .types import int32, int64
from .exceptions import InvalidSortingFieldType, MultipleSortingFields
from .schema import Schema, FieldArgs
from .config import BaseConfig

pydantic.main.ModelField = ModelField
pydantic.main.BaseConfig = BaseConfig


class ModelMetaclass(pydantic.main.ModelMetaclass):
    def to_schema(cls) -> Schema:
        field_dict = cls.__fields__
        field_dict.pop("id", None)
        fields = list(map(lambda field: FieldArgs(name=field.name,
                                                  type=field.type_,
                                                  index=field.field_info.extra.get("index", False),
                                                  facet=field.field_info.extra.get("facet", False),
                                                  optional=field.field_info.extra.get("optional", False)),
                          field_dict.values()))
        schema = Schema(name=cls.__name__.lower(),
                        fields=fields,
                        token_separators=cls.__config__.token_separators,
                        symbols_to_index=cls.__config__.symbols_to_index,
                        default_sorting_field=cls.__config__.default_sort_field)

        return schema

    def __new__(mcs, name, bases, namespace, **kwargs):
        ret: Type[BaseModel] = super().__new__(mcs, name, bases, namespace, **kwargs)
        if (namespace.get('__module__'), namespace.get('__qualname__')) != ('pydantic.main', 'BaseModel'):

            found_sort_field = False
            for field in ret.__fields__.values():
                if field.field_info.extra.get("default_sorting_field", False):
                    if field.type_ not in (float, int32):
                        raise InvalidSortingFieldType(field.type_)
                    if found_sort_field:
                        raise MultipleSortingFields()

                    found_sort_field = True
                    ret.__config__.default_sort_field = field.name

            client: Optional[Client] = None
            for base in bases:
                if base.__client__:
                    client = base.__client__
                    namespace.update({"__client__": client})

            client.create_collection(ret.to_schema())


        # print(namespace)
        return ret


class BaseModel(pydantic.main.BaseModel):
    id: Optional[int64]


def create_base_model(client: Client):
    return ModelMetaclass("BaseModel", (BaseModel,), {"__module__": "pydantic.main",
                                                                    "__qualname__": "BaseModel",
                                                                    "__client__": client})

