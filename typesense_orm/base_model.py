import pydantic
from .field import ModelField
from typing import Dict, ClassVar, Optional, Type
from .logging import logger
from .lower_client import LowerClient
from .types import int32, int64
from .exceptions import InvalidSortingFieldType, MultipleSortingFields, NotOptional
from .schema import Schema, FieldArgs
from .config import BaseConfig
from .lower_client import COLLECTIONS_PATH

pydantic.main.ModelField = ModelField
pydantic.main.BaseConfig = BaseConfig

DOC_ENDPOINT = "documents"


class ModelMetaclass(pydantic.main.ModelMetaclass):
    def to_schema(cls) -> Schema:
        field_dict = cls.__fields__.copy()
        field_dict.pop("id", None)
        fields = list(map(lambda field: FieldArgs(name=field.name,
                                                  type=field.type_,
                                                  index=field.field_info.extra.get("index", False),
                                                  facet=field.field_info.extra.get("facet", False),
                                                  optional=field.field_info.extra.get("optional", True)),
                          field_dict.values()))
        schema = Schema(name=cls.schema_name,
                        fields=fields,
                        token_separators=cls.__config__.token_separators,
                        symbols_to_index=cls.__config__.symbols_to_index,
                        default_sorting_field=cls.__config__.default_sort_field)

        return schema

    @property
    def endpoint_path(cls):
        return f"{COLLECTIONS_PATH}/{cls.schema_name}/{DOC_ENDPOINT}"

    def __new__(mcs, name, bases, namespace, **kwargs):
        ret: Type[BaseModel] = super().__new__(mcs, name, bases, namespace, **kwargs)
        if (namespace.get('__module__'), namespace.get('__qualname__')) != ('typesense_orm.base_model', 'BaseModel'):
            ret.schema_name = ret.__name__.lower()
            found_sort_field = False
            ret.__slots__ = tuple(set(ret.__slots__) | pydantic.BaseModel.__slots__ | set(ret.__fields__.keys()))
            for field in ret.__fields__.values():
                if not field.field_info.extra.get("optional", True) and not field.field_info.extra.get("index", False):
                    raise NotOptional(field.name)
                if field.field_info.extra.get("default_sorting_field", False):
                    if field.type_ not in (float, int32, str):
                        raise InvalidSortingFieldType(field.type_)
                    if found_sort_field:
                        raise MultipleSortingFields()

                    found_sort_field = True
                    ret.__config__.default_sort_field = field.name

            client: Optional[LowerClient] = None
            for base in bases:
                if base.__client__:
                    client = base.__client__
                    namespace.update({"__client__": client})
                    break

            client.create_collection(ret.to_schema())

        return ret


class BaseModel(pydantic.main.BaseModel):
    id: Optional[str]


def create_base_model(client: LowerClient):
    return ModelMetaclass("BaseModel", (BaseModel,), {"__module__": "typesense_orm.base_model",
                                                                    "__qualname__": "BaseModel",
                                                                    "__client__": client})

