import pydantic
from .field import ModelField
from typing import Dict, ClassVar, Optional, Type, Sequence, Union, ClassVar, Callable, Any
from .search import SearchRes, SearchQuery
from .logging import logger
from .lower_client import LowerClient
from .types import int32, int64
from .exceptions import InvalidSortingFieldType, MultipleSortingFields, NotOptional
from .schema import Schema, FieldArgs
from .config import BaseConfig
from .lower_client import COLLECTIONS_PATH
from typeguard import check_type


pydantic.main.ModelField = ModelField
pydantic.main.BaseConfig = BaseConfig

DOC_ENDPOINT = "documents"


class ModelMetaclass(pydantic.main.ModelMetaclass):
    schema_name: str
    schema: Schema
    __client__: LowerClient

    def to_schema(cls: Type['BaseModel']) -> Schema:
        field_dict = cls.__fields__.copy()
        field_dict.pop("id", None)
        fields = dict(map(lambda field: (field.name, FieldArgs(name=field.name,
                                                               type=field.outer_type_,
                                                               index=field.field_info.extra.get("index", False),
                                                               facet=field.field_info.extra.get("facet", False),
                                                               infix=field.field_info.extra.get("infix", False),
                                                               optional=field.field_info.extra.get("optional", True))),
                          field_dict.values()))
        schema_dict = {"name": cls.schema_name, "fields": fields}
        if cls.__config__.token_separators:
            schema_dict.update({"token_separators": cls.__config__.token_separators})
        if cls.__config__.symbols_to_index:
            schema_dict.update({"symbols_to_index": cls.__config__.symbols_to_index})
        if cls.__config__.default_sorting_field:
            schema_dict.update({"default_sorting_field": cls.__config__.default_sorting_field})

        schema = Schema(**schema_dict)

        return schema

    def __getattr__(cls, item):
        res = cls.schema.fields.get(item, None)
        if res:
            return res
        super().__getattr__(item)

    @property
    def endpoint_path(cls):
        return f"{COLLECTIONS_PATH}/{cls.schema_name}/{DOC_ENDPOINT}"

    def __new__(mcs, name, bases, namespace, is_typesense=False, **kwargs):
        ret: Type['BaseModel'] = super().__new__(mcs, name, bases, namespace, **kwargs)
        if is_typesense:
            return ret

        if not (namespace.get("__qualname__") in ["BaseModel", "_BaseModel"] and
                namespace.get("__module__") == "typesense_orm.base_model"):
            ret.schema_name = ret.__name__.lower()
            found_sort_field = False
            ret.__slots__ = tuple(set(ret.__slots__) | pydantic.BaseModel.__slots__ | set(ret.__fields__.keys()))
            for field in ret.__fields__.values():
                field.typesense_field = True
                if not field.field_info.extra.get("optional", True) and not field.field_info.extra.get("index", False):
                    raise NotOptional(field.name)
                if field.field_info.extra.get("default_sorting_field", False):
                    if field.type_ not in (float, int32, str):
                        raise InvalidSortingFieldType(field.type_)
                    if found_sort_field:
                        raise MultipleSortingFields()

                    found_sort_field = True
                    ret.__config__.default_sorting_field = field.name

            ret.schema = ret.to_schema()
            for base in bases:
                if base.__client__:
                    ret.__client__ = base.__client__
                    break

            ret.__client__.create_collection(ret.schema)

        return ret


class _BaseModel(pydantic.main.BaseModel):
    id: Optional[str]

    @classmethod
    def search(cls: ModelMetaclass, query: SearchQuery):
        pass

    def json(
        self,
        *,
        include=None,
        exclude=None,
        by_alias: bool = False,
        skip_defaults: bool = None,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
        encoder: Optional[Callable[[Any], Any]] = None,
        models_as_dict: bool = True,
        **dumps_kwargs: Any,
    ) -> str:
        return super().json(include=include, exclude=exclude, by_alias=by_alias, skip_defaults=skip_defaults,
                            exclude_unset=exclude_unset, exclude_defaults=exclude_defaults, exclude_none=exclude_none,
                            encoder=encoder)

    class Config:
        typesense_mode = True


class BaseModel(_BaseModel, metaclass=ModelMetaclass, is_typesense=True):
    pass


def create_base_model(client: LowerClient) -> Type[BaseModel]:
    # here I consciously lie to type checker 'cause it cannot
    # recognize dynamically created classes, but I still want them to be hinted
    return ModelMetaclass("BaseModel", (_BaseModel,), {"__module__": "typesense_orm.base_model",
                                                                     "__qualname__": "BaseModel",
                                                                     "__client__": client})

