from pydantic import Field as PydanticField
from pydantic import conint, BaseModel, validator
from typing import TypeVar, Tuple, Dict, Sequence, Union, _GenericAlias, get_origin, get_args, Any, Optional
from .exceptions import UnsupportedTypeError
import pydantic
from pydantic.fields import Undefined, NoArgAnyCallable
from .logging import logger

auto = TypeVar("auto")
to_string = TypeVar("to_string")
int32 = conint(ge=-pow(2, 31), lt=pow(2, 31))
int64 = conint(ge=-pow(2, 63), lt=pow(2, 63))
geo = Tuple[float, float]

allowed_types: Dict[Union[type, _GenericAlias], str] = {
    str: "string", Sequence[str]: "string[]",
    int32: "int32", Sequence[int32]: "int32[]",
    int64: "int64", Sequence[int64]: "int64[]",
    float: "float", Sequence[float]: "float[]",
    bool: "bool", Sequence[bool]: "bool[]",
    geo: "geopoint", Sequence[geo]: "geopoint[]",
    to_string: "string*", auto: "auto"
}


def Field(default: Any = Undefined,
          *,
          default_factory: Optional[NoArgAnyCallable] = None,
          facet=False,
          index=False,
          optional=False):
    return PydanticField(default=default, default_factory=default_factory, facet=facet, index=index, optional=optional)


def get_from_opt(some_type):
    opt = False
    field_type = some_type
    if get_origin(some_type) == Union:
        if len(get_args(some_type)) == 2 and type(None) in get_args(some_type):
            opt = True
            if get_args(some_type)[0] == type(None):
                field_type = get_args(some_type)[1]
            else:
                field_type = get_args(some_type)[0]

    return opt, field_type


class ModelField(pydantic.fields.ModelField):
    def _type_analysis(self) -> None:
        logger.debug("performing type analysis")
        opt, field_type = get_from_opt(self.type_)
        origin = get_origin(field_type)
        if origin is not None:
            # god please forgive me for this sin.
            # These checks do not look nice, and they are not, but they need to be done.

            if issubclass(origin, Sequence):
                seq_type = get_args(field_type)
                if field_type is not geo:
                    field_type = Sequence[seq_type]

            if opt:
                self.type_ = Optional[field_type]
            else:
                self.type_ = field_type

        if self.type_ not in allowed_types.keys():
            raise UnsupportedTypeError(self.type_)
        super()._type_analysis()


pydantic.fields.ModelField = ModelField
