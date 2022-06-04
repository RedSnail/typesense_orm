from typing import TypeVar, Dict, Sequence, Tuple, Union, get_args, get_origin, _GenericAlias
from pydantic import conint

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

allowed_types_rev = dict((v, k) for k, v in allowed_types.items())


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
