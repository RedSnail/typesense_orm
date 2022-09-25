from typing import TypeVar, Dict, Sequence, Tuple, Union, get_args, get_origin, _GenericAlias, Iterable, Any, Optional
from pydantic import conint
from enum import IntEnum


def check_subclass(subclass, superclass):
    try:
        return issubclass(subclass, superclass)
    except Exception:
        pass

    if subclass == superclass:
        return True

    if superclass == Any:
        return True

    if (superclass is None) or (subclass is None):
        return False

    sub_origin = get_origin(subclass)
    sub_arg = get_args(subclass)

    if sub_origin == Optional:
        return check_subclass(sub_arg, superclass)

    if sub_origin is None:
        sub_arg = subclass
    if not isinstance(sub_arg, Iterable):
        sub_arg = [sub_arg]

    if sub_origin == Union:
        return all(map(lambda subtype: check_subclass(subtype, superclass), sub_arg))

    super_origin = get_origin(superclass)
    super_arg = get_args(superclass)
    if super_origin == Optional:
        return check_subclass(subclass, super_arg)
    if super_origin is None:
        super_arg = superclass
    if not isinstance(super_arg, Iterable):
        super_arg = [super_arg]

    if super_origin == Union:
        return any(map(lambda supertype: check_subclass(subclass, supertype), super_arg))

    if check_subclass(sub_origin, super_origin):
        if len(sub_arg) == len(super_arg):
            return all(map(check_subclass, sub_arg, super_arg))
        elif len(super_arg) == 0:
            return True
        else:
            return False

    return False


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
    to_string: "string*", auto: "auto",
    IntEnum: "int32", Sequence[IntEnum]: "int32[]",
    int: "int64", Sequence[int]: "int64[]"
}

allowed_types_rev = dict((v, k) for k, v in allowed_types.items())


class TypeDict:
    def __init__(self, data: Dict):
        self.data = data

    def __getitem__(self, item):
        for key, value in self.data.items():
            if check_subclass(item, key):
                return value

        raise KeyError(item)


allowed_types: TypeDict = TypeDict(allowed_types)


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
