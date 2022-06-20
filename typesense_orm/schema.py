from pydantic import BaseModel, Field
from typing import Any, Sequence, Optional, Dict
import json
from .types import get_from_opt, allowed_types, allowed_types_rev
from .search import FieldArgs


def json_dumper(v, *, default):
    for field in v["fields"].values():
        _, field_type = get_from_opt(field["type"])
        field["type"] = allowed_types[field_type]

    v["fields"] = list(v["fields"].values())
    return json.dumps(v)


def json_loader(js_string):
    js_dict = json.loads(js_string)
    fields = {}
    for field in js_dict["fields"]:
        field["type"] = allowed_types_rev[field["type"]]
        fields.update({field["name"]: FieldArgs(**field)})

    js_dict["fields"] = fields
    return js_dict


class Schema(BaseModel):
    name: str
    fields: Dict[str, FieldArgs]
    token_separators: Optional[Sequence[str]]
    symbols_to_index: Optional[Sequence[str]]
    default_sorting_field: Optional[str]
    num_documents: Optional[int]
    created_at: Optional[int]

    @classmethod
    def from_dict(cls, dict_schema: Dict[str, Any]):
        fields = []
        for field in dict_schema["fields"]:
            field["type"] = allowed_types_rev[field["type"]]
            fields.append(FieldArgs(**field))

        dict_schema["fields"] = dict(map(lambda f: (f["name"], f), dict_schema["fields"]))
        return cls(**dict_schema)

    class Config:
        extra = "ignore"
        json_dumps = json_dumper
        json_loads = json_loader
