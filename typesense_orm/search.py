from pydantic import BaseModel, Field, validator, root_validator
from pydantic.generics import GenericModel
from typing import List, Any, Union, Sequence, Optional, Generic, TypeVar, Tuple, Dict
from enum import Enum
from .types import int32, int64
import json


class Condition(Enum):
    IN = ""
    EXACT_MATCH = "="
    NOT_MATCH = "!="
    IN_RANGE = " "
    MORE = ">"
    LESS = "<"
    MORE_OR_EQ = ">="
    LESS_OR_EQ = "<="


class Infix(Enum):
    OFF = "off"
    ALWAYS = "always"
    FALLBACK = "fallback"


class FilterExpression(BaseModel):
    __root__: List['AtomicFilterExpr']

    @validator("__root__", pre=True)
    def to_list(cls, v):
        if not isinstance(v, list):
            return [v]
        else:
            return v

    def __and__(self, other: 'FilterExpression'):
        if isinstance(other, FilterExpression):
            return FilterExpression(__root__=self.__root__ + other.__root__)
        if isinstance(other, AtomicFilterExpr):
            self.__root__.append(other)
            return self

    def to_sting(self):
        return "&".join(map(lambda a: a.to_string(), self.__root__))


numeric = [int, float, int32, int64]
numeric_union = Union[int, float, int32, int64]


class AtomicFilterExpr(BaseModel):
    column: 'FieldArgs'
    condition: Condition
    parameter: Any

    @root_validator(pre=True)
    def validate_condition(cls, values):
        if values.get("condition") in \
                [Condition.IN_RANGE, Condition.LESS, Condition.LESS_OR_EQ, Condition.MORE, Condition.MORE_OR_EQ] and \
                values.get("column").type not in [int64, int32, float]:
            raise ValueError("cannot use numeric comparison condition with non-numeric type")

        if values.get("condition") == Condition.IN:
            if values.get("column").type not in [str, Sequence[str]]:
                raise ValueError("in operator is supported only for string or string array fields.")

        if values.get("condition") == Condition.NOT_MATCH:
            if values.get("column").type != str and not values.get("column").facet:
                raise ValueError("Negation is supported only for str facet fields.")

        if values.get("condition") == Condition.EXACT_MATCH:
            if values.get("column").type not in [str, Sequence[str]]:
                raise ValueError("Exact match is supported only for strings")

        return values

    @root_validator
    def validate_parameter(cls, values):
        param = values.get("parameter")
        cond: Condition = values.get("condition")
        col: 'FieldArgs' = values.get("column")
        if cond in [Condition.LESS, Condition.LESS_OR_EQ, Condition.MORE, Condition.MORE_OR_EQ] and \
           type(param) not in numeric:
            raise ValueError("With numeric comparison parameter should be numeric")

        if cond == Condition.IN_RANGE:
            if type(param) in [list, tuple] and \
                    len(param) == 2 and \
                    type(param[0]) in numeric and \
                    type(param[1]) in numeric:
                pass
            else:
                raise ValueError("Incorrect parameter for in range condition")

        if cond == Condition.IN:
            if type(param) not in [list, tuple]:
                raise ValueError("in operator argument should be list or tuple")
            else:
                for item in param:
                    if type(item) != str:
                        raise ValueError("in operator parameter should contain only strings")

        if cond == Condition.NOT_MATCH:
            if type(param) != str:
                raise ValueError("Not match operator param should be string.")

        if cond == Condition.EXACT_MATCH:
            if type(param) != str:
                raise ValueError("Exact match parameter should be a string.")

        return values

    def __and__(self, other: Union['AtomicFilterExpr', FilterExpression]):
        if isinstance(other, FilterExpression):
            other.__root__.append(self)
            return other
        if isinstance(other, AtomicFilterExpr):
            return FilterExpression(__root__=[self, other])

    def to_string(self):
        return f"{self.column.name}: {self.condition.value} {self.parameter}"


FilterExpression.update_forward_refs()


class FieldArgs(BaseModel):
    name: str
    type: Any
    facet: bool = Field(False)
    index: bool = Field(False)
    infix: bool = Field(False)
    optional: bool = Field(False)

    def in_seq(self, arr: Sequence[Any]):
        return AtomicFilterExpr(column=self, condition=Condition.IN, parameter=arr)

    def __eq__(self, other):
        return AtomicFilterExpr(column=self, condition=Condition.EXACT_MATCH, parameter=other)

    def in_range(self, minimum, maximum):
        return AtomicFilterExpr(column=self, condition=Condition.IN_RANGE, parameter=(minimum, maximum))

    def __ne__(self, other):
        return AtomicFilterExpr(column=self, condition=Condition.NOT_MATCH, parameter=other)

    def __lt__(self, other):
        return AtomicFilterExpr(column=self, condition=Condition.LESS, parameter=other)

    def __le__(self, other):
        return AtomicFilterExpr(column=self, condition=Condition.LESS_OR_EQ, parameter=other)

    def __gt__(self, other):
        return AtomicFilterExpr(column=self, condition=Condition.MORE, parameter=other)

    def __ge__(self, other):
        return AtomicFilterExpr(column=self, condition=Condition.MORE_OR_EQ, parameter=other)


AtomicFilterExpr.update_forward_refs()


def json_dumper(v, *, default):

    return json.dumps(v)


class SearchQuery(BaseModel):
    q: str
    query_by: Sequence[FieldArgs]
    filter_by: Optional[FilterExpression]
    prefix: Optional[Union[bool, Sequence[bool]]]
    infix: Optional[Union[Infix, Sequence[Infix]]]
    split_join_tokens: bool = Field(False)
    pre_segmented_query: bool = Field(False)

    per_page: int = Field(10)

    facet_by: Optional[Sequence[FieldArgs]]
    max_facet_values: Optional[int]
    facet_query: Optional[Dict[FieldArgs, str]]
    facet_query_num_typos: Optional[int]

    @root_validator
    def validate_len(cls, v):
        if type(v.get("prefix")) in [list, tuple]:
            if len(v.get("prefix")) != len(v.get("query_by")):
                raise ValueError("prefix should be equal length with query_by")

        if type(v.get("infix")) in [list, tuple]:
            if len(v.get("prefix")) != len(v.get("query_by")):
                raise ValueError("infix should be equal length with query_by")

        return v

    @validator("facet_by")
    def validate_facet(cls, v):
        if v is None:
            return None

        for field_arg in v:
            if not field_arg.facet:
                raise ValueError(f"field {field_arg.name} is not facet")
        return v

    @validator("facet_query")
    def validate_facet_query(cls, v, values, **kwargs):
        if v is None:
            return None

        if "facet_by" not in values:
            raise ValueError("cannot specify facet_query when facet_by is not specified")

        for field_args in v.keys():
            if field_args not in values.get("facet_by"):
                raise ValueError(f"cannot make facet query on {field_args.name}: it's not specified in facet_by")

    def dict(self, *args, **kwargs) -> Dict[str, Any]:
        ret = super().dict(*args, **kwargs)
        ret["query_by"] = list(map(lambda field: field.name, self.query_by))
        if "filter_by" in ret:
            ret["filter_by"] = self.filter_by.to_sting()
        if "facet_by" in ret:
            ret["facet_by"] = list(map(lambda a: a.name, self.facet_by))
        if "facet_query" in ret:
            ret["facet_query"] = ",".join(map(lambda fargs, q: ":".join([fargs.name, q]), self.facet_query.items()))

        for k, v in ret.items():
            if isinstance(v, bool):
                if v:
                    ret[k] = "true"
                else:
                    ret[k] = "false"

        return ret

    class Config:
        pass
        #json_encoders = {FieldArgs: lambda field: field.name,
        #                 AtomicFilterExpr: lambda expr: f"{expr.column.name}{expr.condition}:{expr.parameter}",
        #                 Tuple[numeric_union, numeric_union]: lambda tup: f"[{tup[0]}..{tup[1]}]"}


class PaginatedQuery(SearchQuery):
    page: int = Field(1)


class RequestParams(BaseModel):
    collection_name: str
    per_page: int
    q: str


class Highlight(BaseModel):
    field: str
    snippet: str
    matched_tokens: Sequence[str]


class ArrayHighlight(BaseModel):
    field: str
    snippets: Sequence[str]
    indices: Sequence[int]
    matched_tokens: Sequence[Sequence[str]]


T = TypeVar("T", bound=BaseModel)


class Hit(GenericModel, Generic[T]):
    highlights: Sequence[Union[Highlight, ArrayHighlight]]
    document: T
    text_match: int


class Count(BaseModel):
    count: int
    highlighted: int
    value: int


class Stats(BaseModel):
    avg: float
    max: float
    min: float
    sum: float
    total_values: int


class FacetRes(BaseModel):
    counts: Sequence[Count]
    field_name: str
    stats: Stats


class SearchRes(GenericModel, Generic[T]):
    facet_counts: Sequence[FacetRes]
    found: int
    out_of: int
    page: int
    request_params: RequestParams
    search_time_ms: int
    hits: Sequence[Hit[T]]


