import pydantic
from .field import ModelField
from typing import Dict, ClassVar

pydantic.main.ModelField = ModelField


class BaseModel(pydantic.main.BaseModel):
    pass

