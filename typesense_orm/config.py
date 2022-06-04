from pydantic.config import BaseConfig as PydanticBaseConfig
from typing import Sequence, Optional


class BaseConfig(PydanticBaseConfig):
    token_separators: Sequence[str] = []
    symbols_to_index: Sequence[str] = []
    default_sorting_field: Optional[str] = "id"

