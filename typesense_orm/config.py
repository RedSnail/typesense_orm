from pydantic.config import BaseConfig as PydanticBaseConfig
from typing import Sequence, Optional


class BaseConfig(PydanticBaseConfig):
    token_separators: Sequence[str] = None
    symbols_to_index: Sequence[str] = None
    default_sorting_field: Optional[str] = None
    typesense_mode: bool = False

