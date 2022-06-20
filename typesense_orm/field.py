from pydantic import Field as PydanticField
from typing import Sequence, _GenericAlias, get_origin, get_args, Any, Optional, Type, Dict
from .exceptions import UnsupportedTypeError
import pydantic
from pydantic.fields import Undefined, NoArgAnyCallable, FieldInfo
from .logging import logger
from .types import get_from_opt, geo, allowed_types


def Field(default: Any = Undefined,
          *,
          default_factory: Optional[NoArgAnyCallable] = None,
          facet=False,
          index=False,
          optional=True,
          default_sorting_field=False,
          infix=False):
    return PydanticField(default=default, default_factory=default_factory, facet=facet, index=index, optional=optional,
                         default_sorting_field=default_sorting_field, infix=infix)


class ModelField(pydantic.fields.ModelField):
    def _type_analysis(self) -> None:
        if self.model_config.typesense_mode:
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

            if field_type not in allowed_types.keys():
                raise UnsupportedTypeError(self.type_)

        super()._type_analysis()

