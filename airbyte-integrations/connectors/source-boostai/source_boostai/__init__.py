#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#


from .source import SourceBoostai

from .cowsay import say, say_dicts

from airbyte_cdk.sources.declarative.interpolation.filters import filters
from airbyte_cdk.sources.declarative.interpolation.macros import macros

# monkey patch cowsay into the filters
filters = filters.update({say.__name__: say})
macros = macros.update({say_dicts.__name__: say_dicts})

__all__ = ["SourceBoostai"]
