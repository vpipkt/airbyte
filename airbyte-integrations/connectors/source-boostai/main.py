#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_boostai import SourceBoostai

if __name__ == "__main__":
    source = SourceBoostai()
    launch(source, sys.argv[1:])
