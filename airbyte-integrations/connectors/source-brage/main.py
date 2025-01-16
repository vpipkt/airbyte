#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_brage import SourceBrage

if __name__ == "__main__":
    source = SourceBrage()
    launch(source, sys.argv[1:])
