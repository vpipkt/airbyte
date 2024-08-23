# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

from abc import abstractmethod
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Set

from airbyte_cdk import StreamSlice
from airbyte_cdk.sources.declarative.async_job.job import AsyncJob
from airbyte_cdk.sources.declarative.requesters.requester import Requester


@dataclass
class AsyncJobRepository:
    create_job_requester: Requester
    # update_job_requester: Requester
    # download_job_requester: Requester

    @abstractmethod
    def start(self, stream_slice: StreamSlice) -> AsyncJob:
        pass

    @abstractmethod
    def update_jobs_status(self, jobs: Set[AsyncJob]) -> None:
        pass

    @abstractmethod
    def fetch_records(self, job: AsyncJob) -> Iterable[Mapping[str, Any]]:
        pass
