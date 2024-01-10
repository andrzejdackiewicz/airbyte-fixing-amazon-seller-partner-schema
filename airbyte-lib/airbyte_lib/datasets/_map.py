"""A generic interface for a set of streams."""

from collections.abc import Iterator, Mapping

from airbyte_lib.datasets._base import DatasetBase


class DatasetMap(Mapping):
    """A generic interface for a set of streams or datasets."""

    def __init__(self) -> None:
        self._datasets: dict[str, DatasetBase] = {}

    def __getitem__(self, key: str) -> DatasetBase:
        return self._datasets[key]

    def __iter__(self) -> Iterator[DatasetBase]:
        return iter(self._datasets)

    def __len__(self) -> int:
        return len(self._datasets)
