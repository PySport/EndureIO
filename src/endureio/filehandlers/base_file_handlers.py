from os import PathLike
from typing import IO
from abc import ABC, abstractmethod

import pandas as pd

from endureio.datacomponents.base_data_components import CompositeData


class BaseFileHandler(ABC):
    """ """

    def __init__(self, file_path_or_buffer: str | bytes | PathLike | IO[bytes]):
        self.file_path_or_buffer = file_path_or_buffer
        self.data: CompositeData
        pass

    @abstractmethod
    def read(self):
        """
        reads file or buffer

        creates a Data component for each section of the file (e.g. header, , etc.)
        And adds them to the data attribute.
        The data attribute is a CompositeData object that contains all
        the metadata components.

        returns base file handler object with data attribute populated.
        """
        pass

    @abstractmethod
    def write(Self):
        """write file"""
        pass

    @abstractmethod
    def validate(self):
        """validate the file or buffer"""
        pass

    def to_df(self) -> pd.DataFrame:
        """
        convert the data attribute to a pandas DataFrame
        """
        return pd.DataFrame(self.data.get_data())

    def to_dict(self) -> dict:
        """
        convert the data attribute to a dictionary
        """
        return self.data.get_data()
