from os import PathLike
from typing import IO, Self
from abc import ABC, abstractmethod

import pandas as pd

from endureio.datacomponents.base_data_components import CompositeData


class BaseFileHandler(ABC):
    """
    BaseFileHandler is an abstract base class that
    provides a blueprint for file handling operations.
    It defines the structure and methods that subclasses
    must implement to handle specific file formats
    or data sources. The class includes methods for reading,
    writing, validating, and converting data.

    Attributes:
        file_path_or_buffer (str | bytes | PathLike | IO[bytes]):
        The file path or buffer to be handled.
        data (CompositeData): A composite data structure
        that stores metadata components extracted
            from the file or buffer.

    Methods:
        read() -> Self:
            Reads the file or buffer and processes its contents.
            This method must be implemented
            by subclasses to populate the `data` attribute
            with metadata components.

        write() -> Self:
            Writes data to a file. Subclasses must implement
            this method to handle the specific
            logic for writing data to a file.

        validate() -> Self:
            Validates the file or buffer. Subclasses must
            implement this method to ensure the
            file or buffer meets the required criteria.

        to_df() -> pd.DataFrame:
            Converts the `data` attribute to a pandas DataFrame.

        to_dict() -> dict:
            Converts the `data` attribute to a dictionary.
    """

    def __init__(self, file_path_or_buffer: str | bytes | PathLike | IO[bytes]):
        self.file_path_or_buffer = file_path_or_buffer
        self.data: CompositeData
        pass

    @abstractmethod
    def read(self) -> Self:
        """ "
        Reads the file or buffer and processes its contents.

        This method creates a `Data` component for each section of the file
        (e.g., header, etc.) and adds them to the `data` attribute. The `data`
        attribute is a `CompositeData` object that contains all the metadata
        components.

        Returns:
            Self: The base file handler object with the `data` attribute populated.
        """
        pass

    @abstractmethod
    def write(self) -> Self:
        """
        Writes data to a file.

        This method should be implemented by subclasses to handle the
        specific logic for writing data to a file. The implementation
        should ensure that the file is written correctly and any necessary
        resources are properly managed.

        Returns:
            Self: The instance of the class, to allow for method chaining.
        """
        pass

    @abstractmethod
    def validate(self) -> Self:
        """
        Validates the file or buffer.

        This method is intended to ensure that the file or buffer meets
        the required criteria or format. Override this method in a subclass
        to implement specific validation logic.

        Returns:
            Self: The instance of the class, allowing for method chaining.
        """
        pass

    def to_df(self) -> pd.DataFrame:
        """ "
        Converts the `data` attribute of the object into a pandas DataFrame.

        Returns:
            pd.DataFrame: A DataFrame representation of the `data` attribute.
        """

        return pd.DataFrame(self.data.get_data())

    def to_dict(self) -> dict:
        """
        convert the `data` attribute of the object into a python dict

        Returns:
            dict: A Dictonary representation of the `data` attribute.
        """
        return self.data.get_data()
