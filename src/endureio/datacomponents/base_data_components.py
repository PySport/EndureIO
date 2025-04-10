from abc import ABC, abstractmethod


# --- Component ---
class DataComponent(ABC):
    """
    DataComponent is an abstract base class that serves as the
    foundation for all metadata components.
    It enforces the implementation of a `get_data` method in
    derived classes and provides default
    implementations for `add` and `remove` methods,
    which raise `NotImplementedError` to indicate
    that the component does not support child components.

    Methods:
        get_data() -> dict:
            Abstract method that must be implemented by subclasses to
            return the data associated with the component as a dictionary.

        add(component: "DataComponent"):
            Raises NotImplementedError. Intended to be overridden
            by subclasses that support adding child components.

        remove(component: "DataComponent"):
            Raises NotImplementedError. Intended to be overridden by subclasses that
            support removing child components.
    """

    @abstractmethod
    def get_data(self) -> dict:
        pass

    def add(self, component: "DataComponent"):
        raise NotImplementedError("This component doesn't support children.")

    def remove(self, component: "DataComponent"):
        raise NotImplementedError("This component doesn't support children.")


# --- Composite ---
class CompositeData(DataComponent):
    """
    CompositeData is a class that represents a composite metadata component,
    which can hold and manage other DataComponent instances as its children.

    Attributes:
        name (str): The name of the composite data component.
        Defaults to an empty string. children (list[DataComponent]):
        A list of child DataComponent instances contained within this composite.

    Methods:
        add(component: DataComponent):
            Adds a child DataComponent to the composite.

        remove(component: DataComponent):
            Removes a child DataComponent from the composite.

        get_data() -> dict:
            Aggregates and returns the data from all child components as a dictionary.
            If the composite has a name,
            the result is nested under the composite's name.
    """

    def __init__(self, name: str = ""):
        self.name = name
        self.children: list[DataComponent] = []

    def add(self, component: DataComponent):
        """
        Adds a DataComponent to the list of children.

        Args:
            component (DataComponent):
            The data component to be added to the children list.
        """
        self.children.append(component)

    def remove(self, component: DataComponent):
        """
        Removes a specified DataComponent from the list of children.

        Args:
            component (DataComponent):
            The data component to be removed from the children list.

        Raises:
            ValueError: If the specified component is not found in the children list.
        """
        self.children.remove(component)

    def get_data(self) -> dict:
        """
        Retrieves the data from the current object and
        its children in a hierarchical structure.

        Returns:
            dict: A dictionary containing the data from
                  the current object and its children.
                  If the current object has a name,
                  the data is nested under the name as the key.
                  Otherwise, the data is returned as a flat dictionary.
        """
        result = {}
        for child in self.children:
            result.update(child.get_data())
        return {self.name: result} if self.name else result


# --- Example Usage for a FIT file ---
if __name__ == "__main__":
    # Example Data Component
    class ExampleData(DataComponent):
        """
        Represents a leaf metadata component (key-value pair or similar).
        The leaf methods should be the low level fields of the different
        files.
        """

        def __init__(self, key: str, value: str):
            self.key = key
            self.value = value

        def get_metadata(self) -> dict:
            return {self.key: self.value}

    # Example usage
    # Build metadata structure
    fit_metadata = CompositeData("fit_file")

    # Header Section
    header = CompositeData("header")
    header.add(ExampleData("file_type", "FIT"))
    header.add(ExampleData("version", "2.0"))

    # Body Section
    body = CompositeData("body")
    # add the training data
    body.add(ExampleData("duration", "45 min"))
    body.add(ExampleData("distance", "10 km"))

    # Add sections to root
    fit_metadata.add(header)
    fit_metadata.add(body)

    print(fit_metadata.get_metadata())
