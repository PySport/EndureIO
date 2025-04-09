from abc import ABC, abstractmethod


# --- Component ---
class DataComponent(ABC):
    """
    Base class for all metadata components.
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
    Represents a composite metadata component (can hold other components).
    """

    def __init__(self, name: str = ""):
        self.name = name
        self.children: list[DataComponent] = []

    def add(self, component: DataComponent):
        self.children.append(component)

    def remove(self, component: DataComponent):
        self.children.remove(component)

    def get_data(self) -> dict:
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
