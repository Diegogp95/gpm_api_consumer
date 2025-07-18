

class GPMException(Exception):
    """Base Exception for GPM API errors."""
    pass

class InvertersNotFoundException(GPMException):
    """Exception for when no inverters are found."""
    def __init__(self, message="No inverter found in the elements"):
        super().__init__(message)

class MeterNotFoundException(GPMException):
    """Exception for when no meters are found."""
    def __init__(self, message="No meter found in the elements"):
        super().__init__(message)

class StringsNotFoundException(GPMException):
    """Exception for when no strings are found."""
    def __init__(self, message="No strings found in the elements"):
        super().__init__(message)

class PlantNotFoundException(GPMException):
    """Exception for when no plant is found."""
    def __init__(self, plant_id=None, safe_name=None):
        if plant_id is not None:
            message = f"No plant found with id={plant_id}"
        elif safe_name is not None:
            message = f"No plant found with safe_name='{safe_name}'"
        else:
            message = "No plant found."
        super().__init__(message)
        self.plant_id = plant_id
        self.safe_name = safe_name

class GPMDataRetrievalException(GPMException):
    """Exception for errors during data retrieval from GPM API."""
    def __init__(self, message="Error retrieving data from GPM API"):
        super().__init__(message)