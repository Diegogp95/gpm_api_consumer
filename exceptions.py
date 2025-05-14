

class GPMException(Exception):
    """Excepción base para el proyecto GPM."""
    pass

class InvertersNotFoundException(GPMException):
    """Excepción para cuando no se encuentra un inversor."""
    def __init__(self, message="No inverter found in the elements"):
        super().__init__(message)

class MeterNotFoundException(GPMException):
    """Excepción para cuando no se encuentra un medidor."""
    def __init__(self, message="No meter found in the elements"):
        super().__init__(message)

class StringsNotFoundException(GPMException):
    """Excepción para cuando no se encuentran strings."""
    def __init__(self, message="No strings found in the elements"):
        super().__init__(message)
