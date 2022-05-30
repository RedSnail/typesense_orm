class NoHealthyNode(Exception):
    pass


class UnsupportedTypeError(Exception):
    def __init__(self, unsupported_type):
        super().__init__(f"Type {unsupported_type} is not supported by typesense")


class InvalidFieldAnnotationType(Exception):
    def __init__(self, option, correct_type):
        super().__init__(f"Field annotation option {option} should be {correct_type}")


class InvalidSortingField(Exception):
    def __init__(self, field):
        super().__init__(f"There is no field called {field}")


class InvalidSortingFieldType(Exception):
    def __init__(self, incorrect_type):
        super().__init__(f"Cannot sort on {incorrect_type} type field")