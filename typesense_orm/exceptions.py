class NoHealthyNode(Exception):
    pass


class UnsupportedTypeError(Exception):
    def __init__(self, unsupported_type):
        super().__init__(f"Type {unsupported_type} is not supported by typesense")


class InvalidFieldAnnotationType(Exception):
    def __init__(self, option, correct_type):
        super().__init__(f"Field annotation option {option} should be {correct_type}")


class MultipleSortingFields(Exception):
    def __init__(self):
        super().__init__(f"Two or more sorting fields are not allowed")


class InvalidSortingFieldType(Exception):
    def __init__(self, incorrect_type):
        super().__init__(f"Cannot sort on {incorrect_type} type field")


class ApiResponseNotOk(Exception):
    def __init__(self, response, status_code):
        self.response = response
        self.status_code = status_code
        super().__init__(f"Response not OK, status {status_code}, message {self.response}")
