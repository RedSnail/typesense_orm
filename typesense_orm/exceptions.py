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


class TaskNotDoneException(Exception):
    def __init__(self, name):
        super().__init__(f"task {name} in not done, cannot put undone task to exc_dict")


class NotOptional(Exception):
    def __init__(self, field_name):
        super().__init__(f"Field {field_name} is not optional and not indexed. All not indexed fields should be optional")


class CollectionUnregistered(Exception):
    def __init__(self, collection_name: str):
        super().__init__(f"collection {collection_name} is not registered")
