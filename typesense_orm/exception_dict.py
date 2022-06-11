from asyncio import Task
from typing import Dict, List, Callable, Any
from .exceptions import TaskNotDoneException
from collections.abc import Mapping
from dataclasses import dataclass
from collections import UserDict
from queue import SimpleQueue

@dataclass
class task_data:
    __slots__ = ("task", "has_error")
    task: Task
    has_error: bool


class ExceptionDict(UserDict):
    def __init__(self, tasks: Dict[str, Task], exception_handler: Callable[[Callable], Any] = lambda r: r()):
        super().__init__()
        self.exception_handler = exception_handler
        for key, task in tasks.items():
            if not task.done():
                raise TaskNotDoneException(key)

            self.data[key] = task_data(task, isinstance(task.exception(), Exception))

    def __getitem__(self, item) -> Any:
        ret = self.data[item]
        if ret.has_error:
            def raiser():
                raise ret.task.exception()
            self.exception_handler(raiser)
        else:
            return ret.task.result()

    def __iter__(self):
        with_exc = List[Task]()
        for k, v in self.data.items():
            if v.has_error:
                with_exc.append(v.task)
            else:
                yield k, v.task.result()

        for task in with_exc:
            def raiser():
                raise task.exception()

            self.exception_handler(raiser)


