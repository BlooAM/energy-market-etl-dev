from typing import Type, Iterable


def __class_name(python_class: Type):
    return python_class.__name__


def class_names(python_classes: [Type, Iterable[Type]]):
    if not isinstance(python_classes, Iterable):
        python_classes = [python_classes]
    return tuple(map(__class_name, python_classes))
