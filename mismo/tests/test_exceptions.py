from __future__ import annotations

import inspect

from mismo import exceptions
from mismo.exceptions import MismoError, MismoWarning


def test_mismo_error_inheritance():
    """Test that MismoError is a base Exception."""
    assert issubclass(MismoError, Exception)


def test_mismo_warning_inheritance():
    """Test that MismoWarning is a base Warning."""
    assert issubclass(MismoWarning, Warning)


def test_all_errors_inherit_from_mismo_error():
    """Test that all exception classes inherit from MismoError."""
    error_classes = [
        cls
        for name, cls in inspect.getmembers(exceptions, inspect.isclass)
        if cls.__module__ == exceptions.__name__
        and issubclass(cls, Exception)
        and not issubclass(cls, Warning)  # Exclude warnings
        and cls is not MismoError
    ]
    assert len(error_classes) > 0, "No error classes found in mismo.exceptions"
    for error_class in error_classes:
        assert issubclass(error_class, MismoError)


def test_all_warnings_inherit_from_mismo_warning():
    """Test that all warning classes inherit from MismoWarning."""
    warning_classes = [
        cls
        for name, cls in inspect.getmembers(exceptions, inspect.isclass)
        if cls.__module__ == exceptions.__name__
        and issubclass(cls, Warning)
        and cls is not MismoWarning
    ]
    assert len(warning_classes) > 0, "No warning classes found in mismo.exceptions"
    for warning_class in warning_classes:
        assert issubclass(warning_class, MismoWarning)
