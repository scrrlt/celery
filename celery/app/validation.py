"""Configuration validation for Celery applications."""

from __future__ import annotations

import logging
import re
import functools
from typing import Any, Callable, Final, TYPE_CHECKING, Container, Iterable, Union

try:
    from typing import TypeAlias, final
except ImportError:
    from typing_extensions import TypeAlias, final

from celery.exceptions import ImproperlyConfigured
from celery.utils.log import get_logger

if TYPE_CHECKING:
    from celery.app.base import Celery

logger = get_logger(__name__)

# PEP 613: TypeAlias for backward compatibility with Python < 3.12
ValidatorFunc: TypeAlias = Callable[[Any, str], Any]

class ValidationError(ImproperlyConfigured):
    """Raised when a configuration value fails validation."""
    
    def __init__(self, message: str, option: str | None = None, value: Any = None) -> None:
        super().__init__(message)
        self.option = option
        self.value = value

@final
class OptionSchema:
    """Schema and validation for a Celery configuration setting.

    Attributes:
        name: Canonical Celery setting name.
        expected_type: Expected Python type or tuple of types.
        default: Fallback value if not provided.
        validator: Optional callable for complex logic.
    """
    __slots__ = ("name", "expected_type", "default", "validator")

    def __init__(
        self,
        name: str,
        expected_type: type | tuple[type, ...],
        default: Any = None,
        validator: ValidatorFunc | None = None
    ) -> None:
        self.name = name
        self.expected_type = expected_type
        self.default = default
        self.validator = validator

    def validate(self, value: Any) -> Any:
        """Coerce and validate a value against the schema.
        
        Raises:
            ValidationError: If type mismatch or logic fails.
        """
        if value is None:
            return self.default

        if not isinstance(value, self.expected_type):
            try:
                types = self.expected_type if isinstance(self.expected_type, tuple) else (self.expected_type,)
                
                if int in types:
                    value = int(value)
                elif float in types:
                    value = float(value)
                elif bool in types and isinstance(value, str):
                    # Strict boolean coercion to prevent silent misconfiguration.
                    normalized = value.lower()
                    if normalized in ("true", "1", "yes", "on"):
                        value = True
                    elif normalized in ("false", "0", "no", "off"):
                        value = False
                    else:
                        raise ValueError(f"Invalid boolean string: {value!r}")
            except (ValueError, TypeError) as exc:
                raise ValidationError(
                    f"Option {self.name!r} must be of type {self.expected_type!r}",
                    option=self.name,
                    value=value
                ) from exc

        if self.validator:
            return self.validator(value, self.name)
        
        return value

def validate_range(min_val: float | None = None, max_val: float | None = None) -> ValidatorFunc:
    """Create a numeric range validator."""
    def _check(value: Any, name: str) -> Any:
        if min_val is not None and value < min_val:
            raise ValidationError(f"{name!r} is below minimum {min_val}", name, value)
        if max_val is not None and value > max_val:
            raise ValidationError(f"{name!r} exceeds maximum {max_val}", name, value)
        return value
    return _check

@functools.lru_cache(maxsize=128)
def _compile_regex(pattern: str) -> re.Pattern:
    """Cache compiled regex objects."""
    return re.compile(pattern)

def validate_regex(pattern: str) -> ValidatorFunc:
    """Create a string regex validator."""
    regex = _compile_regex(pattern)
    def _check(value: Any, name: str) -> Any:
        values_to_check: Iterable[Any] = [value] if isinstance(value, str) else (value if isinstance(value, (list, tuple)) else [value])
        
        for val in values_to_check:
            if not regex.match(str(val)):
                raise ValidationError(f"{name!r} element {val!r} does not match required format", name, value)
        return value
    return _check

def validate_choice(choices: Container) -> ValidatorFunc:
    """Create a choice-based string validator."""
    def _check(value: Any, name: str) -> Any:
        if value not in choices:
            raise ValidationError(
                f"{name!r} must be one of {choices!r}",
                option=name,
                value=value
            )
        return value
    return _check

# Core system settings frequently misconfigured in production.
CELERY_CORE_SCHEMA: Final[dict[str, OptionSchema]] = {
    'broker_url': OptionSchema('broker_url', (str, list), validator=validate_regex(r'^(redis|rediss|amqp|amqps|sqs|memory|sentinel)')),
    'worker_concurrency': OptionSchema('worker_concurrency', int, default=4, validator=validate_range(1, 1000)),
    'task_serializer': OptionSchema('task_serializer', str, default='json', validator=validate_choice({'json', 'pickle', 'yaml', 'msgpack'})),
    'result_backend': OptionSchema('result_backend', str),
    'broker_connection_timeout': OptionSchema('broker_connection_timeout', (int, float), default=4.0),
    'worker_prefetch_multiplier': OptionSchema('worker_prefetch_multiplier', int, default=4, validator=validate_range(0)),
}

class ConfigurationValidator:
    """Application configuration validator."""
    
    def __init__(self, schema: dict[str, OptionSchema] | None = None) -> None:
        self.schema = schema or CELERY_CORE_SCHEMA
        self.errors: list[ValidationError] = []

    def validate(self, config: dict[str, Any]) -> dict[str, Any]:
        """Validate all known keys in the provided configuration.
        
        Returns:
            A new dictionary containing validated and coerced values.
        """
        self.errors.clear()
        validated: dict[str, Any] = config.copy()
        
        for key, schema in self.schema.items():
            if key in config:
                try:
                    validated[key] = schema.validate(config[key])
                except ValidationError as exc:
                    self.errors.append(exc)
                    logger.error("Configuration error: %s", exc)
                    
        return validated
