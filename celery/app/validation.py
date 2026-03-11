"""Configuration validation for Celery applications."""

from __future__ import annotations

import logging
import re
import functools
import types
from typing import Any, Callable, Final, TYPE_CHECKING, Container, Iterable, Union, get_args, get_origin

try:
    from typing import TypeAlias, final
except ImportError:
    from typing_extensions import TypeAlias, final

from celery.exceptions import ImproperlyConfigured
from celery.utils.log import get_logger

if TYPE_CHECKING:
    from celery.app.base import Celery

logger = get_logger(__name__)

# PEP 613: TypeAlias for backward compatibility.
ValidatorFunc: TypeAlias = Callable[[Any, str], Any]

class ValidationError(ImproperlyConfigured):
    """Raised when a configuration value fails validation."""
    
    def __init__(self, message: str, option: str | None = None, value: Any = None) -> None:
        """Initialize validation error with context.
        
        Args:
            message: Human-readable error description.
            option: Configuration option name that failed.
            value: Invalid value that caused the failure.
        """
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
        expected_type: Any,
        default: Any = None,
        validator: ValidatorFunc | None = None
    ) -> None:
        """Initialize option schema definition.
        
        Args:
            name: Canonical Celery configuration setting name.
            expected_type: Expected Python type or Union of types.
            default: Default value when option is not provided.
            validator: Optional custom validation function.
        """
        self.name = name
        
        # Use standard library introspection to safely unwrap Unions/PEP 604 types.
        origin = get_origin(expected_type)
        if origin in (Union, getattr(types, 'UnionType', None)):
            self.expected_type = get_args(expected_type)
        elif hasattr(expected_type, "__args__"):
            self.expected_type = expected_type.__args__
        else:
            self.expected_type = expected_type
            
        self.default = default
        self.validator = validator

    def validate(self, value: Any) -> Any:
        """Coerce and validate a value against the schema.
        
        Uses optimized type-specific coercion instead of retry loops.
        
        Raises:
            ValidationError: If type mismatch or logic fails.
        """
        if value is None:
            return self.default

        if not isinstance(value, self.expected_type):
            types = self.expected_type if isinstance(self.expected_type, tuple) else (self.expected_type,)
            
            # Direct type-specific coercion (more efficient than retry loop)
            coerced_value = None
            coercion_error = None
            
            # Try the most likely type conversions first (int before bool for env vars)
            if isinstance(value, str):
                # String-to-type coercion (common for env vars and config files)
                if int in types:
                    try:
                        coerced_value = int(value)
                    except ValueError as e:
                        coercion_error = e
                        
                if coerced_value is None and bool in types:
                    norm = value.lower()
                    if norm in ("true", "1", "yes", "on"):
                        coerced_value = True
                    elif norm in ("false", "0", "no", "off"):
                        coerced_value = False
                        
                if coerced_value is None and float in types:
                    try:
                        coerced_value = float(value)
                    except ValueError as e:
                        coercion_error = e
            
            elif isinstance(value, (int, float)):
                # Numeric type widening
                if float in types and isinstance(value, int):
                    coerced_value = float(value)
                elif int in types and isinstance(value, float) and value.is_integer():
                    coerced_value = int(value)
            
            if coerced_value is not None:
                value = coerced_value
            else:
                raise ValidationError(
                    f"Option {self.name!r} must be of type {self.expected_type!r}. "
                    f"Failed to coerce {type(value).__name__} value {value!r}."
                    + (f" Last error: {coercion_error}" if coercion_error else ""),
                    option=self.name,
                    value=value
                )

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

def validate_regex(pattern: str) -> ValidatorFunc:
    """Create a string regex validator."""
    # Pre-compile at factory level to avoid O(N) overhead in validation paths.
    regex = re.compile(pattern)
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

# Enhanced telemetry settings for production monitoring.
CELERY_TELEMETRY_SCHEMA: Final[dict[str, OptionSchema]] = {
    'worker_telemetry_enabled': OptionSchema('worker_telemetry_enabled', bool, default=False),
    'worker_telemetry_collection_interval_s': OptionSchema('worker_telemetry_collection_interval_s', (int, float), default=60.0, validator=validate_range(1.0, 3600.0)),
    'worker_telemetry_http_port': OptionSchema('worker_telemetry_http_port', int, default=9808, validator=validate_range(1024, 65535)),
    'worker_telemetry_port_range': OptionSchema('worker_telemetry_port_range', int, default=10, validator=validate_range(1, 100)),
    'worker_telemetry_deterministic_port': OptionSchema('worker_telemetry_deterministic_port', bool, default=False),
    'worker_telemetry_alert_queue_depth_threshold': OptionSchema('worker_telemetry_alert_queue_depth_threshold', int, default=1000, validator=validate_range(1, 10000)),
    'worker_telemetry_alert_latency_threshold_ms': OptionSchema('worker_telemetry_alert_latency_threshold_ms', (int, float), default=5000.0, validator=validate_range(100.0)),
}

class ConfigurationValidator:
    """Application configuration validator."""
    
    def __init__(self, schema: dict[str, OptionSchema] | None = None, warning_only: bool = False) -> None:
        """Initialize configuration validator.
        
        Args:
            schema: Optional custom schema; defaults to core Celery settings.
            warning_only: If True, log warnings instead of raising exceptions.
        """
        self.schema = schema or CELERY_CORE_SCHEMA
        self.errors: list[ValidationError] = []
        self.warning_only = warning_only

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
                    if self.warning_only:
                        logger.warning("Configuration warning: %s", exc)
                        # Keep original value for backward compatibility
                        validated[key] = config[key]
                    else:
                        logger.error("Configuration error: %s", exc)
                        # Still raise in strict mode
                    
        return validated
