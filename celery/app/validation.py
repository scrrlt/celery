"Enhanced configuration validation for Celery.

This module provides high-quality validation for Celery configuration,
ensuring production safety and type integrity.
"

from __future__ import annotations

import os
from typing import Any, Callable, Final, TypeAlias, TYPE_CHECKING
from urllib.parse import urlparse

from celery.exceptions import ImproperlyConfigured
from celery.utils.log import get_logger

if TYPE_CHECKING:
    from celery.app.base import Celery

logger = get_logger(__name__)

# Type aliases
ConfigDict: TypeAlias = dict[str, Any]
ValidatorFunc: TypeAlias = Callable[[Any], Any]

class ValidationError(ImproperlyConfigured):
    """Configuration validation error."""
    pass

class SchemaField:
    """Definition of a validatable configuration field."""
    __slots__ = ("name", "type", "default", "validator")
    
    def __init__(
        self, 
        name: str, 
        type_: type, 
        default: Any = None, 
        validator: ValidatorFunc | None = None
    ):
        self.name = name
        self.type = type_
        self.default = default
        self.validator = validator

    def validate(self, value: Any) -> Any:
        if value is None:
            return self.default
        
        if not isinstance(value, self.type):
            try:
                value = self.type(value)
            except (ValueError, TypeError) as e:
                raise ValidationError(f"Field '{self.name}' must be {self.type.__name__}") from e
        
        if self.validator:
            value = self.validator(value)
        return value

def validate_broker_url(url: str) -> str:
    if not url:
        return ""
    parsed = urlparse(url)
    allowed = {"redis", "rediss", "amqp", "amqps", "sqs", "memory"}
    if parsed.scheme not in allowed:
        raise ValidationError(f"Unsupported broker scheme: {parsed.scheme}")
    return url

# Core configuration schema
CORE_SCHEMA: Final[list[SchemaField]] = [
    SchemaField("broker_url", str, validator=validate_broker_url),
    SchemaField("worker_concurrency", int, default=4),
    SchemaField("task_serializer", str, default="json"),
    SchemaField("result_backend", str),
    SchemaField("broker_connection_timeout", float, default=4.0),
]

def validate_config(config: ConfigDict) -> ConfigDict:
    """Validate and normalize configuration dictionary."""
    validated: ConfigDict = {}
    errors: list[str] = []
    
    schema_map = {f.name: f for f in CORE_SCHEMA}
    
    for key, value in config.items():
        if key in schema_map:
            try:
                validated[key] = schema_map[key].validate(value)
            except ValidationError as e:
                errors.append(str(e))
        else:
            # Pass-through unknown options
            validated[key] = value
            
    if errors:
        raise ImproperlyConfigured(f"Validation failed:\n" + "\n".join(errors))
        
    return validated

def setup_app_validation(app: Celery) -> None:
    """Inject validation logic into a Celery application."""
    original_update = app.conf.update
    
    def validated_update(obj: Any = None, **kwargs: Any) -> None:
        data = dict(obj or {}, **kwargs)
        validated = validate_config(data)
        original_update(validated)
        
    app.conf.update = validated_update
    logger.info("Enhanced configuration validation active")
