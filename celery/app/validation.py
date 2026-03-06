"""Enhanced configuration validation for Celery.

This module provides Pydantic-inspired validation for Celery configuration options,
adding type safety, range validation, and comprehensive error reporting while
maintaining backward compatibility with existing Option-based configuration.

Key features:
- Strict type validation with automatic coercion
- Range and constraint validation for numeric values
- URL and connection string validation
- Backward-compatible integration with existing Celery configurations
- Detailed validation error reporting for operations teams
- Environment variable validation and sanitization

Validation rules are designed for production safety, preventing common
misconfigurations that could impact system stability or security.
"""

import os
import re
import warnings
from typing import Any, Callable, Dict, List, Optional, Type, Union
from urllib.parse import urlparse

from celery.exceptions import ImproperlyConfigured
from celery.utils.log import get_logger

logger = get_logger(__name__)


class ValidationError(ImproperlyConfigured):
    """Configuration validation error with detailed context."""
    
    def __init__(self, field_name: str, value: Any, message: str, suggestion: Optional[str] = None):
        self.field_name = field_name
        self.value = value
        self.suggestion = suggestion
        
        error_msg = f"Configuration validation failed for '{field_name}': {message}"
        if suggestion:
            error_msg += f" Suggestion: {suggestion}"
        
        super().__init__(error_msg)


class EnhancedOption:
    """Enhanced configuration option with comprehensive validation.
    
    Provides Pydantic-inspired validation while maintaining compatibility
    with Celery's existing Option class. Adds production-grade validation
    including type checking, range validation, and security constraints.
    """
    
    def __init__(
        self,
        default: Any = None,
        type_: Type = str,
        description: str = "",
        min_value: Optional[Union[int, float]] = None,
        max_value: Optional[Union[int, float]] = None,
        regex_pattern: Optional[str] = None,
        choices: Optional[List[Any]] = None,
        validator: Optional[Callable[[Any], Any]] = None,
        security_sensitive: bool = False,
        deprecated: bool = False,
        deprecated_message: Optional[str] = None,
        required: bool = False,
        env_var: Optional[str] = None,
        **kwargs
    ):
        """Initialize enhanced configuration option.
        
        Args:
            default: Default value if not specified
            type_: Expected value type (int, float, str, bool, list, dict)
            description: Human-readable description for documentation
            min_value: Minimum value for numeric types
            max_value: Maximum value for numeric types
            regex_pattern: Regex pattern for string validation
            choices: List of allowed values
            validator: Custom validation function
            security_sensitive: Mark as sensitive for logging redaction
            deprecated: Mark option as deprecated
            deprecated_message: Custom deprecation message
            required: Whether option is required (no default)
            env_var: Environment variable name to read from
        """
        self.default = default
        self.type_ = type_
        self.description = description
        self.min_value = min_value
        self.max_value = max_value
        self.regex_pattern = regex_pattern
        self.choices = choices
        self.validator = validator
        self.security_sensitive = security_sensitive
        self.deprecated = deprecated
        self.deprecated_message = deprecated_message
        self.required = required
        self.env_var = env_var
        
        # Compile regex pattern for performance
        self._compiled_regex = None
        if self.regex_pattern:
            try:
                self._compiled_regex = re.compile(self.regex_pattern)
            except re.error as e:
                logger.warning("Invalid regex pattern for option: %s", e)
    
    def validate_and_convert(self, value: Any, field_name: str) -> Any:
        """Validate and convert value with comprehensive error reporting.
        
        Args:
            value: Raw configuration value to validate
            field_name: Name of configuration field for error reporting
            
        Returns:
            Validated and converted value
            
        Raises:
            ValidationError: If validation fails with detailed context
        """
        # Handle deprecation warnings
        if self.deprecated:
            message = self.deprecated_message or f"Configuration option '{field_name}' is deprecated"
            warnings.warn(message, DeprecationWarning, stacklevel=3)
        
        # Handle required fields
        if self.required and (value is None or value == ""):
            raise ValidationError(
                field_name, value,
                "is required but not provided",
                "Provide a valid value or check environment variables"
            )
        
        # Use default if no value provided
        if value is None:
            value = self.default
        
        # Skip validation for None values (unless required)
        if value is None:
            return value
        
        # Type conversion with detailed error handling
        try:
            converted_value = self._convert_type(value, field_name)
        except (ValueError, TypeError) as e:
            raise ValidationError(
                field_name, value,
                f"failed type conversion to {self.type_.__name__}: {e}",
                f"Expected {self.type_.__name__}, got {type(value).__name__}"
            )
        
        # Range validation for numeric types
        if self.type_ in (int, float) and converted_value is not None:
            self._validate_numeric_range(converted_value, field_name)
        
        # String pattern validation
        if self.type_ == str and converted_value and self._compiled_regex:
            self._validate_string_pattern(converted_value, field_name)
        
        # Choice validation
        if self.choices and converted_value not in self.choices:
            raise ValidationError(
                field_name, converted_value,
                f"value not in allowed choices: {self.choices}",
                f"Use one of: {', '.join(str(c) for c in self.choices)}"
            )
        
        # Custom validation
        if self.validator:
            try:
                converted_value = self.validator(converted_value)
            except Exception as e:
                raise ValidationError(
                    field_name, converted_value,
                    f"custom validation failed: {e}",
                    "Check the value format and constraints"
                )
        
        return converted_value
    
    def _convert_type(self, value: Any, field_name: str) -> Any:
        """Convert value to target type with error handling."""
        if self.type_ == bool:
            return self._convert_bool(value, field_name)
        elif self.type_ in (list, tuple):
            return self._convert_sequence(value, field_name)
        elif self.type_ == dict:
            return self._convert_dict(value, field_name)
        else:
            return self.type_(value)
    
    def _convert_bool(self, value: Any, field_name: str) -> bool:
        """Convert value to boolean with support for string representations."""
        if isinstance(value, bool):
            return value
        elif isinstance(value, str):
            lower_val = value.lower()
            if lower_val in ('true', '1', 'yes', 'on', 'enabled'):
                return True
            elif lower_val in ('false', '0', 'no', 'off', 'disabled'):
                return False
            else:
                raise ValueError(f"Cannot convert '{value}' to boolean")
        elif isinstance(value, (int, float)):
            return bool(value)
        else:
            raise TypeError(f"Cannot convert {type(value).__name__} to boolean")
    
    def _convert_sequence(self, value: Any, field_name: str) -> Union[List, tuple]:
        """Convert value to list or tuple."""
        if isinstance(value, (list, tuple)):
            result = list(value) if self.type_ == list else tuple(value)
            return result
        elif isinstance(value, str):
            # Parse comma-separated string
            items = [item.strip() for item in value.split(',') if item.strip()]
            return items if self.type_ == list else tuple(items)
        else:
            # Wrap single value in sequence
            result = [value] if self.type_ == list else (value,)
            return result
    
    def _convert_dict(self, value: Any, field_name: str) -> Dict:
        """Convert value to dictionary."""
        if isinstance(value, dict):
            return value
        elif isinstance(value, str):
            # Simple key=value parsing (basic implementation)
            if not value.strip():
                return {}
            try:
                # Handle basic key=value,key=value format
                pairs = [pair.split('=', 1) for pair in value.split(',')]
                return {key.strip(): val.strip() for key, val in pairs if len(pair) == 2}
            except Exception:
                raise ValueError(f"Cannot parse dictionary from string: {value}")
        else:
            raise TypeError(f"Cannot convert {type(value).__name__} to dict")
    
    def _validate_numeric_range(self, value: Union[int, float], field_name: str) -> None:
        """Validate numeric value is within specified range."""
        if self.min_value is not None and value < self.min_value:
            raise ValidationError(
                field_name, value,
                f"value {value} below minimum {self.min_value}",
                f"Use a value >= {self.min_value}"
            )
        
        if self.max_value is not None and value > self.max_value:
            raise ValidationError(
                field_name, value,
                f"value {value} above maximum {self.max_value}",
                f"Use a value <= {self.max_value}"
            )
    
    def _validate_string_pattern(self, value: str, field_name: str) -> None:
        """Validate string matches required pattern."""
        if not self._compiled_regex.match(value):
            raise ValidationError(
                field_name, value,
                f"does not match required pattern: {self.regex_pattern}",
                "Check the format requirements in documentation"
            )


# Enhanced configuration options with production-grade validation

BROKER_URL_OPTION = EnhancedOption(
    type_=str,
    description="Broker connection URL",
    validator=lambda url: validate_broker_url(url),
    security_sensitive=True,
    env_var="CELERY_BROKER_URL"
)

BROKER_CONNECTION_TIMEOUT_OPTION = EnhancedOption(
    default=4,
    type_=float,
    description="Broker connection timeout in seconds", 
    min_value=0.1,
    max_value=300.0,
    env_var="CELERY_BROKER_CONNECTION_TIMEOUT"
)

BROKER_CONNECTION_RETRY_OPTION = EnhancedOption(
    default=True,
    type_=bool,
    description="Enable broker connection retries",
    env_var="CELERY_BROKER_CONNECTION_RETRY"
)

BROKER_CONNECTION_MAX_RETRIES_OPTION = EnhancedOption(
    default=100,
    type_=int,
    description="Maximum broker connection retries",
    min_value=0,
    max_value=1000,
    env_var="CELERY_BROKER_CONNECTION_MAX_RETRIES"
)

BROKER_HEARTBEAT_OPTION = EnhancedOption(
    default=120,
    type_=int, 
    description="Broker heartbeat interval in seconds",
    min_value=10,
    max_value=3600,
    env_var="CELERY_BROKER_HEARTBEAT"
)

WORKER_CONCURRENCY_OPTION = EnhancedOption(
    type_=int,
    description="Number of worker processes/threads",
    min_value=1,
    max_value=1000,
    validator=lambda x: validate_worker_concurrency(x),
    env_var="CELERY_WORKER_CONCURRENCY"
)

WORKER_POOL_OPTION = EnhancedOption(
    default="prefork",
    type_=str,
    description="Worker pool implementation",
    choices=["prefork", "eventlet", "gevent", "threads", "solo"],
    env_var="CELERY_WORKER_POOL"
)

WORKER_MAX_TASKS_PER_CHILD_OPTION = EnhancedOption(
    type_=int,
    description="Maximum tasks per worker child process",
    min_value=1,
    max_value=100000,
    env_var="CELERY_WORKER_MAX_TASKS_PER_CHILD"
)

WORKER_MAX_MEMORY_PER_CHILD_OPTION = EnhancedOption(
    type_=int,
    description="Maximum memory per worker child (KB)",
    min_value=1024,  # 1MB minimum
    max_value=16777216,  # 16GB maximum  
    env_var="CELERY_WORKER_MAX_MEMORY_PER_CHILD"
)

TASK_TIME_LIMIT_OPTION = EnhancedOption(
    type_=float,
    description="Hard task time limit in seconds",
    min_value=0.1,
    max_value=86400.0,  # 24 hours max
    env_var="CELERY_TASK_TIME_LIMIT"
)

TASK_SOFT_TIME_LIMIT_OPTION = EnhancedOption(
    type_=float,
    description="Soft task time limit in seconds",
    min_value=0.1,
    max_value=86400.0,  # 24 hours max
    env_var="CELERY_TASK_SOFT_TIME_LIMIT"
)

TASK_SERIALIZER_OPTION = EnhancedOption(
    default="json",
    type_=str,
    description="Default task serialization format",
    choices=["json", "pickle", "yaml", "msgpack"],
    env_var="CELERY_TASK_SERIALIZER"
)

RESULT_BACKEND_OPTION = EnhancedOption(
    type_=str,
    description="Result backend URL",
    validator=lambda url: validate_result_backend_url(url),
    security_sensitive=True,
    env_var="CELERY_RESULT_BACKEND"
)

RESULT_EXPIRES_OPTION = EnhancedOption(
    default=3600,
    type_=int,
    description="Result expiration time in seconds",
    min_value=60,  # 1 minute minimum
    max_value=2592000,  # 30 days maximum
    env_var="CELERY_RESULT_EXPIRES"
)


def validate_broker_url(url: str) -> str:
    """Validate broker URL format and security.
    
    Args:
        url: Broker connection URL
        
    Returns:
        Validated URL
        
    Raises:
        ValidationError: If URL is invalid or insecure
    """
    if not url:
        raise ValueError("Broker URL cannot be empty")
    
    try:
        parsed = urlparse(url)
    except Exception as e:
        raise ValueError(f"Invalid URL format: {e}")
    
    # Validate scheme
    valid_schemes = {
        'redis', 'rediss',  # Redis
        'amqp', 'amqps',    # RabbitMQ
        'sqs',              # Amazon SQS
        'memory',           # In-memory
        'mongodb',          # MongoDB
        'sqla',             # SQLAlchemy
    }
    
    if parsed.scheme not in valid_schemes:
        raise ValueError(
            f"Unsupported broker scheme: {parsed.scheme}. "
            f"Supported: {', '.join(valid_schemes)}"
        )
    
    # Security validation for production
    if parsed.scheme in ('redis', 'amqp', 'mongodb') and not parsed.hostname:
        logger.warning(
            "Broker URL missing hostname - ensure this is not production"
        )
    
    # Warn about insecure connections in production
    insecure_schemes = {'redis', 'amqp', 'mongodb'}
    if parsed.scheme in insecure_schemes and os.getenv('CELERY_ENV') == 'production':
        logger.warning(
            "Using insecure broker connection in production environment. "
            "Consider using TLS (rediss://, amqps://)"
        )
    
    return url


def validate_result_backend_url(url: str) -> str:
    """Validate result backend URL format.
    
    Args:
        url: Result backend URL
        
    Returns:
        Validated URL
        
    Raises:
        ValidationError: If URL is invalid
    """
    if not url:
        raise ValueError("Result backend URL cannot be empty")
    
    try:
        parsed = urlparse(url)
    except Exception as e:
        raise ValueError(f"Invalid URL format: {e}")
    
    valid_schemes = {
        'redis', 'rediss',
        'db+postgresql', 'db+mysql', 'db+sqlite',
        'mongodb',
        'cache',
        's3',
        'file'
    }
    
    # Handle SQLAlchemy URLs with db+ prefix
    scheme = parsed.scheme
    if scheme.startswith('db+'):
        scheme = 'db+' + scheme[3:].split('+')[0]
    
    if scheme not in valid_schemes:
        raise ValueError(
            f"Unsupported result backend: {parsed.scheme}. "
            f"Supported: {', '.join(valid_schemes)}"
        )
    
    return url


def validate_worker_concurrency(concurrency: int) -> int:
    """Validate worker concurrency setting.
    
    Args:
        concurrency: Number of worker processes
        
    Returns:
        Validated concurrency value
        
    Raises:
        ValidationError: If concurrency is inappropriate for system
    """
    import multiprocessing
    
    if concurrency <= 0:
        raise ValueError("Concurrency must be positive")
    
    # Get available CPU cores
    try:
        cpu_count = multiprocessing.cpu_count()
    except NotImplementedError:
        cpu_count = 4  # Conservative fallback
    
    # Warn about resource over-allocation
    if concurrency > cpu_count * 4:
        logger.warning(
            "Worker concurrency (%d) significantly exceeds CPU cores (%d). "
            "This may cause performance degradation.",
            concurrency, cpu_count
        )
    
    # Recommend optimal concurrency ranges
    if concurrency > cpu_count * 2:
        logger.info(
            "High concurrency detected (%d workers, %d cores). "
            "Monitor system performance and memory usage.",
            concurrency, cpu_count
        )
    
    return concurrency


class ConfigurationValidator:
    """Centralized configuration validation with comprehensive error reporting."""
    
    def __init__(self):
        self.validation_errors: List[ValidationError] = []
        self.validation_warnings: List[str] = []
    
    def validate_option(self, name: str, value: Any, option: EnhancedOption) -> Any:
        """Validate a single configuration option.
        
        Args:
            name: Option name
            value: Option value to validate
            option: EnhancedOption instance with validation rules
            
        Returns:
            Validated and converted value
            
        Raises:
            ValidationError: If validation fails
        """
        try:
            # Check environment variable override
            if option.env_var:
                env_value = os.getenv(option.env_var)
                if env_value is not None:
                    value = env_value
                    logger.debug(
                        "Configuration option '%s' overridden by environment variable '%s'",
                        name, option.env_var
                    )
            
            validated_value = option.validate_and_convert(value, name)
            
            # Log security-sensitive options (redacted)
            if option.security_sensitive:
                logger.debug("Validated security-sensitive option: %s=[REDACTED]", name)
            else:
                logger.debug("Validated configuration option: %s=%s", name, validated_value)
            
            return validated_value
        
        except ValidationError as e:
            self.validation_errors.append(e)
            raise
    
    def validate_configuration(self, config_dict: Dict[str, Any], option_definitions: Dict[str, EnhancedOption]) -> Dict[str, Any]:
        """Validate entire configuration dictionary.
        
        Args:
            config_dict: Configuration values to validate
            option_definitions: Mapping of option names to EnhancedOption instances
            
        Returns:
            Dictionary of validated configuration values
            
        Raises:
            ValidationError: If any validation fails
        """
        self.validation_errors.clear()
        self.validation_warnings.clear()
        
        validated_config = {}
        
        # Validate known options
        for name, option in option_definitions.items():
            value = config_dict.get(name)
            
            try:
                validated_config[name] = self.validate_option(name, value, option)
            except ValidationError:
                # Error already added to self.validation_errors
                pass
        
        # Check for unknown configuration options
        unknown_options = set(config_dict.keys()) - set(option_definitions.keys())
        for unknown in unknown_options:
            warning = f"Unknown configuration option: {unknown}"
            self.validation_warnings.append(warning)
            logger.warning(warning)
            # Include unknown options in result (pass-through)
            validated_config[unknown] = config_dict[unknown]
        
        # Report validation results
        if self.validation_errors:
            error_summary = "\n".join(str(e) for e in self.validation_errors)
            raise ImproperlyConfigured(
                f"Configuration validation failed:\n{error_summary}"
            )
        
        if self.validation_warnings:
            logger.warning(
                "Configuration validation completed with %d warnings",
                len(self.validation_warnings)
            )
        
        logger.info(
            "Configuration validation successful: %d options validated, %d warnings",
            len(validated_config), len(self.validation_warnings)
        )
        
        return validated_config
    
    def get_validation_summary(self) -> Dict[str, Any]:
        """Get summary of validation results."""
        return {
            "errors": len(self.validation_errors),
            "warnings": len(self.validation_warnings),
            "error_details": [str(e) for e in self.validation_errors],
            "warning_details": self.validation_warnings
        }


# Default validator instance
_default_validator = ConfigurationValidator()


def validate_celery_configuration(config: Dict[str, Any]) -> Dict[str, Any]:
    """Validate Celery configuration with enhanced rules.
    
    Args:
        config: Configuration dictionary to validate
        
    Returns:
        Validated configuration dictionary
        
    Raises:
        ImproperlyConfigured: If validation fails
    """
    # Define enhanced options for key Celery settings
    enhanced_options = {
        'broker_url': BROKER_URL_OPTION,
        'broker_connection_timeout': BROKER_CONNECTION_TIMEOUT_OPTION,
        'broker_connection_retry': BROKER_CONNECTION_RETRY_OPTION,
        'broker_connection_max_retries': BROKER_CONNECTION_MAX_RETRIES_OPTION,
        'broker_heartbeat': BROKER_HEARTBEAT_OPTION,
        'worker_concurrency': WORKER_CONCURRENCY_OPTION,
        'worker_pool': WORKER_POOL_OPTION,
        'worker_max_tasks_per_child': WORKER_MAX_TASKS_PER_CHILD_OPTION,
        'worker_max_memory_per_child': WORKER_MAX_MEMORY_PER_CHILD_OPTION,
        'task_time_limit': TASK_TIME_LIMIT_OPTION,
        'task_soft_time_limit': TASK_SOFT_TIME_LIMIT_OPTION,
        'task_serializer': TASK_SERIALIZER_OPTION,
        'result_backend': RESULT_BACKEND_OPTION,
        'result_expires': RESULT_EXPIRES_OPTION,
    }
    
    return _default_validator.validate_configuration(config, enhanced_options)