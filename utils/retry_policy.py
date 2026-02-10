"""
Retry policy implementation with exponential backoff.
Handles transient failures with configurable retry attempts and delays.
"""

import time
import random
from typing import Callable, Any


class RetryPolicy:
    """Executes operations with exponential backoff retry on failure"""

    def __init__(self, max_retries: int = 3, retry_delay_seconds: float = 30.0):
        """
        Initialize retry policy.

        Args:
            max_retries: Maximum number of attempts
            retry_delay_seconds: Base delay between retries (exponential backoff)
        """
        self.max_retries = max_retries
        self.retry_delay_seconds = retry_delay_seconds

    def execute(self, fn: Callable, *args, operation_name: str = "operation", **kwargs) -> Any:
        """
        Execute function with retry on failure.

        Args:
            fn: Function to execute
            *args: Positional arguments for fn
            operation_name: Name for logging purposes
            **kwargs: Keyword arguments for fn

        Returns:
            Result from successful function call

        Raises:
            Exception: Original exception after all retries exhausted

        Examples:
            >>> def api_call():
            ...     return "success"
            >>> policy = RetryPolicy(max_retries=3, retry_delay_seconds=1)
            >>> policy.execute(api_call, operation_name="test")
            'success'
        """
        last_exception = None

        for attempt in range(1, self.max_retries + 1):
            try:
                return fn(*args, **kwargs)

            except Exception as e:
                last_exception = e

                # If this was the last attempt, re-raise
                if attempt == self.max_retries:
                    raise

                # Calculate delay with exponential backoff
                # delay = base * 2^(attempt-1) + jitter
                delay = self.retry_delay_seconds * (2 ** (attempt - 1))
                jitter = random.uniform(0, delay * 0.1)  # 0-10% jitter
                total_delay = delay + jitter

                # Log the retry (if logger passed as kwarg)
                logger = kwargs.get('_logger')
                if logger:
                    logger.warning(
                        f"[{operation_name}] Attempt {attempt} failed: {type(e).__name__}: {str(e)}"
                    )

                time.sleep(total_delay)

        # Should never reach here, but raise as fallback
        raise last_exception if last_exception else RuntimeError(f"Failed to execute {operation_name}")
