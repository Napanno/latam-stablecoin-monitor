"""
Configuration validation for pipeline startup.
Ensures all required sections and valid values before execution.
"""

import re
from typing import Dict, Any


class ConfigValidator:
    """Validates pipeline configuration structure and content"""

    @staticmethod
    def validate_config(config: Dict[str, Any], logger=None) -> bool:
        """
        Validate complete configuration structure.

        Args:
            config: Configuration dictionary from YAML
            logger: Optional logger instance for warnings

        Returns:
            bool: True if valid, raises ValueError otherwise

        Raises:
            ValueError: If any validation check fails

        Examples:
            >>> config = {'dune': {}, 'tokens': {}, 'output': {}, 'processing': {}, 'execution': {}}
            >>> ConfigValidator.validate_config(config)
            True
        """
        # Check required top-level sections
        required_sections = ['dune', 'tokens', 'output', 'processing', 'execution']
        missing = [s for s in required_sections if s not in config]

        if missing:
            raise ValueError(f"Missing configuration sections: {missing}")

        # Validate each section
        ConfigValidator._validate_dune_section(config.get('dune', {}), logger)
        ConfigValidator._validate_tokens_section(config.get('tokens', {}), logger)
        ConfigValidator._validate_output_section(config.get('output', {}), logger)
        ConfigValidator._validate_execution_section(config.get('execution', {}), logger)

        return True

    @staticmethod
    def _validate_dune_section(dune_config: Dict, logger=None) -> None:
        """Validate Dune API configuration"""
        if 'query_ids' not in dune_config:
            raise ValueError("Missing 'dune' keys: ['query_ids']")

        query_ids = dune_config['query_ids']
        if not isinstance(query_ids, dict) or len(query_ids) == 0:
            raise ValueError("'dune.query_ids' must be non-empty dict")

        # Validate each query ID is an integer
        for query_name, query_id in query_ids.items():
            try:
                int(query_id)
            except (TypeError, ValueError):
                raise ValueError(f"Invalid query ID for '{query_name}': {query_id} (must be integer)")

    @staticmethod
    def _validate_tokens_section(tokens_config: Dict, logger=None) -> None:
        """Validate token configuration"""
        if 'tracked_tokens' not in tokens_config:
            raise ValueError("Missing 'tokens.tracked_tokens'")

        tracked_tokens = tokens_config['tracked_tokens']
        if not isinstance(tracked_tokens, list) or len(tracked_tokens) == 0:
            raise ValueError("'tokens.tracked_tokens' must be non-empty list")

        # Validate each token
        for idx, token in enumerate(tracked_tokens):
            if 'contract_address' not in token:
                raise ValueError(f"Token {idx}: missing 'contract_address'")

            addr = token['contract_address']
            # Validate hex address format 0x[40 hex chars]
            if not re.match(r'^0x[a-fA-F0-9]{40}$', str(addr)):
                raise ValueError(f"Token {idx}: invalid contract address: {addr}")

            if 'blockchain' not in token:
                raise ValueError(f"Token {idx}: missing 'blockchain'")

    @staticmethod
    def _validate_output_section(output_config: Dict, logger=None) -> None:
        """Validate output configuration"""
        required_dirs = ['base_dir']
        missing = [d for d in required_dirs if d not in output_config]

        if missing:
            raise ValueError(f"Missing output directories: {missing}")

    @staticmethod
    def _validate_execution_section(exec_config: Dict, logger=None) -> None:
        """Validate execution configuration"""
        if 'max_retries' in exec_config:
            try:
                retries = int(exec_config['max_retries'])
                if retries < 1:
                    raise ValueError("max_retries must be >= 1")
            except (TypeError, ValueError):
                raise ValueError(f"Invalid max_retries: {exec_config['max_retries']}")

        if 'retry_delay_seconds' in exec_config:
            try:
                delay = float(exec_config['retry_delay_seconds'])
                if delay < 0:
                    raise ValueError("retry_delay_seconds must be >= 0")
            except (TypeError, ValueError):
                raise ValueError(f"Invalid retry_delay_seconds: {exec_config['retry_delay_seconds']}")
