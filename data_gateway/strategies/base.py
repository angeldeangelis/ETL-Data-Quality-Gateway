"""
data_gateway/strategies/base.py
Abstraction Layer - Validation Strategy Base Contract.
"""

from abc import ABC, abstractmethod

class ValidationStrategy(ABC):
    """
    Abstract Base Class (ABC) acting as a strict architectural contract.
    Any domain-specific validation strategy must inherit from this class
    and fully implement its abstract properties to ensure complete
    decoupling from the main ETL Engine.
    """

    @property
    @abstractmethod
    def required_columns(self) -> list[str]:
        """
        Enforces the implementation of a read-only list of mandatory 
        columns that the ingestion payload must contain.
        """
        pass

    @property
    @abstractmethod
    def primary_key(self) -> str:
        """
        Enforces the implementation of the unique business identifier (PK)
        used for data quality deduplication and null checks.
        """
        pass