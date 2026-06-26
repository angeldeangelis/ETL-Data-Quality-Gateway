"""
data_gateway/strategies/financial.py
Domain Layer - Concrete Financial Ingestion Strategy.
"""

from data_gateway.strategies.base import ValidationStrategy

class FinancialValidationStrategy(ValidationStrategy):
    """
    Concrete implementation of the ValidationStrategy contract 
    tailored specifically for corporate financial transaction datasets.
    """

    @property
    def required_columns(self) -> list[str]:
        """
        Returns the rigid schema contract requirements for financial logs.
        """
        return ['transaction_id', 'product_name', 'revenue']

    @property
    def primary_key(self) -> str:
        """
        Defines the financial transaction ID as the immutable unique key.
        """
        return 'transaction_id'