"""
data_gateway/exceptions.py
Infrastructure Layer - Custom System Exception Hierarchy.
"""

class DataGatewayException(Exception):
    """
    Base Exception class for the entire data gateway ecosystem.
    All pipeline-specific errors must inherit from this class to ensure
    monolithic error catching capabilities at the orchestration level.
    """
    pass

class SchemaViolationException(DataGatewayException):
    """
    Raised when an incoming data payload (CSV) does not comply with 
    the mandatory column schema defined by the business strategy rules.
    """
    pass

class DataQualitySLAException(DataGatewayException):
    """
    Raised when a processed dataset violates strict data quality thresholds,
    triggering operational alerts due to high corruption indexes.
    """
    pass