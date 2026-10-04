"""Reusable pipeline code for the Andina Market lakehouse (Bronze/Silver/Gold).

The Databricks notebooks are thin wrappers around these functions so that the
same logic runs in a Databricks Job and in local tests (Spark + Delta).
"""
