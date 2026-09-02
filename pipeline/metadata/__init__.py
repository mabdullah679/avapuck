"""Schema inference and sensitivity classification.

The layer that lets the pipeline ingest a CSV it has never seen. Every
downstream stage reads its column list, its sensitive set, and its primary key
from the manifest this package produces, rather than from a hardcoded list.
"""
