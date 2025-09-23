"""
EurOBIS IMIS Aphia ID Algorithm Package

This package provides functionality to determine which Aphia IDs to use based on 
sorting IDs per DASID, querying via REST calls, caching results, and processing 
a tree structure to find optimal final IDs.
"""

from .algorithm import AphiaIdAlgorithm
from .data_loader import DataLoader
from .api_client import AphiaApiClient
from .utils import RANK_SCORE_DICT, SUBRANK_SCORE_DICT, create_reduction_table

__version__ = "1.0.0"
__author__ = "Cedric DCC"
__description__ = "Algorithm to determine which Aphia IDs to use"

__all__ = [
    "AphiaIdAlgorithm", 
    "DataLoader", 
    "AphiaApiClient",
    "RANK_SCORE_DICT", 
    "SUBRANK_SCORE_DICT",
    "create_reduction_table"
]