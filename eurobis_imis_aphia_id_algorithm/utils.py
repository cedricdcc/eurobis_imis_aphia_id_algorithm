"""
Utility functions and constants for the Aphia ID algorithm.
"""

import json
import ast
import os
from typing import Dict, Any, List, Tuple

# Default amplifier for scoring
DEFAULT_AMPLIFIER = 10

# Constants for ranking algorithms
RANK_SCORE_DICT = {
    "DOMAIN": 8 * DEFAULT_AMPLIFIER,
    "KINGDOM": 7 * DEFAULT_AMPLIFIER,
    "PHYLUM": 6 * DEFAULT_AMPLIFIER,
    "CLASS": 5 * DEFAULT_AMPLIFIER,
    "ORDER": 4 * DEFAULT_AMPLIFIER,
    "FAMILY": 3 * DEFAULT_AMPLIFIER,
    "GENUS": 2 * DEFAULT_AMPLIFIER,
    "SPECIES": 1 * DEFAULT_AMPLIFIER,
}

SUBRANK_SCORE_DICT = {
    "MEGA": +0.4 * DEFAULT_AMPLIFIER,
    "GIGA": +0.3 * DEFAULT_AMPLIFIER,
    "SUPER": +0.2 * DEFAULT_AMPLIFIER,
    "SUB": -0.2 * DEFAULT_AMPLIFIER,
    "INFRA": -0.3 * DEFAULT_AMPLIFIER,
    "PARV": -0.4 * DEFAULT_AMPLIFIER,
}


def calculate_rank_value(rank: str, amplifier: int = DEFAULT_AMPLIFIER) -> int:
    """
    Calculate the rank value for a given taxonomic rank.
    
    Args:
        rank: The taxonomic rank string
        amplifier: Multiplier for the scores
        
    Returns:
        The calculated rank value
    """
    rankupper = rank.upper()
    main_rank_value = 0
    prefix_rank_value = 0
    
    # Create amplified dictionaries
    rank_dict = {k: v * amplifier // DEFAULT_AMPLIFIER for k, v in RANK_SCORE_DICT.items()}
    subrank_dict = {k: v * amplifier // DEFAULT_AMPLIFIER for k, v in SUBRANK_SCORE_DICT.items()}
    
    # Find main rank value
    for rank_name, rank_val in rank_dict.items():
        len_rank = len(rank_name)
        spliced_rank = rankupper[-len_rank:]
        if spliced_rank == rank_name.upper():
            main_rank_value = rank_val
            
    # Find prefix rank value
    for prefix_rank, prefix_rank_val in subrank_dict.items():
        len_prefix_rank = len(prefix_rank)
        spliced_prefix_rank = rankupper[:len_prefix_rank]
        if spliced_prefix_rank == prefix_rank.upper():
            prefix_rank_value = prefix_rank_val
            
    return main_rank_value + prefix_rank_value


def get_child_if_dict(
    child: Dict[str, Any], 
    parent_id: str, 
    parent_list: List[str], 
    aphia_id_list: List[str], 
    scientific_name_list: List[str], 
    rank_list: List[str]
) -> None:
    """
    Recursively extract child information from nested dictionary structure.
    
    Args:
        child: The current child dictionary being processed
        parent_id: ID of the parent node
        parent_list: List to store parent IDs
        aphia_id_list: List to store Aphia IDs
        scientific_name_list: List to store scientific names
        rank_list: List to store taxonomic ranks
    """
    if isinstance(child.get("child"), dict):
        parent_list.append(parent_id)
        aphia_id_list.append(child["AphiaID"])
        parent_id = child["AphiaID"]
        rank_list.append(child["rank"])
        scientific_name_list.append(child["scientificname"])
        get_child_if_dict(
            child["child"],
            parent_id=parent_id,
            parent_list=parent_list,
            aphia_id_list=aphia_id_list,
            scientific_name_list=scientific_name_list,
            rank_list=rank_list,
        )
    else:
        parent_list.append(parent_id)
        aphia_id_list.append(child["AphiaID"])
        rank_list.append(child["rank"])
        scientific_name_list.append(child["scientificname"])


def open_cache_file(cache_file: str, base_path: str = "") -> Dict[str, Any]:
    """
    Open and load a JSON cache file.
    
    Args:
        cache_file: Path to the cache file
        base_path: Base directory path
        
    Returns:
        Dictionary containing cached data
    """
    file_path = os.path.join(base_path, cache_file) if base_path else cache_file
    with open(file_path, "r") as f:
        return json.load(f)


def write_cache_data(cache: Dict[str, Any], cache_file: str, base_path: str = "") -> None:
    """
    Write cache data to a JSON file.
    
    Args:
        cache: Dictionary to cache
        cache_file: Path to the cache file
        base_path: Base directory path
    """
    # Handle UTF-8 encoding
    utf_8_encoded_dict_encoded = {
        str(k).encode("utf-8"): str(v).encode("utf-8") for k, v in cache.items()
    }
    utf_8_encoded_dict = {
        k.decode("utf-8"): v.decode("utf-8")
        for k, v in utf_8_encoded_dict_encoded.items()
    }
    new_dict = {}
    for key, value in utf_8_encoded_dict.items():
        new_dict[key] = ast.literal_eval(value.replace("'", '"'))
        
    file_path = os.path.join(base_path, cache_file) if base_path else cache_file
    with open(file_path, "w") as f:
        json.dump(new_dict, f)


def create_reduction_table(initial_data: Dict[str, Dict[str, Any]], 
                         final_data: Dict[str, Dict[str, Any]]) -> str:
    """
    Create a reduction table showing taxa count by hierarchical level.
    
    Args:
        initial_data: Dictionary of initial taxonomic data
        final_data: Dictionary of final selected taxonomic data
        
    Returns:
        String representation of the reduction table
    """
    # Count taxa by rank for initial and final datasets
    initial_counts = {}
    final_counts = {}
    
    # Count initial data by rank
    for node_data in initial_data.values():
        rank = node_data.get('rank', 'Unknown').title()
        initial_counts[rank] = initial_counts.get(rank, 0) + 1
    
    # Count final data by rank  
    for node_data in final_data.values():
        rank = node_data.get('rank', 'Unknown').title()
        final_counts[rank] = final_counts.get(rank, 0) + 1
    
    # Get all ranks present in either dataset
    all_ranks = set(initial_counts.keys()) | set(final_counts.keys())
    
    # Sort ranks by taxonomic hierarchy (using reverse order of RANK_SCORE_DICT)
    rank_order = ["Domain", "Kingdom", "Phylum", "Class", "Order", "Family", "Genus", "Species"]
    sorted_ranks = []
    
    # Add known ranks in hierarchical order
    for rank in rank_order:
        if rank in all_ranks:
            sorted_ranks.append(rank)
    
    # Add any unknown ranks at the end
    for rank in sorted(all_ranks):
        if rank not in sorted_ranks:
            sorted_ranks.append(rank)
    
    if not sorted_ranks:
        return "No taxonomic data available for reduction table."
    
    # Build table
    table_lines = []
    table_lines.append("=" * 60)
    table_lines.append("TAXA REDUCTION SUMMARY BY HIERARCHICAL LEVEL")
    table_lines.append("=" * 60)
    table_lines.append(f"{'Rank':<15} {'Initial':<8} {'Final':<8} {'Reduced':<8} {'% Reduced':<10}")
    table_lines.append("-" * 60)
    
    total_initial = 0
    total_final = 0
    
    for rank in sorted_ranks:
        initial_count = initial_counts.get(rank, 0)
        final_count = final_counts.get(rank, 0)
        reduced_count = initial_count - final_count
        
        total_initial += initial_count
        total_final += final_count
        
        if initial_count > 0:
            percent_reduced = (reduced_count / initial_count) * 100
        else:
            percent_reduced = 0.0
            
        table_lines.append(
            f"{rank:<15} {initial_count:<8} {final_count:<8} {reduced_count:<8} {percent_reduced:>6.1f}%"
        )
    
    # Add totals
    table_lines.append("-" * 60)
    total_reduced = total_initial - total_final
    total_percent_reduced = (total_reduced / total_initial) * 100 if total_initial > 0 else 0.0
    
    table_lines.append(
        f"{'TOTAL':<15} {total_initial:<8} {total_final:<8} {total_reduced:<8} {total_percent_reduced:>6.1f}%"
    )
    table_lines.append("=" * 60)
    
    return "\n".join(table_lines)