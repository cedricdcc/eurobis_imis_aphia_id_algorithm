"""
Tests for the utility functions.
"""

import pytest
import tempfile
import json
import os
from eurobis_imis_aphia_id_algorithm.utils import (
    calculate_rank_value,
    get_child_if_dict,
    open_cache_file,
    write_cache_data,
    RANK_SCORE_DICT,
    SUBRANK_SCORE_DICT,
    DEFAULT_AMPLIFIER
)


class TestCalculateRankValue:
    """Test cases for calculate_rank_value function."""
    
    def test_basic_rank_values(self):
        """Test basic rank calculations."""
        assert calculate_rank_value("SPECIES") == RANK_SCORE_DICT["SPECIES"]
        assert calculate_rank_value("GENUS") == RANK_SCORE_DICT["GENUS"]
        assert calculate_rank_value("FAMILY") == RANK_SCORE_DICT["FAMILY"]
        
    def test_subrank_values(self):
        """Test subrank calculations."""
        # Test subrank prefix
        expected_super_family = RANK_SCORE_DICT["FAMILY"] + SUBRANK_SCORE_DICT["SUPER"]
        assert calculate_rank_value("SUPERFAMILY") == expected_super_family
        
        expected_sub_genus = RANK_SCORE_DICT["GENUS"] + SUBRANK_SCORE_DICT["SUB"]
        assert calculate_rank_value("SUBGENUS") == expected_sub_genus
        
    def test_case_insensitive(self):
        """Test case insensitive rank matching."""
        assert calculate_rank_value("species") == calculate_rank_value("SPECIES")
        assert calculate_rank_value("Family") == calculate_rank_value("FAMILY")
        
    def test_custom_amplifier(self):
        """Test custom amplifier values."""
        custom_amplifier = 20
        expected = RANK_SCORE_DICT["SPECIES"] * custom_amplifier // DEFAULT_AMPLIFIER
        assert calculate_rank_value("SPECIES", custom_amplifier) == expected
        
    def test_unknown_rank(self):
        """Test handling of unknown ranks."""
        assert calculate_rank_value("UNKNOWN_RANK") == 0


class TestGetChildIfDict:
    """Test cases for get_child_if_dict function."""
    
    def test_single_child(self):
        """Test extraction of single child."""
        child_data = {
            "AphiaID": "123",
            "rank": "Species",
            "scientificname": "Test species",
            "child": "terminal"
        }
        
        parent_list = []
        aphia_id_list = []
        scientific_name_list = []
        rank_list = []
        
        get_child_if_dict(
            child_data, "parent_123", parent_list, 
            aphia_id_list, scientific_name_list, rank_list
        )
        
        assert len(aphia_id_list) == 1
        assert aphia_id_list[0] == "123"
        assert scientific_name_list[0] == "Test species"
        assert rank_list[0] == "Species"
        assert parent_list[0] == "parent_123"
        
    def test_nested_children(self):
        """Test extraction of nested children."""
        nested_data = {
            "AphiaID": "100",
            "rank": "Family", 
            "scientificname": "Test family",
            "child": {
                "AphiaID": "200",
                "rank": "Genus",
                "scientificname": "Test genus",
                "child": "terminal"
            }
        }
        
        parent_list = []
        aphia_id_list = []
        scientific_name_list = []
        rank_list = []
        
        get_child_if_dict(
            nested_data, "", parent_list,
            aphia_id_list, scientific_name_list, rank_list
        )
        
        assert len(aphia_id_list) == 2
        assert aphia_id_list == ["100", "200"]
        assert scientific_name_list == ["Test family", "Test genus"]
        assert rank_list == ["Family", "Genus"]
        assert parent_list == ["", "100"]


class TestCacheOperations:
    """Test cases for cache file operations."""
    
    def test_write_and_read_cache(self):
        """Test writing and reading cache files."""
        test_data = {
            "dasid_123": {
                "data": {"test_key": "test_value"},
                "urls_done": ["url1", "url2"]
            }
        }
        
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_file = "test_cache.json"
            
            # Write cache
            write_cache_data(test_data, cache_file, temp_dir)
            
            # Read cache
            loaded_data = open_cache_file(cache_file, temp_dir)
            
            assert loaded_data == test_data
            
    def test_cache_file_not_found(self):
        """Test handling of missing cache file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            with pytest.raises(FileNotFoundError):
                open_cache_file("nonexistent.json", temp_dir)
                
    def test_empty_cache(self):
        """Test handling of empty cache."""
        empty_data = {}
        
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_file = "empty_cache.json"
            
            write_cache_data(empty_data, cache_file, temp_dir)
            loaded_data = open_cache_file(cache_file, temp_dir)
            
            assert loaded_data == empty_data