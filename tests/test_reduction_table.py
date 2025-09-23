"""
Unit tests for the reduction table functionality.
"""

import pytest
from eurobis_imis_aphia_id_algorithm.utils import create_reduction_table


class TestReductionTable:
    
    def test_create_reduction_table_basic(self):
        """Test basic reduction table creation."""
        initial_data = {
            "1": {"rank": "Kingdom", "scientificname": "Animalia", "aphiaid": "1"},
            "2": {"rank": "Phylum", "scientificname": "Chordata", "aphiaid": "2"},
            "3": {"rank": "Class", "scientificname": "Mammalia", "aphiaid": "3"},
            "4": {"rank": "Order", "scientificname": "Primates", "aphiaid": "4"},
            "5": {"rank": "Family", "scientificname": "Hominidae", "aphiaid": "5"},
            "6": {"rank": "Genus", "scientificname": "Homo", "aphiaid": "6"},
            "7": {"rank": "Species", "scientificname": "Homo sapiens", "aphiaid": "7"},
        }
        
        final_data = {
            "1": {"rank": "Kingdom", "scientificname": "Animalia", "aphiaid": "1"},
            "3": {"rank": "Class", "scientificname": "Mammalia", "aphiaid": "3"},
            "7": {"rank": "Species", "scientificname": "Homo sapiens", "aphiaid": "7"},
        }
        
        table = create_reduction_table(initial_data, final_data)
        
        # Check that all expected elements are in the table
        assert "Kingdom" in table
        assert "Phylum" in table
        assert "Class" in table
        assert "Order" in table
        assert "Family" in table
        assert "Genus" in table
        assert "Species" in table
        assert "TOTAL" in table
        
        # Check numerical accuracy
        assert "7        3        4" in table  # Total line should show 7 initial, 3 final, 4 reduced
        
    def test_create_reduction_table_empty_final(self):
        """Test reduction table when final data is empty."""
        initial_data = {
            "1": {"rank": "Kingdom", "scientificname": "Animalia", "aphiaid": "1"},
            "2": {"rank": "Phylum", "scientificname": "Chordata", "aphiaid": "2"},
        }
        
        final_data = {}
        
        table = create_reduction_table(initial_data, final_data)
        
        assert "Kingdom" in table
        assert "Phylum" in table
        assert "2        0        2" in table  # 2 initial, 0 final, 2 reduced
        assert "100.0%" in table  # Should show 100% reduction
        
    def test_create_reduction_table_no_reduction(self):
        """Test reduction table when no reduction occurs."""
        data = {
            "1": {"rank": "Kingdom", "scientificname": "Animalia", "aphiaid": "1"},
            "2": {"rank": "Phylum", "scientificname": "Chordata", "aphiaid": "2"},
        }
        
        table = create_reduction_table(data, data)
        
        assert "Kingdom" in table
        assert "Phylum" in table
        assert "2        2        0" in table  # 2 initial, 2 final, 0 reduced
        assert "0.0%" in table  # Should show 0% reduction
        
    def test_create_reduction_table_empty_data(self):
        """Test reduction table with empty datasets."""
        table = create_reduction_table({}, {})
        
        assert "No taxonomic data available" in table
        
    def test_create_reduction_table_unknown_ranks(self):
        """Test reduction table with unknown taxonomic ranks."""
        initial_data = {
            "1": {"rank": "UnknownRank1", "scientificname": "Test1", "aphiaid": "1"},
            "2": {"rank": "UnknownRank2", "scientificname": "Test2", "aphiaid": "2"},
        }
        
        final_data = {
            "1": {"rank": "UnknownRank1", "scientificname": "Test1", "aphiaid": "1"},
        }
        
        table = create_reduction_table(initial_data, final_data)
        
        assert "Unknownrank1" in table
        assert "Unknownrank2" in table
        assert "2        1        1" in table  # 2 initial, 1 final, 1 reduced
        
    def test_create_reduction_table_missing_rank_key(self):
        """Test reduction table when rank key is missing."""
        initial_data = {
            "1": {"scientificname": "Test1", "aphiaid": "1"},  # Missing rank
            "2": {"rank": "Species", "scientificname": "Test2", "aphiaid": "2"},
        }
        
        final_data = {
            "2": {"rank": "Species", "scientificname": "Test2", "aphiaid": "2"},
        }
        
        table = create_reduction_table(initial_data, final_data)
        
        assert "Unknown" in table  # Should handle missing rank gracefully
        assert "Species" in table
        
    def test_create_reduction_table_rank_order(self):
        """Test that ranks appear in correct taxonomic hierarchy order."""
        initial_data = {
            "1": {"rank": "Species", "scientificname": "Test species", "aphiaid": "1"},
            "2": {"rank": "Kingdom", "scientificname": "Test kingdom", "aphiaid": "2"},
            "3": {"rank": "Genus", "scientificname": "Test genus", "aphiaid": "3"},
            "4": {"rank": "Family", "scientificname": "Test family", "aphiaid": "4"},
        }
        
        final_data = {}
        
        table = create_reduction_table(initial_data, final_data)
        
        # Check that Kingdom appears before Genus, which appears before Species
        kingdom_pos = table.find("Kingdom")
        genus_pos = table.find("Genus")
        family_pos = table.find("Family")
        species_pos = table.find("Species")
        
        assert kingdom_pos < family_pos < genus_pos < species_pos
        
    def test_percentage_calculation_accuracy(self):
        """Test percentage calculation accuracy."""
        initial_data = {
            "1": {"rank": "Species", "scientificname": "Test1", "aphiaid": "1"},
            "2": {"rank": "Species", "scientificname": "Test2", "aphiaid": "2"},
            "3": {"rank": "Species", "scientificname": "Test3", "aphiaid": "3"},
        }
        
        final_data = {
            "1": {"rank": "Species", "scientificname": "Test1", "aphiaid": "1"},
        }
        
        table = create_reduction_table(initial_data, final_data)
        
        # 2 out of 3 reduced = 66.7%
        assert "66.7%" in table