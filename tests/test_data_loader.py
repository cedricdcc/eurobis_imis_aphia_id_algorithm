"""
Tests for the data loader functionality.
"""

import pytest
import pandas as pd
import tempfile
import os
from eurobis_imis_aphia_id_algorithm.data_loader import DataLoader


class TestDataLoader:
    """Test cases for DataLoader class."""
    
    @pytest.fixture
    def sample_csv_data(self):
        """Create sample CSV data for testing."""
        return pd.DataFrame({
            "IMIS_DasID": [1001, 1001, 1002, 1002, 1003],
            "aphia_id": [12345, 12346, 23456, 23457, 34567]
        })
        
    @pytest.fixture
    def temp_csv_file(self, sample_csv_data):
        """Create a temporary CSV file with sample data."""
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_file = os.path.join(temp_dir, "test_data.csv")
            sample_csv_data.to_csv(csv_file, index=False)
            yield csv_file
    
    def test_load_aphia_ids_csv(self, temp_csv_file, sample_csv_data):
        """Test loading CSV file."""
        temp_dir = os.path.dirname(temp_csv_file)
        csv_filename = os.path.basename(temp_csv_file)
        
        loader = DataLoader(temp_dir)
        df = loader.load_aphia_ids_csv(csv_filename)
        
        pd.testing.assert_frame_equal(df, sample_csv_data)
        
    def test_load_nonexistent_csv(self):
        """Test handling of nonexistent CSV file."""
        loader = DataLoader()
        
        with pytest.raises(FileNotFoundError):
            loader.load_aphia_ids_csv("nonexistent.csv")
            
    def test_group_aphia_ids_by_dasid(self, sample_csv_data):
        """Test grouping Aphia IDs by DASID."""
        loader = DataLoader()
        grouped = loader.group_aphia_ids_by_dasid(sample_csv_data)
        
        expected = {
            1001: [12345, 12346],
            1002: [23456, 23457],
            1003: [34567]
        }
        
        assert grouped == expected
        
    def test_group_missing_columns(self):
        """Test handling of missing required columns."""
        loader = DataLoader()
        invalid_df = pd.DataFrame({
            "wrong_column": [1, 2, 3],
            "another_wrong": [4, 5, 6]
        })
        
        with pytest.raises(ValueError, match="Missing required columns"):
            loader.group_aphia_ids_by_dasid(invalid_df)
            
    def test_load_and_process_csv(self, temp_csv_file):
        """Test the combined load and process operation."""
        temp_dir = os.path.dirname(temp_csv_file)
        csv_filename = os.path.basename(temp_csv_file)
        
        loader = DataLoader(temp_dir)
        result = loader.load_and_process_csv(csv_filename)
        
        expected = {
            1001: [12345, 12346],
            1002: [23456, 23457], 
            1003: [34567]
        }
        
        assert result == expected
        
    def test_empty_csv(self):
        """Test handling of empty CSV file."""
        empty_df = pd.DataFrame(columns=["IMIS_DasID", "aphia_id"])
        
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_file = os.path.join(temp_dir, "empty.csv")
            empty_df.to_csv(csv_file, index=False)
            
            loader = DataLoader(temp_dir)
            result = loader.load_and_process_csv("empty.csv")
            
            assert result == {}
            
    def test_duplicate_entries(self):
        """Test handling of duplicate entries in CSV."""
        duplicate_df = pd.DataFrame({
            "IMIS_DasID": [1001, 1001, 1001],
            "aphia_id": [12345, 12345, 12346]  # Duplicate aphia_id for same DASID
        })
        
        loader = DataLoader()
        result = loader.group_aphia_ids_by_dasid(duplicate_df)
        
        # Should include duplicates as they appear in the data
        expected = {1001: [12345, 12345, 12346]}
        assert result == expected