"""
Tests for the visualization functionality.
"""

import pytest
import os
import tempfile
from eurobis_imis_aphia_id_algorithm.visualization import Visualizer


class TestVisualizer:
    """Test cases for Visualizer class."""
    
    @pytest.fixture
    def visualizer(self):
        """Create a visualizer instance for testing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield Visualizer(base_path=temp_dir)
    
    @pytest.fixture
    def sample_final_ids(self):
        """Create sample final IDs for testing."""
        return {
            "1": {
                "scientificname": "Animalia",
                "aphiaid": 1,
                "rank": "Kingdom",
                "parent": "",
                "children": 2,
                "directchildren": 2
            },
            "2": {
                "scientificname": "Chordata", 
                "aphiaid": 2,
                "rank": "Phylum",
                "parent": "1",
                "children": 1,
                "directchildren": 1
            }
        }
    
    @pytest.fixture
    def sample_all_data(self):
        """Create sample all_data for testing."""
        return {
            "1": {
                "scientificname": "Animalia",
                "aphiaid": 1,
                "rank": "Kingdom", 
                "parent": "",
                "children": 2,
                "directchildren": 2
            },
            "2": {
                "scientificname": "Chordata",
                "aphiaid": 2, 
                "rank": "Phylum",
                "parent": "1",
                "children": 1,
                "directchildren": 1
            },
            "3": {
                "scientificname": "Mammalia",
                "aphiaid": 3,
                "rank": "Class",
                "parent": "2", 
                "children": 0,
                "directchildren": 0
            }
        }
    
    def test_create_sunburst_chart(self, visualizer, sample_final_ids, sample_all_data):
        """Test sunburst chart creation."""
        dasid = 12345
        original_aphia_ids = [1, 2]
        
        # Create sunburst chart
        html_file = visualizer.create_sunburst_chart(
            sample_final_ids, sample_all_data, dasid, original_aphia_ids
        )
        
        # Check that file was created
        assert os.path.exists(html_file)
        assert html_file.endswith("_sunburst.html")
        assert str(dasid) in html_file
        
        # Check that file contains HTML content
        with open(html_file, 'r') as f:
            content = f.read()
            assert "<!DOCTYPE html>" in content or "<html>" in content
            assert "plotly" in content.lower()
            assert "sunburst" in content.lower() or "Sunburst" in content
    
    def test_create_sunburst_chart_custom_filename(self, visualizer, sample_final_ids, sample_all_data):
        """Test sunburst chart creation with custom filename."""
        dasid = 67890
        original_aphia_ids = [1]
        custom_filename = "custom_sunburst.html"
        
        html_file = visualizer.create_sunburst_chart(
            sample_final_ids, sample_all_data, dasid, original_aphia_ids, custom_filename
        )
        
        assert os.path.exists(html_file)
        assert html_file.endswith(custom_filename)
    
    def test_create_sunburst_chart_empty_final_ids(self, visualizer, sample_all_data):
        """Test sunburst chart creation with empty final IDs."""
        dasid = 99999
        original_aphia_ids = []
        
        html_file = visualizer.create_sunburst_chart(
            {}, sample_all_data, dasid, original_aphia_ids
        )
        
        # Should still create a file even with empty final_ids
        assert os.path.exists(html_file)
        
        # Check content exists
        with open(html_file, 'r') as f:
            content = f.read()
            assert len(content) > 0
    
    def test_create_sunburst_chart_hierarchical_structure(self, visualizer):
        """Test sunburst chart correctly handles hierarchical data."""
        # Create hierarchical data with clear parent-child relationships
        final_ids = {
            "3": {
                "scientificname": "Mammalia", 
                "aphiaid": 3,
                "rank": "Class",
                "parent": "2",
                "children": 0,
                "directchildren": 0
            }
        }
        
        all_data = {
            "1": {
                "scientificname": "Animalia",
                "aphiaid": 1,
                "rank": "Kingdom",
                "parent": "",
                "children": 2,
                "directchildren": 1
            },
            "2": {
                "scientificname": "Chordata",
                "aphiaid": 2,
                "rank": "Phylum", 
                "parent": "1",
                "children": 1,
                "directchildren": 1
            },
            "3": {
                "scientificname": "Mammalia",
                "aphiaid": 3,
                "rank": "Class",
                "parent": "2",
                "children": 0,
                "directchildren": 0
            }
        }
        
        dasid = 11111
        original_aphia_ids = [3]
        
        html_file = visualizer.create_sunburst_chart(
            final_ids, all_data, dasid, original_aphia_ids
        )
        
        assert os.path.exists(html_file)
        
        # Read file and check for hierarchical structure indicators
        with open(html_file, 'r') as f:
            content = f.read()
            # Should contain references to all levels in hierarchy
            assert "Animalia" in content
            assert "Chordata" in content
            assert "Mammalia" in content
    
    def test_sunburst_chart_handles_circular_references(self, visualizer):
        """Test sunburst chart handles potential circular references."""
        # Create data that could cause infinite loops
        final_ids = {
            "1": {
                "scientificname": "Node1",
                "aphiaid": 1,
                "rank": "Kingdom",
                "parent": "2",  # Parent points to child (circular)
                "children": 1,
                "directchildren": 1
            }
        }
        
        all_data = {
            "1": {
                "scientificname": "Node1", 
                "aphiaid": 1,
                "rank": "Kingdom",
                "parent": "2",
                "children": 1,
                "directchildren": 1
            },
            "2": {
                "scientificname": "Node2",
                "aphiaid": 2,
                "rank": "Phylum",
                "parent": "1",  # Circular reference
                "children": 1,
                "directchildren": 1
            }
        }
        
        dasid = 22222
        original_aphia_ids = [1, 2]
        
        # Should not raise an exception or hang
        html_file = visualizer.create_sunburst_chart(
            final_ids, all_data, dasid, original_aphia_ids
        )
        
        assert os.path.exists(html_file)