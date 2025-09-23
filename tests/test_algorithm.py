"""
Tests for the algorithm implementation.
"""

import pytest
from eurobis_imis_aphia_id_algorithm.algorithm import AphiaIdAlgorithm


class TestAphiaIdAlgorithm:
    """Test cases for AphiaIdAlgorithm class."""
    
    @pytest.fixture
    def algorithm(self):
        """Create an algorithm instance for testing."""
        return AphiaIdAlgorithm(max_nodes=5, amplifier=10)
    
    @pytest.fixture
    def sample_taxonomic_data(self):
        """Create sample taxonomic data for testing."""
        return {
            "1": {
                "scientificname": "Root",
                "aphiaid": 1,
                "rank": "Kingdom",
                "parent": "",
                "children": 3,
                "directchildren": 2
            },
            "2": {
                "scientificname": "Child1",
                "aphiaid": 2,
                "rank": "Phylum", 
                "parent": "1",
                "children": 1,
                "directchildren": 1
            },
            "3": {
                "scientificname": "Child2",
                "aphiaid": 3,
                "rank": "Phylum",
                "parent": "1", 
                "children": 0,
                "directchildren": 0
            },
            "4": {
                "scientificname": "Grandchild",
                "aphiaid": 4,
                "rank": "Class",
                "parent": "2",
                "children": 0,
                "directchildren": 0
            }
        }
    
    def test_calculate_relevancy(self, algorithm):
        """Test relevancy calculation."""
        # Test normal case
        node_data = {"children": 10, "directchildren": 5}
        assert algorithm.calculate_relevancy(node_data) == 2.0
        
        # Test zero direct children
        node_data = {"children": 10, "directchildren": 0}
        assert algorithm.calculate_relevancy(node_data) == 0.0
        
        # Test missing keys
        node_data = {}
        assert algorithm.calculate_relevancy(node_data) == 0.0
        
    def test_find_root_nodes(self, algorithm, sample_taxonomic_data):
        """Test finding root nodes."""
        root_nodes = algorithm.find_root_nodes(sample_taxonomic_data)
        
        # Should return the node with most children (node "1" with 3 children)
        assert len(root_nodes) == 1
        assert "1" in root_nodes
        assert root_nodes["1"]["aphiaid"] == 1
        
    def test_find_root_nodes_empty_data(self, algorithm):
        """Test finding root nodes with empty data."""
        root_nodes = algorithm.find_root_nodes({})
        assert root_nodes == {}
        
    def test_calculate_node_rankings(self, algorithm, sample_taxonomic_data):
        """Test node ranking calculation."""
        # Use subset of data
        final_ids = {"1": sample_taxonomic_data["1"], "2": sample_taxonomic_data["2"]}
        rankings = algorithm.calculate_node_rankings(final_ids)
        
        assert len(rankings) == 2
        assert all("aphia_id" in ranking for ranking in rankings)
        assert all("relevancy" in ranking for ranking in rankings)
        assert all("rank_value" in ranking for ranking in rankings)
        
        # Rankings should be sorted by rank_value (Kingdom > Phylum)
        assert rankings[0]["rank_value"] >= rankings[1]["rank_value"]
        
    def test_get_children_for_node(self, algorithm, sample_taxonomic_data):
        """Test getting children for a node."""
        children = algorithm.get_children_for_node("1", sample_taxonomic_data)
        
        # Node "1" should have children "2" and "3"
        assert len(children) == 2
        child_ids = [child["aphiaid"] for child in children]
        assert 2 in child_ids
        assert 3 in child_ids
        
    def test_get_children_for_leaf_node(self, algorithm, sample_taxonomic_data):
        """Test getting children for a leaf node."""
        children = algorithm.get_children_for_node("3", sample_taxonomic_data)
        assert len(children) == 0
        
    def test_apply_algorithm_basic(self, algorithm, sample_taxonomic_data):
        """Test basic algorithm application."""
        result = algorithm.apply_algorithm(sample_taxonomic_data)
        
        # Should return some final IDs
        assert len(result) > 0
        assert len(result) <= algorithm.max_nodes
        
        # All results should have required fields
        for node_data in result.values():
            assert "scientificname" in node_data
            assert "aphiaid" in node_data
            assert "rank" in node_data
            assert "parent" in node_data
            
    def test_apply_algorithm_empty_data(self, algorithm):
        """Test algorithm with empty data."""
        result = algorithm.apply_algorithm({})
        assert result == {}
        
    def test_apply_algorithm_single_node(self, algorithm):
        """Test algorithm with single node."""
        single_node_data = {
            "1": {
                "scientificname": "Single",
                "aphiaid": 1,
                "rank": "Species",
                "parent": "",
                "children": 0,
                "directchildren": 0
            }
        }
        
        result = algorithm.apply_algorithm(single_node_data)
        assert len(result) == 1
        assert "1" in result
        
    def test_max_nodes_limit(self):
        """Test that max_nodes limit is respected."""
        algorithm = AphiaIdAlgorithm(max_nodes=2)
        
        # Create data with more nodes than limit
        large_data = {}
        for i in range(10):
            large_data[str(i)] = {
                "scientificname": f"Node{i}",
                "aphiaid": i,
                "rank": "Species",
                "parent": "",
                "children": 0,
                "directchildren": 0
            }
            
        result = algorithm.apply_algorithm(large_data)
        assert len(result) <= 2
        
    def test_min_nodes_functionality_below_threshold(self):
        """Test that min_nodes adds nodes when below threshold."""
        # Create algorithm with min_nodes=3 
        algorithm = AphiaIdAlgorithm(max_nodes=50, min_nodes=3)
        
        # Create small dataset that would normally result in 1 node
        small_data = {
            "1": {
                "scientificname": "Root",
                "aphiaid": 1,
                "rank": "Kingdom",
                "parent": "",
                "children": 0,
                "directchildren": 0
            },
            "2": {
                "scientificname": "Node2",
                "aphiaid": 2,
                "rank": "Phylum",
                "parent": "",
                "children": 0,
                "directchildren": 0
            },
            "3": {
                "scientificname": "Node3", 
                "aphiaid": 3,
                "rank": "Class",
                "parent": "",
                "children": 0,
                "directchildren": 0
            }
        }
        
        result = algorithm.apply_algorithm(small_data)
        # Should have at least min_nodes entries
        assert len(result) >= algorithm.min_nodes
        assert len(result) == 3  # Should select all 3 available nodes
        
    def test_min_nodes_functionality_meets_threshold(self):
        """Test that min_nodes doesn't affect results when threshold is already met."""
        algorithm = AphiaIdAlgorithm(max_nodes=50, min_nodes=2)
        
        # Use existing sample data which typically results in more than 2 nodes
        sample_data = {
            "1": {"scientificname": "Root", "aphiaid": 1, "rank": "Kingdom", "parent": "", "children": 3, "directchildren": 2},
            "2": {"scientificname": "Child1", "aphiaid": 2, "rank": "Phylum", "parent": "1", "children": 1, "directchildren": 1},
            "3": {"scientificname": "Child2", "aphiaid": 3, "rank": "Phylum", "parent": "1", "children": 0, "directchildren": 0},
            "4": {"scientificname": "Grandchild", "aphiaid": 4, "rank": "Class", "parent": "2", "children": 0, "directchildren": 0}
        }
        
        result = algorithm.apply_algorithm(sample_data)
        # Should have at least min_nodes entries
        assert len(result) >= algorithm.min_nodes
        
    def test_min_nodes_zero(self):
        """Test min_nodes=0 doesn't force any nodes to be added."""
        algorithm = AphiaIdAlgorithm(max_nodes=50, min_nodes=0)
        
        # Empty data should return empty result
        result = algorithm.apply_algorithm({})
        assert len(result) == 0
        
    def test_min_nodes_exceeds_available(self):
        """Test warning when min_nodes exceeds available nodes."""
        algorithm = AphiaIdAlgorithm(max_nodes=50, min_nodes=10)
        
        # Only provide 2 nodes but request 10 minimum
        limited_data = {
            "1": {"scientificname": "Node1", "aphiaid": 1, "rank": "Kingdom", "parent": "", "children": 0, "directchildren": 0},
            "2": {"scientificname": "Node2", "aphiaid": 2, "rank": "Phylum", "parent": "", "children": 0, "directchildren": 0}
        }
        
        result = algorithm.apply_algorithm(limited_data)
        # Should return all available nodes (2) even though min_nodes is 10
        assert len(result) == 2