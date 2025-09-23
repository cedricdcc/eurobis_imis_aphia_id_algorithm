"""
Core algorithm implementation for determining optimal Aphia IDs.
"""

from typing import Dict, List, Any, Tuple, Optional
from .utils import calculate_rank_value
from .api_client import AphiaApiClient
from .data_loader import DataLoader
from .visualization import Visualizer


class AphiaIdAlgorithm:
    """
    Main algorithm class for determining which Aphia IDs to use based on
    taxonomic tree structure and relevance calculations.
    """
    
    def __init__(self, 
                 max_nodes: int = 50,
                 amplifier: int = 10,
                 cache_file: str = "data_object.json",
                 base_path: str = "",
                 min_nodes: int = 1):
        """
        Initialize the algorithm.
        
        Args:
            max_nodes: Maximum number of nodes to select per DASID
            amplifier: Multiplier for rank scoring
            cache_file: Name of the cache file
            base_path: Base directory path for file operations
            min_nodes: Minimum number of nodes that the result must have for each DASID
        """
        self.max_nodes = max_nodes
        self.amplifier = amplifier
        self.base_path = base_path
        self.min_nodes = min_nodes
        
        # Initialize components
        self.data_loader = DataLoader(base_path)
        self.api_client = AphiaApiClient(cache_file, base_path)
        self.visualizer = Visualizer(base_path)
        
    def calculate_relevancy(self, node_data: Dict[str, Any]) -> float:
        """
        Calculate relevancy score for a node based on children counts.
        
        Args:
            node_data: Dictionary containing node information
            
        Returns:
            Relevancy score (children/direct_children ratio)
        """
        children = node_data.get("children", 0)
        direct_children = node_data.get("directchildren", 0)
        
        if direct_children > 0:
            return children / direct_children
        return 0.0
    
    def find_root_nodes(self, all_data: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """
        Find root nodes of the taxonomic tree based on children count.
        
        Args:
            all_data: Dictionary containing all taxonomic data
            
        Returns:
            Dictionary containing root nodes
        """
        # Sort by number of children in descending order
        sorted_indices = sorted(all_data.keys(), key=lambda x: all_data[x]["children"], reverse=True)
        
        # Return the node with most children as the root
        final_ids = {}
        if sorted_indices:
            root_key = sorted_indices[0]
            final_ids[root_key] = all_data[root_key]
            
        return final_ids
    
    def calculate_node_rankings(self, final_ids: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Calculate rankings for all nodes in final_ids based on relevancy and rank values.
        
        Args:
            final_ids: Dictionary of current final IDs
            
        Returns:
            List of nodes with their rankings, sorted by rank_value
        """
        relevancy_list = []
        
        for node_key, node_data in final_ids.items():
            # Calculate relevancy
            relevancy = self.calculate_relevancy(node_data)
            
            # Calculate rank value
            rank = node_data.get("rank", "")
            rank_value = calculate_rank_value(rank, self.amplifier)
            
            relevancy_list.append({
                "aphia_id": node_data["aphiaid"],
                "relevancy": relevancy,
                "rank_value": rank_value
            })
            
        # Sort by rank_value in descending order
        return sorted(relevancy_list, key=lambda x: x["rank_value"], reverse=True)
    
    def get_children_for_node(self, aphia_id: str, all_data: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Get all direct children for a given node.
        
        Args:
            aphia_id: The Aphia ID to find children for
            all_data: Dictionary containing all taxonomic data
            
        Returns:
            List of child node data
        """
        children = []
        for node_key, node_data in all_data.items():
            if str(node_data.get("parent", "")) == str(aphia_id):
                children.append(node_data)
        return children
    
    def _ensure_min_nodes(self, final_ids: Dict[str, Dict[str, Any]], 
                         all_data: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """
        Ensure minimum node count is met by adding available nodes.
        
        Args:
            final_ids: Current final IDs
            all_data: All available taxonomic data
            
        Returns:
            Updated final_ids with additional nodes to meet minimum count
        """
        current_count = len(final_ids)
        if current_count >= self.min_nodes:
            return final_ids
            
        # Get all available nodes that aren't already in final_ids
        available_nodes = []
        for node_key, node_data in all_data.items():
            if node_key not in final_ids:
                available_nodes.append((node_key, node_data))
        
        # Sort available nodes by relevancy score (descending)
        available_nodes.sort(key=lambda x: self.calculate_relevancy(x[1]), reverse=True)
        
        # Add nodes until we reach min_nodes or run out of available nodes
        nodes_to_add = self.min_nodes - current_count
        final_ids_copy = final_ids.copy()
        
        for i, (node_key, node_data) in enumerate(available_nodes):
            if i >= nodes_to_add:
                break
            final_ids_copy[node_key] = node_data
            
        if len(final_ids_copy) < self.min_nodes:
            print(f"Warning: Could only add {len(final_ids_copy)} total nodes (requested min_nodes: {self.min_nodes})")
            
        return final_ids_copy
    
    def apply_algorithm(self, all_data: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """
        Apply the main algorithm to select optimal Aphia IDs.
        
        Args:
            all_data: Dictionary containing all taxonomic data for a DASID
            
        Returns:
            Dictionary containing final selected IDs
        """
        # Step 1: Find root nodes
        final_ids = self.find_root_nodes(all_data)
        
        if not final_ids:
            return {}
            
        last_final_id_length = 0
        
        # Step 2: Iteratively add children and remove parents
        while len(final_ids) < self.max_nodes and len(final_ids) > last_final_id_length:
            try:
                # Calculate rankings for current nodes
                ranked_nodes = self.calculate_node_rankings(final_ids)
                
                if not ranked_nodes:
                    break
                    
                changed = False
                
                # Try each ranked node in order
                for ranked_node in ranked_nodes:
                    aphia_id = ranked_node["aphia_id"]
                    
                    # Get children of this node
                    children = self.get_children_for_node(str(aphia_id), all_data)
                    
                    if children:
                        # Check if adding children would exceed max_nodes
                        if len(children) + len(final_ids) <= self.max_nodes:
                            # Add all children to final_ids
                            for child in children:
                                final_ids[str(child["aphiaid"])] = child
                            
                            # Remove parent node
                            parent_key = None
                            for key, data in final_ids.items():
                                if data["aphiaid"] == aphia_id:
                                    parent_key = key
                                    break
                                    
                            if parent_key:
                                final_ids.pop(parent_key)
                            
                            changed = True
                            break
                            
                if not changed:
                    break
                    
                last_final_id_length = len(final_ids)
                
            except (IndexError, KeyError):
                break
        
        # Step 3: Ensure minimum node count is met
        if len(final_ids) < self.min_nodes:
            print(f"Warning: Selected {len(final_ids)} nodes, but min_nodes is {self.min_nodes}. Attempting to add more nodes.")
            final_ids = self._ensure_min_nodes(final_ids, all_data)
                
        return final_ids
    
    def process_dasid(self, dasid: int, aphia_ids: List[int], 
                     refresh_cache: bool = False,
                     save_visualization: bool = True,
                     show_visualization: bool = False) -> Dict[str, Any]:
        """
        Process a single DASID through the complete algorithm pipeline.
        
        Args:
            dasid: The DASID to process
            aphia_ids: List of Aphia IDs associated with the DASID
            refresh_cache: Whether to refresh the API cache
            save_visualization: Whether to save visualization to HTML file
            show_visualization: Whether to show visualization in browser
            
        Returns:
            Dictionary containing processing results
        """
        print(f"Processing DASID: {dasid}")
        
        # Step 1: Fetch and cache data from API
        dasid_data = self.api_client.fetch_and_cache_dasid_data(
            dasid, aphia_ids, refresh_cache
        )
        
        all_data = dasid_data["data"]
        
        if not all_data:
            print(f"No data found for DASID {dasid}")
            return {"final_ids": {}, "csv_file": None, "html_file": None, "sunburst_file": None}
        
        # Step 2: Apply the algorithm
        final_ids = self.apply_algorithm(all_data)
        
        print(f"DASID {dasid}: Selected {len(final_ids)} final IDs from {len(aphia_ids)} original IDs")
        
        # Step 3: Save results to CSV
        csv_file = self.visualizer.write_results_to_csv(final_ids, dasid)
        print(f"Results saved to: {csv_file}")
        
        # Step 4: Generate visualizations
        html_file = None
        sunburst_file = None
        
        if save_visualization:
            html_file = self.visualizer.create_tree_visualization(
                final_ids, all_data, dasid, aphia_ids
            )
            print(f"Tree visualization saved to: {html_file}")
            
            # Generate sunburst chart
            sunburst_file = self.visualizer.create_sunburst_chart(
                final_ids, all_data, dasid, aphia_ids
            )
            print(f"Sunburst visualization saved to: {sunburst_file}")
            
        if show_visualization:
            self.visualizer.show_tree_visualization(final_ids, all_data, dasid, aphia_ids)
            self.visualizer.show_sunburst_chart(final_ids, all_data, dasid, aphia_ids)
        
        return {
            "final_ids": final_ids,
            "csv_file": csv_file,
            "html_file": html_file,
            "sunburst_file": sunburst_file,
            "all_data": all_data
        }
    
    def process_csv_file(self, csv_file: str = "aphia_ids_to_imis.csv",
                        refresh_cache: bool = False,
                        save_visualizations: bool = True,
                        show_visualizations: bool = False) -> Dict[int, Dict[str, Any]]:
        """
        Process all DASIDs from a CSV file through the complete algorithm pipeline.
        
        Args:
            csv_file: Path to the CSV file containing DASID-Aphia ID mappings
            refresh_cache: Whether to refresh the API cache
            save_visualizations: Whether to save visualizations to HTML files
            show_visualizations: Whether to show visualizations in browser
            
        Returns:
            Dictionary mapping DASID to processing results
        """
        # Load and process CSV data
        dasid_mapping = self.data_loader.load_and_process_csv(csv_file)
        
        results = {}
        
        for dasid, aphia_ids in dasid_mapping.items():
            try:
                result = self.process_dasid(
                    dasid, 
                    aphia_ids, 
                    refresh_cache,
                    save_visualizations,
                    show_visualizations
                )
                results[dasid] = result
                
            except Exception as e:
                print(f"Error processing DASID {dasid}: {e}")
                results[dasid] = {"error": str(e)}
                
        return results