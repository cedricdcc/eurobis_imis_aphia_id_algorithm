"""
Visualization utilities for tree maps and data output.
"""

import csv
import os
from typing import Dict, List, Any, Optional, Tuple
import plotly.express as px
import plotly.graph_objects as go


class Visualizer:
    """Class to handle visualization and output generation."""
    
    def __init__(self, base_path: str = ""):
        """
        Initialize the visualizer.
        
        Args:
            base_path: Base directory path for file operations
        """
        self.base_path = base_path
    
    def write_results_to_csv(self, final_ids: Dict[str, Dict[str, Any]], 
                           dasid: int, output_file: Optional[str] = None) -> str:
        """
        Write final results to CSV file.
        
        Args:
            final_ids: Dictionary of final selected IDs and their data
            dasid: The DASID being processed
            output_file: Output file name (if None, uses default naming)
            
        Returns:
            Path to the created CSV file
        """
        if output_file is None:
            output_file = f"{dasid}_chosen_aphia_ids.csv"
            
        file_path = os.path.join(self.base_path, output_file) if self.base_path else output_file
        
        # Convert final_ids to list of dictionaries
        csv_list_final_ids = list(final_ids.values())
        
        # Write CSV file
        with open(file_path, 'w', newline='') as csvfile:
            fieldnames = ['scientificname', 'aphiaid', 'rank', 'parent', 'children', 'directchildren']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(csv_list_final_ids)
            
        return file_path
    
    def create_tree_visualization(self, final_ids: Dict[str, Dict[str, Any]], 
                                all_data: Dict[str, Dict[str, Any]], 
                                dasid: int, original_aphia_ids: List[int],
                                output_file: Optional[str] = None) -> str:
        """
        Create an interactive tree map visualization.
        
        Args:
            final_ids: Dictionary of final selected IDs
            all_data: Dictionary of all cached taxonomic data
            dasid: The DASID being processed
            original_aphia_ids: Original list of Aphia IDs for this DASID
            output_file: Output HTML file name (if None, uses default naming)
            
        Returns:
            Path to the created HTML file
        """
        if output_file is None:
            output_file = f"{dasid}_tree_view.html"
            
        file_path = os.path.join(self.base_path, output_file) if self.base_path else output_file
        
        # Convert final_ids to list for processing
        csv_list_final_ids = list(final_ids.values())
        
        # Prepare data for visualization
        names = []
        scientific_names = []
        scientific_names_parents = []
        parents = []
        ranks = []
        childrens = []
        colors = []
        
        # Process final selected IDs
        for row in csv_list_final_ids:
            names.append(row["aphiaid"])
            scientific_names.append(row["scientificname"])
            parents.append(row["parent"])
            ranks.append(row["rank"])
            childrens.append(row["children"])
            
            # Find parent scientific name
            for node, value_node in all_data.items():
                if str(row["parent"]) == node:
                    scientific_names_parents.append(value_node["scientificname"])
                    break
            else:
                scientific_names_parents.append("")
                
            # Color code based on whether it's in original data
            if row["aphiaid"] in original_aphia_ids:
                colors.append("pink")
            else:
                colors.append("royalblue")
        
        # Add remaining nodes from all_data
        sorted_all_data = sorted(all_data.keys(), key=lambda x: all_data[x]["children"], reverse=True)
        
        for node in sorted_all_data:
            node_data = all_data[node]
            if int(node) not in names:
                names.append(node_data["aphiaid"])
                scientific_names.append(node_data["scientificname"])
                parents.append(node_data["parent"])
                ranks.append(node_data["rank"])
                childrens.append("not important")
                
                # Color coding for additional nodes
                if node_data["aphiaid"] in original_aphia_ids:
                    colors.append("red")
                else:
                    colors.append("lightgrey")
                    
                # Find parent scientific name
                if str(node_data["parent"]) == "":
                    scientific_names_parents.append("")
                else:
                    for nodeu, value_nodeu in all_data.items():
                        if str(node_data["parent"]) == nodeu:
                            scientific_names_parents.append(value_nodeu["scientificname"])
                            break
                    else:
                        scientific_names_parents.append("")
        
        # Create tree map
        fig = px.treemap(
            names=names,
            parents=parents,
            title=f"DASID {dasid} tree view {len(final_ids)}/{len(original_aphia_ids)} (#final/#begin AphiaIDs)",
            color=colors,
            color_discrete_map={
                '(?)': 'lightgrey',
                'lightgrey': 'lightgrey',
                'royalblue': 'royalblue',
                'red': 'red',
                'pink': 'pink'
            },
            hover_name=scientific_names,
            hover_data={
                "rank": ranks, 
                "children reduced": childrens, 
                "parent": scientific_names_parents
            }
        )
        
        fig.update_traces(root_color="lightgrey")
        fig.update_layout(margin=dict(t=25, l=10, r=10, b=10))
        fig.write_html(file_path)
        
        return file_path
    
    def show_tree_visualization(self, final_ids: Dict[str, Dict[str, Any]], 
                              all_data: Dict[str, Dict[str, Any]], 
                              dasid: int, original_aphia_ids: List[int]):
        """
        Show the tree visualization in browser (if in interactive environment).
        
        Args:
            final_ids: Dictionary of final selected IDs
            all_data: Dictionary of all cached taxonomic data
            dasid: The DASID being processed
            original_aphia_ids: Original list of Aphia IDs for this DASID
        """
        # Create the figure following the same logic as create_tree_visualization
        csv_list_final_ids = list(final_ids.values())
        
        names = []
        scientific_names = []
        scientific_names_parents = []
        parents = []
        ranks = []
        childrens = []
        colors = []
        
        # Process final selected IDs
        for row in csv_list_final_ids:
            names.append(row["aphiaid"])
            scientific_names.append(row["scientificname"])
            parents.append(row["parent"])
            ranks.append(row["rank"])
            childrens.append(row["children"])
            
            # Find parent scientific name
            for node, value_node in all_data.items():
                if str(row["parent"]) == node:
                    scientific_names_parents.append(value_node["scientificname"])
                    break
            else:
                scientific_names_parents.append("")
                
            # Color code
            if row["aphiaid"] in original_aphia_ids:
                colors.append("pink")
            else:
                colors.append("royalblue")
        
        # Add remaining nodes
        sorted_all_data = sorted(all_data.keys(), key=lambda x: all_data[x]["children"], reverse=True)
        
        for node in sorted_all_data:
            node_data = all_data[node]
            if int(node) not in names:
                names.append(node_data["aphiaid"])
                scientific_names.append(node_data["scientificname"])
                parents.append(node_data["parent"])
                ranks.append(node_data["rank"])
                childrens.append("not important")
                
                if node_data["aphiaid"] in original_aphia_ids:
                    colors.append("red")
                else:
                    colors.append("lightgrey")
                    
                # Find parent scientific name
                if str(node_data["parent"]) == "":
                    scientific_names_parents.append("")
                else:
                    for nodeu, value_nodeu in all_data.items():
                        if str(node_data["parent"]) == nodeu:
                            scientific_names_parents.append(value_nodeu["scientificname"])
                            break
                    else:
                        scientific_names_parents.append("")
        
        # Create and show figure
        fig = px.treemap(
            names=names,
            parents=parents,
            title=f"DASID {dasid} tree view {len(final_ids)}/{len(original_aphia_ids)} (#final/#begin AphiaIDs)",
            color=colors,
            color_discrete_map={
                '(?)': 'lightgrey',
                'lightgrey': 'lightgrey', 
                'royalblue': 'royalblue',
                'red': 'red',
                'pink': 'pink'
            },
            hover_name=scientific_names,
            hover_data={
                "rank": ranks,
                "children reduced": childrens,
                "parent": scientific_names_parents
            }
        )
        
        fig.update_traces(root_color="lightgrey")
        fig.update_layout(margin=dict(t=25, l=10, r=10, b=10))
        fig.show()
    
    def create_sunburst_chart(self, final_ids: Dict[str, Dict[str, Any]], 
                            all_data: Dict[str, Dict[str, Any]], 
                            dasid: int, original_aphia_ids: List[int],
                            output_file: Optional[str] = None) -> str:
        """
        Create a sunburst chart visualization showing hierarchical relationships.
        
        Args:
            final_ids: Dictionary of final selected IDs
            all_data: Dictionary of all cached taxonomic data
            dasid: The DASID being processed
            original_aphia_ids: Original list of Aphia IDs for this DASID
            output_file: Output HTML file name (if None, uses default naming)
            
        Returns:
            Path to the created HTML file
        """
        if output_file is None:
            output_file = f"{dasid}_sunburst.html"
            
        file_path = os.path.join(self.base_path, output_file) if self.base_path else output_file
        
        # Build lists for sunburst chart using only ids, labels, and parents
        ids = []
        labels = []
        parents = []
        
        # Helper function to build full taxonomic path for each node
        def build_taxonomy_path(node_key: str, all_data: Dict[str, Dict[str, Any]]) -> List[Tuple[str, str, str]]:
            """Build full taxonomic path from root to node. Returns list of (id, label, parent_id) tuples."""
            path = []
            current = node_key
            visited = set()  # Prevent infinite loops
            
            while current and current != "" and current not in visited:
                visited.add(current)
                if current in all_data:
                    node_data = all_data[current]
                    node_id = f"{node_data['aphiaid']}"
                    node_label = f"{node_data['scientificname']}<br>({node_data['rank']})"
                    parent_id = str(node_data.get("parent", "")) if node_data.get("parent", "") != "" else ""
                    path.append((node_id, node_label, parent_id))
                    current = str(node_data.get("parent", ""))
                else:
                    break
                    
            return list(reversed(path))  # Root to leaf order
        
        # Collect all unique nodes from the taxonomic paths
        unique_nodes = {}  # aphia_id -> (label, parent_id, is_final, is_original)
        
        # Process final IDs and their complete taxonomic paths
        for node_key, node_data in final_ids.items():
            path = build_taxonomy_path(node_key, all_data)
            for node_id, node_label, parent_id in path:
                is_final = (node_id == node_key)
                is_original = int(node_id) in original_aphia_ids
                
                if node_id not in unique_nodes:
                    unique_nodes[node_id] = (node_label, parent_id, is_final, is_original)
        
        # Add any additional parent nodes from all_data to ensure complete hierarchy
        for node_key, node_data in all_data.items():
            path = build_taxonomy_path(node_key, all_data)
            for node_id, node_label, parent_id in path:
                is_final = node_id in final_ids
                is_original = int(node_id) in original_aphia_ids
                
                if node_id not in unique_nodes:
                    unique_nodes[node_id] = (node_label, parent_id, is_final, is_original)
        
        # Convert to lists for Plotly sunburst
        colors = []
        for node_id, (node_label, parent_id, is_final, is_original) in unique_nodes.items():
            ids.append(node_id)
            labels.append(node_label)
            parents.append(parent_id)
            
            # Determine color based on node type
            if is_final and is_original:
                colors.append("pink")  # Final and original
            elif is_final:
                colors.append("royalblue")  # Final but not original
            elif is_original:
                colors.append("red")  # Original but not final
            else:
                colors.append("lightgrey")  # Neither
        
        # Create sunburst chart without values
        fig = go.Figure(go.Sunburst(
            ids=ids,
            labels=labels,
            parents=parents,
            hovertemplate='<b>%{label}</b><br>ID: %{id}<extra></extra>',
            maxdepth=8,  # Allow deeper hierarchy
        ))
        
        fig.update_traces(
            marker_colors=colors,
            marker_line=dict(color="white", width=1)
        )
        
        fig.update_layout(
            title=f"DASID {dasid} Taxonomic Sunburst Chart<br>Final: {len(final_ids)} | Original: {len(original_aphia_ids)}",
            font_size=12,
            margin=dict(t=60, l=10, r=10, b=10)
        )
        
        fig.write_html(file_path)
        return file_path
    
    def show_sunburst_chart(self, final_ids: Dict[str, Dict[str, Any]], 
                          all_data: Dict[str, Dict[str, Any]], 
                          dasid: int, original_aphia_ids: List[int]):
        """
        Show the sunburst chart visualization in browser.
        
        Args:
            final_ids: Dictionary of final selected IDs
            all_data: Dictionary of all cached taxonomic data
            dasid: The DASID being processed
            original_aphia_ids: Original list of Aphia IDs for this DASID
        """
        # Build lists for sunburst chart using only ids, labels, and parents
        ids = []
        labels = []
        parents = []
        
        # Helper function to build full taxonomic path for each node
        def build_taxonomy_path(node_key: str, all_data: Dict[str, Dict[str, Any]]) -> List[Tuple[str, str, str]]:
            """Build full taxonomic path from root to node. Returns list of (id, label, parent_id) tuples."""
            path = []
            current = node_key
            visited = set()  # Prevent infinite loops
            
            while current and current != "" and current not in visited:
                visited.add(current)
                if current in all_data:
                    node_data = all_data[current]
                    node_id = f"{node_data['aphiaid']}"
                    node_label = f"{node_data['scientificname']}<br>({node_data['rank']})"
                    parent_id = str(node_data.get("parent", "")) if node_data.get("parent", "") != "" else ""
                    path.append((node_id, node_label, parent_id))
                    current = str(node_data.get("parent", ""))
                else:
                    break
                    
            return list(reversed(path))  # Root to leaf order
        
        # Collect all unique nodes from the taxonomic paths
        unique_nodes = {}  # aphia_id -> (label, parent_id, is_final, is_original)
        
        # Process final IDs and their complete taxonomic paths
        for node_key, node_data in final_ids.items():
            path = build_taxonomy_path(node_key, all_data)
            for node_id, node_label, parent_id in path:
                is_final = (node_id == node_key)
                is_original = int(node_id) in original_aphia_ids
                
                if node_id not in unique_nodes:
                    unique_nodes[node_id] = (node_label, parent_id, is_final, is_original)
        
        # Add any additional parent nodes from all_data to ensure complete hierarchy
        for node_key, node_data in all_data.items():
            path = build_taxonomy_path(node_key, all_data)
            for node_id, node_label, parent_id in path:
                is_final = node_id in final_ids
                is_original = int(node_id) in original_aphia_ids
                
                if node_id not in unique_nodes:
                    unique_nodes[node_id] = (node_label, parent_id, is_final, is_original)
        
        # Convert to lists for Plotly sunburst
        colors = []
        for node_id, (node_label, parent_id, is_final, is_original) in unique_nodes.items():
            ids.append(node_id)
            labels.append(node_label)
            parents.append(parent_id)
            
            # Determine color based on node type
            if is_final and is_original:
                colors.append("pink")  # Final and original
            elif is_final:
                colors.append("royalblue")  # Final but not original
            elif is_original:
                colors.append("red")  # Original but not final
            else:
                colors.append("lightgrey")  # Neither
        
        # Create and show sunburst chart without values
        fig = go.Figure(go.Sunburst(
            ids=ids,
            labels=labels,
            parents=parents,
            hovertemplate='<b>%{label}</b><br>ID: %{id}<extra></extra>',
            maxdepth=8,  # Allow deeper hierarchy
        ))
        
        fig.update_traces(
            marker_colors=colors,
            marker_line=dict(color="white", width=1)
        )
        
        fig.update_layout(
            title=f"DASID {dasid} Taxonomic Sunburst Chart<br>Final: {len(final_ids)} | Original: {len(original_aphia_ids)}",
            font_size=12,
            margin=dict(t=60, l=10, r=10, b=10)
        )
        
        fig.show()