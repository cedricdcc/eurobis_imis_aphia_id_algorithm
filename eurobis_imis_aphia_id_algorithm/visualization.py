"""
Visualization utilities for tree maps and data output.
"""

import csv
import os
from typing import Dict, List, Any, Optional
import plotly.express as px


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