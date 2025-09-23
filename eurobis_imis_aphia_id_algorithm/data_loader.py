"""
Data loading utilities for CSV files and DASID processing.
"""

import pandas as pd
import os
from typing import Dict, List, Optional


class DataLoader:
    """Class to handle loading and processing of CSV data."""
    
    def __init__(self, base_path: str = ""):
        """
        Initialize the DataLoader.
        
        Args:
            base_path: Base directory path for file operations
        """
        self.base_path = base_path
        
    def load_aphia_ids_csv(self, csv_file: str = "aphia_ids_to_imis.csv") -> pd.DataFrame:
        """
        Load the CSV file containing Aphia ID to IMIS mappings.
        
        Args:
            csv_file: Name of the CSV file to load
            
        Returns:
            DataFrame containing the loaded data
            
        Raises:
            FileNotFoundError: If the CSV file doesn't exist
        """
        file_path = os.path.join(self.base_path, csv_file) if self.base_path else csv_file
        
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"CSV file not found: {file_path}")
            
        return pd.read_csv(file_path)
    
    def group_aphia_ids_by_dasid(self, df: pd.DataFrame) -> Dict[int, List[int]]:
        """
        Group Aphia IDs by DASID from the DataFrame.
        
        Args:
            df: DataFrame containing IMIS_DasID and aphia_id columns
            
        Returns:
            Dictionary mapping DASID to list of Aphia IDs
            
        Raises:
            ValueError: If required columns are missing
        """
        required_columns = ["IMIS_DasID", "aphia_id"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
            
        children = {}
        diff_dasids = df["IMIS_DasID"].unique()
        
        for dasid in diff_dasids:
            children[dasid] = df[df["IMIS_DasID"] == dasid]["aphia_id"].tolist()
            
        return children
    
    def load_and_process_csv(self, csv_file: str = "aphia_ids_to_imis.csv") -> Dict[int, List[int]]:
        """
        Load CSV file and process it to group Aphia IDs by DASID.
        
        Args:
            csv_file: Name of the CSV file to load
            
        Returns:
            Dictionary mapping DASID to list of Aphia IDs
        """
        df = self.load_aphia_ids_csv(csv_file)
        return self.group_aphia_ids_by_dasid(df)