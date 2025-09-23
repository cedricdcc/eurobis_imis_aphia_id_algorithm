"""
API client for interacting with the Aphia marine species database.
"""

import requests
import json
import time
import os
from typing import Dict, List, Any, Optional
from .utils import open_cache_file, write_cache_data, get_child_if_dict


class AphiaApiClient:
    """Client for interacting with the Aphia API and managing cache."""
    
    BASE_URL = "https://www.marinespecies.org/rest/AphiaClassificationByAphiaID"
    DEFAULT_CACHE_FILE = "data_object.json"
    
    def __init__(self, cache_file: str = DEFAULT_CACHE_FILE, base_path: str = "", delay: float = 1.5):
        """
        Initialize the API client.
        
        Args:
            cache_file: Name of the cache file
            base_path: Base directory path for file operations
            delay: Delay between API requests in seconds
        """
        self.cache_file = cache_file
        self.base_path = base_path
        self.delay = delay
        self._cached_data: Optional[Dict[str, Any]] = None
        
    @property
    def cache_file_path(self) -> str:
        """Get the full path to the cache file."""
        return os.path.join(self.base_path, self.cache_file) if self.base_path else self.cache_file
        
    def load_cache(self, force_refresh: bool = False) -> Dict[str, Any]:
        """
        Load cached data from file.
        
        Args:
            force_refresh: If True, bypass cached data and reload from file
            
        Returns:
            Dictionary containing cached data
        """
        if self._cached_data is None or force_refresh:
            try:
                self._cached_data = open_cache_file(self.cache_file, self.base_path)
            except (FileNotFoundError, json.JSONDecodeError):
                # Create empty cache file if it doesn't exist
                with open(self.cache_file_path, 'w') as f:
                    json.dump({}, f)
                self._cached_data = {}
                
        return self._cached_data
    
    def save_cache(self, cached_data: Optional[Dict[str, Any]] = None) -> None:
        """
        Save cached data to file.
        
        Args:
            cached_data: Data to cache. If None, uses internal cached data.
        """
        data_to_save = cached_data if cached_data is not None else self._cached_data
        if data_to_save is not None:
            write_cache_data(data_to_save, self.cache_file, self.base_path)
    
    def get_classification_by_aphia_id(self, aphia_id: int) -> Optional[Dict[str, Any]]:
        """
        Get taxonomic classification for an Aphia ID from the API.
        
        Args:
            aphia_id: The Aphia ID to query
            
        Returns:
            JSON response from the API or None if request failed
        """
        url = f"{self.BASE_URL}/{aphia_id}"
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for Aphia ID {aphia_id}: {e}")
            return None
    
    def update_cache(self, response: Dict[str, Any], cache: Dict[str, Any], parent_id: str = "") -> None:
        """
        Update cache with API response data.
        
        Args:
            response: API response data
            cache: Cache dictionary to update
            parent_id: ID of the parent node
        """
        child = response
        parent_list = []
        aphia_id_list = []
        scientific_name_list = []
        rank_list = []
        
        get_child_if_dict(
            child, parent_id, parent_list, aphia_id_list, scientific_name_list, rank_list
        )
        
        # Process each extracted item
        for i, aphia_id in enumerate(aphia_id_list):
            parent = parent_list[i]
            scientific_name = scientific_name_list[i]
            rank = rank_list[i]
            
            # Add to cache if not already present
            if str(aphia_id) not in cache:
                cache[str(aphia_id)] = {
                    "scientificname": scientific_name,
                    "aphiaid": aphia_id,
                    "rank": rank,
                    "parent": parent,
                    "children": 0,
                    "directchildren": 0,
                }
                
                # Update parent's children count
                if parent != "" and i != 0:
                    if str(parent) in cache:
                        cache[str(parent)]["children"] += 1
                        cache[str(parent)]["directchildren"] += 1
            else:
                # Update parent's children count
                if parent != "" and i != 0:
                    if str(parent) in cache:
                        cache[str(parent)]["children"] += 1
    
    def fetch_and_cache_dasid_data(self, dasid: int, aphia_ids: List[int], 
                                  refresh_cache: bool = False) -> Dict[str, Any]:
        """
        Fetch and cache data for all Aphia IDs associated with a DASID.
        
        Args:
            dasid: The DASID being processed
            aphia_ids: List of Aphia IDs to fetch
            refresh_cache: Whether to refresh the cache
            
        Returns:
            Dictionary containing cached data for the DASID
        """
        cached_data = self.load_cache()
        
        if refresh_cache:
            cached_data = {}
            
        # Initialize DASID structure in cache
        dasid_str = str(dasid)
        if dasid_str not in cached_data:
            cached_data[dasid_str] = {
                "data": {},
                "urls_done": []
            }
            
        # Ensure required keys exist
        if "urls_done" not in cached_data[dasid_str]:
            cached_data[dasid_str]["urls_done"] = []
        if "data" not in cached_data[dasid_str]:
            cached_data[dasid_str]["data"] = {}
            
        print(f"Beginning search on DASID: {dasid}")
        
        for i, aphia_id in enumerate(aphia_ids, 1):
            # Progress reporting
            percentage_covered = (i / len(aphia_ids)) * 100
            if i % 10 == 0 or percentage_covered % 10 < (100 / len(aphia_ids)):
                print(f"{dasid} | {percentage_covered:.1f}% done | {i}/{len(aphia_ids)}")
                
            url = f"{self.BASE_URL}/{aphia_id}"
            
            # Skip if already processed
            if url in cached_data[dasid_str]["urls_done"]:
                continue
                
            # Fetch data from API
            api_response = self.get_classification_by_aphia_id(aphia_id)
            if api_response:
                self.update_cache(api_response, cached_data[dasid_str]["data"], parent_id="")
                cached_data[dasid_str]["urls_done"].append(url)
                
                # Save progress periodically
                if i % 10 == 0:
                    self.save_cache(cached_data)
                    
                # Rate limiting
                time.sleep(self.delay)
                
        # Final save
        self.save_cache(cached_data)
        self._cached_data = cached_data
        
        return cached_data[dasid_str]