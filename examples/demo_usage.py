#!/usr/bin/env python3
"""
Example script demonstrating the usage of the eurobis_imis_aphia_id_algorithm package.

This script replicates the behavior of the original demo_algorithm.py script
but uses the new modular package structure.
"""

import os
from eurobis_imis_aphia_id_algorithm import AphiaIdAlgorithm


def main():
    """
    Main function that demonstrates the package usage.
    """
    print("EurOBIS IMIS Aphia ID Algorithm - Example Usage")
    print("=" * 50)
    
    # Get the directory of this script for relative paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Initialize the algorithm with the same parameters as the original script
    algorithm = AphiaIdAlgorithm(
        max_nodes=50,  # Same as 'nodes' variable in original
        amplifier=10,  # Same as original
        cache_file="data_object.json",  # Same cache file name
        base_path=script_dir  # Use script directory as base path
    )
    
    # Check if CSV file exists
    csv_file = "aphia_ids_to_imis.csv"
    csv_path = os.path.join(script_dir, csv_file)
    
    if not os.path.exists(csv_path):
        print(f"Warning: {csv_file} not found in {script_dir}")
        print("Please ensure the CSV file exists in the same directory as this script.")
        print("The CSV file should contain columns: 'IMIS_DasID' and 'aphia_id'")
        return
    
    try:
        # Process all DASIDs from the CSV file
        print(f"Processing data from {csv_file}...")
        results = algorithm.process_csv_file(
            csv_file=csv_file,
            refresh_cache=False,  # Set to True to refresh the cache
            save_visualizations=True,  # Save HTML tree visualizations
            show_visualizations=False  # Set to True to show visualizations in browser
        )
        
        # Print summary of results
        print("\n" + "=" * 50)
        print("Processing Summary:")
        print("=" * 50)
        
        for dasid, result in results.items():
            if "error" in result:
                print(f"DASID {dasid}: ERROR - {result['error']}")
            else:
                final_count = len(result["final_ids"])
                print(f"DASID {dasid}: {final_count} final IDs selected")
                print(f"  - CSV output: {result['csv_file']}")
                if result["html_file"]:
                    print(f"  - Tree visualization: {result['html_file']}")
        
        print("\nProcessing completed successfully!")
        
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please ensure the CSV file exists and has the correct format.")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()