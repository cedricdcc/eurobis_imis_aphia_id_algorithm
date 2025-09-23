#!/usr/bin/env python3
"""
Simple command-line interface for the eurobis_imis_aphia_id_algorithm package.
"""

import argparse
import sys
import os
from eurobis_imis_aphia_id_algorithm import AphiaIdAlgorithm


def main():
    """Main CLI function."""
    parser = argparse.ArgumentParser(
        description="Process Aphia IDs using the EurOBIS IMIS algorithm"
    )
    
    parser.add_argument(
        "csv_file",
        help="Path to CSV file containing IMIS_DasID and aphia_id columns"
    )
    
    parser.add_argument(
        "--max-nodes",
        type=int,
        default=50,
        help="Maximum number of nodes to select per DASID (default: 50)"
    )
    
    parser.add_argument(
        "--amplifier", 
        type=int,
        default=10,
        help="Amplifier for rank scoring (default: 10)"
    )
    
    parser.add_argument(
        "--cache-file",
        default="data_object.json",
        help="Name of the cache file (default: data_object.json)"
    )
    
    parser.add_argument(
        "--refresh-cache",
        action="store_true",
        help="Refresh the API cache (re-fetch all data)"
    )
    
    parser.add_argument(
        "--no-visualizations",
        action="store_true",
        help="Skip generating tree visualizations"
    )
    
    parser.add_argument(
        "--show-visualizations",
        action="store_true", 
        help="Show tree visualizations in browser"
    )
    
    args = parser.parse_args()
    
    # Check if CSV file exists
    if not os.path.exists(args.csv_file):
        print(f"Error: CSV file '{args.csv_file}' not found", file=sys.stderr)
        sys.exit(1)
    
    # Get base path from CSV file location
    base_path = os.path.dirname(os.path.abspath(args.csv_file))
    csv_filename = os.path.basename(args.csv_file)
    
    # Initialize algorithm
    algorithm = AphiaIdAlgorithm(
        max_nodes=args.max_nodes,
        amplifier=args.amplifier,
        cache_file=args.cache_file,
        base_path=base_path
    )
    
    try:
        print(f"Processing {args.csv_file}...")
        results = algorithm.process_csv_file(
            csv_file=csv_filename,
            refresh_cache=args.refresh_cache,
            save_visualizations=not args.no_visualizations,
            show_visualizations=args.show_visualizations
        )
        
        # Print results
        print("\nProcessing Results:")
        print("-" * 30)
        
        for dasid, result in results.items():
            if "error" in result:
                print(f"DASID {dasid}: ERROR - {result['error']}")
            else:
                final_count = len(result["final_ids"])
                print(f"DASID {dasid}: {final_count} final IDs selected")
                if result.get("csv_file"):
                    print(f"  Output: {result['csv_file']}")
        
        print(f"\nProcessed {len(results)} DASIDs successfully!")
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()