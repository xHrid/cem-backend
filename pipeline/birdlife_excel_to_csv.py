#!/usr/bin/env python3
"""
Convert BirdLife taxonomy Excel to IUCN lookup CSV.
Inspects and identifies columns, then extracts scientific_name and iucn_category.
"""

import sys
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    print("ERROR: pandas not installed. Run: pip install pandas openpyxl")
    sys.exit(1)


def inspect_excel_columns(excel_path):
    """Inspect Excel file and print column structure."""
    excel_path = Path(excel_path).resolve()
    
    if not excel_path.is_file():
        print(f"ERROR: Excel file not found: {excel_path}")
        return False
    
    try:
        print(f"\nInspecting: {excel_path}")
        df = pd.read_excel(excel_path, sheet_name=0, header=None)
        print(f"Sheet shape: {df.shape}")
        print(f"\nRow 3 (headers):")
        headers = df.iloc[3].tolist()
        for i, h in enumerate(headers):
            print(f"  Col {i}: {h}")
        return True
    except Exception as e:
        print(f"ERROR reading Excel: {e}")
        return False


def convert_birdlife_excel_to_iucn_csv(excel_path, output_csv_path):
    """Read BirdLife taxonomy Excel and write filtered CSV with scientific_name and iucn_category."""
    
    excel_path = Path(excel_path).resolve()
    output_csv_path = Path(output_csv_path).resolve()
    
    if not excel_path.is_file():
        print(f"ERROR: Excel file not found: {excel_path}")
        return False
    
    try:
        print(f"\nReading Excel file: {excel_path}")
        # Skip first 4 rows and use row 3 (0-indexed) as header
        df = pd.read_excel(excel_path, sheet_name=0, skiprows=3)
    except Exception as e:
        print(f"ERROR reading Excel: {e}")
        return False
    
    print(f"Excel shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    
    # Normalize column names to lowercase for matching
    df_lower_cols = {col: str(col).strip().lower() for col in df.columns}
    
    # Find the scientific name and IUCN category columns
    sci_col = None
    iucn_col = None
    
    for orig_col, lower_col in df_lower_cols.items():
        if 'scientific' in lower_col and 'name' in lower_col:
            sci_col = orig_col
        if 'iucn' in lower_col and ('category' in lower_col or '2025' in lower_col or 'list' in lower_col):
            iucn_col = orig_col
    
    # Fallback: look for common alternatives
    if not sci_col:
        for orig_col, lower_col in df_lower_cols.items():
            if 'binomial' in lower_col or 'species' in lower_col:
                sci_col = orig_col
                break
    
    if not iucn_col:
        for orig_col, lower_col in df_lower_cols.items():
            if 'status' in lower_col or 'threat' in lower_col or 'conservation' in lower_col or 'red list' in lower_col:
                iucn_col = orig_col
                break
    
    if not sci_col or not iucn_col:
        print(f"ERROR: Could not identify required columns")
        print(f"  Scientific name column: {sci_col}")
        print(f"  IUCN category column: {iucn_col}")
        print(f"Available columns: {list(df.columns)}")
        return False
    
    print(f"Using columns: scientific_name='{sci_col}', iucn_category='{iucn_col}'")
    
    # Extract and clean
    result = df[[sci_col, iucn_col]].copy()
    result.columns = ['scientific_name', 'iucn_category']
    
    # Remove rows with missing values
    result = result.dropna(subset=['scientific_name', 'iucn_category'])
    result['scientific_name'] = result['scientific_name'].astype(str).str.strip()
    result['iucn_category'] = result['iucn_category'].astype(str).str.strip()
    
    # Remove empty rows
    result = result[(result['scientific_name'] != '') & (result['iucn_category'] != '')]
    
    # Remove duplicates (keep first occurrence)
    result = result.drop_duplicates(subset=['scientific_name'], keep='first')
    
    # Sort by scientific name
    result = result.sort_values('scientific_name').reset_index(drop=True)
    
    # Write CSV
    try:
        output_csv_path.parent.mkdir(parents=True, exist_ok=True)
        result.to_csv(output_csv_path, index=False)
        print(f"\nSuccess! Wrote {len(result)} species to: {output_csv_path}")
        print(f"Sample rows:")
        print(result.head(10))
        return True
    except Exception as e:
        print(f"ERROR writing CSV: {e}")
        return False


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python birdlife_excel_to_csv.py inspect <excel_file>")
        print("  python birdlife_excel_to_csv.py convert <excel_file> [output_csv]")
        sys.exit(1)
    
    cmd = sys.argv[1]
    
    if cmd == "inspect" and len(sys.argv) >= 3:
        inspect_excel_columns(sys.argv[2])
    
    elif cmd == "convert":
        excel_file = sys.argv[2] if len(sys.argv) >= 3 else "../excel_file.xlsx"
        output_file = sys.argv[3] if len(sys.argv) >= 4 else "birdlife_iucn_lookup.csv"
        convert_birdlife_excel_to_iucn_csv(excel_file, output_file)
    
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
