import pandas as pd
import re
import os
import warnings

# Suppress warnings
warnings.simplefilter("ignore")

def clean_header(col_name):
    """Removes newlines and extra spaces from column headers."""
    if pd.isna(col_name):
        return ""
    return str(col_name).replace('\n', ' ').replace('  ', ' ').strip()

def sanitize_filename(text):
    """Creates a safe filename from the table title."""
    text = re.sub(r'[\\/*?:"<>|]', "", text)
    text = re.sub(r'\s+', '_', text)
    text = text.replace(',', '').replace('.', '')
    return text[:100]

def extract_tables_to_csv(file_path):
    if not os.path.exists(file_path):
        print(f"Error: File '{file_path}' not found.")
        return

    print(f"Processing file: {file_path}")
    xl = pd.ExcelFile(file_path)
    
    output_dir = "processed_tables_clean"
    os.makedirs(output_dir, exist_ok=True)
    
    for sheet_name in xl.sheet_names:
        if sheet_name in ["Instructions", "Table of contents", "Methodology", "Contact info", "Copyright and acknowledgement"]:
            continue
            
        print(f"  Scanning sheet: {sheet_name}")
        
        # --- Scenario A: Raw Data Sheets (Hidden Sheets) ---
        if "DATA" in sheet_name or "hide" in sheet_name:
            try:
                df = pd.read_excel(file_path, sheet_name=sheet_name, header=1)
                
                # Remove empty columns immediately
                df = df.dropna(axis=1, how='all')
                
                # Clean headers
                df.columns = [clean_header(c) for c in df.columns]
                
                # Filter out columns that ended up with empty strings as headers
                df = df.loc[:, [c != "" for c in df.columns]]
                
                if not df.empty:
                     df.iloc[:, 0] = df.iloc[:, 0].ffill()

                clean_name = sanitize_filename(sheet_name)
                output_filename = f"{output_dir}/{clean_name}.csv"
                df.to_csv(output_filename, index=False)
                print(f"    -> Exported raw data: {output_filename}")
            except Exception as e:
                print(f"    -> Error processing raw data sheet {sheet_name}: {e}")
            continue

        # --- Scenario B: Formatted Report Tables ---
        try:
            df_raw = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
        except Exception as e:
            print(f"    -> Could not read sheet {sheet_name}: {e}")
            continue

        table_starts = []
        for idx, row in df_raw.iterrows():
            first_cell = str(row[0]).strip()
            if re.match(r"^Table\s+\d+", first_cell):
                table_starts.append((idx, first_cell))
        
        if not table_starts:
            continue

        for i, (start_row_idx, title_text) in enumerate(table_starts):
            # 1. Generate Filename
            table_id_match = re.match(r"^(Table\s+\d+)", title_text)
            prefix = table_id_match.group(1).replace(' ', '_') if table_id_match else f"Table_{i+1}"
            clean_title = sanitize_filename(title_text)
            final_filename = f"{prefix}_{clean_title}" if not clean_title.startswith(prefix) else clean_title
            
            # 2. Locate Header
            header_row_idx = start_row_idx + 1
            if header_row_idx >= len(df_raw): continue

            # 3. Identify Valid Columns (CRITICAL FIX)
            # Get the row containing headers
            header_row_values = df_raw.iloc[header_row_idx]
            
            # Find indices of columns where the header is NOT NaN and NOT empty
            valid_col_indices = [
                idx for idx, val in enumerate(header_row_values) 
                if not pd.isna(val) and str(val).strip() != ''
            ]
            
            if not valid_col_indices:
                print(f"    -> Skipping {final_filename}: No valid headers found.")
                continue

            # 4. Determine End Row
            next_start_idx = table_starts[i+1][0] if i + 1 < len(table_starts) else len(df_raw)
            end_row_idx = next_start_idx
            for r in range(header_row_idx + 1, next_start_idx):
                cell_val = str(df_raw.iloc[r, 0]).strip()
                if cell_val.startswith("Notes") or cell_val.startswith("Sources"):
                    end_row_idx = r
                    break
            
            # 5. Extract ONLY valid columns
            df_table = df_raw.iloc[header_row_idx+1 : end_row_idx, valid_col_indices].copy()
            
            # 6. Set Headers
            final_headers = [clean_header(df_raw.iloc[header_row_idx, c]) for c in valid_col_indices]
            df_table.columns = final_headers

            # 7. Fix Merged Cells (Forward Fill)
            if not df_table.empty:
                df_table.iloc[:, 0] = df_table.iloc[:, 0].ffill()

            # 8. Clean Empty Rows
            df_table.dropna(how='all', axis=0, inplace=True)
            # Remove rows where first col is empty/nan (often artifacts)
            df_table = df_table[df_table.iloc[:, 0].astype(str) != 'nan']
            df_table = df_table[df_table.iloc[:, 0].astype(str) != '']

            # 9. Save
            output_path = f"{output_dir}/{final_filename}.csv"
            df_table.to_csv(output_path, index=False)
            print(f"    -> Extracted: {output_path}")

# --- Configuration ---
input_excel_file = 'data/care-children-youth-with-mental-disorders-data-tables-en.xlsx'

# --- Execution ---
if __name__ == "__main__":
    extract_tables_to_csv(input_excel_file)