from main import supabase  # Import the Supabase client from main.py
import pandas as pd
import re
from openpyxl.utils import column_index_from_string
# from tabulate import tabulate
# === Step 1: Upload File in Google Colab ===
# from google.colab import files
# uploaded = files.upload()  # Manually upload .xlsx file
# Get the uploaded filename
file_path = r"E:\proj1\F1 PROD REPORT JAN 2025.xlsx"
furnace_identifier = re.search(r"F\d+", file_path).group(0) if re.search(r"F\d+", file_path) else None
# === Step 2: Read All Sheets ===
xls = pd.ExcelFile(file_path)  # Load the workbook
sheet_names = xls.sheet_names  # Get all sheet names
# === Step 3: Function to Extract & Format Data from Each Sheet ===
def fetch_and_stack_single_table(df, start_row, end_row, col_pairs, sheet_name):
    stacked_data = []
    date_col_idx = column_index_from_string("V") - 1  # Extract Date from row 8, column V
    try:
        date_value = df.iloc[6, date_col_idx]  # Row 8 (0-indexed = 7)
    except Exception:
        print(f":warning: Warning: Could not extract date from sheet '{sheet_name}'. Using 'Unknown'.")
        date_value = "Unknown"
    for col1, col2 in col_pairs:
        col1_idx, col2_idx = column_index_from_string(col1) - 1, column_index_from_string(col2) - 1
        subset = df.iloc[start_row-1:end_row, [col1_idx, col2_idx]].copy()
        subset.columns = ["Description", "Values"]
        subset["Description"] = subset["Description"].astype(str).str.rstrip(":")
        for row in subset.itertuples(index=False):
            stacked_data.append([date_value, row.Description, row.Values])  # Add Date column
    return stacked_data
# === Step 4: Process All Sheets & Combine into One Table ===
all_data = []
column_pairs = [("B", "F"), ("K", "M"), ("P", "S")]  # Columns to extract
start_row, end_row = 9, 12  # Rows to extract
for sheet in sheet_names:
    try:
        df = pd.read_excel(file_path, sheet_name=sheet, engine="openpyxl")
        df.columns = df.columns.astype(str).str.strip()
        extracted_data = fetch_and_stack_single_table(df, start_row, end_row, column_pairs, sheet)
        all_data.extend(extracted_data)
    except Exception as e:
        print(f":warning: Skipping sheet '{sheet}' due to error: {e}")
# Convert to DataFrame & Pivot
final_table1 = pd.DataFrame(all_data, columns=["Date", "Description", "Values"])
final_table1 = final_table1.pivot_table(index="Date", columns="Description", values="Values", aggfunc="first").reset_index()
# Clean Column Names
def clean_column_name(name):
    name = re.sub(r"[^\w\s]", "", name)  # Remove special characters
    return re.sub(r"\s+", "_", name.strip())  # Replace spaces with underscores
final_table1.columns = [clean_column_name(col) for col in final_table1.columns]
# Rename last column dynamically
columns = list(final_table1.columns)
columns[-1] = "YTD_Pack_Percent"
final_table1.columns = columns
# Debug: Print row count
print(f":white_check_mark: Processed {final_table1.shape[0]} rows in Table 1")
# === Step 5: Extract Data for Table 2 ===
target_columns = ["K", "M", "N", "O", "Q", "S", "V", "W"]
target_indices = [column_index_from_string(col) - 1 for col in target_columns]
combined_data = []
for sheet_name in xls.sheet_names:
    df = pd.read_excel(xls, sheet_name=sheet_name, engine="openpyxl")
    df.columns = df.columns.astype(str).str.strip()
    valid_row = None
    for row in range(22, 45):
        if str(df.iloc[row, column_index_from_string("B") - 1]).strip() == "Total":
            valid_row = row
            break
    if valid_row is not None:
        headers = [clean_column_name(str(col)) for col in df.iloc[13, target_indices].values]
        headers = [col.replace("Mc_Gob_cut_Output_Furnace_glass_Pull_Ton", "MC_gob_cut_output") for col in headers]
        values = df.iloc[valid_row, target_indices].values.tolist()
        combined_data.append(values)
# Convert to DataFrame
final_table2 = pd.DataFrame(combined_data, columns=headers)
# Debug: Print row count
print(f":white_check_mark: Processed {final_table2.shape[0]} rows in Table 2")
# === Step 6: Ensure Data Consistency Before Merging ===
# Convert all column names to lowercase for uniformity
final_table1.columns = final_table1.columns.str.lower()
final_table2.columns = final_table2.columns.str.lower()
# Ensure 'Date' column exists in final_table2 (use index if missing)
if "date" not in final_table2.columns:
    final_table2.insert(0, "date", final_table1["date"])
# Ensure same row order
final_table2 = final_table2.sort_values("date").reset_index(drop=True)
final_table1 = final_table1.sort_values("date").reset_index(drop=True)
# === Step 6.1: Extract 'Actual_Glass_Density' from (13, V) ===
actual_glass_density_values = []
for sheet_name in xls.sheet_names:
    df = pd.read_excel(xls, sheet_name=sheet_name, engine="openpyxl")
    df.columns = df.columns.astype(str).str.strip()
    # Extract value from (13, V), ensuring it's numeric
    value = df.iloc[11, column_index_from_string("V") - 1]  # Row 13 (0-indexed = 12)
    try:
        numeric_value = float(re.findall(r"\d+\.\d+|\d+", str(value))[0])  # Extract first numeric value
    except (IndexError, ValueError):
        numeric_value = None  # Assign None if no numeric value is found
    actual_glass_density_values.append(numeric_value)
# Add extracted values to final_table2
final_table2["actual_glass_density"] = actual_glass_density_values
# Add extracted values to final_table2
final_table2["actual_glass_density"] = actual_glass_density_values
# Convert all columns to string for consistency
final_table1 = final_table1.astype(str)
final_table2 = final_table2.astype(str)
# === Step 7: Merge Tables ===
merged_table = pd.merge(final_table1, final_table2, on="date", how="outer")
merged_table.columns = merged_table.columns.str.lower()
# Convert all column names to lowercase for uniformity
merged_table.columns = merged_table.columns.str.lower()

# Rename columns
merged_table = merged_table.rename(columns={
    "mc_down_time_jchange_glass_draining_cullet": "mc_dt_or_cgd",
    "daily_pack": "daily_pack_percent",
    "monthly_ton": "monthly_ton_percent",
})

# Multiply specific columns by 100
for col in ["net", "act_pack_eff", "ytd_pack_percent"]:
    merged_table[col] = pd.to_numeric(merged_table[col], errors="coerce") * 100
    merged_table[col] = merged_table[col].apply(lambda x: round(x, 2) if pd.notna(x) else pd.NA)

# Get a list of columns to round (excluding 'date' and 'std_glass_density')
columns_to_round = [col for col in merged_table.columns if col not in ["date", "std_glass_density"]]

# Round the values in the selected columns to 2 decimal places
merged_table[columns_to_round] = merged_table[columns_to_round].apply(pd.to_numeric, errors='coerce').round(2)

if furnace_identifier:
    # Add new column and populate with furnace identifier
    merged_table.insert(0, "furnace", furnace_identifier)  # Insert at the beginning (index 0)
else:
    print(":warning: Warning: Could not extract furnace identifier from filename. Furnace column will be empty.")

# Change date format
merged_table['date'] = pd.to_datetime(merged_table['date'], format="%d.%m.%Y").dt.strftime("%Y-%m-%d")

# Insert data into Supabase
data_to_insert = merged_table.to_dict(orient="records")
response = supabase.table("jg_furnace_data").insert(data_to_insert).execute()