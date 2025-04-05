from main import supabase  # Import the Supabase client from main.py
import json
import pandas as pd
import re
from openpyxl.utils import column_index_from_string
# from tabulate import tabulate
# === Step 1: Define File Path ===
file_path = r"E:\proj1\F2 PROD REPORT JAN 2025.xlsx"
# === Step 2: Read All Sheets ===
xls = pd.ExcelFile(file_path)
all_sheets = xls.sheet_names  # Get all sheet names
# === Step 3: Define Column Mapping ===
column_mapping = {
    "MC": ["B"],
    "Job_Name": ["C", "D", "E"],  # Renamed Ignore1 -> Job_Name
    "Shift": ["F"],
    "IC_Percent": ["G"],  # Renamed IC% -> IC_Percent
    "IM_Percent": ["H"],  # Renamed IM% -> IM_Percent
    "Defects_and_actions": ["I", "J", "K", "L", "M"],  # Merge multiple columns into a single string
    "Stopages": ["N", "O", "P", "Q", "R", "S", "T", "U", "V", "W"],  # Merge multiple columns into a single string
    "Dept": ["X"]
}
# === Step 4: Process Each Sheet & Merge Data ===
final_data = []
for sheet_name in all_sheets:
    df = pd.read_excel(xls, sheet_name=sheet_name, engine="openpyxl", dtype=str)  # Read as string for safe processing
    # === Fetch Value from (8, V) ===
    value_from_8V = df.iloc[6, column_index_from_string("V") - 1]  # Row 8 (Index 7), Column V
    # === Identify Header Row (Where Column F == "Shift") ===
    col_f = df.iloc[:, column_index_from_string("F") - 1].astype(str).str.strip()
    header_row_idx = col_f[col_f.str.contains(r"^\s*Shift\s*$", case=False, na=False)].index[0]
    # Extract headers & process them
    df.columns = df.iloc[header_row_idx].ffill().tolist()
    df = df.iloc[header_row_idx + 1:].reset_index(drop=True)  # Remove rows above headers
    # === Identify End Row (Where Column B Starts with "Da") ===
    col_b = df.iloc[:, column_index_from_string("B") - 1].astype(str).str.strip()
    end_row_idx = col_b[col_b.str.startswith("Da", na=False)].index[0]
    df = df.iloc[:end_row_idx]  # Trim to required rows
    # === Extract Required Columns ===
    temp_df = pd.DataFrame()
    for final_col, excel_cols in column_mapping.items():
        col_indexes = [column_index_from_string(col) - 1 for col in excel_cols]  # Convert letters to indexes
        temp_df[final_col] = df.iloc[:, col_indexes].apply(lambda row: ' '.join(row.dropna().astype(str)), axis=1)
    # === Add Fetched Column & Date Column ===
    temp_df.insert(0, "Date", value_from_8V)  # Add fetched column
    # === Fill Down "MC" Column Until a New Value Appears ===
    temp_df["MC"] = temp_df["MC"].replace("", pd.NA).ffill()
    # === Fill Down "Job_Name" Based on MC ===
    job_name_mapping = {}
    for index, row in temp_df.iterrows():
        mc_value = row['MC']
        job_name_value = row['Job_Name']
        if pd.notna(job_name_value) and job_name_value != "NaN" and job_name_value != "":
            job_name_mapping[mc_value] = job_name_value
        elif mc_value in job_name_mapping:
            temp_df.at[index, 'Job_Name'] = job_name_mapping[mc_value]
    # === Multiply IC_Percent and IM_Percent by 100 ===
    for col in ["IC_Percent", "IM_Percent"]:
        temp_df[col] = pd.to_numeric(temp_df[col], errors="coerce") * 100
        temp_df[col] = temp_df[col].apply(lambda x: round(x, 2) if pd.notna(x) else pd.NA)
    # === Remove Excess Spaces, Line Breaks & Ensure Single Column Data ===
    for col in ["Stopages", "Defects_and_actions"]:
        temp_df[col] = temp_df[col].astype(str).str.replace(r'\s+', ' ', regex=True).str.strip()  # Remove extra spaces
    # === Append Data to Final List ===
    final_data.append(temp_df)
# === Step 5: Merge All DataFrames & Export to CSV ===
final_df = pd.concat(final_data, ignore_index=True)
# === Remove Rows Where "Shift" is Blank or NaN ===
final_df = final_df[final_df["Shift"].astype(str).str.strip() != ""]  # Removes rows where Shift is empty
# === Step 6: Change Date format ===
final_df['Date'] = pd.to_datetime(final_df['Date'], format="%d.%m.%Y").dt.strftime("%Y.%m.%d") # Changed format to "%d.%m.%Y"
# Convert IC_Percent and IM_Percent to float and handle NaN
final_df["IC_Percent"] = final_df["IC_Percent"].astype(float)
final_df["IM_Percent"] = final_df["IM_Percent"].astype(float)

# Replace NaN with None for Supabase compatibility
final_df = final_df.where(pd.notna(final_df), None)

# # === Step 6: Save to CSV Without Stretching Rows ===
# csv_output_path = "/content/F2_Monthly_Report.csv"
# final_df.to_csv(csv_output_path, index=False, quoting=1)  # quoting=1 ensures fields with commas stay intact
# # === Step 7: Display & Confirm Output ===
from IPython.display import display
display(final_df)  # Show final table in Jupyter/Colab
# print(f"CSV file saved at: {csv_output_path}")


# === Convert DataFrame to List of Dictionaries ===
data_to_insert = final_df.to_dict(orient="records")  

# === Insert Data into Supabase ===
response = supabase.table("jg_containers_defects").insert(data_to_insert).execute()