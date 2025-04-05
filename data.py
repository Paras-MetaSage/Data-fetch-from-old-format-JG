from main import supabase  # Import the Supabase client from main.py
import json


# Data

import pandas as pd
import numpy as np
import re
# from tabulate import tabulate
from typing import Dict, List, Optional
class GlassProductionAnalyzer:
    """Process and analyze glass production data from Excel reports."""
    def __init__(self, file_path: str):
        """
        Initialize the analyzer with an Excel file path.
        Args:
            file_path (str): Path to the Excel file containing production data
        """
        self.file_path = file_path
        self.xls = pd.ExcelFile(file_path)
        self.sheet_names = self.xls.sheet_names
        self.required_columns = [
            "Mc", "Shift", "Job_Name", "No.Of_Sect", "Speed_Bpm", "Glass_Weight",
            "Std-_Hrs", "Act-_Hrs", "Furnace_Draw", "Mc_Gob_cut_Output_Furnace_glass_Pull_Ton",
            "Pack_Ton", "Gob_Cut_Output_Quantity", "Actual_-_Pack_Quantity",
            "Act_Pack_Eff_%", "Pass_Quantity", "Net_%", "total_pack_quantity"
        ]
        self.column_renames = {
            "Mc_Gob_cut_Output_Furnace_glass_Pull_Ton": "Glass_Pull_Ton",
            "Pack_Ton": "Pack_Ton",
            "No.Of_Sect": "number_of_section",
            "Gob_Cut_Output_Quantity": "Output_ Quantity",
            "Actual_-_Pack_Quantity": "pack_quantity",
            "Total_Pack_Qty": "total_pack_quantity",
            "Std-_Hrs":"standard_hours",
            "Act-_Hrs": "actual_hours",
            "Net_%": "net",
            "Act_Pack_Eff_%": "actual_pack_efficiency"
        }
        # Define numeric columns for rounding
        self.numeric_columns = [
            "Speed_Bpm", "Glass_Weight", "standard_hours", "actual_hours", "Furnace_Draw",
            "Glass_Pull_Ton", "Pack_Ton", "Output_Quantity", "pack_quantity",
            "actual_pack_efficiency", "Pass_Quantity", "net", "total_pack_quantity"
        ]
    def process_sheet(self, sheet_name: str) -> pd.DataFrame:
        """
        Process a single sheet from the Excel file.
        Args:
            sheet_name (str): Name of the sheet to process
        Returns:
            pd.DataFrame: Processed dataframe
        """
        # Load data
        df = pd.read_excel(self.file_path, sheet_name=sheet_name, header=None, dtype=str)
        # :small_blue_diamond: Extract date from (8, V) (row index 7, column index 21)
        sheet_date = df.iloc[7, 21]
        # :small_blue_diamond: Set headers and trim first 14 rows
        df.columns = df.iloc[14]
        df = df.iloc[15:].reset_index(drop=True)
        # :small_blue_diamond: Remove the extra unnamed column before Mc
        if df.columns[0] != "Mc":
            df = df.iloc[:, 1:]
        # :small_blue_diamond: Clean and process the data
        df = self._clean_columns(df)
        df = self._filter_data(df)
        df = self._handle_special_rows(df)
        df = self._clean_job_names(df)
        df = self._process_numeric_columns(df)
        # :small_blue_diamond: Insert the extracted Date column
        df.insert(0, "Date", sheet_date)
        return df
    def _clean_columns(self, df: pd.DataFrame) -> pd.DataFrame:
      """Clean column names and drop empty columns."""
      df.columns = df.columns.astype(str).str.strip().str.replace(r"\s+", "_", regex=True)
      # Remove unnamed first column if present
      if df.columns[0] == "Unnamed: 0":
          df = df.iloc[:, 1:]
      df = df.dropna(axis=1, how="all")
      df = df[[col for col in self.required_columns if col in df.columns]]
      df = df.rename(columns=self.column_renames)
      return df
    def _filter_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply data filters."""
        total_row_index = df[df.iloc[:, 1].str.strip() == "Total"].index.min()
        if pd.notna(total_row_index):
            df = df.loc[:total_row_index-1]
        df = df.replace(r'^\s*$', np.nan, regex=True)
        df = df.ffill()
        df = df[~df["Job_Name"].astype(str).str.startswith("SD", na=False) & df["Job_Name"].notna()]
        df = df[df["Mc"].astype(str).str.match(r"F\d{2}", na=False)]
        return df
    def _handle_special_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle special rows like Job change and MC DRAINING."""
        if "number_of_section" in df.columns and "total_pack_quantity" in df.columns:
            job_change_mask = df["number_of_section"].str.strip() == "Job change"
            excluded_columns = ["Mc", "Shift", "Job_Name", "number_of_section", "total_pack_quantity"]
            df.loc[job_change_mask, df.columns.difference(excluded_columns)] = np.nan
        if "Job_Name" in df.columns and "total_pack_quantity" in df.columns:
            mc_draining_mask = df["Job_Name"].str.strip() == "MC DRAINING"
            mc_excluded_columns = [
                "Mc", "Shift", "Job_Name", "total_pack_quantity", "number_of_section",
                "Speed_Bpm", "Glass_Weight", "standard_hours", "actual_hours",
                "Furnace_Draw", "Glass_Pull_Ton", "Pack_Ton"
            ]
            df.loc[mc_draining_mask, df.columns.difference(mc_excluded_columns)] = np.nan
        return df
    def _clean_job_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove parentheses from Job Name column."""
        if "Job_Name" in df.columns:
            df["Job_Name"] = df["Job_Name"].astype(str).str.replace(r"[\(\)]", "", regex=True)
        return df
    
    def _process_numeric_columns(self, df: pd.DataFrame) -> pd.DataFrame:
      """Process numeric columns including percentages and round to 2 decimal places."""
      for col in self.numeric_columns:
          if col in df.columns:
              df[col] = pd.to_numeric(df[col], errors='coerce')
              df[col] = df[col].round(2)

      for col in ["net", "actual_pack_efficiency"]:
          if col in df.columns:
              df[col] = pd.to_numeric(df[col], errors='coerce') * 100
              df[col] = df[col].round(2)

      # Convert specific float columns to integer
      float_to_int_cols = [
          "Speed_Bpm", "Glass_Weight", "standard_hours", "actual_hours", "Furnace_Draw",
          "Glass_Pull_Ton", "Pack_Ton", "Output_Quantity", "pack_quantity",
          "actual_pack_efficiency", "Pass_Quantity", "net"
      ]
      for col in float_to_int_cols:
          if col in df.columns:
              df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

      # If "Glass_Pull_Ton" & "Pack_Ton" are 0, set specific columns to NaN
      if "Glass_Pull_Ton" in df.columns and "Pack_Ton" in df.columns:
          zero_mask = (df["Glass_Pull_Ton"] == 0) & (df["Pack_Ton"] == 0)
          nullify_columns = ["Output_Quantity", "pack_quantity", "actual_pack_efficiency", "Pass_Quantity", "net"]
          df.loc[zero_mask, nullify_columns] = np.nan

      return df



    def process_all_sheets(self, df : pd.DataFrame) -> pd.DataFrame:
      """
      Process all sheets, count rows per sheet, sum total rows, and save as a single CSV file.
      Args:
          output_csv_path (str): File path to save the combined CSV
      """
      all_data = []
      sheet_row_counts = {}
      total_rows = 0
      for sheet in self.sheet_names:
          df = self.process_sheet(sheet)
          df["Sheet_Name"] = sheet  # Add sheet name for reference
          row_count = df.shape[0]  # Count rows
          sheet_row_counts[sheet] = row_count
          total_rows += row_count
          all_data.append(df)
      # Combine all sheets into a single DataFrame
      combined_df = pd.concat(all_data, ignore_index=True)
      combined_df['Date'] = pd.to_datetime(combined_df['Date'], format="%d.%m.%Y").dt.strftime("%Y.%m.%d") # Changed format to "%d.%m.%Y"

      return combined_df
    #   combined_df.to_csv(output_csv_path, index=False)
      
      # Print row counts
      print("\n:bar_chart: Row Counts Per Sheet:")
      for sheet, count in sheet_row_counts.items():
          print(f"{sheet}: {count} rows")
      print(f"\n:1234: Total Rows Across All Sheets: {total_rows}")
    #   print(f":white_check_mark: Processed data saved to {output_csv_path}")
    def display_results(self, processed_data: Dict[str, pd.DataFrame]) -> None:
        """Display processed results in a formatted table."""
        # for sheet, df in processed_data.items():
            # print(f"\n:pushpin: Sheet: {sheet}")
            # print(tabulate(df, headers="keys", tablefmt="grid"))
# :white_check_mark: Usage example
analyzer = GlassProductionAnalyzer(r"E:\proj1\F2 PROD REPORT JAN 2025.xlsx")
# analyzer.process_all_sheets("/content/F1_Jan_Data.csv")
processed_data = {}   
for sheet in analyzer.sheet_names:  # Assuming sheet_names is accessible
    processed_data[sheet] = analyzer.process_sheet(sheet)

analyzer.display_results(processed_data)  # Now pass the populated dictionary

# Process all sheets and get final DataFrame
final_df = analyzer.process_all_sheets(processed_data)

# Convert DataFrame to dictionary format
# Convert DataFrame to a list of dictionaries and replace NaN with None
data_to_insert = analyzer.process_all_sheets(pd.DataFrame()).replace({np.nan: None}).to_dict(orient="records")

# Insert data into Supabase
response = supabase.table("jg_containers_data").insert(data_to_insert).execute()