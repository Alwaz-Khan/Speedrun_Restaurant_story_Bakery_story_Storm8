import pandas as pd
import numpy as np
import os
import time
import math

from dotenv import load_dotenv
load_dotenv()

import gspread
from oauth2client.service_account import ServiceAccountCredentials


# =========================
# 📏 ENSURE SHEET SIZE
# =========================
def ensure_sheet_size(sheet, required_rows, required_cols):
    current_rows = sheet.row_count
    current_cols = sheet.col_count

    new_rows = max(current_rows, required_rows)
    new_cols = max(current_cols, required_cols)

    if new_rows != current_rows or new_cols != current_cols:
        print(f"📏 Resizing sheet to {new_rows} rows × {new_cols} cols")
        sheet.resize(rows=new_rows, cols=new_cols)


# =========================
# 🔧 CONFIG
# =========================
PIPELINE_CONFIG = {
    "restaurant": {
        "recipe_file": "data/restaurant/01_recipes_all.csv",
        "appliance_file": "data/restaurant/02_appliances_all.csv",
    },
    "bakery": {
        "recipe_file": "data/bakery/01_recipes_all.csv",
        "appliance_file": "data/bakery/02_appliances_all.csv",
    },
}


# =========================
# 📂 LOAD DATA
# =========================
def load_dataframe(path):
    if os.path.exists(path):
        print(f"📂 Loading: {path}")
        return pd.read_csv(path)
    else:
        raise FileNotFoundError(f"❌ Missing file: {path}")


# =========================
# 🔄 PROCESS SOURCE
# =========================
def process_source(source_name, config):
    print(f"\n🚀 Processing: {source_name.upper()}")

    recipe_df = load_dataframe(config["recipe_file"])
    appliance_df = load_dataframe(config["appliance_file"])

    merged_df = recipe_df.merge(
        appliance_df, on="appl_name", how="inner", suffixes=("_rcp", "_appl")
    )

    return merged_df


# =========================
# 🔁 RETRY LOGIC
# =========================
def retry_api_call(func, retries=5, base_delay=2):
    for attempt in range(retries):
        try:
            return func()
        except Exception as e:
            wait = base_delay * (2**attempt)
            print(f"⚠️ Attempt {attempt+1} failed: {e}")
            print(f"⏳ Retrying in {wait}s...")
            time.sleep(wait)

    raise Exception("❌ API failed after max retries")


# =========================
# 🧹 CLEAN DATA (CRITICAL FIX)
# =========================
def clean_dataframe_for_sheets(df):
    # Replace inf → NaN
    df = df.replace([np.inf, -np.inf], np.nan)

    # Convert all columns to object (VERY IMPORTANT)
    df = df.astype(object)

    # Replace NaN → None (JSON safe)
    df = df.where(pd.notnull(df), None)

    return df


# =========================
# 📦 UPLOAD IN CHUNKS
# =========================
def upload_in_chunks(sheet, df, chunk_size=1000):
    print("\n🚀 Starting Google Sheets upload...")

    # 🔥 CLEAN DATA BEFORE UPLOAD
    df = clean_dataframe_for_sheets(df)

    # Convert to list format
    data = [df.columns.tolist()] + df.values.tolist()

    total_rows = len(data)
    total_chunks = math.ceil(total_rows / chunk_size)

    print(f"📊 Total rows (with header): {total_rows}")
    print(f"📦 Chunk size: {chunk_size}")
    print(f"🔢 Total chunks: {total_chunks}")

    # Ensure sheet size BEFORE upload
    ensure_sheet_size(sheet, total_rows, len(data[0]))

    # Clear sheet
    retry_api_call(lambda: sheet.clear())

    # Upload chunks
    for i in range(0, total_rows, chunk_size):
        chunk = data[i : i + chunk_size]

        start_row = i + 1
        end_row = i + len(chunk)

        print(f"⬆️ Uploading rows {start_row} → {end_row}")

        retry_api_call(lambda: sheet.update(values=chunk, range_name=f"A{start_row}"))

    print("✅ Upload completed successfully!")


# =========================
# 🚀 MAIN
# =========================
if __name__ == "__main__":

    all_merged_dfs = []

    for source_name, config in PIPELINE_CONFIG.items():
        merged_df = process_source(source_name, config)
        all_merged_dfs.append(merged_df)

    # =========================
    # 🔥 COMBINE
    # =========================
    final_df = pd.concat(all_merged_dfs, ignore_index=True)

    print("\n✅ FINAL DATAFRAME SHAPE:", final_df.shape)

    # =========================
    # 🧠 COLUMN NORMALIZATION
    # =========================
    rcp_col = "rcp_obtainability"
    appl_col = "appl_obtainability"

    final_df[rcp_col] = final_df[rcp_col].astype(str).str.lower()
    final_df[appl_col] = final_df[appl_col].astype(str).str.lower()

    # =========================
    # ➕ SCORE CALCULATION
    # =========================
    score = np.where(final_df[rcp_col] == "easy", 1, 3) + np.where(
        final_df[appl_col] == "easy", 1, np.where(final_df[appl_col] == "medium", 2, 3)
    )

    # Store difficulty
    final_df["rcp_difficulty"] = score

    print("✅ Added column: rcp_difficulty")

    # =========================
    # 📊 COLUMN ORDER
    # =========================
    priority_cols = [
        "game_mode_rcp",
        "rcp_name",
        "appl_name",
        "rcp_level",
        "time_min",
        "rcp_xp",
        "rcp_profit",
        "rcp_difficulty",
    ]

    priority_cols = [col for col in priority_cols if col in final_df.columns]
    remaining_cols = [col for col in final_df.columns if col not in priority_cols]

    final_df = final_df[priority_cols + remaining_cols]

    print("✅ Columns reordered successfully")

    # =========================
    # 💾 EXPORT CSV
    # =========================
    final_df.to_csv("data/final_combined_data.csv", index=False)
    print("📁 Exported: final_combined_data.csv")

    # =========================
    # 🔐 GOOGLE SHEETS AUTH
    # =========================
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]

    creds_path = os.getenv("GOOGLE_CREDENTIALS_PATH")
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)

    client = gspread.authorize(creds)


    sheet_name = os.getenv("GSHEET_NAME")
    worksheet_name = os.getenv("GSHEET_WORKSHEET")

    sheet = client.open(sheet_name).worksheet(worksheet_name)

    # =========================
    # 🚀 UPLOAD
    # =========================
    upload_in_chunks(sheet, final_df, chunk_size=1000)
