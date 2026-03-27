from extract_recipe_table import extract_recipes
from extract_appliances_table import extract_appliances
from extract_images import download_images
from load_to_postgresql_server import load_to_postgres
import pandas as pd
import os


def find_mode(base_url):
    if "bakery" in base_url:
        mode = "bakery"
    elif "restaurant" in base_url:
        mode = "restaurant"
    else:
        mode = "other"

    return mode


def load_dataframe(file_paths, file_name):
    full_path = os.path.join(file_paths, file_name)

    if os.path.exists(full_path):
        print(f"📂 Loading existing data from {full_path}")
        return pd.read_csv(full_path)
    else:
        raise FileNotFoundError(
            f"❌ No data found at {full_path}. Run extraction first."
        )


def run_pipeline(
    base_recipe_url,
    base_appliance_url,
    run_recipe_extract=False,
    run_appliance_extract=False,
    run_images=False,
    run_postgresql_load=False
):
    print("🚀 Starting pipeline...\n")

    game_mode = find_mode(base_recipe_url)
    folder_path = os.path.join("data", f"{game_mode}")
    recipe_filename = "01_recipes_all.csv"
    appliance_filename = "02_appliances_all.csv"

    recipe_df = None
    appliance_df = None

    # =========================
    # STEP 1: EXTRACT Recipe File
    # =========================
    if run_recipe_extract:
        print("Step 1: Extracting recipe table...")
        recipe_df = extract_recipes(
            base_recipe_url, os.path.join(folder_path, recipe_filename)
        )

    # =========================
    # STEP 2: Extract Appliances File
    # =========================
    if run_appliance_extract:

        if recipe_df is None:
            recipe_df = load_dataframe(folder_path, recipe_filename)

        print("Step 3: Extracting appliances table...")
        appliance_df = extract_appliances(
            base_appliance_url,
            os.path.join(folder_path, recipe_filename),
            os.path.join(folder_path, appliance_filename),
        )

    # =========================
    # STEP 3: Load to PostgreSQL
    # =========================
    if run_postgresql_load:

        if recipe_df is None:
            recipe_df = load_dataframe(folder_path, recipe_filename)
        if appliance_df is None:
            appliance_df = load_dataframe(folder_path, appliance_filename)

        print("Step 4: Loading to PostgreSQL...")

        db_config = {
        "host": os.getenv("DB_HOST"),
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "port": int(os.getenv("DB_PORT", 5432)),
        }

        load_to_postgres(game_mode,os.path.join(folder_path, recipe_filename),os.path.join(folder_path, appliance_filename), db_config)

    # =========================
    # STEP 4: (Optional) IMAGE DOWNLOAD
    # =========================
    if run_images:

        if recipe_df is None:
            recipe_df = load_dataframe(folder_path, recipe_filename)

        if appliance_df is None:
            appliance_df = load_dataframe(folder_path, appliance_filename)

        print("Step 5: Downloading images...")

        tasks = [
            {"column": recipe_df["rcp_img_url"], "folder": "img_recipes"},
            {"column": appliance_df["appl_img_url"], "folder": "img_appliances"},
        ]

        for task in tasks:
            download_images(task["column"], os.path.join(folder_path, task["folder"]))

    print("\n✅ Pipeline finished.")

    return recipe_df, appliance_df


if __name__ == "__main__":

    # =========================
    # 🔧 PIPELINE FLAGS
    # =========================
    RUN_RECIPE_EXTRACT = True
    RUN_APPLIANCE_EXTRACT = True
    RUN_IMAGES = True
    RUN_POSTGRESQL_LOAD = True
    

    # =========================
    # 🔧 MODE SELECTION
    # =========================
    MODE = "both"  # "restaurant", "bakery", "both"

    # =========================
    # 🔧 CONFIG MAP
    # =========================
    PIPELINE_CONFIG = {
        "restaurant": {
            "recipe_url": "https://stm.gamerologizm.com/s8/restaurant_recipes_all.php?page={}#content",
            "appliance_url": "https://stm.gamerologizm.com/s8/restaurant_appl_retr.php?search_appl={}&submit=Submit#get",
        },
        "bakery": {
            "recipe_url": "https://stm.gamerologizm.com/s8/bakery_recipes_all.php?page={}#content",
            "appliance_url": "https://stm.gamerologizm.com/s8/bakery_appl_retr.php?search_appl={}&submit=Submit#get",
        },
    }

    # =========================
    # 🔧 SELECT TARGETS
    # =========================
    if MODE == "both":
        selected_pipelines = PIPELINE_CONFIG.items()
    elif MODE in PIPELINE_CONFIG:
        selected_pipelines = [(MODE, PIPELINE_CONFIG[MODE])]
    else:
        raise ValueError("Invalid MODE")

    # =========================
    # 🚀 RUN PIPELINE(S)
    # =========================

    for source_name, config in selected_pipelines:

        print(f"\n🚀 Running pipeline for: {source_name.upper()}")

        recipe_df, appliance_df = run_pipeline(
            base_recipe_url=config["recipe_url"],
            base_appliance_url=config["appliance_url"],
            run_recipe_extract=RUN_RECIPE_EXTRACT,
            run_appliance_extract=RUN_APPLIANCE_EXTRACT,
            run_images=RUN_IMAGES,
            run_postgresql_load=RUN_POSTGRESQL_LOAD
        )

        recipe_df_prefixed = recipe_df.add_prefix("rcp_")
        appliance_df_prefixed = appliance_df.add_prefix("appl_")

        merged_df = recipe_df_prefixed.merge(
            appliance_df_prefixed,
            left_on="rcp_appl_name",
            right_on="appl_appl_name",
            how="inner",
        )
