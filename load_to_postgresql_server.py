# reference: https://youtu.be/M2NzvnfS-hI?si=SSTmQt5LLAiGBgk-

import psycopg2
import psycopg2.extras
import pandas as pd
import os


from dotenv import load_dotenv
load_dotenv()


# =========================
# 🔧 TYPE MAPPING
# =========================
def map_dtype(dtype):
    dtype = str(dtype)

    if "int" in dtype:
        return "INT"
    elif "float" in dtype:
        return "FLOAT"
    elif "datetime" in dtype:
        return "TIMESTAMP"
    else:
        return "TEXT"


# =========================
# 🔧 CLEAN COLUMN NAMES
# =========================
def clean_columns(df):
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace(r"[^\w]", "", regex=True)
    )
    return df


# =========================
# 🔧 CREATE TABLE FROM DF
# =========================
def create_table_from_df(conn, df, table_name):
    columns = []

    for col, dtype in zip(df.columns, df.dtypes):
        pg_type = map_dtype(dtype)
        columns.append(f"{col} {pg_type}")

    columns_sql = ", ".join(columns)

    create_query = f"""
        CREATE TABLE {table_name} (
            id SERIAL PRIMARY KEY,
            {columns_sql}
        );
    """

    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table_name}")
        cur.execute(create_query)

    conn.commit()


# =========================
# 🔧 INSERT DATAFRAME
# =========================
def insert_dataframe(conn, df, table_name):
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ",".join(df.columns)

    query = f"INSERT INTO {table_name} ({cols}) VALUES %s"

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, query, tuples)

    conn.commit()


# =========================
# 🚀 MAIN LOAD FUNCTION
# =========================
def load_to_postgres(game_mode, recipe_path, appliance_path, db_config):
    try:
        # =========================
        # 🔐 INPUT VALIDATION (ADD HERE)
        # =========================
        if game_mode not in ["restaurant", "bakery"]:
            raise ValueError(f"Invalid game mode: {game_mode}")
        
        
        # =========================
        # 📂 LOAD CSV FILES
        # =========================
        if not os.path.exists(recipe_path):
            print(f"❌ Recipe file not found: {recipe_path}")
            print("👉 Please run extract_recipe.py first")
            return

        if not os.path.exists(appliance_path):
            print(f"❌ Appliance file not found: {appliance_path}")
            print("👉 Please run extract_appliance.py first")
            return

        recipe_df = pd.read_csv(recipe_path)
        appliance_df = pd.read_csv(appliance_path)

        with psycopg2.connect(**db_config) as conn:

            # =========================
            # 🔧 CLEAN DATA
            # =========================
            recipe_df = clean_columns(recipe_df)
            appliance_df = clean_columns(appliance_df)

            recipe_df.reset_index(drop=True, inplace=True)
            appliance_df.reset_index(drop=True, inplace=True)

            recipe_df = recipe_df.where(pd.notnull(recipe_df), None)
            appliance_df = appliance_df.where(pd.notnull(appliance_df), None)

            print(f"📦 Creating tables for {game_mode}...")

            create_table_from_df(conn, recipe_df, f"recipes_all_{game_mode}")
            create_table_from_df(conn, appliance_df, f"appliances_all_{game_mode}")

            print("🚀 Inserting data...")

            insert_dataframe(conn, recipe_df, f"recipes_all_{game_mode}")
            insert_dataframe(conn, appliance_df, f"appliances_all_{game_mode}")

            print(f"✅ {game_mode} data loaded successfully!")

    except Exception as e:
        print("❌ ERROR: Something failed. Check logs.")

# ================================
# RUN
# ================================
if __name__ == "__main__":

    RUN_MODE = "both"      #Accepted Input: restaurant, bakery, both

    db_config = {
        "host": os.getenv("DB_HOST"),
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "port": int(os.getenv("DB_PORT", 5432)),
        }

    RECIPE = {
        "restaurant": "data_test/restaurant/01_recipes_all.csv",
        "bakery": "data_test/bakery/01_recipes_all.csv"
    }

    APPLIANCE = {
        "restaurant": "data_test/restaurant/02_appliances_all.csv",
        "bakery": "data_test/bakery/02_appliances_all.csv"
    }
    

    if RUN_MODE in ["restaurant", "both"]:
        load_to_postgres("restaurant", RECIPE["restaurant"], APPLIANCE["restaurant"],db_config)

    if RUN_MODE in ["bakery", "both"]:
        load_to_postgres("bakery",RECIPE["bakery"],APPLIANCE["bakery"], db_config)


    
    