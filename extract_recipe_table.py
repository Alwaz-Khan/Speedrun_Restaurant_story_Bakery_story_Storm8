import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import time
import os
import re


# ================================
# CONFIG
# ================================


HEADERS = {
    "User-Agent": "Mozilla/5.0"
}


# ================================
# HELPER FUNCTIONS
# ================================


def safe_text(parent, selector):
    tag = parent.select_one(selector)
    return tag.get_text(strip=True) if tag else None



def clean_int(text):
    return int(re.sub(r"[^\d-]", "", str(text))) if pd.notna(text) else None


def clean_recipes(dataframe1):

    df = dataframe1

    # -------- CLEANING -------- #
    df["rcp_cost"] = df["rcp_cost"].apply(clean_int).abs()
    df["rcp_income"] = df["rcp_income"].apply(clean_int)
    df["rcp_xp"] = df["rcp_xp"].apply(clean_int)
    df["rcp_servings"] = df["rcp_servings"].apply(clean_int)

    # Fix known issues
    fixes = {
        "Turtle Soup": "0.25 hrs",
        "Salmon Nigiri": "0.50 hrs",
        "Gilded Champagne": "1.00 hrs",
        "Midnight Martini": "3.00 hrs",
        "Golden Hour Cocktail": "8.00 hrs",
        "Silver Star Cupcakes": "8.00 hrs"
    }

    for k, v in fixes.items():
        df.loc[df["rcp_name"] == k, "time_hr"] = v

    df["time_min"] = (
        pd.to_numeric(df["time_hr"].str.extract(r'(\d+\.?\d*)')[0], errors="coerce")
        .mul(60)
        .round()
        .astype("Int64")
    )

    df["rcp_profit"] = df["rcp_income"] - df["rcp_cost"]

    df['rcp_obtainability'] = np.where(
        df['rcp_labels'].isna() | 
        (df['rcp_labels'].str.strip().str.lower().isin(['', 'nan'])),
        'easy',
        'hard'
    )
        
    return df



# ================================
# MAIN SCRAPER
# ================================
def extract_recipes(base_url,output_path):

    # Ensure directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    all_data = []
    page = 1

    while True:
        print(f"\n🔄 Scraping page {page}...")

        url = base_url.format(page)
        response = requests.get(url, headers=HEADERS)

        if response.status_code != 200:
            print(f"❌ Stopped at page {page} (status {response.status_code})")
            break

        soup = BeautifulSoup(response.text, "lxml")

        # Robust selector
        recipes = soup.select("span.appliance_single_recipe")

        # Stop condition
        if not recipes:
            print("✅ No more recipes found. Stopping.")
            break

        print(f"✅ Found {len(recipes)} recipes")

        for r in recipes:
            try:
                # ----------------
                # NAME
                # ----------------
                name_tag = r.select_one("div.invtitle2")
                name = name_tag.get_text(strip=True) if name_tag else None

                # ----------------
                # RECIPE IMAGE
                # ----------------
                recipe_img_tag = r.select_one("div.rcp_view img")
                recipe_img = recipe_img_tag["src"] if recipe_img_tag else None

                # ----------------
                # APPLIANCE IMAGE
                # ----------------
                appl_img_tag = r.select_one("span.appl_view img")
                appliance_img = appl_img_tag["src"] if appl_img_tag else None

                # ----------------
                # MAIN BLOCK
                # ----------------
                block = r.select_one("div.hide-on-mobile")
                if not block:
                    continue

                stats = block.select_one("div.detstats")
                if not stats:
                    continue

                # ----------------
                # STATS
                # ----------------

                cost = safe_text(stats, "div.rcpcost")
                servings = safe_text(stats, "div.rcpserv")
                time_val = safe_text(stats, "div.rcptime")
                xp = safe_text(stats, "div.rcpxp")
                income = safe_text(stats, "div.rcpincome")

                # ----------------
                # APPLIANCE + RELEASE DATE
                # ----------------
                appl_names = block.select("div.applname")

                appliance = appl_names[0].get_text(strip=True) if len(appl_names) > 0 else None
                release_date = appl_names[-1].get_text(strip=True) if len(appl_names) > 1 else None

                # ----------------
                # LABELS
                # ----------------
                labels = [
                    l.get_text(strip=True)
                    for l in block.select("span.sd_label")
                    if l.get_text(strip=True)
                ]

                # ----------------
                # LEVEL
                # ----------------
                level = None
                for div in block.find_all("div"):
                    text = div.get_text(strip=True)
                    if "Lvl:" in text:
                        match = re.search(r"Lvl:\s*(\d+)", text)
                        if match:
                            level = int(match.group(1))
                            break

                # ----------------
                # DETAILS URL
                # ----------------
                link_tag = r.find("a", href=True)
                details_url = link_tag["href"] if link_tag else None

                # ----------------
                # APPEND
                # ----------------
                all_data.append({
                    "game_mode": re.search(r"/s8/(\w+)_recipes", base_url).group(1),
                    "rcp_name": name,
                    "appl_name": appliance,
                    "rcp_cost": cost,
                    "rcp_servings": servings,
                    "time_hr": time_val,
                    "rcp_xp": xp,
                    "rcp_income": income,
                    "rcp_labels": ", ".join(labels),
                    "rcp_level": level,
                    "rcp_release_date": release_date,
                    "rcp_img_url": recipe_img,
                    "appl_img_url": appliance_img,
                    "rcp_url": details_url
                })

            except Exception as e:
                print(f"⚠️ Error on page {page}: {e}")

        page += 1
        time.sleep(1)  # polite delay

    # ================================
    # SAVE DATA
    # ================================
    df = pd.DataFrame(all_data).drop_duplicates()
    
    df = clean_recipes(df)

    df.to_csv(output_path, index=False)

    print(f"\n✅ Total recipes scraped: {len(df)}")
    print(f"📁 Cleaned Data saved to: {output_path}")


    return df


# ================================
# RUN
# ================================
if __name__ == "__main__":


    RUN_MODE = "both"      #Accepted Input: restaurant, bakery, both

    URLS = {
        "restaurant": "https://stm.gamerologizm.com/s8/restaurant_recipes_all.php?page={}",
        "bakery": "https://stm.gamerologizm.com/s8/bakery_recipes_all.php?page={}#content"
    }

    OUTPUTS = {
        "restaurant": "data_test/restaurant/01_recipes_all.csv",
        "bakery": "data_test/bakery/01_recipes_all.csv"
    }
    

    if RUN_MODE in ["restaurant", "both"]:
        extract_recipes(URLS["restaurant"], OUTPUTS["restaurant"])

    if RUN_MODE in ["bakery", "both"]:
        extract_recipes(URLS["bakery"], OUTPUTS["bakery"])
