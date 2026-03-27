import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np


# ================================
# HELPERS
# ================================
def to_slug(appliance):
    return appliance.title().replace(" ", "_")


def prep_appliances_url(csv_file, base_url):
    df = pd.read_csv(csv_file)

    appliances = (
        df["appl_name"]
        .dropna()
        .astype(str)
        .str.split(",")
        .explode()
        .str.strip()
    )

    unique_appliances = appliances.drop_duplicates()

    url_list = [
        base_url.format(to_slug(appliance))
        for appliance in unique_appliances
    ]

    return url_list


def fetch_with_retry(url, retries=3):
    for i in range(retries):
        try:
            return requests.get(url, timeout=10)
        except Exception:
            if i == retries - 1:
                raise
            time.sleep(1)


# ================================
# CORE SCRAPER
# ================================
def scrape_url(url):
    rows = []

    try:
        res = fetch_with_retry(url)
        soup = BeautifulSoup(res.text, "html.parser")

        blocks = soup.find_all("div", class_="appliance_parts_block")

        for block in blocks:
            name = block.find("div", class_="appltitle").get_text(strip=True)

            img_tag = block.find("span", class_="appliance_parts_parts").find("img")
            img_url = img_tag["src"] if img_tag else None

            lvl_tag = block.find("div", class_="lvl")
            level = int(lvl_tag.get_text(strip=True)) if lvl_tag else None

            details_text = block.find_all("span", class_="details")[-1].get_text(strip=True)

            cost_match = re.search(r"c:\s*(\d+)", details_text)
            rd_match = re.search(r"RD:\s*([\d-]+)", details_text)

            cost = int(cost_match.group(1)) if cost_match else None
            release_date = rd_match.group(1) if rd_match else None

            recipe_span = block.find("span", class_="bold")
            recipes = int(recipe_span.get_text(strip=True)) if recipe_span else None

            labels = [
                l.get_text(strip=True)
                for l in block.find_all("span", class_="sd_label")
                if l.get_text(strip=True)
            ]

            rows.append({
                "game_mode": re.search(r"/s8/(\w+)_appl", url).group(1),
                "appl_name": name,
                "appl_img_url": img_url,
                "appl_unlock_level": level,
                "appl_cost": cost,
                "appl_release_date": release_date,
                "appl_recipes_count": recipes,
                "appl_labels": ", ".join(labels),
                "appl_source_url": url
            })

    except Exception as e:
        print(f"❌ Error on {url}: {e}")

    return rows


# ================================
# MAIN PIPELINE FUNCTION
# ================================
def extract_appliances(base_url, input_csv_path, output_path, max_workers=10):

    print("🚀 Starting Appliance Pipeline...\n")

    urls = prep_appliances_url(input_csv_path, base_url)

    all_data = []

    print(f"Total URLs: {len(urls)}")
    print(f"Using {max_workers} threads...\n")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(scrape_url, url): url for url in urls}

        for i, future in enumerate(as_completed(futures), 1):
            url = futures[future]

            try:
                result = future.result()
                all_data.extend(result)
                print(f"✅ [{i}/{len(urls)}] Done: {url}")
            except Exception as e:
                print(f"❌ Failed: {url} -> {e}")

    # ================================
    # DATAFRAME CREATION + CLEANING
    # ================================
    df = pd.DataFrame(all_data)
    df = df.drop_duplicates()

    if "restaurant" in base_url.lower():
        df = df[~df["appl_img_url"].str.contains("bkry", case=False, na=False)]

    if "bakery" in base_url.lower():
        df = df[~df["appl_img_url"].str.contains("rstr", case=False, na=False)]

    # ================================
    # 🆕 APPLIANCE OBTAINABILITY LOGIC
    # ================================
    labels_clean = df['appl_labels'].astype(str).str.strip().str.lower()

    # Split into list of labels
    labels_split = labels_clean.str.split(',')

    # Clean each token (remove spaces)
    labels_split = labels_split.apply(lambda x: [i.strip() for i in x])

    df['appl_obtainability'] = np.where(
        df['appl_labels'].isna() | (labels_clean == '') | (labels_clean == 'nan'),
        'easy',
        np.where(
            labels_split.apply(lambda x: len(x) == 1 and x[0] in ['build', 'gem']),
            'medium',
            'hard'
        )
    )

    # ================================
    # SAVE OUTPUT
    # ================================
    df.to_csv(output_path, index=False)

    print("\n✅ Scraping complete.")
    print(f"📁 Saved to: {output_path}")

    return df


# ================================
# RUN
# ================================
if __name__ == "__main__":

    RUN_MODE = "both"      #Accepted Input: restaurant, bakery, both

    URLS = {
        "restaurant": "https://stm.gamerologizm.com/s8/restaurant_appl_retr.php?search_appl={}&submit=Submit#get",
        "bakery": "https://stm.gamerologizm.com/s8/bakery_appl_retr.php?search_appl={}&submit=Submit#get"
    }

    INPUTS = {
        "restaurant": "data_test/restaurant/01_recipes_all.csv",
        "bakery": "data_test/bakery/01_recipes_all.csv"
    }
    
    OUTPUTS = {
        "restaurant": "data_test/restaurant/02_appliances_all.csv",
        "bakery": "data_test/bakery/02_appliances_all.csv"
    }
    

    if RUN_MODE in ["restaurant", "both"]:
        extract_appliances(URLS["restaurant"], INPUTS["restaurant"],OUTPUTS["restaurant"])

    if RUN_MODE in ["bakery", "both"]:
        extract_appliances(URLS["bakery"],INPUTS["bakery"] ,OUTPUTS["bakery"])
