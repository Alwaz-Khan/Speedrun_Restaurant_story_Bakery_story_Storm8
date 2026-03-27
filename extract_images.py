import os
import sys
import pandas as pd
import requests
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

def load_csv_or_warn(path, dataset_name):
    if not os.path.exists(path):
        print(f"❌ CSV not found: {path}")
        print(f"👉 Run extract_recipe_table.py for '{dataset_name}' first.\n")
        sys.exit(1)   # 🔴 stops entire program immediately
    return pd.read_csv(path)

def download_images(image_urls, output_folder, max_workers=10):

    image_urls = image_urls.dropna().tolist()
    total = len(image_urls)

    print(f"[IMG] Downloading {total} images...")

    os.makedirs(output_folder, exist_ok=True)

    headers = {"User-Agent": "Mozilla/5.0"}
    session = requests.Session()
    session.headers.update(headers)

    def download_single(idx_url):
        idx, url = idx_url

        try:
            filename = os.path.basename(urlparse(url).path)

            if not filename.endswith(".png"):
                filename += ".png"

            file_path = os.path.join(output_folder, filename)

            if os.path.exists(file_path):
                return f"[{idx}/{total}] SKIP {filename}"

            response = session.get(url, timeout=10)

            if response.status_code == 200:
                with open(file_path, "wb") as f:
                    f.write(response.content)

                percent = (idx / total) * 100
                return f"[{idx}/{total}] ({percent:.2f}%) DOWNLOADED {filename}"
            else:
                return f"[{idx}/{total}] FAILED {url}"

        except Exception as e:
            return f"[{idx}/{total}] ERROR {url} -> {e}"

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(download_single, (idx, url))
            for idx, url in enumerate(image_urls, start=1)
        ]

        for future in as_completed(futures):
            print(future.result())

    print("[IMG] Done.\n")


# ================================
# RUN
# ================================
if __name__ == "__main__":

    print("Step 2: Downloading images...")

    MODE = "both"   # "restaurant", "bakery", "both"

    if MODE in ["restaurant", "both"]:
        print("\n=== RESTAURANT ===")

        csv_path = "data_test/restaurant/01_recipes_all_raw.csv"
        df = load_csv_or_warn(csv_path, "restaurant")

        download_images(df["rcp_img_url"], "data_test/restaurant/img_recipes")
        download_images(df["appl_img_url"], "data_test/restaurant/img_appliances")


    if MODE in ["bakery", "both"]:
        print("\n=== BAKERY ===")

        csv_path = "data_test/bakery/01_recipes_all_raw.csv"
        df = load_csv_or_warn(csv_path, "bakery")

        download_images(df["rcp_img_url"], "data_test/bakery/img_recipes")
        download_images(df["appl_img_url"], "data_test/bakery/img_appliances")