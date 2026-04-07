import os
import requests
import zipfile
from pathlib import Path

BTS_WEBSITE = "https://transtats.bts.gov/PREZIP/"

LOCAL_SAVE_FOLDER = "./download_files"

Path(LOCAL_SAVE_FOLDER).mkdir(parents=True, exist_ok=True)
print(f"save folder ready: {LOCAL_SAVE_FOLDER}")

YEARS_TO_DOWNLOAD = [2021, 2022]

def download_one_month(year, month, save_folder):

    # file name banaya
    zip_filename = (
        f"On_Time_Reporting_Carrier_On_Time_Performance_"
        f"1987_present_{year}_{month}.zip"
    )

    download_url = BTS_WEBSITE + zip_filename

    zip_save_path = os.path.join(save_folder, zip_filename)

    print(f"Downloading {year}-{month} from {download_url}")

    # 🔹 download zip file
    response = requests.get(download_url, stream=True)

    with open(zip_save_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=1024*1024):
            if chunk:
                f.write(chunk)

    print(f"Downloaded: {zip_filename}")

    # 🔥 unzip file
    with zipfile.ZipFile(zip_save_path, "r") as zip_ref:
        zip_ref.extractall(save_folder)

    print(f"Unzipped: {zip_filename}")

    # 🔥 zip delete (optional but clean)
    os.remove(zip_save_path)

    # 🔹 CSV file find karo
    for file in os.listdir(save_folder):
        if file.endswith(".csv") and str(year) in file:
            return os.path.join(save_folder, file)


# 🔹 loop
for year in YEARS_TO_DOWNLOAD:
    print(f"\nYear {year}")

    for month in range(1, 4):

        csv_file = download_one_month(year, month, LOCAL_SAVE_FOLDER)

        print("CSV file ready:", csv_file)