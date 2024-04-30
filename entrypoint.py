import os
import zipfile
import pandas as pd
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import boto3
import time


# Create a temporary directory to store downloaded files
zip_filename = "c2.zip"
temp_dir = "temp"
os.makedirs(temp_dir, exist_ok=True)

# Set up AWS S3 client
session = boto3.Session()
s3_client = session.client("s3")
bucket_name = "test-bucket"


# Set up Selenium WebDriver
chrome_options = Options()
chrome_options.add_argument("--headless")  # Run in headless mode

chrome_options.add_experimental_option(
    "prefs",
    {
        "download.default_directory": temp_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    },
)
driver = webdriver.Chrome(options=chrome_options)

# Read the CSV file
df = pd.read_csv("c2.csv")


# Loop through each row in the CSV
for _, row in df.iterrows():
    id = row["id"]
    download_link = row["download_link"]
    verification_link = row["verification_link"]
    name = row["name"]

    # Download the certificate from the download link
    try:
        driver.get(download_link)
        print(download_link.split("/")[-1])

        download_button = WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located(
                (By.XPATH, "//button[contains(text(), 'As Image')]")
            )
        )
        print(download_button)

        time.sleep(1)

        download_button.click()

        downloaded_file = download_link.split("/")[-1]
        downloaded_file_path = os.path.join(temp_dir, downloaded_file + ".png")

        print(downloaded_file_path)

        time.sleep(1)

        while not os.path.exists(downloaded_file_path):
            time.sleep(1)

        print(f"Download of {downloaded_file} completed.")

    except Exception as e:
        print(f"Error downloading certificate for {name}: {e}")
        continue


# Create a ZIP archive of the downloaded files
with zipfile.ZipFile(zip_filename, "w") as zip_file:
    for file in os.listdir(temp_dir):
        zip_file.write(os.path.join(temp_dir, file))

# Upload the ZIP file to AWS S3
try:
    s3_client.upload_file(zip_filename, bucket_name, zip_filename)
    print(f"ZIP file uploaded to S3: s3://{bucket_name}/{zip_filename}")
except Exception as e:
    print(f"Error uploading ZIP file to S3: {e}")

# Clean up temporary files
os.remove(zip_filename)
for file in os.listdir(temp_dir):
    os.remove(os.path.join(temp_dir, file))
os.rmdir(temp_dir)

# Close the WebDriver
print("Done")
driver.quit()
