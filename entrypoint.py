import os
import zipfile
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import boto3
import time
import logging
from datetime import datetime

# Set up logging
log_file = f"download_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
)

# Load configuration from environment variables or a separate configuration file
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME", "test-bucket")
S3_BUCKET_LOCATION = os.environ.get("S3_BUCKET_LOCATION", "eu-north-1")
DOWNLOAD_DIRECTORY = os.environ.get("DOWNLOAD_DIRECTORY", "temp")
CSV_FILE_PATH = os.environ.get("CSV_FILE_PATH", "certificates.csv")


def create_temp_directory():
    """
    Create a temporary directory for storing downloaded files.

    Returns:
        str: Path to the temporary directory.
    """
    temp_dir = DOWNLOAD_DIRECTORY
    os.makedirs(temp_dir, exist_ok=True)
    return temp_dir


def setup_s3_client():
    """
    Set up an S3 client using the configured AWS credentials.

    Returns:
        boto3.client: S3 client object.
    """
    session = boto3.Session()
    s3_client = session.client("s3")
    return s3_client


def setup_web_driver(temp_dir):
    """
    Set up a Chrome WebDriver instance with headless mode and download options.

    Args:
        temp_dir (str): Path to the temporary directory for downloads.

    Returns:
        selenium.webdriver.Chrome: Chrome WebDriver instance.
    """
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
    return driver


def download_certificate(download_link, temp_dir, driver, name, max_retries=3):
    """
    Download a certificate from the provided download link.

    Args:
        download_link (str): URL for downloading the certificate.
        temp_dir (str): Path to the temporary directory for downloads.
        driver (selenium.webdriver.Chrome): Chrome WebDriver instance.
        name (str): Name of the certificate.
        max_retries (int): Maximum number of retry attempts for downloading the certificate.

    Returns:
        str: Path to the downloaded certificate file, or None if the download failed.
    """
    retries = 0
    while retries < max_retries:
        try:
            driver.get(download_link)
            logging.info(f"Navigated to {download_link.split('/')[-1]}")

            download_button = WebDriverWait(driver, 10).until(
                EC.visibility_of_element_located(
                    (By.XPATH, "//button[contains(text(), 'As Image')]")
                )
            )
            logging.info("Download button found")

            time.sleep(1)  # Add a delay to ensure the download has started

            download_button.click()

            downloaded_file = download_link.split("/")[-1]
            downloaded_file_path = os.path.join(temp_dir, downloaded_file + ".png")

            logging.info(f"Waiting for download of {downloaded_file} to complete...")

            # Wait for 10 seconds for the file to be downloaded
            for _ in range(10):
                time.sleep(1)
                if os.path.exists(downloaded_file_path):
                    logging.info(f"Download of {downloaded_file} completed.")
                    return downloaded_file_path

            logging.warning(f"Download of {downloaded_file} timed out. Retrying...")
            retries += 1

        except Exception as e:
            logging.error(f"Error downloading certificate for {name}: {e}")
            retries += 1

    logging.error(
        f"Failed to download certificate for {name} after {max_retries} attempts."
    )
    return None


def create_zip_archive(zip_filename, temp_dir, certificates):
    """
    Create a ZIP archive containing the downloaded certificate files.

    Args:
        temp_dir (str): Path to the temporary directory containing the certificate files.
        certificates (list): List of paths to the certificate files.

    Returns:
        str: Path to the created ZIP archive file.
    """
    zip_file_path = os.path.join(temp_dir, zip_filename)

    with zipfile.ZipFile(zip_file_path, "w") as zip_file:
        for file_path in certificates:
            zip_file.write(file_path)

    logging.info(f"ZIP archive created: {zip_file_path}")
    return zip_file_path


def upload_to_s3(s3_client, zip_file_path, bucket_name):
    """
    Upload the ZIP archive to an S3 bucket.

    Args:
        s3_client (boto3.client): S3 client object.
        zip_file_path (str): Path to the ZIP archive file.
        bucket_name (str): Name of the S3 bucket.
    """
    try:
        s3_client.upload_file(
            zip_file_path, bucket_name, os.path.basename(zip_file_path)
        )
        logging.info(
            f"ZIP file uploaded to S3: s3://{bucket_name}/{os.path.basename(zip_file_path)}"
        )
    except Exception as e:
        logging.error(f"Error uploading ZIP file to S3: {e}")


def cleanup(temp_dir, zip_file_path):
    """
    Clean up the temporary directory and the ZIP archive file.

    Args:
        temp_dir (str): Path to the temporary directory.
        zip_file_path (str): Path to the ZIP archive file.
    """
    try:
        os.remove(zip_file_path)
        logging.info(f"Removed ZIP file: {zip_file_path}")
    except Exception as e:
        logging.error(f"Error removing ZIP file: {e}")

    try:
        for file_path in os.listdir(temp_dir):
            os.remove(os.path.join(temp_dir, file_path))
        os.rmdir(temp_dir)
        logging.info(f"Removed temporary directory: {temp_dir}")
    except Exception as e:
        logging.error(f"Error removing temporary directory: {e}")


def main():
    zip_filename = input("Please enter the desired zip file name : ")
    if zip_filename:
        if not zip_filename.endswith(".zip"):
            zip_filename += ".zip"
    else:
        now = datetime.now()
        formatted_timestamp = now.strftime("%Y-%m-%d-%H-%M-%S")
        zip_filename = f"certificates_{formatted_timestamp}.zip"
    
    temp_dir = create_temp_directory()
    s3_client = setup_s3_client()
    driver = setup_web_driver(temp_dir)
    df = pd.read_csv(CSV_FILE_PATH)

    certificates = []
    failed_downloads = []
    for _, row in df.iterrows():
        id = row["id"]
        download_link = row["download_link"]
        verification_link = row["verification_link"]
        name = row["name"]
        certificate_path = download_certificate(download_link, temp_dir, driver, name)
        if certificate_path:
            certificates.append(certificate_path)
        else:
            failed_downloads.append((name, download_link))

    zip_file_path = create_zip_archive(zip_filename, temp_dir, certificates)
    upload_to_s3(s3_client, zip_file_path, S3_BUCKET_NAME)
    cleanup(temp_dir, zip_file_path)
    driver.quit()
    logging.info("Done")

    bucket_location = setup_s3_client().get_bucket_location(Bucket=S3_BUCKET_NAME)
    object_url = "https://s3-{0}.amazonaws.com/{1}/{2}".format(
        S3_BUCKET_LOCATION, S3_BUCKET_NAME, zip_filename
    )

    if failed_downloads:
        logging.error("Failed downloads:")
        err_message = f"Name: {name}, Download link: {download_link}"

        for name, download_link in failed_downloads:
            logging.error(err_message)

        print("Some downloads have failed, checkout the list below:")
        for name, download_link in failed_downloads:
            print(err_message)

    else:
        successful_download_message = f"All items are downloaded successfully. You can find the zip on this link : {object_url}"
        logging.info(successful_download_message)
        print(successful_download_message)


if __name__ == "__main__":
    main()
