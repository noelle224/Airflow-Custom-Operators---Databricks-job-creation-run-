import os
import requests
import boto3
from dotenv import load_dotenv

load_dotenv(dotenv_path = os.path.join(os.path.dirname(__file__), "..", ".venv", "keys.env"))

# --- Store these securely in environment variables ---
API_KEY = os.getenv("KITE_API_KEY")
ACCESS_TOKEN = os.getenv("KITE_ACCESS_TOKEN")
BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_FILE_KEY = "kite/instruments.csv"  # S3 path where file will be uploaded

# --- Step 1: Fetch instruments data from Kite API ---
url = "https://api.kite.trade/instruments"
headers = {
    "X-Kite-Version": "3",
    "Authorization": f"token {API_KEY}:{ACCESS_TOKEN}"
}

response = requests.get(url, headers=headers)

if response.status_code == 200:
    csv_data = response.text

    # --- Step 2: Upload to S3 ---
    s3 = boto3.client("s3")

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=S3_FILE_KEY,
        Body=csv_data,
        ContentType="text/csv"
    )

    print(f"✅ Instruments file uploaded to s3://{BUCKET_NAME}/{S3_FILE_KEY}")

else:
    print(f"❌ Failed to fetch instruments. Status: {response.status_code}")
    print(response.text)
