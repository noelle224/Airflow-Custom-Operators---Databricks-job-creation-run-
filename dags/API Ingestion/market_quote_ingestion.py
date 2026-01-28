import os
import requests
import boto3
from dotenv import load_dotenv

load_dotenv(dotenv_path = os.path.join(os.path.dirname(__file__), "..", ".venv", "keys.env"))

# --- Store these securely in environment variables ---
API_KEY = 'tmngnp7v4xa8h6yo'
ACCESS_TOKEN = 'vggz4zf3i5z91ihte484dxnf04jb119t'

# --- Step 1: Fetch instruments data from Kite API ---
url = "https://api.kite.trade/user/margins"
headers = {
    "X-Kite-Version": "3",
    "Authorization": f"token {API_KEY}:{ACCESS_TOKEN}"
}

response = requests.get(url, headers=headers)
print(response.text)
