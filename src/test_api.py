import requests

url = "http://127.0.0.1:8000/run-job"

payload = {
    "args": [
        "--fmp-hist-ticker",
        "--ticker", "AAPL",
        "--start", "2023-01-01",
        "--end", "2023-12-31"
    ]
}

response = requests.post(url, json=payload)

print("Status Code:", response.status_code)
print("Response JSON:", response.json())
