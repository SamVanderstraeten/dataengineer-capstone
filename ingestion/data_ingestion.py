import boto3, json, requests, pendulum, random, datetime

S3_URL = "s3a://dataminded-academy-capstone-resources/sampxl/ingest/"
BUCKET = "dataminded-academy-capstone-resources"
BUCKET_FOLDER = "sampxl/ingest"
OPENAQ_URL = "https://api.openaq.org/v2/latest?limit=100&page=1&offset=0&sort=desc&radius=1000&order_by=lastUpdated&dumpRaw=false"

if __name__ == "__main__":

    response = requests.get(OPENAQ_URL)
    data = response.json()
    s3 = boto3.resource('s3')
    ct = datetime.datetime.now().timestamp()
    filename = f"aq_data_{ct}"
    s3object = s3.Object(BUCKET, f"{BUCKET_FOLDER}/{filename}.json")
    s3object.put(
        Body=(bytes(json.dumps(data).encode('UTF-8')))
    )
    