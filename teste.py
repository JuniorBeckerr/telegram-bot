import boto3

client = boto3.client(
    "s3",
    region_name="nyc3",
    endpoint_url="https://nyc3.digitaloceanspaces.com",
    aws_access_key_id="DO801N79B86DQ6HH43V6",
    aws_secret_access_key="OSKDipRfn34iIgWf1Gb8DQyQzNUuka69ghX+u7nL40E",
)

resp = client.list_objects_v2(
    Bucket="storage-becker",
    Prefix="data-telegram/ingridgermanno/"
)

for obj in resp.get("Contents", []):
    print(obj["Key"])
