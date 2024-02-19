from src.imagebase import db
from src.imagebase import image
from google.cloud import bigquery as bq
from pprint import pprint as pp
import requests

project = "imagebase-414802"
dataset = "imagebase"
bucket = "nomadia-org-imagebase"
label = "fantasy"

title = "A fantasy image"
prompt = "A fantasy prompt"
model = "StableCascade"
tags = ["#fantasy", "#art", "#magic"]

blob = requests.get("https://i.imgflip.com/6nlb4g.jpg").content

SCHEMA = [
    bq.SchemaField("id", "STRING", mode="REQUIRED"),
    bq.SchemaField("url", "STRING", mode="REQUIRED"),
    bq.SchemaField("blob", "BYTES", mode="NULLABLE"),
    bq.SchemaField("title", "STRING", mode="NULLABLE"),
    bq.SchemaField("prompt", "STRING", mode="NULLABLE"),
    bq.SchemaField("model", "STRING", mode="REQUIRED"),
    bq.SchemaField("tags", "STRING", mode="REPEATED"),
]

db_client = db.DB(project=project, dataset=dataset, table=label)

# db_client.create_table(SCHEMA)

# pp(db_client.get_schema())

# for field in db_client.get_schema():
#     if field.name == "new_column":
#         db_client.remove_column("new_column")


# db_client.add_column("new_column", "STRING", "NULLABLE")
# pp(db_client.get_schema())

# db_client.reset_table()

# pp(db_client.get_schema())

image = image.Image(
    project=project,
    dataset=dataset,
    bucket=bucket,
    label=label,
    blob=blob,
    title=title,
    prompt=prompt,
    model=model,
    tags=tags,
)

## pp(image.__dict__)

image.save()

# db_client.delete_table()
