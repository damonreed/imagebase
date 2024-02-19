from src.imagebase import db
from google.cloud import bigquery as bq
from pprint import pprint as pp

project = "imagebase-414802"
dataset = "imagebase"
table = "fantasy"

SCHEMA = [
    bq.SchemaField("id", "STRING", mode="REQUIRED"),
    bq.SchemaField("url", "STRING", mode="REQUIRED"),
    bq.SchemaField("blob", "BYTES", mode="NULLABLE"),
    bq.SchemaField("title", "STRING", mode="NULLABLE"),
    bq.SchemaField("prompt", "STRING", mode="NULLABLE"),
    bq.SchemaField("tags", "STRING", mode="REPEATED"),
    bq.SchemaField("model", "STRING", mode="REQUIRED"),
]

db_client = db.DB(project=project, dataset=dataset, table=table)
db_client.create_table(SCHEMA)

pp(db_client.get_schema())

for field in db_client.get_schema():
    if field.name == "new_column":
        db_client.remove_column("new_column")

pp(db_client.get_schema())

db_client.add_column("new_column", "STRING", "NULLABLE")
pp(db_client.get_schema())

db_client.reset_table()

pp(db_client.get_schema())

db_client.delete_table()
