#
# __init__.py for image_db
#
"""
Image Database

This module provides a class for managing AI generated images in a BigQuery database.

Classes:    Image   -   Image object

Functions:  get     -   Get a row from the table identified by image_id
            get_all -   Get all rows from the table
            insert  -   Add a row to the table
            update  -   Update an existing row in the table identified by image_id with new data in an Image object

"""
import uuid
from google.cloud import bigquery
from google.cloud import storage


def add_blob(bucket, object_name, data):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_string(data)
    return blob.public_url


# Set up BigQuery CLIENT
CLIENT = bigquery.Client()

SCHEMA = [
    bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("url", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("blob", "BYTES", mode="NULLABLE"),
    bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("descr", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("tags", "STRING", mode="REPEATED"),
    bigquery.SchemaField("context", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("topic", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("prompt", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("response", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("m_context", "STRING", mode="REQUIRED"),
]

# Define the table reference
TABLE = CLIENT.dataset("virtualnetengg").table("images")

# Define the Cloud Storage bucket and blob prefix where the extracted images will be stored
BUCKET = "virtualnetengg"
PREFIX = "images/"


class Image:
    """
    Image object

    Attributes:
        id (str): Unique identifier for the image
        url (str): URL of the image
        blob (bytes): Image in raw bytes
        title (str): Title of the image
        descr (str): Description of the image
        tags (list): List of tags associated with the image
        context (str): Context of the image
        topic (str): Topic of the image
        prompt (str): Prompt for the image
        response (str): Response for the image
        m_context (str): Memester context for the image description
    """

    def __init__(self, **kwargs):
        self.id = kwargs.get("id", str(uuid.uuid4()))
        self.url = kwargs.get(
            "url", "https://www.imgflip.com/s/meme/One-Does-Not-Simply.jpg"
        )
        self.blob = kwargs.get("blob", None)
        self.title = kwargs.get("title", "<insert title here>")
        self.descr = kwargs.get("descr", "<insert description here>")
        self.tags = kwargs.get("tags", ["#tag"])
        self.context = kwargs.get("context", "<insert context here>")
        self.topic = kwargs.get("topic", "<insert topic here>")
        self.prompt = kwargs.get("prompt", "<insert prompt here>")
        self.response = kwargs.get("response", "<insert response here>")
        self.m_context = kwargs.get("m_context", "<insert meme context here>")

    def __str__(self):
        return f"Image(image_id={self.id}, image_url={self.url})"

    def __dict__(self):
        return {
            "id": self.id,
            "url": self.url,
            "blob": self.blob,
            "title": self.title,
            "descr": self.descr,
            "tags": self.tags,
            "context": self.context,
            "topic": self.topic,
            "prompt": self.prompt,
            "response": self.response,
            "m_context": self.m_context,
        }

    def __repr__(self):
        return str(self)

    def save(self):
        # Just inserting for now
        insert(self)


##
## Helper Functions
##


def __create_table():
    # Check if the table exists, if not, create it
    try:
        table = CLIENT.get_table(TABLE)
        print(f"Table {TABLE.table_id} already exists.")
    except:
        print(f"Table {TABLE.table_id} does not exist. Creating it now...")
        table = bigquery.Table(TABLE, schema=SCHEMA)
        table = CLIENT.create_table(table)
        print(f"Table {table.table_id} created successfully.")


def __delete_table():
    try:
        CLIENT.delete_table(TABLE)
        print(f"Table {TABLE.table_id} deleted successfully.")
    except:
        print(f"Table {TABLE.table_id} does not exist.")


# Reset the table
def __reset_table():
    __delete_table()
    __create_table()


##
## CRUD Functions
##


# Get a row from the table identified by image_id
def get(image_id):
    query = f"""
        SELECT * FROM `virtualnetengg.images`
        WHERE id = "{image_id}"
    """
    query_job = CLIENT.query(query)  # Make an API request.
    rows = query_job.result()  # Waits for query to finish
    print(f"Query results loaded to table {TABLE.table_id}.")
    print(rows)
    return rows


# Get all rows from the table
def get_all():
    query = f"""
        SELECT * FROM `virtualnetengg.images`
    """
    query_job = CLIENT.query(query)  # Make an API request.
    rows = query_job.result()  # Waits for query to finish
    return rows


# Add a row to the table
def insert(image):
    image.url = add_blob(BUCKET, f"images/{image.id}", image.blob)

    rows_to_insert = [
        image.__dict__(),
    ]
    table = CLIENT.get_table(TABLE)
    errors = CLIENT.insert_rows(table, rows_to_insert)  # Make an API request.
    if errors == []:
        print("New rows have been added.")
    else:
        print(
            "Encountered errors while inserting rows: {}".format(errors[0]["message"])
        )


# Update an existing row in the table identified by image_id with new data in an Image object
def update(image_id, image):
    query = f"""
        UPDATE `virtualnetengg.images`
        SET url = "{image.url}",
            title = "{image.title}",
            descr = "{image.descr}",
            tags = {image.tags},
            context = "{image.context}",
            prompt = "{image.prompt}",
            m_context = "{image.m_context}"
        WHERE id = "{image_id}"
    """
    query_job = CLIENT.query(query)  # Make an API request.
    rows = query_job.result()  # Waits for query to finish
    print(f"Query results loaded to table {TABLE.table_id}.")
    print(rows)
