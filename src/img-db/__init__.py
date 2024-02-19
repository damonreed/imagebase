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
import image
import db
from google.cloud import storage


def add_blob(bucket, object_name, data):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_string(data)
    return blob.public_url
