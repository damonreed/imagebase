import uuid
from google.cloud import storage
from .db import DB


def add_blob(bucket, object_name, data):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_string(data)
    return blob.public_url


class Image:
    """
    Image object

    Attributes:
        id          -   Unique identifier for the image
        project     -   Project ID
        dataset     -   Dataset name
        bucket      -   Bucket name
        label       -   Table & bucket directory name
        url         -   URL of the image
        blob        -   Image data
        title       -   Title of the image
        prompt      -   Prompt for the image
        model       -   Model used to generate the image
        tags        -   List of tags
    """

    def __init__(self, **kwargs):
        self.id = kwargs.get("id", str(uuid.uuid4()))
        self.project = kwargs.get("project")
        self.dataset = kwargs.get("dataset")
        self.bucket = kwargs.get("bucket")
        self.label = kwargs.get("label")
        self.url = kwargs.get(
            "url", "https://www.imgflip.com/s/meme/One-Does-Not-Simply.jpg"
        )
        self.blob = kwargs.get("blob", None)
        self.title = kwargs.get("title", "<insert title here>")
        self.prompt = kwargs.get("prompt", "<insert prompt here>")
        self.model = kwargs.get("model")
        self.tags = kwargs.get("tags", ["#tag"])
        self.__db = DB(project=self.project, dataset=self.dataset, table=self.label)

    def __str__(self):
        return f"Image(image_id={self.id}, image_url={self.url})"

    def __dict__(self):
        return {
            "id": self.id,
            "url": self.url,
            "blob": self.blob,
            "title": self.title,
            "prompt": self.prompt,
            "model": self.model,
            "tags": self.tags,
        }

    def __repr__(self):
        return str(self)

    def save(self):
        self.url = add_blob(self.bucket, f"{self.label}/{self.id}", self.blob)
        self.__db.insert(self)
