import uuid
from google.cloud import storage


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
        db          -   Database object
        project     -   Project ID
        bucket      -   Bucket name
        directory   -   Directory name
        url         -   URL of the image
        blob        -   Image data
        title       -   Title of the image
        tags        -   List of tags
        prompt      -   Prompt for the image
        model       -   Model used to generate the image
    """

    def __init__(self, **kwargs):
        self.id = kwargs.get("id", str(uuid.uuid4()))
        self.db = kwargs.get("db")
        self.project = kwargs.get("project")
        self.bucket = kwargs.get("bucket")
        self.directory = kwargs.get("directory")
        self.url = kwargs.get(
            "url", "https://www.imgflip.com/s/meme/One-Does-Not-Simply.jpg"
        )
        self.blob = kwargs.get("blob", None)
        self.title = kwargs.get("title", "<insert title here>")
        self.tags = kwargs.get("tags", ["#tag"])
        self.prompt = kwargs.get("prompt", "<insert prompt here>")
        self.model = kwargs.get("model")

    def __str__(self):
        return f"Image(image_id={self.id}, image_url={self.url})"

    def __dict__(self):
        return {
            "id": self.id,
            "url": self.url,
            "title": self.title,
            "prompt": self.prompt,
            "tags": self.tags,
            "model": self.model,
        }

    def __repr__(self):
        return str(self)

    def save(self):
        self.url = add_blob(self.bucket, f"{self.directory}/{self.id}", self.blob)
        self.db.insert(self)
