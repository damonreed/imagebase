from google.cloud import bigquery as bq
from google.cloud import bigquery as bq

SCHEMA = [
    bq.SchemaField("id", "STRING", mode="REQUIRED"),
    bq.SchemaField("url", "STRING", mode="REQUIRED"),
    bq.SchemaField("blob", "BYTES", mode="NULLABLE"),
    bq.SchemaField("title", "STRING", mode="NULLABLE"),
    bq.SchemaField("prompt", "STRING", mode="NULLABLE"),
    bq.SchemaField("tags", "STRING", mode="REPEATED"),
    bq.SchemaField("model", "STRING", mode="REQUIRED"),
]


class DB:
    """
    Database object
    """

    def __init__(self, project, dataset, table):
        self.project = project
        self.dataset = dataset
        self.table = table
        self.client = bq.Client(project=project)
        self.path = f"{self.project}.{self.dataset}.{self.table}"

    def create_table(self, schema):
        # Check if the table exists, if not, create it
        try:
            table = self.client.get_table(self.path)
            print(f"Table {self.path} already exists.")
        except:
            print(f"Table {self.path} does not exist. Creating it now...")
            table = bq.Table(self.path, schema=schema)
            table = self.client.create_table(table)
            print(f"Table {table} created successfully.")

    def delete_table(self):
        try:
            self.client.delete_table(self.path)
            print(f"Table {self.path} deleted successfully.")
        except:
            print(f"Table {self.path} does not exist.")

    def get_schema(self):
        table = self.client.get_table(self.path)
        return table.schema

    # Add a column to the table
    def add_column(self, column_name, column_type, column_mode):
        table = self.client.get_table(self.path)  # Make an API request.

        original_schema = table.schema
        new_schema = original_schema[:]  # Creates a copy of the schema.
        new_schema.append(bq.SchemaField(column_name, column_type, mode=column_mode))

        table.schema = new_schema
        table = self.client.update_table(table, ["schema"])  # Make an API request.

        if len(table.schema) == len(original_schema) + 1 == len(new_schema):
            print("A new column has been added.")
        else:
            print("The column has not been added.")

    def remove_column(self, column_name):
        query = f"""
            ALTER TABLE `{self.path}`
            DROP COLUMN {column_name}
        """
        query_job = self.client.query(query)  # Make an API request.
        response = query_job.result()  # Waits for query to finish
        print(f"{response}")

    # Reset the table
    def reset_table(self):
        schema = self.get_schema()
        self.delete_table()
        self.create_table(schema)

    ##
    ## CRUD Functions
    ##

    # Get a row from the table identified by image_id
    def get(self, image_id):
        query = f"""
            SELECT * FROM `{self.path}`
            WHERE id = "{image_id}"
        """
        query_job = self.client.query(query)  # Make an API request.
        rows = query_job.result()  # Waits for query to finish
        print(f"Query results loaded to table {self.path}.")
        print(rows)
        return rows

    # Get all rows from the table
    def get_all(self):
        query = f"""
            SELECT * FROM `{self.path}`
        """
        query_job = self.client.query(query)  # Make an API request.
        rows = query_job.result()  # Waits for query to finish
        return rows

    # Add a row to the table
    def insert(self, image):

        rows_to_insert = [
            image.__dict__(),
        ]
        table = self.client.get_table(self.path)
        errors = self.client.insert_rows(table, rows_to_insert)  # Make an API request.
        if errors == []:
            print("New rows have been added.")
        else:
            print("Encountered errors while inserting rows: {}".format(errors))

    # Update an existing row in the table identified by image_id with new data in an Image object
    def update(self, image_id, image):
        query = f"""
            UPDATE `{self.path}`
            SET url = "{image.url}",
                title = "{image.title}",
                descr = "{image.descr}",
                tags = {image.tags},
                context = "{image.context}",
                prompt = "{image.prompt}",
                m_context = "{image.m_context}"
            WHERE id = "{image_id}"
        """
        query_job = self.client.query(query)  # Make an API request.
        rows = query_job.result()  # Waits for query to finish
        print(f"Query results loaded to table {self.path}.")
        print(rows)
