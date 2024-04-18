from google.cloud import bigquery
from google.oauth2 import service_account
import jsonlines
import json


month_to_int = {
    'enero': 1,
    'febrero': 2,
    'marzo': 3,
    'abril': 4,
    'mayo': 5,
    'junio': 6,
    'julio': 7,
    'agosto': 8,
    'septiembre': 9,
    'octubre': 10,
    'noviembre': 11,
    'diciembre': 12
}

json_file_path = "ordenes_r2"
with open(json_file_path + '.json', 'r', encoding='utf-8') as json_file:
    json_data = json_file.read()

json_data = json_data.replace('ó', 'o')

data = json.loads(json_data)

with jsonlines.open(json_file_path + '.jsonl', 'w') as writer:
    for month in data:
        for item in data[month]:
            for key in item.keys():
                if key != 'orden_id':
                    fruit = {
                        'fruta' : key,
                        'cantidad' : item[key],
                        'mes' : month_to_int[month],
                        'orden_id' : item['orden_id'],
                        'region' : 2
                    }
                    writer.write(fruit)

# Replace with your own JSON file path and BigQuery dataset and table details
project_id = "bq-project-418519"
dataset_id = "tarea1"
table_id = "ordenes"

# Set up credentials (replace 'path/to/your/credentials.json' with your service account key file)
credentials = service_account.Credentials.from_service_account_file(
    "bq-project-418519-a392272e58f9.json",
)

# Create a BigQuery client
client = bigquery.Client(project=project_id, credentials=credentials)

# Specify the dataset and table to which you want to upload the data
dataset_ref = client.dataset(dataset_id)
table_ref = dataset_ref.table(table_id)

# Load the JSON file into BigQuery
job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
job_config.autodetect = True  # This allows BigQuery to automatically detect the schema

with open(json_file_path + '.jsonl', "rb") as source_file:
    job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

job.result()  # Wait for the job to complete

print(f"Loaded {job.output_rows} rows into {table_id}")