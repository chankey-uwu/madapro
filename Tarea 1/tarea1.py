from google.cloud import bigquery
from google.oauth2 import service_account
import jsonlines
import json

json_file_path = "usuarios_ordenes_r2"
json_data = open(json_file_path + '.json')
data = json.load(json_data)

for item in data['values']:
    item['region'] = 2
    item['dia'] = int(item['fecha'].split('T')[0].split('-')[2])
    item['mes'] = item.pop('fecha')
    item['mes'] = int(item['mes'].split('-')[1])

with jsonlines.open(json_file_path + '.jsonl', 'w') as writer:
    writer.write_all(data['values'])

# Replace with your own JSON file path and BigQuery dataset and table details
project_id = "bq-project-418519"
dataset_id = "tarea1"
table_id = "usuarios_ordenes"

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