from google.cloud import bigquery
import time

def upload_file(df, client, dataset_id, table_id):
    
    schema = [bigquery.SchemaField(name, 'STRING') for name in df.columns]

    # Cria BigQuery "table object"
    table_ref = client.dataset(dataset_id).table(table_id)
    table = bigquery.Table(table_ref, schema=schema)

    # Escreve dados
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.autodetect = True
    job = client.load_table_from_dataframe(df, table, job_config=job_config)

    # Espera a conclusão do upload
    #job.result()

    # Mostra no console que o upload continua!
    while job.state != 'DONE':
        time.sleep(5)
        job.reload()
        print(job.state)

    print(f'{job.output_rows} rows imported to {table_id}')
    print("\n\nFIM UPLOAD DE TABELA\n\n")