import zipfile, os
from google.cloud import bigquery
import pandas as pd
import time
from upload_vars import UPLOAD_TEST, UPLOAD_CNPJ, CNPJ_working_directory, UPLOAD_CEIS, CEIS_working_directory, UPLOAD_CEPIM, CEPIM_working_directory
from upload_vars import UPLOAD_PEP, PEP_working_directory, UPLOAD_CNEP, CNEP_working_directory, UPLOAD_CEAF, CEAF_working_directory, UPLOAD_Leniencia, Leniencia_working_directory
from upload import upload_file

# Definindo Dataset de Teste!
# ------------------

data = {'name': ['Alice', 'Bob', 'Charlie', 'David', 'Ella'],
        'age': [25, 30, 35, 40, 45],
        'country': ['USA', 'Canada', 'UK', 'Australia', 'New Zealand']}

df = pd.DataFrame(data)

# Definindo dataset e tabela!
dataset_id = 'datasetTEST'
table_id = 'tableTEST'

# Credenciais dos dados
# ------------------

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'projetodatamining-9ee812a16378.json'

client = bigquery.Client()

# Upload dos dados de Teste!
# ------------------

if UPLOAD_TEST == 1:
    schema = [
        bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("age", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("country", "STRING", mode="REQUIRED"),
    ]

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
    print("FIM UPLOAD DE TABELA TESTES")


# ------------------------------------------------------

if UPLOAD_CNPJ == 1:

    print("Dando unzip em todos os arquivos da pasta de CNPJ!")
    
    os.chdir(CNPJ_working_directory)

    for file in os.listdir(CNPJ_working_directory):   
        if zipfile.is_zipfile(file): 
            with zipfile.ZipFile(file) as item: 
                item.extractall()  
                print("Dando unzip em - " + str(file))

    print("Lendo arquivos de CNPJ")

    for file in os.listdir(CNPJ_working_directory):   
        if not zipfile.is_zipfile(file) and "pdf" not in str(file):
            
            print("Lendo - " + str(file))
            
            if "EMPRECSV" in file:
                # esse encoding inclui acentos
                df = pd.read_csv(file, encoding="ISO-8859-1")
                upload_file(df, client, "datasetCNPJ", "tableEmpresas")

# ------------------------------------------------------

if UPLOAD_CEIS == 1:

    os.chdir(CEIS_working_directory)
    for file in os.listdir(CEIS_working_directory):   
        if not zipfile.is_zipfile(file) and file != ".DS_Store":

            print("Lendo - " + str(file))
            # esse encoding inclui acentos
            df = pd.read_csv(file, sep=';', encoding="ISO-8859-1", on_bad_lines='skip')
            df = df.astype(str)

            if file == "20230223_CEIS.csv":

                upload_file(df, client, "datasetCEIS", "tableCEIS_2")
            
            else:

                upload_file(df, client, "datasetCEIS", "tableCEIS")

# ------------------------------------------------------

if UPLOAD_CEPIM == 1:
    
    os.chdir(CEPIM_working_directory)
    for file in os.listdir(CEPIM_working_directory):   
        if not zipfile.is_zipfile(file) and file != ".DS_Store":

            print("Lendo - " + str(file))
            # esse encoding inclui acentos
            df = pd.read_csv(file, sep=';', encoding="ISO-8859-1", on_bad_lines='skip')
            df = df.astype(str)

            upload_file(df, client, "datasetCEPIM", "tableCEPIM")

# ------------------------------------------------------

if UPLOAD_PEP == 1:

    os.chdir(PEP_working_directory)
    for file in os.listdir(PEP_working_directory):   
        if not zipfile.is_zipfile(file) and file != ".DS_Store":

            print("Lendo - " + str(file))
            # esse encoding inclui acentos
            df = pd.read_csv(file, sep=';', encoding="ISO-8859-1", on_bad_lines='skip')
            df = df.astype(str)

            upload_file(df, client, "datasetPEP", "tablePEP")

# ------------------------------------------------------

if UPLOAD_CNEP == 1:

    os.chdir(CNEP_working_directory)
    for file in os.listdir(CNEP_working_directory):   
        if not zipfile.is_zipfile(file) and file != ".DS_Store":

            print("Lendo - " + str(file))
            # esse encoding inclui acentos
            df = pd.read_csv(file, sep=';', encoding="ISO-8859-1", on_bad_lines='skip')
            df = df.astype(str)

            upload_file(df, client, "datasetCNEP", "tableCNEP")

# ------------------------------------------------------

if UPLOAD_CEAF == 1:
    
    os.chdir(CEAF_working_directory)
    for file in os.listdir(CEAF_working_directory):   
        if not zipfile.is_zipfile(file) and file != ".DS_Store":

            print("Lendo - " + str(file))
            # esse encoding inclui acentos
            df = pd.read_csv(file, sep=';', encoding="ISO-8859-1", on_bad_lines='skip')
            df = df.astype(str)

            upload_file(df, client, "datasetCEAF", "tableExpulsoes")

# ------------------------------------------------------

if UPLOAD_Leniencia == 1:
    
    os.chdir(Leniencia_working_directory)
    for file in os.listdir(Leniencia_working_directory):   
        if not zipfile.is_zipfile(file) and file != ".DS_Store":

            print("Lendo - " + str(file))
            # esse encoding inclui acentos
            df = pd.read_csv(file, sep=';', encoding="ISO-8859-1", on_bad_lines='skip')
            df = df.astype(str)

            if "Acordos" in str(file):
                upload_file(df, client, "datasetLeniencia", "tableAcordos")

            elif "Efeitos" in str(file):
                upload_file(df, client, "datasetLeniencia", "tableEfeitos")


# ------------------------------------------------------


# ------------------------------------------------------
