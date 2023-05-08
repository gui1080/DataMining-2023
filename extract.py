import zipfile, os
from google.cloud import bigquery
import pandas as pd
import time
import sqlite3
from upload_vars import UPLOAD_TEST, UPLOAD_CNPJ, CNPJ_working_directory, UPLOAD_CEIS, CEIS_working_directory, UPLOAD_CEPIM, CEPIM_working_directory
from upload_vars import UPLOAD_PEP, PEP_working_directory, UPLOAD_CNEP, CNEP_working_directory, UPLOAD_CEAF, CEAF_working_directory, UPLOAD_Leniencia, Leniencia_working_directory
from upload_vars import UPLOAD_Benef, Benef_working_directory, UPLOAD_DividaAtiva, Divida_working_directory, UPLOAD_ComprasNet, ComprasNet_database_directory
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

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'projetodatamining-b682ac1b63ad.json'

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

if UPLOAD_Benef == 1:

    os.chdir(Benef_working_directory)

    for root, dirs, files in os.walk(Benef_working_directory):
        for file in files:

            file_path = os.path.join(root, file)
            
            if ".csv" in file:

                print("Lendo - " + str(file))
                # esse encoding inclui acentos
                df = pd.read_csv(file_path, sep=';', encoding="ISO-8859-1", on_bad_lines='skip')
                df = df.astype(str)
                
                if "SeguroDefeso" in str(file):
                    upload_file(df, client, "datasetBeneficiosSociais", "tableSeguroDefeso")

                elif "BPC" in str(file):
                    upload_file(df, client, "datasetBeneficiosSociais", "tableBenefPrestacaoContinuada")
                
                elif "AuxilioBrasil" in str(file):
                    upload_file(df, client, "datasetBeneficiosSociais", "tableAuxilioBrasil")
                
                elif "AuxilioEmergencial" in str(file):
                    upload_file(df, client, "datasetBeneficiosSociais", "tableAuxilioEmergencial")

                elif "GarantiaSafra" in str(file):
                    upload_file(df, client, "datasetBeneficiosSociais", "tableGarantiaSafra")

                elif "PETI" in str(file):
                    upload_file(df, client, "datasetBeneficiosSociais", "tablePETI")

# ------------------------------------------------------

if UPLOAD_DividaAtiva == 1:

    os.chdir(Divida_working_directory)

    for root, dirs, files in os.walk(Divida_working_directory):
        for file in files:

            file_path = os.path.join(root, file)
            
            if ".csv" in file:

                print("Lendo - " + str(file))
                # esse encoding inclui acentos
                df = pd.read_csv(file_path, sep=';', encoding="ISO-8859-1", on_bad_lines='skip')
                df = df.astype(str)
                
                if "lai_SIDA" in str(file):
                    upload_file(df, client, "datasetDividaAtiva", "tableNaoPrevidenciario")

                elif "lai_FGTS" in str(file):
                    upload_file(df, client, "datasetDividaAtiva", "tableFGTS")
                
                elif "lai_PREV" in str(file):
                    upload_file(df, client, "datasetDividaAtiva", "tableDadosAbertosPrevidenciarios")
                
# ------------------------------------------------------

if UPLOAD_ComprasNet == 1:

    conn = sqlite3.connect(ComprasNet_database_directory)
    c = conn.cursor()

    c.execute('''
        SELECT * FROM comprasnet_contratos_anual_contratos_latest
    ''')

    # comprasnet_contratos_anual_contratos_latest
    df = pd.DataFrame(c.fetchall(), columns=["id", "receita_despesa", "numero", "orgao_codigo", "orgao_nome", "unidade_codigo", "unidade_nome_resumido", "unidade_nome", "unidade_origem_codigo", "unidade_origem_nome", "fornecedor_tipo", "fonecedor_cnpj_cpf_idgener", "fornecedor_nome", "codigo_tipo", "tipo", "categoria", "processo", "objeto", "fundamento_legal", "informacao_complementar", "codigo_modalidade", "modalidade", "unidade_compra", "licitacao_numero", "data_assinatura", "data_publicacao", "vigencia_inicio", "vigencia_fim", "valor_inicial", "valor_global", "num_parcelas", "valor_parcela", "valor_acumulado", "situacao"])
    df = df.astype(str)
    
    time.sleep(1)
    upload_file(df, client, "datasetComprasNet", "tableContratos")
    time.sleep(1)

    #comprasnet_contratos_anual_cronogramas_latest

    c.execute('''
        SELECT * FROM comprasnet_contratos_anual_cronogramas_latest
    ''')

    df = pd.DataFrame(c.fetchall(), columns=["id","contrato_id","tipo","numero","receita_despesa","observacao","mesref","anoref","vencimento","retroativo","valor"])
    df = df.astype(str)
    
    time.sleep(1)
    upload_file(df, client, "datasetComprasNet", "tableCronogramas")
    time.sleep(1)

    # comprasnet_contratos_anual_despesas_acessorias_latest

    c.execute('''
        SELECT * FROM comprasnet_contratos_anual_despesas_acessorias_latest
    ''')

    df = pd.DataFrame(c.fetchall(), columns=["id","contrato_id","tipo_id","recorrencia_id","descricao_complementar","vencimento","valor"])
    df = df.astype(str)
    
    time.sleep(1)
    upload_file(df, client, "datasetComprasNet", "tableDespesasAcessorias")
    time.sleep(1)

    # comprasnet_contratos_anual_empenhos_latest

    c.execute('''
        SELECT * FROM comprasnet_contratos_anual_empenhos_latest
    ''')

    df = pd.DataFrame(c.fetchall(), columns=["id","unidade","unidade_nome","gestao","numero_empenho","data_emissao","cpf_cnpj_credor","credor","fonte_recurso","naturezadespesa","naturezadespesa_descricao","planointerno","planointerno_descricao","valor_empenhado","valor_aliquidar","valor_liquidado","valor_pago","valor_rpinscrito","valor_rpaliquidar","valor_rpaliquidado","valor_rppago","informacao_complementar","sistema_origem","contrato_id"])
    df = df.astype(str)
    
    time.sleep(1)
    upload_file(df, client, "datasetComprasNet", "tableEmpenhos")
    time.sleep(1)

    # comprasnet_contratos_anual_faturas_latest

    c.execute('''
        SELECT * FROM comprasnet_contratos_anual_empenhos_latest
    ''')

    df = pd.DataFrame(c.fetchall(), columns=["id","contrato_id","tipolistafatura_id","justificativafatura_id","numero","emissao","prazo","vencimento","valor","juros","multa","glosa","valorliquido","processo","protocolo","ateste","repactuacao","infcomplementar","mesref","anoref","situacao"])
    df = df.astype(str)
    
    time.sleep(1)
    upload_file(df, client, "datasetComprasNet", "tableFaturas")
    time.sleep(1)

    # comprasnet_contratos_anual_garantias_latest

    c.execute('''
        SELECT * FROM comprasnet_contratos_anual_garantias_latest
    ''')

    df = pd.DataFrame(c.fetchall(), columns=["id","contrato_id","tipo","valor","vencimento"])
    df = df.astype(str)
    
    time.sleep(1)
    upload_file(df, client, "datasetComprasNet", "tableGarantias")
    time.sleep(1)

    # comprasnet_contratos_anual_historicos_latest

    c.execute('''
        SELECT * FROM comprasnet_contratos_anual_historicos_latest
    ''')

    df = pd.DataFrame(c.fetchall(), columns=["id","contrato_id","receita_despesa","numero","observacao","ug","gestao","fornecedor","codigo_tipo","tipo","categoria","processo","objeto","fundamento_legal_aditivo","informacao_complementar","modalidade","licitacao_numero","codigo_unidade_origem","nome_unidade_origem","data_assinatura","data_publicacao","vigencia_inicio","vigencia_fim","valor_inicial","valor_global","num_parcelas","valor_parcela","novo_valor_global","novo_num_parcelas","novo_valor_parcela","data_inicio_novo_valor","retroativo","retroativo_mesref_de","retroativo_anoref_de","retroativo_mesref_ate","retroativo_anoref_ate","retroativo_vencimento","retroativo_valor"])
    df = df.astype(str)
    
    time.sleep(1)
    upload_file(df, client, "datasetComprasNet", "tableHistoricos")
    time.sleep(1)

    # comprasnet_contratos_anual_itens_latest

    c.execute('''
        SELECT * FROM comprasnet_contratos_anual_itens_latest
    ''')

    df = pd.DataFrame(c.fetchall(), columns=["id","contrato_id","tipo_id","grupo_id","catmatseritem_id","descricao_complementar","quantidade","valorunitario","valortotal"])
    df = df.astype(str)
    
    time.sleep(1)
    upload_file(df, client, "datasetComprasNet", "tableContratosAnuais")
    time.sleep(1)

    # comprasnet_contratos_anual_prepostos_latest

    c.execute('''
        SELECT * FROM comprasnet_contratos_anual_prepostos_latest
    ''')

    df = pd.DataFrame(c.fetchall(), columns=["id","contrato_id","doc_formalizacao","informacao_complementar","data_inicio","data_fim","situacao"])
    df = df.astype(str)
    
    time.sleep(1)
    upload_file(df, client, "datasetComprasNet", "tableContratosAnuaisPrepostos")
    time.sleep(1)

    # comprasnet_contratos_anual_responsaveis_latest

    c.execute('''
        SELECT * FROM comprasnet_contratos_anual_responsaveis_latest
    ''')

    df = pd.DataFrame(c.fetchall(), columns=["id","contrato_id","funcao_id","instalacao_id","portaria","situacao","data_inicio","data_fim"])
    df = df.astype(str)
    
    time.sleep(1)
    upload_file(df, client, "datasetComprasNet", "tableContratosAnuaisResponsaveis")
    time.sleep(1)

    # comprasnet_contratos_anual_terceirizados_latest

    c.execute('''
        SELECT * FROM comprasnet_contratos_anual_terceirizados_latest
    ''')

    df = pd.DataFrame(c.fetchall(), columns=["id","contrato_id","funcao_id","descricao_complementar","jornada","unidade","custo","escolaridade_id","data_inicio","data_fim","situacao","aux_transporte","vale_alimentacao"])
    df = df.astype(str)
    
    time.sleep(1)
    upload_file(df, client, "datasetComprasNet", "tableContratosAnuaisTerceirizados")
    time.sleep(1)

# ------------------------------------------------------
