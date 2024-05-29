import boto3 
import time
import pandas as pd

def execute_query(select):
    athena_client = boto3.client('athena', region_name='us-east-1')  

    # Define os parâmetros para a consulta
    database = 'covid_database_glue'
    output_location = 's3://covidforecaststack-athenaqueryresultsbucketae74152-4y25qi9nzh1i'  # Substitua pelo seu local de saída do S3

    response = athena_client.start_query_execution(
        QueryString=select,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': output_location
        }
    )
    
    query_execution_id = response['QueryExecutionId']
    query_state = 'RUNNING'
    while query_state in ['RUNNING', 'QUEUED']:
        response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        query_state = response['QueryExecution']['Status']['State']
        time.sleep(0.05)
    
    # Inicializa a lista para armazenar todas as linhas de resultados
    all_rows = []

    # Busca os resultados iniciais
    result_response = athena_client.get_query_results(QueryExecutionId=query_execution_id)
    all_rows.extend(result_response['ResultSet']['Rows'])

    # Verifica se há mais páginas de resultados
    next_token = result_response.get('NextToken')
    while next_token:
        result_response = athena_client.get_query_results(
            QueryExecutionId=query_execution_id,
            NextToken=next_token
        )
        all_rows.extend(result_response['ResultSet']['Rows'])
        next_token = result_response.get('NextToken')
    
    print(len(all_rows))
    
    # Processa os resultados para um DataFrame
    headers = [col['VarCharValue'] for col in all_rows[0]['Data']]
    data = [[col.get('VarCharValue', '') for col in row['Data']] for row in all_rows[1:]]
    return pd.DataFrame(data, columns=headers)


# df_drs = execute_query('SELECT * FROM "covid_database_glue"."casos_obitos_drs"')

# df_drs.to_parquet('dados_drs.parquet')

df_municipio = execute_query('SELECT * FROM "covid_database_glue"."casos_obitos_municipio"')
df_municipio.to_parquet('dados_municipio.parquet')