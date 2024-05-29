import awswrangler as wr
import pandas as pd
import json

def read_data(event):
    body = json.loads(event['Records'][0]['body'])
    s3_notification = json.loads(body['Message'])
    print(s3_notification)
    bucket_name = s3_notification['Records'][0]['s3']['bucket']['name']
    object_key = s3_notification['Records'][0]['s3']['object']['key']
    complete_path = f's3://{bucket_name}/{object_key}'
    wr.s3.read_csv(path=complete_path,delimiter=';')

def read_df(path):
    return wr.s3.read_csv(path=path,delimiter=';')
def write_data(df,path):
    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,  # If you want to save as a dataset with partitions, you can specify partition_cols=['column_name']
        mode='overwrite'  # Options: 'append', 'overwrite', 'overwrite_partitions'
    )
    print("dados enviado para stage")

def write_data_processed(df,path):
    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,  # If you want to save as a dataset with partitions, you can specify partition_cols=['column_name']
        mode='overwrite_partitions', # Options: 'append', 'overwrite', 'overwrite_partitions'
        partition_cols=['nome_drs','nome_munic']
    )
    print("dados enviados para processed")

def treat_data(df):
    df['nome_munic'] = df.nome_munic.astype('string')
    df['codigo_ibge'] = df.codigo_ibge.astype('string')
    df['dia'] = df.dia.astype('int')
    df['mes'] = df.mes.astype('int')
    df['datahora'] = pd.to_datetime(df.datahora)
    df['semana_epidem'] = df.semana_epidem.astype('string')
    df['casos'] = df.casos.astype('int')
    df['casos_novos'] = df.casos_novos.astype('int')

    df['casos_pc'] = df.casos_pc.apply(lambda x: str(x).replace(',','.'))
    df['casos_pc'] = df.casos_pc.astype('double')

    df['casos_mm4w'] = df.casos_mm4w.apply(lambda x: str(x).replace(',','.'))
    df['casos_mm4w'] = df.casos_mm4w.astype('double')

    df['obitos']  = df.obitos.astype('int')

    df['obitos_pc'] = df.obitos_pc.apply(lambda x: str(x).replace(',','.'))
    df['obitos_pc'] = df.obitos_pc.astype('double')

    df['obitos_mm4w'] = df.obitos_mm4w.apply(lambda x: str(x).replace(',','.'))
    df['obitos_mm4w'] = df.obitos_mm4w.astype('double')

    df['letalidade'] = df.letalidade.apply(lambda x: str(x).replace(',','.'))
    df['letalidade'] = df.letalidade.astype('double')

    df['nome_ra'] = df.nome_ra.astype('string')
    df['cod_ra'] = df.cod_ra.astype('string')
    df['nome_drs'] = df.nome_drs.astype('string')
    df['cod_drs'] = df.cod_drs.astype('string')

    df['pop']  = df['pop'].astype('int')
    df['pop_60']  = df['pop_60'].astype('int')
    df['area']  = df.area.astype('int')
    df['map_leg'] = df.map_leg.astype('string')
    df['map_leg_s'] = df.map_leg_s.astype('int')
    df['latitude'] = df.latitude.apply(lambda x: str(x).replace(',','.'))
    df['latitude'] = df.latitude.astype('double')

    df['longitude'] = df.longitude.apply(lambda x: str(x).replace(',','.'))
    df['longitude'] = df.longitude.astype('double')
    return df

def lambda_handler(event,context):
    # df = read_df('s3://covid-forecast-raw-4288/covid_daily/dados_covid_tratado.csv')
    df = pd.read_csv('src/cloud_infrastructure/raw_to_stage/dados_covid_tratado.csv',sep=';')
    print(df.head())

    df = treat_data(df)
    write_data(df,'s3://covid-forecast-stage-4288/covid_daily/')
    write_data_processed(df,'s3://covid-forecast-processed-4288/covid_daily/')

lambda_handler('','')
