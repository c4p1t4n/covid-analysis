import streamlit as st
import pandas as pd
import numpy as np
import plost
import folium
from streamlit_folium import st_folium
import boto3 
import time
import pandas as pd


def tratar_visao_drs(df):
    df['soma_casos_novos'] = df.soma_casos_novos.astype('int')
    df['soma_casos'] = df.soma_casos.astype('int')
    df['soma_obitos'] = df.soma_obitos.astype('int')
    df['soma_obitos_novos'] = df.soma_obitos_novos.astype('int')
    df['data'] = pd.to_datetime(df.dia)
    df['nome_drs'] = df.nome_drs.astype('string')
    return df


def visao_drs():
    # Carregar o DataFrame a partir do arquivo Parquet
    df = pd.read_parquet('dados_drs.parquet')
    
    # Tratar o DataFrame (a função 'tratar_visao_drs' deve estar definida em algum lugar)
    df = tratar_visao_drs(df)
    
    # Obter as opções únicas de DRS e adicionar "Geral"
    ra_options = df['nome_drs'].unique().tolist()
    ra_options.insert(0,"Geral")
    selected_ra = st.selectbox('Selecione o Departamento regional de Saúde (DRS)', ra_options)
    
    # Filtrar o DataFrame de acordo com a seleção
    if selected_ra == "Geral":
        filtered_df =  df.groupby('data')[['soma_casos', 'soma_casos_novos','soma_obitos','soma_obitos_novos']].sum().reset_index()
        st.write('Dados para todos os DRS')
    else:
        filtered_df = df[df['nome_drs'] == selected_ra]
        st.write(f'Dados para o DRS: {selected_ra}')
    
    # Exibir o gráfico de linha
    st.subheader("Casos e obitos ao longo do tempo")
    col1, col2 = st.columns(2)
    st.line_chart(filtered_df, x="data", y=("soma_casos","soma_obitos"))
    st.line_chart(filtered_df, x="data", y="soma_obitos")
    st.subheader("novos casos e obitos ao longo do tempo")
    st.line_chart(filtered_df, x="data", y="soma_casos_novos")
    st.line_chart(filtered_df, x="data", y="soma_obitos_novos")
    # st.dataframe(filtered_df)
    # st.write('Casos e Óbitos')
    # st.line_chart(filtered_df, x="mes", y="soma_casos")
    # st.line_chart(filtered_df, x="mes", y="soma_obitos_novos")

def tratar_visao_municipio(df):
    df['soma_casos_novos'] = df.soma_casos_novos.astype('int')
    df['soma_casos'] = df.soma_casos.astype('int')
    df['soma_obitos'] = df.soma_obitos.astype('int')
    df['soma_obitos_novos'] = df.soma_obitos_novos.astype('int')
    df['data'] = pd.to_datetime(df.dia)
    df['municipio'] = df.municipio.astype('string')
    df['latitude'] = df.latitude.astype('double')
    df['longitude'] = df.longitude.astype('double')
    return df


def mapa():
    st.title("Mapa de Casos Novos")
    df = pd.read_parquet('dados_municipio.parquet')
    ra_options = df['municipio'].unique().tolist()
    selected_ra = st.selectbox('Municipio', ra_options)
    filtered_df = df[df['municipio'] == selected_ra]
    df = tratar_visao_municipio(df)
    mapa = folium.Map(location=[filtered_df['latitude'].iloc[0], filtered_df['longitude'].iloc[0]], zoom_start=10)
    
# Adicionando marcadores para cada ponto no mapa

    # Exibindo o mapa no Streamlit
    st_folium(mapa)
    print("aqui")

st.title('Painel de Analytics de Saúde')
tab1, tab2 = st.tabs(["Visão por DRS", "Visão Por municipio"])
with tab1:
    visao_drs()
with tab2:
    mapa()
# Casos e Óbitos Novos
# st.write('Casos e Óbitos Novos')
# st.line_chart(filtered_df[['casos_novos','datahora']])

# # Mapas
# st.subheader('Mapa de Casos')
# # st.map(filtered_df[['latitude', 'longitude']])

# # Estatísticas
# st.subheader('Estatísticas')
# st.write('Estatísticas Descritivas dos Dados Selecionados')
# st.write(filtered_df.describe())

# # Filtros adicionais
# st.subheader('Filtros Adicionais')
# pop_min = st.slider('População Mínima', min_value=int(df['pop'].min()), max_value=int(df['pop'].max()), value=int(df['pop'].min()))
# pop_max = st.slider('População Máxima', min_value=int(df['pop'].min()), max_value=int(df['pop'].max()), value=int(df['pop'].max()))

# filtered_df = filtered_df[(filtered_df['pop'] >= pop_min) & (filtered_df['pop'] <= pop_max)]
# st.write(f'Dados filtrados por população entre {pop_min} e {pop_max}')
# st.dataframe(filtered_df)
