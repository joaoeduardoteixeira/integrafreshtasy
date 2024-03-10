#importa a biblioteca de conexão
import cx_Oracle

#importa o biblioteca que lida com os dataframes
import pandas as pd
from pandas import json_normalize
pd.set_option('display.expand_frame_repr', False)

#importa a biblioteca de requisições API
import requests

#importa a biblioteca que trata expressões regulares
import re

#Ilimita o tamanho das colunas
pd.set_option('max_colwidth', 1000)
#valor original é 61

#biblioteca com funções para tratar json
import json

#biblioteca de data e hora
from datetime import date
data_atual = date.today()

# Variável do Conector
connection = cx_Oracle.connect(user="usuario",
                               password="senha",
                               dsn="host/service_name")


#Variavel de execução
cursor = connection.cursor()

#Variável que guarda a consulta
query = """
SELECT 
    A.nr_seq_wheb,
    A.nr_sequencia,
    b.nr_sequencia as nr_seq_historico,
    A.nm_usuario,
    to_char(cast(obter_somente_numero(A.CD_PROJETO) as int)) as projeto,
    B.dt_historico
FROM
    man_ordem_servico A,
    man_ordem_serv_tecnico B
WHERE 
    1=1
    AND A.nr_sequencia=B.nr_seq_ordem_serv
    and b.nr_sequencia not in (select nr_seq_historico from AFP_VALIDA_IDS_OS)
    --AND A.cd_pessoa_solicitante=1021922
    --AND to_char(cast(obter_somente_numero(A.CD_PROJETO) as int)) not in ('202') 
    --AND A.NR_SEQUENCIA IN (39432,39487)
    AND A.NR_SEQUENCIA IN (42035)
    --AND B.NR_SEQUENCIA IN (99593,99585,99584)
    AND to_char (DT_HISTORICO, 'dd/mm/yyyy') > '21/02/2024'
    --and b.nr_sequencia = 99575
order by projeto asc
"""
#202
#Variável que conecta, lê um SQL e cria um data frame chamado DF
df = pd.read_sql(query, connection)

#Renomeia as colunas do objeto DF
df.columns = ['num_os_philips', 'num_os_interno','nr_seq_historico','usuario','projeto','data_historico']
    #AND A.NR_SEQUENCIA IN (39432,39487)
    #AND A.NR_SEQUENCIA IN (34869)



df



# Valida se há dados no DF. Se houver, segue. Caso contrário, pare.

if not df.empty:
    # Se o DataFrame não estiver vazio, continue com o código
    # Coloque o restante do seu código aqui
    print("O DataFrame não está vazio. Continuando com a execução.")
else:
    # Se o DataFrame estiver vazio, imprima uma mensagem e interrompa a execução
    print()



#DROPA OS AUSENTES DA COLUNA PROJETO
df.dropna(subset=['projeto'], inplace=True)
#df.dropna(subset=['projeto'], inplace=True)
df


type(df)



# IMPRIME A COLUNA 'PROJETO' PARA VALIDAR O TRATAMENTO DOS AUSENTES. 
# DESNECESSARIO PARA FINS DE APLICACAO
#df.dtypes
#df.index #índices das colunas
df['projeto']



#CONFERE O SHAPE DO DATAFRAME
#print (df)
df.shape


import requests

# Lista vazia que receberá os JSON'S de retorno da API
osIntegrada = []
osNaointegrada = []

# Parâmetros da API
authV = 'SH8azt8PD4tGhWt4B7sJ', 'x'
headersV = {'content-type': 'application/json'}
jsonV = {'body': 'NOTA_INSERIDA_COM_SUCESSO_VIA_API'}

# Loop dentro do df, criando o link para consumo da API
for i in df['projeto']:
    try:
        urlV = "https://afpergs.freshservice.com/api/v2/tickets/" + i + "/notes"  # (ENDPOINT DE POST)   
        res = requests.post(urlV, auth=authV, headers=headersV, json=jsonV)  # REQUEST DE POST
        # urlV = "https://afpergs.freshservice.com/api/v2/tickets/"+i+"" #(ENDPOINT DE GET)
        # res = requests.get(urlV, auth=authV, headers=headers) #REQUEST DE GET
        if res.status_code in range(200, 299):
            osIntegrada.append(res.json())
        elif res.status_code in range(400, 504):
            osNaointegrada.append(res.json())
        else:
            print('Erro não tratado')
        # sai do primeiro loop
        print(urlV)
    except UnboundLocalError:
        print("A variável urlV não recebeu um valor adequado. Verifique a variável i ou a estrutura do seu código.")
        break







#CONVERTE O CONTEÚDO DA LISTA PREVIAMENTE CRIADA EM UM DATAFRAME, COM A FUNÇÃO JSON_NORMALIZE
from pandas import json_normalize
dfOsintegrada = json_normalize(osIntegrada)
#print(dfOsintegrada)

#RENOMEIA AS COLUNAS DO DATAFRAME USANDO COMO REFERÊNCIA A CHAVE (CHAVE-VALOR) DA LISTA osIntegrada
dfOsintegrada.columns = ['id', 'user_id', 'to_emails', 'body', 'body_text', 'ticket_id', 'created_at', 'updated_at',
                         'incoming', 'private', 'support_email', 'attachments']

#CONVERTE O TIPO DE DADO DO CAMPO "PROJETO" para inteiro. É necessário para efetuar o join com
#o campo ticket_id da tabela dfOsintegrada
df['projeto']=df['projeto'].astype(int)

#CRIA A TABELA NOVA, LINKANDO df com dfOsintegrada
dfJoinbdjs = pd.merge(df, dfOsintegrada, left_on=['projeto'], right_on=['ticket_id'])

#EXCLUI LINHAS DUPLICADAS, POIS O LOOP DO BLOCO ACIMA PERCORRE CADA ITEM DO DF(LINHAS E COLUNAS), GERANDO UMA MATRIZ.
#EX: CASO HAJA UM DF DE 6 COLUNAS E 6 LINHAS, VAI SER GERADO UM DATAFRAME 36 LINHAS, SENDO 30 REPETIDAS. 

dfJoinbdjs.drop_duplicates(subset=['nr_seq_historico'], keep='first', inplace=True)

#REORGANIZA OS ÍNDICES
dfJoinbdjs = dfJoinbdjs.reset_index(drop=True)

#Cria um DF limpo e filtrado para poder iterar
dfFiltrado=dfJoinbdjs[['nr_seq_historico','updated_at', 'created_at','usuario']]

dfFiltrado





dfOsintegrada



for index, row in dfFiltrado.iterrows():
    sql ='insert into AFP_VALIDA_IDS_OS (NR_SEQ_HISTORICO, DT_HISTORICO, NM_USUARIO, DT_ATUALIZACAO) values (:s, :s, :s, :s)'
    values = (row['nr_seq_historico'], row['created_at'], row['usuario'], data_atual )
    cursor.execute(sql, values)
connection.commit()
