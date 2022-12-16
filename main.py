
# import Libs
import os
import pandas as pd
from DAP_Funcoes import *
import numpy as np
from bcb import sgs
from datetime import datetime, timedelta
import math
import time
import warnings
warnings.filterwarnings('ignore')



# Restrições:
considerar_pmp_menor = False #Se verdadeiro, o código irá considderar os pmp's menores que 3 anos (em torno de 10h de processamento)
produtos = ["DAP"] #Código
patrimonio_inicial = 10000000.00 #Reais
dia_comeco = "02/01/2018"
dia_fim = "17/01/2020"
tolerancia_pmp = 10 #porcentagem
pmp = 3 #Anos
multiplo_compra = 5
Variacao_IPCA_DAP = 1
ano_util = 252 #dias
taxa_corretagem = 0 #porcentagem

dia_comeco_date = datetime. strptime(dia_comeco, '%d/%m/%Y')
dia_fim_date = datetime. strptime(dia_fim, '%d/%m/%Y')
pmp_max = pmp*365*(1+tolerancia_pmp/100)
pmp_min = pmp*365*(1-tolerancia_pmp/100)


    #caixa zero
    #Apenas compra PU e compra apenas no primeiro dia: minha interpretação foi de que apenas haverá uma compra de titulos durante
    # o periodo do fundo (no primeiro dia) e nenhuma venda, ou seja, consideraremos apenas o valor do título no vencimento, pois não
    # haverá a sua venda.
    #sem slippage



# Importacao da base:

scriptpath = os.getcwd() + "\\"
DAP_file_name = "historico_dap.xlsx"
DAP_import = import_excel(scriptpath + DAP_file_name)


#Extração IPCA pela API do BCB:

#ipca_mensal = sgs.get({'ipca': 433},start = dia_comeco_date)

    # Como teremos títulos vencendo após o fim do período do fundo, considerei que poderia ser válido calcular o retorno com a inflação IPCA,
    # pois pode haver uma variação de anos entre os vencimentos dos títulos, causando uma diferença não desprezível.
    # Porém a simplificação da DAP mitiga essa diferença, visto que para calcular a correção monetária, seria preciso dividir o PU
    # pela variação do IPCA, nos resultando a mesma fórmula da simplificação. Para fins de comparação de retornos e não o valor
    # final obtido, a simplificação da DAP é um modelo válido sem haver divergências devido ao IPCA.


# obtenção dos títulos possíveis de compra:

    # pela simplificação, haverá compras apenas no dia de abertura do fundo e nenhuma outra transação posterior, portanto
    # os únicos títulos válidos serão os disponíveis na data de abertura do fundo.
    # Além disso, pela simplificação, é possível dizer que o PU no vencimento será de exatamente R$100.000,00, pois (1+ taxa)^0 = 1,
    # então não será necessário obter nenhum outro dado fora os títulos disponíveis para compra no dia de abertura, seus respectivos PUs neste dia
    # e sua data de vencimento.

DAP_titulos_possiveis = DAP_import[(DAP_import['data_ref'] == dia_comeco_date) & (DAP_import['produto'].isin(produtos))][{'codigo','data_venc','pu'}]


# a) obtendo o retorno de cada vencimento

    # como o PU no vencimento será de R$100.000,00, então o retorno será de  R$100.000,00/(valor de compra) -1.

DAP_titulos_possiveis['retorno_venc'] = (100000/DAP_titulos_possiveis['pu']*(1+taxa_corretagem) - 1)*100

print(DAP_titulos_possiveis)
DAP_titulos_possiveis.to_csv('contratos_validos.csv',sep=';',decimal=',')
# b) rankeando composições

    # como podemos ter um número muito grande de composições possíveis, gerar todas as combinações poderia custar muito computacionalmente.
    # utilizar alguma otimização por métodos tardicionais de ML poderia ser um problema, pois como é necessário rankear as composições,
    # alguns máximos locais poderiam ser omitidos.
    # portanto é preciso encontrar um método que reduza o custo computacional mas ainda consiga encontrar todos os resultados relevantes,
    # como por exemplo, somente as composições que chegam o mais próximo possível do patrimônio e para as restrições de PMP.

    # a lógica do algoritmo abaixo consiste em primeiro separar todas as formas de compra possíveis para cada título, ou seja, se o título k35 valesse
    # 5 milhoes, então haveria 2 formas de comprar ele: comprar apenas 1 ou comprar 2 unidades, então teriam 2 registros. Se o título k36 valesse 2 milhoes,
    # enttão haveria 5 formas de comprar: comprar  1, 2, 3, 4 ou 5 unidades, então teriam 5 registros, e assim por diante. Caso seu PMP passe do limite,
    # seu registro será desconsiderado.
    # após separar cada forma possível de comprar cada título, eles serão ordenados por valor e permutados entre si de uma forma inteligente,
    # ignorando casos em que o valor passe o patrimônio. Dentre esses, finalmente, haverá uma validação de PMP final para verificar se a composição está dentro
    # da restrição estabelecida.

    # desconsiderando a inflação comforme a simplificação da DAP, todos os títulos com data de vencimento menor que a data de fechamento do fundo terão o mesmo
    # pmp de zero, já que não há movimentação de caixa do fundo. portanto, podemos dizer que eles são análogos entre si, e apenas o com o melhor retorno será relevante.


# Primeira parte: separando formas de compra
# Excluindo títulos análogos:
DAP_titulos_possiveis_analogos = DAP_titulos_possiveis[(DAP_titulos_possiveis['data_venc'] <= dia_fim_date)]
DAP_titulos_possiveis_analogos = DAP_titulos_possiveis_analogos[(DAP_titulos_possiveis_analogos['retorno_venc'] != max(DAP_titulos_possiveis_analogos['retorno_venc']))]
DAP_titulos_possiveis = DAP_titulos_possiveis[(~DAP_titulos_possiveis['codigo'].isin(DAP_titulos_possiveis_analogos['codigo']))]

# valor minimo compra (multiplo de 5):

DAP_titulos_possiveis['compra minima'] = DAP_titulos_possiveis['pu']*5*(1+taxa_corretagem)


# máximos possiveis
    # máximo por pmp
    # o pmp do título sozinho não pode passar o pmp máximo.
    # menor data de vencimento:
DAP_titulos_possiveis['deltadata'] = (DAP_titulos_possiveis['data_venc']-dia_fim_date)/timedelta(1)
DAP_titulos_possiveis.loc[DAP_titulos_possiveis['deltadata'] <= 0, 'deltadata'] = 0.1
DAP_titulos_possiveis['Maximo pmp'] = pmp_max*patrimonio_inicial/DAP_titulos_possiveis['deltadata']
DAP_titulos_possiveis['Qtde Maxima pmp'] = (DAP_titulos_possiveis['Maximo pmp']/DAP_titulos_possiveis['compra minima']).apply(np.floor)
    # maximo por patrimonio
DAP_titulos_possiveis['Qtde Maxima patrimonio'] = (patrimonio_inicial/DAP_titulos_possiveis['compra minima']).apply(np.floor)

#Máximo possivel geral
DAP_titulos_possiveis['Qtde Maxima'] = DAP_titulos_possiveis[['Qtde Maxima pmp','Qtde Maxima patrimonio']].min(axis=1)
DAP_titulos_possiveis = DAP_titulos_possiveis.drop(['deltadata','Maximo pmp','Qtde Maxima pmp','Qtde Maxima patrimonio' ], axis = 1)

print(DAP_titulos_possiveis)

# criando dataframe com todas as compras individuais possiveis:

max_compra = max(DAP_titulos_possiveis['Qtde Maxima'])
DAP_titulos_compras_individuais = DAP_titulos_possiveis[['codigo','data_venc','retorno_venc','pu','Qtde Maxima']]
DAP_titulos_compras_individuais['Qtde Compra'] = 5

# loopa por todas as possiveis compras, comprando 5 unidades, 10, 15, etc até chegar na quantidade maxima de compras possível e concatena num só dataframe.
for i in range(2,int(max_compra)+1):
    DAP_tci_aux = DAP_titulos_possiveis[DAP_titulos_possiveis['Qtde Maxima'] >=i][{'codigo','data_venc','retorno_venc','pu','Qtde Maxima'}]
    DAP_tci_aux['Qtde Compra'] = 5*i
    DAP_titulos_compras_individuais = pd.concat([DAP_titulos_compras_individuais,DAP_tci_aux])

# cria campos importantes para a permutação
DAP_titulos_compras_individuais['Valor Compra'] = DAP_titulos_compras_individuais['Qtde Compra']*DAP_titulos_compras_individuais['pu']
DAP_titulos_compras_individuais = DAP_titulos_compras_individuais.sort_values(by=['Valor Compra'],ascending=False)
DAP_titulos_compras_individuais = DAP_titulos_compras_individuais.reset_index(level=0, drop=True).reset_index()
DAP_titulos_compras_individuais['pmp parcial'] = (DAP_titulos_compras_individuais['data_venc'] - dia_fim_date)/timedelta(1)*DAP_titulos_compras_individuais['Valor Compra']/patrimonio_inicial
DAP_titulos_compras_individuais.loc[DAP_titulos_compras_individuais['pmp parcial'] < 0, 'pmp parcial'] = 0

#ordena os registros por valor decrescente e deixa os casos com pmp maior que o limite em cima:
DAP_tci_pmp = DAP_titulos_compras_individuais[((DAP_titulos_compras_individuais['data_venc']-dia_fim_date)/timedelta(1))>=pmp_min].sort_values(by='Valor Compra',ascending=False)
DAP_tci_resto = DAP_titulos_compras_individuais[((DAP_titulos_compras_individuais['data_venc']-dia_fim_date)/timedelta(1))<pmp_min].sort_values(by='Valor Compra',ascending=False)
DAP_titulos_compras_individuais = pd.concat([DAP_tci_pmp,DAP_tci_resto])

DAP_titulos_compras_individuais['composicao individual'] = DAP_titulos_compras_individuais['Qtde Compra'].astype(str) + DAP_titulos_compras_individuais['codigo']
DAP_titulos_compras_individuais.to_csv('formas_possiveis_individuais.csv',sep=';',decimal=',')
print(DAP_titulos_compras_individuais)

# Segunda Parte: Permutando entre combinações possíveis

# alocando Dataframes
DAP_composicoes = pd.DataFrame(columns = ['composicao','pmp_parcial','valor_total','retorno_total'])
DAP_composicao_analisada = composicao()
listacodigo = []

DAP_titulos_compras_individuais = DAP_titulos_compras_individuais[ ['codigo', 'data_venc', 'retorno_venc', 'Qtde Compra', 'Valor Compra', 'pmp parcial','composicao individual']]
DAP_titulos_compras_individuais = DAP_titulos_compras_individuais.rename(columns={ 'retorno_venc': 'retorno', 'Qtde Compra': 'qtde_compra', 'Valor Compra':'vlr_compra' , 'pmp parcial': 'pmp_parcial','composicao individual':'composicao_individual'})
base_iteraDAP_titulos_compras_individuaiscao = DAP_titulos_compras_individuais.reset_index(level=0, drop=True)


start_time = time.time()

#função de permutação
DAP_composicoes = iterador(DAP_titulos_compras_individuais,DAP_composicoes,composicao(),listacodigo,dia_fim_date,patrimonio_inicial,pmp_max,pmp_min,start_time,considerar_pmp_menor)





DAP_composicoes['retorno_total'] = DAP_composicoes['retorno_total']*DAP_composicoes['valor_total']/patrimonio_inicial
DAP_composicoes = DAP_composicoes.sort_values(by=['retorno_total'],ascending=False)

DAP_composicoes['composicao'] = DAP_composicoes['composicao'].str[:-1]
DAP_composicoes['pmp'] = DAP_composicoes['pmp_parcial']/365

DAP_composicoes.to_csv('DAP_composicoes.csv',sep=';',decimal=',')

