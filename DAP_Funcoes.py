import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time


def import_excel(filepath):
    file_extension = filepath.split(".")[-1]
    #verifica se é arquivo excel
    if "xls" not in file_extension:
        raise Exception('Arquivo com extensão desconhecida.')
    df_import_raw = pd.read_excel(filepath)
    #tratamento inicial dos dados, limpar dados inválidos
    df_import_clean = check_DAP_columns_df(df_import_raw)
    return df_import_clean



def check_DAP_columns_df(df):
    #checa se todas as colunas necessárias estão presentes
    column_layout = ['data_ref', 'produto',
                     'codigo', 'data_venc',
                     'pu', 'taxa_ajuste',
                     'taxa_ab', 'taxa_min',
                     'taxa_max', 'taxa_med',
                     'taxa_ult', 'taxa_ult_cp',
                     'taxa_ult_vd', 'prazo (dias uteis)'
                     ]
    column_names = df.columns.values.tolist()
    if column_names != column_layout:
        raise Exception('Erro de validação das colunas do arquivo importado.')
    else:
        #validação de cada tipo de dado
        df['data_ref'].update(df['data_ref'].apply(check_DAP_dates_df).dropna())
        df['codigo'].update(df['codigo'].apply(check_DAP_name_df).dropna())
        df['data_venc'].update(df['data_venc'].apply(check_DAP_dates_df).dropna())
        df['pu'].update(df['pu'].apply(check_DAP_number_df).dropna())
        df['taxa_ajuste'].update(df['taxa_ajuste'].apply(check_DAP_number_df).dropna())
        df['taxa_ab'].update(df['taxa_ab'].apply(check_DAP_number_df).dropna())
        df['taxa_min'].update(df['taxa_min'].apply(check_DAP_number_df).dropna())
        df['taxa_max'].update(df['taxa_max'].apply(check_DAP_number_df).dropna())
        df['taxa_med'].update(df['taxa_med'].apply(check_DAP_number_df).dropna())
        df['taxa_ult'].update(df['taxa_ult'].apply(check_DAP_number_df).dropna())
        df['taxa_ult_cp'].update(df['taxa_ult_cp'].apply(check_DAP_number_df).dropna())
        df['taxa_ult_vd'].update(df['taxa_ult_vd'].apply(check_DAP_number_df).dropna())
        df['prazo (dias uteis)'].update(df['prazo (dias uteis)'].apply(check_DAP_int_df).dropna())
        return df



def check_DAP_dates_df(value):
    #validação de datas, limpa qualquer valor que não seja data ou uma data não coerente
    try:
        float(value)
        if value < datetime.today() - timedelta(days = 100 * 365) or value > datetime.today() + timedelta(days = 100 * 365):
            return np.NaN
        else:
            return value
    except:
        return np.NaN


def check_DAP_number_df(value):
    #limpa valores invalidos em numero
    try:
        float(value)
        return value
    except:
        return np.NaN

def check_DAP_name_df(value):
    #checa se o contrato é um código válido
    if len(value) > 3:
        return np.NaN
    else:
        try:
            int(value[-3:])
            return value
        except:
            return np.NaN

def check_DAP_int_df(value):
    #checa valores que precisem ser inteiros
    try:
        int(value)
        return value
    except:
        return np.NaN



#criação da classe composição para aliviar o tempo de processamento, pois os DFs do pandas custam muito tempo em loops compridos.
class composicao:
    composicao = ""
    pmp_parcial = 0
    valor_total = 0
    retorno_total = 0

def class_to_df(composicao_atual):
    #funcao de tranformaçao de objeto classe para DF
    comp_df = pd.DataFrame({'composicao':[composicao_atual.composicao],'pmp_parcial':[composicao_atual.pmp_parcial],'valor_total':[composicao_atual.valor_total],'retorno_total':[composicao_atual.retorno_total]})
    return comp_df

def iter_resto(pares_validos,comp_atual):
    #iteração em caso especial: caso todos os contratos válidos para a combinação especificada possam ser adicionados de uma vez, esta função os adiciona.
    full_df = pd.DataFrame
    pares_validos = pares_validos.drop_duplicates(subset='codigo',keep='first')
    for index, row in pares_validos.iterrows():
        comp_atual.composicao = comp_atual.composicao + row['composicao_individual'] + ','
        comp_atual.pmp_parcial = comp_atual.pmp_parcial + row['pmp_parcial']
        comp_atual.retorno_total = (comp_atual.retorno_total*comp_atual.valor_total+row['retorno']*row['vlr_compra'])/(comp_atual.valor_total+row['vlr_compra'])
        comp_atual.valor_total = comp_atual.valor_total + row['vlr_compra']
        full_df = pd.concat([full_df,class_to_df(comp_atual)])

def iterador(base_it,output,comp_atual,listacodigo,dia_fim,pat,pmp_max,pmp_min,start_time,flag_pmp):
    comp_atual1 = composicao()
    j = 0
    base_it = base_it[['codigo','data_venc','retorno','pmp_parcial','qtde_compra','vlr_compra','composicao_individual']].reset_index(level=0,drop=True)
    for i in range(0,base_it.shape[0]):
        listacodigo1 = list(listacodigo)
        listacodigo1.append(base_it['codigo'].loc[[i]].values[0])
        valor_compra_maximo = pat - comp_atual.valor_total - base_it['vlr_compra'].loc[[i]].values[0]
        pmp_atual = comp_atual.pmp_parcial + base_it['pmp_parcial'].loc[[i]].values[0]
        flag_clean = False
        if len(base_it.drop_duplicates(subset='codigo',keep='first')['codigo'].tolist()) == 1:
            base_it = base_it.drop_duplicates(subset='codigo',keep='first')
            flag_clean = True
        if valor_compra_maximo >=0 and pmp_atual <= pmp_max:
            if flag_pmp is False:
                if ((base_it['data_venc'].loc[[i]]-dia_fim)/timedelta(1)).values[0]<pmp_min and len(listacodigo1) == 1:
                    return output
            pares_validos = base_it[(~base_it['codigo'].isin(listacodigo1)) & (base_it['vlr_compra'] <= valor_compra_maximo) & (pmp_atual + base_it['pmp_parcial'] <= pmp_max)].reset_index(level=0,drop=True)

            comp_atual1.composicao = comp_atual.composicao + base_it['composicao_individual'].loc[[i]].values[0] + ','
            comp_atual1.pmp_parcial = comp_atual.pmp_parcial + base_it['pmp_parcial'].loc[[i]].values[0]
            comp_atual1.retorno_total = (comp_atual.retorno_total * comp_atual.valor_total + base_it['retorno'].loc[[i]].values[0] * base_it['vlr_compra'].loc[[i]].values[0]) / (comp_atual.valor_total + base_it['vlr_compra'].loc[[i]].values[0])
            comp_atual1.valor_total = comp_atual.valor_total + base_it['vlr_compra'].loc[[i]].values[0]
            if pares_validos.empty is True:
                if flag_pmp is False:
                    if comp_atual1.pmp_parcial >= pmp_min:
                        if comp_atual1.valor_total >= 0.1*pat:
                             output = pd.concat([output, class_to_df(comp_atual1)])
                else:
                    if comp_atual1.valor_total >= 0.1 * pat:
                        output = pd.concat([output, class_to_df(comp_atual1)])
            else:
                if valor_compra_maximo - sum(pares_validos['vlr_compra'])>= 0.1*pat or (comp_atual1.pmp_parcial + sum(pares_validos['pmp_parcial'])<pmp_min and flag_pmp is False):
                    return output
                elif flag_pmp is False and sum(pares_validos['vlr_compra']) <= valor_compra_maximo and comp_atual1.pmp_parcial + sum(pares_validos['pmp_parcial']) <= pmp_max and comp_atual1.pmp_parcial + sum(pares_validos['pmp_parcial']) >= pmp_min:
                    output = pd.concat([output, class_to_df(comp_atual1)])
                    if base_it['composicao_individual'].loc[[i+1]].values[0] in pares_validos['composicao_individual'].tolist():
                        return output
                elif flag_pmp is True and sum(pares_validos['vlr_compra']) <= valor_compra_maximo and comp_atual1.pmp_parcial + sum(pares_validos['pmp_parcial']) <= pmp_max:
                    output = pd.concat([output, class_to_df(comp_atual1)])
                    if base_it['composicao_individual'].loc[[i+1]].values[0] in pares_validos['composicao_individual'].tolist():
                        return output
                else:
                    output = iterador(pares_validos,output,comp_atual1,listacodigo1,dia_fim,pat,pmp_max,pmp_min,start_time,flag_pmp)
        #print de tempo e de status
        if len(listacodigo1) == 1:
            time_now = time.time()
            if i ==0:
                end_time = time_now - start_time
            end_time = time_now - end_time
            output.to_csv('composicao.csv',sep=';',decimal=',')
            j = j+1
            print(base_it['composicao_individual'].loc[[i]].values[0] + ' ' + str(j) + ' ' + str(round(time_now - start_time)))
        if flag_clean is True:
            return output
        base_it = base_it.drop(labels=i)
    return output

# a funcao "iterador" acabou sendo mais complexa do que o esperado. como julguei válido testar casos com pmp menor que 3 anos, implementei de forma que pudesse
# ser feito das duas formas. porém o tempo de processamento da permutação considerando eles acaba sendo não factível para o escopo do desafio.
# a funçao faz uma permutaçao de forma bruta de todas as composicoes válidas, porém com alguns atalhos inteligentes para reduzir o custo computacional.
# explicar em detalhe  cada linha poderia acabar sendo contra-produtivo sem explicar a lógica por trás da funcao, por isso me abstive de comentar as linhas.

# Segue abaixo a explicaçao da logica:

# primeiramente, conforme explicado em main, o algoritmo separa anteriormente a base em todas as formas possíveis de se comprar um dado contrato.
# essas formas então são permutadas entre si até se chegar no limite estabelecido de patrimônio e pmp. As vantagens em separar desta forma são que não haverá
# nenhum registro em duplicidade, ou seja, é possível remover o último valor permutado e diminuindo o tamanho da base a cada permutaçao, como também nos facilita
# na hora de filtrar todos os contratos que a combinação é válida. O loop da permutaçao é feito tanto com um for quanto com uma recursão, com o for servindo para
# iterar entre os próximos contratos válidos de combinação, e se houver mais de uma possibilidade o algoritmo irá bifurcar com uma recursão da função,e assim por diante
# até se chegar na condiçao em que nao há mais contratos validos a serem adicionados para aquela composiçao.

# as regras de filtro são de bater o valor do patrimonio e estar dentro das restricoes de pmp, como também não pode ser possível repetir o mesmo contrato (pois o contrato
# repetido já aparece em outro registro ( exemplo: temos um regsitro de 5K23 e um de 10K23, ou seja, os dois juntos seriam equivalentes a um registro 15K23, que já
# aparece na base. Além disso, a funcao possui alguns atalhos para reduzir tempo de processamento e calculos de composicoes piores em todos os sentidos a uma outra já calcualda,
# aglutinando todos os registros que são válidos (após filtrados) caso sua soma saia das restriçoes.

# se escolhermos apenas os casos com pmp próximo ao valor estipulado, o algoritmo separa apenas os casos em que seria possível cheagar em tal pmp, ou seja,
# o primeiro contrato escolhido sempre vai ser com pmp maior ou igual ao valor estipulado.
# a ordem dos registros também se tornou importante, visto que valores maiores escolhidos antes custam muito menos computacionalmente para o código, podendo entao
# limpar a base com uma eficiencia maior, e alem disso é pela ordem que é possivel filtrar apenas os casoscom pmp maior, de acordo com a estrutura do codigo.

# uma permutação de forca bruta com todos os casos possíveis chega na magnitude de 10^17 possibilidades, o que torna esta forma de encontrar as combinações não viável.
# com todos os atalhos é muito difícil estimar todas as possibilidades, que podem variar dependendo da entrada, porém há uma melhora significativa no tempo de processamento.


# uma forma encontrada de reduzir o tempo computacional do algoritmo seria migrar tudo que fosse dataframe do pandas para alguma forma mais eficiente de calcular
# as tabelas. porém isso alteraria toda a estrutura da função, o que acabaria fugindo do prazo estabelecido para a entrega.