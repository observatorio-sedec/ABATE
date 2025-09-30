import datetime
import ssl
import openpyxl
import pandas as pd
import requests as rq


class TLSAdapter(rq.adapters.HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        ctx.set_ciphers("DEFAULT@SECLEVEL=1")
        ctx.options |= 0x4   # OP_LEGACY_SERVER_CONNECT
        kwargs["ssl_context"] = ctx
        return super(TLSAdapter, self).init_poolmanager(*args, **kwargs)

def requisitando_dados(api):
    with rq.session() as s:
        s.mount("https://", TLSAdapter())
        dados_brutos_api = s.get(api, verify=True)
    
    if dados_brutos_api.status_code != 200:
        raise Exception(f"A solicitação à API falhou com o código de status: {dados_brutos_api.status_code}")

    try:
        dados_brutos = dados_brutos_api.json()
    except Exception as e:
        raise Exception(f"Erro ao analisar a resposta JSON da API: {str(e)}")

    if len(dados_brutos) < 3:
            dados_brutos_151 = None
            dados_brutos_284 = None
            dados_brutos_285 = None
            return dados_brutos_151, dados_brutos_284, dados_brutos_285
    
    if dados_brutos_api.status_code == 500:
        raise Exception(f"Os dados passou de 100.0000 por isso o codigo de: {dados_brutos_api.status_code}")

    dados_brutos_151 = dados_brutos[0]
    dados_brutos_284 = dados_brutos[1]
    dados_brutos_285 = dados_brutos[2]

    return dados_brutos_151, dados_brutos_284, dados_brutos_285

def tratando_dados(dados_brutos_151, dados_brutos_284, dados_brutos_285):
    dados_limpos_151 = []
    dados_limpos_284 = []
    dados_limpos_285 = []
    referencia_temporal = None
    tipo_rebanho = None
    tipo_inspec = None

    variaveis = [dados_brutos_151, dados_brutos_284, dados_brutos_285]

    for i in variaveis:
        id_tabela = i['id']
        variavel = i['variavel']
        unidade = i['unidade']
        dados = i['resultados']

        for ii in dados:
            dados_produto = ii['classificacoes']
            dados_producao = ii['series']
            for iii in dados_produto:
                dados_id_produto = iii['categoria']
                if iii['nome'] == 'Referência temporal':
                    referencia_temporal = list(iii['categoria'].values())[0]
                elif iii['nome'] == 'Tipo de rebanho bovino':
                    tipo_rebanho = list(iii['categoria'].values())[0]
                elif iii['nome'] == 'Tipo de inspeção':
                    tipo_inspec = list(iii['categoria'].values())[0]
                    
                    for iv in dados_producao:
                        
                        id = iv['localidade']['id']
                        nome = iv['localidade']['nome'].replace(' (MT)', '')
                        dados_ano_producao = iv['serie'] 
                        
                        for ano, producao in dados_ano_producao.items():
                            partes = ano.split("/")
                            ano = int(partes[0][:4])
                            mes = (partes[0][5:6])
                            mes = int(mes)
                            
                            producao = producao.replace('-', '0').replace('...', '0')
                            
                            dict = {

                                'id': id,
                                'nome': nome,
                                'referencia': referencia_temporal,
                                'tipo_rebanho': tipo_rebanho,
                                'tipo_inspecao': tipo_inspec,
                                variavel: producao,
                                'ano': f"01/{mes * 3 if mes * 3 == 12 else f'0{mes * 3}'}/{ano}",
                                'trimestre': mes   
                            }
                           
                            if id_tabela == '151':
                                dados_limpos_151.append(dict)
                            elif id_tabela == '284':
                                dados_limpos_284.append(dict)
                            elif id_tabela == '285':
                                dados_limpos_285.append(dict)



    return dados_limpos_151, dados_limpos_284, dados_limpos_285

ano_atual = datetime.datetime.now().year
def executando_estadual(tabela, bovino=False):
    lista_dados_151 = [] 
    lista_dados_284 = []
    lista_dados_285 = []
    
    for ano in range(2014, int(ano_atual)+1):
        print(ano)
        trimestres = range(1, 5)
        for tri in trimestres:
            if bovino:
                api_estadual = f'https://servicodados.ibge.gov.br/api/v3/agregados/{tabela}/periodos/{ano}0{tri}/variaveis/151|284|285?localidades=N3[all]&classificacao=12716[all]|18[55,56,111734,111735,57]|12529[all]'
            else:
                api_estadual = f'https://servicodados.ibge.gov.br/api/v3/agregados/{tabela}/periodos/{ano}0{tri}/variaveis/151|284|285?localidades=N3[all]&classificacao=12716[all]|12529[all]'
            variavel151estadual, variavel_284_estadual, variavel_285_estadual = requisitando_dados(api_estadual)
            if variavel151estadual == None and variavel_284_estadual == None and variavel_285_estadual == None:
                break
            if len(variavel151estadual) == 0 and len(variavel_284_estadual) == 0 and len(variavel_285_estadual) == 0:
                lista_dados_151, lista_dados_284, lista_dados_285 = tratando_dados(variavel151estadual, variavel_284_estadual, variavel_285_estadual)
            else:
                novos_dados_151, novos_dados_284, novos_dados_285 = tratando_dados(variavel151estadual, variavel_284_estadual, variavel_285_estadual)
                lista_dados_151.extend(novos_dados_151)
                lista_dados_284.extend(novos_dados_284)
                lista_dados_285.extend(novos_dados_285)

    return lista_dados_151, lista_dados_284, lista_dados_285


def gerando_dataframe(dados_limpos_151, dados_limpos_284, dados_limpos_285, tipo = None):

    df151 = pd.DataFrame(dados_limpos_151)
    df284 = pd.DataFrame(dados_limpos_284)
    df285 = pd.DataFrame(dados_limpos_285)


    dataframe = pd.merge(df151, df284, on=['id', 'nome',  'referencia', 'ano', 'trimestre','tipo_rebanho', 'tipo_inspecao'], how='inner')
    dataframe = pd.merge(dataframe, df285, on=['id', 'nome', 'referencia', 'ano', 'trimestre','tipo_rebanho', 'tipo_inspecao'], how='inner')
    dataframe['Animais abatidos'] = dataframe['Animais abatidos'].replace("X", 0)
    dataframe['Animais abatidos'] = dataframe['Animais abatidos'].astype(int)
    dataframe['Peso total das carcaças'] = dataframe['Peso total das carcaças'].replace("X", 0)
    dataframe['Peso total das carcaças'] = dataframe['Peso total das carcaças'].astype(int)

    dataframe['Número de informantes'] = dataframe['Número de informantes'].astype(int)
    if tipo == 'Suino':
        dataframe['tipo_rebanho'] = dataframe['tipo_rebanho'].fillna('Suínos')
    elif tipo == 'Frangos':
        dataframe['tipo_rebanho'] = dataframe['tipo_rebanho'].fillna('Frangos')
        
    dataframe['ano'] = pd.to_datetime(dataframe['ano'], format='%d/%m/%Y')
    return dataframe

dados_limpos_151_estadual, dados_limpos_284_estadual, dados_limpos_285_estadual= executando_estadual(1092, True)
dataframe_1092 = gerando_dataframe(dados_limpos_151_estadual, dados_limpos_284_estadual, dados_limpos_285_estadual)
dados_limpos_151_estadual, dados_limpos_284_estadual, dados_limpos_285_estadual= executando_estadual(1093)
dataframe_1093 = gerando_dataframe(dados_limpos_151_estadual, dados_limpos_284_estadual, dados_limpos_285_estadual, 'Suino')
dados_limpos_151_estadual, dados_limpos_284_estadual, dados_limpos_285_estadual= executando_estadual(1094)
dataframe_1094 = gerando_dataframe(dados_limpos_151_estadual, dados_limpos_284_estadual, dados_limpos_285_estadual, 'Frangos')

df_total = pd.merge(dataframe_1092, dataframe_1093, how='outer')
df_total = pd.merge(df_total, dataframe_1094, how='outer')

df_total.to_excel('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\DADOS\\Abate de animais\\teste.xlsx', index=False)

if __name__ == '__main__':
    from sql import executar_sql
    executar_sql()