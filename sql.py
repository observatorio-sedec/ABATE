import psycopg2
from ETL_abate import df_total
from conexão import conexao

def executar_sql():
    cur = conexao.cursor()
    
    cur.execute('SET search_path TO abate, public')
    
    abate_estadual = '''
    CREATE TABLE IF NOT EXISTS abate.ptaa_estadual (
        id_abate_estadual SERIAL PRIMARY KEY,
        id TEXT,
        local TEXT,
        referencia TEXT,
        tipo_rebanho TEXT,
        tipo_inspecao TEXT,
        numero_de_informantes INTEGER,
        ano DATE,
        trimestre TEXT,
        Animais_abatidos NUMERIC,
        Peso_total_das_carcaças NUMERIC
    );
    '''
    
    cur.execute(abate_estadual)


    verificando_existencia_abate_estadual = '''
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'abate' AND table_type = 'BASE TABLE' AND table_name = 'ptaa_estadual';
    '''
    
    cur.execute(verificando_existencia_abate_estadual)
    resultado_abate_estadual = cur.fetchone()
    
    # Truncando a tabela se ela existir
    if resultado_abate_estadual[0] == 1:
        dropando_tabela_abate_estadual = '''
        TRUNCATE TABLE abate.ptaa_estadual;
        '''
        cur.execute(dropando_tabela_abate_estadual)

    # Inserindo dados
    inserindo_abate_estadual = '''
    INSERT INTO abate.ptaa_estadual (id, local, referencia, tipo_rebanho, tipo_inspecao, numero_de_informantes, ano, trimestre, Animais_abatidos, Peso_total_das_carcaças)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''
    
    try:
        for idx, i in df_total.iterrows():
            dados = (
                i['id'],
                i['nome'], 
                i['referencia'],
                i['tipo_rebanho'],
                i['tipo_inspecao'],
                i['Número de informantes'],
                i['ano'],
                i['trimestre'],
                i['Animais abatidos'],
                i['Peso total das carcaças']
            )
            cur.execute(inserindo_abate_estadual, dados)
            
        conexao.commit()
    except psycopg2.Error as e:
        print(f"Erro ao inserir dados estaduais: {e}")
        conexao.rollback()  
        
    finally:
        cur.close()
        if conexao:
            conexao.close()
