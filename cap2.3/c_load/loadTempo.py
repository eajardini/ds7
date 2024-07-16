# cap2.3/c_load/loadTempo.py

import pandas as pd
import psycopg2
from dotenv import load_dotenv
import os


## Load
# Parâmetros de conexão
load_dotenv()
db_params = {
    'dbname': os.getenv('DB_NAME_DW'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

create_table_query = '''
create table if not exists bi_DTempo
as
SELECT
	datum as Data,
	extract(year from datum) AS Ano,
	extract(month from datum) AS Mes,
	-- Localized month name
	to_char(datum, 'TMMonth') AS NomeMes,
	extract(day from datum) AS Dia,
	extract(doy from datum) AS DiaDoAno,
	-- Localized weekday
	to_char(datum, 'TMDay') AS NomeDiaSemana,
	-- ISO calendar week
	extract(week from datum) AS SemanaCalendario,
	to_char(datum, 'dd/mm/yyyy') AS DataFormatada,
	'Q' || to_char(datum, 'Q') AS Trimestre,	
	-- Weekend
	CASE WHEN extract(isodow from datum) in (6, 7) THEN 'Final de Semana' ELSE 'Dia da Semana' END AS FinalSemana,
	-- Fixed holidays 
        -- for Brasil
        CASE WHEN to_char(datum, 'DDMM') IN ('0101', '2104', '0105',  '0709', '2512')
		THEN 'Feriado' ELSE 'Dia útil' END
		AS Feriado,    
	-- ISO start and end of the week of this date
	datum + (1 - extract(isodow from datum))::integer AS InicioSemana,
	datum + (7 - extract(isodow from datum))::integer AS FimSemana,
	-- Start and end of the month of this date
	datum + (1 - extract(day from datum))::integer AS InicioMes,
	(datum + (1 - extract(day from datum))::integer + '1 month'::interval)::date - '1 day'::interval AS FimMes
FROM (
	-- There are 3 leap years in this range, so calculate 365 * 10 + 2 records
	SELECT '2022-01-01'::DATE + sequence.day AS datum
	FROM generate_series(0,3652) AS sequence(day)
	GROUP BY sequence.day
     ) DQ
order by 1;   

'''

insert_query = '''

'''


def createTableBI(connPar):   
  try:
    cur = connPar.cursor()
    cur.execute(create_table_query)
    connPar.commit()
    cur.close()

    return 1
  except Exception as e:
    print(f"[loadTempo.py|executeTransform] Ocorreu um erro: {e}")
    return None
  


def executeLoad():  
  try:
    print(f"Etapa: Carregando Tempo para o banco de dados")
    # Conecta com o Postgres
    conn = psycopg2.connect(**db_params)    
    createTableBI(conn)     

    conn.close()   
    return 1
  except Exception as e:
        print(f"[loadVendedor.py|executeTransform] Ocorreu um erro: {e}")
        return None