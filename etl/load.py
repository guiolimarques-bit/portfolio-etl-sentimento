import pandas as pd
from sqlalchemy import create_engine, text
import os

def load_to_sql(df_final):
    """Carrega o DataFrame final para o banco de dados SQLite (Demonstração de SQL/Carga)."""
    
    database_path = 'data/processed/reviews_db.sqlite'
    engine = create_engine(f'sqlite:///{database_path}')
    
    # 1. Carga dos Dados na Tabela Fato
    try:
        df_final.to_sql(
            'fato_avaliacoes', 
            con=engine, 
            if_exists='replace', 
            index=False
        )
        print(f"[L] Tabela 'fato_avaliacoes' criada e carregada com sucesso no SQL.")
        
        # 2. Query de Verificação (Demonstração de SQL)
        with engine.connect() as connection:
            sql_query = """
                SELECT sentiment_class, COUNT(*) 
                FROM fato_avaliacoes 
                GROUP BY sentiment_class 
                ORDER BY 2 DESC
            """
            # Executa a query usando a função text()
            result = connection.execute(text(sql_query)).fetchall()
            
            print("\n[L] Query de Verificação (Distribuição de Sentimento):")
            for row in result:
                print(f"  - {row[0]}: {row[1]} avaliações")
                
    except Exception as e:
        print(f"Erro ao carregar dados ou executar query SQL: {e}")