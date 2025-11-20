import pandas as pd
import os

# Importa as funções dos módulos ETL
from etl.extract import extract_api_data, generate_reviews
from etl.transform import run_transformations
from etl.load import load_to_sql


def run_pipeline():
    
    print("\n--- 1. ETAPA E (EXTRAÇÃO) ---")
    
    # E1: Extrai Metadados e Gera Reviews
    df_products = extract_api_data(num_records=50)
    # df_products é passado para gerar as reviews com base nos IDs
    df_reviews = generate_reviews(df_products, num_reviews=300)
    
    if df_products.empty or df_reviews.empty:
        print("Pipeline abortada: Falha na extração de dados.")
        return
        
    # E2: Salva RAW para persistência
    os.makedirs('data/raw', exist_ok=True)
    df_products.to_csv('data/raw/products_metadata_raw.csv', index=False)
    df_reviews.to_csv('data/raw/product_reviews_raw.csv', index=False)
    print(f"[{'E'}] {len(df_products)} produtos e {len(df_reviews)} reviews salvos em data/raw/.")


    print("\n--- 2. ETAPA T (TRANSFORMAÇÃO) ---")
    
    # T: Roda a limpeza, Análise de Sentimento (NLTK) e Modelagem
    df_final = run_transformations(df_products, df_reviews)
    
    # T: Salva o resultado transformado
    os.makedirs('data/processed', exist_ok=True)
    df_final.to_csv('data/processed/transformed_data.csv', index=False)
    print(f"[{'T'}] {len(df_final)} registros finais transformados e salvos em data/processed/.")
    
    
    print("\n--- 3. ETAPA L (CARGA) ---")
    
    # L: Carrega o resultado final no SQL 
    load_to_sql(df_final)
    
    print("\n--- PIPELINE ETL CONCLUÍDA COM SUCESSO! ---")


if __name__ == "__main__":
    run_pipeline()