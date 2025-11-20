import pandas as pd
import numpy as np
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

try:
    nltk.data.find('sentiment/vader_lexicon.zip')
except:
    nltk.download('vader_lexicon')

SIA = SentimentIntensityAnalyzer()

def clean_data(df_products, df_reviews):
    """Limpeza e padronização dos dados."""
    
    df_products.columns = [col.lower() for col in df_products.columns]
    df_reviews.columns = [col.lower() for col in df_reviews.columns]
    
    df_reviews.dropna(subset=['review_text', 'product_id', 'rating'], inplace=True)
    
    df_reviews['review_date'] = pd.to_datetime(df_reviews['review_date'], errors='coerce')
    df_products['launch_date'] = pd.to_datetime(df_products['launch_date'], errors='coerce')
    
    return df_products, df_reviews

def analyze_sentiment(df_reviews):
    """Aplica a análise de sentimento VADER (NLP) e classifica o resultado."""
    
    def get_sentiment_score(text):
        return SIA.polarity_scores(text)['compound'] if pd.notna(text) else 0.0
    
    def classify_sentiment(score):
        if score >= 0.05: return 'Positivo'
        elif score <= -0.05: return 'Negativo'
        else: return 'Neutro'
            
    df_reviews['sentiment_score'] = df_reviews['review_text'].apply(get_sentiment_score)
    df_reviews['sentiment_class'] = df_reviews['sentiment_score'].apply(classify_sentiment)
    
    return df_reviews

def run_transformations(df_products, df_reviews):
    """Orquestra a limpeza, o enriquecimento e a modelagem (Merge) dos dados."""
    
    df_products_clean, df_reviews_clean = clean_data(df_products, df_reviews)
    df_reviews_enriched = analyze_sentiment(df_reviews_clean)
    
    # Modelagem: União dos dados (Reviews e Metadados)
    df_final = pd.merge(
        df_reviews_enriched, 
        df_products_clean[['product_id', 'product_name', 'category', 'launch_date']], 
        on='product_id', 
        how='left' 
    )
    
    # Selecionar e reordenar colunas finais (Esquema Fato)
    df_final = df_final[[
        'review_id', 'product_id', 'product_name', 'category', 'review_date', 
        'rating', 'review_text', 'sentiment_score', 'sentiment_class', 'launch_date'
    ]]
    
    return df_final