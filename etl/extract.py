import requests
import pandas as pd
import random
from datetime import datetime, timedelta

def extract_api_data(num_records=50):
    """Extrai metadados de produtos simulados da API e retorna DataFrame."""
    url = f"https://randomuser.me/api/?results={num_records}&inc=name,location,dob,login"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json().get('results', [])
        
        product_data = []
        for item in data:
            product_data.append({
                'product_id': item['login']['uuid'],
                'product_name': f"Produto {item['name']['first']} {item['name']['last']}",
                'category': item['location']['country'],
                'launch_date': item['dob']['date'][:10]
            })
            
        df = pd.DataFrame(product_data)
        print(f"[E] API: {len(df)} registros de produtos extraídos.")
        return df

    except requests.exceptions.RequestException as e:
        print(f"Erro na extração da API: {e}")
        return pd.DataFrame()

def generate_reviews(df_products, num_reviews=300): # <-- CORRIGIDO AQUI!
    """Gera avaliações simuladas e associa aos IDs de produtos, retornando DataFrame."""
    
    positive_reviews = ["Produto excelente!", "Qualidade superior.", "Recomendo a todos!"]
    negative_reviews = ["Quebrou rápido.", "Muito decepcionante.", "Não vale o preço."]
    neutral_reviews = ["É ok, está na média.", "Funcional, mas simples."]
    all_reviews = positive_reviews + negative_reviews + neutral_reviews
    
    reviews = []
    end_date = datetime.now()
    product_ids = df_products['product_id'].tolist()
    
    for _ in range(num_reviews): # <-- Usa o argumento
        product_id = random.choice(product_ids)
        review_text = random.choice(all_reviews)
        
        if review_text in positive_reviews: rating = random.randint(4, 5)
        elif review_text in negative_reviews: rating = random.randint(1, 2)
        else: rating = random.randint(3, 4)
            
        random_days = random.randint(1, 90)
        review_date = (end_date - timedelta(days=random_days)).strftime('%Y-%m-%d')
        
        reviews.append({
            'review_id': random.randint(1000, 99999),
            'product_id': product_id,
            'review_text': review_text,
            'rating': rating,
            'review_date': review_date
        })

    df = pd.DataFrame(reviews)
    return df