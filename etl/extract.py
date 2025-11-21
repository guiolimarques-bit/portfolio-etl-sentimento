import requests
import pandas as pd
import random
from datetime import datetime, timedelta

def extract_api_data(num_records=50):
    """Extrai metadados de produtos simulados, garantindo launch_date anterior a 2023."""
    url = f"https://randomuser.me/api/?results={num_records}&inc=name,location,dob,login"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json().get('results', [])
        
        product_data = []
        # launch_date ANTERIOR ÀS REVIEWS (antes de 2021) ⬇️
        end_date_limit = datetime(2021, 1, 1).date() 
        # Início da faixa de datas (ex: 2 anos antes de 2021)
        start_date = end_date_limit - timedelta(days=2 * 365)
        
        time_between_dates = end_date_limit - start_date
        days_between_dates = time_between_dates.days
        
        for item in data:
            # Gerar uma data aleatória 
            random_number_of_days = random.randrange(days_between_dates)
            random_launch_date = start_date + timedelta(days=random_number_of_days)

            product_data.append({
                'product_id': item['login']['uuid'],
                'product_name': f"Produto {item['name']['first']} {item['name']['last']}",
                'category': item['location']['country'],
                'launch_date': random_launch_date.strftime('%Y-%m-%d') 
            })
            
        df = pd.DataFrame(product_data)
        print(f"[E] API: {len(df)} registros de produtos extraídos.")
        return df

    except requests.exceptions.RequestException as e:
        print(f"Erro na extração da API: {e}")
        return pd.DataFrame()

def generate_reviews(df_products, num_reviews=300):
    """Gera avaliações simuladas e as associa aos IDs de produtos, com datas abrangendo 2023-2025."""
    
    positive_reviews = ["Produto excelente!", "Qualidade superior.", "Recomendo a todos!"]
    negative_reviews = [
        "Quebrou rápido. This is TERRIBLE.", # Usando o diagnóstico de sentimento
        "Muito decepcionante.",
        "Não vale o preço.",
    ]
    neutral_reviews = ["É ok, está na média.", "Funcional, mas simples."]
    all_reviews = positive_reviews + negative_reviews + neutral_reviews
    
    reviews = []
    
    # CORREÇÃO: review_date PARA VÁRIOS ANOS (2021 até hoje)
    start_review_date = datetime(2021, 1, 1) # Início das avaliações
    end_review_date = datetime.now()         # Hoje
    
    time_delta = end_review_date - start_review_date
    days_range = time_delta.days
    
    product_ids = df_products['product_id'].tolist()
    
    for _ in range(num_reviews):
        product_id = random.choice(product_ids)
        review_text = random.choice(all_reviews)
        
        # Seleciona um dia aleatório dentro do intervalo de anos 
        random_days = random.randint(0, days_range)
        review_date = (start_review_date + timedelta(days=random_days)).strftime('%Y-%m-%d')
        
        if review_text in positive_reviews: rating = random.randint(4, 5)
        elif review_text in negative_reviews: rating = random.randint(1, 2)
        else: rating = random.randint(3, 4)
            
        reviews.append({
            'review_id': random.randint(1000, 99999),
            'product_id': product_id,
            'review_text': review_text,
            'rating': rating,
            'review_date': review_date
        })

    df = pd.DataFrame(reviews)
    return df