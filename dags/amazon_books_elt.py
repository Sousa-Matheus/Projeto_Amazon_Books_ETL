# importando bibliotecas
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
from datetime import datetime, timedelta
import pandas as pd
from bs4 import BeautifulSoup

# definindo os readers para o uso no requests
headers = {
    "Referer": "https://www.amazon.com/",
    "Sec-Ch-Ua": "Not_A Brand",
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": "Windows",
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/118.0.5993.70 Safari/537.36")
}

# Criando função para trazer dados do site no formato html com requests e utilizando beautifulsoup para organizar e consultar
# e consultar de acordo com a classe, e fazer alguns tratamentos simples com pandas
def get_amazon_books_data(num_books, ti):
    
    base_url = f"https://www.amazon.com/data-engineering/s?k=data+engineering"

    books = []

    seen_titles = set()

    page = 1

    while len(books) < num_books:
        url = f"{base_url}&page={page}"
        
        response = requests.get(url, headers=headers)

        if response.status_code == 200:

            soup = BeautifulSoup(response.content, "html.parser")

            book_containers = soup.find_all("div", {"class": "s-result-item"})

            for book in book_containers:
                title = book.find("span", {"class": "a-text-normal"})
                author = book.find("a", {"class": "a-size-base"})
                price = book.find("span", {"class": "a-price-whole"})
                rating = book.find("span", {"class": "a-icon-alt"})

                if title and author and price and rating:
                    book_title = title.text.strip()

                    if book_title  not in seen_titles:
                        seen_titles.add(book_title)

                        books.append({
                            "title": book_title,
                            "author": author.text.strip(),
                            "price": price.text.strip(),
                            "rating": rating.text.strip(),
                        })

            page += 1
        else:
            print(f"Failed to retrieve page {page}")
            break

    books = books[:num_books]

    df = pd.DataFrame(books)

    df.drop_duplicates(subset=["title"], inplace=True)

    ti.xcom_push(key="books_data", value=df.to_dict(orient="records"))

# Função para inserir o dados no postgress
def load_books_data(ti):
    book_data = ti.xcom_pull(key="books_data", task_ids="get_amazon_books_data")
    if not book_data:
        raise ValueError("No book data found in XCom")
    
    postgreshook = PostgresHook(postgres_conn_id="books_connection")
    insert_query = """
    INSERT INTO amazon_books (title, author, price, rating)
    VALUES (%s, %s, %s, %s)
    """
    
    for book in book_data:
        postgreshook.run(insert_query, parameters=(book["title"], book["author"], book["price"], book["rating"]))

# Criando as dags e as tasks
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 9, 12),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

dag = DAG(
    "amazon_books_elt",
    default_args=default_args,
    description="Extract, Load, Transform Amazon Books Data in PostgreSQL",
    schedule_interval=timedelta(days=1),
)

fetch_books_task = PythonOperator(
    task_id="get_amazon_books_data",
    python_callable=get_amazon_books_data,
    op_kwargs={"num_books": 50},
    dag=dag,
)

create_table_task = PostgresOperator(
    task_id="create_books_table",
    postgres_conn_id="books_connection",
    sql="""
    CREATE TABLE IF NOT EXISTS amazon_books (
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        author TEXT,
        price TEXT,
        rating TEXT
    );
    """,
    dag=dag,
)

load_books_task = PythonOperator(
    task_id="load_books_data",
    python_callable=load_books_data,
    dag=dag,
)

fetch_books_task >> create_table_task >> load_books_task
      
    
