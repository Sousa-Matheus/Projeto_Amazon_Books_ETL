📚 Projeto Amazon Books ETL
📌 Visão Geral

Este projeto consiste em um pipeline de Engenharia de Dados que realiza web scraping no site da Amazon para coletar livros relacionados a Engenharia de Dados.

Os dados são extraídos, tratados de forma simples (remoção de duplicados) e armazenados em um banco de dados PostgreSQL. Todo o fluxo é orquestrado utilizando Apache Airflow, permitindo automação e monitoramento do pipeline.

O projeto foi desenvolvido com base no conteúdo do canal:
👉 https://www.youtube.com/@sunjanaindata

🏗️ Arquitetura

O pipeline segue o modelo ETL (Extract, Transform, Load):

🔹 Extract (Extração)
Realiza requisições HTTP utilizando requests
Faz scraping das páginas da Amazon com BeautifulSoup
Coleta informações como:
Título do livro
Preço
Avaliação
Número de reviews
🔹 Transform (Transformação)
Estrutura os dados extraídos
Remove registros duplicados

⚠️ Não há tratamento de valores nulos neste projeto

🔹 Load (Carga)
Insere os dados tratados em um banco PostgreSQL
🔹 Orquestração
Pipeline gerenciado por DAGs no Apache Airflow
Permite execução agendada e monitoramento
🛠️ Tecnologias Utilizadas
Python
Requests
BeautifulSoup
Apache Airflow
PostgreSQL
Docker
📂 Estrutura do Projeto
Projeto_Amazon_Books_ETL/
│
├── dags/                 # DAGs do Airflow
├── logs/                 # Logs do Airflow
├── docker-compose.yaml   # Configuração dos containers
├── requirements.txt      # Dependências do projeto
└── README.md             # Documentação
⚙️ Como Funciona
O Airflow executa a DAG em um intervalo definido
O script realiza scraping no site da Amazon
Os dados são extraídos e estruturados
Registros duplicados são removidos
Os dados são carregados no PostgreSQL
Logs são gerados para acompanhamento
🚀 Como Executar o Projeto
1. Clonar o repositório
git clone https://github.com/seu-usuario/Projeto_Amazon_Books_ETL.git
cd Projeto_Amazon_Books_ETL
2. Subir os containers
docker-compose up --build
3. Acessar o Airflow
URL: http://localhost:8080
Executar a DAG manualmente ou aguardar o agendamento
🧠 Aprendizados
Construção de pipeline ETL simples
Web scraping com Python
Orquestração com Airflow
Integração com banco de dados PostgreSQL
Uso de Docker para ambientes reprodutíveis
⚠️ Aviso

Este projeto é apenas para fins educacionais.

Respeite os termos de uso dos sites
Evite excesso de requisições
Utilize scraping de forma responsável
📈 Melhorias Futuras
Implementar tratamento de valores nulos
Adicionar controle de erros e retries
Suporte a paginação
Integração com Data Lake (ex: Azure)
Camadas Bronze / Silver / Gold
Visualização dos dados (Power BI ou Streamlit)
🙌 Créditos

Projeto baseado no conteúdo do canal:
👉 https://www.youtube.com/@sunjanaindata
