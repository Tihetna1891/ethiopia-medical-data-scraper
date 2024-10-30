from sqlalchemy import create_engine
import psycopg2
# PostgreSQL connection details
db_config = {
    'dbname': 'telegram_data',
    'user': 'postgress',
    'password': '1891',
    'host': 'localhost',  # Or your host address
    'port': '5432'
}

# Create PostgreSQL connection engine using SQLAlchemy
engine = create_engine(f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}")
