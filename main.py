from supabase import create_client, Client

url = "https://sksyzcsstxtirchbuulk.supabase.co"
key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InNrc3l6Y3NzdHh0aXJjaGJ1dWxrIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MzQ5MzE5NzMsImV4cCI6MjA1MDUwNzk3M30.0xMUC8m_vWp-NYTFoIKSkZhFlmafKAuwkuwRLMgXjzo"

supabase: Client = create_client(url, key)

# import psycopg2

# # Database connection details (Replace with your actual values)
# DB_CONFIG = {
#     "host": "aws-0-ap-south-1.pooler.supabase.com",
#     "port": "6543",
#     "dbname": "postgres",
#     "user": "postgres.sksyzcsstxtirchbuulk",
#     "password": "transaction"
# }

# def get_db_connection():
#     """Establish and return a database connection."""
#     return psycopg2.connect(**DB_CONFIG)
