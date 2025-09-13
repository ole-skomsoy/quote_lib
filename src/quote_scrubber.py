import requests
import time
import sqlite3
import hashlib, re

def get_quotes():
    quotes = requests.get('https://zenquotes.io/api/quotes').json()
    return quotes
    
def store_quotes(quotes):
    connection = sqlite3.connect("quotes.db")
    cursor = connection.cursor()
    
    for quote in quotes:
        if quote_exists(quote, cursor):
            print('quote already exists', quote)
            return
        
        qid = make_quote_id(quote)
        cursor.execute("""
        INSERT OR IGNORE INTO quotes (id, quote, author)
        VALUES (?, ?, ?)
        """, (qid, quote['q'], quote['a']))
        
    connection.commit()
    connection.close()

def init_db():
    connection = sqlite3.connect("quotes.db")
    cursor = connection.cursor()
    
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS quotes (
        id TEXT PRIMARY KEY,
        quote TEXT NOT NULL,
        author TEXT,
        added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )   
    """)
    
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_quotes_text_author
    ON quotes (quote, author);
    """)

    connection.commit()
    connection.close()

def make_quote_id(quote):
    key = f"{normalize(quote['q'])}|{normalize(quote['a'])}"
    return hashlib.sha256(key.encode('utf-8')).hexdigest()

def quote_exists(quote, cursor):
    qid = make_quote_id(quote)
    cursor.execute("SELECT 1 FROM quotes WHERE id = ?", (qid,))
    return cursor.fetchone() is not None

def normalize(s):
    if not s:
        return ""
    s = s.strip().lower()
    s = re.sub(r'\s+', ' ', s)
    return s

def main():
    init_db()
    while True:
        quotes = get_quotes()
        if (quotes): store_quotes(quotes)
        time.sleep(50)
main()