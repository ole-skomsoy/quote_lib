import requests
import time
import sqlite3
import hashlib, re

def get_quote():
    quote = requests.get('https://zenquotes.io/api/random').json()[0]
    if quote['a'] == 'zenquotes.io':
        print('Too many requests...')
        return
    print(quote)
    return quote
    
def store_quote(quote):
    connection = sqlite3.connect("quotes.db")
    cursor = connection.cursor()
    
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
        quote = get_quote()
        if (quote): store_quote(quote)
        time.sleep(10)
        # time.sleep(6) # 5 per 30-second period
    
main()