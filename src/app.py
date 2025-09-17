# app.py
import requests
import time
import sqlite3
import hashlib, re, os, threading
from typing import List, Optional, Dict, Any
from datetime import datetime
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

DB_PATH = os.getenv("QUOTES_DB", "quotes.db")
POLL_INTERVAL_SEC = 50 

# ---------- DB helpers ----------
def connect():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with connect() as conn:
        cur = conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS quotes (
            id TEXT PRIMARY KEY,
            quote TEXT NOT NULL,
            author TEXT,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_quotes_quote_author
        ON quotes(quote, author)
        """)
        conn.commit()

def normalize(s: Optional[str]) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", " ", s.strip().lower())

def make_quote_id(q: Dict[str, Any]) -> str:
    key = f"{normalize(q.get('q'))}|{normalize(q.get('a'))}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()

# ---------- Fetch/store ----------
def get_quotes() -> List[Dict[str, Any]]:
    try:
        r = requests.get("https://zenquotes.io/api/quotes", timeout=30)
        r.raise_for_status()
        data = r.json()
        return data if isinstance(data, list) else []
    except requests.RequestException as e:
        print("Fetch error:", e)
        return []

def store_quotes(quotes: List[Dict[str, Any]]) -> int:
    if not quotes: return 0
    inserted = 0
    print('>>> Storing quotes');
    with connect() as conn:
        cur = conn.cursor()
        for q in quotes:
            text = q.get("q")
            if not text:
                continue
            qid = make_quote_id(q)
            cur.execute(
                "INSERT OR IGNORE INTO quotes (id, quote, author) VALUES (?, ?, ?)",
                (qid, text, q.get("a")),
            )
            if cur.rowcount == 1:
                inserted += 1
        conn.commit()
    print('>>> Done storing quotes');
    return inserted

def print_total_count():
    with connect() as conn:
        cur = conn.cursor()
        (count,) = cur.execute("SELECT count(*) from quotes").fetchone()
        print(f'>>> Total: {count}')

# ---------- API models ----------
class QuoteOut(BaseModel):
    id: str
    quote: str
    author: Optional[str] = None
    added_at: datetime

def row_to_out(row: sqlite3.Row) -> QuoteOut:
    val = row["added_at"]
    added = datetime.fromisoformat(val) if isinstance(val, str) else val
    return QuoteOut(id=row["id"], quote=row["quote"], author=row["author"], added_at=added)

# ---------- Background fetch loop ----------
def fetch_loop(stop_event: threading.Event):
    init_db()
    while not stop_event.is_set():
        try:
            print('>>> Getting quotes');
            added = store_quotes(get_quotes())
            if added:
                print(f"Inserted {added} new quotes.")
        except Exception as e:
            print("Fetcher error:", e)
        finally:
            print_total_count()
            print('========================================')
            
        stop_event.wait(POLL_INTERVAL_SEC)

# ---------- Lifespan (modern startup/shutdown) ----------
stop_event = threading.Event()
fetch_thread: Optional[threading.Thread] = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global fetch_thread
    init_db()
    stop_event.clear()
    fetch_thread = threading.Thread(target=fetch_loop, args=(stop_event,), daemon=True)
    fetch_thread.start()
    print("Fetcher thread started.")
    try:
        yield
    finally:
        stop_event.set()
        if fetch_thread and fetch_thread.is_alive():
            fetch_thread.join(timeout=5)
        print("Fetcher thread stopped.")

# Create the app with lifespan
app = FastAPI(title="Quotes API", version="1.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten in prod
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------- Routes ----------
@app.get("/quotes/random", response_model=QuoteOut)
def random_quote():
    with connect() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM quotes ORDER BY RANDOM() LIMIT 1")
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="No quotes available")
        return row_to_out(row)

