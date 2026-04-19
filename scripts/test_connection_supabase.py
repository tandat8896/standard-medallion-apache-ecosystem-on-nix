import os
import psycopg2
import sys
from dotenv import load_dotenv

# 1. Load các biến từ file .env
load_dotenv()

# 2. Lấy link từ biến ATLAS_STAGING_URL
DATABASE_URL = os.getenv("ATLAS_STAGING_URL")

def check_connection():
    if not DATABASE_URL:
        print("[FAIL] Không tìm thấy biến ATLAS_STAGING_URL trong file .env!")
        sys.exit(1)

    print(f"--- [START] Testing Connection from .env ---")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        cur.execute("SELECT current_database();")
        db_name = cur.fetchone()[0]
        
        print(f"[OK] Kết nối tới Supabase thành công!")
        print(f"[INFO] Đang đứng tại Database: {db_name}")
        
        cur.close()
        conn.close()
        print(f"--- [FINISH] SUCCESS ---")

    except Exception as e:
        print(f"[FAIL] Lỗi rồi Đạt ơi: {e}")
        sys.exit(1)

if __name__ == "__main__":
    check_connection()
