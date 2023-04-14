from dblib import init_db, insert
import asyncio
import sqlite3

DBFILE_PATH = "../rates.db"

if __name__ == "__main__":
    # init_db(DBFILE_PATH, init=False)
    
    # data = [{'ask': '4019486', 'bid': '4016913', 'high': '4045596', 'last': '4019486', 'low': '3977002', 'symbol': "BTC_JPY", 'timestamp': "today", 'volume': '1206.61'}]

    # asyncio.run(insert(data, DBFILE_PATH))

    # insertされているかチェック
    conn = sqlite3.connect(DBFILE_PATH)
    cur = conn.cursor()
    cur.execute("SELECT * FROM rates")
    res = cur.fetchall()
    print(res)
    print(len(res))
    cur.close()
    conn.close()