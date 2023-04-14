"""
GMOコインのwebsocket APIからリアルタイムに最新レートを取得するサーバ。
参考: https://note.com/nuko_bot/n/n4be85e09fbb3

third party library
    pybotters : https://github.com/MtkN1/pybotters
    aiosqlite : https://github.com/omnilib/aiosqlite
"""

import asyncio
import os
import sqlite3
from contextlib import closing

import aiosqlite
import pybotters

# CONST
DBFILE_PATH = "../rates.db"

BASE_REST_EP = "https://api.coin.z.com/public"
BASE_WS_EP = "wss://api.coin.z.com/ws/public/v1"

request_data = []  # 便宜上
request_lock = asyncio.Lock()


def create_table(dbfile_path):
    """
    databaseを作成し、テーブルを作成する。
    """

    with closing(sqlite3.connect(dbfile_path)) as connection:
        cursor = connection.cursor()

        create_sql = "CREATE TABLE rates(\
            id INTEGER PRIMARY KEY AUTOINCREMENT, \
            ask FLOAT, \
            bid FLOAT, \
            high FLOAT, \
            last FLOAT, \
            low FLOAT, \
            symbol STRING, \
            unixtime FLOAT, \
            volume STRING)"

        cursor.execute(create_sql)

        connection.commit()


def confirm_init_db(init=False):
    """
    databaseの初期化を実行するかの確認。
    """

    if init == False:
        return False

    print("\n[ Final Confirmation ]\n")
    print("Do you really want to initialize the database?(Y/n) : ", end="")
    ans = input()

    if ans == "Y":
        return True
    else:
        print("\nExit Program.")
        print("Correct the argument about init_db().")
        return False


def init_db(dbfile_path, init=False):
    """
    databaseの初期化の実行。
    """

    confirm = confirm_init_db(init=init)
    if confirm == False:
        return

    if os.path.isfile(dbfile_path):
        os.remove(dbfile_path)
    create_table(dbfile_path)


async def insert(data, dbfile_path):
    """
    非同期にデータベースにinsertする関数。

    dataは辞書のリスト。個別の辞書の要素は以下。
    {
        ask : decimal.Decimal
        bid : decimal.Decimal
        high : decimal.Decimal
        last : decimal.Decimal
        low : decimal.Decimal
        symbol : enum 'Symbol'
        timestamp : datetime.datetime
        volume : decimal.Decimal
    }
    """

    async with aiosqlite.connect(dbfile_path) as connection:
        values = []
        for i in range(len(data)):
            data_comps = data[i]
            ask = float(data_comps["ask"])
            bid = float(data_comps["bid"])
            high = float(data_comps["high"])
            last = float(data_comps["last"])
            low = float(data_comps["low"])
            symbol = data_comps["symbol"].name  # 列挙型からnameを取り出す
            unixtime = data_comps["timestamp"].timestamp()  # unixtimeに変換
            volume = float(data_comps["volume"])

            values.append((ask, bid, high, last, low, symbol, unixtime, volume))

        insert_sql = "INSERT INTO rates(ask, bid, high, last, low, symbol, unixtime, volume) VALUES(?, ?, ?, ?, ?, ?, ?, ?)"
        await connection.executemany(insert_sql, values)
        await connection.commit()

        print("insert.")


async def push(ticker):
    """
    ws_fetchで取得したtickerデータをglobal変数のrequest_dataにextendしていく。
    db操作でもたついても、fetchを中断させないため。
    """

    print("push() called.")
    global request_data
    async with request_lock:
        request_data.extend(ticker)


async def ws_subscribe(base_rest_ep, base_ws_ep):
    """
    GMOコインのweb socketに接続して、tickerデータを購読する。
    取得したデータは、request_dataにpushしていく。
    """

    async with pybotters.Client(base_url=base_rest_ep) as client:
        store = pybotters.GMOCoinDataStore()
        await client.ws_connect(
            base_ws_ep,
            send_json={
                "command": "subscribe",
                "channel": "ticker",
                "symbol": "BTC_JPY",
            },
            hdlr_json=store.onmessage,
        )

        while not len(store.ticker):
            await store.ticker.wait()

        while True:
            await store.ticker.wait()
            data = store.ticker.find()
            await push(data)


async def getter(dbfile_path):
    """
    requestデータをinsert関数に渡す。
    ws_subscribe()と非同期で処理される。
    """

    global request_data

    while True:
        async with request_lock:
            data = request_data.copy()
            request_data = []

        if len(data) > 0:
            print("len of data: {}".format(len(data)))
            await insert(data, dbfile_path)

        await asyncio.sleep(0.03)


def main(base_rest_ep, base_ws_ep, dbfile_path):
    
    # databaseの初期化
    init_db(DBFILE_PATH, init=True)

    # event loop
    loop = asyncio.get_event_loop()
    asyncio.ensure_future(ws_subscribe(base_rest_ep, base_ws_ep))
    asyncio.ensure_future(getter(dbfile_path))
    loop.run_forever()


if __name__ == "__main__":
    main(BASE_REST_EP, BASE_WS_EP, DBFILE_PATH)
