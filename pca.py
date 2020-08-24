import asyncio, aiomysql
from websockets import connect
import multiprocessing
import enum, json, time
import pandas as pd
import numpy as np
from collections import namedtuple
from datetime import datetime, timedelta

# pd.set_option('display.width', 250)
# pd.set_option('display.max_columns', 25)

kraken_msg = {"event": "subscribe","pair": ["XBT/USD",],"subscription": {"name": "book"}}
kraken_msg = json.dumps(kraken_msg)

Exchange = namedtuple('Exchange' , 'id name uri msg fee fee_percent withdraw_fee')
deribit = Exchange(0, 'deribit' ,'wss://www.deribit.com/ws/api/v2', None, 0.001, None, None)
binance = Exchange(1, 'binance', 'wss://stream.binance.com:9443/ws/'+'btcusdt@depth5@100ms', None, None, 0.001, 0.0004)
kraken = Exchange(2, 'kraken', 'wss://ws.kraken.com', kraken_msg, None, 0.0026, 0.0005)

WDRW_FEE = max(binance.withdraw_fee, kraken.withdraw_fee)
SIZE_THRESH = 0.001 # ignore if there are orders in the spot order books smaller than this size
PERIOD_DAYS = 183 # include BTC option instruments with expiry up to `PERIOD_DAYS` days from now.
STREAM_INTERVAL = 0
CPU_WORKER_INTERVAL = 0
DB_INTERVAL = 0


async def getInstruments():
    msg = \
            {
                "jsonrpc": "2.0",
                "id": 7617,
                "method": "public/get_instruments",
                "params": {
                    "currency": "BTC",
                    "kind": "option",
                    "expired": False
                }
            }

    msg = json.dumps(msg)

    async with connect(deribit.uri) as websocket:
        await websocket.send(msg)
        return await websocket.recv()

def get_channel_list(days = 7):
    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(getInstruments())
    result = json.loads(result)
    result = result['result']
    instruments = pd.DataFrame(result)
    cutoff = datetime.today() + timedelta(days = days)
    cutoff = cutoff.timestamp() * 1000
    instruments = instruments[instruments['expiration_timestamp'] < cutoff]
    channel_list = 'book.' + instruments.instrument_name.values + '.100ms'
    channel_list = list(channel_list)

    return channel_list

class Pca:

    def __init__(self, channel_list):

        self.loop = asyncio.get_event_loop()
        self.main_queue = multiprocessing.Queue(maxsize=7)
        self.sql_queue = multiprocessing.Queue(maxsize=7)

        self.pool = None
        self.sql_loop = asyncio.get_event_loop()
        self.sql_loop.run_until_complete(self.pool_init(self.sql_loop))
        self.sql_loop.run_until_complete(self.init_existing_tables())

        self.streams = [
            self.sql_io(),
            self.deribit_stream(channel_list),
            self.open_exchange_stream(binance),
            self.open_exchange_stream(kraken),
        ]

        options_table = pd.DataFrame(columns=['Expiry','Strike', 'Timestamp','CallBid', 'CallSize', 'PutAsk', 'PutSize'])
        self.options_table_queue = multiprocessing.Queue(maxsize=1)
        self.options_table_queue.put(options_table)
        
        kraken_table = pd.DataFrame(columns=['Ask', 'Size', 'Timestamp'])
        self.kraken_table_queue = multiprocessing.Queue(maxsize=1)
        self.kraken_table_queue.put(kraken_table)

        binance_table = pd.DataFrame(columns=['Ask', 'Size'])
        self.binance_table_queue = multiprocessing.Queue(maxsize=1)
        self.binance_table_queue.put((0, binance_table))

        profit_table = pd.DataFrame(columns=['Expiry','Strike', 'Timestamp','CallBid', 'PutAsk', 'SpotAsk', 'Size', 'Exchange', 'Profit'])
        self.profit_table_queue = multiprocessing.Queue(maxsize=1)
        self.profit_table_queue.put(profit_table)

        self.start_workers()

        self.loop.run_until_complete(asyncio.gather(*self.streams))

    async def pool_init(self, loop):
        with open('mysqlUser.key', 'r') as f:
            user = f.read()
        with open('mysqlPwd.key', 'r') as f:
            pwd = f.read()
        self.pool =  await aiomysql.create_pool(host='127.0.0.1', port=3306, user=user, password=pwd, db='mysql',loop=loop)

    async def init_existing_tables(self):
        exist_tables = await self.sql_read("SHOW TABLES LIKE 'pca%'")
        init_timestamp = int(datetime.now().timestamp() * 1000)
        stmt_list = []
        for et in exist_tables:
            table_name = et[0]
            stmt = "INSERT INTO {} (timestamp, callbid, putask, spotask, size, exchange, profit) ".format(table_name)
            stmt += "VALUES ({}, {}, {}, {}, {}, '{}', {})".format(init_timestamp, 0,0,0,0,'None',0)
            stmt_list.append(stmt)

        await self.sql_write(stmt_list)

    async def sql_read(self, stmt):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(stmt)
                return await cur.fetchall()

    async def sql_write(self, stmt_list):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                for stmt in stmt_list:
                    await cur.execute(stmt)
                await conn.commit()

    async def sql_io(self):
        while True:
            if self.sql_queue.empty():
                await asyncio.sleep(DB_INTERVAL)
                continue
            stmt_list = self.sql_queue.get()
            await self.sql_write(stmt_list)
            # self.sql_queue.task_done()

    async def deribit_stream(self,instr_list):

        msg = \
            {
                "jsonrpc": "2.0",
                "id": 3600,
                "method": "public/subscribe",
                "params": {
                    "channels": instr_list
                }
            }
        msg = json.dumps(msg)
        async with connect(deribit.uri) as ws:
            await ws.send(msg)
            print('Initiated deribit stream.')
            while True:
                try:
                    response = await ws.recv()
                    # Discard oldest data in queue if full
                    if self.main_queue.full():
                        self.main_queue.get()
                    self.main_queue.put((deribit.id,response))
                    await asyncio.sleep(STREAM_INTERVAL)
                except Exception as e:

                    if not ws.open:
                        while True:
                            try:
                                print('{}. reconnecting deribit..'.format(e))
                                ws = await connect(deribit.uri)
                                break
                            except:
                                print('Timestamp: {} UTC+8. Pause for awhile..'.format(datetime.now()))
                                time.sleep(65)
                                continue
                    if ws.open:
                        print('deribit reconnected.')

    async def open_exchange_stream(self,exchange):
        async with connect(exchange.uri) as ws:
            if exchange.msg!= None:
                await ws.send(exchange.msg)
            print('Initiated {} stream.'.format(exchange.name))
            while True:
                try:
                    response = await ws.recv()
                    # Discard oldest data in queue if full
                    if self.main_queue.full():
                        self.main_queue.get()
                    # print(response)
                    self.main_queue.put((exchange.id,response))
                    await asyncio.sleep(STREAM_INTERVAL)
                except Exception as e:

                    if not ws.open:
                        while True:
                            try:
                                print('{}. reconnecting {}..'.format(e, exchange.name))
                                ws = await connect(exchange.uri)
                                break
                            except:
                                print('Timestamp: {} UTC+8. Pause for awhile..'.format(datetime.now()))
                                time.sleep(65)
                                continue
                    if ws.open:
                        print('{} reconnected.'.format(exchange.name))

    def start_workers(self):

        num_workers = multiprocessing.cpu_count() - 1
        for _ in range(num_workers):
            p = multiprocessing.Process(target=self.update_worker)
            p.start()

        p = multiprocessing.Process(target=self.comp_worker)
        p.start()

    def comp_worker(self):

        while True:

            # read
            options_table = self.options_table_queue.get()
            table = options_table.copy()
            sql_timestamp = int(datetime.now().timestamp()*1000)

            prev_timestamp, bnn_table = self.binance_table_queue.get()
            kkn_table = self.kraken_table_queue.get()

            profit_table = self.profit_table_queue.get()

            # return control
            self.options_table_queue.put(options_table)
            self.binance_table_queue.put((prev_timestamp, bnn_table))
            self.kraken_table_queue.put(kkn_table)
            try:
                if (len(table)>0) and (len(bnn_table)>0) and (len(kkn_table)>0):

                    # columns=['Expiry', 'Strike', 'Timestamp', 'CallBid', 'CallSize', 'PutAsk', 'PutSize']

                    bnn_table = bnn_table[bnn_table['Size'] > SIZE_THRESH]
                    binance_best_price = bnn_table.iloc[0]['Ask']

                    kkn_table = kkn_table[kkn_table['Size'] > SIZE_THRESH]
                    kkn_table = kkn_table.sort_values(by='Ask')
                    kraken_best_price = kkn_table.iloc[0]['Ask']

                    table = table[(table['CallBid'] > 0) & (table['PutAsk'] > 0) & (table['CallSize'] > 0) & (table['PutSize'] > 0)]

                    table['ProfitableSo'] = table['Strike'].values / (1 + deribit.fee + WDRW_FEE + table['PutAsk'].values - table['CallBid'].values)

                    binance_profit_rate = table['ProfitableSo'].values - (binance_best_price / (1 - binance.fee_percent))
                    kraken_profit_rate =  table['ProfitableSo'].values - (kraken_best_price  / (1 - kraken.fee_percent))

                    binance_best_size = np.minimum(np.minimum(table['CallSize'].values, table['PutSize'].values),bnn_table.iloc[0]['Size'])
                    kraken_best_size = np.minimum(np.minimum(table['CallSize'].values, table['PutSize'].values), kkn_table.iloc[0]['Size'])

                    binance_profit = binance_best_size * binance_profit_rate
                    kraken_profit = kraken_best_size * kraken_profit_rate

                    condition = binance_profit > kraken_profit

                    table['Exchange'] = np.where(condition, binance.name, kraken.name)
                    table['SpotAsk'] = np.where(condition, binance_best_price, kraken_best_price)
                    table['Size'] = np.where(condition, binance_best_size, kraken_best_size)
                    table['Profit'] = np.where(condition, binance_profit, kraken_profit)

                    profitable = table[table['Profit'] > 0]

                    changes = profit_table.merge(profitable, how='outer', indicator='merger', on=['Expiry', 'Strike'])

                    if len(changes) > 0:
                        changes = changes[changes['Size_x'] != changes['Size_y']]

                        if len(changes) > 0:

                            to_drop_keys = changes[changes['merger'] == 'left_only']['Expiry'].values, changes[changes['merger'] == 'left_only']['Strike'].values
                            to_append_keys = changes[changes['merger'] == 'right_only']['Expiry'].values, changes[changes['merger'] == 'right_only']['Strike'].values
                            to_update_keys = changes[changes['merger'] == 'both']['Expiry'].values, changes[changes['merger'] == 'both']['Strike'].values

                            #region: create sql statements
                            stmt_list = []

                            for i in range(len(to_drop_keys[0])):
                                expiry = to_drop_keys[0][i]
                                strike = to_drop_keys[1][i]
                                table_name = 'PCA_BTC_{}_{}'.format(expiry,int(strike))

                                stmt = "INSERT INTO {} (timestamp, callbid, putask, spotask, size, exchange, profit) ".format(
                                    table_name)
                                stmt += "VALUES ({}, {}, {}, {}, {}, '{}', {})".format(sql_timestamp, 0, 0, 0, 0, 'None', 0)
                                stmt_list.append(stmt)

                            for i in range(len(to_update_keys[0])):
                                expiry = to_update_keys[0][i]
                                strike = to_update_keys[1][i]
                                table_name = 'PCA_BTC_{}_{}'.format(expiry, int(strike))
                                row = profitable[(profitable['Expiry'] == expiry) & (profitable['Strike'] == strike)]

                                stmt = "INSERT INTO {} (timestamp, callbid, putask, spotask, size, exchange, profit) ".format(
                                    table_name)
                                stmt += "VALUES ({}, {}, {}, {}, {}, '{}', {})".format(
                                    sql_timestamp, row['CallBid'].values[0], row['PutAsk'].values[0], row['SpotAsk'].values[0], row['Size'].values[0],
                                    row['Exchange'].values[0], row['Profit'].values[0])

                                stmt_list.append(stmt)

                            for i in range(len(to_append_keys[0])):
                                expiry = to_append_keys[0][i]
                                strike = to_append_keys[1][i]
                                table_name = 'PCA_BTC_{}_{}'.format(expiry, int(strike))
                                row = profitable[(profitable['Expiry'] == expiry) & (profitable['Strike'] == strike)]

                                stmt = "CREATE TABLE IF NOT EXISTS {} ".format(table_name)
                                stmt += "(idx INT UNSIGNED NOT NULL AUTO_INCREMENT,"
                                stmt += "timestamp BIGINT NOT NULL,"
                                stmt += "callbid FLOAT NOT NULL,"
                                stmt += "putask FLOAT NOT NULL,"
                                stmt += "spotask FLOAT NOT NULL,"
                                stmt += "size FLOAT NOT NULL,"
                                stmt += "exchange VARCHAR(16) NOT NULL,"
                                stmt += "profit FLOAT NOT NULL,"
                                stmt += "PRIMARY KEY(idx))"

                                stmt_list.append(stmt)

                                stmt = "INSERT INTO {} (timestamp, callbid, putask, spotask, size, exchange, profit) ".format(table_name)
                                stmt+= "VALUES ({}, {}, {}, {}, {}, '{}', {})".format(
                                    sql_timestamp, row['CallBid'].values[0], row['PutAsk'].values[0], row['SpotAsk'].values[0], row['Size'].values[0],
                                    row['Exchange'].values[0],row['Profit'].values[0])

                                stmt_list.append(stmt)
                            #endregion
                            self.sql_queue.put(stmt_list)

                            profit_table = profitable.copy()
            except:
                print('=== bnn_table ===')
                print(bnn_table)
                print(len(bnn_table))
                print(len(bnn_table) > 0)


            # only return control of profit_table here
            self.profit_table_queue.put(profit_table)
            time.sleep(CPU_WORKER_INTERVAL)

    def update_worker(self):

        while True:
            exchange, response = self.main_queue.get()

            if exchange == deribit.id:
                try:
                    response = json.loads(response)

                    data = response['params']['data']
                    timestamp = data['timestamp']
                    expiry, strike, cp = data['instrument_name'].split('-')[1:4]

                    if cp == 'C':
                        bids = data['bids']
                        if len(bids) <= 0:
                            #task_done
                            continue
                        best_bid = bids[0][1]
                        best_bid_size = bids[0][2]
                        table = self.options_table_queue.get()

                        row = table[(table['Expiry'] == expiry) & (table['Strike'] == int(strike))]
                        if len(row) > 0:
                            if (timestamp < row['Timestamp'].values[0]) & (row['CallBid'].values[0] != -1):
                                # task_done
                                self.options_table_queue.put(table)
                                continue
                            table.loc[row.index, 'Timestamp'] = timestamp
                            table.loc[row.index, 'CallBid'] = best_bid
                            table.loc[row.index, 'CallSize'] = best_bid_size

                        else:
                            table = table.append({'Expiry':expiry,'Strike':int(strike), 'Timestamp':timestamp,'CallBid':best_bid,
                                                  'CallSize':best_bid_size, 'PutAsk':-1, 'PutSize':-1} , ignore_index=True)

                        self.options_table_queue.put(table)

                    else:
                        asks = data['asks']
                        if len(asks) <= 0:
                            # task_done
                            continue
                        best_ask = asks[0][1]
                        best_ask_size = asks[0][2]

                        table = self.options_table_queue.get()

                        row = table[(table['Expiry'] == expiry) & (table['Strike'] == int(strike))]
                        if len(row) > 0:
                            if (timestamp < row['Timestamp'].values[0]) and (row['PutAsk'].values[0] != -1):
                                # task_done
                                self.options_table_queue.put(table)
                                continue
                            table.loc[row.index, 'Timestamp'] = timestamp
                            table.loc[row.index, 'PutAsk'] = best_ask
                            table.loc[row.index, 'PutSize'] = best_ask_size
                        else:
                            table = table.append(
                                {'Expiry': expiry, 'Strike': int(strike), 'Timestamp': timestamp, 'CallBid': -1,
                                 'CallSize': -1, 'PutAsk': best_ask, 'PutSize': best_ask_size}, ignore_index=True)

                        self.options_table_queue.put(table)

                except Exception as e:
                    print('excepted:',e)
                    continue

            elif exchange == binance.id:
                response = json.loads(response)
                asks = response['asks']
                timestamp = response['lastUpdateId']

                prev_timestamp, bnn_table = self.binance_table_queue.get()
                if timestamp > prev_timestamp:
                    bnn_table = pd.DataFrame(asks, columns=['Ask', 'Size'], dtype=float)
                self.binance_table_queue.put((timestamp, bnn_table))

            elif exchange == kraken.id:
                response = json.loads(response)
                try:
                    asks = response[1]['a']
                except KeyError:
                    try:
                        asks = response[1]['as']
                    except KeyError:
                        continue

                kkn_table = self.kraken_table_queue.get()

                for a in asks:
                    a = list(map(float, a[:3]))
                    row = kkn_table[(kkn_table['Ask'] == a[0]) & (kkn_table['Timestamp'] < a[2])]

                    if len(row) == 0:
                        kkn_table = kkn_table.append({'Ask': a[0], 'Size': a[1], 'Timestamp': a[2]}, ignore_index=True)
                    else:
                        if a[1] > 0:
                            kkn_table.loc[row.index, 'Size'] = a[1]
                            kkn_table.loc[row.index, 'Timestamp'] = a[2]
                        else:
                            kkn_table = kkn_table.drop(row.index)
                self.kraken_table_queue.put(kkn_table)
            time.sleep(CPU_WORKER_INTERVAL)

if __name__ == '__main__':

    print('Beginning time: {}'.format(datetime.now()))
    channel_list = get_channel_list(days=PERIOD_DAYS)

    print('Number of option instruments: {}'.format(len(channel_list)))
    pca_obj = Pca(channel_list)

