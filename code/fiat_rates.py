from .util import *
from .sqlite import *

import requests
import datetime
import sortedcontainers

class Twelve:
    FIAT_SYMBOLS = {'USD':{'left_symbol':'$'},
                    'AUD':{'left_symbol':'$'},
                    'CAD':{'left_symbol':'$'},
                    'EUR':{'right_symbol':'€'},
                    'GBP':{'left_symbol':'£'},
                    'JPY':{'left_symbol':'¥'},
                    'CHF':{'left_symbol':'fr.'},
                    'NZD':{'left_symbol':'$'}}
    @classmethod
    def is_fiat(cls,symbol):
        if symbol in Twelve.FIAT_SYMBOLS:
            return True
        return False

    def __init__(self,symbol):
        self.api_key = os.environ.get('api_key_twelve')
        self.session = requests.session()
        self.db = None
        self.fiat = symbol


    def init_rates(self):
        self.rate_tables = {}
        self.shortcut_tables = {}
        self.db = SQLite('db', do_logging=False, read_only=True)
        for symbol in Twelve.FIAT_SYMBOLS:
            rate_table = sortedcontainers.SortedDict()
            rows = self.db.select("SELECT timestamp,rate FROM fiat_rates WHERE currency='"+symbol+"'")
            for row in rows:
                rate_table[row[0]] = row[1]
            self.rate_tables[symbol] = rate_table
            self.shortcut_tables[symbol] = {}
        self.db.disconnect()
        self.db = None

    def lookup_rate(self,symbol,ts):
        if symbol == 'USD':
            return 1

        rate_table = self.rate_tables[symbol]
        shortcut_table = self.shortcut_tables[symbol]

        if ts in shortcut_table:
            return shortcut_table[ts]

        times = rate_table.keys()
        first = times[0]
        last = times[-1]

        if ts < first:
            rate = rate_table[first]
            if ts < first - 3600:
                log("Bad rate for in fiat lookup",self.fiat, ts, "is smaller than first timestamp", first, filename='fiat.txt')

        elif ts > last:
            rate = rate_table[last]
            if ts > last + 3600*24*3:
                log("Bad rate for in lookup", self.fiat, ts, "is larger than last timestamp", last, filename='fiat.txt')

        else:
            rate_table = rate_table
            idx = rate_table.bisect_left(ts)
            ts_bottom = rate_table.keys()[idx - 1]
            ts_top = rate_table.keys()[idx]
            bot_fraction = 1 - (ts - ts_bottom) / (ts_top - ts_bottom)
            top_fraction = 1 - (ts_top - ts) / (ts_top - ts_bottom)

            rate = rate_table[ts_bottom] * bot_fraction + rate_table[ts_top] * top_fraction
        shortcut_table[ts] = rate
        return rate


    @classmethod
    def create_table(cls):
        db = SQLite('db', do_logging=False, read_only=False)
        db.create_table('fiat_rates', 'currency, timestamp INTEGER, rate NUMERIC', drop=False)
        db.create_index('fr1','fiat_rates', 'currency')
        db.create_index('fr2','fiat_rates', 'currency, timestamp', unique=True)
        db.commit()

    def download_all_rates(self,do_wait=False):
        self.db = SQLite('db', do_logging=False, read_only=True)
        all_db_writes = []
        for symbol in Twelve.FIAT_SYMBOLS.keys():
            if symbol != 'USD':
                need_to_wait, db_writes = self.download_rates(symbol)
                if need_to_wait and do_wait:
                    time.sleep(10)
                all_db_writes.extend(db_writes)

        if len(all_db_writes) > 0:
            log('all_db_writes',len(all_db_writes), filename='fiat.txt')
            self.db.disconnect()
            self.db = SQLite('db', do_logging=False, read_only=False)
            for entry in all_db_writes:
                self.db.insert_kw('fiat_rates',currency=entry[0],timestamp=entry[1],rate=entry[2])
            self.db.commit()
        self.db.disconnect()



    def check_download_needed(self, symbol, ts):
        assert symbol in Twelve.FIAT_SYMBOLS
        rows = self.db.select("SELECT MAX(timestamp) FROM fiat_rates WHERE currency='"+symbol+"'")
        val = rows[0][0]
        if val is None or val < ts - 3*24*3600:
            return True
        return False

    def download_rates(self, symbol):
        assert symbol in Twelve.FIAT_SYMBOLS
        db_writes = []
        t = int(time.time())


        #they only have hourly data from 2020 onward. Get daily data from earlier than that.
        rows = self.db.select("SELECT MAX(timestamp) FROM fiat_rates WHERE currency='" + symbol + "'")
        latest_available = rows[0][0]
        log("latest available",symbol,latest_available, filename='fiat.txt')
        if latest_available is not None and latest_available > t - 3*24*3600:
            return 0, db_writes


        if latest_available is None or latest_available < 1580342400:
            url = "https://api.twelvedata.com/time_series?symbol=USD/"+symbol+"&interval=1day&outputsize=5000&timezone=UTC&start_date=2012-12-31&end_date=2020-01-30&order=ASC&apikey="+str(self.api_key)
            log("Sending request", url, filename='fiat.txt')
            try:
                resp = self.session.get(url)
                js = resp.json()
                data = js['values']
                for entry in data:
                    ts = entry['datetime']
                    ts_datestart = int(datetime.datetime.strptime(ts,"%Y-%m-%d").replace(tzinfo=datetime.timezone.utc).timestamp())
                    start_rate = float(entry['open'])
                    mid_rate = (float(entry['open'])+float(entry['close'])+float(entry['high'])+float(entry['low']))/4
                    end_rate = float(entry['close'])
                    start_ts = ts_datestart+3600
                    mid_ts = ts_datestart+12*3600
                    end_ts = ts_datestart+23*3600
                    db_writes.append([symbol,start_ts,start_rate])
                    db_writes.append([symbol,mid_ts, mid_rate])
                    db_writes.append([symbol,end_ts, end_rate])
                    latest_available = end_ts
            except:
                log_error("Failed to retrieve old rates for "+symbol+" from twelve",url)
                return 1, db_writes



        while latest_available < t - 3*24*3600:
            start_date = timestamp_to_date(latest_available, format="%Y-%m-%d", utc=True)
            end_date = timestamp_to_date(latest_available + 185 * 24 * 3600, format="%Y-%m-%d", utc=True)
            url = "https://api.twelvedata.com/time_series?symbol=USD/" + symbol + "&interval=1h&outputsize=5000&timezone=UTC&start_date="+start_date+"&end_date="+end_date+"&order=ASC&apikey=" + str(
                self.api_key)
            log("Sending request",url,filename='fiat.txt')
            try:
                resp = self.session.get(url)
                js = resp.json()
                data = js['values']
                for entry in data:
                    ts = entry['datetime']
                    ts = int(datetime.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=datetime.timezone.utc).timestamp())
                    rate = float(entry['close'])
                    db_writes.append([symbol,ts,rate])
                    if ts > latest_available:
                        latest_available = ts

                if latest_available < t - 3*24*3600:
                    time.sleep(10)
            except:
                log_error("Failed to retrieve new rates for "+symbol+" from twelve",url)
                return 1, db_writes


        # if len(db_writes):
        #     disconnect = False
        #     if self.db is None:
        #         self.db = SQLite('db', do_logging=False, read_only=False)
        #         disconnect = True
        #     if self.db.read_only:
        #         self.db.disconnect()
        #         self.db = SQLite('db', do_logging=False, read_only=False)
        #     for entry in db_writes:
        #         self.db.insert_kw('fiat_rates',currency=symbol,timestamp=entry[0],rate=entry[1])
        #     self.db.commit()
        #     if disconnect:
        #         self.db.disconnect()
        #         self.db = None
        #     return 1

        return 1,db_writes

            # pprint.pprint(db_writes)



