import requests
import pprint
import sortedcontainers
import time
import pickle
import traceback
from collections import defaultdict
import bisect
from .util import *
from .sqlite import *

class Coingecko:
    def __init__(self, verbose=False):
        self.contracts_map = {}
        # self.symbol_map = defaultdict(dict)
        self.rates = None
        self.shortcut_rates = defaultdict(dict)
        self.inferred_rates = {}
        self.shortcut_hits = 0
        self.verbose= verbose
        self.initialized = False

        self.time_spent_looking_up = 0

        # < option
        # value = 'ETH' > Ethereum < / option >
        # < option
        # value = 'Polygon' > Polygon < / option >
        # < option
        # value = 'Arbitrum' > Arbitrum < / option >
        # < option
        # value = 'Avalanche' > Avalanche < / option >
        # < option
        # value = 'Fantom' > Fantom < / option >
        # < option
        # value = 'BSC' > BSC < / option >
        # < option
        # value = 'HECO' > HECO < / option >
        # < option
        # value = 'Moonriver' > Moonriver < / option >
        self.chain_mapping = {
            'ETH':{'platform':'ethereum','id':'ethereum'},
            'Polygon':{'platform':'matic-network','id':'matic-network'},
            'Arbitrum':{'platform':'arbitrum-one','id':'ethereum'},
            'Avalanche':{'platform':'avalanche','id':'avalanche-2'},
            'Fantom':{'platform':'fantom','id':'fantom'},
            'BSC':{'platform':'binance-smart-chain','id':'binancecoin'},
            'HECO':{'platform':'huobi-token','id':'huobi-token'},
            'Moonriver':{'platform':'moonriver','id':'moonriver'},
            'Solana':{'platform':'solana','id':'solana'}
        }

        #symbol->coingecko id
        # self.base_assets = {
        #     'MATIC':'matic-network',
        #     'ETH':'ethereum',
        #     'HT':'huobi-token',
        #     'BNB':'binancecoin',
        #
        #     'FTM':'fantom',
        #     'AVAX':'avalanche-2',
        #     'SOL':'solana',
        #     'BTC':'bitcoin',
        #     'OKT':'okexchain',
        #     'ONE':'harmony',
        #     'XDAI':'xdai'
        # }


        self.custom_platform_mapping = {
            'huobi-token': {
                'usd-coin': ('0X9362BBEF4B8313A8AA9F0C9808B80577AA26B73B', 'USDC'),
                'dai': ('0X3D760A45D0887DFD89A2F5385A236B29CB46ED2A', 'DAI'),
            }
        }


    def dump(self,chain):
        rates_dump_file = open('data/users/' + chain.addr +"/"+chain.name+ "_rates", "wb")
        pickle.dump(self, rates_dump_file)
        rates_dump_file.close()

    @classmethod
    def init_from_cache(cls,chain):
        C = pickle.load(open('data/users/' + chain.addr+"/"+chain.name + "_rates", "rb"))
        # log("coingecko initialized",C.initialized)
        # log("coingecko BUSD rate",C.lookup_rate('0xe9e7cea3dedca5984780bafc599bd69add087d56',1623272501))
        return C


    def init_from_db(self, chain, contracts, my_address, initial=True):
        t = time.time()
        chain_name = chain.name


        db = SQLite('db',do_logging=False)
        rows = db.select("select symbols.id, symbol, name, platform, address from symbols, platforms where symbols.id = platforms.id")

        for row in rows:
            id,symbol,name,platform,address = row
            self.contracts_map[address] = {'id':id,'symbol':symbol}

        for platform, mapping in self.custom_platform_mapping.items():
            for id, tuple in mapping.items():
                self.contracts_map[tuple[0].lower()] = {'id':id,'symbol':tuple[1]}

        # pprint.pprint(self.contracts_map)
        # if self.verbose:
        #     log("map",self.contracts_map)

        ids = set()
        for contract in contracts:
            if contract in self.contracts_map:
                ids.add(self.contracts_map[contract]['id'])
        ids.add(self.chain_mapping[chain_name]['id'])
        self.contracts_map[chain.main_asset] = {'id':self.chain_mapping[chain_name]['id'],'symbol':chain.main_asset}
        if self.verbose:
            log("getting rates for",len(ids), ids)

        self.rates = {}
        if initial:
            pb = 32
            pb_update_per_id = 8./len(ids)
            progress_bar_update(my_address, 'Loading coingecko rates', pb)
        for idx,id in enumerate(ids):
            rows = db.select("select timestamp, rate from rates where id='"+id+"' order by timestamp ASC")
            rate_table = sortedcontainers.SortedDict()
            for row in rows:
                rate_table[row[0]] = row[1]
            if initial:
                pb += pb_update_per_id
                if idx % 10 == 0:
                    progress_bar_update(my_address, 'Loading coingecko rates', pb)
            self.rates[id] = rate_table

        db.disconnect()
        if self.verbose:
            log("init_from_db timing",time.time()-t)
        self.initialized = True

    # def get_rates(self):
    #     db = SQLite('db')
    #     rates = {}
    #     for contract, symbol_data in self.contracts_map.items():
    #         pass
    #     db.disconnect()


    # def get_rates(self):
    #     print("Getting coingecko rates",len(self.contracts_map))
    #     try:
    #         self.rates = pickle.load(open("data/coingecko_rates", "rb"))
    #         # log(self.rates)
    #         return
    #     except:
    #         print("Failed to load symbols from HD")
    #
    #
    #
    #     rates = {}
    #     for contract, symbol_data in self.contracts_map.items():
    #         coingecko_id = symbol_data['id']
    #
    #         if coingecko_id not in rates:
    #             url = 'https://api.coingecko.com/api/v3/coins/' + coingecko_id + '/market_chart?vs_currency=usd&days=max&interval=daily'
    #             resp = requests.get(url)
    #             data = resp.json()
    #             rate_table = sortedcontainers.SortedDict()
    #             for entry in data['prices']:
    #                 rate_table[entry[0] // 1000] = entry[1]
    #             rates[coingecko_id] = rate_table
    #             # pprint.pprint(dict(rate_table))
    #
    #         time.sleep(0.3)
    #     self.rates = rates
    #     pickle.dump(rates, open("data/coingecko_rates", "wb"))


    def download_symbols_to_db(self):
        db = SQLite('db',do_logging=False)
        db.create_table('symbols','id PRIMARY KEY, symbol, name, rates_acquired INTEGER DEFAULT 0',drop=True)
        db.create_table('platforms', 'id, platform, address', drop=True)
        db.create_index('platforms_i1', 'platforms', 'id')
        db.create_index('platforms_i1', 'platforms', 'platform, address')

        url = 'https://api.coingecko.com/api/v3/coins/list?include_platform=true'
        resp = requests.get(url)
        data = resp.json()
        for entry in data:
            id = entry['id']
            values = [id,entry['symbol'],entry['name'], 0]
            db.insert_kw('symbols',values=values)
            for platform,address in entry['platforms'].items():
                if address is not None and len(address) > 10 and platform is not None and len(platform) > 1:
                    values = [id,platform,address]
                    db.insert_kw('platforms', values=values)
            db.commit()
            print(id)
        db.disconnect()

    def download_all_coingecko_rates(self,reset=False):
        tstart = time.time()
        db = SQLite('db',do_logging=False)
        db.create_table('rates', 'id, timestamp INTEGER, rate NUMERIC', drop=False)
        db.create_index('rates_i1', 'rates', 'id, timestamp', unique=True)
        db.create_index('rates_i2', 'rates', 'id')
        if reset:
            db.query('UPDATE symbols SET rates_acquired = 0')



        print("Finding recent updates")
        latest = db.select('select id, max(timestamp) from rates group by id order by id ASC')
        print("Done finding recent updates",time.time()-tstart)
        update_map = {}
        for idx, row in enumerate(latest):
            update_map[row[0]] = row[1]

        bases = set()
        for platform_info in list(self.chain_mapping.values()):
            bases.add(platform_info['id'])
        bases = "','".join(list(bases))
        rows = db.select("SELECT id FROM symbols WHERE rates_acquired == 0 and (id in (SELECT id from platforms) or id in ('" + bases + "')) ORDER BY id ASC")
        for idx, row in enumerate(rows):
            id = row[0]
            if id in update_map:
                ts = update_map[id]
                print("Downloading recent rates for " + id,"starting from",ts,str(idx)+"/"+str(len(rows)))
                rv = self.download_coingecko_rates(db, id, starting_from=ts)
            else:
                print("Downloading recent rates for " + id, str(idx) + "/" + str(len(rows)))
                rv = self.download_coingecko_rates(db, id)
            if rv:
                db.query('UPDATE symbols SET rates_acquired = 1 WHERE id == "'+id+'"')
                db.commit()
            time.sleep(1)


        # processed = set()
        # for idx,row in enumerate(latest):
        #     id, ts = row
        #     print("Downloading recent rates for " + id,"starting from",ts,str(idx)+"/"+str(len(latest)))
        #     rv = self.download_coingecko_rates(db, id, starting_from=ts)
        #     if rv:
        #         db.query('UPDATE symbols SET rates_acquired = 1 WHERE id == "'+id+'"')
        #         db.commit()
        #     time.sleep(1)
        #     processed.add(id)
        #
        #
        #
        #
        # rows = db.select("SELECT id FROM symbols ORDER BY id ASC")
        # all = set()
        # for idx,row in enumerate(rows):
        #     id = row[0]
        #     all.add(id)
        #
        # remaining = list(all - processed)
        # for idx, id in enumerate(remaining):
        #     print("Downloading rates for " + id,str(idx)+"/"+str(len(remaining)))
        #     rv= self.download_coingecko_rates(db, id)
        #     if rv:
        #         db.query('UPDATE symbols SET rates_acquired = 1 WHERE id == "'+id+'"')
        #         db.commit()
        #     time.sleep(1)
        db.disconnect()
        print("Total time",time.time()-tstart)

    def download_coingecko_rates(self, db, id, starting_from=1514764800):

        session = requests.session()
        end = int(time.time())
        offset = 86400 * 90
        while end >= starting_from:
            start = end - offset
            print(start, end)
            url = "https://api.coingecko.com/api/v3/coins/" + id + "/market_chart/range?vs_currency=usd&from=" + str(start) + "&to=" + str(end)
            data = session.get(url)
            try:
                data = data.json()
            except:
                print("Can't json coingecko response",data.content)
                return 0
            if 'prices' not in data:
                print("Couldn't find prices in data", data)
                return 0
            prices = data['prices']
            for ts, price in prices:
                db.insert_kw('rates', values=[id, int(ts / 1000), price])
            db.commit()

            if len(prices) == 0:
                break
            time.sleep(1)
            end = start
        return 1



    def lookup_name(self,contract):
        if contract not in self.contracts_map:
            return contract
        return self.contracts_map[contract]['symbol']


    def add_rate(self, contract, ts, rate, certainty, rate_source):
        if self.verbose:
            log("coingecko add shortcut 0", "add_rate", contract, ts, rate, certainty, rate_source)
            log("coingecko adding rate",contract,ts,rate, certainty, rate_source)
        ts = int(ts)
        self.shortcut_rates[contract][ts] = certainty, rate, rate_source
        if contract not in self.inferred_rates:
            self.inferred_rates[contract] = sortedcontainers.SortedDict()
        self.inferred_rates[contract][ts] = rate

    def lookup_rate(self,contract,ts):
        t = time.time()
        verbose = self.verbose
        if verbose:
            log('coingecko rate lookup',contract,ts)
        found = 0
        source = 'unknown'
        if contract == 'USD':
            return 1, 1, 'usd'

        try:
            rv = self.shortcut_rates[contract][ts]
            self.shortcut_hits += 1
            if verbose:
                log('shortcut hit', rv)
            return rv
        except:
            pass


        good = 1
        ts = int(ts)
        # assert contract in self.contracts_map
        if contract not in self.contracts_map:
            if verbose:
                log("Bad rate for in lookup", contract, ts, "contract is not in the map")
            if contract in self.inferred_rates:
                if verbose:
                    log("Contract present in inferred rates")
                rates_table = self.inferred_rates[contract]

                first = rates_table.keys()[0]
                last = rates_table.keys()[-1]
                source = "inferred"
                if ts < first:
                    good = 0
                    rate = rates_table[first]
                    source += ', before first'
                elif ts > last:
                    good = 0.5
                    rate = rates_table[last]
                    source += ', after last'
                else:
                    idx = rates_table.bisect_left(ts)
                    ts_lookup = rates_table.keys()[idx - 1]
                    rate = rates_table[ts_lookup]
                    good = 0.5

                self.time_spent_looking_up += (time.time() - t)
                if self.verbose:
                    log("coingecko add shortcut 1",contract,ts,good,rate,source)
                self.shortcut_rates[contract][ts] = (good, rate, source)
                return good, rate, source
            return 0, None, None

        coingecko_id = self.contracts_map[contract]['id']
        try:
            rates_table = self.rates[coingecko_id]
        except:
            log("EXCEPTION, could not find rates table",contract, coingecko_id, traceback.format_exc())
            return 0, None, None

        if ts in rates_table:
            if verbose:
                log("Exact rate for in lookup", contract, ts)
            rate = rates_table[ts]
            good = 2
            source = 'exact'
        else:

            times = list(rates_table.keys())
            try:
                first = min(times)
                last = max(times)
            except:
                log("failed rate lookup minmax",coingecko_id, contract)
                self.time_spent_looking_up += (time.time() - t)
                self.shortcut_rates[contract][ts] = (0,None, "missing")
                return 0, None, None


            if ts < first:
                if verbose:
                    log("Bad rate for in lookup",contract,coingecko_id,ts,"is smaller than first timestamp",first)
                good = 0.3
                found = 1
                rate = rates_table[first]
                source = 'before first'

            if ts > last:
                if verbose:
                    log("Bad rate for in lookup", contract, coingecko_id, ts, "is larger than last timestamp", last)
                good = 0.3
                found = 1
                rate = rates_table[last]
                source = 'after last'



            if not found:
                idx = rates_table.bisect_left(ts)
                ts_bottom = rates_table.keys()[idx-1]
                ts_top = rates_table.keys()[idx]
                bot_fraction = 1-(ts - ts_bottom) / (ts_top - ts_bottom)
                top_fraction = 1-(ts_top - ts) / (ts_top - ts_bottom)


                try:
                    rate = rates_table[ts_bottom] * bot_fraction + rates_table[ts_top] * top_fraction
                    found = True
                    source = 'normal'
                except:
                    log("EXCEPTION, EXITING IN lookup_rate",contract,coingecko_id,ts, traceback.format_exc())
                    log(first,last,ts_bottom,ts_top)
                    # pprint.pprint(rates_table)
                    return 0, None, None



            # print("Looking up rate for ", contract, "at", ts,rate)
        self.time_spent_looking_up += (time.time() - t)
        self.shortcut_rates[contract][ts] = (good, rate, source)
        if self.verbose:
            log("coingecko add shortcut 2", source, contract, ts, good, rate)
        return good, rate, source


