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
from .chain import Chain

class Coingecko:
    def __init__(self, verbose=False,use_pro=True):
        self.contracts_map = defaultdict(dict)

        # self.symbol_map = defaultdict(dict)
        self.rates = None
        self.shortcut_rates = defaultdict(dict)
        self.inferred_rates = {}
        self.shortcut_hits = 0
        self.verbose= verbose
        self.initialized = False

        self.timings = defaultdict(float)

        # self.chain_mapping = {
        #     'ETH':{'platform':'ethereum','id':'ethereum'},
        #     'Polygon':{'platform':'polygon-pos','id':'matic-network'},
        #     'Arbitrum':{'platform':'arbitrum-one','id':'ethereum'},
        #     'Avalanche':{'platform':'avalanche','id':'avalanche-2'},
        #     'Fantom':{'platform':'fantom','id':'fantom'},
        #     'BSC':{'platform':'binance-smart-chain','id':'binancecoin'},
        #     'HECO':{'platform':'huobi-token','id':'huobi-token'},
        #     'Moonriver':{'platform':'moonriver','id':'moonriver'},
        #     'Solana':{'platform':'solana','id':'solana'},
        #     'Cronos':{'platform':'cronos','id':'crypto-com-chain'},
        #     'Gnosis':{'platform':'xdai','id':'xdai'},
        #     'Optimism':{'platform':'optimistic-ethereum','id':'ethereum'},
        #     'Celo':{'platform':'celo','id':'celo'},
        #     'ETC':{'platform':'ethereum-classic','id':'ethereum-classic'},
        #     'Chiliz':{'platform':'chiliz','id':'ethereum'},
        #     'Oasis': {'platform': 'oasis', 'id': 'oasis-network'},
        #     'Doge':{'platform':'dogechain','id':'dogecoin'},
        #     'Songbird': {'platform': 'songbird', 'id': 'songbird'},
        #     'Metis': {'platform': 'metis-andromeda', 'id': 'metis-token'},
        #     'Boba': {'platform': 'boba', 'id': 'boba-network'},
        #     'SXNetwork': {'platform': 'sx-network', 'id': 'sx-network'},
        #     'Astar': {'platform': 'astar', 'id': 'astar'},
        #     'Evmos': {'platform': 'evmos', 'id': 'evmos'},
        #     'Kava': {'platform': 'kava', 'id': 'kava'},
        #     'Canto': {'platform': 'canto', 'id': 'canto'},
        #     'Aurora': {'platform': 'aurora', 'id': 'ethereum'},
        #     'Step': {'platform': 'step-network', 'id': 'step-app-fitfi'},
        # }

        self.chain_mapping = {}
        self.base_ids = set()
        for chain_name, conf in Chain.CONFIG.items():
            id = platform = chain_name.lower()
            if 'coingecko_id' in conf:
                id = conf['coingecko_id']
            if 'coingecko_platform' in conf:
                platform = conf['coingecko_platform']
            self.chain_mapping[chain_name] = {'platform':platform,'id':id}
            self.base_ids.add(id)


        self.reverse_chain_mapping = {}
        for k,v in self.chain_mapping.items():
            self.reverse_chain_mapping[v['platform']] = k



        self.custom_platform_mapping = {
            'huobi-token': {
                'usd-coin': ('0X9362BBEF4B8313A8AA9F0C9808B80577AA26B73B', 'USDC'),
                'dai': ('0X3D760A45D0887DFD89A2F5385A236B29CB46ED2A', 'DAI'),
            },
            'fantom': {
                'tether':('0x049d68029688eabf473097a2fc38ef61633a3c7a', 'USDT')
            },
            'oasis': {
                'usd-coin': ('0x94fbffe5698db6f54d6ca524dbe673a7729014be','USDC')
            },
            'dogechain': {
                'tether': ('0xE3F5a90F9cb311505cd691a46596599aA1A0AD7D', 'USDT')
            }
        }

        self.ignore = ['curve-fi-amdai-amusdc-amusdt']

        self.use_pro = use_pro
        self.api_key = os.environ.get('api_key_coingecko')




    def dump(self,user):
        rates_dump_file = open('data/users/' + user.address +"/rates", "wb")
        pickle.dump(self, rates_dump_file)
        rates_dump_file.close()

    @classmethod
    def init_from_cache(cls,user):
        C = pickle.load(open('data/users/' + user.address+"/rates", "rb"))
        log('coingecko ifc contracts_map',C.contracts_map,filename='lookups.txt')
        if len(C.contracts_map) == 0:
            return C
        chains = list(C.contracts_map.keys())
        c1 = chains[0]
        ids = list(C.contracts_map[c1].keys())
        id1 = C.contracts_map[c1][ids[0]]['id'] #raises an exception if format is wrong

        inverse_contract_map = {}
        #self.contracts_map[chain_name][address.lower()] = {'id': id, 'symbol': symbol}
        for chain_name in C.contracts_map:
            if chain_name not in inverse_contract_map:
                inverse_contract_map[chain_name] = {}
            for contract, data in C.contracts_map[chain_name].items():
                id = data['id']
                inverse_contract_map[chain_name][id] = contract
        C.inverse_contract_map = inverse_contract_map

        # eth = C.contracts_map['ETH']['eth']['id'] #raises an exception if format is wrong
        # assert eth == 'ethereum'

        # log("coingecko initialized",C.initialized)
        # log("coingecko BUSD rate",C.lookup_rate('0xe9e7cea3dedca5984780bafc599bd69add087d56',1623272501))
        return C

    @classmethod
    def find_range(cls,ts, ranges):
        for idx, (start, end) in enumerate(ranges):
            try:
                if ts < start:
                    return False, idx
            except:
                log("WTF",ts,ranges)
                exit(1)
            if ts <= end:
                return True, idx

        return False, len(ranges)

    @classmethod
    def merge_ranges(cls,ranges,start,end):
        if len(ranges) == 0:
            ranges.append([start, end])
        else:
            start_in_range, start_range_idx = Coingecko.find_range(start, ranges)
            end_in_range, end_range_idx = Coingecko.find_range(end, ranges)
            log("indexes", start_range_idx, end_range_idx, filename='coingecko2.txt')
            if not start_in_range and not end_in_range:  # add new
                ranges.insert(start_range_idx, [start, end])
                del ranges[start_range_idx + 1:end_range_idx + 1]
            elif start_in_range and not end_in_range:  # extend start's range to include the end
                ranges[start_range_idx][1] = end
                del ranges[start_range_idx + 1:end_range_idx]
            elif not start_in_range and end_in_range:  # extend end's range to include the left
                ranges[end_range_idx][0] = start
                del ranges[start_range_idx:end_range_idx]
            elif start_range_idx != end_range_idx:  # merge two
                ranges[start_range_idx][1] = ranges[end_range_idx][1]
                del ranges[start_range_idx + 1:end_range_idx + 1]
        return ranges

    def make_contracts_map(self):
        db = SQLite('db', do_logging=False, read_only=True)
        Q = "select symbols.id, symbol, name, platform, address from symbols LEFT OUTER JOIN platforms ON symbols.id = platforms.id"
        rows = db.select(Q)
        db.disconnect()
        if len(rows) == 0:
            self.download_symbols_to_db(drop=False)
            db = SQLite('db', do_logging=False, read_only=True)
            Q = "select symbols.id, symbol, name, platform, address from symbols LEFT OUTER JOIN platforms ON symbols.id = platforms.id"
            rows = db.select(Q)
            db.disconnect()

        valid_ids = set()
        for row in rows:
            id, symbol, name, platform, address = row
            valid_ids.add(id)

            if platform in self.reverse_chain_mapping:
                chain_name = self.reverse_chain_mapping[platform]
                self.contracts_map[chain_name][address.lower()] = {'id': id, 'symbol': symbol}

        for platform, mapping in self.custom_platform_mapping.items():
            chain_name = self.reverse_chain_mapping[platform]
            for id, tuple in mapping.items():
                self.contracts_map[chain_name][tuple[0].lower()] = {'id': id, 'symbol': tuple[1]}

        for chain_name in self.chain_mapping:
            main_id = self.chain_mapping[chain_name]['id']
            conf = Chain.CONFIG[chain_name]
            base_asset = chain_name
            if 'base_asset' in conf:
                base_asset = conf['base_asset']
            self.contracts_map[chain_name][base_asset.lower()] = {'id': main_id, 'symbol': base_asset}
        self.valid_ids = valid_ids



    def init_from_db_2(self, chain_dict,needed_token_times, progress_bar=None):
        log('needed_token_times',needed_token_times,filename='coingecko2.txt')
        pb_alloc = 17.

        db = SQLite('db', do_logging=False, read_only=True)
        # Q = "select symbols.id, symbol, name, platform, address from symbols LEFT OUTER JOIN platforms ON symbols.id = platforms.id"
        #
        # rows = db.select(Q)




        id_times = {}
        for coingecko_id in needed_token_times:
            id_times[coingecko_id] = {'needed':needed_token_times[coingecko_id]}


        rq_cnt = 0
        ld_cnt = 0
        for id,id_data in id_times.items():
            if len(id_data['needed']) == 0:
                continue
            ld_cnt += 1


        idx = 0
        if self.rates is None:
            self.rates = {}
        for id,id_data in id_times.items():

            if len(id_data['needed']) == 0:
                continue

            if progress_bar:
                idx += 1
                progress_bar.update('Loading coingecko rates: ' + str(idx) + "/" + str(ld_cnt), pb_alloc * 0.3 / ld_cnt)
            to_download, ranges = self.load_rates(db, id, id_data['needed'])
            id_data['ranges'] = ranges
            if len(to_download) > 0:
                id_data['to_download'] = to_download
                rq_cnt += 1

        db.disconnect()


        if rq_cnt > 0:
            idx = 0
            db = SQLite('db', do_logging=False, read_only=False)
            # session = requests.session()
            for id, id_data in id_times.items():
                if 'to_download' not in id_data or len(id_data['to_download']) == 0 or id not in self.valid_ids:
                    continue
                to_download = id_data['to_download']
                ranges = id_data['ranges']
                self.download_rates(db, id, to_download, ranges)
                if progress_bar:
                    idx += 1
                    progress_bar.update('Downloading coingecko rates [' + id + '], ' + str(idx) + "/" + str(rq_cnt), pb_alloc * 0.7 / rq_cnt)

                # rate_table = rate_tables[id]
                # for start in to_download:
                #     end = min(start+d90,int(time.time()))
                #     idx += 1
                #     if progress_bar:
                #         progress_bar.update('Downloading coingecko rates ['+id+']: ' + str(idx) + "/" + str(rq_cnt), pb_alloc * 0.7 / rq_cnt)
                #     if self.use_pro:
                #         url = "https://pro-api.coingecko.com/api/v3/coins/" + id + "/market_chart/range?vs_currency=usd&from=" + str(start) + "&to=" + str(end) + "&x_cg_pro_api_key=" + self.api_key
                #         sleep = 0.2
                #     else:
                #         url = "https://api.coingecko.com/api/v3/coins/" + id + "/market_chart/range?vs_currency=usd&from=" + str(start) + "&to=" + str(end)
                #         sleep = 2
                #     log("Calling",url,filename='coingecko2.txt')
                #     time.sleep(sleep)
                #     try:
                #         data = session.get(url, timeout=20)
                #     except:
                #         log_error("Couldn't connect to coingecko",id,start)
                #         continue
                #     try:
                #         data = data.json()
                #     except:
                #         log_error("Couldn't parse coingecko response",id,start)
                #         continue
                #     if 'prices' not in data:
                #         log_error("Couldn't find price data",id,start)
                #         continue
                #     prices = data['prices']
                #
                #     for ts, price in prices:
                #         ts = int(ts / 1000)
                #         db.insert_kw('rates', values=[id, ts, price],ignore=True)
                #         rate_table[ts] = price
                #
                #     #merge ranges
                #     log('merging ranges, current',ranges,'adding',start,end,filename='coingecko2.txt')
                #     ranges = Coingecko.merge_ranges(ranges,start,end)
                #     log('merged ranges, new', ranges,filename='coingecko2.txt')
                # db.query("DELETE FROM rates_ranges WHERE id='"+id+"'")
                # for range in ranges:
                #     db.insert_kw("rates_ranges",id=id,start=range[0],end=range[1])
                #
                # db.commit()
            db.disconnect()
        self.initialized = True


    def load_rates(self, db, id, needed_times):
        d90 = 86400 * 90
        to_download = []
        rows = db.select("SELECT start,end FROM rates_ranges WHERE id = '" + id + "' ORDER BY start ASC")
        ranges = []
        for row in rows:
            ranges.append([row[0], row[1]])
        needed_times = sorted(list(needed_times))

        t = time.time()
        # rows = db.select("select timestamp, rate from rates where id='" + id + "' order by timestamp ASC")
        rows = db.select("select timestamp, rate from rates where id='" + id + "'",raw=True)
        log('rate_table population time, select', id, len(rows), time.time() - t, filename='coingecko2.txt')
        if id not in self.rates:
            rate_table = sortedcontainers.SortedDict()
        else:
            rate_table = self.rates[id]
        self.rates[id] = rate_table
        if len(rows):
            t = time.time()
            # for row in rows:
            #     rate_table[row[0]] = row[1]
            rate_table.update(rows)
            log('rate_table population time, pop',id,len(rows),time.time()-t,filename='coingecko2.txt')

        for ts in needed_times:

            in_range, range_idx = Coingecko.find_range(ts, ranges)
            log(id, "checking time", ts, "in_range",in_range,(len(to_download) == 0 or ts > to_download[-1] + d90),ts < time.time() - 3600,filename='coingecko2.txt')
            if not in_range and (len(to_download) == 0 or ts > to_download[-1] + d90) and ts < time.time() - 3600:
                log(id, "adding to download")
                to_download.append(ts)

        log('needed', id, needed_times, filename='coingecko2.txt')
        log('to_download', id, to_download, filename='coingecko2.txt')
        return to_download, ranges


    def download_rates(self,db, id, to_download, ranges):
        d90 = 86400 * 90

        rate_table = self.rates[id]
        for start in to_download:
            end = min(start + d90, int(time.time()))
            if self.use_pro:
                url = "https://pro-api.coingecko.com/api/v3/coins/" + id + "/market_chart/range?vs_currency=usd&from=" + str(start) + "&to=" + str(end) + "&x_cg_pro_api_key=" + self.api_key
                sleep = 0.2
            else:
                url = "https://api.coingecko.com/api/v3/coins/" + id + "/market_chart/range?vs_currency=usd&from=" + str(start) + "&to=" + str(end)
                sleep = 2
            log("Calling", url, filename='coingecko2.txt')
            time.sleep(sleep)
            try:
                data = requests.get(url, timeout=20)
            except:
                log_error("Couldn't connect to coingecko", id, start)
                continue
            try:
                data = data.json()
            except:
                log_error("Couldn't parse coingecko response", id, start)
                continue
            if 'prices' not in data:
                log_error("Couldn't find price data", id, start)
                continue

            prices = data['prices']

            for ts, price in prices:
                ts = int(ts / 1000)
                db.insert_kw('rates', values=[id, ts, price], ignore=True)
                rate_table[ts] = price

            # merge ranges
            log('merging ranges, current', ranges, 'adding', start, end, filename='coingecko2.txt')
            ranges = Coingecko.merge_ranges(ranges, start, end)
            log('merged ranges, new', ranges, filename='coingecko2.txt')
        db.query("DELETE FROM rates_ranges WHERE id='" + id + "'")
        for range in ranges:
            db.insert_kw("rates_ranges", id=id, start=range[0], end=range[1])

        db.commit()

    # def init_from_db(self, chain_dict, cp_dict, progress_bar=None,retrieve_latest=True):
    #     pb_alloc = 17.
    #     t = time.time()
    #     # chain_name = chain.name
    #     # progress_bar = chain.progress_bar
    #     if progress_bar:
    #         progress_bar.update('Loading coingecko rates', 0)
    #     t = time.time()
    #
    #
    #     ids = {}
    #     log('coingecko_t0', '0', filename='coingecko.txt')
    #     db = SQLite('db',do_logging=False)
    #     log('coingecko_t1', time.time() - t, filename='coingecko.txt')
    #     platform_list = []
    #     for chain_name,chain_data in chain_dict.items():
    #         chain = chain_data['chain']
    #
    #         platform_list.append(self.chain_mapping[chain_name]['platform'])
    #         main_id = self.chain_mapping[chain_name]['id']
    #         main_cp = chain_name+":"+chain.main_asset
    #         if main_cp in cp_dict:
    #             ids[main_id] = cp_dict[main_cp]
    #             if chain.wrapper is not None and chain_name+":"+chain.wrapper not in cp_dict:
    #                 cp_dict[chain_name+":"+chain.wrapper] = cp_dict[main_cp]
    #
    #         self.contracts_map[chain_name][chain.main_asset.lower()] = {'id': main_id, 'symbol': chain.main_asset}
    #     log('coingecko_t2', time.time() - t, filename='timing.txt')
    #
    #     Q = "select symbols.id, symbol, name, platform, address from symbols, platforms where symbols.id = platforms.id and platform IN "+sql_in(platform_list)
    #     log('coingecko_q',Q,filename='coingecko.txt')
    #     rows = db.select(Q)
    #     log('coingecko_t3', time.time() - t, filename='coingecko.txt')
    #
    #     for row in rows:
    #         id,symbol,name,platform,address = row
    #         chain_name = self.reverse_chain_mapping[platform]
    #         self.contracts_map[chain_name][address.lower()] = {'id':id,'symbol':symbol}
    #
    #     for platform, mapping in self.custom_platform_mapping.items():
    #         chain_name = self.reverse_chain_mapping[platform]
    #         for id, tuple in mapping.items():
    #             self.contracts_map[chain_name][tuple[0].lower()] = {'id':id,'symbol':tuple[1]}
    #     log('coingecko_t4', time.time() - t, filename='coingecko.txt')
    #
    #
    #     # log("Contracts map",self.contracts_map)
    #     # exit(0)
    #
    #     # pprint.pprint(self.contracts_map)
    #     # if self.verbose:
    #     #     log("map",self.contracts_map)
    #     if self.verbose:
    #         log("Looking for rates",list(cp_dict.items()))
    #
    #     latest = 0
    #     for cp_pair,last_needed in cp_dict.items():
    #         chain_name,contract = cp_pair.split(":")
    #         contract = contract.lower()
    #         if contract in self.contracts_map[chain_name]:
    #             coingecko_id = self.contracts_map[chain_name][contract]['id']
    #             if coingecko_id not in ids or ids[coingecko_id] < last_needed:
    #                 ids[coingecko_id] = last_needed
    #         if last_needed > latest:
    #             latest = last_needed
    #
    #
    #
    #     if self.verbose:
    #         log("getting rates for",len(ids), ids)
    #
    #     self.rates = {}
    #     log('coingecko_t5', time.time() - t, filename='coingecko.txt')
    #
    #     need_to_retrieve = []
    #     rq_cnt = 0
    #     for idx, (id, latest_needed) in enumerate(ids.items()):
    #         if retrieve_latest and latest_needed is not None:
    #             latest_available = 1514764800
    #             rows = db.select("select MAX(timestamp) from rates where id='"+id+"'")
    #             if len(rows) > 0 and rows[0][0] is not None:
    #                 latest_available = int(rows[0][0])
    #
    #             if latest_available < latest_needed-3600:
    #                 calls_needed = (latest_needed-latest_available)/(86400*90)
    #                 need_to_retrieve.append([id,latest_available,calls_needed])
    #                 rq_cnt += calls_needed
    #
    #     for idx,entry in enumerate(need_to_retrieve):
    #         id, latest_available, calls_needed = entry
    #         try:
    #             self.download_coingecko_rates(db, id, starting_from=latest_available)
    #             # pass
    #         except:
    #             log_error("Failed to download coingecko rates", id, latest_available)
    #         if progress_bar:
    #             progress_bar.update('Downloading coingecko data ['+str(id)+"] "+str(idx)+"/"+str(len(need_to_retrieve)), pb_alloc*0.7*calls_needed/rq_cnt)
    #
    #     for idx,(id,latest_needed) in enumerate(ids.items()):
    #         if progress_bar:
    #             progress_bar.update('Loading coingecko rates: '+str(idx)+"/"+str(len(ids)), pb_alloc*0.3/len(ids))
    #         rows = db.select("select timestamp, rate from rates where id='"+id+"' order by timestamp ASC")
    #         rate_table = sortedcontainers.SortedDict()
    #         for row in rows:
    #             rate_table[row[0]] = row[1]
    #
    #         log('loaded rate table',id,len(rate_table))
    #         self.rates[id] = rate_table
    #
    #     db.disconnect()
    #     if self.verbose:
    #         log("init_from_db timing",time.time()-t)
    #     self.initialized = True

        # pprint.pprint(dict(self.contracts_map))
        # exit(1)

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


    def download_symbols_to_db(self,drop=False, progress_bar=None):

        pb_alloc = 2.
        if drop:
            db = SQLite('db', do_logging=False)
            db.create_table('symbols','id PRIMARY KEY, symbol, name',drop=drop)
            db.create_table('platforms', 'id, platform, address', drop=drop)
            db.create_index('platforms_i1', 'platforms', 'id')
            db.create_index('platforms_i2', 'platforms', 'platform, address',unique=True)
            db.disconnect()

        if self.use_pro:
            url = 'https://pro-api.coingecko.com/api/v3/coins/list?include_platform=true&x_cg_pro_api_key='+self.api_key
        else:
            url = 'https://api.coingecko.com/api/v3/coins/list?include_platform=true'
        try:
            resp = requests.get(url,timeout=10)
            data = resp.json()
        except:
            log("Failed to download coingecko symbols",traceback.format_exc())
            return
        if progress_bar:
            progress_bar.update('Loading coingecko symbols',1)
        db = SQLite('db', do_logging=False)
        try:
            for idx,entry in enumerate(data):
                id = entry['id']
                values = [id,entry['symbol'],entry['name']]
                db.insert_kw('symbols',values=values,ignore=not drop)
                for platform,address in entry['platforms'].items():
                    if address is not None and len(address) > 10 and platform is not None and len(platform) > 1:
                        values = [id,platform,address]
                        db.insert_kw('platforms', values=values,ignore=not drop)


                if progress_bar:
                    progress_bar.update('Loading coingecko symbols: '+str(idx)+"/"+str(len(data)), pb_alloc/len(data))
        except:
            log_error("Failed to insert coingecko symbols", traceback.format_exc())
            db.commit()
            db.disconnect()
            return
        db.commit()



        db.disconnect()

    # def download_all_coingecko_rates(self,reset=False):
    #     tstart = time.time()
    #     db = SQLite('db',do_logging=False)
    #     db.create_table('rates', 'id, timestamp INTEGER, rate NUMERIC', drop=False)
    #     db.create_index('rates_i1', 'rates', 'id, timestamp', unique=True)
    #     db.create_index('rates_i2', 'rates', 'id')
    #
    #
    #
    #
    #     print("Finding recent updates")
    #     latest = db.select('select id, max(timestamp) from rates group by id order by id ASC')
    #     print("Done finding recent updates",time.time()-tstart)
    #     update_map = {}
    #     for idx, row in enumerate(latest):
    #         update_map[row[0]] = row[1]
    #
    #     bases = set()
    #     for platform_info in list(self.chain_mapping.values()):
    #         bases.add(platform_info['id'])
    #     rows = db.select("SELECT id FROM symbols WHERE rates_acquired == 0 and (id in (SELECT id from platforms) or id in "+sql_in(list(bases))+" ORDER BY id ASC")
    #     for idx, row in enumerate(rows):
    #         id = row[0]
    #         if id in update_map:
    #             ts = update_map[id]
    #             print("Downloading recent rates for " + id,"starting from",ts,str(idx)+"/"+str(len(rows)))
    #             rv = self.download_coingecko_rates(db, id, starting_from=ts)
    #         else:
    #             print("Downloading recent rates for " + id, str(idx) + "/" + str(len(rows)))
    #             rv = self.download_coingecko_rates(db, id)
    #         if rv:
    #             db.query('UPDATE symbols SET rates_acquired = 1 WHERE id == "'+id+'"')
    #             db.commit()
    #         time.sleep(1)


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
        # db.disconnect()
        # print("Total time",time.time()-tstart)

    # def download_coingecko_rates(self, db, id, starting_from=1514764800):
    #
    #     if self.use_pro:
    #         sleep = 0.2
    #     else:
    #         sleep = 2
    #
    #     session = requests.session()
    #     end = int(time.time())
    #     offset = 86400 * 90
    #     while end >= starting_from:
    #         start = end - offset
    #         time.sleep(sleep)
    #         if self.use_pro:
    #             url = "https://pro-api.coingecko.com/api/v3/coins/" + id + "/market_chart/range?vs_currency=usd&from=" + str(start) + "&to=" + str(end)+"&x_cg_pro_api_key="+self.api_key
    #         else:
    #             url = "https://api.coingecko.com/api/v3/coins/" + id + "/market_chart/range?vs_currency=usd&from=" + str(start) + "&to=" + str(end)
    #         try:
    #             data = session.get(url,timeout=20)
    #         except:
    #             log("Couldn't connect to coingecko")
    #             return 0
    #         try:
    #             data = data.json()
    #         except:
    #             log("Can't json coingecko response",data.content)
    #             return 0
    #         if 'prices' not in data:
    #             log("Couldn't find prices in data", data)
    #             return 0
    #         prices = data['prices']
    #         for ts, price in prices:
    #             db.insert_kw('rates', values=[id, int(ts / 1000), price])
    #         db.commit()
    #
    #         if len(prices) == 0:
    #             break
    #
    #
    #         end = start
    #     return 1



    def lookup_id(self, chain_name, contract):
        contract = contract.lower()
        try:
            return self.contracts_map[chain_name][contract]['id']
        except:
            return None


    def add_rate(self, chain_name, contract, ts, rate, certainty, rate_source):

        coingecko_id = self.lookup_id(chain_name, contract)

        if coingecko_id is None:
            coingecko_id_or_cp = chain_name +":" + contract
        else:
            coingecko_id_or_cp = coingecko_id
        log("Adding rate", chain_name, contract, ts, rate, certainty, rate_source, coingecko_id_or_cp)
        if self.verbose:
            log("coingecko add shortcut 0", "add_rate", coingecko_id_or_cp, ts, rate, certainty, rate_source)
            log("coingecko adding rate",coingecko_id_or_cp,ts,rate, certainty, rate_source)
        ts = int(ts)
        self.shortcut_rates[coingecko_id_or_cp][ts] = certainty, rate, rate_source
        if coingecko_id_or_cp not in self.inferred_rates:
            self.inferred_rates[coingecko_id_or_cp] = sortedcontainers.SortedDict()
        self.inferred_rates[coingecko_id_or_cp][ts] = rate



    def lookup_rate(self,chain_name,contract,ts):

        coingecko_id = self.lookup_id(chain_name,contract)
        if coingecko_id is None or (hasattr(self,'ignore') and coingecko_id in self.ignore):
            coingecko_id_or_cp = chain_name+":"+contract
        else:
            coingecko_id_or_cp = coingecko_id

        rv = self.lookup_rate_by_id(coingecko_id_or_cp,ts)
        # if contract == '0xdaf66c0b7e8e2fc76b15b07ad25ee58e04a66796':
        # log("lookup_rate_by_id", chain_name, contract, coingecko_id_or_cp, rv)
        return rv

    def lookup_rate_by_id(self,coingecko_id_or_cp,ts):
        t = time.time()
        verbose = self.verbose
        log('coingecko rate lookup',coingecko_id_or_cp,ts,filename='lookups.txt')
        found = 0
        source = 'unknown'
        # if coingecko_id_or_cp == 'FIAT':
        #     return 1, 1, 'fiat'

        try:
            rv = self.shortcut_rates[coingecko_id_or_cp][ts]
            self.shortcut_hits += 1
            log('shortcut hit', rv,filename='lookups.txt')
            return rv
        except:
            pass



        good = 1
        ts = int(ts)
        # assert contract in self.contracts_map
        if coingecko_id_or_cp not in self.rates:
            t_inf = time.time()
            log("Bad rate in lookup", coingecko_id_or_cp, ts, "contract is not in the rates",filename='lookups.txt')
            if coingecko_id_or_cp in self.inferred_rates:
                cp_pair = coingecko_id_or_cp
                log("Contract present in inferred rates",filename='lookups.txt')
                rates_table = self.inferred_rates[cp_pair]

                first = rates_table.keys()[0]
                last = rates_table.keys()[-1]
                source = "inferred"
                if ts < first:
                    good = 0.5
                    rate = rates_table[first]
                    if ts < first - 3600:
                        good = 0
                        source += ', before first '+str(first)
                elif ts > last:
                    rate = rates_table[last]
                    good = 0.5
                    if ts > last + 3600:
                        source += ', after last '+str(last)
                else:
                    idx = rates_table.bisect_left(ts)
                    ts_lookup = rates_table.keys()[idx - 1]
                    rate = rates_table[ts_lookup]
                    good = 0.5

                # self.time_spent_looking_up += (time.time() - t)
                log("coingecko add shortcut 1",cp_pair,ts,good,rate,source,filename='lookups.txt')
                self.shortcut_rates[cp_pair][ts] = (good, rate, source)
                return good, rate, source
            return 0, None, None

        # coingecko_id = self.contracts_map[chain_name][contract]['id']
        # try:
        #     rates_table = self.rates[coingecko_id]
        # except:
        #     log("EXCEPTION, could not find rates table",coingecko_id, traceback.format_exc())
        #     return 0, None, None
        coingecko_id = coingecko_id_or_cp
        rates_table = self.rates[coingecko_id]

        if ts in rates_table:
            log("Exact rate for in lookup", coingecko_id, ts,filename='lookups.txt')
            rate = rates_table[ts]
            good = 2
            source = 'exact'
        else:
            t_oob = time.time()
            # times = list(rates_table.keys())
            # if record_timing:
            #     c31 = time.time()
            #     self.timings['c31'] += (c31 - c3)
            # try:
            #     first = min(times)
            #     last = max(times)
            # except:
            #     log("failed rate lookup minmax",coingecko_id,filename='lookups.txt')
            #     # self.time_spent_looking_up += (time.time() - t)
            #     self.shortcut_rates[coingecko_id][ts] = (0,None, "missing")
            #     if record_timing:
            #         self.timings['r5'] += (time.time() - t)
            #     return 0, None, None
            # if record_timing:
            #     c32 = time.time()
            #     self.timings['c32'] += (c32 - c31)
            try:
                times = rates_table.keys()
                first = times[0]
                last = times[-1]
            except:
                log("failed rate lookup minmax",coingecko_id,ts)
                # self.time_spent_looking_up += (time.time() - t)
                self.shortcut_rates[coingecko_id][ts] = (0,None, "missing")
                return 0, None, None


            if ts < first:
                found = 1
                rate = rates_table[first]
                if ts < first - 3600:
                    log("Bad rate for in lookup", coingecko_id, ts, "is smaller than first timestamp", first,filename='lookups.txt')
                    good = 0.3
                    source = 'before first '+str(first)
                else:
                    source = 'normal'

            if ts > last:
                found = 1
                rate = rates_table[last]
                if ts > last + 3600:
                    log("Bad rate for in lookup", coingecko_id, ts, "is larger than last timestamp", last,filename='lookups.txt')
                    good = 0.3
                    source = 'after last ' + str(last)
                else:
                    source = 'normal'



            if not found:
                tcore = time.time()
                idx = rates_table.bisect_left(ts)
                ts_bottom = rates_table.keys()[idx-1]
                ts_top = rates_table.keys()[idx]
                bot_fraction = 1-(ts - ts_bottom) / (ts_top - ts_bottom)
                top_fraction = 1-(ts_top - ts) / (ts_top - ts_bottom)


                try:
                    rate = rates_table[ts_bottom] * bot_fraction + rates_table[ts_top] * top_fraction
                    found = True
                    source = 'normal'
                    self.timings['core'] += (time.time()-tcore)
                except:
                    log("EXCEPTION, EXITING IN lookup_rate",coingecko_id,ts, traceback.format_exc(),filename='lookups.txt')
                    log(first,last,ts_bottom,ts_top,filename='lookups.txt')
                    # pprint.pprint(rates_table)
                    return 0, None, None


            # print("Looking up rate for ", contract, "at", ts,rate)
        # self.time_spent_looking_up += (time.time() - t)
        self.shortcut_rates[coingecko_id][ts] = (good, rate, source)
        log("coingecko add shortcut 2", source, coingecko_id, ts, good, rate,filename='lookups.txt')
        return good, rate, source


