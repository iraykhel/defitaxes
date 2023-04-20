import math

import requests
import traceback
from .util import *
from .sqlite import *
from .chain import Chain
import sortedcontainers


#not implemented, no good source as of 04/19/23

class Pair:
    def __init__(self,contract,chain_name,exchange,base_token,base_symbol, quote_token, quote_symbol):
        self.contract = contract
        self.chain_name = chain_name
        self.exchange = exchange
        self.base_token = base_token
        self.base_symbol = base_symbol
        self.quote_token = quote_token
        self.quote_symbol = quote_symbol

    def __str__(self):
        return self.chain_name+":"+self.contract+" "+self.exchange+":"+self.base_symbol+"<->"+self.quote_symbol

    def __repr__(self):
        return self.__str__()

    def get_cmc_id(self):
        headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'}
        chain_map = {'ether':'ethereum'}
        chain_name = self.chain.name
        if self.chain_name in chain_map:
            chain_name = chain_map[self.chain_name]


        url = "https://api.coinmarketcap.com/dexer/v3/dexer/pair-info?dexer-platform-name="+chain_name+"&address="+self.contract
        try:
            log("Calling", url, filename='dex.txt')
            resp = requests.get(url,headers=headers,timeout=5)
            print(resp.status_code, resp.content)
            data = resp.json()['data']
            pool_id = data['poolId']
            reverse_order = data['reverseOrder']
            print("pool_id",pool_id,"reverse",reverse_order)
            self.CMC_id = pool_id
            self.CMC_reverse = reverse_order
            return True
        except:
            log("Exception", url, traceback.format_exc(), filename='dex.txt')
            return False

    def download_dexscreener_rates(self,latest_available=None):
        headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
                   'cookie':'__cf_bm=pML34Jg8XyYFiXT6sgDproVJK.JQDBQdBQQKXAYyCww-1681327384-0-AXjDfpJox/KgD0h3R0Zui8hZdDY55qlHPr0hmCIc0i/XZu10mFugCm8KT5duT4YuQmaP0Eg0hvt8ik3j5BOeqOk1JrpuJEDwFpqGAOW6eFhW; __cflb=0H28vzQ7jjUXq92cxrPT8f9co4Fnb1oNKNc8BPn8GZB',
                   'accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                   'accept-language':'en-US,en;q=0.9,ru;q=0.8',
                   'sec-ch-ua':'"Google Chrome";v="111", "Not(A:Brand";v="8", "Chromium";v="111"',
                   'sec-ch-ua-platform':"Windows",
                   'sec-fetch-dest':'document',
                   'sec-fetch-mode': 'navigate',
                   'sec-fetch-user': '?1',
                   'upgrade-insecure-requests':'1'
                   }
        try:
            dexscreener_name = Chain.CONFIG[self.chain_name]['dexscreener_mapping']
            if dexscreener_name is None:
                return None
        except:
            dexscreener_name = self.chain_name.lower()
        t = str(int(time.time()*1000))
        if latest_available is None:
            start = '0'
        else:
            start = str((latest_available-3600)*1000)
        url = 'https://io.dexscreener.com/dex/chart/amm/uniswap/bars/'+dexscreener_name+'/'+self.contract+'?from='+start+'&to='+t+'&res=60&cb=10&q='+self.quote_token
        try:
            log("Calling", url, filename='dex.txt')
            print('headers',headers)
            resp = requests.get(url, headers=headers, timeout=30)
            print(resp.status_code, resp.content)
            data = resp.json()['bars']
            rates_table = sortedcontainers.SortedDict()
            for entry in data:
                ts = entry['timestamp']//1000
                rate = float(entry['closeUsd'])
                rates_table[ts] = rate
            return rates_table
        except:
            log("Exception", url, traceback.format_exc(), filename='dex.txt')
            return None


class DEX:
    def __init__(self):
        pass


    def locate_pair(self,contract, chain_name):
        headers = {'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'}
        url = 'https://www.dextools.io/shared/search/pair?query='+contract
        week = 86400*7
        try:
            log("Calling", url, filename='dex.txt')
            resp = requests.get(url,headers=headers,timeout=5)
            print(resp.status_code, resp.content)
            data = resp.json()['results']
            if len(data) == 0:
                return None

            best_candidate = None
            best_liq = 0
            earliest_creation = math.inf
            for entry in data:
                score = entry['score']['dextScore']
                if score < 0.1:
                    continue
                try:
                    liquidity = entry['metrics']['liquidity']
                except:
                    continue
                try:
                    liquidityUpdatedAt = entry['metrics']['liquidityUpdatedAt']
                    liquidityUpdatedAt_ts = int(datetime.datetime.strptime(liquidityUpdatedAt[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=datetime.timezone.utc).timestamp())
                    if time.time() - liquidityUpdatedAt_ts > week*4:
                        continue
                except:
                    continue

                try:
                    price = entry['price']
                except:
                    continue

                try:
                    if entry['id']['token'].lower() != contract.lower() and entry['id']['tokenRef'] != contract.lower():
                        continue
                except:
                    continue

                creation_time = entry['creationTime']
                creation_ts = int(datetime.datetime.strptime(creation_time[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=datetime.timezone.utc).timestamp())
                print('ent',entry['id']['pair'],creation_time,creation_ts, liquidity)
                if creation_ts < earliest_creation - week:
                    best_candidate = entry
                    best_liq = liquidity
                    earliest_creation = creation_ts
                    print("best")
                elif creation_ts < earliest_creation + week:
                    if liquidity > best_liq:
                        best_candidate = entry
                        best_liq = liquidity
                        print("best")

            if best_candidate is not None:
                fields = best_candidate['id']
                base_symbol = best_candidate['symbol']
                quote_symbol = best_candidate['symbolRef']

                pair = Pair(fields['pair'],chain_name,fields['exchange'],fields['token'],base_symbol,fields['tokenRef'],quote_symbol)

                print('final best',best_candidate)
                return pair
            else:
                return None
        except:
            log("Exception", url, traceback.format_exc(), filename='dex.txt')
            return None


