
import os
import traceback
import time
import json
import copy
import uuid
from code.coingecko import Coingecko
from code.chain import Chain
from code.solana import Solana
from code.util import log
from code.sqlite import *
import pickle
from datetime import datetime
from code.signatures import *
from code.user import User
from code.tax_calc import Calculator
from code.fiat_rates import Twelve
from code.dex_rates import *
from dotenv import load_dotenv

def update_rates_db():
    C = Coingecko()
    C.download_all_coingecko_rates(reset=False) #reset true on first run, false on runs to correct errors
    exit(0)

def update_signatures():
    S = Signatures()
    S.download_signatures_to_db(start_page=588,endid = 448521) #endid is biggest id currently in db
    exit(0)

def add_chain_to_addresses(chain_name):
    address_db = SQLite('addresses_prod')
    chain_name = chain_name.upper().replace(" ","_")
    address_db.create_table(chain_name+"_addresses","address PRIMARY KEY, tag, ancestor_address, entity,inclusion_reason",drop=False)
    address_db.create_table(chain_name + "_labels", "address, label", drop=False)
    address_db.create_index(chain_name+"_addresses_idx_1",chain_name+"_addresses","entity")
    address_db.create_index(chain_name + "_labels_idx_1", chain_name + "_labels", "address,label")
    address_db.create_index(chain_name + "_adr_idx_1", chain_name + "_addresses", "address")
    address_db.create_index(chain_name + "_adr_idx_2", chain_name + "_addresses", "ancestor_address")
    address_db.commit()
    address_db.disconnect()


def process(address, chain_name, do_import=True, do_calc=True, do_lookups=True):
    chain_names = [chain_name]
    S = Signatures()
    C = Coingecko(verbose=False)
    user = User(address, do_logging=False)
    user.wipe_transactions()
    user.set_address_present(address, chain_names[0], value=1, commit=True)
    user.set_address_used(address, chain_names[0], value=1, commit=True)

    # chain_names = Chain.list()
    chains = {}
    for chain_name in chain_names:
        chains[chain_name] = {'chain': user.chain_factory(chain_name), 'current_tokens':{}, 'is_upload':False}

    user.get_custom_rates()


    address_db = SQLite('addresses')
    for chain_name, chain in chains.items():
        chain['chain'].init_addresses(address_db)

    if do_import:
        user.start_import(chains)
        for chain_name, chain_data in chains.items():
            chain_data['import_addresses'] = [address]
            chain = chain_data['chain']
            transactions = chain.get_transactions(user, address, 0)  # alloc 20
            chain_data['transactions'] = transactions
            chain.correct_transactions(address, transactions, 0)  # alloc 5
            current_tokens = chain.get_current_tokens(address)
            chain_data['current_tokens'][address] = current_tokens
            chain.covalent_download(chain_data)
            chain.covalent_correction(chain_data)

        user.get_thirdparty_data(chains)
        for chain_name, chain_data in chains.items():
            chain = chain_data['chain']
            chain.balance_provider_correction(chain_data)

        for chain_name, chain_data in chains.items():
            chain = chain_data['chain']
            print("Storing transactions",chain,len(chain_data['transactions']))
            user.store_transactions(chain_data['chain'], chain_data['transactions'], address,C)
            user.store_current_tokens(chain_data['chain'], chain_data['current_tokens'])

    user.load_addresses()
    user.load_tx_counts()


    transactions, _ = user.load_transactions(chains)
    print("Loaded transactions", len(transactions))
    contract_dict, counterparty_by_chain, input_list = user.get_contracts(transactions)
    if do_lookups:
        print('contract_dict', contract_dict)
        print('counterparty_by_chain', counterparty_by_chain)
        for chain_name, chain_data in chains.items():
            chain = chain_data['chain']
            filtered_counterparty_list = chain.filter_progenitors(list(counterparty_by_chain[chain_name]))
            print(chain_name, 'counterparty_list', filtered_counterparty_list)
            if len(filtered_counterparty_list) > 0:
                chain_data['progenitor_db_writes'] = chain.update_progenitors(filtered_counterparty_list, 0)  # alloc 30

        all_db_writes = []
        for chain_name, chain_data in chains.items():
            if 'progenitor_db_writes' in chain_data:
                all_db_writes.extend(chain_data['progenitor_db_writes'])

        if len(all_db_writes):
            insert_cnt = 0
            for write in all_db_writes:
                chain_name, values = write
                entity = values[-2]
                address_to_add = values[0]
                rc = address_db.insert_kw(chain_name + '_addresses', values=values, ignore=(entity == 'unknown'))
                if rc > 0:
                    address_db.insert_kw(chain_name + '_labels', values=[address_to_add, 'auto'], ignore=True)
                    insert_cnt += 1
            if insert_cnt > 0:
                address_db.commit()
                log('New addresses added', insert_cnt, filename='address_lookups.txt')


    S.init_from_db(input_list)
    needed_token_times = user.get_needed_token_times(transactions)

    C.init_from_db_2(chains, needed_token_times)
    if do_import:
        user.finish_import()
    user.load_current_tokens(C)
    address_db.disconnect()

    transactions_js = user.transactions_to_log(C, S, transactions, store_derived=True)  # alloc 20
    print("do_calc",do_calc)
    if do_calc:
        custom_types = user.load_custom_types()
        calculator = Calculator(user,C)
        calculator.process_transactions(transactions_js,user)
        calculator.matchup()
        calculator.cache()

        log("Calculator summary",calculator.CA_short)

if __name__ == "__main__":
    os.environ['debug'] = '2'
    os.environ['version'] = '1.42'
    load_dotenv()
    address = '0x032b7d93aeed91127baa55ad570d88fd2f15d589'  # hodl
    process(address, 'Arbitrum', do_import=True, do_lookups=True)
    exit(0)


    #dex
    # dex = DEX()
    # pair = dex.locate_pair('0x0da67235dd5787d67955420c84ca1cecd4e5bb3b','Avalanche')
    # # pair = dex.locate_pair('0x136acd46c134e8269052c62a67042d6bdedde3c9', 'Avalanche')
    # print('pair',pair)
    # # rates = pair.download_dexscreener_rates()
    # # pprint.pprint(dict(rates))
    # # if pair is not None:
    # #     pair.get_cmc_id()
    # exit(0)



    #twelve
    # url = "https://api.twelvedata.com/forex_pairs"
    # resp = requests.get(url)
    # data = resp.json()
    # for entry in data['data']:
    #     if entry['currency_group'] == 'Major':
    #         print(entry)
    # symbol = "EUR"
    # url = "https://api.twelvedata.com/time_series?symbol=USD/" + symbol + "&interval=1day&outputsize=5000&timezone=UTC&start_date=2013-01-01&end_date=2019-12-31&order=ASC&apikey=" + ""
    # js = requests.get(url).json()
    # data = js['values']
    # for entry in data:
    #     date = entry['datetime']
    #     ts = datetime.datetime.strptime(date,"%Y-%m-%d").replace(tzinfo=datetime.timezone.utc).timestamp()
    #     print(date,ts)
    # Twelve.create_table()
    # T = Twelve('USD')
    # T.download_all_rates()
    # exit(0)


    # s = Solana()
    # json_template = {"method": "getTransaction", "jsonrpc": "2.0", "params": [None, {"encoding": "jsonParsed", "commitment": "confirmed", "maxSupportedTransactionVersion": 0}]}
    # list = ['3dy39aBE1kXumXz2Vz85tWV9unkarGcRFkK8PNnqTne28htWFaoLbvKvXF8vFKbm2SRbATK3s4hmP1k32B8zHKqn','2oTtPc7fTWQhh7QfGwUCkFqca2i3KciH2zXW3vpzNcLSB4pFk6yRabA7pU994QKXqmy6ACGpHEnpFoD6Tot2nvqz']
    #
    # json_template = {"method": "getSignaturesForAddress", "jsonrpc": "2.0", "params": [None, {"limit": 1000}]}
    # list = ['95iZStZPdxWoKUfinEtxq8X7SfTn496D1tKDiUuyNeqC']
    # multi_explorer_request = []
    # explorer_headers = {
    #     'content-type': 'application/json',
    #     'solana-client': 'js/0.0.0-development',
    #     'origin': 'https://explorer.solana.com',
    #     'referer': 'https://explorer.solana.com/',
    #     'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36'
    # }
    # for query_datum in list:
    #     uid = str(uuid.uuid4())
    #     explorer_dump = copy.deepcopy(json_template)
    #     explorer_dump['params'][0] = query_datum
    #     explorer_dump['id'] = uid
    #     multi_explorer_request.append(explorer_dump)
    #
    # url = 'https://floral-prettiest-wish.solana-mainnet.discover.quiknode.pro//'
    # # print('dump',multi_explorer_request)
    # # # multi_explorer_request = [{"jsonrpc":"2.0","method":"getBalance","params":["95iZStZPdxWoKUfinEtxq8X7SfTn496D1tKDiUuyNeqC"],"id":1}]
    # # multi_explorer_request = [{"jsonrpc": "2.0", "method": "getSignaturesForAddress", "params": ["95iZStZPdxWoKUfinEtxq8X7SfTn496D1tKDiUuyNeqC", {"limit":1}], "id": 1}]
    # multi_explorer_request = {"method": "getAccountInfo", "jsonrpc": "2.0", "params": ["59RrHznHcpnSTgtwp2DwXtP1fS8VUzJWBifW2XoP9xpF", {"encoding": "jsonParsed", "commitment": "confirmed"}]}
    # resp = requests.post(url, timeout=60, json=multi_explorer_request)
    # print(resp.status_code)
    # print(resp.headers)
    # print(resp.content)
    # #
    # exit(0)


    # covalent
    # session = requests.session()
    # session.auth = ("", "")
    # session.headers = {'Content-Type': 'application/json'}
    # url ='https://api.covalenthq.com/v1/250/address/0x7d93f170dfd65d14d58682678b7a0d171f287c93/transactions_v2/?no-logs=true&page-size=1000'
    # t = time.time()
    # resp = session.get(url, timeout=100)
    # data = resp.json()
    # pprint.pprint(data)
    # print(time.time()-t)
    # # entries = data['data']['items']
    # # pprint.pprint(entries)
    # exit(0)





    # # debank pro
    # session = requests.session()
    # session.headers.update({'AccessKey':''})
    # # # url = 'https://pro-openapi.debank.com/v1/user/all_complex_protocol_list?id=0xd603a49886c9B500f96C0d798aed10068D73bF7C'
    # # # url = 'https://pro-openapi.debank.com/v1/token?chain_id=eth&id=0x5954aB967Bc958940b7EB73ee84797Dc8a2AFbb9'
    # # # url = 'https://pro-openapi.debank.com/v1/user/all_token_list?id=0x50b1c57159be31b401adc4ee5ab454b2277b6b5f'
    # url = 'https://pro-openapi.debank.com/v1/chain/list'
    # # # url = 'https://pro-openapi.debank.com/v1/user/history_list?id=0xd603a49886c9B500f96C0d798aed10068D73bF7C&chain_id=arb&token_id=arb&page_count=100'
    # # url = 'https://pro-openapi.debank.com/v1/token?id=0x130966628846bfd36ff31a822705796e8cb8c18d&chain_id=avax'
    # # url = 'https://pro-openapi.debank.com/v1/token?id=0x82f0b8b456c1a451378467398982d4834b6829c1&chain_id=ftm'
    # #
    # resp = session.get(url,timeout=10)
    # if resp.status_code !=200:
    #     print(resp.status_code, resp.content)
    # data = resp.json()
    # pprint.pprint(data)
    # # print(len(data['history_list']))
    # exit(0)


    # ids = []
    # total = 0
    # done = False
    # # t = int(time.time())
    # t = 1634314020
    # while not done:
    #     time.sleep(0.5)
    #     url = 'https://pro-openapi.debank.com/v1/user/history_list?id=0x5e014aa0649102e07c074f498845f01bcd520317&chain_id=arb&token_id=arb&start_time='+str(t)
    #     resp = session.get(url, timeout=10)
    #     data = resp.json()
    #     # pprint.pprint(data)
    #     adr = '0x5e014aa0649102e07c074f498845f01bcd520317'
    #     print(url,t,len(data['history_list']),total)
    #     added = 0
    #     entries = data['history_list'][::-1]
    #     for entry in entries:
    #         id = entry['id']
    #         pprint.pprint(entry)
    #         if id in ids:
    #             print("SKIP")
    #             continue
    #         ids.append(id)
    #         added += 1
    #         try:
    #             type = entry['tx']['name']
    #             if type == 'createRetryableTicket' and entry['tx']['to_addr'] == '0x000000000000000000000000000000000000006e':
    #                 total += entry['tx']['value']
    #                 print("total-dep", total)
    #                 continue
    #         except:
    #             pass
    #
    #         if 'sends' in entry:
    #             for s in entry['sends']:
    #                 if s['token_id'] == 'arb':
    #                     total -= s['amount']
    #         if 'receives' in entry:
    #             for s in entry['receives']:
    #                 if s['token_id'] == 'arb':
    #                     total += s['amount']
    #         try:
    #             eth_gas_fee = entry['tx']['eth_gas_fee']
    #             if entry['tx']['from_addr'] == adr:
    #                 total -= eth_gas_fee
    #         except:
    #             pass
    #         print("total",total)
    #         t = int(entry['time_at'])
    #     if added == 0:
    #         done = True
    #
    # print('total',total)

    #ankr
    # session = requests.session()
    # url = 'https://rpc.ankr.com/multichain/'
    # # url = 'https://rpc.ankr.com/arbitrum/'
    # session.headers.update({'Content-Type': 'application/json'})
    # # proxy = ''
    # # session.proxies = {
    # #     'http': proxy,
    # #     'https': proxy
    # # }
    # js = {'id': 1, 'jsonrpc': '2.0', 'method': 'ankr_getTransfersByAddress', 'params':
    #     {'blockchain':'optimism','pageSize':10,'walletAddress':'0x5e014aa0649102e07c074f498845f01bcd520317','orderAsc':True,'transactionType':'REGULAR_TRANSACTION'}}
    # # js = {'id':1, 'jsonrpc':'2.0','method':'ankr_getAccountBalance','params':
    # #     {'blockchain':['eth'],'onlyWhitelisted':False,'pageSize':100000,'walletAddress':'0xd603a49886c9B500f96C0d798aed10068D73bF7C'}}
    # # js = {"jsonrpc":"2.0","method":"eth_getTransactionByHash","params":["0x96eacea31034ca72f17dd0a14d9c1747c3d59c4153caa184c26234fafc39bea7"],"id":1}
    # resp = session.post(url, json=js)
    # data = resp.json()
    # # print(len(data['result']['assets']))
    # pprint.pprint(data)
    # exit(0)
    # adr = '0x5e014aa0649102e07c074f498845f01bcd520317'
    # txs = data['result']['transactions']
    # total = 0
    # for entry in txs:
    #     if  entry['contractAddress'] == '':
    #         amt = float(entry['value']) / pow(10,18)
    #         if amt == 0:
    #             continue
    #         to = entry['toAddress']
    #         fr = entry['fromAddress']
    #         if to == '0x000000000000000000000000000000000000006e' and fr == adr:
    #             total += amt
    #         else:
    #             if to[:-3] == '0x0000000000000000000000000000000000000' or fr[:-3] == '0x0000000000000000000000000000000000000':
    #                 print("????",entry)
    #             if to == adr:
    #                 total += amt
    #             if fr == adr:
    #                 total -= amt
    #             if to == fr:
    #                 print("????!!", entry)
    #         print(entry['transactionHash'],amt,total)
    # done = False
    # nextPageToken = None
    # page_tokens = []
    # while not done:
    #     print('tok', nextPageToken)
    #     time.sleep(0.1)
    #     js = {'id': 1, 'jsonrpc': '2.0', 'method': 'ankr_getNFTsByOwner', 'params':
    #          {'blockchain':['eth'],'pageSize':50,'walletAddress':'0xd603a49886c9B500f96C0d798aed10068D73bF7C'}}
    #     if nextPageToken:
    #         js['params']['pageToken'] = nextPageToken
    #     resp = session.post(url,json=js)
    #     print(resp.status_code)
    #     if resp.status_code != 200:
    #         print(resp.content)
    #     # print(resp.content)
    #     data = resp.json()
    #     # pprint.pprint(data)
    #     print (len(data['result']['assets']))
    #     if 'nextPageToken' in data['result']:
    #         nextPageToken = data['result']['nextPageToken']
    #         if len(nextPageToken) < 2:
    #             done = True
    #     else:
    #         done = True
    # exit(0)

    # session = requests.session()
    # session.headers.update({'X-API-Key': })
    # url = 'https://api.simplehash.com/api/v0/nfts/owners?chains=arbitrum&wallet_addresses=0xd603a49886c9B500f96C0d798aed10068D73bF7C&queried_wallet_balances=1'
    # resp = session.get(url)
    # print(resp.status_code)
    # data =resp.json()
    # # print('cnt',len(data['nfts']),data['count'])
    # pprint.pprint(data)
    # exit(0)

    # session = requests.session()
    # session.headers.update({'X-API-Key': })
    # url = 'https://api.center.dev/v1/polygon-mainnet/account/0xd603a49886c9B500f96C0d798aed10068D73bF7C/assets-owned?limit=100'
    # resp = session.get(url)
    # print(resp.status_code)
    # assets = []
    # for entry in resp.json()['items']:
    #     assets.append({'Address':entry['address'],'TokenID':str(entry['tokenId'])})
    # print(len(assets))
    # print(assets)
    # url = 'https://api.center.dev/v1/ethereum-mainnet/assets'
    # resp = session.post(url,json={'assets':assets})
    # print('assets',resp.status_code, resp.content)

    # session.headers.update({'X-API-KEY': })
    # url = 'https://api.opensea.io/api/v1/assets?owner=0xed2ab4948bA6A909a7751DEc4F34f303eB8c7236'
    # resp = session.get(url)
    # print(resp.status_code)
    # print(resp.content)
    # exit(0)
    # url = 'https://webapi.nftscan.com/nftscan/getUserNftByContract?ercType=erc721&walletType=3&user_address=0xd603a49886c9B500f96C0d798aed10068D73bF7C&nft_address=&pageSize=40&pageIndex=2&sortBy=recently'
    # headers = {
    #     'origin':'https://www.nftscan.com',
    #     'referer':'https://www.nftscan.com',
    #     'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36',
    #     'chain':'ETH'
    # }
    #
    # resp = requests.get(url,headers=headers)
    # print(resp.status_code)
    # print(resp.content)
    # exit(0)

    # add_chain_to_addresses('Flare')
    # add_chain_to_addresses('Arbitrum Nova')
    # add_chain_to_addresses('Celo')
    # add_chain_to_addresses('ETC')
    # add_chain_to_addresses('Chiliz')
    # add_chain_to_addresses('Oasis')
    # add_chain_to_addresses('Astar')
    # add_chain_to_addresses('Evmos')
    # add_chain_to_addresses('Kucoin')
    # add_chain_to_addresses('EnergyWeb')
    # add_chain_to_addresses('Bitgert')
    # add_chain_to_addresses('Metis')
    # add_chain_to_addresses('Songbird')
    # add_chain_to_addresses('Kava')
    # add_chain_to_addresses('Stepn')
    # add_chain_to_addresses('Boba')
    # add_chain_to_addresses('SXNetwork')
    # add_chain_to_addresses('Step')
    # add_chain_to_addresses('Velas')
    # add_chain_to_addresses('Moonbeam')
    # add_chain_to_addresses('KCC')
    # add_chain_to_addresses('smartBCH')
    # exit(0)

    # update_signatures()
    # os.chdir('/home/ubuntu/hyperboloid')
    # update_rates_db()
    # exit(0)

    # formalize_names('BSC')
    # exit(0)

    # C = Coingecko()
    # # C.download_symbols_to_db()
    # C.download_all_coingecko_rates()
    address = '0xd603a49886c9b500f96c0d798aed10068d73bf7c'
    # address = '0xD69F1F0c7c40d119C3BfEc0E89553aC8f40284F2'
    address = '95iZStZPdxWoKUfinEtxq8X7SfTn496D1tKDiUuyNeqC' #solana
    address = '9HdPeqZGJDTtoHoGz4x6vNoPBxhnQLazjmfzYAjAiZVK'


    # chain = Chain('ETH', 'https://api.etherscan.io/api', 'ETH', '',
    #               outbound_bridges=['0XA0C68C638235EE32657E8F720A23CEC1BFC77C77',  # polygon
    #                                 '0X40EC5B33F54E0E8A33A975908C5BA1C14E5BBBDF',  # polygon
    #                                 '0X59E55EC322F667015D7B6B4B63DC2DE6D4B541C3'],  # bsc
    #               inbound_bridges=['0X56EDDB7AA87536C09CCC2793473599FD21A8B17F'], addr=address)

    # address = '0x5fe41f8e36b1b2c72aa0091626841c989d90b0d9'
    # address = '0x712d0f306956a6a4b4f9319ad9b9de48c5345996'
    # address = '0x22fa8cc33a42320385cbd3690ed60a021891cb32'
    # address = '0x083fc10ce7e97cafbae0fe332a9c4384c5f54e45' #k06a

    # address = '0x6867115787080d4e95cbcb6471fa85a9458a5e43' #subvert
    # address = '0x3401ea5a8d91c5e3944962c0148b08ac4a77f153' #so many nfts
    # address = '0x641c2fef13fb417db01ef955a54904a6400f8b07' #delso
    # address = '0x6f69f79cea418024b9e0acfd18bd8de26f9bbe39'  #cap
    address = '0x032b7d93aeed91127baa55ad570d88fd2f15d589' #hodl
    # address = '0xd96fbf82f4445be4833f87006c597e1732aea739'
    # address = '0x6cf9aa65ebad7028536e353393630e2340ca6049' #swissborg 4, a giant bank
    # address = '0x134926384758acb7c95cd8ebbf84d655a19138ec' #ancestor retrieval problem on Avalanche
    # address = '0xd787a6604b825d5486ad391c9cad16658a973438' #problem in correct_transactions
    # address = '0xac0537fdb3591444094004791182a0e5fa3669f0' #problem on Fantom
    # address = '0xd20ce27f650598c2d790714b4f6a7222b8ddce22' #problem in correct_transactions
    # address = '0x96174454e62bdb0270ac89c3ab24289a1871233a' #connection problem on ETH
    # address = '0x9f9aa7870f8cac738582ad72d359791b4935a878' #avalanche
    # address = '0xa335ade338308b8e071cf2c8f3ca5e50f6563c60' #eth
    # address = '0x5c2e7837a5dc5d07c6977bfedb46a1f3ec6a5a77' #eth
    # address = '0x06024bd5a02639c3c974d7e0259e3b3db52ba637' #polygon
    # address = '0xe4b2d0fb698db5a0a23c4ab740303706d036044e' #avalanche
    # address = '0x574941d6266615f2695fc62f9f2ecc362e7c4916' #ETH
    # address = '0xb0cbcc777509b0ff315392144b0e739fb443531b' #polygon
    # address = '0x48dce6dc2c47a97632945918b3a4611b62472b69' #avalanche
    # address = '0x59e59e4313f7f3158119dea1fd10655888a72640' #avalanche
    # address = '0xf56345338cb4cddaf915ebef3bfde63e70fe3053' #eth
    # address = '0x936a09f27fe490362074cf45e485c4e68b2db2da' #eth
    # address = '0xc821e6331f3535c8202f4d8ab25e15215ac71ad6' #polygon
    # address = '0xec7372db51f9a162c3c9fa59a2e2004837b86622' #fantom
    # address = '0x4ddc2d193948926d02f9b1fe9e1daa0718270ed5' #eth #record number of transactions
    # address = '0x3da48c5e033edc7dbb3bcc1e68293753c1ce9cf6' #fantom
    # address = '0x57a15518b0ab5309d6459dc32a8acd1e89e4f333' #avalanche
    # address = '0x1c7e188674d67d9db00a17696c02462bae80ac7c'
    # address = '0xffea5a2cfaf1aafbb87a1fe4eed5413da45c30a0' #55000 transactions
    # address = '0xeb7d0be769cf857afa5626b5f8208528f034b1da' #cronos
    # address = '0x7c2ffb059a5b0a9dad492d57484626bc16cac021' #moonriver
    # address = '0xb3bb91c89d5cffa88926d0ff7c51b705a0702ffc' #cronos, fantom
    # address = '0x4c48b9e6324b9597d47ec7c748817b3fbee3ae6a' #xdai
    # address = '0x5e014aa0649102e07c074f498845f01bcd520317' #optimism
    # address = '0x0e8b0cdf27b9dd2ddec656ed31bb086b8aed495c' #fantom, also coingecko rates get loaded every time
    # address = '0xeebb57a49de7631a04eecea0c37f211e342421e9' #shitcoin heaven





    # ranges = [[100,200],[300,500],[600,800]]
    # range_test = [[0,50],[0,150],[0,250],[0,400],[0,550],[0,700],[0,900]]
    # range_test = [[150, 170], [150, 250], [150, 400], [150, 550], [150, 700], [150, 900]]
    # range_test = [[250,270], [250, 400], [250, 550], [250, 700], [250, 900]]
    # range_test = [[0, 900],[150, 900],[250, 900],[350, 900],[550, 900],[650, 900],[850, 900]]
    # for entry in range_test:
    #     print("\nmerge test", ranges, entry)
    #     res = Coingecko.merge_ranges(copy.deepcopy(ranges),entry[0],entry[1])
    #     print("res",res)
    # exit(0)

    # rv = chain.get_ancestor('0xc2edad668740f1aa35e4d8f227fb8e17dca888cd')



