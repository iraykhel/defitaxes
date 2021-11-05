import requests
import pprint
import math
from collections import defaultdict
from sortedcontainers import *
import time
from .transaction import *
from .classifiers import Classifier
import csv
from .util import log, progress_bar_update
import bs4
from .pool import Pools





class Chain:
    def __init__(self,address_db,name,domain,main_asset, api_key, addr='0xd603a49886c9B500f96C0d798aed10068D73bF7C',outbound_bridges=(),inbound_bridges=(),wrapper=None):
        addr = addr.lower()
        self.domain = domain
        self.explorer_url = 'https://api.'+domain+'/api'
        self.main_asset = main_asset
        self.addr = addr
        self.api_key = api_key
        self.name=name
        self.address_db = address_db
        self.outbound_bridges = []
        self.inbound_bridges = []
        for bridge in outbound_bridges:
            self.outbound_bridges.append(bridge.lower())

        for bridge in inbound_bridges:
            self.inbound_bridges.append(bridge.lower())
        self.wrapper = None
        if wrapper is not None:
            self.wrapper = wrapper.lower()

        self.hif = '0xef42b4c07a58aa49b7e8fc5e558af854a9f1bf97428ab56e5cef7147e873b3a2'

        # address_db.create_table(name + '_ancestry', 'address PRIMARY KEY, progenitor', drop=False)
        # address_db.create_table(name + '_names', 'address PRIMARY KEY, name', drop=False)
        # address_db.create_table(name + '_custom_names', 'user, address, name', drop=False)
        # address_db.create_index(name + '_custom_names_idx', name + '_custom_names', 'user, address', unique=True)

        # address_db.create_table(name + '_last_call', 'user PRIMARY KEY, timestamp INTEGER', drop=False)


        # self.progenitors = {}
        # rows = address_db.select("SELECT * FROM "+name+"_ancestry")
        # for row in rows:
        #     self.progenitors[row[0]] = row[1]
        #
        # self.progenitor_names = {}
        # rows = address_db.select("SELECT * FROM " + name + "_names")
        # for row in rows:
        #     self.progenitor_names[row[0]] = row[1]
        self.addresses = {}
        rows = address_db.select("SELECT * FROM " + name + "_addresses")
        for row in rows:
            self.addresses[row[0]]={'tag':row[1],'entity':row[3],'ancestor':row[2]}

        # self.custom_addresses = {}
        # rows = address_db.select("SELECT * FROM " + name + "_custom_names WHERE user='"+self.addr+"'")
        # for row in rows:
        #     self.custom_addresses[row[1]] = {'entity':row[2]}

        # self.last_call = 0
        # rows = address_db.select("SELECT timestamp FROM "+name+"_last_call WHERE user='"+self.addr+"'")
        # if len(rows) == 1:
        #     self.last_call = rows[0][0]

        self.scrapes = 0


        self.lp_token_addresses = []
        self.vault_holds = {}
        self.stake_addresses = []
        self.swap_addresses = {}


        self.pools = Pools(self)



    @classmethod
    def from_name(cls,chain_name,address_db,address):
        if chain_name == 'ETH':
            chain = Chain(address_db,'ETH', 'etherscan.io', 'ETH', 'ABGDZF9A4GIPCHYZZS4FVUBFXUPXRDZAKQ',
                        outbound_bridges=['0XA0C68C638235EE32657E8F720A23CEC1BFC77C77',  # polygon
                                          '0X40EC5B33F54E0E8A33A975908C5BA1C14E5BBBDF',  # polygon
                                          '0X59E55EC322F667015D7B6B4B63DC2DE6D4B541C3'],  # bsc
                        inbound_bridges=['0X56EDDB7AA87536C09CCC2793473599FD21A8B17F'],
                        wrapper='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
                        addr=address)

        if chain_name == 'Polygon':
            chain = Chain(address_db,'Polygon', 'polygonscan.com', 'MATIC', 'A1FQ2P7N8199KNXQNNC5GUXV329VX6U3AN',
                            outbound_bridges=['0X0000000000000000000000000000000000000000', '0X7CEB23FD6BC0ADD59E62AC25578270CFF1B9F619'],
                            inbound_bridges=['0X0000000000000000000000000000000000000000'],
                          wrapper='0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270',
                          addr=address)

        if chain_name == 'BSC':
            chain = Chain(address_db,'BSC', 'bscscan.com', 'BNB', 'EVFEA2Z91JKN557RRY6AK7KCB8NM1PMBEZ',
                        outbound_bridges=['0X37C9980809D205972D8D092D5A5AE912BC91DA4C', '0X2170ED0880AC9A755FD29B2688956BD959F933F8'],  # eth
                        inbound_bridges=['0X8894E0A0C962CB723C1976A4421C95949BE2D4E3'],
                          wrapper='0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c',
                          addr=address)  # eth

        if chain_name == 'HECO':
            chain = Chain(address_db,'HECO', 'hecoinfo.com', 'HT', 'T4UDKXYGYSFA3ACAX3XCD546IMH622HNV5', addr=address, wrapper='0x5545153ccfca01fbd7dd11c0b23ba694d9509a6f')

        return chain

    def unwrap(self, what):
        if what == self.wrapper:
            return self.main_asset
        return what

    def get_progenitor_entity(self,user,address):
        custom_addresses = user.get_custom_addresses(self.name)
        if address in custom_addresses:
            return custom_addresses[address]['entity'], None, 1


        if address in self.addresses:
            ancestor = self.addresses[address]['ancestor']
            if ancestor in custom_addresses:
                # return self.custom_addresses[ancestor]['entity'], None, 1
                return custom_addresses[ancestor]['entity'], ancestor, 1
            return self.addresses[address]['entity'], ancestor, 1

        # if address in self.progenitor_names:
        #     return address,self.progenitor_names[address],0
        #
        # if address in self.progenitors:
        #     progenitor_address = self.progenitors[address]
        #     if progenitor_address in self.custom_progenitor_names:
        #         return progenitor_address, self.custom_progenitor_names[progenitor_address], 1
        #
        #     if progenitor_address in self.progenitor_names:
        #         return progenitor_address, self.progenitor_names[progenitor_address], 0
        #
        #     return progenitor_address,None,1
        return None,None,1

    def scrape_address_info(self,address):
        log('scrapping scan for',address)
        self.scrapes += 1
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.164 Safari/537.36',
            # 'cache-control':'max-age=0',
            # 'cookie': '_ga=GA1.2.467112662.1615497128; etherscan_cookieconsent=True; __stripe_mid=5a09d00b-4eae-4692-8504-f313de02aa144b4a30; etherscan_userid=iraykhel; etherscan_pwd=4792:Qdxb:EWvgzItHTspsgiGejmq+IyCF6VkM5FCC+CnRgsL0EiE=; etherscan_autologin=True; ASP.NET_SessionId=rclihdriqigdpnfiu5jygebv; __cflb=02DiuFnsSsHWYH8WqVXcJWaecAw5gpnmeVbBoLqEHwzz8; _gid=GA1.2.885744722.1628003673; _gat_gtag_UA_46998878_6=1; __cf_bm=763969dfe2b4a8b08b7baf2ade8c42a99a92215a-1628011329-1800-ATJVGPjy7InedZvxrWeZmEKX0F2aD+Vb119x0YA2D9LF70kEAAGyZsTRQYv43oTLCmhO02yGmcEuDiuMKOmjrvr9TVRF9H7vngozwaoXjINf1PhUSKnw1wcJudy2C/ZlCg=='
        }
        url = 'https://'+self.domain+'/address/'+address.lower()
        log(url)
        session = requests.session()
        # ip = '52.193.76.133'
        # session.proxies = {'http': 'http://' + ip + ":8888", 'https': 'http://' + ip + ":8888"}
        # cont = requests.get(url, headers=headers).content
        cont = session.get(url, headers=headers).content
        html = cont.decode('utf-8')


        soup = bs4.BeautifulSoup(html, features="lxml")
        log(soup)
        # print(soup)
        cont = soup.find('div',class_='py-3')
        labels_els = cont.find_all('a',class_='u-label')
        labels = []
        for el in labels_els:
            label = el.contents[0].strip()
            labels.append(label)
        # print(labels)

        nametag = None
        nametag_el = soup.find('span',class_='u-label')
        if len(nametag_el) == 3:
            nametag = nametag_el.contents[0].contents[0].strip()
        # nametag = soup.select('span[data-original-title="Public Name Tag (viewable by anyone)"]')
        #     print(nametag)

        # creator = soup.find('a',{'data-original-title':'Creator Address'})
        # creator = soup.select('a[data-original-title="Creator Address"]')
        creator = None
        creator_el = soup.find('div',id='ContentPlaceHolder1_trContract')

        if creator_el is not None and len(creator_el) > 0:
            creator = creator_el.find('a').contents[0].strip()
            # print(creator)

        ens = None
        ens_el = soup.find('a',id='ensName')
        if ens_el is not None:
            ens = ens_el.contents[1].strip()


        return nametag, labels, creator, ens


    #match interface to scrape_address_info
    def get_ancestor(self,address):
        # log('ancestor for',address)
        address = address.lower()

        self.scrapes += 1
        log("Looking up ancestor on scan",address)
        url = self.explorer_url+"?module=account&action=txlist&address="+address+"&page=1&sort=asc&apikey="+self.api_key+"&offset=5"
        resp = requests.get(url)
        # print(address,'normal', resp.content)
        time.sleep(0.5)
        data = resp.json()['result']
        # print(address, transaction)
        # if len(transaction['input']) <=2:
        #     return -1, None
        # print(transaction['to'],
        try:
            for transaction in data:
                if transaction['to'] == "" and transaction['contractAddress'].lower() == address and len(transaction['input']) > 2:
                    return None, [], transaction['from'], None
        except:
            log('ERROR',url,data)
            exit(1)
        url = self.explorer_url + "?module=account&action=txlistinternal&address=" + address + "&page=1&sort=asc&apikey=" + self.api_key + "&offset=5"
        resp = requests.get(url)
        print(address, 'internal', resp.content)
        time.sleep(0.5)
        data = resp.json()['result']
        if len(data) == 0:
            return None, [], None, None
        for transaction in data:
            if transaction['to'] == "" and transaction['contractAddress'].lower() == address and 'create' in transaction["type"]:
                return None, [], transaction['from'], None
                # return 0, transaction['from']
        # transaction = data[0]

        # return -1, None
        return None, [], None, None




    def get_transactions(self):
        hif = self.hif

        t = time.time()
        progress_bar_update(self.addr, 'Retrieving '+self.main_asset+' transactions',1)
        div = 1000000000000000000.
        log('\n\ngetting transaction for',self.addr,self.name)
        # url = self.explorer_url + '?module=account&action=tokenmultitx&address=' + self.addr + '&apikey=' + self.api_key
        # print(url)
        # resp = requests.get(url)
        # data = resp.json()['result']
        # pprint.pprint(data)
        # exit(0)

        url = self.explorer_url + '?module=account&action=txlist&address=' + self.addr + '&apikey=' + self.api_key + '&sort=asc'
        # log(url)
        resp = requests.get(url)
        data = resp.json()['result']
        # pprint.pprint(resp.json())
        transactions = SortedDict()
        for entry in data:
            if entry['isError'] != '0':
                continue
            hash = entry['hash']
            if hash == hif:
                pprint.pprint(entry)
            ts = entry['timeStamp']
            uid = str(ts) + "_" + str(hash)
            if uid not in transactions:
                transactions[uid] = Transaction(self)
            fr = entry['from'].lower()
            to = entry['to'].lower()
            input = entry['input']
            if input == 'deprecated':
                input_len = -1
                input = None
            else:
                input_len = len(input)
            val = float(entry['value']) / div
            fee = float(entry['gasUsed']) * float(entry['gasPrice']) / div
            row = [hash, ts, fr, to, val, self.main_asset, None, None, fee, input_len, input]
            # if fee + val > 0:
            transactions[uid].append(1, row)
        t1 = time.time()

        progress_bar_update(self.addr, 'Retrieving internal transactions', 5)
        url = self.explorer_url + '?module=account&action=txlistinternal&address=' + self.addr + '&apikey=' + self.api_key
        print(url)
        resp = requests.get(url)
        print(resp.content)
        data = resp.json()['result']
        # pprint.pprint(data['result'])
        for entry in data:
            # print(entry)
            if entry['isError'] != '0':
                continue
            hash = entry['hash']
            if hash == hif:
                pprint.pprint(entry)
            ts = entry['timeStamp']
            uid = str(ts) + "_" + str(hash)
            if uid not in transactions:
                transactions[uid] = Transaction(self)
            fr = entry['from'].lower()
            to = entry['to'].lower()
            input = entry['input']
            if input == 'deprecated':
                input_len = -1
                input = None
            else:
                input_len = len(input)
            val = float(entry['value']) / div
            row = [hash, ts, fr, to, val, self.main_asset, None, None, 0,input_len, input]
            # if val > 0:
            transactions[uid].append(2, row)

        t2 = time.time()
        progress_bar_update(self.addr, 'Retrieving token transactions', 10)
        url = self.explorer_url + '?module=account&action=tokentx&address=' + self.addr + '&apikey=' + self.api_key
        resp = requests.get(url)
        data = resp.json()['result']
        # pprint.pprint(resp.json())
        for entry in data:
            hash = entry['hash']
            if hash == hif:
                pprint.pprint(entry)
            ts = entry['timeStamp']
            uid = str(ts) + "_" + str(hash)
            if uid not in transactions:
                transactions[uid] = Transaction(self)
            fr = entry['from'].lower()
            to = entry['to'].lower()
            input = entry['input']
            if input == 'deprecated':
                input_len = -1
                input = None
            else:
                input_len = len(input)
            token_contract = entry['contractAddress'].lower()
            tokendiv = float(math.pow(10, int(entry['tokenDecimal'])))
            token = entry['tokenSymbol']
            val = float(entry['value']) / tokendiv
            fee = 0  # accounted in base transactions
            row = [hash, ts, fr, to, val, token, token_contract, None, 0, input_len, input]
            # if val > 0:
            transactions[uid].append(3, row)

        t3 = time.time()
        progress_bar_update(self.addr, 'Retrieving NFT transactions', 15)
        url = self.explorer_url + '?module=account&action=tokennfttx&address=' + self.addr + '&apikey=' + self.api_key
        resp = requests.get(url)
        data = resp.json()['result']
        # pprint.pprint(resp.json())
        for entry in data:
            hash = entry['hash']
            if hash == hif:
                pprint.pprint(entry)
            ts = entry['timeStamp']
            uid = str(ts) + "_" + str(hash)
            if uid not in transactions:
                transactions[uid] = Transaction(self)
            fr = entry['from'].lower()
            to = entry['to'].lower()
            input = entry['input']
            if input == 'deprecated':
                input_len = -1
                input = None
            else:
                input_len = len(input)
            token_contract = entry['contractAddress'].lower()
            token = entry['tokenSymbol']# + " ("+entry['tokenID']+")"
            token_nft_id = entry['tokenID']
            # tokenID = entry['tokenID']
            val = 1
            fee = 0  # accounted in base transactions
            row = [hash, ts, fr, to, val, token, token_contract, token_nft_id, 0, input_len, input]
            # if val > 0:
            transactions[uid].append(4, row)

        t4 = time.time()
        # pprint.pprint(transactions)
        log('timing:get transactions',t1-t,t2-t1,t3-t2,t4-t3)
        return transactions







    # def get_ancestor(self,address):
    #     # log('ancestor for',address)
    #     address = address.lower()
    #     if address == 'genesis':
    #         return 1,self.name
    #     if address in self.addresses:
    #         return 1,self.addresses[address]
    #
    #     self.ancestor_lookups += 1
    #     log("Looking up ancestor on scan",address)
    #     url = self.explorer_url+"?module=account&action=txlist&address="+address+"&page=1&sort=asc&apikey="+self.api_key+"&offset=5"
    #     resp = requests.get(url)
    #     # print(address,'normal', resp.content)
    #     time.sleep(0.5)
    #     data = resp.json()['result']
    #     # print(address, transaction)
    #     # if len(transaction['input']) <=2:
    #     #     return -1, None
    #     # print(transaction['to'],
    #     try:
    #         for transaction in data:
    #             if transaction['to'] == "" and transaction['contractAddress'].lower() == address and len(transaction['input']) > 2:
    #                 return 0, transaction['from']
    #     except:
    #         log('ERROR',url,data)
    #         exit(1)
    #     url = self.explorer_url + "?module=account&action=txlistinternal&address=" + address + "&page=1&sort=asc&apikey=" + self.api_key + "&offset=5"
    #     resp = requests.get(url)
    #     print(address, 'internal', resp.content)
    #     time.sleep(0.5)
    #     data = resp.json()['result']
    #     if len(data) == 0:
    #         return -1, None
    #     for transaction in data:
    #         if transaction['to'] == "" and transaction['contractAddress'].lower() == address and 'create' in transaction["type"]:
    #             return 0, transaction['from']
    #     # transaction = data[0]
    #
    #     return -1, None

    # def get_progenitor(self,address):
    #     # log("progenitor for",address)
    #     if address.lower() == '0x0000000000000000000000000000000000000000':
    #         return None
    #     # print("Getting progenitor for ",address)
    #     prev_ancestor = None
    #     ancestry = []
    #     type, ancestor = self.get_ancestor(address)
    #     while type == 0:
    #         prev_ancestor = ancestor
    #         ancestry.append(prev_ancestor)
    #         type, ancestor = self.get_ancestor(ancestor)
    #     #
    #     if len(ancestry) > 0:
    #         print('ancestry', address, ancestry)
    #         if type == 1:
    #             progenitor = ancestor
    #         else:
    #             progenitor = ancestry[-1]
    #         descendants = [address] + ancestry[:-1]
    #         if progenitor in descendants:
    #             print("PROGENITOR IN DESCENDANTS")
    #             print(progenitor)
    #             print(descendants)
    #             print(address)
    #             exit(1)
    #
    #         if progenitor in self.addresses:
    #             entity = self.addresses[progenitor]
    #         else:
    #             entity = 'unknown'
    #
    #         if address not in self.addresses:
    #             self.addresses[address] = entity
    #
    #         for descendant in descendants:
    #             vals = [descendant,None,progenitor, entity]
    #             rowcount = self.address_db.insert_kw(self.name+'_addresses',values=vals, ignore=True)
    #             if rowcount == 1:
    #                 self.address_db.insert_kw(self.name + '_labels', values=[descendant,'auto'], ignore=True)
    #             # self.address_db.insert_kw(self.name+'_ancestry',values=vals)
    #             # self.progenitors[descendant] = progenitor
    #             print('inserting',vals)
    #         # if progenitor not in self.progenitor_names:
    #         #     self.progenitor_names[progenitor] = 'unktnown'
    #         #     self.address_db.insert_kw(self.name+'_names', values=[progenitor,"unknown"])
    #         #     print('inserting into names',progenitor)
    #         self.address_db.commit()
    #
    #     if type == -1:
    #         return prev_ancestor
    #
    #     return ancestor

    def extract_entity(self,tag):
        if ':' in tag:
            row_entity = tag[:tag.index(':')].upper()
        else:
            tag_parts = tag.split(' ')
            if tag_parts[-1].isdigit():
                row_entity = ' '.join(tag_parts[:-1]).upper()
            else:
                row_entity = tag.upper()
        return row_entity



    def update_address_from_scan(self,user, address, max_depth=1):
        address = address.lower()
        # log("progenitor for",address)
        if address == '0x0000000000000000000000000000000000000000':
            return

        entity,_,_ = self.get_progenitor_entity(user,address)
        if entity is not None:
            return

        # log('updating address for',address)
        # print("Getting progenitor for ",address)
        prev_ancestor = address
        ancestry = []

        entity = None
        db_ancestor = None
        # nametag, labels, creator, ens = self.scrape_address_info(address)
        nametag, labels, creator, ens = self.get_ancestor(address)
        depth = 1
        while nametag is None and ens is None and creator is not None and depth <= max_depth:

            entity, db_ancestor, _ = self.get_progenitor_entity(user,creator)
            if entity is not None:
                break
            else:
                if depth == max_depth:
                    break

                if len(labels) > 0:
                    log("populate labels for", address, labels)
                    for label in labels:
                        self.address_db.insert_kw(self.name + '_labels', values=[address, label], ignore=True)

                ancestry.append(prev_ancestor)
                prev_ancestor = creator
                # nametag, labels, creator, ens = self.scrape_address_info(creator)
                nametag, labels, creator, ens = self.get_ancestor(creator)

                if len(labels) > 0:
                    log("populate labels for", creator, labels)
                    for label in labels:
                        self.address_db.insert_kw(self.name + '_labels', values=[creator, label], ignore=True)

            depth += 1

        if entity is None:
            if nametag is not None:
                entity = self.extract_entity(nametag)
            elif ens is not None:
                entity = ens
        # else:
        #     if db_ancestor is not None:
        #         log("setting creator from db, to",db_ancestor)
        #         creator = db_ancestor

        if entity is None:
            entity = 'unknown'



        log("prev_ancestor", prev_ancestor, "creator", creator, "nametag", nametag, "ens", ens, "entity", entity, "descendants", ancestry)
        rc = self.address_db.insert_kw(self.name + '_addresses', values=[prev_ancestor,nametag,creator,entity,'lookup'], ignore=True)
        if rc > 0:
            self.address_db.insert_kw(self.name + '_labels', values=[prev_ancestor, 'auto'], ignore=True)
        self.addresses[prev_ancestor] = {'tag': nametag, 'entity': entity, 'ancestor': creator}

        if creator is None:
            creator = prev_ancestor
        for descendant in ancestry:
            rc = self.address_db.insert_kw(self.name + '_addresses', values=[descendant, None, creator, entity,'lookup-descendant'], ignore=True)
            if rc > 0:
                self.address_db.insert_kw(self.name + '_labels', values=[descendant, 'auto'], ignore=True)
            self.addresses[descendant] = {'tag': None, 'entity': entity, 'ancestor': creator}
        self.address_db.commit()

        # print("creator",creator,"nametag",nametag,"ens",ens,"entity",entity,"descendants",ancestry)




        # type, ancestor = self.get_ancestor(address)
        # while type == 0:
        #     prev_ancestor = ancestor
        #     ancestry.append(prev_ancestor)
        #     type, ancestor = self.get_ancestor(ancestor)
        # #
        # if len(ancestry) > 0:
        #     print('ancestry', address, ancestry)
        #     if type == 1:
        #         progenitor = ancestor
        #     else:
        #         progenitor = ancestry[-1]
        #     descendants = [address] + ancestry[:-1]
        #     if progenitor in descendants:
        #         print("PROGENITOR IN DESCENDANTS")
        #         print(progenitor)
        #         print(descendants)
        #         print(address)
        #         exit(1)
        #
        #     if progenitor in self.addresses:
        #         entity = self.addresses[progenitor]
        #     else:
        #         entity = 'unknown'
        #
        #     if address not in self.addresses:
        #         self.addresses[address] = entity
        #
        #     for descendant in descendants:
        #         vals = [descendant,None,progenitor, entity]
        #         rowcount = self.address_db.insert_kw(self.name+'_addresses',values=vals, ignore=True)
        #         if rowcount == 1:
        #             self.address_db.insert_kw(self.name + '_labels', values=[descendant,'auto'], ignore=True)
        #         # self.address_db.insert_kw(self.name+'_ancestry',values=vals)
        #         # self.progenitors[descendant] = progenitor
        #         print('inserting',vals)
        #     # if progenitor not in self.progenitor_names:
        #     #     self.progenitor_names[progenitor] = 'unknown'
        #     #     self.address_db.insert_kw(self.name+'_names', values=[progenitor,"unknown"])
        #     #     print('inserting into names',progenitor)
        #     self.address_db.commit()
        #
        # if type == -1:
        #     return prev_ancestor
        #
        # return ancestor

    def get_contracts(self,transactions):
        contract_list = set()
        counterparty_list = set()
        input_list = set()
        if self.wrapper is not None:
            contract_list.add(self.wrapper)
        for transaction in transactions.values():
            t_contracts, t_counterparties, t_inputs = transaction.get_contracts()
            contract_list = contract_list.union(t_contracts)
            counterparty_list = counterparty_list.union(t_counterparties)
            input_list = input_list.union(t_inputs)
        return list(contract_list), list(counterparty_list), list(input_list)


    def update_progenitors(self,user, counterparty_list):
        pb = 20
        pb_per_contract = 15./len(counterparty_list)
        for idx,contract in enumerate(counterparty_list):
            self.update_address_from_scan(user,contract.lower())
            pb += pb_per_contract
            if idx % 10 == 0:
                progress_bar_update(self.addr, 'Looking up counterparties: '+str(idx)+'/'+str(len(counterparty_list)),pb)
            # if progenitor is None:
            #     print("No progenitor for",contract.lower())





    def transactions_to_log(self,user,coingecko_rates, signatures, transactions, mode='rows'):
        t = time.time()
        all_rows = []

        progress_bar_update(self.addr, 'Classifying transactions', 40)

        pb = 40
        pb_update_per_transaction = 28./len(transactions)

        # print('transactions', len(transactions))
        user.prepare_all_custom_types()
        user.prepare_all_custom_treatment_and_rates()
        classifier = Classifier(self)
        for idx,transaction in enumerate(transactions.values()):
            # transaction.calc_totals()
            # transaction.calc_usd_totals(coingecko_rates)
            transaction.finalize(user,coingecko_rates,signatures)
            txid = transaction.txid
            if txid is not None and txid in user.tx_ctype_mapping:
                type_id = user.tx_ctype_mapping[txid]
                info = user.ctype_info[type_id]
                user.apply_custom_type_one_transaction(self, transaction, type_id, info['name'], info['balanced'], info['rules'])
            else:
                classifier.classify(transaction)
            transaction.add_fee_transfer()
            transaction.infer_and_adjust_rates(user,coingecko_rates)
            user.apply_custom_treatment_or_rate(transaction)
            # transaction.record(user.db)
            # transaction.classify()
            pb += pb_update_per_transaction
            if idx % 10 == 0:
                progress_bar_update(self.addr, 'Classifying transactions', pb)
        log('timing:transactions_to_log 1', time.time() - t)
        user.db.commit()

        print(self.pools)

        t = time.time()
        progress_bar_update(self.addr, 'Preparing transaction data for display', 70)
        pb = 70
        for idx,transaction in enumerate(transactions.values()):
            # print()
            if mode == 'rows':
                rows = transaction.to_rows()
                all_rows.extend(rows)
            else:
                js = transaction.to_json()
                all_rows.append(js)
            pb += pb_update_per_transaction
            # if idx % 10 == 0:
            #     progress_bar_update(self.addr, 'Preparing transaction data for display', pb)

        log('timing:transactions_to_log 2', time.time() - t)

        return all_rows

        # log_file = open('data/'+self.name+'.csv', 'w', newline='')
        # writer = csv.writer(log_file)
        # writer.writerow(['ID', 'Timestamp', 'Quote', 'Base', 'Side', 'Base amount', 'Quote Amount'])
        # writer.writerows(all_rows)
        # log_file.close()