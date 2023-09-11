# from flask import app, current_app
import copy

from sortedcontainers import *
from .sqlite import SQLite
from .util import log, ProgressBar
from .transaction import *
from .coingecko import Coingecko
from .fiat_rates import Twelve
from .signatures import Signatures
from .classifiers import Classifier
from .chain import Chain
from .imports import Import
from .solana import Solana
from .redis_wrap import Redis
from datetime import datetime
import re
import os
import json, csv
import requests
import shutil
from statistics import median
from werkzeug.utils import secure_filename

import hashlib

class User:
    def __init__(self,address, do_logging=False, load_addresses=True):
        version = float(os.environ.get('version'))
        debug_level = int(os.environ.get('debug'))
        self.debug = debug_level > 0
        self.sql_logging = debug_level > 2 or do_logging
        self.last_db_modification_timestamp = 0
        self.current_tokens = None

        # try:
        #     self.debug = g.debug
        # except:
        #     self.debug = True
        #     do_logging = True



        address = normalize_address(address)
        self.address = address


        # if relevant_addresses is None:
        #     relevant_addresses = [address]
        #
        # self.relevant_addresses = relevant_addresses
        self.current_import = None




        # self.rate_sources = ['usd','shortcut','inferred','exact','cg before first','cg after last','cg','adjusted']

        path = 'data/users/'+address
        first_run = False
        if not os.path.exists(path):
            os.makedirs(path)
            first_run = True

        path = 'data/users/'+address+'/db.db'
        if not os.path.exists(path):
            first_run = True
        self.first_run = first_run
        self.db = SQLite('users/' + address+'/db',do_logging=self.sql_logging)



        drop = False
        if first_run:
            self.db.create_table('info','field primary key, value',drop=drop)
            self.db.create_table('custom_names', 'chain, address, name', drop=drop)
            self.db.create_index('custom_names_idx', 'custom_names', 'chain,address', unique=True)

            self.db.create_table('custom_types', 'chain, id integer primary key autoincrement, name, description, balanced integer', drop=drop)
            self.db.create_index('custom_types_idx', 'custom_types', 'chain, name', unique=True)
            self.db.create_table('custom_types_rules', 'id integer primary key autoincrement, type_id integer, '
                                                       'from_addr, from_addr_custom, to_addr, to_addr_custom, token, token_custom, treatment, vault_id, vault_id_custom',
                                 drop=drop)

            self.db.create_table('addresses', 'id integer primary key, chain, address', drop=drop)
            self.db.create_index('addresses_idx', 'addresses', 'address, chain', unique=True)

            self.db.create_table('tokens', 'id integer primary key, chain, contract, symbol, coingecko_id, custom_coingecko_id', drop=drop)
            self.db.create_index('tokens_idx', 'tokens', 'chain, contract, symbol', unique=True)


            self.db.create_table('transactions', 'id integer primary key autoincrement, chain, hash, timestamp INTEGER, nonce INTEGER, block INTEGER, interacted_addr_id INTEGER, originator_addr_id INTEGER, function, custom_type_id INTEGER, custom_color_id INTEGER, custom_note, minimized INTEGER, manual INTEGER, import_id INTEGER, upload_id INTEGER',drop=drop)
            self.db.create_index('transactions_idx', 'transactions', 'hash', unique=True)


            self.db.create_table('transaction_transfers', 'id integer primary key autoincrement, type INTEGER, transaction_id INTEGER, from_addr_id INTEGER, to_addr_id INTEGER, val REAL, token_id INTEGER, token_nft_id TEXT, base_fee REAL, input_len INTEGER, input, '
                                                          'custom_treatment, custom_rate REAL, custom_vaultid, synthetic INTEGER DEFAULT 0, import_id INTEGER, upload_id INTEGER', drop=drop)
            self.db.create_index('transaction_transfers_idx1', 'transaction_transfers', 'from_addr_id')
            self.db.create_index('transaction_transfers_idx2', 'transaction_transfers', 'to_addr_id')
            self.db.create_index('transaction_transfers_idx3', 'transaction_transfers', 'transaction_id')
            # self.db.create_index('transaction_transfers_idx', 'transaction_transfers', 'idx, transaction_id', unique=True)

            self.db.create_table('transactions_derived', 'id integer primary key, category INTEGER, claim INTEGER, nft INTEGER, protocol, protocol_note, certainty, cp_progenitor, cp_address, cp_name, sig_hex, sig_decoded, balanced INTEGER DEFAULT 0', drop=drop)
            self.db.create_table('transaction_transfers_derived', 'id integer, coingecko_id, rate REAL, rate_found INTEGER, rate_source, treatment, vault_id', drop=drop)
            self.db.create_index('transaction_transfers_derived_idx2', 'transaction_transfers_derived', 'id')
            # self.db.create_table('transaction_transfers_derived', 'idx INTEGER, transaction_id INTEGER, coingecko_id, rate REAL, rate_found INTEGER, rate_source, treatment, vault_id', drop=drop)
            # self.db.create_index('transaction_transfers_derived_idx', 'transaction_transfers_derived', 'idx, transaction_id', unique=True)

            self.db.create_table('user_addresses', 'id integer primary key autoincrement, address, chain, previously_used INTEGER DEFAULT 0, present INTEGER DEFAULT 0, last_update INTEGER DEFAULT 0, is_upload INTEGER DEFAULT 0', drop=False)
            self.db.create_index('user_addresses_idx', 'user_addresses', 'address, chain', unique=True)

            self.db.create_table('latest_token_amounts', 'user_address_id INTEGER, token_id INTEGER, nft_id, amount REAL, debank_rate REAL, nft_eth_floor REAL', drop=drop)
            self.db.create_index('latest_token_amounts_idx', 'latest_token_amounts', 'user_address_id, token_id, nft_id', unique=True)

            self.db.create_table('imports', 'id integer primary key autoincrement, started INTEGER, ended INTEGER, version NUMERIC, status INTEGER', drop=drop)
            self.db.create_table('imports_addresses', 'import_id integer, chain TEXT, address TEXT', drop=drop)
            self.db.create_index('imports_addresses_idx', 'imports_addresses', 'import_id')
            self.db.create_table('imports_errors', 'id integer primary key autoincrement, import_id integer, chain TEXT, address TEXT, txtype INTEGER, error_code INTEGER, additional_text TEXT, debug_info TEXT', drop=drop)
            self.db.create_index('imports_errors_idx', 'imports_errors', 'import_id')

            self.db.create_table('uploads', 'id integer primary key autoincrement, started INTEGER, ended INTEGER, version NUMERIC, status INTEGER, source', drop=drop)


            self.db.commit()
            self.set_info('version', version)
            self.set_info('data_version', version)

            self.make_sample_types()
        else:
            self.last_db_modification_timestamp = os.path.getmtime('data/users/' + address+'/db.db')
            current_version = self.get_info('version')
            data_version = self.get_info('data_version')
            log('version',current_version)
            if data_version is None:
                data_version = current_version
            if data_version is None:
                data_version = 1
            if (current_version is None or float(current_version) != version):
                self.make_backup(current_version)

            commit = False


            if current_version is None:
                self.db.do_error_logging = self.debug
                try:
                    self.db.create_table('info', 'field primary key, value',drop=False)
                    self.db.create_table('transactions_derived',
                                         'id integer primary key, category INTEGER, claim INTEGER, nft INTEGER, protocol, protocol_note, certainty, cp_progenitor, cp_address, cp_name, sig_hex, sig_decoded, balanced INTEGER DEFAULT 0', drop=drop)
                    self.db.create_table('transaction_transfers_derived', 'id integer, coingecko_id, rate REAL, rate_found INTEGER, rate_source, treatment, vault_id',
                                         drop=drop)
                    self.db.commit()
                except:
                    pass

                try:
                    self.db.create_table('user_addresses',
                                         'id integer primary key autoincrement, address, chain, previously_used INTEGER DEFAULT 0, present INTEGER DEFAULT 0, last_update INTEGER DEFAULT 0',
                                         drop=True)
                    self.db.create_index('user_addresses_idx', 'user_addresses', 'address, chain', unique=True)
                    self.db.commit()
                except:
                    pass



                try :
                    self.db.create_index('transaction_transfers_idx3', 'transaction_transfers', 'transaction_id')
                    self.db.commit()
                except:
                    pass



                update_queries = [
                    'ALTER TABLE transactions ADD COLUMN nonce INTEGER',
                    'ALTER TABLE transactions ADD COLUMN block INTEGER',
                    'ALTER TABLE transactions ADD COLUMN manual INTEGER',
                    'ALTER TABLE transactions ADD COLUMN custom_note',
                    'ALTER TABLE transaction_transfers ADD COLUMN synthetic INTEGER DEFAULT 0',
                    'ALTER TABLE transactions_derived ADD COLUMN protocol_note',
                    'ALTER TABLE transactions_derived ADD COLUMN balanced INTEGER DEFAULT 0',

                    #get rid of idx+txid lookup on derived transfer data, replace with transfer id lookup
                    'ALTER TABLE transaction_transfers_derived ADD COLUMN id INTEGER',
                    'UPDATE transaction_transfers_derived SET id = (SELECT id FROM transaction_transfers as tt WHERE tt.transaction_id=transaction_transfers_derived.transaction_id and tt.idx=transaction_transfers_derived.idx) WHERE transaction_transfers_derived.id IS NULL',
                    'UPDATE transaction_transfers_derived SET id = -1 WHERE id IS NULL',
                    'DROP INDEX transaction_transfers_idx',
                    'DROP INDEX transaction_transfers_derived_idx',

                    #multiple address lookup
                    'CREATE INDEX transaction_transfers_idx1 ON transaction_transfers(from_addr_id)',
                    'CREATE INDEX transaction_transfers_idx2 ON transaction_transfers(to_addr_id)'

                ]



                for chain_name in Chain.list():
                    # presence = self.get_info(chain_name+"_presence")
                    present = 0
                    adr_id_rows = self.db.select("SELECT id FROM addresses WHERE chain='"+chain_name+"' AND address='"+self.address+"'")
                    if len(adr_id_rows) == 1:
                        present = 1
                    used = self.get_info(chain_name + "_used")
                    if used or (used is None and present):
                        used = 1
                    else:
                        used = 0
                    last_update = self.get_info(chain_name + "_last_update")
                    if last_update is None:
                        last_update = 0
                    self.db.insert_kw('user_addresses',address=self.address, chain=chain_name, previously_used=used, present=present, last_update=last_update)
                    fields = [chain_name+"_presence",chain_name+"_used",chain_name+"_last_update"]
                    self.db.query("DELETE FROM info WHERE field IN "+sql_in(fields))

                self.db.do_error_logging = False
                for query in update_queries:
                    try:
                        self.db.query(query)
                        commit = True
                    except:
                        pass


                try:
                    self.db.create_index('transaction_transfers_derived_idx2', 'transaction_transfers_derived', 'id')
                    commit = True
                except:
                    pass



                self.db.do_error_logging = True
                current_version = 1.2
            else:
                current_version = float(current_version)

            if current_version < 1.3:
                rows = self.db.select("SELECT address,chain FROM user_addresses WHERE present IS NULL")
                for row in rows:
                    adr = row[0]
                    chain_name = row[1]
                    adr_id_rows = self.db.select("SELECT id FROM addresses WHERE chain='" + chain_name + "' AND address='" + adr + "'")
                    prused = 0
                    if len(adr_id_rows) == 1:
                        prused = 1
                    self.db.query("UPDATE user_addresses SET previously_used="+str(prused)+", present="+str(prused)+" WHERE chain='" + chain_name + "' AND address='" + adr + "'")


                self.db.create_table('latest_token_amounts', 'user_address_id INTEGER, token_id INTEGER, nft_id, amount REAL, debank_rate REAL, nft_eth_floor REAL', drop=drop)
                self.db.create_index('latest_token_amounts_idx', 'latest_token_amounts', 'user_address_id, token_id, nft_id', unique=True)
                self.db.query('ALTER TABLE transactions ADD COLUMN interacted_addr_id INTEGER')
                self.db.query('ALTER TABLE transactions ADD COLUMN function')
                self.db.query('ALTER TABLE transactions ADD COLUMN import_id INTEGER')
                self.db.query('ALTER TABLE transaction_transfers ADD COLUMN import_id INTEGER')

                self.db.create_table('imports', 'id integer primary key autoincrement, started INTEGER, ended INTEGER, version NUMERIC, status INTEGER', drop=drop)
                self.db.create_table('imports_addresses', 'import_id integer, chain TEXT, address TEXT', drop=drop)
                self.db.create_index('imports_addresses_idx', 'imports_addresses', 'import_id')
                self.db.create_table('imports_errors', 'id integer primary key autoincrement, import_id integer, chain TEXT, address TEXT, txtype INTEGER, error_code INTEGER, additional_text TEXT, debug_info TEXT, txhash TEXT', drop=drop)
                self.db.create_index('imports_errors_idx', 'imports_errors', 'import_id')

            if current_version < 1.4:
                self.db.query('ALTER TABLE user_addresses ADD COLUMN is_upload INTEGER DEFAULT 0')
                self.db.query('ALTER TABLE transactions ADD COLUMN upload_id INTEGER')
                self.db.query('ALTER TABLE transaction_transfers ADD COLUMN upload_id INTEGER')

                self.db.create_table('uploads', 'id integer primary key autoincrement, started INTEGER, ended INTEGER, version NUMERIC, status INTEGER, source', drop=drop)

                self.db.query('ALTER TABLE tokens ADD COLUMN coingecko_id')
                self.db.query('ALTER TABLE tokens ADD COLUMN custom_coingecko_id')

                try:
                    coingecko_id_update_query = "select token_id, coingecko_id, count( distinct coingecko_id) " \
                        "from transaction_transfers as t, transaction_transfers_derived as td " \
                        "where t.id = td.id and coingecko_id is not NULL " \
                        "group by token_id"
                    rows = self.db.select(coingecko_id_update_query)
                    for row in rows:
                        token_id, coingecko_id, cnt = row
                        if cnt == 1:
                            self.db.update_kw("tokens","id="+str(token_id),coingecko_id=coingecko_id)
                except:
                    log_error("Failed executing coingecko_id_update_query")

            if current_version < 1.41:
                self.db.query('ALTER TABLE transactions ADD COLUMN originator_addr_id INTEGER')

            if current_version < 1.42:
                self.db.query('ALTER TABLE transactions ADD COLUMN minimized INTEGER')

            if current_version != version:
                self.set_info('data_version', data_version)
                self.set_info('version',version)
                # self.set_info('update_import_needed',1)
                commit = True
            if commit:
                self.db.commit()



        self.custom_addresses = {}
        self.custom_rates = {}
        self.version = version

        self.fiat = 'USD'
        fiat = self.get_info('fiat')
        if fiat is not None:
            self.fiat = fiat
        self.fiat_rates = Twelve(self.fiat)

        # if load_fiat:
        #     self.load_fiat()

        if load_addresses:
            self.load_addresses()

    def load_fiat(self):
        self.fiat_rates.init_rates()

    def make_backup(self,backup_version):
        if backup_version is None:
            backup_version = 1
        backup_version_str = str(backup_version).replace(".","").rjust(3,"0")
        path = 'data/users/' + self.address
        shutil.copy(path+"/db.db",path+"/db_backup_"+backup_version_str+".db")

    def done(self):
        try:
            self.db.disconnect()
        except:
            pass



    #not fugly
    def load_addresses(self):
        all_info = self.db.select("SELECT a.id, a.address, a.chain, ua.previously_used, ua.present, ua.last_update "
                                  "FROM user_addresses as ua, addresses as a "
                                  "WHERE ua.chain = a.chain AND ua.address = a.address")
        all_addresses = defaultdict(dict)
        # relevant_addresses = set()
        relevant_address_ids = set()
        # relevant_address_pairs = set()
        for row in all_info:
            id, address, chain, previously_used, present, last_update = row
            all_addresses[address][chain] = {'present':present == 1, 'used':previously_used == 1,'last_update':last_update, 'id':id}
            if previously_used:
                # relevant_addresses.add(address)
                relevant_address_ids.add(id)
                # relevant_address_pairs.add(chain+":"+address)

        self.all_addresses = dict(all_addresses)
        # self.relevant_addresses = list(relevant_addresses)
        self.relevant_address_ids = relevant_address_ids#list(relevant_address_ids)
        # self.relevant_address_pairs = list(relevant_address_pairs)


    def load_tx_counts(self):
        id_mapping = {}
        for address in self.all_addresses:
            for chain in self.all_addresses[address]:
                id_mapping[self.all_addresses[address][chain]['id']] = self.all_addresses[address][chain]
        id_list = sql_in(list(id_mapping.keys()))
        q = "select count(distinct id) as c,uid from " \
            "(select distinct tx.id, tr.from_addr_id as uid " \
            "from transactions as tx, transaction_transfers as tr " \
            "WHERE tx.id = tr.transaction_id and from_addr_id in "+id_list+" and tr.val > 0 " \
            "UNION ALL " \
            "select distinct tx.id, tr.to_addr_id as uid " \
            "from transactions as tx, transaction_transfers as tr " \
            "WHERE tx.id = tr.transaction_id and to_addr_id in "+id_list +" and tr.val > 0 " \
            "order by tx.id) " \
            "group by uid"
        rows = self.db.select(q)
        log("tx counts",rows,filename='address_proc.txt')
        for row in rows:
            cnt, id = row
            id_mapping[id]['tx_count'] = cnt

    def check_user_address(self,chain,address, strict=False):

        try:

            #needs to be strict in classifier
            if strict and self.all_addresses[address][chain]['used']:
                return True
            if not strict and address in self.all_addresses:
                return True
        except:
            pass
        return False

    def load_solana_nfts(self):


        nft_data = {}
        nft_rows = self.db.select("SELECT * FROM tokens WHERE chain='Solana' AND symbol LIKE '%~|~%'")
        for row in nft_rows:
            spl = row[3].split("~|~")

            nft_data[row[2]] = [False]+spl
        self.solana_nft_data = nft_data

        proxies = {}
        nft_rows = self.db.select("SELECT * FROM tokens WHERE chain='Solana' AND symbol LIKE 'proxy for:'")
        for row in nft_rows:
            proxies[row[2]] = [False]+[row[3][10:]]
        self.solana_proxy_map = proxies

        self.solana_cid_to_proxies_map = {}
        for proxy, entry in self.solana_proxy_map.items():
            token_address = entry[1]
            if token_address in self.solana_nft_data:
                dt = self.solana_nft_data[token_address]
                token_id = dt[2]
                if dt[2] != '':
                    token_id += ":"+dt[3]
                if token_id not in self.solana_cid_to_proxies_map:
                    self.solana_cid_to_proxies_map = {'token_address':token_address,'proxies':set()}
                self.solana_cid_to_proxies_map[token_id]['proxies'].add(proxy)

    def store_solana_nfts(self):
        do_commit = False
        for nft_address, data in self.solana_nft_data.items():
            new_entry, symbol, token_id, name = data
            if new_entry:
                self.db.insert_kw('tokens', chain='Solana', contract=nft_address, symbol=symbol + "~|~" + token_id + "~|~" + name)
                do_commit = True

        for proxy, data in self.solana_proxy_map.items():
            new_entry, token = data
            if new_entry:
                self.db.insert_kw('tokens', chain='Solana', contract=proxy, symbol='proxy for:'+token)
                do_commit = True

        if do_commit:
            self.db.commit()

    def check_address_present(self,address,chain_name):
        try:
            return self.all_addresses[address][chain_name]['present']
        except:
            return False

    def check_address_used(self,address,chain_name):
        try:
            return self.all_addresses[address][chain_name]['used']
        except:
            return False

    def set_address_present(self, address, chain_name,value=1,commit=True):
        self.db.insert_kw('user_addresses', address=address, chain=chain_name, ignore=True)
        self.db.update_kw('user_addresses', "address='" + address + "' AND chain='" + chain_name + "'", present=value)
        if commit:
            self.db.commit()
        # self.all_addresses[address][chain_name]['present'] = value==1
        # self.load_addresses()

    def set_address_used(self, address, chain_name,value=1,commit=True):
        self.db.insert_kw('user_addresses', address=address, chain=chain_name, ignore=True)
        self.db.update_kw('user_addresses', "address='" + address + "' AND chain='" + chain_name + "'", previously_used=value)
        if commit:
            self.db.commit()
        # self.all_addresses[address][chain_name]['used'] = value==1
        # self.load_addresses()

    def set_address_update(self, address, chain_name):
        now = int(time.time())
        self.db.insert_kw('user_addresses', address=address, chain=chain_name, ignore=True)
        self.db.update_kw('user_addresses', "address='" + address + "' AND chain='" + chain_name + "'", last_update=now)
        self.db.commit()
        # self.all_addresses[address][chain_name]['last_update'] = now
        # self.load_addresses()

    # def set_address_present(self,address,chain_name):
    #     if address not in self.all_addresses:
    #         self.all_addresses[address] = {}
    #     if chain_name not in self.all_addresses[address]:
    #         self.all_addresses[address][chain_name] = {}
    #         self.db.insert_kw('user_addresses', address=address, chain=chain_name)
    #     self.all_addresses[address][chain_name]['present'] = True
    #     self.db.update_kw('user_addresses',"address='"+address+"' AND chain='"+chain_name+"'",present=1)
    #     self.db.commit()


    #fugly
    # def load_addresses(self):
    #     all_info = self.db.select("SELECT * FROM info")
    #     # chain_list = Chain.list()
    #     all_addresses = []
    #     relevant_addresses = []
    #     for field,value in all_info:
    #         if "_used" in field or "_presence" in field:
    #             try:
    #                 chain_name,address,val_type = field.split("_",2)
    #                 if address not in all_addresses:
    #                     all_addresses.append(address)
    #                 if val_type == 'used' and float(value) == 1 and address not in relevant_addresses:
    #                     relevant_addresses.append(address)
    #             except:
    #                 pass
    #     self.all_addresses = all_addresses
    #     self.relevant_addresses = relevant_addresses



    def make_sample_types(self):
        self.db.insert_kw('custom_types',chain='ALL',name='Swap',
                          description='This is a generic swap of one token for another. It can also be used to just sell, or just buy a token.',balanced=1)
        self.db.insert_kw('custom_types_rules',type_id=1,from_addr='my_address',to_addr='any',token='any',treatment='sell',vault_id='address')
        self.db.insert_kw('custom_types_rules', type_id=1, from_addr='any', to_addr='my_address', token='any', treatment='buy', vault_id='address')
        self.db.insert_kw('custom_types', chain='ALL', name='Claim reward',
                          description='You can use this type when getting staking rewards, or just generally getting tokens out of thin air.', balanced=1)
        self.db.insert_kw('custom_types_rules', type_id=2, from_addr='any', to_addr='my_address', token='any', treatment='income', vault_id='address')
        self.db.insert_kw('custom_types', chain='ALL', name='Spam',
                          description='You can use this type when receiving spammy airdrops of scam tokens or worthless NFTs.', balanced=0)
        self.db.insert_kw('custom_types_rules', type_id=3, from_addr='any', to_addr='my_address', token='any', treatment='ignore', vault_id='address')
        self.db.insert_kw('custom_types', chain='ALL', name='Bridge',
                          description='Apply this type to bridging transactions -- both outgoing and incoming.', balanced=0)
        self.db.insert_kw('custom_types_rules', type_id=4, from_addr='my_address', to_addr='any', token='any', treatment='deposit', vault_id='other', vault_id_custom='Bridge')
        self.db.insert_kw('custom_types_rules', type_id=4, from_addr='any', to_addr='my_address', token='any', treatment='exit', vault_id='other', vault_id_custom='Bridge')
        self.db.commit()


    def get_contracts(self,transactions):
        contract_dict = {}
        counterparty_by_chain = defaultdict(set)
        # counterparty_list = set()
        input_list = set()
        for transaction in transactions:
            counterparty_list = counterparty_by_chain[transaction.chain.name]
            ts = transaction.ts
            t_contracts, t_counterparties, t_inputs = transaction.get_contracts()
            for contract in t_contracts:
                cp_pair = transaction.chain.name+":"+contract
                if cp_pair not in contract_dict or contract_dict[cp_pair] is None:
                    contract_dict[cp_pair] = ts
                elif ts > contract_dict[cp_pair]:
                    contract_dict[cp_pair] = ts
            counterparty_by_chain[transaction.chain.name] = counterparty_list.union(t_counterparties)
            input_list = input_list.union(t_inputs)
        return contract_dict, counterparty_by_chain, list(input_list)


    def get_needed_token_times(self,transactions):
        result = {}
        for transaction in transactions:
            ts = transaction.ts
            for type, sub_data, _, _, _, _, _, _ in transaction.grouping:
                hash, ts, nonce, block, fr, to, val, token, token_contract, coingecko_id, token_nft_id, base_fee, input_len, input = sub_data
                if coingecko_id is not None:
                    if coingecko_id not in result:
                        result[coingecko_id] = set()
                    result[coingecko_id].add(ts)
            # t_contracts, t_counterparties, t_inputs = transaction.get_contracts()
            # for contract in t_contracts:
            #     if contract not in result[chain_name]:
            #         result[chain_name][contract] = set()
            #     result[chain_name][contract].add(ts)
            #     log("Adding to needed token times",transaction.hash,contract,ts,filename='coingecko2.txt')
        return result

    def locate_insert_transaction(self,chain_name,hash,timestamp,nonce,block, interacted, originator, function, custom_note=None,upload_id=None,minimized=None):
        db = self.db
        rows = db.select("SELECT * FROM transactions WHERE hash='" + hash + "'",return_dictionaries=True)
        interacted_addr_id = self.locate_insert_address(chain_name, interacted)  # interacted might be None
        originator_addr_id = self.locate_insert_address(chain_name, originator)
        import_id = None
        if self.current_import is not None:
            import_id = self.current_import.id
        if len(rows) == 1:
            row = rows[0]
            update_fields = {'chain':chain_name,'interacted_addr_id':interacted_addr_id,'originator_addr_id':originator_addr_id,'function':function,'import_id':import_id,'custom_note':custom_note,'upload_id':upload_id}
            for field in update_fields:
                if row[field] != update_fields[field]:
                    db.update_kw('transactions','id='+str(row['id']),**update_fields)
                    break

            return row['id'], True
        else:
            db.insert_kw('transactions', chain=chain_name, hash=hash,timestamp=timestamp,nonce=nonce,block=block,
                         interacted_addr_id=interacted_addr_id,originator_addr_id=originator_addr_id,function=function,import_id=import_id,
                         custom_note=custom_note, upload_id=upload_id, minimized=minimized)
            return db.select("SELECT last_insert_rowid()")[0][0], False

    def locate_insert_address(self,chain_name,address):
        db = self.db
        if address is None:
            row = db.select("SELECT id FROM addresses WHERE LOWER(chain)=LOWER('" + chain_name + "') and address IS NULL")
        else:
            row = db.select("SELECT id FROM addresses WHERE LOWER(chain)=LOWER('" + chain_name + "') and address = '" + address + "'")
        if len(row) == 1:
            return row[0][0]
        else:
            db.insert_kw('addresses', chain=chain_name, address=address)
            return db.select("SELECT last_insert_rowid()")[0][0]

    def locate_insert_token(self,chain_name,contract,symbol,coingecko_id=None):
        # log('locate_insert_token', chain_name, contract, symbol, filename='token_insert.txt')
        db = self.db
        assert contract is not None or symbol is not None
        if symbol is None:
            symbol = contract.upper()
        else:
            symbol = symbol.upper()
        if contract is None:
            contract = symbol.upper()

        if len(contract) < 20 and contract.upper() == symbol:
            contract = contract.upper()

        # if contract is None or contract.upper() == symbol:
        #     Q = "SELECT id FROM tokens WHERE chain='" + chain_name + "' and symbol = '" + symbol + "' and (contract IS NULL OR contract ='"+symbol+"') ORDER BY id ASC"
        # else:
        #     Q = "SELECT id FROM tokens WHERE chain='" + chain_name + "' and contract = '" + contract + "'  ORDER BY id ASC"
        Q = "SELECT id, coingecko_id FROM tokens WHERE LOWER(chain)=LOWER('" + chain_name + "') and contract = '" + contract + "'  ORDER BY id ASC"
        rows = db.select(Q)

        if len(rows) >= 1:
            # log('locate_insert_token-found ',len(row), row[0][0], filename='token_insert.txt')
            id, current_coingecko_id = rows[0]
            if coingecko_id is not None and coingecko_id != current_coingecko_id:
                db.update_kw('tokens','id='+str(id),coingecko_id=coingecko_id)
            return id
        else:
            db.insert_kw('tokens', chain=chain_name, contract=contract, symbol=symbol,coingecko_id=coingecko_id)
            id = db.select("SELECT last_insert_rowid()")[0][0]
            # log('locate_insert_token-inserting',id, chain_name, contract, symbol, filename='token_insert.txt')
            return id




    def wipe_transactions(self):
        if not self.first_run:
            self.make_backup('manual_wipe')
        try:
            self.db.disconnect()
        except:
            pass
        path = 'data/users/' + self.address
        os.remove(path+"/db.db")
        filenames = ["db.db","transactions.json","rates","data_cache.json","calculator_cache"]
        for filename in filenames:
            try:
                os.remove(path+"/"+filename)
            except:
                pass
        try:
            redis = Redis(self.address)
            to_delete = [self.address + "_progress", self.address + "_progress_entry", self.address + "_last_update", self.address +"_running"]
            redis.R.delete(*to_delete)
            redis.deq()
        except:
            pass

        try:
            shutil.rmtree(path+"/uploads")
        except:
            pass
        self.__init__(self.address,load_addresses=True)

    def restore_backup(self):
        path = 'data/users/' + self.address
        shutil.copy(path + "/db_backup_manual_wipe.db", path + "/db.db")


    def store_current_tokens(self,chain,current_tokens):
        if current_tokens is None:
            return

        log("storing tokens",chain.name,current_tokens)
        db = self.db
        for address, tokens in current_tokens.items():
            row = db.select("SELECT id FROM addresses WHERE chain='" + chain.name + "' and address = '" + address+"'")
            address_id = row[0][0]
            db.query('DELETE FROM latest_token_amounts WHERE user_address_id=' + str(address_id))
            if tokens is not None:
                for contract,token_data in tokens.items():
                    symbol = token_data['symbol']
                    # if contract == chain.main_asset:
                    #     contract = None
                    token_id = self.locate_insert_token(chain.name, contract, symbol)

                    if 'nft_amounts' in token_data:
                        eth_floor = None
                        if 'eth_floor' in token_data:
                            eth_floor = token_data['eth_floor']

                        for nft_id, amount in token_data['nft_amounts'].items():
                            db.insert_kw('latest_token_amounts', user_address_id=address_id, token_id=token_id, nft_id=nft_id, amount=amount, nft_eth_floor=eth_floor)

                    if 'amount' in token_data:
                        amount = token_data['amount']
                        debank_rate = None
                        if 'rate' in token_data:
                            debank_rate = token_data['rate']

                        db.insert_kw('latest_token_amounts', user_address_id=address_id, token_id=token_id, amount=amount, debank_rate=debank_rate)


        db.commit()

    def load_current_tokens(self,coingecko):
        db = self.db
        t = int(time.time())
            # addresses = [self.address]

        query = "select a.chain, a.address, tokens.contract, tokens.symbol, lta.nft_id, lta.amount, lta.debank_rate, lta.nft_eth_floor " \
                "from latest_token_amounts as lta, tokens, addresses as a " \
                "where lta.user_address_id in "+sql_in(self.relevant_address_ids)+" and lta.user_address_id=a.id and lta.token_id = tokens.id"

        log("TOKEN AMOUNTS QUERY", query)
        rows = db.select(query)
        log('resp len', len(rows))
        rv = {}
        _, eth_rate, _ = self.lookup_rate_including_custom(coingecko,'ETH','ETH',t)
        fiat_rate = self.fiat_rates.lookup_rate(self.fiat,t)
        for row in rows:
            chain_name, address, contract, symbol, nft_id, amount, debank_rate, nft_eth_floor = row
            if chain_name not in rv:
                rv[chain_name] = {}
            if address not in rv[chain_name]:
                rv[chain_name][address] = {}
            if contract is None:
                contract = symbol
            if contract not in rv[chain_name][address]:
                rv[chain_name][address][contract] = {'symbol':symbol}
            if nft_id is not None:
                if 'nft_amounts' not in rv[chain_name][address][contract]:
                    rv[chain_name][address][contract]['nft_amounts'] = {}
                rv[chain_name][address][contract]['nft_amounts'][nft_id] = amount
                if nft_eth_floor is not None:
                    rv[chain_name][address][contract]['rate'] = [1,nft_eth_floor*eth_rate*fiat_rate,'opensea_nft_floor']
            else:
                rv[chain_name][address][contract]['amount'] = amount
                if debank_rate is None:
                    rate_found, rate, rate_source = self.lookup_rate_including_custom(coingecko,chain_name,contract,t-3600)
                    log('load_current_tokens rate lookup',chain_name,contract,rate_found,rate,rate_source)
                    if rate_source is not None and 'after last' in rate_source:
                        rate_found = 1
                    if rate is not None:
                        rate *= fiat_rate
                    rv[chain_name][address][contract]['rate'] = [rate_found, rate, rate_source]

                else:
                    if debank_rate is not None:
                        debank_rate *= fiat_rate
                    rv[chain_name][address][contract]['rate'] = [1,debank_rate,'debank']

        self.current_tokens = rv
        return rv


    def store_transactions(self,chain,transactions, import_addresses, coingecko):
        chain.update_pb('Storing transactions',0)
        db = self.db

        import_address_ids = []
        for adr in import_addresses:
            id = self.locate_insert_address(chain.name,adr)
            import_address_ids.append(id)

        for idx, transaction in enumerate(transactions.values()):
            hash, timestamp, nonce, block = transaction.grouping[0][1][0:4]

            # db.insert_kw('transactions', chain=chain.name, hash=hash, timestamp=timestamp)
            # txid = db.select("SELECT last_insert_rowid()")[0][0]
            txid, existing = self.locate_insert_transaction(chain.name,hash,timestamp,nonce,block,transaction.interacted,transaction.originator,transaction.function, transaction.minimized)

            new_transfers = {}
            tx_has_suspect_amount = False
            for index_, (type, sub_data, id, _, _, _, synthetic, _) in enumerate(transaction.grouping):
                if synthetic & Transfer.SUSPECT_AMOUNT:
                    log("Found suspect transfer",hash,filename='suspects.txt')
                    tx_has_suspect_amount = True
                    break

            for index_, (type, sub_data, id, _, _, _, synthetic, _) in enumerate(transaction.grouping):
                hash, ts, nonce, block, fr, to, val, token, token_contract, _, token_nft_id, base_fee, input_len, input = sub_data
                fr_id = self.locate_insert_address(chain.name, fr)
                to_id = self.locate_insert_address(chain.name, to)
                coingecko_id = coingecko.lookup_id(chain.name,token_contract)
                log('locate_insert_token',chain.name,token_contract,token,coingecko_id,filename='tokens.txt')
                token_id = self.locate_insert_token(chain.name, token_contract, token, coingecko_id=coingecko_id)


                if chain.name != 'Solana':
                    if input is not None:
                        input = input[:20]

                #exclude amount from transfer comparison if there's an untrustworthy transfer
                if tx_has_suspect_amount:
                    valstr = "!!!"
                else:
                    try:
                        valstr = str(dec(val, 6))
                    except:
                        valstr = str(val)

                tr_hash_fields = [str(fr_id), str(to_id), valstr, str(token_id), str(token_nft_id)]
                tr_hash = "|".join(tr_hash_fields)
                transfer = {'type': type, 'transaction_id': txid, 'from_addr_id': fr_id, 'to_addr_id': to_id,
                            'val': val, 'token_id': token_id, 'token_nft_id': token_nft_id, 'base_fee': base_fee, 'input_len': input_len, 'input': input,
                            'synthetic': synthetic & 0x1111111111, 'import_id':self.current_import.id}
                if tr_hash not in new_transfers:
                    new_transfers[tr_hash] = [transfer]
                else:
                    new_transfers[tr_hash].append(transfer)

            new_hashes = list(new_transfers.keys())
            clog(transaction,"storing, new",new_transfers)

            ids_to_delete = []
            transfers_to_add = []
            if existing:
                rows = self.db.select("select * from transaction_transfers as tt where transaction_id=" + str(txid) +
                                      " and (from_addr_id in "+sql_in(import_address_ids)+" or to_addr_id in "+sql_in(import_address_ids)+")", return_dictionaries=True)
                existing_transfers = {}
                for row in rows:
                    if tx_has_suspect_amount:
                        valstr = "!!!"
                    else:
                        try:
                            valstr = str(dec(row['val'],6))
                        except:
                            valstr = str(row['val'])

                    tr_hash_fields = [str(row['from_addr_id']),str(row['to_addr_id']),valstr,str(row['token_id']),str(row['token_nft_id'])]
                    tr_hash = "|".join(tr_hash_fields)
                    if tr_hash not in existing_transfers:
                        existing_transfers[tr_hash] = [row]
                    else:
                        existing_transfers[tr_hash].append(row)
                clog(transaction,"storing, existing", existing_transfers)

                for tr_hash, identical_existing_transfers in existing_transfers.items():
                    #possibly overwrite old transfers on perfect import
                    if tr_hash in new_transfers:
                        identical_new_transfers = new_transfers[tr_hash]
                    else:
                        identical_new_transfers = []
                    len_diff = len(identical_existing_transfers)-len(identical_new_transfers)
                    if len_diff > 0:
                        for i in range(len_diff):
                            ids_to_delete.append(identical_existing_transfers[i]['id'])
                    if len_diff < 0:
                        for i in range(-len_diff):
                            transfers_to_add.append(identical_new_transfers[i])
                    if tr_hash in new_transfers:
                        del new_transfers[tr_hash]

            #unaccounted remainder
            clog(transaction,"storing, unaccounted", new_transfers)
            for tr_hash, identical_new_transfers in new_transfers.items():
                transfers_to_add.extend(identical_new_transfers)

            if existing and len(ids_to_delete) > 0:
                clog(transaction,'transfer overwrite',chain.name,txid,hash,'ids_to_delete',ids_to_delete,'transfers_to_add',transfers_to_add,'existing hashes',list(existing_transfers.keys()),'new hashes',new_hashes,filename='overwrites.txt')

            if len(ids_to_delete) > 0:
                clog(transaction,"storing, ids to delete", ids_to_delete)
                db.query('DELETE FROM transaction_transfers WHERE id IN '+sql_in(ids_to_delete))

            for prepared_transfer in transfers_to_add:
                db.insert_kw('transaction_transfers', **prepared_transfer)

        db.commit()
        chain.update_pb('Storing transactions', 0)

    # def set_last_update(self,address,chain):
    #     db = self.db
    #     db.insert_kw('info', field=chain.name +"_"+address+ "_last_update", value=int(time.time()))
    #     db.commit()

    def wipe_derived_data(self,transactions=None):
        db = self.db
        if transactions is not None:
            for transaction in transactions:
                id = transaction.txid

                db.query("DELETE FROM transaction_transfers_derived WHERE id IN (SELECT id FROM transaction_transfers WHERE transaction_id=" + str(id) + ")")
                db.query("DELETE FROM transactions_derived WHERE id=" + str(id))
        else:
            db.query("DELETE FROM transaction_transfers_derived")
            db.query("DELETE FROM transactions_derived")
        db.commit()

    def store_derived_data(self,transaction):
        db = self.db
        category = protocol = cp_progenitor = cp_address = cp_name = sig_hex = sig_decoded = protocol_note = None
        certainty = claim = nft = balanced = 0
        id = transaction.txid
        type = transaction.type
        if isinstance(type, Category):
            category = type.category
            certainty = type.certainty
            nft = type.nft
            claim = type.claim
            protocol = type.protocol
        if hasattr(transaction,'protocol_note'):
            protocol_note = transaction.protocol_note

        if len(transaction.counter_parties) == 1:
            cp_progenitor = list(transaction.counter_parties.keys())[0]
            cp_name, sig_hex, sig_decoded, _, cp_address = transaction.counter_parties[cp_progenitor]





        for transfer in transaction.transfers.values():
            log("Storing derived transfer",id,transaction.hash,transfer.coingecko_id,transfer.rate)
            db.insert_kw('transaction_transfers_derived',id=transfer.id, coingecko_id=transfer.coingecko_id,
                         rate=transfer.rate, rate_found = transfer.rate_found, rate_source=transfer.rate_source,
                         treatment = transfer.treatment, vault_id = transfer.vault_id)
        db.insert_kw('transactions_derived',id=id,category=category,claim=claim,nft=nft,protocol=protocol,protocol_note=protocol_note,certainty=certainty,
                     cp_progenitor=cp_progenitor,cp_address=cp_address,cp_name=cp_name,sig_hex=sig_hex,sig_decoded=sig_decoded,balanced=transaction.balanced)


        # self.db.create_table('transactions_derived', 'id integer primary key, category INTEGER, claim INTEGER, nft INTEGER, protocol, certainty, cp_progenitor, cp_address, cp_name, sig_hex, sig_decoded', drop=drop)
        # self.db.create_table('transaction_transfers_derived',
        #                      'id integer primary key, idx INTEGER, transaction_id INTEGER, coingecko_id, rate REAL, rate_found INTEGER, rate_source, treatment, vault_id', drop=drop)


    def set_info(self,field,value):
        db = self.db
        db.insert_kw('info', field=field, value=value)
        db.commit()

    def get_info(self,field):
        rows = self.db.select("SELECT value FROM info WHERE field='"+field+"'")
        if len(rows) == 1:
            return rows[0][0]
        return None

    def check_info(self,field):
        val = self.get_info(field)
        if val is not None and int(val) == 1:
            return True
        return False


    def load_transactions(self,chain_dict=None,tx_id_list=None,load_derived=False):
        db = self.db
        self.relevant_import_ids = set()
        fiat_needed = set()



        query = "SELECT " \
                "tx.chain, tx.id, tx.hash, tx.timestamp, tx.nonce, tx.block, interacted_addr.address, tx.function, tx.custom_type_id, tx.custom_color_id, tx.minimized, tx.manual, tx.custom_note, tx.import_id, tx.upload_id, " \
                "tr.id, tr.type, tr.from_addr_id, from_addr.address, tr.to_addr_id, to_addr.address, tr.val, tk.symbol, IFNULL(tk.contract,tk.symbol), tk.coingecko_id, tk.custom_coingecko_id, tr.token_nft_id, tr.base_fee, tr.input_len, tr.input," \
                "tr.custom_treatment, tr.custom_rate, tr.custom_vaultid, tr.synthetic "
        if load_derived:
            query += ",txd.category,txd.claim,txd.nft,txd.protocol,txd.protocol_note,txd.certainty,txd.cp_progenitor,txd.cp_address,txd.cp_name,txd.sig_hex,txd.sig_decoded,txd.balanced, " \
                     "trd.coingecko_id,trd.rate,trd.rate_found,trd.rate_source,trd.treatment,trd.vault_id "


        query +="FROM transactions as tx, transaction_transfers as tr, addresses as from_addr, addresses as to_addr, tokens as tk "
        query +="LEFT OUTER JOIN addresses as interacted_addr  ON interacted_addr.id = tx.interacted_addr_id "
        query += "LEFT OUTER JOIN addresses as originator_addr  ON originator_addr.id = tx.originator_addr_id "
        if load_derived:
            query += "LEFT OUTER JOIN transactions_derived as txd ON txd.id=tx.id " \
                     "LEFT OUTER JOIN transaction_transfers_derived as trd ON tr.id=trd.id "
        query +="WHERE tx.id = tr.transaction_id AND tr.from_addr_id = from_addr.id AND tr.to_addr_id = to_addr.id AND tr.token_id = tk.id "+\
                "AND (from_addr.id IN "+ sql_in(self.relevant_address_ids)+" OR to_addr.id IN "+sql_in(self.relevant_address_ids)+")"

        created_chain_dict = None
        address_db = None
        if chain_dict is not None:
            chain_name_list = list(chain_dict.keys())
            query += " AND tx.chain IN "+sql_in(chain_name_list)
        else:
            if not load_derived:
                address_db = SQLite('addresses',read_only=True)
            created_chain_dict = {}
        if tx_id_list is not None:
            query += " AND tx.id IN " + sql_in(tx_id_list)
        timing_adjustment = "CASE tx.chain WHEN 'Fantom' THEN -30 WHEN 'Avalanche' THEN -30 ELSE 0 END"
        query += " ORDER BY tx.timestamp + "+timing_adjustment+", tx.nonce, tx.id, tx.hash, tr.id"

        log("BIG QUERY", query)

        rows = db.select(query)
        transactions = []
        prev_txid = None

        for row in rows:
            if load_derived:
                chain_name, txid, hash, ts, nonce, block, interacted, function, custom_type_id, custom_color_id, minimized, manual, custom_note, import_id, upload_id, \
                trid, transfer_type, fr_id, fr, to_id, to, val, token, token_contract, coingecko_id, custom_coingecko_id, token_nft_id, base_fee, input_len, input, \
                custom_treatment, custom_rate, custom_vaultid, synthetic, \
                d_category,d_claim,d_nft,d_protocol,d_protocol_note,d_certainty,d_cp_progenitor,d_cp_address,d_cp_name,d_sig_hex,d_sig_decoded,d_balanced,\
                d_coingecko_id,d_rate,d_rate_found,d_rate_source,d_treatment,d_vault_id = row
            else:
                chain_name,txid, hash, ts, nonce, block, interacted, function, custom_type_id, custom_color_id, minimized, manual, custom_note, import_id, upload_id, \
                trid, transfer_type, fr_id, fr, to_id, to,val,token,token_contract, coingecko_id, custom_coingecko_id, token_nft_id, base_fee, input_len, input, \
                custom_treatment, custom_rate, custom_vaultid, synthetic = row

            if import_id is not None:
                self.relevant_import_ids.add(import_id)

            if txid != prev_txid:
                if created_chain_dict is not None:
                    if chain_name not in created_chain_dict:
                        chain = self.chain_factory(chain_name, is_upload = upload_id is not None)
                        if not load_derived and upload_id is None:
                            chain.init_addresses(address_db)
                        created_chain_dict[chain_name] = chain
                    else:
                        chain = created_chain_dict[chain_name]
                else:
                    chain = chain_dict[chain_name]['chain']

                # if fr in self.relevant_addresses:
                #     address = fr
                # else:
                #     address = to

                T = Transaction(self, chain, txid=txid, custom_type_id=custom_type_id, custom_color_id=custom_color_id, custom_note=custom_note, manual=manual, upload_id=upload_id)
                T.interacted = interacted
                T.function = function
                T.minimized = minimized
                if load_derived:
                    T.derived_data = {'category':d_category,'claim':d_claim,'nft':d_nft,'protocol':d_protocol,
                                 'protocol_note':d_protocol_note,'certainty':d_certainty,'cp_progenitor':d_cp_progenitor,'cp_address':d_cp_address,'cp_name':d_cp_name,
                                 'sig_hex':d_sig_hex,'sig_decoded':d_sig_decoded,'balanced':d_balanced}
                    log('\nsetting dd',T.hash, T.derived_data,filename='derived.txt')
                transactions.append(T)
                prev_txid = txid
            if val is not None and "," in str(val):
                val = float(val.replace(",",""))

            # T.chain.transferred_tokens.add(token_contract)
            row_derived = None
            if load_derived:
                row_derived = {'coingecko_id':d_coingecko_id,'rate':d_rate,'rate_found':d_rate_found,'rate_source':d_rate_source,'treatment':d_treatment,'vault_id':d_vault_id}
                log('setting ddt', row_derived, filename='derived.txt')
            if custom_coingecko_id is not None:
                coingecko_id = custom_coingecko_id
            # elif coingecko_id is not None:
            #     token_contract = 'coingecko_id:'+coingecko_id
            row = [hash, ts, nonce, block, fr, to, val, token, token_contract, coingecko_id, token_nft_id, base_fee, input_len, input]

            # log('loaded transfer',txid,transfer_type,row)
            T.append(transfer_type, row, transfer_id=trid, custom_treatment=custom_treatment, custom_rate=custom_rate, custom_vaultid=custom_vaultid, synthetic=synthetic, derived=row_derived)
            T.chain.transferred_tokens.add(token_contract)

            if token_contract in Twelve.FIAT_SYMBOLS and token_contract != 'USD':
                fiat_needed.add(token_contract)
        self.relevant_import_ids = list(self.relevant_import_ids)
        if address_db is not None:
            address_db.disconnect()
        if len(fiat_needed) or self.fiat != 'USD':
            self.load_fiat()
        return transactions, created_chain_dict


    def get_custom_addresses(self,chain):
        if chain not in self.custom_addresses:
            custom_addresses = {}
            rows = self.db.select("SELECT * FROM custom_names WHERE chain='"+chain+"'")
            for row in rows:
                custom_addresses[row[1]] = {'entity': row[2]}
            log('custom_addresses', custom_addresses)
            self.custom_addresses[chain] = custom_addresses
        else:
            return self.custom_addresses[chain]
        return custom_addresses

    def get_custom_rates(self):
        Q = "select t.timestamp, tokens.chain, tokens.contract, tokens.symbol, tt.type, tt.token_nft_id, tt.custom_rate " \
            "from tokens,transaction_transfers as tt,transactions as t " \
            "WHERE tt.custom_rate is not NULL and tt.token_id = tokens.id and tt.transaction_id = t.id"
        rows = self.db.select(Q)
        for row in rows:
            timestamp, chain_name, contract, symbol, transfer_type, nft_id, rate = row
            if contract is None:
                contract = symbol
            if transfer_type == 5:
                contract = contract +'_'+str(nft_id)
            cp_pair = chain_name +":"+ contract
            if cp_pair not in self.custom_rates:
                self.custom_rates[cp_pair] = SortedDict()
            self.custom_rates[cp_pair][timestamp] = rate



    def save_custom_type(self,name,description,balanced,rules, id=None):
        log('save_custom_type',name,description,balanced,rules)
        chain_name = 'ALL'
        if id is not None:
            # self.db.query("UPDATE custom_types SET name = '"+name+"', chain = '"+chain_name+"', description = '"+description+"', balanced = "+str(balanced)+" WHERE id="+str(id))
            self.db.update_kw('custom_types',"id="+str(id),chain=chain_name, name=name, description=description, balanced=balanced)
            self.db.query("DELETE FROM custom_types_rules WHERE type_id=" + id)
        else:
            self.db.insert_kw('custom_types', chain=chain_name, name=name, description=description, balanced=balanced)
        self.db.commit()


        rows = self.db.select("SELECT id FROM custom_types WHERE name='"+name+"' and (chain='"+chain_name+"' or chain='ALL')")
        type_id = rows[0][0]
        for rule in rules:
            from_addr, from_addr_custom, to_addr, to_addr_custom, token, token_custom, treatment, vault_id, vault_id_custom = rule
            self.db.insert_kw('custom_types_rules',type_id=type_id,from_addr=from_addr,from_addr_custom=from_addr_custom,
                              to_addr=to_addr,to_addr_custom=to_addr_custom,
                              token=token,token_custom=token_custom, treatment=treatment, vault_id=vault_id, vault_id_custom=vault_id_custom)
        self.db.commit()

    def delete_custom_type(self,id):
        self.db.query("DELETE FROM custom_types WHERE id=" + id)
        self.db.query("DELETE FROM custom_types_rules WHERE type_id=" + id)
        self.db.commit()

    def load_custom_types(self,chain_name=None):
        if chain_name is None:
            rows = self.db.select(
                "SELECT t.id, t.name, t.chain, t.description, t.balanced, r.* FROM custom_types as t, custom_types_rules as r WHERE t.name != '' and t.id = r.type_id ORDER BY t.name COLLATE NOCASE ASC, r.id ASC")
        else:
            rows = self.db.select("SELECT t.id, t.name, t.chain, t.description, t.balanced, r.* FROM custom_types as t, custom_types_rules as r WHERE (t.chain='"+chain_name+"' or t.chain='ALL') and t.name != '' and t.id = r.type_id ORDER BY t.name COLLATE NOCASE ASC, r.id ASC")
        if len(rows) == 0:
            return []

        js = []
        cur_type = {'id':rows[0][0], 'name':rows[0][1], 'chain_specific':rows[0][2]!='ALL', 'rules':[], 'description':rows[0][3],'balanced':rows[0][4]}
        for row in rows:
            type_id, type_name, chain, type_desc, type_balanced, rule_id, _, from_addr, from_addr_custom, to_addr, to_addr_custom, token, token_custom, treatment, vault_id, vault_id_custom = row
            if cur_type['id'] != type_id:
                js.append(cur_type)
                cur_type = {'id':type_id,'name':type_name,'chain_specific':chain != 'ALL','description':type_desc,'balanced':type_balanced,'rules':[]}
            cur_type['rules'].append([rule_id,from_addr,from_addr_custom,to_addr,to_addr_custom,token,token_custom,treatment,vault_id,vault_id_custom])
        js.append(cur_type)
        log(js)
        return js


    def prepare_all_custom_types(self,chain_name=None):
        # tx_ct_mapping = {}
        # rows = self.db.select('select * from custom_types_applied')
        # for row in rows:
        #     tx_ct_mapping[row[1]] = row[0]

        ct_info = {}
        if chain_name is None:
            rows = self.db.select("SELECT id, name, description, balanced FROM custom_types")
        else:
            rows = self.db.select("SELECT id, name, description, balanced FROM custom_types where chain='"+chain_name+"' or chain='ALL'")
        for row in rows:
            rules = self.db.select("SELECT * FROM custom_types_rules WHERE type_id = "+str(row[0])+" ORDER BY id ASC")
            ct_info[row[0]] = {'name': row[1], 'description':row[2],'balanced':row[3],'rules':rules}
        # self.tx_ctype_mapping = tx_ct_mapping
        self.ctype_info = ct_info



    def apply_custom_type_one_transaction(self,transaction,type_name,balanced,rules):
        transaction.type = Category(custom_type=type_name)
        transaction.classification_certainty_level = 10
        transaction.balanced = balanced
        self.custom_treatment_by_rules(transaction, transaction.custom_type_id, type_name, rules)

    def apply_custom_type(self, type_id,transaction_list):
        for txid in transaction_list:
            log('apply type',type_id, txid)
            # self.db.insert_kw('custom_types_applied', type_id=type_id, transaction_id=txid)
            self.db.update_kw('transactions', 'id='+str(txid), custom_type_id=type_id)
        self.db.commit()

        rules = self.db.select("SELECT * FROM custom_types_rules WHERE type_id = "+type_id+" ORDER BY id ASC")
        type_name, balanced = self.db.select("SELECT name, balanced FROM custom_types WHERE id = "+type_id)[0]


        # chain = Chain.from_name(chain_name, address_db, address)

        # S = Signatures()
        transactions,chain_dict = self.load_transactions(tx_id_list=transaction_list,load_derived=True)
        # contract_list, counterparty_by_chain, input_list = self.get_contracts(transactions)
        # S.init_from_db(input_list)
        # C.init_from_db(chain.main_asset, contract_list, chain.addr, initial=False)
        C = Coingecko.init_from_cache(self)
        # classifier = Classifier()
        res = []
        for idx,transaction in enumerate(transactions):
            transaction.finalize(C, self.fiat_rates, None)
            self.apply_final_rate(C, transaction)
            self.apply_custom_cp_name(transaction)
            # classifier.classify(transaction)
            self.apply_custom_type_one_transaction(transaction, type_name,balanced, rules)
            # transaction.add_fee_transfer()
            transaction.infer_and_adjust_rates(self,C)
            self.apply_custom_val(transaction)
            self.apply_fiat(transaction)
            js = transaction.to_json()
            res.append(js)

        return res

    def unapply_custom_type(self, type_id, transaction_list=None):
        if transaction_list is None:
            transaction_list = []
            rows = self.db.select("SELECT id FROM transactions WHERE custom_type_id="+type_id)
            for row in rows:
                transaction_list.append(str(row[0]))

        self.db.update_kw('transactions', 'id IN '+sql_in(transaction_list)+' AND custom_type_id='+type_id, custom_type_id=None)

        self.db.commit()



        # chain = Chain.from_name(chain_name, address_db, address)

        # S = Signatures()
        transactions,chain_dict = self.load_transactions(tx_id_list=transaction_list, load_derived=True)
        # contract_list, counterparty_list, input_list = self.get_contracts(transactions)
        # S.init_from_db(input_list)
        # C.init_from_db(chain.main_asset,contract_list,chain.addr,initial=False)
        C = Coingecko.init_from_cache(self)
        # classifier = Classifier()
        res = []
        classifier = Classifier()
        for idx, transaction in enumerate(transactions):
            transaction.finalize(C, self.fiat_rates, None)
            self.apply_final_rate(C, transaction)
            self.apply_custom_cp_name(transaction)
            if transaction.custom_type_id is not None:
                type_id = str(transaction.custom_type_id)
                rules = self.db.select("SELECT * FROM custom_types_rules WHERE type_id = " + type_id + " ORDER BY id ASC")
                type_name, balanced = self.db.select("SELECT name, balanced FROM custom_types WHERE id = " + type_id)[0]
                self.apply_custom_type_one_transaction(transaction, type_name, balanced, rules)
            else:
                classifier.classify(transaction)
            # transaction.add_fee_transfer()
            transaction.infer_and_adjust_rates(self, C)
            self.apply_custom_val(transaction)
            self.apply_fiat(transaction)
            js = transaction.to_json()
            res.append(js)

        return res


    def custom_treatment_by_rules(self, transaction, type_id, type_name, rules):
        if transaction.hash == transaction.chain.hif:
            print("Applying custom type rules to",transaction.hash)

        def check_address_match(transfer_addr,rule_addr,rule_addr_custom):
            if rule_addr_custom is not None:
                rule_addr_custom = normalize_address(rule_addr_custom)
            if rule_addr == 'my_address':
                if transfer_addr not in self.all_addresses:
                    return 0

                if rule_addr_custom is not None and len(rule_addr_custom) > 0:
                    if transfer_addr != rule_addr_custom:
                        return 0

            if rule_addr == '0x0000000000000000000000000000000000000000' and transfer_addr != '0x0000000000000000000000000000000000000000':
                return 0
            if rule_addr == 'specific':
                if transfer_addr != rule_addr_custom:
                    return 0
                else:
                    return 1
            if rule_addr == 'specific_excl':
                if transfer_addr == rule_addr_custom:
                    return 0
                else:
                    return 1
            if transfer_addr == 'network':
                return 0
            return 1

        def check_token_match(transfer_contract, transfer_symbol, transfer_coingecko_id, rule_token, rule_token_custom):
            # log('ctm',rule_token, 'transfer_symbol', transfer_symbol,'transfer_contract', transfer_contract,'rule_token_custom', rule_token_custom,filename='custom_types.txt')
            if rule_token_custom is not None:
                if is_ethereum(rule_token_custom) or is_solana(rule_token_custom):
                    rule_token_custom = normalize_address(rule_token_custom)
                else:
                    rule_token_custom = rule_token_custom.lower()
            transfer_symbol = transfer_symbol.lower()
            if transfer_coingecko_id is not None:
                transfer_coingecko_id = transfer_coingecko_id.lower()
            if rule_token == 'specific' and rule_token_custom in [transfer_contract,transfer_symbol,transfer_coingecko_id]:
                return 1
            if rule_token == 'specific_excl' and rule_token_custom not in [transfer_contract,transfer_symbol,transfer_coingecko_id]:
                return 1
            if rule_token == 'any':
                return 1
            if rule_token.lower() == transfer_symbol:
                return 1
            return 0

        deductible_fee = False
        for transfer_id,transfer in transaction.transfers.items():
            fr = normalize_address(transfer.fr)
            to = normalize_address(transfer.to)
            what = normalize_address(transfer.what)
            symbol = transfer.symbol.lower()
            coingecko_id = transfer.coingecko_id

            rule_found = False
            selected_treatment = None
            # log("Checking transfer",transfer)
            for rule in rules:
                # log("Against rule",rule)
                id, type_id, from_addr, from_addr_custom, to_addr, to_addr_custom, token, token_custom, treatment, vault_id, vault_id_custom = rule
                # if (fr == self.address and from_addr != 'my_address') or (to == self.address and to_addr != 'my_address'):
                #     continue
                if not check_address_match(fr,from_addr,from_addr_custom):
                    # log("Fr address fail")
                    continue

                if not check_address_match(to,to_addr,to_addr_custom):
                    # log("To address fail")
                    continue

                if not check_token_match(what, symbol, coingecko_id, token, token_custom):
                    # log("Token fail")
                    continue

                rule_found = True
                # log("Match")
                selected_treatment = treatment
                if selected_treatment in ['deposit','withdraw','exit','borrow','repay','liquidation','full_repay']:
                    if vault_id == 'address':
                        if transfer.outbound:
                            vault_id = to
                        else:
                            vault_id = fr
                    elif vault_id == 'type_name':
                        vault_id = type_name
                    else:
                        vault_id = vault_id_custom
                    transfer.vault_id = vault_id

                if selected_treatment in ['buy','sell','income']:
                    deductible_fee = True

                transfer.treatment = selected_treatment
                break

            if not rule_found and transfer.synthetic != transfer.FEE:
                log('transaction',transaction.txid,'could not find matching rule for transfer_idx',transfer_id)


        if transaction.fee_transfer is not None and transaction.fee_transfer.treatment is None:
            if deductible_fee:
                transaction.fee_transfer.treatment = 'fee'
            else:
                transaction.fee_transfer.treatment = 'loss'






    # def add_rate(self, transaction_id, transfer_idx, rate, source, level):
    #     self.db.insert_kw('rates', transaction_id=transaction_id, transfer_idx=transfer_idx, rate=rate, source=self.rate_sources.index(source), level=level)
    #
    # def wipe_rates(self):
    #     self.db.query('DELETE FROM rates')
    #     self.db.commit()

    # def save_custom_val(self,chain_name,address,transaction_id, transfer_idx, treatment=None, rate=None, vaultid = None):
    #     where = 'transaction_id='+str(transaction_id)+' and idx='+str(transfer_idx)
    #     if treatment is not None:
    #         self.db.update_kw('transaction_transfers', where, custom_treatment=treatment)
    #     if rate is not None:
    #         self.db.update_kw('transaction_transfers', where, custom_rate=rate)
    #     if vaultid is not None:
    #         self.db.update_kw('transaction_transfers', where, custom_vaultid=vaultid)
    #     self.db.commit()



    def save_custom_val(self, transaction_id, transfer_id_str, prop, new_value):
        where = 'transaction_id=' + str(transaction_id) + ' and id IN (' + str(transfer_id_str)+')'
        if prop == 'treatment':
            self.db.update_kw('transaction_transfers', where, custom_treatment=new_value)
        if prop == 'rate':
            self.db.update_kw('transaction_transfers', where, custom_rate=new_value)
        if prop == 'vault_id':
            self.db.update_kw('transaction_transfers', where, custom_vaultid=new_value)
        self.db.commit()

    def undo_custom_changes(self,transaction_id):
        # self.db.query("DELETE FROM custom_treatment WHERE transaction_id="+str(transaction_id))
        # self.db.query("DELETE FROM custom_rates WHERE transaction_id=" + str(transaction_id))
        self.db.update_kw('transaction_transfers',"transaction_id=" + str(transaction_id),custom_treatment=None,custom_rate=None,custom_vaultid=None)
        self.db.commit()

        # chain = Chain.from_name(chain_name, address_db, address)

        # S = Signatures()
        transactions,_ = self.load_transactions(tx_id_list=[transaction_id],load_derived=True)
        transaction = transactions[0]

        # contract_list, counterparty_list, input_list = transaction.get_contracts()
        # S.init_from_db(input_list)
        # C.init_from_db(chain.main_asset, contract_list, chain.addr, initial=False)
        C = Coingecko.init_from_cache(self)
        classifier = Classifier()

        transaction.finalize(C, self.fiat_rates, None)
        log('undoing custom changes 1', transaction)
        # self.apply_final_rate(C, transaction)
        # log('undoing custom changes 2', transaction)
        self.apply_custom_cp_name(transaction)
        # classifier.classify(transaction)


        if transaction.custom_type_id is not None:
            type_id = str(transaction.custom_type_id)
            rules = self.db.select("SELECT * FROM custom_types_rules WHERE type_id = " + type_id + " ORDER BY id ASC")
            type_name, balanced = self.db.select("SELECT name, balanced FROM custom_types WHERE id = " + type_id)[0]
            self.apply_custom_type_one_transaction(transaction, type_name,balanced, rules)
        else:
            classifier.classify(transaction)
        # transaction.add_fee_transfer()
        transaction.infer_and_adjust_rates(self, C)
        self.apply_fiat(transaction)
        js = transaction.to_json()

        return js


    def apply_custom_val(self, transaction):
        # txid = transaction.txid
        applied = False
        for transfer in transaction.transfers.values():
            if transfer.custom_rate is not None:
                transfer.rate = 'custom:'+str(transfer.custom_rate)
                applied = True
            if transfer.custom_treatment is not None:
                transfer.treatment = 'custom:'+str(transfer.custom_treatment)
                applied = True
            if transfer.custom_vaultid is not None:
                transfer.vault_id = 'custom:'+str(transfer.custom_vaultid)
                applied = True
        return applied

    def apply_final_rate(self,coingecko_rates,transaction):

        for transfer in transaction.transfers.values():
            lookup_rate_contract = transfer.what
            coingecko_id = transfer.coingecko_id
            if transfer.type == 5:
                lookup_rate_contract = lookup_rate_contract + "_" + str(transfer.token_nft_id)
            transfer.rate_found, transfer.rate, transfer.rate_source = self.lookup_rate_including_custom(coingecko_rates, transaction.chain.name, lookup_rate_contract, transaction.ts, coingecko_id=coingecko_id, verbose=transaction.hash == transaction.chain.hif)
            if transaction.hash == transaction.chain.hif:
                log('LOOKUP RATE RESULT',transfer,transfer.rate_found, transfer.rate, transfer.rate_source)

    def apply_custom_cp_name(self, transaction):
        cp = transaction.counter_parties
        if len(cp) != 1:
            return
        address = list(cp.keys())[0]
        chain = transaction.chain
        custom_addresses = self.get_custom_addresses(chain.name)

        if address in custom_addresses:
            cp[address][0] = custom_addresses[address]['entity']


    def recolor(self,color_id,transaction_list):
        if color_id == 'undo':
            color_id = None
        for txid in transaction_list:
            self.db.update_kw('transactions', 'id='+str(txid), custom_color_id=color_id)
        self.db.commit()

    def save_note(self,note,txid):
        if len(note) == 0:
           note = None
        self.db.update_kw('transactions','id='+str(txid),custom_note=note)
        self.db.commit()

    def save_manual_transactions(self,chain_name,address,all_tx_blobs,coingecko):
        txid_list = []
        chain = self.chain_factory(chain_name)
        for row in all_tx_blobs:
            ts, hash, op, cp, transfers, txid = row
            log('save_manual_transaction',chain_name,address,ts,hash,op,cp,transfers,txid)

            # ts = None
            # try:
            #     ts = datetime.strptime(dt+" "+tm, "%m/%d/%Y %H:%M:%S")
            # except:
            #     try:
            #         ts = datetime.strptime(dt, "%m/%d/%Y")
            #     except:
            #         exit(1)
            # ts = int(ts.timestamp())
            ts = int(ts)

            assert len(transfers) >= 1

            # address_db = SQLite('addresses')




            log('ts',ts)
            if hash == '':
                hash = None

            transfers_to_delete = []
            if txid is None:
                self.db.insert_kw('transactions', chain=chain_name, hash=hash, timestamp=ts, function=op, manual=1)
                txid = self.db.select("SELECT last_insert_rowid()")[0][0]
            else:
                self.db.update_kw('transactions','id='+str(txid), chain=chain_name, hash=hash,timestamp=ts, function=op)
                rows = self.db.select('SELECT id FROM transaction_transfers WHERE transaction_id='+str(txid))
                for row in rows:
                    transfers_to_delete.append(int(row[0]))
            txid_list.append(str(txid))
                # self.db.query("DELETE FROM transaction_transfers WHERE transaction_id="+str(txid))


            for tridx, transfer in enumerate(transfers):
                trid, fr, to, what, amount, nft_id = transfer
                trid = int(trid)
                if trid in transfers_to_delete:
                    transfers_to_delete.remove(trid)
                input = None
                input_len = -1
                if tridx == 0:
                    if op is not None and len(op) > 0:
                        input = 'custom:'+op
                        input_len = 10

                log('trtop',fr,to,what,amount,nft_id)
                amount = amount.replace(",","")
                fr = normalize_address(fr)
                to = normalize_address(to)
                if nft_id == '':
                    nft_id = None
                if tridx == 0:
                    pass
                transfer_type = 3
                if nft_id is not None:
                    transfer_type = 4
                elif what.upper() == chain.main_asset.upper():
                    transfer_type = 1
                    what = what.upper()
                    coingecko_id = coingecko.lookup_id(chain_name,what)
                    tok_id = self.locate_insert_token(chain_name, what, what, coingecko_id=coingecko_id)

                if transfer_type != 1:
                    contract = re.search(r'0x[0-9a-fA-F]{40}', what)
                    if contract is not None:
                        contract = contract.group()
                    else:
                        contract = what
                    coingecko_id = coingecko.lookup_id(chain_name, contract)
                    tok_id = self.locate_insert_token(chain_name, contract, contract[:8], coingecko_id=coingecko_id)


                fr_id = self.locate_insert_address(chain_name,fr)
                to_id = self.locate_insert_address(chain_name,to)

                if fr in self.all_addresses:
                    if chain_name not in self.all_addresses[fr]:
                        self.set_address_present(fr,chain_name, commit=False)
                        self.set_address_used(fr, chain_name, commit=False)
                if to in self.all_addresses:
                    if chain_name not in self.all_addresses[to]:
                        self.set_address_present(to,chain_name, commit=False)
                        self.set_address_used(to, chain_name, commit=False)


                log('saving manual transfer',trid,fr_id,to_id,tok_id)

                if trid == -1:
                    self.db.insert_kw('transaction_transfers',
                                      type=transfer_type, transaction_id=txid, from_addr_id=fr_id, to_addr_id=to_id, val=amount, token_id=tok_id, token_nft_id = nft_id,
                                      base_fee=0, input=input, input_len=input_len)
                else:
                    self.db.update_kw('transaction_transfers','id='+str(trid),
                                      type=transfer_type, from_addr_id=fr_id, to_addr_id=to_id, val=amount, token_id=tok_id, token_nft_id = nft_id,
                                      base_fee=0, input=input, input_len=input_len)

            for trid in transfers_to_delete:
                self.db.query('DELETE FROM transaction_transfers WHERE id='+str(trid))


        self.db.commit()

        S = Signatures()
        transactions,_ = self.load_transactions({chain_name:{'chain': chain}}, tx_id_list=txid_list)
        _, _, input_list = self.get_contracts(transactions)
        S.init_from_db(input_list)
        C = Coingecko.init_from_cache(self)
        self.wipe_derived_data(transactions)
        transactions_js = self.transactions_to_log(C, S, transactions, store_derived=True)

        # address_db.disconnect()
        return transactions_js

    def delete_manual_transaction(self,txid):
        self.db.query("DELETE FROM transactions WHERE id=" + txid)
        self.db.query("DELETE FROM transactions_derived WHERE id=" + txid)
        self.db.query("DELETE FROM transaction_transfers_derived WHERE id IN (SELECT id FROM transaction_transfers WHERE transaction_id=" + txid + ")")
        self.db.query("DELETE FROM transaction_transfers WHERE transaction_id=" + txid)
        self.db.commit()



    def json_to_csv(self):
        custom_types_js = self.load_custom_types()
        custom_types = {}
        for entry in custom_types_js:
            custom_types[entry['id']] = entry

        path = 'data/users/'+self.address+'/transactions.json'
        f = open(path,'r')
        js = json.load(f)
        f.close()
        color_map = {0:'red',3:'orange',5:'yellow',10:'green'}
        type_map = {1:'base token transfer',2:'internal transfer',3:'ERC20 transfer',4:'ERC721 (NFT) transfer',5:'ERC1155 (multi-token) transfer',
                    20:'received transfer from an upload',21:'sent transfer from an upload',22:'fee transfer from an upload'}
        csv_rows = []
        for T in js:
            if 'custom_color_id' in T:
                color = color_map[int(T['custom_color_id'])]
            else:
                color = color_map[int(T['classification_certainty'])]

            if 'ct_id' in T and T['ct_id']:
                type = custom_types[T['ct_id']]['name']
            else:
                type = T['type']

            if 'custom_note' in T and T['custom_note'] is not None:
                note = T['custom_note']
            else:
                note = ''

            common = [T['ts'],datetime.utcfromtimestamp(T['ts']),T['chain'],T['hash'],color,type, note]

            if len(T['counter_parties']) ==0:
                cp = ['','','','']
            else:
                counter_parties = T['counter_parties']
                cp_adr = list(counter_parties.keys())[0]
                cp_val = counter_parties[cp_adr]
                cp = [cp_val[4], cp_val[0], cp_val[1], cp_val[2]]


            for t in T['rows'].values():
                rate,_ = decustom(t['rate'])
                treatment,_ = decustom(t['treatment'])
                vault_id = ''
                if treatment in ['deposit','withdraw','exit','borrow','repay','full_repay']:
                    vault_id = t['vault_id']
                if treatment is None:
                    treatment = 'ignore'
                nft_id = t['token_nft_id']
                if nft_id is not None:
                    nft_id = str(nft_id)
                transfer = [t['fr'],t['to'],t['amount'],t['what'],str(t['symbol'].encode('utf-8'))[2:-1],nft_id,type_map[t['type']],treatment,vault_id,rate]

                # str(ca_line['symbol'].encode("utf-8"))[2:-1]

                csv_row = common + cp + transfer
                csv_rows.append(csv_row)

        fields = ['timestamp','UTC datetime','chain','transaction hash','color','classification','custom note',
                  'counterparty address','counterparty name',
                  'function hex signature','operation (decoded hex signature)',
                  'source address','destination address','amount transfered','token contract address','token symbol','token unique ID','transfer type','tax treatment','vault id','USD rate']

        path = 'data/users/' + self.address + '/transactions.csv'
        f = open(path, 'w', encoding='utf-8')
        csvwriter = csv.writer(f)
        csvwriter.writerow(fields)
        csvwriter.writerows(csv_rows)
        f.close()

    # def delete_derived_data(self):
    #     db = self.db
    #     db.query("DELETE FROM transactions_derived")
    #     db.query("DELETE FROM transaction_transfers_derived")
    #     db.commit()

    def transactions_to_log(self,coingecko_rates, signatures, transactions, progress_bar=None, store_derived=False):
        log("transactions_to_log called",len(transactions),'store_derived',store_derived)
        t = time.time()
        t_stats = {'finalize':0,'classify':0,'iad':0}
        all_rows = []
        if len(transactions) == 0:
            return all_rows

        pb_alloc = 7.
        pb_update_per_transaction = pb_alloc/len(transactions)
        if store_derived:
            pb_update_per_transaction /= 2.

        # print('transactions', len(transactions))
        self.prepare_all_custom_types()
        # user.prepare_all_custom_treatment_and_rates()
        classifier = Classifier()
        t_fin_all = None
        for idx, transaction in enumerate(transactions):
            t1 = time.time()
            t_fin = transaction.finalize(coingecko_rates, self.fiat_rates, signatures, store_derived)
            if t_fin_all is None:
                t_fin_all = t_fin
            else:
                for i,t in enumerate(t_fin):
                    t_fin_all[i] += t
            t2 = time.time()
            classifier.classify(transaction)
            t3 = time.time()
            transaction.infer_and_adjust_rates(self, coingecko_rates)
            t4 = time.time()
            t_stats['finalize']+=(t2-t1)
            t_stats['classify'] += (t3 - t2)
            t_stats['iad'] += (t4 - t3)
            if progress_bar is not None:
                progress_bar.update( 'Classifying transactions: '+str(idx)+"/"+str(len(transactions)), pb_update_per_transaction)

        if store_derived:
            if progress_bar is not None:
                progress_bar.update('Saving classification data')

            log("Storing derived",len(transactions),filename='derived.txt')
            for idx, transaction in enumerate(transactions):
                classifier.compare(transaction)
                self.store_derived_data(transaction)
                if progress_bar is not None:
                    progress_bar.update('Saving classification data: ' + str(idx) + "/" + str(len(transactions)), pb_update_per_transaction)
            self.set_info('force_forget_derived',0)
            self.db.commit()

        if progress_bar is not None:
            progress_bar.set('Applying custom changes',87)

        for idx, transaction in enumerate(transactions):
            self.apply_final_rate(coingecko_rates,transaction)
            self.apply_custom_cp_name(transaction)
            if transaction.custom_type_id is not None:
                info = self.ctype_info[transaction.custom_type_id]
                self.apply_custom_type_one_transaction(transaction, info['name'], info['balanced'], info['rules'])
            transaction.infer_and_adjust_rates(self, coingecko_rates)
            self.apply_custom_val(transaction)


        # for idx,transaction in enumerate(transactions):
        #     transaction.finalize(self,coingecko_rates,signatures)
        #
        #     do_store_derived = store_derived
        #
        #     if do_store_derived:
        #         classifier.classify(transaction)
        #         transaction.infer_and_adjust_rates(self, coingecko_rates)
        #         self.store_derived_data(transaction)
        #
        #     if transaction.custom_type_id is not None:
        #         info = self.ctype_info[transaction.custom_type_id]
        #         self.apply_custom_type_one_transaction(transaction, info['name'], info['balanced'], info['rules'])
        #         transaction.infer_and_adjust_rates(self, coingecko_rates)
        #     elif not do_store_derived:
        #         classifier.classify(transaction)
        #         transaction.infer_and_adjust_rates(self,coingecko_rates)
        #
        #     self.apply_custom_val(transaction)
        #     if progress_bar is not None:
        #         progress_bar.update( 'Classifying transactions', pb_update_per_transaction)
        # log('timing:transactions_to_log 1', time.time() - t)
        # self.db.commit()

        t = time.time()
        if progress_bar:
            progress_bar.set('Preparing transactions for display', 88)
        for idx,transaction in enumerate(transactions):
            self.apply_fiat(transaction)
            js = transaction.to_json()
            if len(js['rows']) == 0:
                continue #no non-zero transfers, skip
            all_rows.append(js)

        log('transactions_to_log timing stats',t_stats,'finalize stats',t_fin_all, filename='timing.txt')

        # pprint.pprint(dict(classifier.outgoing_transfers))

        return all_rows



    def lookup_rate_including_custom(self,coingecko, chain_name, token_contract, ts, coingecko_id=None, verbose=False):

        #first look in custom rates
        log('lookup_rate_including_custom',coingecko.initialized,chain_name,token_contract,ts,coingecko_id,filename='lookups.txt')
        cust_level = -1
        level = -1
        cp_pair = chain_name +":"+token_contract
        if cp_pair in self.custom_rates:
            rates_table = self.custom_rates[cp_pair]
            cust_source = "custom_rates"
            if ts in rates_table:
                return 1, rates_table[ts], cust_source

            first = rates_table.keys()[0]
            last = rates_table.keys()[-1]
            if ts < first:
                cust_level = 0.3
                cust_rate = rates_table[first]
                cust_source += ', before first ' + str(first)
            elif ts > last:
                cust_level = 0.5
                cust_rate = rates_table[last]
                cust_source += ', after last ' + str(last)
            else:
                idx = rates_table.bisect_left(ts)
                ts_lookup = rates_table.keys()[idx - 1]
                cust_rate = rates_table[ts_lookup]
                cust_level = 0.5

        if token_contract == self.fiat:
            level, rate, source = 1, 1./self.fiat_rates.lookup_rate(token_contract,ts), 'fiat'
            return level, rate, source
        elif token_contract in Twelve.FIAT_SYMBOLS:
            level, rate, source = 1, 1./self.fiat_rates.lookup_rate(token_contract,ts), 'fiat-other'
            return level, rate, source

        if coingecko.initialized:
            if coingecko_id is not None:
                level, rate, source = coingecko.lookup_rate_by_id(coingecko_id, ts)
            else:
                level, rate, source = coingecko.lookup_rate(chain_name,token_contract, ts)

        if verbose:
            if level > -1:
                log("COINGECKO RATE",level, rate, source)
            if cust_level > -1:
                log("CUSTOM RATE", cust_level, cust_rate, cust_source)


        #what follows is an ungodly mess of selecting which rate is better.
        if cust_level == -1 and level == -1:
            return 0, None, None

        if cust_level > -1 and level <= 0:
            return cust_level, cust_rate, cust_source

        if cust_level == -1 and level > -1:
            return level, rate, source

        if level == 1:
            return level, rate, source

        if 'before' in cust_source and 'before' not in source:
            return level, rate, source

        if 'before' in source and 'before' not in cust_source:
            return cust_level, cust_rate, cust_source

        comp_ts = False
        if 'before' in source and 'before' in cust_source:
            diff_coingecko = abs(int(source[source.index('before first ')+13:])-ts)
            diff_custom = abs(int(cust_source[cust_source.index('before first ') + 13:])-ts)
            comp_ts = True
        if 'after' in source and 'after' in cust_source:
            diff_coingecko = abs(int(source[source.index('after last ') + 11:]) - ts)
            diff_custom = abs(int(cust_source[cust_source.index('after last ') + 11:]) - ts)
            if verbose:
                log('RATE CALC DIFFS',ts,diff_coingecko,diff_custom)
            comp_ts = True

        if comp_ts:
            if diff_coingecko < diff_custom:
                return level, rate, source
            else:
                return cust_level, cust_rate, cust_source


        return 0, None, None


    def chain_factory(self,chain_name, is_upload=False):
        if is_upload:
            chain = Chain.from_upload(chain_name)
        elif chain_name == 'Solana':
            chain = Solana()
        else:
            chain = Chain.from_name(chain_name)
        return chain


    def get_thirdparty_data(self,all_chains, progress_bar=None):

        session = requests.session()
        session.headers.update({'AccessKey': os.environ.get('debank_access_key')})
        # debank_chain_mapping = {
        #     'ETH': 'eth', 'Polygon': 'matic', 'Arbitrum': 'arb', 'Avalanche': 'avax', 'Fantom': 'ftm', 'BSC': 'bsc', 'HECO': 'heco', 'Moonriver': 'movr', 'Cronos': 'cro', 'Gnosis': 'xdai',
        #     'Optimism': 'op', 'Celo':'celo', 'Doge':'doge', 'Songbird':'sgb', 'Metis':'metis', 'Boba':'boba', 'Astar':'astar', 'Evmos':'evmos','Kava':'kava','Canto':'canto',
        #     'Aurora':'aurora','Step':'step','KCC':'kcc'
        # }
        debank_chain_mapping = {}
        for chain_name,conf in Chain.CONFIG.items():
            if 'debank_mapping' in conf:
                if conf['debank_mapping'] is None:
                    continue
                debank_mapping = conf['debank_mapping']
            elif 'base_asset' in conf:
                debank_mapping = conf['base_asset'].lower()
            else:
                debank_mapping = chain_name.lower()
            debank_chain_mapping[chain_name] = debank_mapping
        inverse_debank_mapping = {}
        for k, v in debank_chain_mapping.items():
            inverse_debank_mapping[v] = k
        all_addresses = set()
        for chain_name, chain_data in all_chains.items():
            if chain_data['is_upload']:
                continue
            all_addresses = all_addresses.union(set(chain_data['import_addresses']))
        addresses = list(all_addresses)
        log('all addresses for debank',addresses, filename='current_tokens_log.txt')
        # addresses = all_chains['ETH']['import_addresses']
        for idx, active_address in enumerate(addresses):
            address = normalize_address(active_address)
            if not is_ethereum(address):
                continue
            if progress_bar:
                progress_bar.update('DeBank: Retrieving your current token balances on '+active_address, 1./len(addresses))
            url = 'https://pro-openapi.debank.com/v1/user/all_token_list?id='+active_address
            try:
                resp = session.get(url, timeout=10)
                data = resp.json()
                log('debank request',url,filename='debank.txt')
                log('debank response', data, filename='debank.txt')
                for entry in data:
                    debank_chain_name = entry['chain']
                    if debank_chain_name in inverse_debank_mapping:
                        chain_name = inverse_debank_mapping[debank_chain_name]
                        if chain_name in all_chains:
                            chain_data = all_chains[chain_name]
                            if chain_data['is_upload']:
                                continue
                            if active_address in chain_data['import_addresses']:
                                amount = entry['amount']
                                #occasional misclassified NFT? ex 0xdf5d68d54433661b1e5e90a547237ffb0adf6ec2
                                if amount == 1e-18:
                                    if entry['raw_amount'] == 1 and entry['price'] == 0 and not entry['is_core'] and not entry['is_verified'] and entry['decimals'] == 18:
                                        continue



                                token = entry['id']
                                symbol = entry['symbol']
                                rate = 0
                                if entry['is_core'] or entry['is_verified']:
                                    rate = float(entry['price'])
                                if len(token) != 42:
                                    token = symbol.upper()
                                else:
                                    token = normalize_address(token)
                                if address not in chain_data['current_tokens'] or chain_data['current_tokens'][address] is None:
                                        chain_data['current_tokens'][address] = {}
                                chain_data['current_tokens'][address][token] = {'symbol': symbol, 'amount': amount}
                                if rate != 0:
                                    chain_data['current_tokens'][address][token]['rate'] = rate
                log("Debank: current tokens on",address, filename='current_tokens_log.txt')
                for chain_name, chain_data in all_chains.items():
                    log(chain_name,chain_data['current_tokens'], filename='debank.txt')
            except:
                self.current_import.add_error(Import.DEBANK_TOKEN_FAILURE,address=address,debug_info=traceback.format_exc())
                log_error("Failed to get_current_tokens", active_address)

        for idx, active_address in enumerate(addresses):
            address = normalize_address(active_address)
            if not is_ethereum(address):
                continue
            if progress_bar:
                progress_bar.update('DeBank: Retrieving your counterparties for '+active_address, 1./len(addresses))
            url = 'https://pro-openapi.debank.com/v1/user/all_complex_protocol_list?id='+active_address
            try:
                resp = session.get(url, timeout=10)
                data = resp.json()
                for entry in data:
                    debank_chain_name = entry['chain']
                    if debank_chain_name in inverse_debank_mapping:
                        chain_name = inverse_debank_mapping[debank_chain_name]
                        if chain_name in all_chains:
                            chain_data = all_chains[chain_name]
                            if active_address in chain_data['import_addresses']:
                                # counterparties = chain_data['chain'].addresses

                                pil = entry['portfolio_item_list']
                                cp_name = entry['name']
                                address_set = {}
                                for pi in pil:
                                    pool = pi['pool']
                                    tag = pool['adapter_id']
                                    ids = pool['id'].split(":")
                                    for id_sub in ids:
                                        address_set[id_sub] = tag
                                    address_set[pool['controller']] = tag
                                    if pool['index'] is not None:
                                        address_set[pool['index']] = tag
                                for cp_address,cp_tag in address_set.items():
                                    entity_map = chain_data['chain'].entity_map
                                    if cp_address not in entity_map or entity_map[cp_address][0] == 'unknown':
                                        entity_map[cp_address] = [cp_name,None]

            except:
                self.current_import.add_error(Import.DEBANK_PROTOCOL_FAILURE, address=address, debug_info=traceback.format_exc())
                log_error("Failed to get counterparties from debank", active_address)


        # simplehash_chain_mapping = {'ETH':'ethereum','Polygon':'polygon','Arbitrum':'arbitrum','Avalanche':'avalanche','Gnosis':'gnosis','Optimism':'optimism','BSC':'bsc'}

        session = requests.session()
        session.headers.update({'X-API-Key': os.environ.get('api_key_simplehash')})
        rq_cnt = 0
        for chain_name, chain_data in all_chains.items():
            if chain_data['is_upload']:
                continue
            if 'simplehash_mapping' in Chain.CONFIG[chain_name]:
                rq_cnt += 1

        for chain_name, chain_data in all_chains.items():
            if chain_data['is_upload']:
                continue
            accepted_addresses = []
            addresses = chain_data['import_addresses']
            chain = chain_data['chain']
            for active_address in addresses:
                active_address = normalize_address(active_address)
                if chain.check_validity(active_address):
                    accepted_addresses.append(active_address)

            # if chain_name in simplehash_chain_mapping:
            if 'simplehash_mapping' in Chain.CONFIG[chain_name] and len(accepted_addresses) > 0:
                simplehash_mapping = Chain.CONFIG[chain_name]['simplehash_mapping']
                done = False
                if progress_bar:
                    progress_bar.update('Simplehash: Retrieving your NFTs on ' + chain_name,0)
                url = 'https://api.simplehash.com/api/v0/nfts/owners?chains=' + simplehash_mapping + '&wallet_addresses=' + ','.join(
                    accepted_addresses) + '&queried_wallet_balances=1&count=1'
                try:
                    page_idx = 0
                    while not done:
                        time.sleep(0.2)
                        log('simplehash url',url)
                        resp = session.get(url,timeout=15)
                        if resp.status_code != 200:
                            log_error("Failed to retrieve NFT data", url, resp.content)
                            break
                        data = resp.json()
                        total_count = data['count']
                        total_pages = total_count // 50 + 1

                        if progress_bar:
                            progress_bar.update('Simplehash: Retrieving your NFTs on ' + chain_name+': '+str(page_idx+1)+'/'+str(total_pages), 3./rq_cnt/total_pages)
                        entries = data['nfts']
                        for entry in entries:
                            contract_address = normalize_address(entry['contract_address'])
                            nft_id = entry['token_id']
                            symbol = entry['contract']['symbol']
                            type = entry['contract']['type']
                            #'ETH','Polygon','Arbitrum','Optimism','Avalanche','Gnosis'
                            if chain_name in ['BSC','Polygon','Fantom','zkEVM'] and type == 'ERC1155': #no scanner support for ERC1155 on these chains. Polygon support is shitty.
                                continue
                            for bal in entry['queried_wallet_balances']:
                                amount = bal['quantity']
                                owner = bal['address']
                                first_acquired = bal['first_acquired_date'][:19]
                                try:
                                    first_acquired_ts = int(datetime.strptime(first_acquired, "%Y-%m-%dT%H:%M:%S").timestamp())
                                except:
                                    log_error('failed to convert sh timestamp',first_acquired)
                                    first_acquired_ts = None
                                owner = normalize_address(owner)
                                if owner not in chain_data['current_tokens'] or chain_data['current_tokens'][owner] is None:
                                    log("missing current_tokens",chain_name,owner)
                                    chain_data['current_tokens'][owner] = {}
                                ct = chain_data['current_tokens'][owner]
                                if contract_address not in ct or 'nft_amounts' not in ct[contract_address]:
                                    ct[contract_address] = {'symbol':symbol,'nft_amounts':{},'acquisitions':{},'type':type}

                                if 'nft_amounts' not in ct[contract_address]:
                                    log_error(chain_name, contract_address, "trying to add an NFT",nft_id,amount," but counted as token on",chain_name)
                                ct[contract_address]['nft_amounts'][nft_id] = amount
                                ct[contract_address]['acquisitions'][nft_id] = first_acquired_ts
                                log("simplehash acquisition",contract_address,nft_id,first_acquired_ts)
                                floor_prices = entry['collection']['floor_prices']
                                if len(floor_prices) >= 1:
                                    for fp in floor_prices:
                                        if fp['marketplace_id'] == 'opensea':
                                            if fp['payment_token']['payment_token_id'] == 'ethereum.native':
                                                eth_floor_rate = fp['value'] / float(pow(10,18))
                                                ct[contract_address]['eth_floor'] = eth_floor_rate

                        if len(entries) < 50:
                            done = True

                        url = data['next']
                        if url is None:
                            done = True
                        page_idx += 1
                except:
                    self.current_import.add_error(Import.SIMPLEHASH_FAILURE, chain=chain, debug_info=traceback.format_exc())
                    log_error(chain_name, ": Failed to get_current_tokens:NFTs")



    def start_import(self,all_chains):
        self.current_import = Import(self,all_chains)

    def finish_import(self):
        self.current_import.finish(self)

    # def load_last_import(self,all_chains):
    #     self.current_import = Import.load_last(self,all_chains)
    def load_relevant_errors(self):
        return Import.load_errors(self)

    def load_import_versions(self):
        db = self.db
        rows = db.select(
            "SELECT address, chain, min(version) as min_ver, max(version) as max_ver "
            "FROM imports as i, imports_addresses as ia "
            "WHERE i.id = ia.import_id AND i.id IN " + sql_in(self.relevant_import_ids) + " GROUP BY address,chain", return_dictionaries=True)

        for row in rows:
            addr = row['address']
            chain_name = row['chain']
            if addr in self.all_addresses:
                if chain_name in self.all_addresses[addr]:
                    self.all_addresses[addr][chain_name]['min_ver'] = row['min_ver']
                    self.all_addresses[addr][chain_name]['max_ver'] = row['max_ver']


    def upload_csv(self,source,file, coingecko, pb):



        log('source',source,filename='file_uploads.txt')
        path = 'data/users/' + self.address+'/uploads/csv/'
        if not os.path.exists(path):
            os.makedirs(path)

        filename = secure_filename(source+".csv")
        file.save(os.path.join(path,filename))

        file = open(path+filename,'r', encoding='utf_8')
        csv_reader = csv.reader(file, delimiter=',')
        try:
            first_row = csv_reader.__next__()
        except:
            return "Unexpected characters in the first row. Is it really a CSV?", None
        ignore_column_indexes = set()
        headers = []
        for c_idx,h in enumerate(first_row):
            if len(h) == 0 or h[0] != '#':
                headers.append(h)
            else:
                ignore_column_indexes.add(c_idx)


        if len(headers) != 15:
            return "Unexpected number of headers, expected 15, got "+str(len(headers)), None

        # #no headers?
        # try:
        #     dt = datetime.strptime(headers[0][:19], "%Y-%m-%d %H:%M:%S")
        #     headers = ['Timestamp','Type','Base Currency','Base Amount','Quote Currency','Quote Amount','Fee Currency','Fee Amount','From','To','Blockchain','ID','Description','Reference Price Per Unit','Reference Price Currency']
        # except:
        #     pass


        rows = []

        for idx, row in enumerate(csv_reader):
            rows.append(row)
        file.close()

        fields = [
            {'name': headers[0], 'type':'date', 'required':True},
            {'name': headers[1], 'type': 'label', 'required':True},
            {'name': headers[2], 'type': 'currency','required':True},
            {'name': headers[3], 'type': 'float','required':True},
            {'name': headers[4], 'type': 'currency'},
            {'name': headers[5], 'type': 'float'},
            {'name': headers[6], 'type': 'currency'},
            {'name': headers[7], 'type': 'float'},
            {'name': headers[8], 'type': 'place'},
            {'name': headers[9], 'type': 'place'},
            {'name': headers[10], 'type': 'chain'},
            {'name': headers[11], 'type': 'id'},
            {'name': headers[12], 'type': 'desc'},
            {'name': headers[13], 'type': 'float'},
            {'name': headers[14], 'type': 'currency'},
        ]

        pb.set('Checking format', 5)


        #pass 1 -- check for errors and normalize values
        parsed = []
        for ridx,row in enumerate(rows):
            row_index = ridx+1
            vals = []

            field_cnt = 0
            for cidx, _ in enumerate(row):
                if cidx not in ignore_column_indexes:
                    field_cnt += 1
            if field_cnt != 15:
                return "Row " + str(row_index) + ": Unexpected number of columns, expected 15, got "+str(field_cnt), None
            #skip empty rows and comments
            if ''.join(row) == "" or len(row[0]) == 0 or row[0][0] == "#":
                continue
            skip = False
            # for field_idx,field in enumerate(fields):
            #     if field_idx in ignore_column_indexes:
            #         continue
            field_idx = -1
            for col_idx, val in enumerate(row):
                if col_idx in ignore_column_indexes:
                    continue
                if len(val) > 0 and val[0] == '#':
                    val = ""
                field_idx += 1
                field = fields[field_idx]
                val = val.strip()
                type = field['type']
                name = field['name']
                valm = val
                if 'required' in field and field['required'] and val == "":
                    return "Row "+str(row_index)+":"+ name + " is required", None


                if type == 'date':

                    try:
                        dt = datetime.strptime(val[:19], "%Y-%m-%d %H:%M:%S")
                        valm = int(dt.timestamp())
                    except:
                        return "Unexpected "+name+" in row " + str(row_index) + ", got \"" + val + "\", expecting YYYY-MM-DD HH:MM:SS", None
                elif type == 'float':
                    if len(val) != 0:
                        try:
                            valm = val.replace(",", "")
                            valm = float(valm)
                            if valm < 0:
                                return "Row "+str(row_index)+":"+name+" needs to be a positive number, got " + val, None
                        except:
                            return "Row "+str(row_index)+":"+name+" needs to be a positive number, got \"" + val +"\"", None
                elif type == 'currency':
                    valm = val.upper()
                elif type == 'label':
                    valm = val.lower()
                    if valm == 'skip':
                        skip = True
                        break


                    label = valm
                    if ":" in valm:
                        label,_ = valm.split(":")


                    if label not in ['sell', 'buy', 'fiat-deposit', 'fiat-withdrawal', 'receive', 'send', 'chain-split', 'expense',
                            'stolen', 'lost', 'burn', 'income', 'interest', 'mining', 'airdrop', 'staking', 'cashback', 'royalty', 'royalties', 'personal-use',
                            'gift', 'borrow', 'loan-repayment', 'liquidate', 'fee', 'margin-fee','from-vault','to-vault','realized-profit','realized-loss']:
                        return "Row " + str(row_index) + ": unknown " + name+", got \""+valm+"\", please click the help icon for the supported list", None
                vals.append(valm)

            if skip:
                continue
            # log('row',row_index,'vals',vals,filename='file_uploads.txt')

            for cp_idx in [2,4,6,13]:
                if (vals[cp_idx] != "" and vals[cp_idx+1] == "") or (vals[cp_idx] == "" and vals[cp_idx+1] != ""):
                    return "Row "+str(row_index)+":"+fields[cp_idx]['name']+" and "+fields[cp_idx+1]['name']+" should both be present, or both missing", None

            if vals[14] not in ["",self.fiat]:
                return "Row "+str(row_index)+":"+fields[14]['name'] + " can only be empty, or "+self.fiat+", got \""+str(vals[14])+"\" instead.", None

            if label in ['realized-profit','realized-loss'] and vals[2] == 'USD':
                return "Row " + str(row_index) + ": quote currency needs to be USD, base currency needs to be the name of the futures contract", None

            # if is_ethereum(vals[8]) and vals[10] == '':
            #     return "Row " + str(row_index) + ": please specify "+fields[10]['name']+" for "+vals[8], None
            # if is_ethereum(vals[9]) and vals[10] == '':
            #     return "Row " + str(row_index) + ": please specify "+fields[10]['name']+" for "+vals[9], None

            # if vals[1] in ['sell','buy']:
            #     if vals[5] == "" and vals[11] == "":
            #         return "Row " + str(row_index) + ": needs either " + fields[5]['name'] + " or " + fields[11]['name'], None




            parsed.append(vals)

        #pass 2 -- group trades together to find rates
        trades = {}
        def add_asset(trade,where,cur,amt):
            if cur not in trade[where]:
                trade[where][cur] = 0
            trade[where][cur] += amt


        def make_hash(row):
            ts, label, base_cur, base_amt, quote_cur, quote_amt, fee_cur, fee_amt, fr, to, chain_name, txhash, desc, usd_ref_price, usd_cur = row
            if txhash == "":
                txhash = hashlib.sha256((str(ts)+str(base_cur)+str(quote_cur)).encode('UTF-8')).hexdigest()
            return txhash

        for row in parsed:
            ts, label, base_cur, base_amt, quote_cur, quote_amt, fee_cur, fee_amt, fr, to, chain_name, txhash, desc, usd_ref_price, usd_cur = row
            if label not in ['buy','sell']:
                continue
            # if txhash == "":
            #     txhash = str(abs(hash(str(ts) + str(base_amt) + str(quote_amt) + str(fee_amt))))
            txhash = make_hash(row)

            if txhash not in trades:
                trade = {'in':{},'out':{},'ts':set()}
            else:
                trade = trades[txhash]
            trade['ts'].add(ts)
            if label == 'buy':
                add_asset(trade,'in',base_cur,base_amt)
                if quote_cur != "":
                    add_asset(trade, 'out', quote_cur, quote_amt)
            else:
                add_asset(trade, 'out', base_cur, base_amt)
                if quote_cur != "":
                    add_asset(trade, 'in', quote_cur, quote_amt)
            trades[txhash] = trade




        pb.set('Looking up rates', 20)
        #pass 3 -- coingecko rates -- need to map symbols to ids
        symbol_times = {}
        majors = ['BTC','ETH','USDT','USDC']
        for row in parsed:
            # log('row', row, filename='file_uploads.txt')
            # ts, out_amt, out_cur, in_amt, in_cur, fee_amt, fee_cur, usd_amt, usd_cur, label, desc, txhash = row
            ts, label, base_cur, base_amt, quote_cur, quote_amt, fee_cur, fee_amt, fr, to, chain_name, txhash, desc, usd_ref_price, usd_cur = row
            for cur in [base_cur, quote_cur, fee_cur]:
                if cur != "":
                    if cur not in symbol_times:
                        symbol_times[cur] = {'needed':set(),'custom_rates':{},'id_candidates':[],'BTC_rates':{},'ETH_rates':{},'USDC_rates':{},'USDT_rates':{}}
                    symbol_times[cur]['needed'].add(ts)
            if usd_ref_price != '':
                symbol_times[base_cur]['custom_rates'][ts] = usd_ref_price

        updated_trades = {}
        for txhash,trade in trades.items():
            if len(trade['in']) == 1 and len(trade['out']) == 1 and len(trade['ts']) == 1:
                log("trade",trade,filename='file_uploads.txt')
                in_cur = list(trade['in'].keys())[0]
                out_cur = list(trade['out'].keys())[0]
                in_amt = trade['in'][in_cur]
                out_amt = trade['out'][out_cur]
                ts = list(trade['ts'])[0]
                if in_cur == self.fiat:
                    symbol_times[out_cur]['custom_rates'][ts] = in_amt / out_amt
                if out_cur == self.fiat:
                    symbol_times[in_cur]['custom_rates'][ts] = out_amt / in_amt

                for coin in majors:
                    if in_cur == coin:
                        symbol_times[out_cur][coin+'_rates'][ts] = in_amt / out_amt
                    if out_cur == coin:
                        symbol_times[in_cur][coin+'_rates'][ts] = out_amt / in_amt
                updated_trades[txhash] = trade

            # elif base_amt != '' and quote_amt != '':
            #     if base_cur == 'USD':
            #         symbol_times[quote_cur]['custom_rates'][ts] = base_amt / quote_amt
            #     if quote_cur == 'USD':
            #         symbol_times[base_cur]['custom_rates'][ts] = quote_amt / base_amt
            #
            #
            #     for coin in majors:
            #         if base_cur == coin:
            #             symbol_times[quote_cur][coin+'_rates'][ts] = base_amt / quote_amt
            #         if quote_cur == coin:
            #             symbol_times[base_cur][coin+'_rates'][ts] = quote_amt / base_amt


        symbol_id_mapping = {
            'BTC':'bitcoin',
            'ETH':'ethereum',
            'USDT':'tether',
            'USDC':'usd-coin',
            'BNB':'binancecoin',
            'XRP':'ripple',
            'SOL':'solana',
            'DOGE':'dogecoin',
            'BCH':'bitcoin-cash',
            'AVAX':'avalanche-2'
        }

        rows = self.db.select("SELECT symbol, coingecko_id FROM tokens WHERE chain='"+source+"' AND coingecko_id IS NOT NULL")
        for row in rows:
            symbol_id_mapping[row[0]] = row[1]

        db = SQLite('db', do_logging=False, read_only=False)
        for symbol in majors: #need these first to get USD rates through them
            if symbol in symbol_times:
                id = symbol_id_mapping[symbol]
                to_download, ranges = coingecko.load_rates(db, id, symbol_times[symbol]['needed'])
                if len(to_download) > 0:
                    pb.update('Downloading rates for '+str(id))
                    coingecko.download_rates(db, id, to_download, ranges)

        for symbol,symbol_data in symbol_times.items():
            log('\nmapping '+symbol+' to coingecko id','symbol_data', symbol_data,filename='file_uploads.txt')
            if symbol in Twelve.FIAT_SYMBOLS:
                log(symbol,'is FIAT, skip',filename='file_uploads.txt')
                continue
            if symbol in symbol_id_mapping:
                id = symbol_id_mapping[symbol]
                log('assigning id -- in symbol_id_mapping', symbol, id, filename='file_uploads.txt')
                symbol_data['id'] = id
                to_download, ranges = coingecko.load_rates(db, id, symbol_data['needed'])
                if len(to_download) > 0:
                    pb.update('Downloading rates for ' + str(id))
                    coingecko.download_rates(db, id, to_download, ranges)
                continue



            log('still mapping ' + symbol + ' to coingecko id', filename='file_uploads.txt')
            sqlite_symbol = symbol.lower().replace("'","")
            rows = db.select("SELECT id FROM symbols WHERE symbol='"+sqlite_symbol+"' AND id NOT LIKE '%-peg%'")
            log('candidates', rows,  filename='file_uploads.txt')
            min_metric = 1
            max_available = 0
            best_match = None
            cand_count = len(rows)
            for row in rows:
                id = row[0]
                log("checking", id, symbol_data['needed'], filename='file_uploads.txt')



                to_download, ranges = coingecko.load_rates(db, id, symbol_data['needed'])
                log('loaded rates', id, ranges, filename='file_uploads.txt')
                if len(to_download) > 0:
                    log('init download',id,to_download, filename='file_uploads.txt')
                    pb.update('Downloading rates for ' + str(id))
                    coingecko.download_rates(db, id, to_download, ranges)

                rate_diff_entries = []
                for ts,usd_rate in symbol_data['custom_rates'].items():
                    good, coingecko_rate, rate_source = coingecko.lookup_rate_by_id(id,ts)
                    if good >= 1:
                        rate_diff = abs(usd_rate-coingecko_rate)/(usd_rate+coingecko_rate)
                        rate_diff_entries.append(rate_diff)
                for coin in majors:
                    for ts, custom_rate in symbol_data[coin+'_rates'].items():
                        good1, coingecko_rate, rate_source = coingecko.lookup_rate_by_id(id, ts)
                        good2, major_rate, rate_source = coingecko.lookup_rate_by_id(symbol_id_mapping[coin],ts)
                        if good1 >=1 and good2 >= 1:
                            usd_rate = custom_rate*major_rate
                            rate_diff = abs(usd_rate - coingecko_rate) / (usd_rate + coingecko_rate)
                            rate_diff_entries.append(rate_diff)

                if len(rate_diff_entries) > 0:
                    available = len(rate_diff_entries)
                    id_match_metric = median(rate_diff_entries)
                    log('metric calc',id_match_metric,rate_diff_entries,'available',available, filename='file_uploads.txt')
                    if min_metric < 0.02 and id_match_metric < 0.02: #if multiple good candidates, pick one with more data
                        if available > max_available:
                            best_match = id
                            max_available = available

                    elif id_match_metric < min_metric:
                        min_metric = id_match_metric
                        best_match = id
                        max_available = available
                else:
                    log('no rate data for needed times',filename='file_uploads.txt')

            if min_metric < 0.05:
                log('assigning id -- good metric', symbol, best_match, min_metric, filename='file_uploads.txt')
                symbol_data['id'] = best_match

            elif cand_count == 1 and min_metric == 1:
                log('assigning id -- only one option with no metric', symbol, rows[0], min_metric, filename='file_uploads.txt')
                symbol_data['id'] = rows[0][0]
            else:
                log('id not found', symbol, len(rows), min_metric, filename='file_uploads.txt')



        db.disconnect()

        pb.set('Populating transactions',40)
        # pass 3 -- determine token and address IDs, create/locate transactions, populate transfer arrays (but don't add them to db yet)
        db = self.db
        t = int(time.time())
        db.insert_kw('uploads',started=t,status=0,source=source,version=self.version)
        upload_id = db.select("SELECT last_insert_rowid()")[0][0]

        self_address='my account'
        self_address_id = self.locate_insert_address(source, self_address)
        rows = db.select("SELECT * FROM user_addresses WHERE address='"+self_address+"' AND chain='"+source+"'")
        if len(rows) == 0:
            db.insert_kw('user_addresses',address=self_address,chain=source,previously_used=1,present=1,last_update=t, is_upload=1)
        else:
            db.update_kw('user_addresses',"address='"+self_address+"' AND chain='"+source+"'",last_update=t)

        other_address_id = self.locate_insert_address(source, 'elsewhere')
        fee_address_id = self.locate_insert_address(source, source+' fee')
        margin_fee_address_id = self.locate_insert_address(source, source+' margin fee')
        loan_address_id = self.locate_insert_address(source, source+' loan')
        vault_address_id = self.locate_insert_address(source, source + ' vault')
        counter_address_id = self.locate_insert_address(source, 'counter-trader')
        all_new_transfers = {}
        function = None



        for row in parsed:
            ts, label, base_cur, base_amt, quote_cur, quote_amt, fee_cur, fee_amt, fr, to, chain_name, txhash, desc, usd_ref_price, usd_cur = row
            # if label in ['fiat-deposit','fiat-withdrawal']:
            #     continue

            # if txhash == "": #create unique id for transaction
            #     txhash = str(ts)+str(base_amt)+str(quote_amt)+str(fee_amt)
            # txhash = source +txhash
            # txhash = str(hash(txhash))

            # if txhash == "":
            #     txhash = str(abs(hash(str(ts) + str(base_amt) + str(quote_amt) + str(fee_amt))))
            txhash = make_hash(row)


            if desc == "":
                desc = None



            base_token_id = None
            quote_token_id = None
            fee_token_id = None
            # chain_name = chain_name.lower()

            # see if we can find on-chain contracts
            if chain_name != "":
                symbol_data = symbol_times[base_cur]
                try:
                    coingecko_id = symbol_data['id']
                    contract = coingecko.inverse_contract_map[chain_name][coingecko_id]
                    base_token_id = self.locate_insert_token(chain_name, contract, base_cur, coingecko_id=coingecko_id)
                    log('Found contract for ', chain_name, coingecko_id, contract, base_token_id, filename='file_uploads.txt')
                except:
                    log('Did not find contract for ',chain_name, base_cur, symbol_data, traceback.format_exc(), filename='file_uploads.txt')

                if quote_cur != "":
                    symbol_data = symbol_times[quote_cur]
                    try:
                        coingecko_id = symbol_data['id']
                        contract = coingecko.inverse_contract_map[chain_name][coingecko_id]
                        quote_token_id = self.locate_insert_token(chain_name, contract, quote_cur, coingecko_id=coingecko_id)
                        log('Found contract for ', chain_name, coingecko_id, contract, quote_token_id, filename='file_uploads.txt')
                    except:
                        log('Did not find contract for ', chain_name, quote_cur, symbol_data, traceback.format_exc(), filename='file_uploads.txt')

                if fee_cur != "":
                    if fee_cur == base_cur:
                        if base_token_id is not None:
                            fee_token_id = base_token_id
                    elif fee_cur == quote_cur:
                        if quote_token_id is not None:
                            fee_token_id = quote_token_id
                    else:
                        symbol_data = symbol_times[fee_cur]
                        try:
                            coingecko_id = symbol_data['id']
                            contract = coingecko.inverse_contract_map[chain_name][coingecko_id]
                            fee_token_id = self.locate_insert_token(chain_name, contract, fee_cur, coingecko_id=coingecko_id)
                            log('Found contract for ', chain_name, coingecko_id, contract, fee_token_id, filename='file_uploads.txt')
                        except:
                            log('Did not find contract for ', chain_name, fee_cur, symbol_data, traceback.format_exc(), filename='file_uploads.txt')

            if base_token_id is None:
                symbol_data = symbol_times[base_cur]
                coingecko_id = None
                if 'id' in symbol_data:
                    coingecko_id = symbol_data['id']
                base_token_id = self.locate_insert_token(source, base_cur, base_cur, coingecko_id=coingecko_id)

            if quote_cur != "" and quote_token_id is None:
                symbol_data = symbol_times[quote_cur]
                coingecko_id = None
                if 'id' in symbol_data:
                    coingecko_id = symbol_data['id']
                quote_token_id = self.locate_insert_token(source, quote_cur, quote_cur, coingecko_id=coingecko_id)

            if fee_cur != "" and fee_amt > 0:
                if fee_cur == base_cur:
                    if base_token_id is not None:
                        fee_token_id = base_token_id
                elif fee_cur == quote_cur:
                    if quote_token_id is not None:
                        fee_token_id = quote_token_id
                else:
                    symbol_data = symbol_times[fee_cur]
                    coingecko_id = None
                    if 'id' in symbol_data:
                        coingecko_id = symbol_data['id']
                    fee_token_id = self.locate_insert_token(source, fee_cur, fee_cur, coingecko_id=coingecko_id)

            if chain_name is None or chain_name not in Chain.CONFIG:
                chain_name = source

            fr_id = None
            if fr != "" and fr is not None:
                fr_id = self.locate_insert_address(chain_name, fr)

            to_id = None
            if to != "" and to is not None:
                to_id = self.locate_insert_address(chain_name, to)

            # if is_ethereum(fr):
            #     fr_id = self.locate_insert_address(chain_name, fr)
            # if chain_name == 'Solana' and is_solana(fr):
            #     fr_id = self.locate_insert_address(chain_name, fr)
            #
            # to_id = None
            # if is_ethereum(to):
            #     to_id = self.locate_insert_address(chain_name, to)
            # if chain_name == 'Solana' and is_solana(to):
            #     to_id = self.locate_insert_address(chain_name, to)

            received_token_id = None
            sent_token_id = None
            received_treatment = None
            received_vaultid = None
            sent_treatment = None
            sent_vaultid = None
            extra_token_id = None

            # if label in ['buy','sell'] and txhash in updated_trades:
            vault_id = None
            if ":" in label:
                label,vault_id = label.split(":")

            # if label != "fee":
            function = label

            if base_amt > 0:
                if label == 'buy':
                    received_token_id = base_token_id
                    received_amt = base_amt
                    received_fr_id = fr_id or counter_address_id
                    received_to_id = to_id or self_address_id


                    if quote_token_id is not None:
                        sent_token_id = quote_token_id
                        sent_amt = quote_amt
                        sent_fr_id = fr_id or self_address_id
                        sent_to_id = to_id or counter_address_id
                    function = "trade"


                elif label == 'sell':
                    sent_token_id = base_token_id
                    sent_amt = base_amt
                    sent_fr_id = fr_id or self_address_id
                    sent_to_id = to_id or counter_address_id


                    if quote_token_id is not None:
                        received_token_id = quote_token_id
                        received_amt = quote_amt
                        received_fr_id = fr_id or counter_address_id
                        received_to_id = to_id or self_address_id
                    function = "trade"


                elif label in ['receive','deposit','chain-split','income','interest','mining','airdrop','cashback','royalty','royalties','gift','borrow','staking','fiat-deposit','from-vault']:
                    received_token_id = base_token_id
                    received_amt = base_amt
                    if fr_id is None:
                        if label == 'from-vault':
                            received_fr_id = vault_address_id
                            received_vaultid = vault_id
                        elif label == 'borrow':
                            received_fr_id = loan_address_id
                            received_vaultid = vault_id
                        else:
                            received_fr_id = other_address_id
                    else:
                        received_fr_id = fr_id
                    if to_id is None:
                        received_to_id = self_address_id
                    else:
                        received_to_id = to_id
                    # if label in ['chain-split','income','interest','mining','airdrop','cashback','royalty','royalties','gift','staking']:
                    #     received_treatment = 'income'
                    # if label == 'borrow':
                    #     received_treatment = 'borrow'
                    #     received_vaultid = source+" "+base_cur

                elif label in ['send','withdrawal','expense','stolen','lost','burn','personal-use','loan-repayment','liquidate','margin-fee','fiat-withdrawal','to-vault']:
                    sent_token_id = base_token_id
                    sent_amt = base_amt
                    if fr_id is None:
                        sent_fr_id = self_address_id
                    else:
                        sent_fr_id = fr_id
                    if to_id is None:
                        if label == 'margin-fee':
                            sent_to_id = margin_fee_address_id
                        elif label == 'to-vault':
                            sent_to_id = vault_address_id
                            sent_vaultid = vault_id
                        elif label == 'loan-repayment':
                            sent_to_id = loan_address_id
                            sent_vaultid = vault_id
                        else:
                            sent_to_id = other_address_id
                    else:
                        sent_to_id = to_id

                elif label in ['realized-profit','realized-loss']:
                    received_token_id = sent_token_id = base_token_id
                    received_amt = sent_amt = base_amt
                    received_fr_id = sent_to_id = other_address_id
                    received_to_id = sent_fr_id = self_address_id
                    if quote_token_id is not None:
                        extra_treatment = None
                        extra_token_id = quote_token_id
                        extra_amt = quote_amt
                        if label == 'realized-profit':
                            extra_type = Transfer.UPLOAD_IN
                            extra_to_id = self_address_id
                            extra_fr_id = other_address_id
                        else:
                            extra_type = Transfer.UPLOAD_OUT
                            extra_to_id = other_address_id
                            extra_fr_id = self_address_id


                    # if label in ['expense','stolen','lost','burn','personal-use','liquidate','margin-fee']:
                    #     sent_treatment = 'burn'

                elif label == 'fee':
                    # sent_treatment = 'fee'
                    fee_token_id = base_token_id
                    fee_amt = base_amt

                    # if label == 'loan-repayment':
                    #     sent_treatment = 'repay'
                    #     sent_vaultid = source+" "+base_cur

            custom_rate = None
            if usd_ref_price != "":
                custom_rate = usd_ref_price
                log('custom rate for txhash',txhash,usd_ref_price,filename='file_uploads.txt')

            # locate_insert_transaction(self,chain_name,hash,timestamp,nonce,block, interacted, function, custom_note=None,upload_source=None)

            txid, existing = self.locate_insert_transaction(source, txhash, ts, 0, 0, None, None, function, custom_note=desc, upload_id=upload_id)
            log('locate_insert_transaction', txhash, txid, existing, filename='file_uploads.txt')
            if txid not in all_new_transfers:
                all_new_transfers[txid] = {'existing': existing, 'new_transfers': [], 'txhash': txhash, 'functions':set(), 'ts':ts, 'notes':[]}
            all_new_transfers[txid]['functions'].add(function)
            if desc is not None:
                all_new_transfers[txid]['notes'].append(desc)

            new_transfers = []
            if received_token_id is not None:
                new_transfers.append({'type':Transfer.UPLOAD_IN,'from_addr_id':received_fr_id,'to_addr_id':received_to_id, 'val':received_amt,
                                      'token_id':received_token_id, 'upload_id':upload_id, 'custom_treatment':received_treatment, 'custom_vaultid':received_vaultid,
                                      'custom_rate':custom_rate, 'base_fee':0,'input_len':0})
            #
            if sent_token_id is not None:
                new_transfers.append({'type':Transfer.UPLOAD_OUT,'from_addr_id': sent_fr_id, 'to_addr_id': sent_to_id, 'val': sent_amt,
                                      'token_id': sent_token_id, 'upload_id':upload_id, 'custom_treatment':sent_treatment, 'custom_vaultid':sent_vaultid,
                                      'custom_rate':custom_rate, 'base_fee':0,'input_len':0})
            #
            if fee_token_id is not None:
                new_transfers.append({'type':Transfer.UPLOAD_FEE,'from_addr_id': self_address_id, 'to_addr_id': fee_address_id, 'val': fee_amt, 'token_id': fee_token_id, 'upload_id':upload_id,
                                      'custom_treatment':None, 'custom_vaultid':None,'base_fee':0,'input_len':0, 'synthetic':Transfer.FEE})

            if extra_token_id is not None:
                new_transfers.append({'type': extra_type, 'from_addr_id': extra_fr_id, 'to_addr_id': extra_to_id, 'val': extra_amt,
                                      'token_id': extra_token_id, 'upload_id': upload_id, 'custom_treatment': extra_treatment, 'custom_vaultid':None,
                                      'custom_rate':custom_rate, 'base_fee': 0, 'input_len': 0})

            all_new_transfers[txid]['new_transfers'].extend(new_transfers)



        pb.set('Adding data to database', 60)
        #pass 4 -- compare new transfers to existing ones, find approximate matches, insert/update/delete
        for txid in all_new_transfers:
            new_transfers = all_new_transfers[txid]['new_transfers']
            txhash = all_new_transfers[txid]['txhash']
            notes = all_new_transfers[txid]['notes']
            ts = all_new_transfers[txid]['ts']
            functions = list(all_new_transfers[txid]['functions'])
            #multiple rows for the same ID, and multiple different labels -- agglo labels
            if len(functions) > 1 or len(notes) > 1:
                if 'fee' in functions:
                    functions.remove('fee')
                elif 'margin-fee' in functions:
                    functions.remove('margin-fee')
                function = ','.join(functions)
                note = None
                if len(notes) > 0:
                    note = '<br>'.join(notes)
                txid, existing = self.locate_insert_transaction(source, txhash, ts, 0, 0, None, None, function, custom_note=note, upload_id=upload_id)
            log('\n\ntxid, hash',txid,txhash, filename='file_uploads.txt')

            existing_transfers = db.select("SELECT * FROM transaction_transfers WHERE transaction_id="+str(txid)+" AND upload_id IS NOT NULL",return_dictionaries=True)
            log('existing_transfers',existing_transfers, filename='file_uploads.txt')
            log('new_transfers', new_transfers, filename='file_uploads.txt')

            transfer_fields = ['type','from_addr_id','to_addr_id','val','token_id']
            update_fields = ['custom_treatment','custom_vaultid']
            #finds similar transfers among existing ones. If only different in one thing, update. Otherwise, replace.
            for nt in new_transfers:
                best_diff = 100
                best_match = None
                for et in existing_transfers:
                    if 'matched' not in et:
                        diffcnt = 0
                        for field in transfer_fields:
                            if nt[field] != et[field]:
                                diffcnt += 1
                        if diffcnt == 0:
                            log(txhash,"Fully matched existing transfer",et,"to new transfer", nt, filename='file_uploads.txt')
                            for field in update_fields:
                                if nt[field] != et[field]:
                                    log(txhash, "Updating due to new update_field", nt,'new',nt[field],'old',et[field], filename='file_uploads.txt')
                                    db.update_kw('transaction_transfers', 'id=' + str(et['id']), **nt)
                                    break
                            et['matched'] = 'full'
                            break
                        if diffcnt < best_diff:
                            best_diff = diffcnt
                            best_match = et
                else:
                    # if best_diff == 1:
                    if best_diff <= 1:
                        best_match['matched'] = 'partial'
                        log(txhash,"Partially matched existing transfer",best_match,"to new transfer",nt,"updating", filename='file_uploads.txt')
                        db.update_kw('transaction_transfers','id='+str(best_match['id']),**nt)
                    else:
                        log(txhash,"Did not match existing transfers", best_diff, "to new transfer", nt, "inserting", filename='file_uploads.txt')
                        nt['transaction_id'] = txid
                        db.insert_kw('transaction_transfers', **nt)
            for et in existing_transfers:
                if 'matched' not in et:
                    log(txhash,"Existing transfer not found in new", et, "deleting", filename='file_uploads.txt')
                    db.query("DELETE FROM transaction_transfers WHERE id="+str(et['id']))


        #delete transactions that have become empty
        upload_ids = []
        rows = db.select("SELECT id FROM uploads WHERE source='"+source+"'")
        for row in rows:
            upload_ids.append(row[0])
        txids = set()
        rows = db.select("SELECT id FROM transactions WHERE upload_id IN "+sql_in(upload_ids))
        for row in rows:
            txids.add(row[0])
        txids_from_transfers = set()
        rows = db.select("select distinct transaction_id from transaction_transfers")
        for row in rows:
            txids_from_transfers.add(row[0])
        txids_to_delete = txids.difference(txids_from_transfers)
        log("Deleting empty transactions", list(txids_to_delete), filename='file_uploads.txt')
        if len(txids_to_delete) > 0:
            db.query("DELETE FROM transactions WHERE id IN "+sql_in(list(txids_to_delete)))



        db.update_kw('uploads', "id="+str(upload_id), ended=int(time.time()), status=1)
        db.commit()
        coingecko.dump(self)

        new_txids = list(txids.difference(txids_to_delete))
        log("Loading transactions", list(new_txids),filename='file_uploads.txt')
        pb.set('Loading transactions from database', 75)
        chain = self.chain_factory(source,is_upload=True)
        S = Signatures()

        self.load_addresses()
        self.load_tx_counts()
        transactions, _ = self.load_transactions({source: {'chain': chain}}, tx_id_list=new_txids)
        _, _, input_list = self.get_contracts(transactions)
        S.init_from_db(input_list)
        self.wipe_derived_data(transactions)

        pb.set('Classifying transactions', 85)
        transactions_js = self.transactions_to_log(coingecko, S, transactions, store_derived=True)
        self.set_info('force_forget_derived', 1)

        return None, transactions_js


    def delete_upload(self,source):
        db = self.db
        txids = []
        txrows = db.select("SELECT id FROM transactions WHERE chain='"+source+"'")
        for row in txrows:
            txids.append(row[0])

        self.db.query("DELETE FROM transactions WHERE id IN " + sql_in(txids))
        self.db.query("DELETE FROM transactions_derived WHERE id IN " + sql_in(txids))
        self.db.query("DELETE FROM transaction_transfers_derived WHERE id IN (SELECT id FROM transaction_transfers WHERE transaction_id IN " + sql_in(txids) + ")")
        self.db.query("DELETE FROM transaction_transfers WHERE transaction_id IN " + sql_in(txids))

        self.db.query("DELETE FROM addresses WHERE chain='" + source + "'")
        self.db.query("DELETE FROM tokens WHERE chain='" + source + "'")
        self.db.query("DELETE FROM user_addresses WHERE chain='"+source+"'")
        self.db.query("VACUUM")
        self.set_info('force_forget_derived',1)
        self.db.commit()
        return txids

    def delete_address(self,address_to_delete):
        address_ids_to_delete = set()
        address_ids_to_keep = set()
        reproc_needed = False
        for address in self.all_addresses:
            for chain,entry in self.all_addresses[address].items():
                id = entry['id']
                if address == address_to_delete:
                    address_ids_to_delete.add(id)
                    if entry['used']:
                        reproc_needed = True
                else:
                    address_ids_to_keep.add(id)
        ids_to_keep_query = \
            "SELECT id, transaction_id FROM transaction_transfers WHERE from_addr_id in "+sql_in(address_ids_to_keep)+" OR to_addr_id in "+sql_in(address_ids_to_keep)
        log("address ids, keep",address_ids_to_keep,"delete",address_ids_to_delete,filename='delete_address.txt')
        transfer_ids_to_keep = set()
        transaction_ids_to_keep = set()
        rows = self.db.select(ids_to_keep_query)
        for row in rows:
            transfer_ids_to_keep.add(row[0])
            transaction_ids_to_keep.add(row[1])
        log("entry ids to keep, transfers", len(transfer_ids_to_keep), "transactions",  len(transaction_ids_to_keep), filename='delete_address.txt')
        self.db.query("DELETE FROM transaction_transfers WHERE id NOT IN "+sql_in(transfer_ids_to_keep))
        self.db.query("DELETE FROM transactions WHERE id NOT IN " + sql_in(transaction_ids_to_keep))
        if not reproc_needed:
            self.db.query("DELETE FROM transaction_transfers_derived WHERE id NOT IN " + sql_in(transfer_ids_to_keep))
            self.db.query("DELETE FROM transactions_derived WHERE id NOT IN " + sql_in(transaction_ids_to_keep))

        self.db.query("DELETE FROM user_addresses WHERE address='"+address_to_delete+"'")
        self.db.commit()
        return reproc_needed


    def update_coingecko_id(self,chain_name,contract,new_id, coingecko, pb):
        pb.set('Updating token', 5)
        db = self.db
        if len(new_id) == 0:
            new_id = None
        db.update_kw('tokens',"contract='"+contract+"' AND chain='"+chain_name+"'",custom_coingecko_id=new_id)
        db.commit()
        rows = db.select("SELECT id FROM tokens WHERE contract='"+contract+"' AND chain='"+chain_name+"'")
        if len(rows) != 1:
            return "Error when looking for a token", None
        token_id = rows[0][0]
        rows = db.select("SELECT transaction_id FROM transaction_transfers WHERE token_id="+str(token_id))
        txids = []
        for row in rows:
            txids.append(row[0])

        pb.set('Loading transactions from database', 15)
        chain = self.chain_factory(chain_name, is_upload=chain_name not in Chain.CONFIG)
        chain_dict = {chain_name: {'chain': chain}}
        S = Signatures()
        self.load_addresses()
        self.load_tx_counts()

        transactions, _ = self.load_transactions(chain_dict,tx_id_list=txids)
        needed_token_times = self.get_needed_token_times(transactions)

        pb.set('Updating Coingecko data', 50)
        coingecko.init_from_db_2(chain_dict,needed_token_times,pb)
        coingecko.dump(self)


        _, _, input_list = self.get_contracts(transactions)
        S.init_from_db(input_list)
        self.wipe_derived_data(transactions)
        pb.set('Classifying transactions', 85)
        transactions_js = self.transactions_to_log(coingecko, S, transactions, store_derived=True)
        self.set_info('force_forget_derived', 1)
        return None, transactions_js

    def apply_fiat(self,transaction):
        ts = transaction.ts
        rate = self.fiat_rates.lookup_rate(self.fiat,ts)
        transaction.fiat_rate = rate

    def adjust_custom_rates(self,new_fiat):
        query = "select t.timestamp, tr.id, tr.custom_rate from transactions as t, transaction_transfers as tr where t.id = tr.transaction_id and custom_rate is not null"
        rows = self.db.select(query)
        if len(rows):
            for row in rows:
                ts, transfer_id, old_custom_rate = row
                old_fiat_rate = self.fiat_rates.lookup_rate(self.fiat,ts)
                new_fiat_rate = self.fiat_rates.lookup_rate(new_fiat, ts)
                new_custom_rate = old_custom_rate * new_fiat_rate/old_fiat_rate
                self.db.update_kw('transaction_transfers',"id="+str(transfer_id),custom_rate=new_custom_rate)
            self.db.commit()