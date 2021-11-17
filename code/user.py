from sortedcontainers import *
from .sqlite import SQLite
from .util import log
from .transaction import *
from .coingecko import Coingecko
from .signatures import Signatures
from .classifiers import Classifier
from .chain import Chain
import os

class User:

    def __init__(self,address):
        address = address.lower()
        self.address = address

        self.rate_sources = ['usd','shortcut','inferred','exact','cg before first','cg after last','cg','adjusted']

        path = 'data/users/'+address
        log("check",path)
        if not os.path.exists(path):
            log("doesn't exist")
            os.makedirs(path)

        self.db = SQLite('users/' + address+'/db')

        drop = False
        self.db.create_table('custom_names', 'chain, address, name', drop=drop)
        self.db.create_index('custom_names_idx', 'custom_names', 'chain,address', unique=True)

        self.db.create_table('custom_types', 'chain, id integer primary key autoincrement, name, description, balanced integer', drop=drop)
        self.db.create_index('custom_types_idx', 'custom_types', 'chain, name', unique=True)
        self.db.create_table('custom_types_rules', 'id integer primary key autoincrement, type_id integer, '
                                                   'from_addr, from_addr_custom, to_addr, to_addr_custom, token, token_custom, treatment, vault_id, vault_id_custom',
                             drop=drop)
        # self.db.create_table('custom_types_applied', 'type_id integer, transaction_id integer primary key', drop=drop)
        # self.db.create_index('custom_types_applied_idx', 'custom_types_applied', 'type_id')

        # self.db.create_table('custom_colors', 'transaction_id integer primary key, color integer', drop=drop)
        # self.db.create_index('custom_colors_idx', 'custom_colors', 'color')


        # drop=False
        self.db.create_table('addresses', 'id integer primary key, chain, address', drop=drop)
        self.db.create_index('addresses_idx', 'addresses', 'address, chain', unique=True)

        self.db.create_table('tokens', 'id integer primary key, chain, contract, symbol', drop=drop)
        self.db.create_index('tokens_idx', 'tokens', 'chain, contract, symbol', unique=True)


        self.db.create_table('transactions', 'id integer primary key autoincrement, chain, hash, timestamp INTEGER, custom_type_id INTEGER, custom_color_id INTEGER',drop=drop)
        self.db.create_index('transactions_idx', 'transactions', 'hash', unique=True)

        self.db.create_table('transaction_transfers', 'id integer primary key autoincrement, type integer, idx INTEGER, transaction_id INTEGER, from_addr_id INTEGER, to_addr_id INTEGER, val REAL, token_id INTEGER, token_nft_id INTEGER, base_fee REAL, input_len INTEGER, input, '
                                                      'custom_treatment, custom_rate REAL, custom_vaultid', drop=drop)
        self.db.create_index('transaction_transfers_idx', 'transaction_transfers', 'idx, transaction_id', unique=True)

        # self.db.create_table('rates', 'transaction_id INTEGER, transfer_idx INTEGER, rate REAL, source INTEGER, level INTEGER', drop=drop)
        # self.db.create_index('rates_idx', 'rates', 'transaction_id, transfer_idx', unique=True)

        # self.db.create_table('custom_treatment', 'transaction_id INTEGER, transfer_idx INTEGER, treatment', drop=drop)
        # self.db.create_index('custom_treatment_idx', 'custom_treatment', 'transaction_id, transfer_idx', unique=True)
        #
        # self.db.create_table('custom_rates', 'transaction_id INTEGER, transfer_idx INTEGER, rate REAL', drop=drop)
        # self.db.create_index('custom_rates_idx', 'custom_rates', 'transaction_id, transfer_idx', unique=True)
        self.db.commit()



        self.custom_addresses = {}


    def locate_insert_transaction(self,chain_name,hash,timestamp):
        db = self.db
        row = db.select("SELECT id FROM transactions WHERE hash='" + hash + "'")
        if len(row) == 1:
            return row[0][0]
        else:
            db.insert_kw('transactions', chain=chain_name, hash=hash,timestamp=timestamp)
            return db.select("SELECT last_insert_rowid()")[0][0]

    def locate_insert_address(self,chain_name,address):
        db = self.db
        row = db.select("SELECT id FROM addresses WHERE chain='" + chain_name + "' and address = '" + address + "'")
        if len(row) == 1:
            return row[0][0]
        else:
            db.insert_kw('addresses', chain=chain_name, address=address)
            return db.select("SELECT last_insert_rowid()")[0][0]

    def locate_insert_token(self,chain_name,contract,symbol):
        db = self.db
        if contract is None:
            row = db.select("SELECT id FROM tokens WHERE chain='" + chain_name + "' and symbol = '" + symbol + "'")
        else:
            row = db.select("SELECT id FROM tokens WHERE chain='" + chain_name + "' and contract = '" + contract + "'")
        if len(row) == 1:
            return row[0][0]
        else:
            db.insert_kw('tokens', chain=chain_name, contract=contract, symbol=symbol)
            return db.select("SELECT last_insert_rowid()")[0][0]


    def store_transactions(self,chain,transactions):
        db = self.db
        for idx, transaction in enumerate(transactions.values()):
            hash = transaction.grouping[0][1][0]
            timestamp = transaction.grouping[0][1][1]

            # db.insert_kw('transactions', chain=chain.name, hash=hash, timestamp=timestamp)
            # txid = db.select("SELECT last_insert_rowid()")[0][0]
            txid = self.locate_insert_transaction(chain.name,hash,timestamp)
            for index, (type, sub_data, loaded_index, _, _, _) in enumerate(transaction.grouping):
                if loaded_index is not None:
                    index = loaded_index


                hash, ts, fr, to, val, token, token_contract, token_nft_id, base_fee, input_len, input = sub_data
                fr_id = self.locate_insert_address(chain.name, fr)
                to_id = self.locate_insert_address(chain.name, to)
                token_id = self.locate_insert_token(chain.name,token_contract,token)

                #type integer, idx INTEGER, transaction_id, from_addr_id, to_addr_id, val REAL, token_id,  base_fee REAL, input_len INTEGER, input
                if input is not None:
                    input = input[:20]
                # db.insert_kw('transaction_transfers', values=[type,index,txid,fr_id,to_id,val,token_id,token_nft_id,base_fee,input_len,input])
                db.insert_kw('transaction_transfers', type=type, idx=index, transaction_id=txid,from_addr_id=fr_id,to_addr_id=to_id,
                             val=val,token_id=token_id,token_nft_id=token_nft_id,base_fee=base_fee,input_len=input_len,input=input, ignore=True)
        db.commit()

    def load_transactions(self,chain,tx_id_list=None):
        db = self.db
        # query = "select * from transactions, transaction_transfers where chain='" + self.name + " and transactions.hash = transaction_transfers.hash ORDER BY timestamp,idx"
        query = "SELECT " \
                "tx.id, tx.hash, tx.timestamp, tx.custom_type_id, tx.custom_color_id, " \
                "tr.id, tr.type, tr.idx, from_addr.address, to_addr.address, tr.val, tk.symbol, tk.contract, tr.token_nft_id, tr.base_fee, tr.input_len, tr.input," \
                "tr.custom_treatment, tr.custom_rate, tr.custom_vaultid " \
                "FROM transactions as tx, transaction_transfers as tr, addresses as from_addr, addresses as to_addr, tokens as tk " \
                "WHERE tx.id = tr.transaction_id AND tr.from_addr_id = from_addr.id AND tr.to_addr_id = to_addr.id AND tr.token_id = tk.id "
        if chain is not None:
            query += " AND tx.chain = '"+chain.name+"' "
        if tx_id_list is not None:
            tx_id_str = ",".join(tx_id_list)
            query += " AND tx.id IN ("+tx_id_str+") "
        query += "ORDER BY tx.timestamp, tr.idx"

        rows = db.select(query)
        transactions = SortedDict()
        for row in rows:
            txid, hash, ts, custom_type_id, custom_color_id, trid, transfer_type,idx,fr,to,val,token,token_contract,token_nft_id, base_fee, input_len, input, \
            custom_treatment, custom_rate, custom_vaultid = row
            uid = str(ts) + "_" + str(hash)
            if uid not in transactions:
                transactions[uid] = Transaction(chain,txid=txid, custom_type_id=custom_type_id, custom_color_id=custom_color_id)
            row = [hash, ts, fr, to, val, token, token_contract, token_nft_id, base_fee, input_len, input]
            transactions[uid].append(transfer_type, row, transfer_idx=idx, custom_treatment=custom_treatment, custom_rate=custom_rate, custom_vaultid=custom_vaultid)
        return transactions


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



    def save_custom_type(self,chain_name,address,name,description,balanced,rules, id=None):
        if id is not None:
            # self.db.query("DELETE FROM custom_types WHERE id="+id)
            self.db.query("UPDATE custom_types SET name = '"+name+"', description = '"+description+"', balanced = "+str(balanced)+" WHERE id="+str(id))
            self.db.query("DELETE FROM custom_types_rules WHERE type_id=" + id)
        else:
            self.db.insert_kw('custom_types', chain=chain_name, name=name, description=description, balanced=balanced)


        rows = self.db.select("SELECT id FROM custom_types WHERE name='"+name+"'")
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

    def load_custom_types(self,chain_name):
        rows = self.db.select("SELECT t.id, t.name, t.description, t.balanced, r.* FROM custom_types as t, custom_types_rules as r WHERE t.chain='"+chain_name+"' and t.name != '' and t.id = r.type_id ORDER BY t.name ASC, r.id ASC")
        if len(rows) == 0:
            return []

        js = []
        cur_type = {'id':rows[0][0], 'name':rows[0][1], 'rules':[], 'description':rows[0][2],'balanced':rows[0][3]}
        for row in rows:
            type_id, type_name, type_desc, type_balanced, rule_id, _, from_addr, from_addr_custom, to_addr, to_addr_custom, token, token_custom, treatment, vault_id, vault_id_custom = row
            if cur_type['id'] != type_id:
                js.append(cur_type)
                cur_type = {'id':type_id,'name':type_name,'description':type_desc,'balanced':type_balanced,'rules':[]}
            cur_type['rules'].append([rule_id,from_addr,from_addr_custom,to_addr,to_addr_custom,token,token_custom,treatment,vault_id,vault_id_custom])
        js.append(cur_type)
        log(js)
        return js


    def prepare_all_custom_types(self,chain_name):
        # tx_ct_mapping = {}
        # rows = self.db.select('select * from custom_types_applied')
        # for row in rows:
        #     tx_ct_mapping[row[1]] = row[0]

        ct_info = {}
        rows = self.db.select("SELECT id, name, description, balanced FROM custom_types where chain='"+chain_name+"'")
        for row in rows:
            rules = self.db.select("SELECT * FROM custom_types_rules WHERE type_id = "+str(row[0])+" ORDER BY id ASC")
            ct_info[row[0]] = {'name': row[1], 'description':row[2],'balanced':row[3],'rules':rules}
        # self.tx_ctype_mapping = tx_ct_mapping
        self.ctype_info = ct_info
    #
    # def prepare_all_custom_treatment_and_rates(self):
    #     tx_ctreat_mapping = {}
    #     tx_crate_mapping = {}
    #     rows = self.db.select('select * from custom_treatment')
    #     for row in rows:
    #         txid = row[0]
    #         if txid not in tx_ctreat_mapping:
    #             tx_ctreat_mapping[txid] = {}
    #         tx_ctreat_mapping[txid][row[1]] = row[2]
    #     self.tx_ctreat_mapping = tx_ctreat_mapping
    #
    #     rows = self.db.select('select * from custom_rates')
    #     for row in rows:
    #         txid = row[0]
    #         if txid not in tx_crate_mapping:
    #             tx_crate_mapping[txid] = {}
    #         tx_crate_mapping[txid][row[1]] = row[2]
    #     self.tx_crate_mapping = tx_crate_mapping


    def apply_custom_type_one_transaction(self,chain,transaction,type_name,balanced,rules):
        transaction.type = Category(custom_type=type_name)
        transaction.classification_certainty_level = 10
        transaction.balanced = balanced
        self.custom_treatment_by_rules(chain, transaction, transaction.custom_type_id, type_name, rules)

    def apply_custom_type(self,chain_name, address,type_id,transaction_list):
        for txid in transaction_list:
            log('apply type',type_id, txid)
            # self.db.insert_kw('custom_types_applied', type_id=type_id, transaction_id=txid)
            self.db.update_kw('transactions', 'id='+str(txid), custom_type_id=type_id)
        self.db.commit()

        rules = self.db.select("SELECT * FROM custom_types_rules WHERE type_id = "+type_id+" ORDER BY id ASC")
        type_name, balanced = self.db.select("SELECT name, balanced FROM custom_types WHERE id = "+type_id)[0]

        address_db = SQLite('addresses')
        chain = Chain.from_name(chain_name, address_db, address)

        S = Signatures()
        transactions = self.load_transactions(chain, tx_id_list=transaction_list)
        contract_list, counterparty_list, input_list = chain.get_contracts(transactions)
        S.init_from_db(input_list)
        # C.init_from_db(chain.main_asset, contract_list, chain.addr, initial=False)
        C = Coingecko.init_from_cache(chain)
        # classifier = Classifier()
        res = []
        for idx,transaction in enumerate(transactions.values()):
            transaction.finalize(self, C, S)
            # classifier.classify(transaction)
            self.apply_custom_type_one_transaction(chain, transaction, type_name,balanced, rules)
            transaction.add_fee_transfer()
            transaction.infer_and_adjust_rates(self,C)
            self.apply_custom_val(transaction)
            js = transaction.to_json()
            res.append(js)

        address_db.disconnect()
        return res

    def unapply_custom_type(self, chain_name, address, type_id, transaction_list=None):
        if transaction_list is not None:
            for txid in transaction_list:
                log('unapply type', type_id, txid)
                self.db.update_kw('transactions', 'id=' + str(txid), custom_type_id=None)
                # self.db.query("DELETE FROM custom_types_applied WHERE type_id="+str(type_id)+" AND transaction_id="+str(txid))
        else:
            self.db.update_kw('transactions', 'custom_type_id=' + str(type_id), custom_type_id=None)
            # transaction_list = []
            # rows = self.db.select("SELECT transaction_id FROM custom_types_applied WHERE type_id="+str(type_id)+" ORDER BY transaction_id ASC")
            # for row in rows:
            #     transaction_list.append(str(row[0]))
            # self.db.query("DELETE FROM custom_types_applied WHERE type_id=" + str(type_id))
        self.db.commit()


        address_db = SQLite('addresses')
        chain = Chain.from_name(chain_name, address_db, address)

        S = Signatures()
        transactions = self.load_transactions(chain, tx_id_list=transaction_list)
        contract_list, counterparty_list, input_list = chain.get_contracts(transactions)
        S.init_from_db(input_list)
        # C.init_from_db(chain.main_asset,contract_list,chain.addr,initial=False)
        C = Coingecko.init_from_cache(chain)
        # classifier = Classifier()
        res = []
        classifier = Classifier(chain)
        for idx, transaction in enumerate(transactions.values()):
            transaction.finalize(self, C, S)
            classifier.classify(transaction)
            transaction.add_fee_transfer()
            transaction.infer_and_adjust_rates(self, C)
            self.apply_custom_val(transaction)
            js = transaction.to_json()
            res.append(js)


        address_db.disconnect()
        return res


    def custom_treatment_by_rules(self, chain, transaction, type_id, type_name, rules):
        if transaction.hash == chain.hif:
            print("Applying custom type rules to",transaction.hash)

        def check_address_match(transfer_addr,rule_addr,rule_addr_custom):
            rule_addr_custom = rule_addr_custom.lower()
            if rule_addr == 'my_address' and transfer_addr != self.address:
                return 0
            if rule_addr == '0x0000000000000000000000000000000000000000' and transfer_addr != '0x0000000000000000000000000000000000000000':
                return 0
            if rule_addr == 'specific' and transfer_addr != rule_addr_custom:
                return 0
            if rule_addr == 'specific_excl' and transfer_addr == rule_addr_custom:
                return 0
            return 1

        def check_token_match(transfer_contract, transfer_symbol, rule_token, rule_token_custom):
            # log('ctm',rule_token, transfer_symbol, rule_token_custom, transfer_contract, rule_token_custom)
            rule_token_custom = rule_token_custom.lower()
            transfer_symbol = transfer_symbol.lower()
            if rule_token == 'base' and transfer_symbol.lower() == chain.main_asset.lower():
                return 1
            if rule_token == 'specific' and (transfer_symbol == rule_token_custom or transfer_contract == rule_token_custom):
                return 1
            if rule_token == 'specific_excl' and (transfer_symbol != rule_token_custom and transfer_contract != rule_token_custom):
                return 1
            if rule_token == 'any':
                return 1
            if rule_token.lower() == transfer_symbol:
                return 1
            return 0


        for transfer_idx,transfer in enumerate(transaction.transfers):
            fr = transfer.fr.lower()
            to = transfer.to.lower()
            what = transfer.what.lower()
            symbol = transfer.symbol.lower()

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

                if not check_token_match(what, symbol, token, token_custom):
                    # log("Token fail")
                    continue

                rule_found = True
                # log("Match")
                selected_treatment = treatment
                if selected_treatment in ['deposit','withdraw','exit']:
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

                transfer.treatment = selected_treatment
                break

            if not rule_found:
                log('transaction',transaction.txid,'could not find matching rule for transfer_idx',transfer_idx)








    # def add_rate(self, transaction_id, transfer_idx, rate, source, level):
    #     self.db.insert_kw('rates', transaction_id=transaction_id, transfer_idx=transfer_idx, rate=rate, source=self.rate_sources.index(source), level=level)
    #
    # def wipe_rates(self):
    #     self.db.query('DELETE FROM rates')
    #     self.db.commit()

    def save_custom_val(self,chain_name,address,transaction_id, transfer_idx, treatment=None, rate=None, vaultid = None):
        where = 'transaction_id='+str(transaction_id)+' and idx='+str(transfer_idx)
        if treatment is not None:
            self.db.update_kw('transaction_transfers', where, custom_treatment=treatment)
        if rate is not None:
            self.db.update_kw('transaction_transfers', where, custom_rate=rate)
        if vaultid is not None:
            self.db.update_kw('transaction_transfers', where, custom_vaultid=vaultid)
        self.db.commit()


    def undo_custom_changes(self,chain_name,address,transaction_id):
        # self.db.query("DELETE FROM custom_treatment WHERE transaction_id="+str(transaction_id))
        # self.db.query("DELETE FROM custom_rates WHERE transaction_id=" + str(transaction_id))
        self.db.update_kw('transaction_transfers',"transaction_id=" + str(transaction_id),custom_treatment=None,custom_rate=None,custom_vaultid=None)
        self.db.commit()

        address_db = SQLite('addresses')
        chain = Chain.from_name(chain_name, address_db, address)

        S = Signatures()
        transactions = self.load_transactions(chain, tx_id_list=[transaction_id])
        transaction = transactions.values()[0]
        contract_list, counterparty_list, input_list = transaction.get_contracts()
        S.init_from_db(input_list)
        # C.init_from_db(chain.main_asset, contract_list, chain.addr, initial=False)
        C = Coingecko.init_from_cache(chain)
        classifier = Classifier(chain)

        transaction.finalize(self, C, S)
        # classifier.classify(transaction)


        if transaction.custom_type_id is not None:
            type_id = str(transaction.custom_type_id)
            rules = self.db.select("SELECT * FROM custom_types_rules WHERE type_id = " + type_id + " ORDER BY id ASC")
            type_name, balanced = self.db.select("SELECT name, balanced FROM custom_types WHERE id = " + type_id)[0]
            self.apply_custom_type_one_transaction(chain, transaction, type_name,balanced, rules)
        else:
            classifier.classify(transaction)
        transaction.add_fee_transfer()
        transaction.infer_and_adjust_rates(self, C)
        js = transaction.to_json()

        address_db.disconnect()
        return js


    def apply_custom_val(self, transaction):
        # txid = transaction.txid
        for transfer in transaction.transfers:
            if transfer.custom_rate is not None:
                transfer.rate = 'custom:'+str(transfer.custom_rate)
            if transfer.custom_treatment is not None:
                transfer.treatment = 'custom:'+str(transfer.custom_treatment)
            if transfer.custom_vaultid is not None:
                transfer.vault_id = 'custom:'+str(transfer.custom_vaultid)


    def recolor(self,chain_name, address,color_id,transaction_list):
        if color_id == 'undo':
            color_id = None
        for txid in transaction_list:
            self.db.update_kw('transactions', 'id='+str(txid), custom_color_id=color_id)
        self.db.commit()
