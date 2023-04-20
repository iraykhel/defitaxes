import time

from .util import normalize_address, sql_in

class Import:
    #errors
    NO_API_RESPONSE = 0
    NOTHING_RETURNED = 1
    BAD_API_RESPONSE = 2
    UNEXPECTED_DATA = 3
    UNKNOWN_ERROR = 4
    COVALENT_FAILURE = 5
    DEBANK_TOKEN_FAILURE = 6
    DEBANK_PROTOCOL_FAILURE = 7
    SIMPLEHASH_FAILURE = 8
    PRESENCE_CHECK_FAILURE = 9
    NO_CREATORS = 10
    TOO_MANY_TRANSACTIONS = 11
    COINGECKO_CACHE_FAIL = 12
    COVALENT_OVERLOAD = 13

    def __init__(self,user,all_chains=None, id=None):
        if id is None:
            t = int(time.time())
            db = user.db
            db.insert_kw('imports', started=t, status=0, version=user.version)
            db.commit()
            self.id = db.select("SELECT last_insert_rowid()")[0][0]
        else:
            self.id = id
        self.errors = []
        self.codes = set()
        if all_chains is not None:
            for chain_name, chain_data in all_chains.items():
                chain = chain_data['chain']
                chain.current_import = self
        self.overwrites_ok = True

    def populate_addresses(self,user,all_chains):
        db = user.db
        for chain_name, chain_data in all_chains.items():
            addresses = chain_data['import_addresses']
            chain = chain_data['chain']
            for active_address in addresses:
                active_address = normalize_address(active_address)
                if chain.check_validity(active_address):
                    db.insert_kw('imports_addresses', import_id=self.id, chain=chain.name, address=active_address)
        db.commit()

    def finish(self, user):
        t = int(time.time())
        db = user.db
        status = 0
        if len(self.errors) == 0:
            status = 1
        db.update_kw('imports', "id=" + str(self.id), ended=t, status=status)
        for error in self.errors:
            #self.db.create_table('imports_errors', 'id integer, chain TEXT, address TEXT, txtype INTEGER, error_code INTEGER', drop=drop)
            db.insert_kw('imports_errors',import_id=self.id,chain=error['chain'],address=error['address'],txtype=error['txtype'],error_code=error['error_code'],
                         additional_text = error['additional_text'],debug_info=error['debug_info'])
        db.commit()

    def add_error(self,error_code,chain=None,address=None,txtype=None,additional_text=None,debug_info=None,txhash=None):
        chain_name = None
        if chain is not None:
            chain_name = chain.name
        # if error_code not in [Import.COVALENT_FAILURE, Import.DEBANK_TOKEN_FAILURE, Import.DEBANK_PROTOCOL_FAILURE, Import.PRESENCE_CHECK_FAILURE]:
        #     self.overwrites_ok = False
        self.codes.add(error_code)
        self.errors.append({'chain':chain_name,'address':address,'error_code':error_code,'txtype':txtype,'additional_text':additional_text,'debug_info':debug_info, 'txhash':txhash})

    @classmethod
    def load_errors(cls,user):
        db = user.db
        rows = db.select("SELECT * FROM imports_errors WHERE import_id IN " + sql_in(user.relevant_import_ids), return_dictionaries=True)
        return Import.errors_to_text(rows)

    @classmethod
    def errors_to_text(cls,errors):
        action_error_mapping = {'txlist': 'base asset', 'tokentx': 'token', 'txlistinternal': 'internal', 'tokennfttx': 'NFT', 'token1155tx': 'ERC-1155'}

        text_strings = []
        for error in errors:
            code = error['error_code']
            action = error['txtype']
            chain = error['chain']
            err_tx_type = "transactions"
            if action is not None:
                err_tx_type = action_error_mapping[action] + " transactions"
            if code == Import.NO_API_RESPONSE:
                s = "no response from scanner when asking for " + err_tx_type
            elif code == Import.NOTHING_RETURNED:
                s = "no "+err_tx_type+" returned from the scanner"
            elif code == Import.BAD_API_RESPONSE:
                s = "could not get "+err_tx_type+" from the scanner"
            elif code == Import.UNEXPECTED_DATA:
                s = "unexpected data from scanner instead of "+err_tx_type
            elif code == Import.COVALENT_FAILURE:
                if chain == 'Arbitrum':
                    s = "failed to get data from CovalentHQ, fees might be off"
                elif chain == 'Fantom':
                    s = "failed to get data from CovalentHQ, failed transactions are counted as completed"
                elif chain == 'ETH':
                    s = "failed to get data from CovalentHQ, some counterparty info might be missing"
            elif code == Import.COVALENT_OVERLOAD:
                s = "too many transactions, stopped Covalent data at 50 requests, some transaction data might be wrong"
            elif code == Import.DEBANK_TOKEN_FAILURE:
                s = "failed to get token data from DeBank, balance check can't be performed"
            elif code == Import.DEBANK_PROTOCOL_FAILURE:
                s = "failed to get protocol data from DeBank, some counterparty info might be missing"
            elif code == Import.SIMPLEHASH_FAILURE:
                s = "failed to get NFT data from Simplehash, some NFT transfers might be missing or incorrect"
            elif code == Import.PRESENCE_CHECK_FAILURE:
                s = "failed to get data from scanner, it might be down or its API may have changed"
            elif code == Import.NO_CREATORS:
                s = "failed to get contract creators from scanner, some counterparty info might be missing"
            elif code == Import.TOO_MANY_TRANSACTIONS:
                if chain == 'Solana':
                    s = "too many transactions, we support up to 10000. We pay our data provider per transaction retrieved."
                else:
                    s = "too many transactions, we support up to 50000 per chain per address."
            elif code == Import.COINGECKO_CACHE_FAIL:
                s = "Failed to load coingecko cache, importing of transactions is necessary"
            else:
                s = "unknown error"
            prefix = "Problem"
            if error['address'] is not None:
                prefix += " with "+error['address']
            if error['chain'] is not None:
                prefix += " on "+error['chain']
            error_string = prefix +": "+s
            text_strings.append(error_string)
        return text_strings
