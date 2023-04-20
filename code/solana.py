import math

import requests
import time
import traceback
import copy
import uuid
import os
from collections import defaultdict

from .transaction import Transaction, Transfer
from .chain import Chain
from .util import log, normalize_address, log_error
from .imports import Import

import base64
import base58
import struct
from hashlib import sha256
from pure25519.basic import decodepoint, NotOnCurve
# from solana.publickey import PublicKey

class Solana(Chain):

    #order matters, weirdest last
    NATIVE_PROGRAMS =  {
            '11111111111111111111111111111111': 'System Program',
            'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA': 'Token Program',
            'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL': 'Token Account Program',
            'MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr': 'Memo Program',
            'ComputeBudget111111111111111111111111111111': 'Compute Budget',
            'Vote111111111111111111111111111111111111111': 'Vote Program',
            'Stake11111111111111111111111111111111111111': 'Stake Program',
            'BPFLoaderUpgradeab1e11111111111111111111111': 'BPF Loader',
            'Ed25519SigVerify111111111111111111111111111': 'Signature Verifier',
            'KeccakSecp256k11111111111111111111111111111': 'Secp256k1 Program',

            'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s': 'Metaplex Metadata'
        }

    def __init__(self):
        Chain.__init__(self,'Solana','solscan.io','SOL', None)
        self.explorer_url = 'https://public-api.solscan.io/'
        self.domain = 'explorer.solana.com'
        self.wait_time = 0.25
        self.solscan_session = requests.session()
        self.explorer_session = requests.session()
        self.hif = '2GAypSN6CZKChduqsGB4EvmVgwSzGvxjdHbKrigv7Vkj2mByQDkZN4xdJ4HjoAr7Sv73ivk2nU8k6coVS1DcmDFP'

        self.solana_nft_data = {}
        self.solana_proxy_map = {}

        self.mode = 'explorer'



    def get_ancestor(self, address):
        return None, [], None, None

    def check_presence(self,address):
        # headers = {'accept': 'application/json', 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36'}
        # url = "https://public-api.solscan.io/account/solTransfers?account=" + address + "&limit=1"
        # resp = requests.get(url, headers=headers, timeout=5)
        # data = resp.json()
        # data = self.explorer_multi_request({"method":"getConfirmedSignaturesForAddress2","jsonrpc":"2.0","params":[None,{"limit":1}]},[address], timeout=30)
        data = self.explorer_multi_request({"method": "getSignaturesForAddress", "jsonrpc": "2.0", "params": [None, {"limit": 1}]}, [address], timeout=30)
        if len(data) > 0:
            return True
        return False


        # try:
        #     headers = {'accept': 'application/json', 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36'}
        #     url = "https://public-api.solscan.io/account/solTransfers?account=" + address + "&limit=1"
        #     resp = requests.get(url, headers=headers, timeout=5)
        #     data = resp.json()
        #     self.mode = 'solscan'
        #     log('switch mode to solscan')
        #     if len(data) > 0:
        #         return True
        # except:
        #     pass


    def sumup_tx(self,T,address):
        totals = {}
        grp = T.grouping
        for entry_idx, entry in enumerate(grp):
            type = entry[0]
            row = entry[1]
            fr, to, val, symbol, what = row[4:9]
            if fr == address:
                val = -val
            # if what is None or len(what) < 10:
            #     what_str = str(what)
            # else:
            #     what_str = what[:2]+".."+what[-2:]
            if (what is None or what.lower() == 'sol') and symbol.lower() == 'sol':
                id_str = 'SOL'
            elif 'Unknown token' in symbol:
                id_str = symbol
            elif what is None:
                id_str = symbol
            else:
                id_str = what

            if what == 'So11111111111111111111111111111111111111112':
                # symbol = 'SOL' #treat WSOL as SOL
                # what = 'SOL'
                # type = 1
                symbol = 'WSOL'

            # id_str = str(symbol)
            if id_str not in totals:
                totals[id_str] = {'s':0,'c':0, 'what':what, 'symbol':symbol,'indexes':[],'type':type}
            totals[id_str]['s'] += val
            totals[id_str]['c'] += 1
            totals[id_str]['indexes'].append(entry_idx)
        return dict(totals)

    def totals_to_str(self,totals):
        dct_to_print = {}
        for i,dct in totals.items():
            # assert dct['symbol'] not in dct_to_print
            symbol = dct['symbol']
            if symbol in dct_to_print:
                log("REPEAT SYMBOL",totals)
                symbol = i
            dct_to_print[symbol] = str(dct['s'])+":"+str(dct['c'])
        return str(dct_to_print)

    def get_transactions(self,user, address,pb_alloc):
        log("Getting solana transactions")
        # transactions = self.get_transactions_joint(user,address,pb_alloc)
        transactions = self.get_transactions_from_explorer(user,address,pb_alloc)
        log("Got solana transactions")
        return transactions




    def get_nft_info_from_solscan(self, nft_address):
        headers = {'accept': 'application/json', 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36'}
        url = "https://public-api.solscan.io/account/" + nft_address
        time.sleep(0.25)
        log("getting nft info from solscan", nft_address, url)
        try:
            resp = self.solscan_session.get(url, timeout=10, headers=headers)
        except:
            log("Failed to get token info from solscan",url, traceback.format_exc())
            return None
        if resp.status_code != 200:
            log("Failed to get token info from solscan",url,resp.content)
            return None


        data = resp.json()
        try:
            info = data['tokenInfo']
            symbol = info['symbol']
            name = info['name']

            meta = data['metadata']

            try:
                name_2 = meta['data']['name']
                if len(name_2) > len(name):
                    name = name_2
            except:
                pass

            update_authority = meta['updateAuthority']
            minter = meta['mint']

            try:
                uri = meta['external_url']
            except:
                uri = None

            return {'name': name, 'symbol': symbol, 'uri': uri, 'update_authority': update_authority, 'minter': minter}
        except:
            log("Failed to get token info from solscan",url,data)
            return None





    def explorer_multi_request(self,json_template, query_list, batch_size=90,pb_alloc=None,pb_text = None, timeout=30):
        if len(query_list) == 0:
            log('error: query_list is empty for',json_template,filename='solana.txt')
            return {}
        rpc_url = 'https://floral-prettiest-wish.solana-mainnet.discover.quiknode.pro/'+os.environ.get('quicknode_solana_auth_token')+'/'

        query_list = list(query_list)
        log('rpc call', json_template, len(query_list), query_list[0], filename='solana.txt')
        # explorer_headers = {
        #     'content-type': 'application/json',
        #     'solana-client': 'js/0.0.0-development',
        #     'origin': 'https://explorer.solana.com',
        #     'referer': 'https://explorer.solana.com',
        #     'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36'
        # }
        # assert len(query_list) > 0
        if batch_size is None:
            batch_size = len(query_list)

        batch_cnt = len(query_list) // batch_size
        if len(query_list) % batch_size != 0:
            batch_cnt += 1

        offset = 0
        uid_mapping = {}
        output_mapping = {}
        method = json_template['method']
        if pb_text is not None and pb_alloc is None:
            pb_alloc = 0

        for batch_idx in range(batch_cnt):
            if pb_alloc is not None:
                pb_entry = None
                if pb_text is not None:
                    pb_entry = pb_text +": "+str(batch_idx+1)+"/"+str(batch_cnt)
                self.update_pb(entry=pb_entry, percent=float(pb_alloc) / batch_cnt)
            multi_explorer_request = []
            batch = query_list[offset:offset+batch_size]

            for query_datum in batch:
                uid = str(uuid.uuid4())
                explorer_dump = copy.deepcopy(json_template)
                explorer_dump['params'][0] = query_datum
                explorer_dump['id'] = uid
                multi_explorer_request.append(explorer_dump)
                uid_mapping[uid] = query_datum



            t = time.time()
            log("Sending multi dump batch",batch_idx,"out of", batch_cnt, method, 'length',len(multi_explorer_request),filename='solana.txt')
            time.sleep(1)
            try:
                log('post dump',rpc_url,multi_explorer_request,filename='solana.txt')
                resp = self.explorer_session.post(rpc_url, timeout=timeout, json=multi_explorer_request)
                # resp = self.explorer_session.post('https://api.mainnet-beta.solana.com', timeout=timeout, json=multi_explorer_request, headers=explorer_headers)
            except:
                log("Request failed, timeout", traceback.format_exc(),filename='solana.txt')
                return None
            log("Timing", method, time.time() - t,filename='solana.txt')

            # headers = resp.headers
            # l0 = headers['x-ratelimit-conn-remaining']
            # l1 = headers['x-ratelimit-method-remaining']
            # l2 = headers['x-ratelimit-rps-remaining']
            # log("Remaining limits after", method, l0, l1, l2,filename='solana.txt')

            if resp.status_code != 200:
                log("Request failed", resp.status_code, resp.content,filename='solana.txt')
                return None

            multi_data = resp.json()
            log('response meta', 'status', resp.status_code, 'len',len(multi_data), 'headers', resp.headers, filename='solana.txt')
            # log('response',multi_data,filename='solana.txt')

            for entry in multi_data:
                uid = entry['id']
                if 'result' not in entry:
                    log("BAD ENTRY",json_template, entry, uid, uid_mapping[uid],filename='solana.txt')
                output_mapping[uid_mapping[uid]] = entry['result']

            offset += batch_size
        log("dump length",len(output_mapping),filename='solana.txt')
        return output_mapping

    def get_all_instructions(self, explorer_tx_data):
        all_instructions = []

        outer_instructions = explorer_tx_data['transaction']['message']['instructions']

        for instruction in outer_instructions:
            instruction['source'] = 'message'
            all_instructions.append([instruction])

        if 'innerInstructions' in explorer_tx_data['meta']:
            innerInstructions = explorer_tx_data['meta']['innerInstructions']
            for entry in innerInstructions:
                idx = entry['index']
                # if 'parsed' in outer_instructions[idx]:
                #     log('get_all_instructions',idx,'already parsed',outer_instructions[idx]['parsed']['type'],outer_instructions[idx])
                instructions = entry['instructions']
                for instruction in instructions:
                    instruction['source']='innerInstructions'
                    instruction['index'] = idx
                all_instructions[idx].extend(instructions)

        flattened_all_instructions = []
        for subset in all_instructions:
            flattened_all_instructions.extend(subset)


        return flattened_all_instructions

    def get_nft_address_from_tx(self, entry):
        pre_bal = entry['meta']['preTokenBalances']
        post_bal = entry['meta']['postTokenBalances']
        bal_change = {}
        for bal in pre_bal:
            amt = bal['uiTokenAmount']
            if amt['decimals'] == 0:
                bal_change[bal['mint']] = amt['uiAmount']
        for bal in post_bal:
            amt = bal['uiTokenAmount']
            if amt['decimals'] == 0:
                bal_change[bal['mint']] -= amt['uiAmount']

        cands = []
        for mint, change in bal_change.items():
            if change == 0:
                cands.append(mint)
        if len(cands) == 1:
            return cands[0]

        # balances = pre_bal + post_bal
        # mints = set()
        # for bal in balances:
        #     # if bal['uiTokenAmount']['decimals'] == 0:
        #     if 'mint' in bal:
        #         mint_cand = bal['mint']
        #         mints.add(mint_cand)
        #         log('found proxy on explorer', mint_cand)
        # if len(mints) == 1:
        #     mints = list(mints)
        #     return mints[0]
        # else:
        #     all_instructions = self.get_all_instructions(entry)
        #     for instruction in all_instructions:
        #         try:
        #             info = instruction['parsed']['info']
        #             proxy = info['account']
        #             mint = info['mint']
        #             log('found proxy on explorer (2)', mint)
        #             return mint
        #         except:
        #             pass
        # return None

    def find_matching_sum(self,total,num_list,fee,index = 0, running_sum = 0, accum_list = None, subsets = None):
        # print("Calling fms",total,num_list,'index',index,'running_sum',running_sum,'accumulator',accum_list)


        am_spawn = False
        if subsets is None:
            subsets = []
            accum_list = []
            am_spawn = True
            if total in num_list:
                return [[num_list.index(total)]]

        if running_sum in [total-fee,total, total+fee]:
            # print("MATCH",accum_list)
            return accum_list
        if running_sum > 0 and running_sum > total+fee:
            if am_spawn:
                return subsets
            # print("Over total")
            return None

        for idx,num in enumerate(num_list[index:]):
            if accum_list == [] or index+idx > accum_list[-1]:
                rv = self.find_matching_sum(total,num_list,fee,index+1,running_sum+num, accum_list + [index+idx], subsets=subsets)
                if rv is not None and rv != [] and rv not in subsets:
                    subsets.append(rv)
        if am_spawn:
            # print('subsets', subsets)
            if len(subsets) > 1:
                cull = set()
                for idx in range(0, len(subsets) - 1):
                    for idx2 in range(idx, len(subsets)):
                        subset = subsets[idx]
                        subset2 = subsets[idx2]
                        if set(subset) <= set(subset2):
                            cull.add(idx + idx2)
                        elif set(subset2) <= set(subset):
                            cull.add(idx)
                # print('cull', cull)
                new_subsets = []
                for idx, subset in enumerate(subsets):
                    if idx not in cull:
                        new_subsets.append(subset)
                subsets = new_subsets
            return subsets


    def get_transactions_from_explorer(self,user,address, pb_alloc):
        def get_authority(info):
            if 'authority' in info:
                return info['authority']
            if 'multisigAuthority' in info:
                return info['multisigAuthority']
            raise None

        def wsol_operation(proxy,op,idx):
            if proxy in proxy_to_token_mapping:
                token = proxy_to_token_mapping[proxy]['token']
                if token == WSOL:
                    if op == 'transfer':
                        if op not in wsol_indexes[proxy]:
                            wsol_indexes[proxy][op] = []
                        wsol_indexes[proxy][op].append(idx)
                    else:
                        wsol_indexes[proxy][op] = idx

        def proxy_is_owned(proxy, ts):
            if proxy not in proxy_to_token_mapping:
                return False
            periods = proxy_to_token_mapping[proxy]['periods']
            for period in periods:
                start, end = period
                if ts >= start and (end is None or ts <= end):
                    return True
                elif start > ts:
                    break
            return False


        done = False
        limit = 500
        tx_list = []
        self.update_pb('Getting signatures for ' + address)
        # json_template = {"method": "getConfirmedSignaturesForAddress2", "jsonrpc": "2.0", "params": [None, {"limit": limit}]}
        json_template = {"method": "getSignaturesForAddress", "jsonrpc": "2.0", "params": [None, {"limit": limit}]}
        while not done:
            tx_multi_list = self.explorer_multi_request(json_template, [address], pb_text='Getting signatures for ' + address, timeout=120)


            output = tx_multi_list[address]
            log('Retrieved signatures',output,filename='solana.txt')
            for entry in output:
                tx_list.append(entry['signature'])
            if len(output) == limit:
                json_template['params'][1]['before'] = tx_list[-1]
                time.sleep(1)
            else:
                done = True
            if len(tx_list) >= 10000:
                self.current_import.add_error(Import.TOO_MANY_TRANSACTIONS, chain=self, address=address)
                done = True

        self.update_pb(None, pb_alloc*0.1)

        tx_list = tx_list[::-1]
        log('tx_list',len(tx_list),tx_list)

        # self.update_pb('Getting transactions for ' + address)
        all_tx_data = self.explorer_multi_request(
            {"method": "getTransaction", "jsonrpc": "2.0", "params": [None, {"encoding": "jsonParsed", "commitment": "confirmed", "maxSupportedTransactionVersion": 2}]},
            tx_list,  timeout=60, pb_text='Getting transactions for ' + address, pb_alloc=pb_alloc*0.7)


        proxy_to_token_mapping = {}
        SOL = '11111111111111111111111111111111'
        SPL = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
        WSOL = 'So11111111111111111111111111111111111111112'
        #collect all proxy accounts and valid proxy ownership periods OHMYGOD SOLANA WHY
        account_deposits = {}
        for tx_hash, tx_data in all_tx_data.items():
            if tx_data is None:
                log("NO INFO FOR TX",tx_hash,filename='solana.txt')
                continue
            ts = tx_data['blockTime']
            log("\n\nextracting accounts from tx",tx_hash, tx_data,filename='solana.txt')
            all_instructions = self.get_all_instructions(tx_data)
            err = tx_data['meta']['err']
            if err is not None:
                log("Transaction fail",tx_hash,err,filename='solana.txt')
                continue

            for instruction in all_instructions:
                log('instruction',instruction,filename='solana.txt')
                if 'parsed' not in instruction:
                    continue
                try:
                    parsed = instruction['parsed']
                    programId = instruction['programId']
                    if 'type' in parsed:
                        type = parsed['type']
                        info = parsed['info']
                        if len(type) >= 17 and type[:17] == 'initializeAccount': #can be initializeAccount3
                            if programId == SPL:
                                owner = info['owner']
                                if owner == address:
                                    mint = info['mint']
                                    proxy = info['account']
                                    if proxy not in proxy_to_token_mapping:
                                        log("Create proxy account", proxy, ":", mint,ts,filename='solana.txt')
                                        proxy_to_token_mapping[proxy] = {'token':mint,'periods':[[ts,None]]}
                                    else:
                                        log("Recreate proxy account", proxy, ":", mint, ts, filename='solana.txt')
                                        proxy_to_token_mapping[proxy]['periods'].append([ts,None])
                                    # account_deposits[proxy] = 0


                except:
                    log('error - failed to parse',traceback.format_exc(),filename='solana.txt')
                    continue

            # for instruction in all_instructions:
            #     try:
            #         parsed = instruction['parsed']
            #         programId = instruction['programId']
            #         type = parsed['type']
            #         info = parsed['info']
            #         if type in ['createAccount','createAccountWithSeed'] and programId == SOL:
            #
            #             source = info['source']
            #             owner = info['owner']
            #             if source == address:# and owner == address:
            #                 destination = info['newAccount']
            #                 lamports = info['lamports']
            #                 # if destination in account_deposits:
            #                 # if instruction['source'] == 'message' or instruction['index'] == 0 or owner == SPL: #I have no idea why
            #                 # log("Deposit on account creation", destination, ":", lamports)
            #                 log("Account opened", destination)
            #                 account_deposits[destination] = 0
            #                 # account_deposits[destination] = lamports
            #     except:
            #         continue

            for instruction in all_instructions:
                if 'parsed' not in instruction:
                    continue
                try:
                    parsed = instruction['parsed']
                    programId = instruction['programId']

                    if 'type' in parsed:
                        type = parsed['type']
                        info = parsed['info']
                        if type == 'setAuthority' and programId == SPL:
                            if info['authorityType'] == 'accountOwner':
                                proxy = info['account']
                                old = get_authority(info)
                                new = info['newAuthority']
                                if address == old:
                                    proxy_to_token_mapping[proxy]['periods'][-1][1] = ts
                                    log("Reassign proxy away", proxy,proxy_to_token_mapping[proxy])

                                if address == new:
                                    mint = self.get_nft_address_from_tx(tx_data)
                                    if proxy not in proxy_to_token_mapping:
                                        proxy_to_token_mapping[proxy] = {'token':mint,'periods':[[ts,None]]}
                                    else:
                                        proxy_to_token_mapping[proxy]['periods'].append([ts,None])
                                    log("Reassign proxy here", proxy, mint)

                except:
                    log('error - failed to parse',traceback.format_exc(),filename='solana.txt')
                    continue
        log("proxy mapping",len(proxy_to_token_mapping), proxy_to_token_mapping,filename='solana.txt')

        all_token_data = {}
        for proxy, data in proxy_to_token_mapping.items():
            token = data['token']
            if token not in all_token_data:
                all_token_data[token] = {'proxies':[], 'name':'Unknown token','symbol':'Unknown ('+token[:6]+'...)','mint_authority':token,'uri':None,'update_authority':token, 'decimals':6}
            all_token_data[token]['proxies'].append(proxy)

        log('token_to_proxies',len(all_token_data),filename='solana.txt')

        pulled_tokens = self.get_current_tokens_internal(address)
        log("pulled tokens", len(pulled_tokens), pulled_tokens, filename='solana.txt')

        missing_from_pulled = set(all_token_data.keys())-set(pulled_tokens.keys())
        missing_from_running = set(pulled_tokens.keys()) - set(all_token_data.keys())
        log("missing_from_pulled",len(missing_from_pulled),missing_from_pulled, filename='solana.txt')
        log("missing_from_running", len(missing_from_running), missing_from_running, filename='solana.txt')

        for token in list(missing_from_running):
            all_token_data[token] = {'proxies': [], 'name': 'Unknown token', 'symbol': 'Unknown (' + token[:6] + '...)', 'mint_authority': token, 'uri': None, 'update_authority': token, 'decimals': 6}

        account_info_list = self.explorer_multi_request({"method": "getAccountInfo", "jsonrpc": "2.0", "params": [None, {"encoding": "jsonParsed", "commitment": "confirmed"}]},
                                                        list(all_token_data.keys()),  pb_text='Getting info for tokens at ' + address,pb_alloc=pb_alloc*0.05)
        for token, entry in account_info_list.items():
            try:
                data = entry['value']['data']['parsed']['info']
            except:
                log("Failed to get info", token, entry)
                continue

            try:
                all_token_data[token]['decimals'] = data['decimals']
                all_token_data[token]['mint_authority'] = data['mintAuthority']
            except:
                log("required fields not found in info", token, data)


        token_metadata_accounts = {}
        for token in all_token_data.keys():
            metadata_account = self.get_metadata_account(token)
            token_metadata_accounts[metadata_account] = token
        log("token_metadata_accounts", token_metadata_accounts,filename='solana.txt')
        account_info_list = self.explorer_multi_request({"method": "getAccountInfo", "jsonrpc": "2.0", "params": [None, {"encoding": "jsonParsed", "commitment": "confirmed"}]},
                                                        list(token_metadata_accounts.keys()),  pb_text='Getting metadata for tokens at ' + address,pb_alloc=pb_alloc*0.05)
        metadata_fails = []
        for metadata_account, entry in account_info_list.items():
            token = token_metadata_accounts[metadata_account]
            try:
                datadump = entry['value']['data'][0]
            except:
                log("Failed to get metadata", token, metadata_account, entry)
                metadata_fails.append(token)
                continue

            log("meta dump", token, datadump)

            decoded_dump = self.unpack_metadata_account(datadump)
            log("meta dump decoded", token, metadata_account, decoded_dump)

            try:
                data = decoded_dump['data']
                all_token_data[token]['name'] = data['name']
                all_token_data[token]['update_authority'] = decoded_dump['update_authority'].decode("utf-8")
                all_token_data[token]['symbol'] = data['symbol']
                all_token_data[token]['uri'] = data['uri']
            except:
                log("required fields not found in decoding", token, decoded_dump)

        log('metadata_fails',len(metadata_fails),metadata_fails)

        batch_size = 50
        batch_cnt = len(metadata_fails) // batch_size + 1
        offset = 0
        for batch_idx in range(batch_cnt):
            self.update_pb('Getting metadata for more tokens at ' + address+': '+str(batch_idx+1)+'/'+str(batch_cnt))
            subset = metadata_fails[offset:offset+batch_size]
            if len(subset) > 0:
                time.sleep(1)
                url = 'https://hyper.solana.fm/v3/token?address='+','.join(subset)
                try:
                    resp = requests.get(url,timeout=10)
                    data = resp.json()
                    for token, token_data in data.items():
                        if token_data is not None:
                            try:
                                all_token_data[token]['symbol'] = token_data['symbol']
                                all_token_data[token]['name'] = token_data['name']
                                all_token_data[token]['decimals'] = token_data['decimals']
                                all_token_data[token]['update_authority'] = all_token_data[token]['mint_authority'] = token
                            except:
                                log("Couldn't parse token info from solana.fm for",token, token_data)
                except:
                    log("Couldn't get token info from solana.fm",traceback.format_exc())
                    break
            offset += batch_size



        proxies = list(proxy_to_token_mapping.keys())
        # proxy_tx_list = self.explorer_multi_request({"method": "getConfirmedSignaturesForAddress2", "jsonrpc": "2.0", "params": [None, {"limit": 1000}]},proxies)
        proxy_tx_list = self.explorer_multi_request({"method": "getSignaturesForAddress", "jsonrpc": "2.0", "params": [None, {"limit": 1000}]},proxies,
                                                    pb_text='Getting signatures for token-holding accounts belonging to ' + address,pb_alloc=pb_alloc*0.05)
        all_proxy_signatures = set()
        for proxy, proxy_transactions in proxy_tx_list.items():
            # periods = proxy_to_token_mapping[proxy]['periods']
            for entry in proxy_transactions:
                signature = entry['signature']
                if signature not in all_tx_data: #retrieve tx if it hasn't already been retrieved and if it's inside a valid ownership period
                    ts = entry['blockTime']
                    if proxy_is_owned(proxy,ts):
                        all_proxy_signatures.add(signature)
                    # for period in periods:
                    #     start,end = period
                    #     if ts >= start and (end is None or ts <= end):
                    #         all_proxy_signatures.add(signature)
                    #         break
                    #     elif start > ts:
                    #         break

        log("Additional transactions to retrieve",len(all_proxy_signatures),all_proxy_signatures,filename='solana.txt')
        additional_tx_data = self.explorer_multi_request(
            {"method": "getTransaction", "jsonrpc": "2.0", "params": [None, {"encoding": "jsonParsed", "commitment": "confirmed", "maxSupportedTransactionVersion": 0}]},
            all_proxy_signatures,  timeout=60, pb_text='Getting transactions for token-holding accounts belonging to ' + address,pb_alloc=pb_alloc*0.05)

        all_tx_data.update(additional_tx_data)

        self.update_pb('Processing transactions for ' + address)
        tx_list = []
        for tx_hash, tx_data in all_tx_data.items():
            tx_list.append([tx_hash, tx_data['blockTime'], tx_data])
        tx_list = sorted(tx_list, key=lambda tup: tup[1])

        prev_ts = None
        nonce = 0
        all_transactions = {}
        type_counter = defaultdict(int)

        tx_sol_mismatches = []
        for tx_hash, ts, tx_data in tx_list:
            transfers = []
            if ts != prev_ts:
                nonce = 1
            else:
                nonce+=1

            log("\n\nprocessing tx", tx_hash, tx_data,filename='solana.txt')
            err = tx_data['meta']['err']
            if err is not None:
                log("Transaction fail", tx_hash, err,filename='solana.txt')
                continue

            all_instructions = self.get_all_instructions(tx_data)

            pre_balances = tx_data['meta']['preBalances']
            post_balances = tx_data['meta']['postBalances']
            fee = tx_data['meta']['fee']

            sol_transfers_processed_via_balances = False
            sol_changes = {}
            my_sol_change = 0
            accounts_data = tx_data['transaction']['message']['accountKeys']
            add_fee = False
            if accounts_data[0]['pubkey'] == address:
                add_fee = True

            for entry_idx,entry in enumerate(accounts_data):
                account = entry['pubkey']
                # sol_change = post_balances[entry_idx]-pre_balances[entry_idx]
                # if entry_idx == 0:
                #     sol_change -= fee
                # if account == address:
                #     my_sol_change = sol_change
                # elif sol_change != 0:
                sol_changes[account] = post_balances[entry_idx]-pre_balances[entry_idx]

            total_rewards_fee = 0
            rewards_data = tx_data['meta']['rewards']
            for reward in rewards_data:
                if reward['pubkey'] == address:
                    total_rewards_fee += reward['lamports']
                else:
                    total_rewards_fee -= reward['lamports']

            if address in sol_changes:
                sol_changes[address] += total_rewards_fee




            wsol_indexes = defaultdict(dict)
            # for instruction in all_instructions:
            #     log("instruction pass 2", instruction)
            #     if 'parsed' not in instruction:
            #         continue
            #
            #     try:
            #         parsed = instruction['parsed']
            #         programId = instruction['programId']
            #         type = parsed['type']
            #         info = parsed['info']
            #         if type in ['createAccount','createAccountWithSeed']:
            #             source = info['source']
            #             owner = info['owner']
            #             if source == address:# and owner == address:
            #                 destination = info['newAccount']
            #                 lamports = info['lamports']
            #                 sol_amount = lamports / 1000000000.
            #
            #                 account_deposits[destination] = lamports
            #                 log("SOL createaccount", source, "->", destination, ":", sol_amount)
            #                 wsol_operation(destination,'create',len(transfers))
            #                 transfers.append({'what': 'SOL', 'from': source, 'to': destination, 'amount': sol_amount})
            #
            #     except:
            #         continue

            programs = set()
            operations = defaultdict(int)
            for instruction in all_instructions:
                log("instruction pass 3",instruction,filename='solana.txt')
                if 'programId' in instruction and instruction['source'] == 'message':#only outer ones
                    programId = instruction['programId']
                    programs.add(programId)
                    # if programId not in [SOL, SPL, 'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL','MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr','ComputeBudget111111111111111111111111111111']:
                    #     programs.add(programId)

                if 'parsed' not in instruction:
                    # operations.append('?')
                    continue

                try:
                    parsed = instruction['parsed']
                    programId = instruction['programId']
                    if 'type' not in parsed:
                        continue
                    type = parsed['type']
                    operations[type] += 1
                    info = parsed['info']
                    type_counter[type] += 1


                    if type in ['transfer','transferChecked']:
                        source = info['source']
                        destination = info['destination']
                        if programId == SOL:
                            if address in [source, destination]:
                                lamports = info['lamports']
                                sol_amount = lamports / 1000000000.
                                log("SOL transfer",source,"->",destination,":",sol_amount)
                                if destination in account_deposits:
                                    log("Depositing SOL to owned account", destination)
                                    account_deposits[destination] += lamports
                                    wsol_operation(destination, 'deposit', len(transfers))

                                if source in account_deposits:
                                    log("WARNING Withdrawing SOL from owned account", source,filename='solana.txt')
                                    account_deposits[source] -= lamports
                                    wsol_operation(source, 'withdraw', len(transfers))

                                transfers.append({'what':'SOL','from':source,'to':destination,'amount':sol_amount})
                                # sol_changes[source] += lamports
                                # sol_changes[destination] -= lamports
                        if programId == SPL:
                            token = None
                            authority = get_authority(info)


                            if proxy_is_owned(source,ts):
                                proxy = source
                                token = proxy_to_token_mapping[source]['token']
                                source = address
                            elif authority is not None and authority != address:
                                source = authority

                            if proxy_is_owned(destination, ts):
                                proxy = destination
                                token = proxy_to_token_mapping[destination]['token']
                                destination = address
                            elif authority is not None and authority != source:
                                destination = authority

                            if token is not None:
                                if address in [source, destination]:
                                    if 'amount' in info:
                                        decimals = all_token_data[token]['decimals']
                                        amount = float(info['amount'])/float(math.pow(10,decimals))
                                    elif 'tokenAmount' in info:
                                        amount = info['tokenAmount']['uiAmount']
                                    if amount > 0:
                                        log("Token transfer", source, "->", destination, ":", amount, 'proxy',proxy,'token',token,filename='solana.txt')
                                        transfers.append({'what': token, 'from': source, 'to': destination, 'amount': amount})

                                        if token == WSOL:
                                            wsol_operation(proxy, 'transfer', len(transfers)-1)
                                            if source == destination:
                                                log("WARNING WTF source=destination for WSOL",filename='solana.txt')
                                                continue

                                            if source == address:
                                                account_deposits[proxy] -= int(info['amount'])
                                                # assert account_deposits[proxy] >= 0
                                                if account_deposits[proxy] < 0:
                                                    log("WARNING NEGATIVE DEPOSIT",proxy, account_deposits[proxy],filename='solana.txt')

                                            if destination == address:
                                                account_deposits[proxy] += int(info['amount'])

                    if type == 'create':
                        if info['source'] == address:
                            account = info['account']
                            if account not in account_deposits:
                                account_deposits[account] = 0
                                log("create", account,filename='solana.txt')

                    if type in ['createAccount','createAccountWithSeed']:
                        source = info['source']
                        # owner = info['owner']
                        if source == address:# and owner == address:
                            destination = info['newAccount']
                            lamports = info['lamports']
                            sol_amount = lamports / 1000000000.
                            account_deposits[destination] = lamports
                            log("SOL createaccount", source, "->", destination, ":", sol_amount,filename='solana.txt')
                            wsol_operation(destination, 'create', len(transfers))
                            transfers.append({'what': 'SOL', 'from': source, 'to': destination, 'amount': sol_amount})

                    if type == 'closeAccount':
                        destination = info['destination']
                        # owner = info['owner']
                        if destination == address:# and owner == address:
                            account = info['account']
                            if account in account_deposits:
                                lamports = account_deposits[account]
                                sol_amount = lamports / 1000000000.
                                if lamports <= 0:
                                    log("WARNING CLOSE ACCOUNT, OVERDRAFT", account, lamports,filename='solana.txt')
                                log("SOL closeaccount", account, "->", destination, ":", sol_amount,filename='solana.txt')
                                wsol_operation(account,'close',len(transfers))
                                transfers.append({'what': 'SOL', 'from': account, 'to': destination, 'amount': sol_amount})
                                # sol_changes[account] += lamports
                                # sol_changes[destination] -= lamports
                                # del account_deposits[account]
                                account_deposits[account] -= lamports



                    # if type == 'transferChecked':
                    #     source = info['source']
                    #     destination = info['destination']
                    #     token = info['mint']
                    #     authority = get_authority(info)
                    #     if destination in proxy_to_token_mapping:
                    #         destination = address
                    #         if authority is not None and authority != address:
                    #             source = authority
                    #     if source in proxy_to_token_mapping:
                    #         source = address
                    #         if authority is not None and authority != address:
                    #             destination = authority
                    #     if address in [source,destination]:
                    #         token_amount = info['tokenAmount']['uiAmount']
                    #         log("Token transfer",token, source, "->", destination, ":", token_amount)
                    #         transfers.append({'what': token, 'from': source, 'to': destination, 'amount': token_amount})

                    if type == 'setAuthority' and programId == SPL:
                        if info['authorityType'] == 'accountOwner':
                            proxy = info['account']
                            old = get_authority(info)
                            new = info['newAuthority']
                            if address == old:
                                source = address
                                destination = new

                            if address == new:
                                destination = address
                                source = old

                            if address in [source, destination]:
                                if proxy_is_owned(proxy, ts):
                                    token = proxy_to_token_mapping[proxy]['token']
                                    log("Authority reassignment", proxy, ":", token, source, "->", destination,filename='solana.txt')
                                    transfers.append({'what': token, 'from': source, 'to': destination, 'amount': 1})
                                # periods = proxy_to_token_mapping[proxy]['periods']
                                # for period in periods:
                                #     start, end = period
                                #     if ts >= start and (end is None or ts <= end):
                                #         log("Authority reassignment", proxy,":", token, source, "->", destination)
                                #         transfers.append({'what': token, 'from': source, 'to': destination, 'amount': 1})
                                #         break
                                #     elif start > ts:
                                #         break

                    if type in ['mintTo','mintToChecked','burn'] and programId == SPL:
                        token = info['mint']
                        proxy = info['account']

                        # if proxy in proxy_to_token_mapping:
                        if proxy_is_owned(proxy,ts):
                            decimals = all_token_data[token]['decimals']
                            if type == 'mintToChecked':
                                amount = float(info['tokenAmount']['uiAmount'])
                            else:
                                amount = float(info['amount']) / float(math.pow(10, decimals))
                            if 'mintTo' in type:
                                log("Mint", token,amount,filename='solana.txt')
                                transfers.append({'what': token, 'from': 'mint', 'to': address, 'amount': amount})
                            else:
                                log("Burn", token, amount,filename='solana.txt')
                                transfers.append({'what': token, 'from': address, 'to': 'burn', 'amount': amount})



                except:
                    log("WARNING Failure to parse", traceback.format_exc(),filename='solana.txt')
                    continue




            for t in transfers: #accounting balance changes, after this sol_changes should be 0
                if t['what'] == 'SOL':
                    lamports = int(round(t['amount'] * 1000000000))
                    sol_changes[t['from']] += lamports
                    sol_changes[t['to']] -= lamports

            total_unaccounted = 0
            unaccounted_changes = {}
            my_unaccounted_change = 0
            for account, amount in sol_changes.items():
                total_unaccounted += amount
                if amount != 0:
                    if account == address:
                        my_unaccounted_change = amount
                    else:
                        unaccounted_changes[account] = amount

            if abs(my_unaccounted_change) > fee:
                log("Unaccounted",total_unaccounted, 'my_unaccounted_change', my_unaccounted_change, 'unaccounted_changes',unaccounted_changes,filename='solana.txt')
                tx_sol_mismatches.append(tx_hash)

                pair_list = sorted(list(unaccounted_changes.items()), key=lambda tup: tup[1])
                num_list = []
                for pair in pair_list:
                    num_list.append(pair[1])
                adjusted_sol_change = -my_unaccounted_change
                log("calling find_matching_sum", adjusted_sol_change, num_list, fee,'rewards',total_rewards_fee,filename='solana.txt')
                matching_subsets = self.find_matching_sum(adjusted_sol_change, num_list, fee)
                if len(matching_subsets) != 1:
                    log("matching_subsets", my_unaccounted_change, 'adjusted_sol_change', adjusted_sol_change, 'fee', fee, 'subsets', len(matching_subsets), matching_subsets,filename='solana.txt')
                else:
                    for idx in matching_subsets[0]:
                        account, lamports = pair_list[idx]
                        sol_amount = lamports / 1000000000.
                        if sol_amount > 0:
                            source = address
                            destination = account
                        else:
                            source = account
                            destination = address
                            sol_amount = -sol_amount
                        log("SOL transfer via balances", source, "->", destination, ":", sol_amount,filename='solana.txt')
                        transfers.append({'what': 'SOL', 'from': source, 'to': destination, 'amount': sol_amount})


            if len(wsol_indexes) > 0:
                log('wsol_indexes',dict(wsol_indexes),filename='solana.txt')
                log('all transfers',transfers,filename='solana.txt')
                to_delete = []
                new_transfers = []
                for proxy, transfer_index_dict in wsol_indexes.items():

                    if 'transfer' in transfer_index_dict:
                        for idx in transfer_index_dict['transfer']:
                            log("Changing transfer item",idx, transfers[idx]['what'],"->","SOL",filename='solana.txt')
                            transfers[idx]['what'] = 'SOL'
                    for type in ['create','deposit','close']:
                        if type in transfer_index_dict:
                            to_delete.append(transfer_index_dict[type])
                            log("Deleting transfer",type, transfer_index_dict[type],filename='solana.txt')

                    # if 'create' in transfer_index_dict and 'transfer' in transfer_index_dict and 'close' in transfer_index_dict:
                    #     for idx in transfer_index_dict['transfer']:
                    #         log("Changing transfer item", transfers[idx]['what'],"->","SOL")
                    #         transfers[idx]['what'] = 'SOL'
                    #
                    #     to_delete = [transfer_index_dict['create']] + [transfer_index_dict['close']]
                    #     if 'deposit' in transfer_index_dict:
                    #         to_delete.append(transfer_index_dict['deposit'])
                    #     log("Deleting transfers", to_delete)
                    #
                    # elif 'transfer' in transfer_index_dict and len(transfer_index_dict['transfer']) == 1:
                    #     wsol_transfer_idx = transfer_index_dict['transfer'][0]
                    #     wsol_t = transfers[wsol_transfer_idx]
                    #     log("Changing transfer item (2)", wsol_t['what'], "->", "SOL")
                    #     wsol_t['what'] = 'SOL'
                    #     if 'create' in transfer_index_dict:
                    #         parallel_idx = transfer_index_dict['create']
                    #     elif 'close' in transfer_index_dict:
                    #         parallel_idx = transfer_index_dict['close']
                    #     else:
                    #         continue
                    #
                    #     sol_t = transfers[parallel_idx]
                    #     if wsol_t['to'] == sol_t['to'] or wsol_t['from'] == sol_t['from']:
                    #         if wsol_t['amount'] == sol_t['amount']:
                    #             log("Deleting transfer",parallel_idx)
                    #             to_delete.append(parallel_idx)
                    #         else:
                    #             log("Changing transfer amount", sol_t['amount'], "->", sol_t['amount']-wsol_t['amount'])
                    #             sol_t['amount'] -= wsol_t['amount']


                for idx, t in enumerate(transfers):
                    if idx not in to_delete:
                        new_transfers.append(t)
                    else:
                        log("Ignoring transfer index", idx,filename='solana.txt')
                transfers = new_transfers

            if add_fee:
                transfers.append({'what': 'SOL', 'from': address, 'to': 'network', 'amount': fee / 1000000000.})

            if len(programs) > 1:
                log("warning, multiple programs", list(programs),filename='solana.txt')

            #
            self.all_token_data = all_token_data


            #remove same transfers going opposite ways
            if len(transfers) > 1:
                amt_mapping = {}
                to_del = set()
                for t_idx,t in enumerate(transfers):
                    what = t['what']
                    amt = t['amount']
                    fr = t['from']
                    to = t['to']
                    if fr == to:
                        continue
                    if fr == address:
                        amt = -amt
                    if what not in amt_mapping:
                        amt_mapping[what] = {}

                    if -amt in amt_mapping[what]:
                        to_del.add(t_idx)
                        to_del.add(amt_mapping[what][-amt])
                        del amt_mapping[what][-amt]
                    else:
                        amt_mapping[what][amt] = t_idx
                log('amt_mapping',amt_mapping,filename='solana.txt')
                if len(to_del) > 0:
                    new_transfers = []
                    for t_idx, t in enumerate(transfers):
                        if t_idx not in to_del:
                            new_transfers.append(t)
                    transfers = new_transfers



            if len(transfers) > 0:
                T = Transaction(user, self)
                for t in transfers:
                    token = t['what']
                    nft_id = None
                    input_len = 0
                    input = None
                    type = Transfer.ERC20
                    fr = t['from']
                    to = t['to']
                    if fr == to:
                        continue

                    if token == 'SOL':
                        symbol = 'SOL'
                        type = Transfer.BASE
                    else:
                        token_data = all_token_data[token]
                        symbol = token_data['symbol']
                        if token == WSOL: #replace WSOL transfers with SOL
                            symbol = 'WSOL'
                            # type = 1
                        elif all_token_data[token]['decimals'] == 0:
                            type = Transfer.ERC721
                            nft_id = all_token_data[token]['name']
                            input_len = 200
                            input = token
                            if 'update_authority' in all_token_data[token]:
                                ua = all_token_data[token]['update_authority']
                                if ua != address:
                                    nft_id += " "+token
                                    token = ua

                    # if input is None and len(programs) > 0:
                    #     input_len = 100
                    #     input = str(list(programs))+":"+str(operations)

                    program = None
                    if len(programs) > 1: #only allow one program, the weirdest one
                        current_best = [None,-1]
                        # program_priority = [SOL, SPL, 'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL','MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr','ComputeBudget111111111111111111111111111111']
                        program_priority = list(Solana.NATIVE_PROGRAMS.keys())
                        for prog_cand in list(programs):
                            try:
                                idx = program_priority.index(prog_cand)
                                if idx > current_best[1]:
                                    current_best = [prog_cand,idx]
                            except:
                                program = prog_cand
                                break
                        else:
                            program = current_best[0]
                    elif len(programs) == 1:
                        program = list(programs)[0]

                    if program is not None:
                        T.interacted = program
                        op_str_lst = []
                        for op,cnt in operations.items():
                            op_str = op
                            if cnt > 1:
                                op_str += "(x"+str(cnt)+")"
                            op_str_lst.append(op_str)
                        T.function = ', '.join(op_str_lst)
                        # str(list(T.solana_external_programs)) + ":" + str(sorted(list(T.solana_operations)))

                    row = [tx_hash, ts, nonce, ts, fr, to, t['amount'], symbol, token, None, nft_id, 0, input_len, input]
                    T.append(type, row)
                all_transactions[tx_hash] = T
                # all_transactions.append(T)

        log("final all_token_data", all_token_data)
        log("type_counter",type_counter)
        log("tx_sol_mismatches",len(tx_sol_mismatches),tx_sol_mismatches,filename='solana.txt')
        return all_transactions






    # def get_transactions_joint(self,user,address,pb_alloc):
    #     bq = Bitquery()
    #     transactions_dict_bq = {}
    #     WSOL = 'So11111111111111111111111111111111111111112'
    #
    #
    #
    #     token_accounts = defaultdict(lambda: defaultdict(set))
    #     # self.update_pb('Retrieving Solana transaction list from bitquery', 0)
    #     # tx_list = bq.query_solana_txlist(address)
    #     # log("txlist", len(tx_list), tx_list)
    #
    #
    #     # batch_size = 100
    #     # batch_cnt = len(tx_list) // batch_size + 1
    #     # offset = 0
    #     # all_data = []
    #     # for i in range(batch_cnt):
    #     #     self.update_pb('Retrieving Solana transfer data from bitquery, '+str(i+1)+"/"+str(batch_cnt), 0)
    #     #     tx_list_batch = tx_list[offset:offset + batch_size]
    #     #     query, variables = bq.query_solana_transfers(tx_list_batch)
    #     #     log("transfer query", query)
    #     #     data = bq.request(query, variables=variables)
    #     #     all_data.extend(data)
    #     #     log("transfer query output", data)
    #     #     offset += batch_size
    #     pulled_tokens = self.get_current_tokens(address)
    #
    #     all_data = []
    #     for addressType in ['receiver', 'sender']:
    #         self.update_pb('Retrieving Solana transfers from bitquery for ' + address +' as '+addressType, pb_alloc*0.2)
    #         data = bq.query_with_limit(bq.query_solana_transfers, *[address, addressType])
    #         log('data length',len(data))
    #         all_data.extend(data)
    #
    #     for entry in data:
    #         receiver = entry['receiver']
    #         sender = entry['sender']
    #         token = entry['currency']['address']
    #         uid = entry['transaction']['signature']
    #         if len(token) > 10:
    #             # if token not in token_accounts:
    #             if 1:
    #                 if receiver['address'] == address and receiver['mintAccount'] != address:
    #                     token_accounts[token][receiver['mintAccount']].add(uid)
    #                     # log("token account (1)", token, entry)
    #                 if sender['address'] == address and sender['mintAccount'] != address:
    #                     token_accounts[token][sender['mintAccount']].add(uid)
    #                     # log("token account (2)", token, entry)
    #
    #
    #     log("WSOL accounts", len(token_accounts[WSOL])) #some transfers arrive to WSOL mint accounts and are not reflected in the previous request
    #     additional_accounts = []
    #     for wsol_address, wsol_tx_list in token_accounts[WSOL].items():
    #         log(wsol_address, len(wsol_tx_list), wsol_tx_list)
    #         if len(wsol_tx_list) > 1:
    #             additional_accounts.append(wsol_address)
    #
    #     data_to_add = []
    #     all_accounts = additional_accounts + [address]
    #     for additional_address in additional_accounts:
    #         for addressType in ['receiver', 'sender']:
    #             self.update_pb('Retrieving Solana WSOL transfers from bitquery for ' + additional_address + ' as ' + addressType, pb_alloc*0.1/len(additional_accounts))
    #             data = bq.query_with_limit(bq.query_solana_transfers, *[additional_address, addressType])
    #             log("Additional data", len(data), data)
    #             for entry in data:
    #                 if entry['receiver']['address'] == additional_address and entry['sender']['address'] not in all_accounts:
    #                     data_to_add.append(entry)
    #
    #                 if entry['sender']['address'] == additional_address and entry['receiver']['address'] not in all_accounts:
    #                     data_to_add.append(entry)
    #     log("Data to add", len(data_to_add))
    #     all_data.extend(data_to_add)
    #
    #     bq_proxy_list = {}
    #
    #
    #     pathed_data = {}
    #     for entry in all_data:
    #         uid = entry['transaction']['signature']
    #         if uid not in pathed_data:
    #             pathed_data[uid] = {
    #                 'uid':uid,
    #                 'ts':entry['block']['timestamp']['unixtime'],
    #                 'nonce':int(entry['transaction']['transactionIndex']),
    #                 'transfers':defaultdict(list)}
    #
    #         path = entry['instruction']['callPath']
    #         pathed_data[uid]['transfers'][path].append(entry)
    #
    #     count_sol_wsol = 0
    #     for uid, dt in pathed_data.items():
    #         wsol_inc = 0
    #         sol_inc = 0
    #
    #         trdt = dt['transfers']
    #         delete_paths = set()
    #         for path, trlist in trdt.items():
    #
    #             if len(trlist) > 1:
    #                 currency = trlist[0]['currency']
    #
    #                 if currency['address'] == WSOL or (currency['address'] == "-" and currency['symbol'] == 'SOL'):
    #                     for entry in trlist:
    #                         fr = entry['sender']['address']
    #                         to = entry['receiver']['address']
    #                         if fr in additional_accounts:
    #                             fr = address
    #                         if to in additional_accounts:
    #                             to = address
    #                         if fr == to or fr =='' or to =='':
    #                             delete_paths.add(path)
    #                             # break
    #
    #                         if currency['address'] == WSOL:
    #                             wsol_inc = 1
    #                         if (currency['address'] == "-" and currency['symbol'] == 'SOL'):
    #                             sol_inc = 1
    #         if wsol_inc and sol_inc:
    #             count_sol_wsol += 1
    #
    #         for path in delete_paths:
    #             log("Deleting path", uid, path)
    #             del trdt[path]
    #     log("Both sol and wsol", count_sol_wsol)
    #
    #     self.update_pb('Processing Solana transactions from bitquery for ' + address, 0)
    #     log("pathed data length",len(pathed_data))
    #     nft_minted_address_list = {}
    #     for uid, dt in pathed_data.items():
    #         if uid == self.hif:
    #             log("HIF pathed data",dt)
    #
    #
    #         mints = []
    #         trdt = dt['transfers']
    #         for path, entries in trdt.items():
    #             entry = entries[0]
    #
    #             fr = entry['sender']['address']
    #             fr_mintaccount = entry['sender']['mintAccount']
    #
    #             to = entry['receiver']['address']
    #             to_mintaccount = entry['receiver']['mintAccount']
    #             to_type = entry['receiver']['type']
    #
    #             action = entry['instruction']['action']['name']
    #
    #             if action == 'createAccount' and to == to_mintaccount and fr == fr_mintaccount and fr==address and to_type == 'mint':
    #                 mints.append(to)
    #                 log('tx',uid,'found mint',to)
    #
    #
    #         for path, entries in trdt.items():
    #             entry = entries[0]
    #
    #             ts = entry['block']['timestamp']['unixtime']
    #             nonce = int(entry['transaction']['transactionIndex'])
    #             block = None
    #             fr = entry['sender']['address']
    #             fr_mintaccount = entry['sender']['mintAccount']
    #             fr_type = entry['sender']['type']
    #
    #             to = entry['receiver']['address']
    #             to_mintaccount = entry['receiver']['mintAccount']
    #             to_type = entry['receiver']['type']
    #
    #             action = entry['instruction']['action']['name']
    #
    #
    #             if fr in additional_accounts and to != address:
    #                 fr = address
    #             if to in additional_accounts and fr != address:
    #                 to = address
    #
    #             if address not in [fr,to]:# or fr == to:
    #                 continue
    #
    #
    #
    #
    #             transfer_type = entry['transferType']
    #             # if transfer_type in ['close_account']:
    #             #     continue
    #
    #             if fr == '':
    #                 fr = 'mint'
    #
    #             if to == '':
    #                 to = 'burn'
    #
    #             # if transfer_type == 'mint' and fr == '':  # wrap
    #             #     fr = 'mint' #entry['receiver']['mintAccount']
    #             # elif len(fr) == 0:
    #             #     fr = 'unknown'
    #             #
    #             # if transfer_type == 'burn' and to == '':  # unwrap
    #             #     to = 'burn'
    #             #     # to = entry['sender']['mintAccount']
    #             # elif len(to) == 0:
    #             #     to = 'unknown'
    #
    #             val = entry['amount']
    #             # val = float(valstr)
    #             if val == 0:
    #                 continue
    #
    #             if uid not in transactions_dict_bq:
    #                 T = Transaction(user, self)
    #                 T.fee = 0
    #                 T.solana_external_programs = set()
    #                 T.solana_operations = set()
    #                 T.pathes = []
    #                 transactions_dict_bq[uid] = T
    #
    #             T = transactions_dict_bq[uid]
    #
    #
    #                 # try:
    #                 #     T.solana_external_program = entry['instruction']['externalProgram']['id']
    #                 #     T.solana_operation = entry['instruction']['action']['name']
    #                 # except:
    #                 #     T.solana_external_program = None
    #                 #     T.solana_operation = None
    #
    #             # if action in ['transfer']:
    #             if 1:
    #
    #                 external = entry['instruction']['externalProgram']['id']
    #                 if external != '11111111111111111111111111111111' and external[:6] != 'AToken' and external != 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA':
    #                     T.solana_external_programs.add(external)
    #                     if len(action):
    #                         T.solana_operations.add(action.lower())
    #
    #
    #
    #             # path = entry['instruction']['callPath']
    #             # if path in T.pathes:
    #             #     continue
    #             # T.pathes.append(path)
    #
    #             # decimals = entry['currency']['decimals']
    #             # log("val chk",uid,valstr,val,isinstance(val,int),len(valstr),len(str(val)),decimals)
    #             # # if isinstance(val, int) and decimals != 0:
    #             # if int(val) == val and len(valstr) - len(str(int(val))) == 2:
    #             #     log("adjusting val for bq", uid, val, decimals)
    #             #     val = val / float(math.pow(10, 9 - decimals))
    #
    #             symbol = entry['currency']['symbol']
    #             contract = entry['currency']['address']
    #             proxy = None
    #             alternate_proxy = None
    #             input = None
    #             input_len = 0
    #             nft_id = None
    #             if symbol == 'SOL' and contract == '-':
    #                 type = 1
    #                 # if contract != '-':
    #                 #     print("SKIPPING", entry)
    #                 #     continue
    #                 contract = 'SOL'
    #             elif (symbol == '-' and contract == '-' and entry['currency']['decimals'] == 0):  # NFT
    #                 if fr_mintaccount == address:
    #                     proxy = fr_mintaccount
    #                     if to_mintaccount != '':
    #                         alternate_proxy = to_mintaccount
    #                 else:
    #                     if action == 'mintTo' and len(mints) == 1:
    #                         log("TX",uid,"found NFT mint",mints[0])
    #                         # proxy = mints[0]
    #                         proxy = to_mintaccount
    #                         nft_minted_address_list[proxy] = mints[0]
    #                     elif action == 'burn' and to_mintaccount == '':
    #                         log("TX",uid,"burn",fr_mintaccount)
    #                         proxy = fr_mintaccount
    #                     else:
    #                         if to_mintaccount != '':
    #                             proxy = to_mintaccount
    #                         if fr_mintaccount != '':
    #                             alternate_proxy = fr_mintaccount
    #                             if proxy is None:
    #                                 proxy = alternate_proxy
    #                                 alternate_proxy = None
    #
    #                 assert proxy is not None
    #                 contract = proxy
    #                 if val.is_integer():
    #                     type = 4
    #                     log("NFT transfer", uid, entry)
    #                     log("nft_address", uid, proxy, 'mints', mints, 'action', action, fr_mintaccount, address)
    #                 else:
    #                     type = 3
    #             else:
    #                 if entry['currency']['decimals'] == 0 and val.is_integer() and val <= 1000:
    #                     #NFT, but marked up
    #                     type = 4
    #                     input_len = 200
    #                     input = contract
    #                     nft_id = entry['currency']['name']
    #                 else:
    #                     type = 3
    #                 if symbol == 'SOL':
    #                     if contract == WSOL:
    #                         symbol = 'WSOL'
    #                     else:
    #                         log("UNEXPECTED", entry)
    #                         exit(1)
    #                 # #missing all data except amount -- a single case of this had 9 decimals
    #                 # if symbol == '-' and contract == '-' and entry['currency']['decimals'] == 0:
    #                 #     val = val / float(math.pow(10,9))
    #
    #             if symbol == '-':
    #                 symbol = "Unknown token"
    #                 if contract != "-":
    #                     symbol += " ("+contract[:6]+"...)"
    #             fee = 0
    #             if entry['transaction']['feePayer'] == address:
    #                 fee = entry['transaction']['fee']
    #                 T.fee = fee
    #
    #             # input = None
    #             # input_len = 0
    #             # try:
    #             #     input = entry['instruction']['action']['name']
    #             #     input_len = len(input)
    #             # except:
    #             #     pass
    #
    #             # input = entry['instruction']['externalProgram']['id']
    #             # input_len = len(input)
    #             # if input == '11111111111111111111111111111111':
    #             #     input = None
    #             #     input_len = 0
    #
    #
    #             # #save counterparty info SOMEWHERE
    #             # if len(T.grouping) == 0:
    #             #     input_len = 100
    #             #     input = str(list(T.solana_external_programs))+":"+str(list(T.solana_operations))
    #
    #             row = [uid, ts, nonce, block, fr, to, val, symbol, contract, nft_id, 0, input_len, input]
    #
    #             if proxy is not None and type == 4:
    #                 if proxy not in bq_proxy_list:
    #                     bq_proxy_list[proxy] = {'alternate':None,'nft':type == 4,'tx_hash':uid}
    #                 # bq_proxy_list[proxy]['rows'].append(row)
    #                 if alternate_proxy is not None:
    #                     bq_proxy_list[proxy]['alternate'] = alternate_proxy
    #             T.append(type, row)
    #             if row[0] is None or row[1] is None:
    #                 log("Empty field", uid, row)
    #                 exit(1)
    #
    #     log('transactions_dict_bq length',len(transactions_dict_bq))
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #     # div = 1000000000.
    #     # cutoff = 1640995200
    #     # tx_map = {}
    #     # solscan_nft_proxy_address_mapping = {}
    #
    #
    #     # headers = {'accept': 'application/json', 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36'}
    #     # batch_size = 50
    #     # done = False
    #     # offset = 0
    #     # transactions_dict_solscan = {}
    #     # self.update_pb('Retrieving Solana SOL transfers from solscan for ' + address, pb_alloc*0.1)
    #     # while not done:
    #     #     url = "https://public-api.solscan.io/account/solTransfers?account=" + address + "&limit=" + str(batch_size) + "&offset=" + str(offset)
    #     #     log('solana url', url)
    #     #     resp = self.solscan_session.get(url, headers=headers)
    #     #     data = resp.json()['data']
    #     #     log('data received',len(data))
    #     #     for entry in data:
    #     #         uid = entry['txHash']
    #     #         ts = entry['blockTime']
    #     #         if uid not in transactions_dict_solscan:
    #     #             T = Transaction(user, self)
    #     #             transactions_dict_solscan[uid] = T
    #     #         fr = entry['src']
    #     #         to = entry['dst']
    #     #         fee = float(entry['fee']) / div
    #     #         if uid not in tx_map:
    #     #             tx_map[uid] = {'fee': 0}
    #     #         tx_map[uid]['fee'] += fee
    #     #         val = float(entry['lamport']) / div
    #     #
    #     #         if val == 0:
    #     #             continue
    #     #
    #     #         row = [uid, ts, None, None, fr, to, val, self.main_asset, None, None, 0, 0, None]
    #     #         transactions_dict_solscan[uid].append(1, row)
    #     #         if ts < cutoff:
    #     #             done = True
    #     #     if len(data) < batch_size:
    #     #         done = True
    #     #     offset += batch_size
    #     #     time.sleep(self.wait_time)
    #
    #     # done = False
    #     # offset = 0
    #     # self.update_pb('Retrieving Solana token transfers from solscan for ' + address, pb_alloc*0.1)
    #     # while not done:
    #     #     url = "https://public-api.solscan.io/account/splTransfers?account=" + address + "&limit=" + str(batch_size) + "&offset=" + str(offset)
    #     #     log('solana url', url)
    #     #     resp = self.solscan_session.get(url, headers=headers, timeout=10)
    #     #     log('WTFHERE -1')
    #     #     data = resp.json()['data']
    #     #     for entry in data:
    #     #         hashes = entry['signature']
    #     #         hash_cnt = 0
    #     #         for phash in hashes:
    #     #             if phash in tx_map or phash in transactions_dict_bq: #_bq is correct
    #     #                 hash = phash
    #     #                 hash_cnt += 1
    #     #         if hash_cnt == 0 and len(hashes) == 1:
    #     #             hash = hashes[0]
    #     #             fee = 0
    #     #             if 'fee' in entry:
    #     #                 fee = entry['fee']
    #     #             tx_map[hash] = {'fee': fee, 'ops': 'transfer'}
    #     #             hash_cnt = 1
    #     #
    #     #         if hash_cnt != 1:
    #     #             log("hash cnt is", hash_cnt, entry)
    #     #             continue
    #     #             # exit(1)
    #     #
    #     #         ts = entry['blockTime']
    #     #         uid = hash
    #     #         if uid not in transactions_dict_solscan:
    #     #             transactions_dict_solscan[uid] = Transaction(user, self)
    #     #
    #     #         if 'fee' in entry:
    #     #             fee = float(entry['fee']) / div
    #     #             if hash not in tx_map:
    #     #                 tx_map[hash] = {'fee': 0}
    #     #             tx_map[hash]['fee'] += fee
    #     #
    #     #         val = float(entry['changeAmount']) / pow(10, int(entry['decimals']))
    #     #         if val == 0:
    #     #             continue
    #     #
    #     #         if val < 0 or (val == 0 and entry['changeType'] == 'dec'):
    #     #             fr = address
    #     #             to = entry['address']
    #     #             val = -val
    #     #         else:
    #     #             fr = entry['address']
    #     #             to = address
    #     #
    #     #         what = entry['tokenAddress']
    #     #         #NFT?
    #     #         if 'symbol' not in entry and entry['decimals'] == 0 and int(entry['changeAmount']) == float(entry['changeAmount']):
    #     #             type = 4
    #     #             symbol = what
    #     #             solscan_nft_proxy_address_mapping[entry['address']] = what
    #     #             # solscan_nft_address_list[what].append(uid)
    #     #         else:
    #     #             try:
    #     #                 symbol = entry['symbol']
    #     #             except:
    #     #                 symbol = "Unknown token"
    #     #                 log("Unknown token",entry)
    #     #             type = 3
    #     #
    #     #         row = [hash, ts, None, None, fr, to, val, symbol, what, None, 0, 0, None]
    #     #         transactions_dict_solscan[uid].append(type, row)
    #     #         if ts < cutoff:
    #     #             done = True
    #     #     if len(data) < batch_size:
    #     #         done = True
    #     #     offset += batch_size
    #     #     time.sleep(self.wait_time)
    #     #     log("WTFHERE 0")
    #     #
    #     # log("WTFHERE 1")
    #
    #
    #     self.solana_nft_data = user.solana_nft_data
    #     self.solana_proxy_map = user.solana_proxy_map
    #     self.solana_cid_to_proxies_map = user.solana_cid_to_proxies_map
    #     log("loaded nft data")
    #     log(self.solana_nft_data)
    #
    #     idx = 0
    #     log("bq_proxy_list",len(bq_proxy_list), bq_proxy_list)
    #     log('nft_minted_address_list',nft_minted_address_list)
    #     proxy_cnt = len(bq_proxy_list)
    #
    #     def filter_proxy_list(bq_proxy_list, pulled_tokens):
    #         unified_proxy_mapping = {}
    #         for token, data in pulled_tokens.items():
    #             if data['nft']:
    #                 for proxy in data['proxies']:
    #                     unified_proxy_mapping[proxy] = token
    #
    #         # filtered_proxy_list = []
    #         transactions_to_query = {}
    #         for primary_proxy, proxy_data in bq_proxy_list.items():
    #             alternate_proxy = proxy_data['alternate']
    #             tx_hash = proxy_data['tx_hash']
    #             is_nft = proxy_data['nft']
    #             log("getting proxy info for ", primary_proxy, alternate_proxy, is_nft)
    #             # self.update_pb('Retrieving Solana token data from solscan (runs slowly once): ' + str(idx + 1) + "/" + str(proxy_cnt), pb_alloc * 0.2 / proxy_cnt)
    #             found_address = None
    #             for proxy in [primary_proxy, alternate_proxy]:
    #                 if proxy is None:
    #                     continue
    #                 if proxy in self.solana_proxy_map and self.solana_proxy_map[proxy][1] in self.solana_nft_data:
    #                     found_address = self.solana_proxy_map[proxy][1]
    #                     log('gpi1')
    #                 elif proxy in unified_proxy_mapping:
    #                     log('gpi2')
    #                     found_address = unified_proxy_mapping[proxy]
    #                     pass
    #                 # elif proxy in self.proxy_to_token_mapping:
    #                 #     found_address = self.proxy_to_token_mapping[proxy]
    #                 #     log('gpi2')
    #                 # elif proxy in solscan_nft_proxy_address_mapping:
    #                 #     found_address = solscan_nft_proxy_address_mapping[proxy]
    #                 #     log('gpi3')
    #                 elif proxy in nft_minted_address_list:
    #                     found_address = nft_minted_address_list[proxy]
    #                     log('gpi4')
    #                 elif not is_nft:
    #                     found_address = proxy
    #                     log('gpi5')
    #
    #                 if found_address:
    #                     break
    #
    #             if found_address is not None:
    #                 for proxy in [primary_proxy, alternate_proxy]:
    #                     if proxy is not None:
    #                         current_entry = None
    #                         if proxy in unified_proxy_mapping:
    #                             current_entry = unified_proxy_mapping[proxy]
    #                         unified_proxy_mapping[proxy] = found_address
    #                         log("Added proxy mapping",proxy,"->",found_address)
    #                         if current_entry is not None and current_entry != found_address:
    #                             log("OVERWRITING PROXY MAPPING FOR",proxy, "FROM", current_entry,"TO", found_address)
    #             else:
    #                 log('address not found','tx',tx_hash,'proxies',primary_proxy, alternate_proxy)
    #                 transactions_to_query[tx_hash] = [primary_proxy, alternate_proxy]
    #                 # for proxy in [primary_proxy, alternate_proxy]:
    #                 #     if proxy is not None:
    #                 #         filtered_proxy_list.append(proxy)
    #
    #
    #             # for proxy in [primary_proxy, alternate_proxy]:
    #             #     if proxy is None:
    #             #         continue
    #             #     elif proxy in self.solana_proxy_map and self.solana_proxy_map[proxy][1] in self.solana_nft_data:
    #             #         unified_proxy_mapping[proxy] = self.solana_proxy_map[proxy][1]
    #             #         log('gpi1',proxy, self.solana_proxy_map[proxy][1])
    #             #         break
    #             # else:
    #             #     for proxy in [primary_proxy, alternate_proxy]:
    #             #         if proxy in unified_proxy_mapping:
    #             #             log('gpi2',proxy, unified_proxy_mapping[proxy])
    #             #             break
    #             #         elif proxy in solscan_nft_proxy_address_mapping:
    #             #             log('gpi3',proxy,  solscan_nft_proxy_address_mapping[proxy])
    #             #             unified_proxy_mapping[proxy] = solscan_nft_proxy_address_mapping[proxy]
    #             #             break
    #             #         elif proxy in nft_minted_address_list:
    #             #             log('gpi4',proxy,  nft_minted_address_list[proxy])
    #             #             unified_proxy_mapping[proxy] = nft_minted_address_list[proxy]
    #             #             break
    #             #         elif not is_nft:
    #             #             log('gpi5 -- not nft', proxy)
    #             #             unified_proxy_mapping[proxy] = proxy
    #             #             break
    #             #     else:
    #             #         log('gpi6 -- not found')
    #             #         for proxy in [primary_proxy, alternate_proxy]:
    #             #             if proxy is not None:
    #             #                 filtered_proxy_list.append(proxy)
    #         return transactions_to_query, unified_proxy_mapping
    #
    #
    #
    #
    #
    #
    #
    #
    #     transactions_to_query, unified_proxy_mapping = filter_proxy_list(bq_proxy_list, pulled_tokens)
    #     log('transactions_to_query',len(transactions_to_query),transactions_to_query)
    #     proxies_not_found = []
    #     if len(transactions_to_query) > 0:
    #         filtered_proxy_list = []
    #         inverse_tx_map = {}
    #         for tx_hash,proxies in transactions_to_query.items():
    #             for proxy in proxies:
    #                 if proxy is not None:
    #                     inverse_tx_map[proxy] = tx_hash
    #             filtered_proxy_list.extend(proxies)
    #         account_info_list = self.explorer_multi_request({"method": "getAccountInfo", "jsonrpc": "2.0", "params": [None, {"encoding": "jsonParsed", "commitment": "confirmed"}]},
    #                                                         filtered_proxy_list, batch_size=50)
    #         for proxy, output in account_info_list.items():
    #             log('account info output',proxy,output)
    #             try:
    #                 mint = output['value']['data']['parsed']['info']['mint']
    #                 log('found proxy on explorer by account',proxy,mint)
    #                 # unified_proxy_mapping[proxy] = mint
    #                 tx_hash = inverse_tx_map[proxy]
    #                 for proxy in transactions_to_query[tx_hash]:
    #                     if proxy is not None:
    #                         unified_proxy_mapping[proxy] = mint
    #                 del transactions_to_query[tx_hash]
    #             except:
    #                 pass
    #
    #         if len(transactions_to_query) > 0:
    #             tx_data_multi = self.explorer_multi_request(
    #                 {"method": "getTransaction", "jsonrpc": "2.0", "params": [None, {"encoding": "jsonParsed", "commitment": "confirmed", "maxSupportedTransactionVersion": 0}]},
    #                                                   list(transactions_to_query.keys()), pb_alloc=pb_alloc*0.1, batch_size=50)
    #             for tx_hash,entry in tx_data_multi.items():
    #                 log("looking up proxy in tx", tx_hash)
    #                 mint = self.get_nft_address_from_tx(entry)
    #                 if mint is not None:
    #                     for proxy in transactions_to_query[tx_hash]:
    #                         if proxy is not None:
    #                             unified_proxy_mapping[proxy] = mint
    #                 else:
    #                     proxies_not_found.extend(transactions_to_query[tx_hash])
    #
    #         if len(proxies_not_found) > 0:
    #             log("proxies_not_found",proxies_not_found)
    #             for proxy in proxies_not_found:
    #                 if proxy is not None:
    #                     unified_proxy_mapping[proxy] = proxy
    #
    #
    #     inverse_proxy_mapping = {}
    #     for proxy, nft_address in unified_proxy_mapping.items():
    #         if nft_address not in inverse_proxy_mapping:
    #             inverse_proxy_mapping[nft_address] = set()
    #         inverse_proxy_mapping[nft_address].add(proxy)
    #
    #
    #
    #
    #     #compare the two
    #     cnt = 0
    #     match_cnt = 0
    #     transactions_dict_final = {}
    #     log('transactions_dict_bq len 2', len(transactions_dict_bq))
    #     for uid, T in transactions_dict_bq.items():
    #         bq_totals = self.sumup_tx(T, address)
    #
    #         transactions_dict_final[uid] = T
    #         initial_totals = copy.deepcopy(bq_totals)
    #
    #
    #         #remove transfers that add up to zero or near zero
    #         to_ignore = []
    #         for i, dct in bq_totals.items():
    #             if abs(dct['s']) <= 1e-10 and dct['c'] > 1 and i != 'SOL' and i != WSOL:
    #                 to_ignore.extend(dct['indexes'])
    #
    #
    #         # change WSOL to SOL
    #         T_new = Transaction(uid, self)
    #         T_new.fee = T.fee
    #         T_new.solana_external_programs = T.solana_external_programs
    #         T_new.solana_operations = T.solana_operations
    #         for row_idx,entry in enumerate(T.grouping):
    #             if row_idx not in to_ignore:
    #                 type = entry[0]
    #                 row = uid, ts, nonce, block, fr, to, val, symbol, contract, _, fee, input_len, input = entry[1]
    #                 if type == 3 and symbol == 'WSOL' and contract == WSOL:
    #                     type = 1
    #                     row = uid, ts, nonce, block, fr, to, val, 'SOL', 'SOL', _, fee, input_len, input
    #                 T_new.append(type, list(row))
    #         T = T_new
    #
    #         #find pairs of transfers that can be ignored
    #         value_mapping = {}
    #         to_ignore = []
    #         for row_idx, entry in enumerate(T.grouping):
    #             uid, ts, nonce, block, fr, to, val, symbol, contract, _, fee, input_len, input = entry[1]
    #             if fr == to:
    #                 to_ignore.append(row_idx)
    #                 continue
    #
    #             if contract in unified_proxy_mapping:
    #                 contract = unified_proxy_mapping[contract]
    #
    #             if contract not in value_mapping:
    #                 value_mapping[contract] = {}
    #             if fr == address:
    #                 val = -val
    #             if -val in value_mapping[contract] and len(value_mapping[contract][-val]) > 0: #transfer with this amount but opposite direction already mapped -- delete both
    #                 to_ignore.append(row_idx)
    #                 to_ignore.append(value_mapping[contract][-val][0])
    #                 del value_mapping[contract][-val][0]
    #             else:
    #                 if val not in value_mapping[contract]:
    #                     value_mapping[contract][val] = []
    #                 value_mapping[contract][val].append(row_idx)
    #
    #         #find pairs of SOL transfers that are really really close
    #         for row_idx in range(0, len(T.grouping)-1):
    #             entry = T.grouping[row_idx]
    #             uid, ts, nonce, block, fr, to, val, symbol, contract, _, fee, input_len, input = entry[1]
    #             if contract != 'SOL' or row_idx in to_ignore:
    #                 continue
    #             for row_idx2 in range(row_idx+1, len(T.grouping)):
    #                 entry2 = T.grouping[row_idx2]
    #                 uid, ts, nonce, block, fr2, to2, val2, symbol2, contract2, _, fee, input_len, input = entry2[1]
    #                 if contract2 != 'SOL' or row_idx2 in to_ignore:
    #                     continue
    #                 if abs(val-val2) < 0.01 and val != val2 and fr != fr2 and val > 0.01 and val2 > 0.01:
    #                     if val > val2:
    #                         entry[1][6] -= val2
    #                         val -=val2
    #                         to_ignore.append(row_idx2)
    #                         log("ignore near SOL (1)", row_idx2, val, val2)
    #                     else:
    #                         entry2[1][6] -= val
    #                         val2 -=val
    #                         to_ignore.append(row_idx)
    #                         log("ignore near SOL (2)", row_idx, val, val2)
    #
    #
    #         if len(to_ignore) > 0:
    #             log("ignore matching transfers, tx", uid, to_ignore)
    #             T_new = Transaction(uid, self)
    #             T_new.fee = T.fee
    #             T_new.solana_external_programs = T.solana_external_programs
    #             T_new.solana_operations = T.solana_operations
    #
    #             for row_idx, entry in enumerate(T.grouping):
    #                 if row_idx not in to_ignore:
    #                     type = entry[0]
    #                     row = uid, ts, nonce, block, fr, to, val, symbol, contract, _, fee, input_len, input = entry[1]
    #                     T_new.append(type, list(row))
    #
    #             if len(T_new.grouping) == 0: #everything cancelled each other, only the fee remains
    #                 T_new.hash, T_new.ts, T_new.block, T_new.nonce = T.hash, T.ts, T.block, T.nonce
    #             T = T_new
    #
    #
    #         transactions_dict_final[uid] = T_new
    #
    #         new_totals = self.sumup_tx(T,address)
    #         log("Totals comp",T.hash,self.totals_to_str(initial_totals), self.totals_to_str(new_totals))
    #         log("Externals",list(T.solana_external_programs))
    #         log("Operations",list(T.solana_operations))
    #
    #
    #     #add fee and counterparty
    #     for uid in transactions_dict_final:
    #         T = transactions_dict_final[uid]
    #         input = str(list(T.solana_external_programs)) + ":" + str(sorted(list(T.solana_operations)))
    #         if T.fee > 0:
    #             row = [T.hash, T.ts, T.nonce, T.block, address, "network", T.fee, 'SOL', 'SOL', None, 0, 100, input]
    #             T.append(1, row, synthetic=Transfer.FEE)
    #         else:
    #             for entry in T.grouping:
    #                 type = entry[0]
    #                 if type != 4:
    #                     entry[1][-2:] = [100,input]
    #                     break
    #             # try:
    #             #     T.grouping[-1][1][-2] = 100
    #             #     T.grouping[-1][1][-1] =
    #             #     # nonce, block = T.grouping[0][1][2],T.grouping[0][1][3]
    #             #     # log("Both in grouping",uid,T.grouping[0][1][-1])
    #             # except:
    #             #     pass
    #     log("Comp stats",cnt-match_cnt,cnt)
    #
    #     # transactions_dict_final = transactions_dict_solscan
    #     # for uid in transactions_dict_bq.keys():
    #     #     if uid not in transactions_dict_final or len(transactions_dict_final[uid].grouping) == 0:
    #     #         transactions_dict_final[uid] = transactions_dict_bq[uid]
    #     #     else:
    #     #         ts = transactions_dict_final[uid].grouping[0][1][1]
    #     #         log("comp ts",ts,cutoff)
    #     #         if ts < cutoff:
    #     #             transactions_dict_final[uid] = transactions_dict_bq[uid]
    #
    #     log('unified_proxy_mapping',unified_proxy_mapping)
    #
    #
    #     running_tokens = {}
    #     # exit(0)
    #     for hash, T in transactions_dict_final.items():
    #         if T.hash is None or T.ts is None:
    #             log('missing data for tx in bq',hash, T.hash, T.ts)
    #             exit(1)
    #         for row_idx, entry in enumerate(T.grouping):
    #             type = entry[0]
    #             uid, ts, nonce, block, fr, to, val, symbol, contract, nft_id, fee, input_len, input = entry[1]
    #
    #             if address not in [fr,to]:
    #                 continue
    #
    #             if fr == address:
    #                 val = -val
    #             nft = False
    #             if type == 4:
    #                 nft = True
    #                 if contract in unified_proxy_mapping:
    #                     contract = unified_proxy_mapping[contract]
    #
    #             if contract not in running_tokens:
    #                 running_tokens[contract] = {'amount':0,'symbol':symbol,'nft':nft,'accounted_for':False}
    #
    #             running_tokens[contract]['amount'] += val
    #             log('tok change', hash, contract, val, 'type',type,'nft',nft)
    #
    #
    #     log("pulled tokens", pulled_tokens)
    #     log("running tokens",running_tokens)
    #     log("comparing pulled tokens to running tokens", len(pulled_tokens),len(running_tokens))
    #     missing_nft_list = []
    #     missing_list_unexplained = []
    #     mismatch_list_unexplained = []
    #     mismatch_list_decimal = {}
    #     for contract, data in pulled_tokens.items():
    #         proxies = set(data['proxies'])
    #         if contract in inverse_proxy_mapping:
    #             proxies = proxies.union(set(inverse_proxy_mapping[contract]))
    #
    #         proxies = list(proxies)
    #         # cid = contract
    #         # if data['nft']:
    #             # if contract in self.solana_nft_data:
    #             #     log('pulled nft',contract, self.solana_nft_data[contract])
    #             #     cid = self.solana_nft_data[contract][2]
    #             #     if self.solana_nft_data[contract][3] != '':
    #             #         cid += ":" + self.solana_nft_data[contract][3]
    #             # else:
    #             #     log('pulled nft, did not find in mapping', contract)
    #
    #
    #         if contract in running_tokens:
    #             diff = abs(data['amount'] - running_tokens[contract]['amount'])
    #             if diff > 1e-11 and diff > data['amount'] / 10000.:
    #                 log("mismatch 1",'nft',data['nft'],contract,'pulled',data['amount'],'running',running_tokens[contract]['amount'])
    #                 if running_tokens[contract]['amount'] / data['amount'] in [100,1000,10000,100000,1000000,10000000,100000000, 1000000000]:
    #                     mismatch_list_decimal[contract] = running_tokens[contract]['amount'] / data['amount']
    #                 else:
    #                     mismatch_list_unexplained.append([contract, proxies])
    #             running_tokens[contract]['accounted_for'] = True
    #         else:
    #             if data['amount'] != 0:
    #                 log("mismatch 2",'nft',data['nft'],contract, 'pulled', data['amount'], 'running missing', contract in self.solana_nft_data)
    #                 if data['nft']:
    #                     missing_nft_list.append([contract,proxies])
    #                     # missing_nft_list.append(cid)
    #                 else:
    #                     # missing_list_unexplained.append(cid)
    #                     missing_list_unexplained.append([contract,proxies])
    #
    #     log("Missing (from running -- missed buy) NFT list length", len(missing_nft_list), missing_nft_list)
    #
    #     missing_nft_list_pulled = []
    #     for contract,data in running_tokens.items():
    #         if contract != 'SOL' and contract not in pulled_tokens and abs(data['amount']) > 1e-8:# not data['accounted_for']:
    #             missing_nft_list_pulled.append(contract)
    #             missing_nft_list.append([contract, list(inverse_proxy_mapping[contract])])
    #             # cid_entry = self.solana_cid_to_proxies_map[contract]
    #             # missing_nft_list.append([cid_entry['token_address'],list(cid_entry['proxies'])])
    #
    #     log("Missing (from pulled -- missed sell) NFT list length", len(missing_nft_list_pulled), missing_nft_list_pulled)
    #
    #     # joined_missing_list = list(set(missing_nft_list).union(set(missing_nft_list_pulled)))
    #
    #     # log("Joined missing list", len(joined_missing_list), joined_missing_list)
    #
    #
    #
    #
    #
    #     def get_multi_nft_info_from_explorer(nft_address_list):
    #
    #         rv = {}
    #         self.update_pb("Retrieving NFT metadata")
    #
    #         metadata_account_map = {}
    #         for nft_address in nft_address_list:
    #             metadata_account = self.get_metadata_account(nft_address)
    #             log("derived metadata_account",nft_address,metadata_account)
    #             metadata_account_map[metadata_account] = nft_address
    #
    #         # limit = 100
    #         # tx_list = self.explorer_multi_request({"method": "getConfirmedSignaturesForAddress2", "jsonrpc": "2.0", "params": [None, {"limit": limit}]},nft_address_list, pb_alloc=pb_alloc*0.1)
    #         #
    #         #
    #         # nft_mapping = {}
    #         # hash_map = {}
    #         # for nft_address, output in tx_list.items():
    #         #     rv[nft_address] = None
    #         #     if len(tx_list) == limit:
    #         #         continue
    #         #     tx_hash = output[-1]['signature']
    #         #     log('mint tx for nft', nft_address, tx_hash)
    #         #     hash_map[tx_hash] = nft_address
    #         #     nft_mapping[nft_address] = {'mint_tx':tx_hash}
    #         #
    #         # self.update_pb("Retrieving NFT metadata from explorer, step 2/3")
    #         # tx_data_multi = self.explorer_multi_request({"method": "getTransaction", "jsonrpc": "2.0", "params": [None, {"encoding": "jsonParsed", "commitment": "confirmed", "maxSupportedTransactionVersion": 0}]},
    #         #                                  list(hash_map.keys()), pb_alloc=pb_alloc*0.1)
    #         #
    #         # metadata_account_map = {}
    #         # for tx_hash, tx_data in tx_data_multi.items():
    #         #     candidates = defaultdict(int)
    #         #     all_instructions = get_all_instructions(tx_data)
    #         #
    #         #     for instruction in all_instructions:
    #         #         programId = instruction['programId']
    #         #         # log('instruction programId',hash_map[tx_hash], tx_hash, programId)
    #         #         if programId == 'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s':
    #         #             acc = instruction['accounts'][0]
    #         #             data = instruction['data']
    #         #             candidates[acc] += 1
    #         #             candidates[acc] += len(data) // 50
    #         #         if 'parsed' in instruction:
    #         #             parsed = instruction['parsed']
    #         #             if programId == '11111111111111111111111111111111':
    #         #                 info = parsed['info']
    #         #                 if 'owner' in info and info['owner'] == 'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s' and parsed['type'] == 'assign':
    #         #                     candidates[info['account']] += 1
    #         #
    #         #     winner = None
    #         #     if len(candidates) > 0:
    #         #         sorted_candidates = sorted(list(candidates.items()), key = lambda x: x[1], reverse=True)
    #         #         winner = sorted_candidates[0][0]
    #         #         if (len(sorted_candidates) > 1 and sorted_candidates[0][1] == sorted_candidates[1][1]) or sorted_candidates[0][1] == 1: #tied or low number
    #         #             winner = None
    #         #     log('candidates',hash_map[tx_hash],tx_hash, dict(candidates),'winner',winner)
    #         #     if winner is not None:
    #         #         metadata_account_map[winner] = hash_map[tx_hash]
    #
    #         # self.update_pb("Retrieving NFT metadata from explorer, step 3/3")
    #         metadata = self.explorer_multi_request({"method":"getAccountInfo","jsonrpc":"2.0","params":[None,{"encoding":"jsonParsed","commitment":"confirmed"}]},
    #                                           list(metadata_account_map.keys()),batch_size=20)
    #         for metadata_address, entry in metadata.items():
    #             nft_address = metadata_account_map[metadata_address]
    #             rv[nft_address] = None
    #             try:
    #                 datadump = entry['value']['data'][0]
    #             except:
    #                 log("Failed to get metadata",nft_address,metadata_address,entry)
    #                 continue
    #
    #
    #             log("meta dump", nft_address, datadump)
    #
    #             decoded_dump = self.unpack_metadata_account(datadump)
    #             log("meta dump decoded", nft_address, metadata_address, decoded_dump)
    #
    #             try:
    #                 data = decoded_dump['data']
    #                 name = data['name']
    #                 update_authority = decoded_dump['update_authority'].decode("utf-8")
    #                 minter = decoded_dump['mint'].decode("utf-8")
    #                 symbol = data['symbol']
    #                 uri = data['uri']
    #                 rv[nft_address] = {'name':name,'symbol':symbol,'uri':uri,'update_authority':update_authority,'minter':minter}
    #             except:
    #                 log("required fields not found in decoding", nft_address, datadump)
    #         return rv
    #
    #     missing_nft_address_list = []
    #     for nft_address, proxies in missing_nft_list:
    #         missing_nft_address_list.append(nft_address)
    #     log('missing_nft_address_list', missing_nft_address_list)
    #     log('inverse_proxy_mapping',inverse_proxy_mapping)
    #     log('missing_nft_address_list',list(missing_nft_address_list))
    #
    #     nft_address_list = set(inverse_proxy_mapping.keys()).union(set(missing_nft_address_list))
    #     log("All nfts len", len(nft_address_list))
    #
    #     all_nft_data = get_multi_nft_info_from_explorer(nft_address_list)
    #     failed_retrieval = []
    #     for nft_address, data in all_nft_data.items():
    #         if data is None:
    #             failed_retrieval.append(nft_address)
    #     log("Failed to retrieve", len(failed_retrieval),failed_retrieval)
    #     # full_retrieval_fail = []
    #     # for idx, nft_address in enumerate(failed_retrieval):
    #     #     self.update_pb("Retrieving fake NFT metadata from Solscan, "+str(idx+1)+"/"+str(len(failed_retrieval)), pb_alloc * 0.1/len(failed_retrieval))
    #     #     nft_data = self.get_nft_info_from_solscan(nft_address)
    #     #     if nft_data is not None:
    #     #         all_nft_data[nft_address] = nft_data
    #     #     else:
    #     #         full_retrieval_fail.append(nft_address)
    #     # log("full_retrieval_fail",len(full_retrieval_fail),full_retrieval_fail)
    #     # exit(1)
    #
    #
    #     proxies_to_query = []
    #     for nft_address, proxies in missing_nft_list:
    #         if proxies is not None:
    #             proxies_to_query.extend(proxies)
    #         else:
    #             log("No proxies for missing address", nft_address)
    #     log('proxies_to_query',proxies_to_query)
    #     if len(proxies_to_query) > 0:
    #         self.update_pb("Retrieving authority reassignments, step 1/2", 0)
    #         # tx_multi_list = self.explorer_multi_request({"method": "getConfirmedSignaturesForAddress2", "jsonrpc": "2.0", "params": [None, {"limit": 1000}]}, proxies_to_query, pb_alloc=pb_alloc * 0.1)
    #         tx_multi_list = self.explorer_multi_request({"method": "getSignaturesForAddress", "jsonrpc": "2.0", "params": [None, {"limit": 1000}]}, proxies_to_query, pb_alloc=pb_alloc * 0.1)
    #
    #         txs_to_retrieve = set()
    #         for proxy, output in tx_multi_list.items():
    #             for entry in output:
    #                 tx_hash = entry['signature']
    #                 if tx_hash in transactions_dict_final:
    #                     txs_to_retrieve.add(tx_hash)
    #         txs_to_retrieve = list(txs_to_retrieve)
    #
    #         if len(txs_to_retrieve) > 0:
    #             log("Transactions to check for setauthority", len(txs_to_retrieve),txs_to_retrieve)
    #             self.update_pb("Retrieving authority reassignments, step 2/2",0)
    #             tx_multi_data = self.explorer_multi_request({"method":"getTransaction","jsonrpc":"2.0","params":[None,{"encoding":"jsonParsed","commitment":"confirmed","maxSupportedTransactionVersion":0}]},
    #                                                         txs_to_retrieve, pb_alloc=pb_alloc * 0.1,batch_size=20)
    #
    #             nfts_found = set()
    #             for txhash, tx_data in tx_multi_data.items():
    #                 all_instructions = self.get_all_instructions(tx_data)
    #                 new_row = None
    #                 programid = None
    #
    #                 for instruction in all_instructions:
    #                     log("tx",txhash,"instruction",instruction)
    #
    #
    #                     try:
    #                         parsed = instruction['parsed']
    #                         type = parsed['type']
    #                         params = parsed['info']
    #                         if type == 'setAuthority':
    #
    #                             if params['account'] in proxies_to_query and params['authorityType'] == 'accountOwner' and address in [params['authority'], params['newAuthority']]:
    #                                 log("Adding NFT transfer to transaction", txhash, params['account'])
    #                                 new_row = [txhash, T.ts, T.nonce, T.block, params['authority'], params['newAuthority'], 1, 'placeholder symbol', params['account'], 'placeholder name', 0, 0, None]
    #                                 nfts_found.add(params['account'])
    #
    #                         if type == 'createAccount':# and params['newAccount'] in proxies_to_query:
    #                             log("Setting programid to",params['owner'])
    #                             programid = params['owner']
    #
    #                     except:
    #                         log('fail to parse')
    #                         pass
    #                 if new_row:
    #                     T = transactions_dict_final[txhash]
    #                     if programid:
    #                         for row_idx,entry in enumerate(T.grouping):
    #                             row = entry[1]
    #                             if row[-2] == 100:
    #                                 if row[-1] == "[]:[]":
    #                                     row[-1] = str([programid]) + ":" + str(['setAuthority'])
    #                                 else:
    #                                     break
    #                         else:
    #                             row[-2:] = [100, str([programid]) + ":" + str(['setAuthority'])]
    #                         # new_row[-2:] = [100,str([programid]) + ":" + str(['setAuthority'])]
    #                     log("New row", new_row)
    #
    #                     T.append(4, new_row, prepend=True)
    #
    #
    #             log("Setauthorities found", len(nfts_found),nfts_found)
    #
    #
    #
    #     # found_cnt = 0
    #     # for idx, (nft_address, proxies) in enumerate(missing_nft_list):
    #     #     self.update_pb('Retrieving missing Solana nft data from solscan (runs slowly once): ' + str(idx + 1) + "/" + str(len(missing_nft_list)), 0 * 0.2 / len(missing_nft_list))
    #     #     added = False
    #     #     log("Getting missing nft", nft_address, 'proxy length', len(proxies), proxies)
    #     #     _, symbol, token_address, name = self.get_token_info_solscan(nft_address, True)
    #     #     # url = 'https://api.solscan.io/account/transaction?address=' + nft_address
    #     #     # url = 'https://public-api.solscan.io/account/transactions?account=' + nft_address
    #     #     for proxy in proxies:
    #     #         # url = 'https://public-api.solscan.io/account/transactions?account=' + proxy+'&limit=20'
    #     #         # log("Getting transactions for missing nft", nft_address, url)
    #     #         # time.sleep(0.15)
    #     #         # t = time.time()
    #     #         # resp = self.solscan_session.get(url, timeout=10)
    #     #
    #     #         explorer_dump = {"method":"getConfirmedSignaturesForAddress2","jsonrpc":"2.0","params":[proxy,{"limit":25}],"id":str(uuid.uuid4())}
    #     #         log("Getting transactions for missing nft", proxy, explorer_dump)
    #     #         t = time.time()
    #     #         resp = self.explorer_session.post('https://explorer-api.mainnet-beta.solana.com', timeout=10, json=explorer_dump, headers=explorer_headers )
    #     #         log("Timing",time.time()-t)
    #     #         if resp.status_code == 200:
    #     #             headers = resp.headers
    #     #             l0 = headers['x-ratelimit-conn-remaining']
    #     #             l1 = headers['x-ratelimit-method-remaining']
    #     #             l2 = headers['x-ratelimit-rps-remaining']
    #     #             log("Remaining limits",l0,l1,l2)
    #     #
    #     #             data = resp.json()
    #     #
    #     #             data = data['result'] #explorer
    #     #
    #     #             if len(data) == 20:
    #     #                 log("Transaction list data at limit")
    #     #             for entry in data:
    #     #                 # txhash = entry['txHash']
    #     #                 txhash = entry['signature'] #explorer
    #     #                 log("Checking NFT transaction",txhash)
    #     #                 if txhash in transactions_dict_final:
    #     #                     log("Found")
    #     #
    #     #                     time.sleep(0.15)
    #     #
    #     #                     url = 'https://public-api.solscan.io/transaction/' + txhash
    #     #                     log("Getting individual tx info for", txhash, url)
    #     #                     t = time.time()
    #     #                     resp = self.solscan_session.get(url, timeout=10)
    #     #
    #     #
    #     #                     # explorer_dump = {"method":"getTransaction","jsonrpc":"2.0","params":[txhash,{"encoding":"jsonParsed","commitment":"confirmed","maxSupportedTransactionVersion":0}],"id":str(uuid.uuid4())}
    #     #                     # log("Getting individual tx info for", txhash, explorer_dump)
    #     #                     # t = time.time()
    #     #                     # resp = self.explorer_session.post('https://explorer-api.mainnet-beta.solana.com', timeout=10, json=explorer_dump, headers=explorer_headers )
    #     #                     # log("Timing", time.time() - t)
    #     #                     # headers = resp.headers
    #     #                     # l0 = headers['x-ratelimit-conn-remaining']
    #     #                     # l1 = headers['x-ratelimit-method-remaining']
    #     #                     # l2 = headers['x-ratelimit-rps-remaining']
    #     #                     # log("Remaining limits", l0, l1, l2)
    #     #
    #     #                     if resp.status_code == 200:
    #     #                         # data = resp.json()
    #     #                         # instructions = data['result']['meta']['innerInstructions'][0]['instructions']
    #     #                         # for instruction in instructions:
    #     #                         #     parsed = instruction['parsed']
    #     #                         #     type = parsed['type']
    #     #                         #     if type == 'setAuthority':
    #     #                         #         params = parsed['info']
    #     #                         #         if params['account'] == proxy and params['authorityType'] == 'accountOwner':
    #     #                         #             log("Adding NFT transfer to transaction", txhash)
    #     #                         #             T = transactions_dict_final[txhash]
    #     #                         #             row = txhash, T.ts, T.nonce, T.block, params['authority'], params['newAuthority'], 1, \
    #     #                         #                   symbol, token_address, name, 0, 0, None
    #     #                         #             T.append(4,row,prepend=True)
    #     #                         #             added = True
    #     #
    #     #                         balances = data['tokenBalanes'] #sic
    #     #                         instructions = data['innerInstructions'][0]['parsedInstructions']
    #     #                         for bal in balances:
    #     #                             tok = bal['token']['tokenAddress']
    #     #                             if tok == nft_address and bal['amount']['postAmount'] == bal['amount']['preAmount']:
    #     #                                 log("Looking for setauthority")
    #     #                                 for inst in instructions:
    #     #                                     if inst['type'] == 'setAuthority':
    #     #                                         params = inst['params']
    #     #                                         if params['account'] == proxy and params['authorityType'] == 'accountOwner':
    #     #                                             log("Adding NFT transfer to transaction", txhash)
    #     #                                             T = transactions_dict_final[txhash]
    #     #                                             row = txhash, T.ts, T.nonce, T.block, params['authority'], params['newAuthority'], 1, \
    #     #                                                   symbol, token_address, name, 0, 0, None
    #     #                                             T.append(4,row,prepend=True)
    #     #                                             added = True
    #     #
    #     #                                 break
    #     #                     else:
    #     #                         log("Failed to get individual transaction","code", resp.status_code, resp.content)
    #     #                         exit(1)
    #     #
    #     #
    #     #         else:
    #     #             log("Failed to get transactions", "code", resp.status_code, resp.content)
    #     #             exit(1)
    #     #     if added:
    #     #          found_cnt += 1
    #     #     else:
    #     #         log("Not found")
    #     # log("Added missing nfts", found_cnt)
    #
    #     log("Processing decimal mismatches", len(mismatch_list_decimal), mismatch_list_decimal)
    #     if len(mismatch_list_decimal):
    #         for uid, T in transactions_dict_final.items():
    #             for row_idx,entry in enumerate(T.grouping):
    #
    #
    #                 row = entry[1]
    #                 contract = row[8]
    #                 if uid == 'NcopDrjrmoFsnkFyiGnvjRxMj3Q8tDq6ZAKECPUydb8cw3usY3NQFPTtZuyCcD8PbyvEiYFpvHWMNKXc3LugKB4':
    #                     log("BUG row", row)
    #                     log("BUG contract", contract)
    #                 if contract in unified_proxy_mapping:
    #                     nft_address = unified_proxy_mapping[contract]#solscan and bitquery disagree whether this is an NFT -- proxy in transaction list, but nft in mismatch list
    #                     if nft_address in pulled_tokens and pulled_tokens[nft_address]['nft'] == False:
    #                         contract = nft_address
    #
    #                 if contract in mismatch_list_decimal:
    #                     row[6] /= mismatch_list_decimal[contract]
    #                     log("Updated row "+str(row_idx)+" in tx", uid, contract, mismatch_list_decimal[contract])
    #
    #
    #     #replace NFT proxies with NFT addresses
    #     running_tokens = {}
    #     for txhash, T in transactions_dict_final.items():
    #         for row_idx, entry in enumerate(T.grouping):
    #             row = entry[1]
    #             uid, ts, nonce, block, fr, to, val, symbol, contract, nft_id, fee, input_len, input = entry[1]
    #             running_contract = contract
    #
    #
    #             if contract in unified_proxy_mapping:
    #                 nft_address = unified_proxy_mapping[contract]
    #                 log('renaming nft 1', txhash, contract, nft_address)
    #                 nft_data = all_nft_data[nft_address]
    #                 running_contract = contract = nft_address
    #                 if nft_data is not None:
    #                     if nft_data['update_authority'] != address:
    #                         contract = nft_data['update_authority']
    #
    #                     nft_id = nft_data['name']
    #                     symbol = nft_data['symbol']
    #                 else:
    #                     symbol = 'unknown ('+nft_address[:6]+'...)'
    #                     nft_id = ''
    #                 log('renaming nft 2',txhash, 'nft_address',nft_address, 'symbol',symbol, 'ua',contract, 'nft_id',nft_id, 'fr',fr, 'to',to, 'val',val)
    #                 if contract in pulled_tokens and pulled_tokens[contract]['nft'] == False:  # bitquery sometimes gets it wrong
    #                     entry[0] = 3
    #                     row[7:9] = symbol, nft_address
    #                 else:
    #                     row[7:13] = symbol, contract, nft_id, 0, 200, nft_address
    #
    #
    #             if running_contract not in running_tokens:
    #                 running_tokens[running_contract] = 0
    #             running_amount = 0
    #             if fr == address:
    #                 running_amount = -val
    #             if to == address:
    #                 running_amount = val
    #             log("adjusting running tokens", 'running_contract',running_contract, 'current val',running_tokens[running_contract], 'adjustment',running_amount)
    #             running_tokens[running_contract] += running_amount
    #
    #     mismatch = []
    #     missing_from_pulled = []
    #     missing_from_running = []
    #     for address, amount in running_tokens.items():
    #         if abs(amount) > 1e-8:
    #             if address not in pulled_tokens:
    #                 missing_from_pulled.append([address,amount])
    #             elif abs(pulled_tokens[address]['amount'] - amount) > 1e-8:
    #                 mismatch.append([address,pulled_tokens[address]['amount'] - amount])
    #     for address, data in pulled_tokens.items():
    #         if data['amount'] != 0:
    #             if address not in running_tokens:
    #                 missing_from_running.append([address, data['amount']])
    #
    #     log('missing_from_pulled',len(missing_from_pulled),missing_from_pulled,filename='solana.txt')
    #     log('missing_from_running', len(missing_from_running), missing_from_running,filename='solana.txt')
    #     log('mismatch',len(mismatch), mismatch,filename='solana.txt')
    #
    #     # log("Unexplained mismatches",len(mismatch_list_unexplained), mismatch_list_unexplained)
    #     # log("Unexplained misses", len(missing_list_unexplained), missing_list_unexplained)
    #     #
    #     # for idx,nft_address in enumerate(missing_nft_list):
    #     #     self.update_pb('Retrieving missing Solana nft data from solscan (runs slowly once): ' + str(idx + 1) + "/" + str(len(missing_nft_list)), 0 * 0.2 / len(missing_nft_list))
    #     #     log("Getting missing nft", nft_address)
    #     #     _, symbol, token_address, name = self.get_token_info_solscan(nft_address, nft_address, nft_mapping, True)
    #     #
    #     #     url = 'https://api.solscan.io/nft/trade?mint='+nft_address
    #     #     log("Getting trades for missing nft", nft_address, url)
    #     #     time.sleep(0.25)
    #     #     resp = self.solscan_session.get(url,timeout=5)
    #     #     if resp.status_code == 200:
    #     #         data = resp.json()
    #     #         if data['success']:
    #     #             for entry in data['data']:
    #     #                 log("Checking NFT transfer",entry['buyer'],entry['seller'],address)
    #     #                 if address in (entry['buyer'],entry['seller']):
    #     #                     uid = entry['signature']
    #     #                     log("Adding NFT transfer to transaction",uid)
    #     #                     T = transactions_dict_final[uid]
    #     #                     row = uid, T.ts, T.nonce, T.block, entry['seller'], entry['buyer'], 1, symbol, token_address, name, 0, 0, None
    #     #                     T.append(4,row)
    #     #         else:
    #     #             log("Failed to get trades", data)
    #     #     else:
    #     #         log("Failed to get trades", "code", resp.status_code)
    #
    #     txlist = list(transactions_dict_final.values())
    #
    #     txlist.sort(key=lambda x: str(x.ts) + "_" + x.hash)
    #     return txlist

    def get_current_tokens_internal(self, address):
        tokens = {}
        resp = self.explorer_multi_request({"method":"getTokenAccountsByOwner","jsonrpc":"2.0","params":[None,{"programId":"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"},{"encoding":"jsonParsed","commitment":"processed"}]}
                                    ,[address])
        data = resp[address]['value']
        for entry in data:
            proxy = entry['pubkey']
            info = entry['account']['data']['parsed']['info']
            amount = float(info['tokenAmount']['uiAmount'])
            token = info['mint']
            nft = False
            if info['tokenAmount']['decimals'] == 0 and amount.is_integer():
                nft = True

            if token not in tokens:
                tokens[token] = {'amount':amount,'nft':nft, 'proxies':[proxy]}
            else:
                tokens[token]['amount'] += amount
                tokens[token]['proxies'].append(proxy)

        return tokens

    #must be ran after get_transactions, not before
    def get_current_tokens(self, address):
        try:
            resp = self.explorer_multi_request({"method": "getAccountInfo", "jsonrpc": "2.0", "params": [None, {"encoding": "jsonParsed", "commitment": "confirmed"}]}, [address])
            data = resp[address]['value']
            log("sol balance",data)
            lamports = data['lamports']
            sol_amt = lamports / 1000000000.
            rv = {}
            rv['SOL'] = {'symbol':'SOL','amount':sol_amt}

            tokens = self.get_current_tokens_internal(address)
            for contract, token_data in tokens.items():
                amount = token_data['amount']
                if amount == 0:
                    continue
                gathered_token_data = self.all_token_data[contract]
                symbol = gathered_token_data['symbol']

                if token_data['nft']:
                    nft_id = gathered_token_data['name']
                    if 'update_authority' in gathered_token_data:
                        ua = gathered_token_data['update_authority']
                        if ua != address:
                            nft_id += " " + contract
                            contract = ua
                    if contract not in rv:
                        rv[contract] = {'symbol': symbol, 'nft_amounts':{}}

                    rv[contract]['nft_amounts'][nft_id] = amount

                    # rv[contract][nft_id] = [symbol,amount]
                else:
                    rv[contract] = {'symbol': symbol, 'amount':amount}

            rv = dict(rv)
            WSOL = "So11111111111111111111111111111111111111112"
            if WSOL in rv:
                try:
                    rv['SOL']['amount'] += rv[WSOL]['amount']
                    del rv[WSOL]
                except:
                    pass

            log("current tokens to store",rv,filename='solana.txt')
            return rv
        except:
            log_error("SOLANA: Failed to get_current_tokens", address)
            return None



    # def get_current_tokens(self,address):
    #     tokens = {}
    #     # self.proxy_to_token_mapping = {}
    #     url = "https://public-api.solscan.io/account/tokens?account="+address
    #
    #     try:
    #         resp = self.solscan_session.get(url,timeout=5)
    #     except:
    #         return None
    #     data = resp.json()
    #     for entry in data:
    #         amount = float(entry['tokenAmount']['uiAmount'])
    #         proxy = entry['tokenAccount']
    #         token = entry['tokenAddress']
    #         # self.proxy_to_token_mapping[proxy] = token
    #         symbol = None
    #         nft = False
    #         if entry['tokenAmount']['decimals'] == 0 and 'tokenSymbol' not in entry and amount.is_integer():# (amount == 1 or amount == 0):
    #             nft = True
    #
    #
    #         if 'tokenSymbol' in entry:
    #             symbol = entry['tokenSymbol']
    #         if token not in tokens:
    #             tokens[token] = {'amount':amount,'symbol':symbol,'nft':nft, 'proxies':[proxy]}
    #         else:
    #             tokens[token]['amount'] += amount
    #             tokens[token]['proxies'].append(proxy)
    #     return tokens

    def get_solana_counterparties(self, transaction):
        #counter_parties[prog_addr] = [prog_name, sig, decoded_sig, editable, addr]
        pass

    def get_contracts(self,transactions):
        return [], [], []

    # def update_progenitors(self,user, counterparty_list, pb_alloc):
    #     return
    # def update_address_from_scan(self, address_db, user, address, max_depth=None):
    #     # log("progenitor for",address)
    #     if address is None or len(address) < 32 or len(address) > 44:
    #         return
    #
    #     entity,_,_ = self.get_progenitor_entity(address)
    #     if entity is not None:
    #         return
    #
    #     log('solana-looking up address',address)
    #     url = 'https://api.solscan.io/search?keyword='+address
    #     resp = self.solscan_session.get(url,timeout=3)
    #     label = None
    #     try:
    #         data = resp.json()
    #         label = data['data'][0]['result'][0]['name']
    #         log('found label',label)
    #     except:
    #         log('resp for ',address,resp.content)
    #
    #     if label is not None:
    #         label_words = label.split(" ")
    #         entity = label_words[0].upper()
    #         address_db.insert_kw(self.name + '_labels', values=[address, entity], ignore=True)
    #         address_db.insert_kw(self.name + '_addresses', values=[address, label, None, entity, 'lookup'], ignore=True)
    #         address_db.commit()
    #         self.addresses[address] = {'entity': entity, 'ancestor': None}
    #
    #     if label is None:
    #         url = 'https://hyper.solana.fm/v2/address/'+address
    #         try:
    #             resp = self.solscan_session.get(url, timeout=3)
    #             data = resp.json()
    #             data = data['address']
    #             if data is not None:
    #                 label = data['FriendyName']
    #                 log('found label on solana.fm', label,filename='solana.txt')
    #         except:
    #             log('resp for ', address, resp.content)

    def correct_transactions(self, address, transactions, pb_alloc):
        return transactions


    #https://chainstack.com/the-mystery-of-solana-metaplex-nft-metadata-encoding/
    def unpack_metadata_account(self,data):
        data = base64.b64decode(data)

        assert (data[0] == 4)
        i = 1
        source_account = base58.b58encode(bytes(struct.unpack('<' + "B" * 32, data[i:i + 32])))
        i += 32
        mint_account = base58.b58encode(bytes(struct.unpack('<' + "B" * 32, data[i:i + 32])))
        i += 32
        name_len = struct.unpack('<I', data[i:i + 4])[0]
        i += 4
        name = struct.unpack('<' + "B" * name_len, data[i:i + name_len])
        i += name_len
        symbol_len = struct.unpack('<I', data[i:i + 4])[0]
        i += 4
        symbol = struct.unpack('<' + "B" * symbol_len, data[i:i + symbol_len])
        i += symbol_len
        uri_len = struct.unpack('<I', data[i:i + 4])[0]
        i += 4
        uri = struct.unpack('<' + "B" * uri_len, data[i:i + uri_len])
        i += uri_len
        fee = struct.unpack('<h', data[i:i + 2])[0]
        i += 2
        has_creator = data[i]
        i += 1
        creators = []
        verified = []
        share = []
        if has_creator:
            creator_len = struct.unpack('<I', data[i:i + 4])[0]
            i += 4
            for _ in range(creator_len):
                creator = base58.b58encode(bytes(struct.unpack('<' + "B" * 32, data[i:i + 32])))
                creators.append(creator)
                i += 32
                verified.append(data[i])
                i += 1
                share.append(data[i])
                i += 1
        primary_sale_happened = bool(data[i])
        i += 1
        is_mutable = bool(data[i])
        metadata = {
            "update_authority": source_account,
            "mint": mint_account,
            "data": {
                "name": bytes(name).decode("utf-8").strip("\x00"),
                "symbol": bytes(symbol).decode("utf-8").strip("\x00"),
                "uri": bytes(uri).decode("utf-8").strip("\x00"),
                "seller_fee_basis_points": fee,
                "creators": creators,
                "verified": verified,
                "share": share,
            },
            "primary_sale_happened": primary_sale_happened,
            "is_mutable": is_mutable,
        }
        return metadata


    # def create_program_address(self,seeds, program_id):
    #     """Derive a program address from seeds and a program ID."""
    #     buffer = b"".join(seeds + [bytes(program_id), b"ProgramDerivedAddress"])
    #     hashbytes = sha256(buffer).digest()
    #     try:
    #         decodepoint(hashbytes)
    #         raise Exception("Invalid seeds, address must fall off the curve")
    #     except:
    #         address = base58.b58encode(bytes(hashbytes)).decode("utf-8")
    #         return address
    #
    #
    # def find_program_address(self,seeds, program_id):
    #     """Find a valid program address.
    #     Valid program addresses must fall off the ed25519 curve.  This function
    #     iterates a nonce until it finds one that when combined with the seeds
    #     results in a valid program address.
    #     """
    #     nonce = 255
    #     while nonce != 0:
    #         try:
    #             buffer = seeds + [nonce.to_bytes(1, byteorder="little")]
    #             address = self.create_program_address(buffer, program_id)
    #         except Exception:
    #             nonce -= 1
    #             continue
    #         return address, nonce
    #     raise KeyError("Unable to find a viable program address nonce")
    #
    # def bytes(self,val):
    #     rv = base58.b58decode(val)
    #     if len(rv) == 32:
    #         return rv
    #     else:
    #         return rv.rjust(32, b"\0")

    def get_metadata_account(self,mint_key):
        metaplex = 'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s'
        METADATA_PROGRAM_ID = PublicKey(metaplex)

        pk = PublicKey.find_program_address(
            [b'metadata', bytes(METADATA_PROGRAM_ID), bytes(PublicKey(mint_key))],
            METADATA_PROGRAM_ID
        )[0]

        metadata_address = str(pk)
        return metadata_address

    def balance_provider_correction(self,chain_data):
        return

    def get_progenitor_entity(self,address):
        if address in Solana.NATIVE_PROGRAMS:
            return Solana.NATIVE_PROGRAMS[address],None
        return super(Solana,self).get_progenitor_entity(address)

    def update_progenitors(self, counterparty_list, pb_alloc):
        all_db_writes = []
        if len(counterparty_list) == 0:
            return

        addresses_to_lookup = []
        for address in counterparty_list:
            if address in Solana.NATIVE_PROGRAMS:
                continue

            entity,_ = self.get_progenitor_entity(address)
            if entity is not None:
                continue
            addresses_to_lookup.append(normalize_address(address))
        log(self.name, "Addresses to lookup",addresses_to_lookup,filename='address_lookups.txt')


        if len(addresses_to_lookup) > 0:
            batch_size = 100
            batch_cnt = len(addresses_to_lookup) // batch_size + 1
            pb_per_batch = pb_alloc / batch_cnt
            offset = 0
            for batch_idx in range(batch_cnt):
                good, db_writes = self.update_multiple_addresses_from_scan(addresses_to_lookup[offset:offset+batch_size])
                all_db_writes.extend(db_writes)
                offset += 5
                self.update_pb('Looking up counterparties (runs slowly once): ' + str(batch_idx+1) + '/' + str(batch_cnt), pb_per_batch)
                if not good:
                    break
        return all_db_writes


    def update_multiple_addresses_from_scan(self,addresses):
        log(self.name,"multi address lookup", addresses, filename='address_lookups.txt')
        db_writes = []

        if len(addresses) == 0:
            return True,[]



        url = "https://hyper.solana.fm/v2/address/" + ','.join(addresses)
        try:
            resp = requests.get(url)
            time.sleep(0.5)
        except:
            log_error("Failed to get contract creators", url)
            self.current_import.add_error(Import.NO_CREATORS, chain=self, debug_info=traceback.format_exc())
            return False, []

        try:
            data = resp.json()
        except:
            log_error("Failed to get contract creators",url,resp.status_code,resp.content)
            self.current_import.add_error(Import.NO_CREATORS, chain=self, debug_info=traceback.format_exc())
            return False, []
        if data is None:
            return False, []

        try:
            for address,entry in data.items():
                entity = 'unknown'
                if entry is not None and 'FriendlyName' in entry:
                    entity = entry['FriendlyName']
                    self.entity_map[address] = [entity,None]
                db_writes.append([self.name,[address,None,None,entity,'lookup']])

        except:
            log("Unexpected data",data,filename='address_lookups.txt')
            return False, []

        return True, db_writes





class PublicKey:
    LENGTH = 32
    """Constant for standard length of a public key."""

    def __init__(self, value):
        """Init PublicKey object."""
        self._key = None
        if isinstance(value, str):
            try:
                self._key = base58.b58decode(value)
            except ValueError as err:
                raise ValueError("invalid public key input:", value) from err
            if len(self._key) != self.LENGTH:
                raise ValueError("invalid public key input:", value)
        elif isinstance(value, int):
            self._key = bytes([value])
        else:
            self._key = bytes(value)

        if len(self._key) > self.LENGTH:
            raise ValueError("invalid public key input:", value)

    def __bytes__(self) -> bytes:
        """Public key in bytes."""
        if not self._key:
            return bytes(self.LENGTH)
        return self._key if len(self._key) == self.LENGTH else self._key.rjust(self.LENGTH, b"\0")

    def __eq__(self, other) -> bool:
        """Equality definition for PublicKeys."""
        return False if not isinstance(other, PublicKey) else bytes(self) == bytes(other)

    def __repr__(self) -> str:
        """Representation of a PublicKey."""
        return str(self)

    def __str__(self) -> str:
        """String definition for PublicKey."""
        return self.to_base58().decode("utf-8")

    def to_base58(self) -> bytes:
        """Public key in base58."""
        return base58.b58encode(bytes(self))

    @staticmethod
    def create_with_seed(from_public_key, seed, program_id):
        """Derive a public key from another key, a seed, and a program ID."""
        raise NotImplementedError("create_with_seed not implemented")

    @staticmethod
    def create_program_address(seeds, program_id):
        """Derive a program address from seeds and a program ID."""
        buffer = b"".join(seeds + [bytes(program_id), b"ProgramDerivedAddress"])
        hashbytes = sha256(buffer).digest()
        if not PublicKey._is_on_curve(hashbytes):
            return PublicKey(hashbytes)
        raise Exception("Invalid seeds, address must fall off the curve")

    @staticmethod
    def find_program_address(seeds, program_id):

        nonce = 255
        while nonce != 0:
            try:
                buffer = seeds + [nonce.to_bytes(1, byteorder="little")]
                address = PublicKey.create_program_address(buffer, program_id)
            except Exception:
                nonce -= 1
                continue
            return address, nonce
        raise KeyError("Unable to find a viable program address nonce")

    @staticmethod
    def _is_on_curve(pubkey_bytes):
        """Verify the point is on curve or not."""
        try:
            decodepoint(pubkey_bytes)
            return True
        except:
            return False