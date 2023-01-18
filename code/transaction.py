from collections import defaultdict
import pprint
import ast
import traceback
from .util import *
from .category import Category
from sortedcontainers import SortedDict

class Transfer:
    #transfer categories used in classifier
    SENT = 0
    RECEIVED = 1
    MINTED = 2
    FROM_BRIDGE = 3
    TO_BRIDGE = 4
    UNSTAKED_LP = 5
    BURNED = 6
    ZERO_VALUED = 8
    STAKED_LP = 9
    NFT_IN = 10
    NFT_OUT = 11
    REDEEMED_LP = 12
    REWARDS_LP = 13
    ERROR = 14
    UNVAULTED = 15
    INTERACTED = 16
    MINTED_NFT = 17
    SELF = 18

    #synthetic transfer types fr, to, val, token, token_contract, token_nft_id
    SUSPECT_FROM = 1<<20
    SUSPECT_TO = 1<<21
    SUSPECT_AMOUNT = 1 << 22
    SUSPECT_WHAT = 1<<23
    SUSPECT_NFTID = 1<<24

    FEE = 1
    WRAP = 2
    REBASE = 3
    MISSED_MINT = 4
    ARBITRUM_BRIDGE = 5

    name_map = {
        SENT:'sent',
        RECEIVED:'received',
        MINTED:'minted',
        FROM_BRIDGE:'from bridge',
        TO_BRIDGE:'to bridge',
        UNSTAKED_LP:'unstaked',
        BURNED:'burned',
        ZERO_VALUED:'zero-valued',
        STAKED_LP:'staked',
        REDEEMED_LP:'redeemed',
        NFT_IN:'NFTs received',
        NFT_OUT:'NFTs sent',
        REWARDS_LP:'rewards',
        ERROR:'error',
        UNVAULTED:'unvaulted',
        INTERACTED: 'interacted',
        MINTED_NFT: 'minted NFT',
        SELF:'self-transfer'

    }

    ALL_FIELDS = ['type','from_me', 'fr', 'to_me', 'to', 'amount', 'what', 'symbol', 'coingecko_id', 'input_len', 'rate_found', 'rate', 'rate_source', 'free','treatment', 'input','amount_non_zero','input_non_zero','outbound','id','token_nft_id','vault_id','synthetic']
    def __init__(self, id, type, from_me, fr, to_me, to, val, token_contract,token_name,coingecko_id, token_nft_id, input_len, rate_found, rate, rate_source, base_fee, input=None, treatment = None, outbound=False, self_transfer=False,
                 synthetic=False,vault_id=None, custom_treatment=None, custom_rate=None, custom_vaultid=None):
        if val is None or val == '':
            val = 0
        if ',' in str(val):
            val = float(val.replace(",",""))
        self.type = type
        self.from_me = from_me
        self.fr = fr
        self.to_me = to_me
        self.to = to
        self.amount = val
        self.amount_non_zero = val > 0
        self.what = token_contract
        self.coingecko_id = coingecko_id
        self.symbol = token_name
        self.input_len = input_len
        self.input_non_zero = input_len > 2
        self.rate_found = rate_found
        self.rate = rate
        self.rate_source = rate_source
        self.free = base_fee == 0
        self.treatment = treatment
        self.input = input
        self.outbound = outbound
        self.token_nft_id = token_nft_id
        self.id=id
        self.synthetic = synthetic
        self.vault_id=vault_id
        self.custom_treatment = custom_treatment
        self.custom_rate = custom_rate
        self.custom_vaultid = custom_vaultid
        self.self_transfer = self_transfer


    def __getitem__(self,key):
        return getattr(self,key)

    def to_dict(self):
        dct = {}
        for f in Transfer.ALL_FIELDS:
            dct[f] = self[f]
        return dct

    def __str__(self):
        return str(self.to_dict())

    def __repr__(self):
        return self.__str__()

    def set_default_vaultid(self,cp_name):
        if self.outbound:
            adr = self.to
        else:
            adr = self.fr

        if cp_name == adr:
            self.vault_id = cp_name[:6]
        else:
            if adr is not None:
                self.vault_id = cp_name[:6] + " " + adr[:6]
            else:
                self.vault_id = 'Network'




class Transaction:
    IGNORE = 0
    BURN = 1
    SELL = 2
    BUY = 3
    GIFT = 4
    # MAPPED_FIELDS = {'fr': 2, 'to': 3, 'amount': 4, 'what': 5, 'type': 1, 'rate_found': 8, 'free':10, 'symbol':6,'amount_non_zero':}
    MAPPED_FIELDS = ['from_me', 'fr', 'to_me', 'to', 'amount','what','type','rate_found','free','symbol','amount_non_zero','input_non_zero']
    # ALL_FIELDS = {'index':0, 'type':1, 'from':2, 'to':3, 'amount':4, 'what':5, 'input_len':6, 'rate_found':7, 'rate':8,'free':9}

    def __init__(self, user, chain, hash=None, ts=None, block=None,nonce=None, txid = None, custom_type_id=None, custom_color_id = None, custom_note=None, manual=None):
        self.hash = hash
        self.ts = ts
        self.type = None
        self.block = block
        self.nonce=nonce
        self.grouping = []
        self.chain = chain
        self.main_asset = chain.main_asset
        self.user = user
        self.total_fee = None
        self.fee_transfer = None
        self.combo = None
        self.transaction_value = None
        self.classification_certainty_level = 0
        self.rate_inferred = False
        # self.rate_adjusted = False
        # self.local_rates = defaultdict(dict)
        self.balanced = False
        self.txid=txid
        self.custom_type_id=custom_type_id
        self.custom_color_id = custom_color_id
        self.custom_note = custom_note
        if manual == 1:
            self.manual = 1
        else:
            self.manual = 0
        self.interacted = None
        self.function = None

        self.derived_data = None
        self.success = None



    def append(self,cl,row, transfer_id=None, custom_treatment=None, custom_rate=None, custom_vaultid=None, synthetic=0, derived=None, prepend=False):
        hash, ts, nonce, block = row[0:4]
        if hash == self.chain.hif:
            log('Add row to tx',hash,cl,row,'callstack',traceback.format_stack(),filename='specific_tx.txt')
        self.hash = hash
        self.ts = ts
        if nonce is not None:
            self.nonce = int(nonce)
        if block is not None:
            self.block = int(block)

        if prepend:
            self.grouping.insert(0,[cl, row, transfer_id, custom_treatment, custom_rate, custom_vaultid, synthetic, derived])
        else:
            self.grouping.append([cl,row, transfer_id,custom_treatment,custom_rate,custom_vaultid,synthetic, derived])





    # def get_solana_cp(self,user, input_len, input):
    #     if input_len == 100:  # input is counterparty
    #
    #         cp_str, input_str = input.split(":")
    #         counterparty_list = ast.literal_eval(cp_str)
    #         if len(counterparty_list) > 0:
    #             sig = None
    #             cp_addr = counterparty_list[0]
    #             prog_name, prog_addr, editable = self.chain.get_progenitor_entity(cp_addr)
    #             log("finalize, solana", self.hash, input, prog_name)
    #             if prog_name is None:
    #                 prog_name = cp_addr[:12]
    #             input_list = ast.literal_eval(input_str)
    #             sig = ','.join(input_list)
    #             return {cp_addr:[prog_name, sig, sig, True, cp_addr]}
    #     return {}
                # counter_parties[cp_addr] = [prog_name, sig, sig, True, cp_addr]

    def finalize(self,coingecko_rates, signatures):
        self.total_fee = 0
        self.transfers = SortedDict()
        counter_parties = {}
        potentates = {}

        amounts = defaultdict(float)
        dd = self.derived_data


        self.mappings = {}
        for key in Transaction.MAPPED_FIELDS:#.keys():
            self.mappings[key] = defaultdict(list)

        tx_input = None
        for _,(type,sub_data,id,custom_treatment,custom_rate,custom_vaultid,synthetic,derived) in enumerate(self.grouping):
            hash, ts, nonce, block, fr, to, val, token, token_contract, token_nft_id, base_fee, input_len,input = sub_data
            fr = normalize_address(fr)
            to = normalize_address(to)
            self.hash = hash
            self.ts = ts
            if block is not None:
                self.block = block

            if token_contract is None:
                token_contract = self.main_asset

            assert id is not None

            # if id is not None:
            #     index = id

            # rate_found, rate, rate_source = coingecko_rates.lookup_rate(token_contract, ts)
            # coingecko_rates.verbose=True
            # log('finalize',hash,dd)
            if dd is not None:
                coingecko_id, rate_found, rate, rate_source = derived['coingecko_id'], derived['rate_found'], derived['rate'], derived['rate_source']
            else:
                coingecko_id = coingecko_rates.lookup_id(self.chain.name,token_contract)
                # print("lookup_id",self.chain.name, token_contract, coingecko_id)
                rate_found, rate, rate_source = coingecko_rates.lookup_rate(self.chain.name,token_contract, ts) #can't use custom rates here because they'll get saved into derived data
                # rate_found, rate, rate_source = self.lookup_rate(user,coingecko_rates,lookup_rate_contract,ts)


            # log("RATE LOOKUP",self.hash,token_contract,ts,rate_found,rate)
            # transfer = [index, type, fr, to, val, token_contract, input_len, rate_found, rate,base_fee == 0]
            # transfer = {'type':type, 'from':fr, 'to':to, 'amount':val, 'what':token_contract,'input_len':input_len, 'rate_found':rate_found, 'rate':rate}
            decustomed_input, is_custom_op = decustom(input)
            if not is_custom_op:
                passed_input = None
            else:
                passed_input = input

            self_transfer = False
            from_me = self.my_address(fr)
            to_me = self.my_address(to)
            # if to in self.user.relevant_addresses and fr in self.user.relevant_addresses:
            if from_me and to_me:
                self_transfer = True

            # outbound = not self_transfer and fr in self.user.relevant_addresses
            outbound = not to_me and from_me



            # fr_id = self.user.all_addresses[fr][self.chain.name]['id']
            # to_id = self.user.all_addresses[to][self.chain.name]['id']

            transfer = Transfer(id, type, from_me, fr, to_me, to, val, token_contract, token, coingecko_id, token_nft_id, input_len, rate_found, rate, rate_source,base_fee, outbound = outbound, self_transfer=self_transfer,
                                custom_treatment=custom_treatment, custom_rate=custom_rate, custom_vaultid=custom_vaultid, input=passed_input,synthetic=synthetic)
            if dd is not None:
                transfer.derived_data = derived
            if transfer.synthetic in [transfer.MISSED_MINT, transfer.REBASE]:
                transfer.treatment = 'gift'

            self.transfers[id] = transfer

            log("tx hash",self.hash,_,input_len,input,"transfer conditions",transfer.synthetic, Transfer.FEE, self_transfer, dd is None, self.chain)

            # if self.chain.name == 'Solana':
            #     counter_parties.update(self.get_solana_cp(self.user, input_len, input))

            if transfer.synthetic != Transfer.FEE: #mostly ignore fee transfer
                if self.chain.name == 'Solana' and transfer.what == 'SOL' and transfer.amount < 0.03: #ignore SOL dust
                    pass
                else:
                    for key in Transaction.MAPPED_FIELDS:
                        self.mappings[key][transfer[key]].append(id)

                    if not self_transfer: #don't lookup counterparties for self transfer
                        if val != 0:
                            # if fr in self.user.relevant_addresses:
                            if self.my_address(fr):
                                amounts[token_contract] -= val

                            # if to in self.user.relevant_addresses:
                            if self.my_address(to):
                                amounts[token_contract] += val

                if not self_transfer:
                    if dd is None:
                        if self.chain.name != 'Solana':
                            for addr in [fr, to]:
                                # if addr not in user.relevant_addresses and addr != '0x0000000000000000000000000000000000000000' and addr[:2].lower() == '0x':
                                if not self.my_address(addr) and addr != '0x0000000000000000000000000000000000000000' and addr[:2].lower() == '0x':
                                    # prog_addr, prog_name, editable = self.chain.get_progenitor_name(addr)
                                    prog_name, prog_addr = self.chain.get_progenitor_entity(addr)
                                    if prog_addr is None or prog_addr == 'None':
                                        prog_addr = addr
                                    if self.chain.hif == self.hash:
                                        log("Looked up progenitor for", addr, "got", prog_addr, prog_name,)

                                    if input_len > 2:
                                        tx_input = input
                                    #     decoded_sig, sig = signatures.lookup_signature(input)
                                    #     # if prog_name is not None:
                                    #     if prog_name is None:
                                    #         prog_name = 'UNKNOWN'
                                    #     counter_parties[prog_addr] = [prog_name, sig, decoded_sig, editable, addr]
                                    #     cp_found = True
                                    # else:
                                    if prog_name is not None:
                                        potentates[prog_addr] = [prog_name,None,None, 1, addr]
                                        # cp_found = True

                    if self.chain.name == 'Solana' and input_len == 200: #input is nft address
                        transfer.input = input
                        log("setting input to",input)


            else:
                # transfer.treatment = 'burn'
                self.total_fee = transfer.amount
                self.fee_transfer = transfer


        if dd is not None:
            if dd['cp_progenitor'] is not None:
                counter_parties[dd['cp_progenitor']] = [dd['cp_name'],dd['sig_hex'],dd['sig_decoded'],1,dd['cp_address']]
        else:
            if self.interacted is not None :
                prog_name, prog_addr = self.chain.get_progenitor_entity( self.interacted)
                if prog_name is None:
                    prog_name = 'unknown'
                if prog_addr is None:
                    prog_addr = self.interacted
                decoded_sig, sig = None, None

                if self.function is not None:
                    if self.function[:2] == '0x':#sometimes it's just plain wrong
                        decoded_sig, unique, sig = signatures.lookup_signature(self.function)
                    else:
                        decoded_sig, sig = self.function, self.function

                if tx_input is not None:
                    decoded_sig_cand, unique, sig_cand = signatures.lookup_signature(tx_input)
                    if unique or decoded_sig is None:
                        decoded_sig, sig = decoded_sig_cand, sig_cand
                        self.function = decoded_sig
                counter_parties[prog_addr] = [prog_name, sig, decoded_sig, 1, self.interacted]

                #if we interacted with a token, it's probably a transfer, and not a useful counterparty
                if self.interacted in self.chain.transferred_tokens and (decoded_sig is None or 'transfer' in decoded_sig.lower()) and len(potentates) > 0:
                    if self.interacted in potentates:
                        del potentates[self.interacted]
                    counter_parties = potentates

            elif self.manual:
                prog_name, prog_addr = self.chain.get_progenitor_entity('0xmanual')
                if prog_name is None:
                    prog_name = 'Manual transaction'
                if prog_addr is None:
                    prog_addr = '0xmanual'
                decoded_sig, sig = None, None
                if self.function is not None:
                    decoded_sig, sig = self.function, self.function
                counter_parties[prog_addr] = [prog_name, sig, decoded_sig, 1, prog_addr]

        if len(counter_parties) > 1: #remove unknowns
            new_cps = {}
            for prog_addr,cp_data in counter_parties.items():
                if cp_data[0] is not None and cp_data[0].lower() != 'unknown':
                    new_cps[prog_addr] = cp_data
            counter_parties = new_cps

            # if len(counter_parties) == 0:
            #     counter_parties.update(potentates)


        log("finalizing",self.hash,counter_parties)

        self.counter_parties = counter_parties

        cp_name = "unknown"
        if len(self.counter_parties):
            cp_name = list(self.counter_parties.values())[0][0]
        for transfer in self.transfers.values():
            transfer.set_default_vaultid(cp_name)


        # _, self.main_asset_rate, rate_source = coingecko_rates.lookup_rate(self.main_asset, self.ts)
        # _, self.main_asset_rate, self.main_asset_rate_source = self.lookup_rate(user, coingecko_rates, self.main_asset, self.ts)

        self.amounts = dict(amounts)

        out_cnt = 0
        in_cnt = 0
        for k, v in self.amounts.items():
            if v > 0:
                in_cnt += 1
            if v < 0:
                out_cnt += 1
        self.in_cnt = in_cnt
        self.out_cnt = out_cnt




    #finds all matching transfers by a dictionary of AND-ed field=value pairs
    def lookup(self,fv_pairs, count_only=False):
        if self.hash == self.chain.hif:
            log('transfer lookup',fv_pairs)

        matching_ids = None
        for field, value in fv_pairs.items():
            # if self.hash == self.chain.hif:
            #     log('transfer lookup 2',field,value)
            assert field in Transaction.MAPPED_FIELDS
            mapping = self.mappings[field]
            if isinstance(value,list) or isinstance(value,set): #find everyone that's in the list
                subset = set()
                value_list = value

                for val in mapping.keys():
                    # if self.hash == self.chain.hif:
                    #     log('transfer lookup 21 field',field, 'checking val', val, len(mapping[val]), 'against',value_list)
                    if val in value_list:
                        subset = subset.union(mapping[val])
                # print('mapping',mapping,'val list',value_list,"SUBSET",subset)
                # if self.hash == self.chain.hif:
                #     log('transfer lookup 2 res 1', subset)
                if matching_ids is None:
                    matching_ids = subset
                else:
                    matching_ids = matching_ids.intersection(subset)
            else:
                if value not in mapping:
                    matching_ids = set()
                    break
                if matching_ids is None:
                    matching_ids = set(mapping[value])
                else:
                    matching_ids = matching_ids.intersection(mapping[value])
                # if self.hash == self.chain.hif:
                #     log('transfer lookup 2 res 2', mapping[value])
            # if self.hash == self.chain.hif:
            #     log('transfer lookup 3 running res', matching_ids)
            if len(matching_ids) == 0:
                break
        if count_only:
            return len(matching_ids)
        outs = []
        for id in matching_ids:
            outs.append(self.transfers[id])
        return outs

    def tval(self,transfer, field):
        return transfer[Transaction.ALL_FIELDS[field]]


    def __str__(self):
        if self.hash is not None:
            rv = "HASH:"+str(self.hash)+", TIMESTAMP:"+str(self.ts)
            for transfer in self.transfers.values():
                rv += str(transfer)+"\n"
        else:
            return str(self.grouping)
        return rv

    def __repr__(self):
        return self.__str__()

    def my_address(self,address):
        return self.user.check_user_address(self.chain.name,address)

    def get_contracts(self):
        contract_list = set()
        counterparty_list = set()
        input_list = set()


        for type,sub_data,_,_,_,_,_,_ in self.grouping:
            hash, ts, nonce, block, fr, to, val, token, token_contract, token_nft_id, base_fee, input_len, input = sub_data
            # log('get_contracts token_contract',token_contract,'transaction id',self.txid,'hash',hash)
            if token_contract is not None:
                contract_list.add(token_contract)
            if self.chain.name != 'Solana':
                if input_len > 2: #ignore 0x
                    if input is not None:
                        input_list.add(input)
                    # if to not in self.user.relevant_addresses:
                    if not self.my_address(to):
                        counterparty_list.add(to)

                    if not self.my_address(fr):
                        counterparty_list.add(fr)
                        # log("counter", hash, fr, input_len)
            if self.chain.name == 'Solana':
                if self.interacted is not None:
                    counterparty_list = [self.interacted]
                if self.function is not None:
                    input_list = self.function.split(",")
                # if input_len == 100:
                #     cp_str,input_str = input.split(":")
                #     counterparty_list = ast.literal_eval(cp_str)
                #     input_list = ast.literal_eval(input_str)
                #     log('get_contracts',self.hash,counterparty_list,input_list)


        return contract_list, counterparty_list, input_list


    def infer_and_adjust_rates(self,user, coingecko_rates, skip_adjustment=False):
        do_print = False
        if self.hash == self.chain.hif:
            do_print = True
            log("infer and adjust rates for tx", self.txid)
            log("transaction",self)

        if not self.balanced:
            if do_print:
                log("tx not balanced")
            return

        in_cnt = 0
        out_cnt = 0
        amounts = defaultdict(float)
        symbols = {}



        for transfer in self.transfers.values():
            if do_print:
                log("Proc transfer",transfer)
            if transfer.synthetic == Transfer.FEE:
                continue
            val = transfer.amount
            lookup_contract = transfer.what
            if transfer.type == 5: #multi-tokens are too different from each other to assume all same
                lookup_contract = transfer.what + "_"+str(transfer.token_nft_id)
            # log('lookup_contract',lookup_contract,transfer.token_nft_id)

            if val > 0:
                if transfer.treatment == 'buy':
                    in_cnt += 1
                    amounts[lookup_contract] += val
                    symbols[lookup_contract] = {'symbol': transfer.symbol, 'rate': transfer.rate, 'rate_found': transfer.rate_found, 'rate_source':transfer.rate_source}
                elif transfer.treatment == 'sell':
                    out_cnt += 1
                    amounts[lookup_contract] -= val
                    symbols[lookup_contract] = {'symbol': transfer.symbol, 'rate': transfer.rate, 'rate_found': transfer.rate_found, 'rate_source':transfer.rate_source}
                if transfer.type in [4,5]:
                    skip_adjustment = True
        if do_print:
            log('infer_and_adjust_rates symbols',symbols)
            log('infer_and_adjust_rates amounts', amounts)
        combo = (out_cnt, in_cnt)



        if combo[0] > 0 and combo[1] > 0:

            # print(hash)
            add_rate_for = None
            bad_out = 0
            bad_in = 0
            iffy_out = 0
            iffy_in = 0
            good_count = 0
            unaccounted_total = 0
            unaccounted_total_iffy = 0
            total_in = 0
            total_in_iffy = 0
            total_out = 0
            total_out_iffy = 0
            worst_inferrer = 1
            for contract, amt in amounts.items():
                good = symbols[contract]['rate_found']
                rate = symbols[contract]['rate']
                # source = symbols[contract]['rate_source']
                # if rate == 0 or rate is None:
                #     good = 0
                try:
                    rate = float(rate)
                except:
                    good = 0
                    rate = 0
                # good, rate = coingecko_rates.lookup_rate(contract, ts)
                if do_print:
                    log("Rate lookup result",contract,symbols[contract],good,rate)
                if good == 0:
                    if amt <= 0:
                        bad_out += 1
                        bad_contract = contract
                        bad_total = -amt
                    if amt >= 0:
                        bad_in += 1
                        bad_contract = contract
                        bad_total = amt
                else:
                    if good < worst_inferrer:
                        worst_inferrer = good
                    unaccounted_total += rate * amt
                    if amt > 0:
                        total_in += rate * amt
                    else:
                        total_out -= rate * amt

                if good < 1:
                    if amt < 0:
                        iffy_out += 1
                        iffy_contract = contract
                        iffy_total = -amt
                    if amt > 0:
                        iffy_in += 1
                        iffy_contract = contract
                        iffy_total = amt
                else:
                    unaccounted_total_iffy += rate * amt

                if good >= 1:
                    good_count += 1

            unaccounted_total = abs(unaccounted_total)
            unaccounted_total_iffy = abs(unaccounted_total_iffy)



            #if there's one really bad rate, infer that (including from iffy rates)
            #if there are no really bad rates, and one iffy rate, infer that instead
            if bad_in + bad_out == 1:
                add_rate_for = bad_contract
            elif bad_in + bad_out == 0 and iffy_in + iffy_out == 1:
                worst_inferrer = 1
                add_rate_for = iffy_contract
                bad_total = iffy_total
                unaccounted_total = unaccounted_total_iffy

            if self.hash == self.chain.hif:
                log('stats',bad_in,bad_out,iffy_in,iffy_out, add_rate_for)

            if add_rate_for:
                if do_print:
                    log('add_rate_for', add_rate_for,'unaccounted_total',unaccounted_total,'bad_total',bad_total, 'rate',unaccounted_total / bad_total)


            if add_rate_for is not None:
                try:
                    symbol = symbols[add_rate_for]['symbol']
                    # print("ADDING INFERRED RATE",add_rate_for,symbol,ts,self.transaction_value,unaccounted_total,bad_total)
                    inferred_rate = unaccounted_total / bad_total
                    # self.local_rates[add_rate_for][self.ts] = inferred_rate
                    # coingecko_rates.add_rate(add_rate_for,symbol,ts,unaccounted_total/bad_total)
                    self.rate_inferred = symbol
                    if worst_inferrer == 1:
                        rate_source = "inferred"
                    else:
                        rate_source = "inferred from " + str(worst_inferrer)

                    lookup_what = add_rate_for
                    if '_' in add_rate_for:
                        lookup_what = add_rate_for[:add_rate_for.index('_')]

                    if do_print:
                        log("lookup_what",lookup_what)
                    for transfer in self.lookup({'what':lookup_what}):
                        transfer.rate = inferred_rate
                        transfer.rate_found = worst_inferrer
                        transfer.rate_source = rate_source
                        if do_print:
                            log("changing rate ",self.hash,transfer)



                    # if self.type not in ['remove liquidity', 'add liquidity']:
                    coingecko_rates.add_rate(self.chain.name, add_rate_for, self.ts, inferred_rate, worst_inferrer, rate_source)

                except:
                    log('EXCEPTION','contract', add_rate_for)
                    log(traceback.format_exc())
                    log(self)
                    exit(0)


            #don't adjust rates for receipt tokens
            # if self.type in ['remove liquidity','add liquidity']:
            #     return

            if bad_in + bad_out == 0 and add_rate_for is None and not skip_adjustment and total_out > 0 and total_in > 0:

            # if good_count == len(self.amounts):
                total_avg = (total_in + total_out) / 2.
                try:
                    mult_adjustment_in = total_avg / total_in
                except:
                    # print("ADJUSTMENT FAIL",traceback.format_exc())
                    # print('bad inout',bad_in,bad_out)
                    # print(self)
                    exit(1)
                mult_adjustment_out = total_avg / total_out
                adjustment_factor = abs(mult_adjustment_in - 1)
                # if self.hash.lower() == '0x6833b6ecf00860bb7d32048367305fc0c5d6ca156843e15d4a23c3cc262e1423':
                #     log('adjust', in_cnt, out_cnt,total_in,total_out)
                if adjustment_factor > 0.05:
                    rate_fluxes = []
                    for contract, amt in amounts.items():
                        good = symbols[contract]['rate_found']
                        rate = symbols[contract]['rate']
                        rate_pre_good = rate_pre_source = rate_aft_good = rate_aft_source = None
                        if contract == self.main_asset:
                            rate_flux = 0
                        else:
                            # rate_pre_good, rate_pre, rate_pre_source = self.lookup_rate(user,coingecko_rates,contract,int(self.ts) - 3600)
                            # rate_aft_good, rate_aft, rate_aft_source = self.lookup_rate(user, coingecko_rates, contract, int(self.ts) + 3600)
                            rate_pre_good, rate_pre, rate_pre_source = coingecko_rates.lookup_rate(self.chain.name,contract, int(self.ts) - 3600)
                            rate_aft_good, rate_aft, rate_aft_source = coingecko_rates.lookup_rate(self.chain.name,contract, int(self.ts) + 3600)


                            if rate_pre is None or rate_aft is None or rate_pre == 0:
                                rate_flux = 1
                                log("Couldn't find nearby rates, txid",self.txid,"hash",self.hash)
                                # return
                            else:
                                rate_flux = abs(rate_aft / rate_pre - 1)
                            # else: #if rate is inferred, assume it's much more likely to be the wrong one
                            #     rate_flux = 1-good
                            if good < 1:
                                rate_flux += (1-good)
                        rate_fluxes.append((contract, rate_flux, rate, amt))
                        # log(self.hash,contract,rate_flux,rate, rate_aft, rate_pre,amt,good)
                    max_flux = max(rate_fluxes, key=lambda t: t[1])
                    if do_print:
                        log('fluxes',rate_fluxes)

                    max_flux_contract = max_flux[0]
                    max_flux_amt = max_flux[3]
                    max_flux_rate = max_flux[2]
                    if max_flux_amt > 0:
                        adjusted_rate = (total_out - (total_in - max_flux_amt*max_flux_rate))/max_flux_amt
                        if do_print:
                            log('adjusted_rate (single 1)',max_flux_contract, adjusted_rate, total_out, total_in, max_flux_amt, max_flux_rate)
                    else:
                        max_flux_amt = -max_flux_amt
                        adjusted_rate = (total_in - (total_out - max_flux_amt * max_flux_rate)) / max_flux_amt
                        if do_print:
                            log('adjusted_rate (single 2)',max_flux_contract, adjusted_rate, total_in, total_out, max_flux_amt, max_flux_rate)

                    adjustment_factor = abs(max_flux_rate / adjusted_rate-1)
                    for transfer in self.lookup({'what': max_flux_contract}):
                        transfer.rate = adjusted_rate
                        transfer.rate_source += ", adjusted by " + str(adjustment_factor)
                else:
                    for transfer in self.lookup({'from_me': True}):
                        if transfer.treatment == 'sell':
                            transfer.rate *= mult_adjustment_out
                        # transfer.rate_source += ", adjusted by " + str(adjustment_factor)

                    for transfer in self.lookup({'to_me': True}):
                        if transfer.treatment == 'buy':
                            transfer.rate *= mult_adjustment_in
                        # transfer.rate_source += ", adjusted by " + str(adjustment_factor)

                    # if min_flux[3] > 0:
                    #     mult_adjustment_in = 1
                    #     mult_adjustment_out = total_in / total_out
                    # else:
                    #     mult_adjustment_in = total_out / total_in
                    #     mult_adjustment_out = 1


                # for contract, amt in amounts.items():
                #     rate = symbols[contract]['rate']
                #     local_adjustment = 1
                #     if amt < 0:
                #         local_adjustment = mult_adjustment_out
                #     if amt > 0:
                #         local_adjustment = mult_adjustment_in
                #     if local_adjustment != 1:
                #         adjusted_rate = rate * local_adjustment
                #         if do_print:
                #             log('adjusted rate',contract,rate,'->',adjusted_rate)
                #         for transfer in self.lookup({'what':contract}):
                #             transfer.rate = adjusted_rate
                #             transfer.rate_source += ", adjusted by "+str(local_adjustment)
                        # transfer.good_rate = 1
                        # user.add_rate(self.txid, transfer.index, adjusted_rate, 'adjusted', 1)
                    # self.local_rates[contract][self.ts] = adjusted_rate

                    # coingecko_rates.add_rate(self, contract, symbols[contract]['symbol'], self.ts, adjusted_rate)

                # self.rate_adjusted = adjustment_factor


    # def add_fee_transfer(self):
    #     # log("AFT")
    #     if self.total_fee != 0:
    #         treatment = 'fee'
    #         if len(self.transfers) == 1:
    #             treatment = 'loss'
    #         # log("AFT loss")
    #         # if isinstance(self.type, Category) and \
    #         #         (self.type.category in [Category.SWAP, Category.ADD_LIQUIDITY, Category.REMOVE_LIQUIDITY, Category.MINT_NFT, Category.CLAIM] or self.type.claim):
    #         #     treatment = 'fee'
    #             # log("AFT fee",self.type.category,self.type.claim)
    #
    #
    #         extra_transfer = Transfer(len(self.transfers),1, self.addr, None, self.total_fee, self.main_asset, self.main_asset, None, -1, 1, self.main_asset_rate, 'normal', 0, treatment=treatment, outbound=True, synthetic=True)
    #         self.transfers.append(extra_transfer)


    def type_to_typestr(self):
        type = self.type
        typestr = None
        nft = False
        if isinstance(type, Category):
            typestr = str(type)
            if type.nft:
                nft = True
        elif isinstance(type, list):
            typestr = 'NOT SURE:' + str(type)
        return nft,typestr

    def to_json(self):
        ts = self.ts
        counter_parties = self.counter_parties

        nft, typestr = self.type_to_typestr()





        js = {'txid':self.txid,'chain':self.chain.name,'type': typestr, 'ct_id':self.custom_type_id, 'nft':nft,'hash':self.hash,'ts':ts,'classification_certainty':self.classification_certainty_level,'counter_parties':counter_parties}

        if self.custom_color_id is not None:
            js['custom_color_id'] = self.custom_color_id

        if self.custom_note is not None:
            js['custom_note'] = self.custom_note

        if self.manual:
            js['manual'] = self.manual

        if hasattr(self,'protocol_note'):
            js['protocol_note'] = self.protocol_note

        rows = {}
        for trid, transfer in self.transfers.items():
            if transfer.amount != 0:
                row = transfer.to_dict()
                rows[trid] = row


        if self.hash == self.chain.hif:
            log("json transaction",rows)

        js['rows'] = rows


        return js




