from collections import defaultdict
import pprint
import traceback
from .util import *
from .category import Category

class Transfer:
    #transfer categories used in classifier
    SENT = 0
    RECEIVED = 1
    MINTED = 2
    FROM_BRIDGE = 3
    TO_BRIDGE = 4
    UNSTAKED_LP = 5
    BURNED = 6
    FEE = 7
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

    name_map = {
        SENT:'sent',
        RECEIVED:'received',
        MINTED:'minted',
        FROM_BRIDGE:'from bridge',
        TO_BRIDGE:'to bridge',
        UNSTAKED_LP:'unstaked',
        BURNED:'burned',
        FEE:'fee',
        ZERO_VALUED:'zero-valued',
        STAKED_LP:'staked',
        REDEEMED_LP:'redeemed',
        NFT_IN:'NFTs received',
        NFT_OUT:'NFTs sent',
        REWARDS_LP:'rewards',
        ERROR:'error',
        UNVAULTED:'unvaulted',
        INTERACTED: 'interacted',
        MINTED_NFT: 'minted NFT'

    }

    ALL_FIELDS = ['type', 'fr', 'to', 'amount', 'what', 'symbol', 'input_len', 'rate_found', 'rate', 'rate_source', 'free','treatment', 'input','amount_non_zero','input_non_zero','outbound','index','token_nft_id','vault_id']
    def __init__(self, index, type, fr, to, val, token_contract,token_name, token_nft_id, input_len, rate_found, rate, rate_source, base_fee, input=None, treatment = None, outbound=False,
                 synthetic=False,vault_id=None, custom_treatment=None, custom_rate=None, custom_vaultid=None):
        if val is None or val == '':
            val = 0
        self.type = type
        self.fr = fr
        self.to = to
        self.amount = val
        self.amount_non_zero = val > 0
        self.what = token_contract
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
        self.index=index
        self.synthetic = synthetic
        self.vault_id=vault_id
        self.custom_treatment = custom_treatment
        self.custom_rate = custom_rate
        self.custom_vaultid = custom_vaultid

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


class Transaction:
    IGNORE = 0
    BURN = 1
    SELL = 2
    BUY = 3
    GIFT = 4
    # MAPPED_FIELDS = {'fr': 2, 'to': 3, 'amount': 4, 'what': 5, 'type': 1, 'rate_found': 8, 'free':10, 'symbol':6,'amount_non_zero':}
    MAPPED_FIELDS = ['fr','to','amount','what','type','rate_found','free','symbol','amount_non_zero','input_non_zero']
    # ALL_FIELDS = {'index':0, 'type':1, 'from':2, 'to':3, 'amount':4, 'what':5, 'input_len':6, 'rate_found':7, 'rate':8,'free':9}

    def __init__(self, chain,txid = None, custom_type_id=None, custom_color_id = None, manual=None):
        self.hash = None
        self.type = None
        self.grouping = []
        self.chain = chain
        self.main_asset = chain.main_asset
        self.addr = chain.addr
        self.total_fee = None
        self.combo = None
        self.transaction_value = None
        self.classification_certainty_level = 0
        self.rate_inferred = False
        self.rate_adjusted = False
        # self.local_rates = defaultdict(dict)
        self.balanced = False
        self.txid=txid
        self.custom_type_id=custom_type_id
        self.custom_color_id = custom_color_id
        if manual == 1:
            self.manual = 1
        else:
            self.manual = 0

    def append(self,cl,row, transfer_idx=None, custom_treatment=None, custom_rate=None, custom_vaultid=None):
        self.grouping.append([cl,row, transfer_idx,custom_treatment,custom_rate,custom_vaultid])


    def lookup_rate(self,user,coingecko, token_contract, ts, tr_index):

        # if self.txid is not None:
        #     q = "SELECT rate, source, level FROM rates WHERE transaction_id="+str(self.txid)+" AND transfer_idx="+str(tr_index)+" ORDER BY level DESC"
        #     # log(q)
        #     rows = user.db.select(q)
        #     if len(rows) >= 1:
        #         rate, source_code, level = rows[0]
        #         return level, rate, user.rate_sources[source_code]
        # log("Looking up rate",token_contract,ts,coingecko.initialized)
        if coingecko.initialized:
            level, rate, source = coingecko.lookup_rate(token_contract, ts)
            # log("rate",level,rate,source)
            #transaction_id INTEGER, transfer_idx INTEGER, rate REAL, source INTEGER, level INTEGER
            # if self.txid is not None and source is not None:
            #     user.add_rate(self.txid, tr_index, rate, source, level)
                # user.db.insert_kw('rates',transaction_id=self.txid, transfer_idx=tr_index, rate=rate, source = sources.index(source), level=level)


            return level, rate, source

        return 0, None, None

    def finalize(self,user,coingecko_rates, signatures):
        self.total_fee = 0
        self.transfers = []
        counter_parties = {}
        potentates = {}

        amounts = defaultdict(float)


        self.mappings = {}
        for key in Transaction.MAPPED_FIELDS:#.keys():
            self.mappings[key] = defaultdict(list)


        for index,(type,sub_data,loaded_index,custom_treatment,custom_rate,custom_vaultid) in enumerate(self.grouping):
            hash, ts, fr, to, val, token, token_contract, token_nft_id, base_fee, input_len,input = sub_data
            self.hash = hash
            self.ts = ts

            if token_contract is None:
                token_contract = self.main_asset
            if loaded_index is not None:
                index = loaded_index

            # rate_found, rate, rate_source = coingecko_rates.lookup_rate(token_contract, ts)
            # coingecko_rates.verbose=True
            rate_found, rate, rate_source = self.lookup_rate(user,coingecko_rates,token_contract,ts,index)
            # log("RATE LOOKUP",self.hash,token_contract,ts,rate_found,rate)
            # transfer = [index, type, fr, to, val, token_contract, input_len, rate_found, rate,base_fee == 0]
            # transfer = {'type':type, 'from':fr, 'to':to, 'amount':val, 'what':token_contract,'input_len':input_len, 'rate_found':rate_found, 'rate':rate}
            decustomed_input, is_custom_op = decustom(input)
            if not is_custom_op:
                passed_input = None
            else:
                passed_input = input

            transfer = Transfer(index, type, fr, to, val, token_contract, token, token_nft_id, input_len, rate_found, rate, rate_source,base_fee, outbound = (fr.lower() == self.addr.lower()),
                                custom_treatment=custom_treatment, custom_rate=custom_rate, custom_vaultid=custom_vaultid, input=passed_input)
            self.transfers.append(transfer)
            for key in Transaction.MAPPED_FIELDS:#.keys():
                # self.mappings[key][transfer[Transaction.MAPPED_FIELDS[key]]].append(index)
                self.mappings[key][transfer[key]].append(index)

            if val != 0:
                if fr == self.addr:
                    amounts[token_contract] -= val
                if to == self.addr:
                    amounts[token_contract] += val
            self.total_fee += base_fee




            cp_found = False
            for addr in [fr, to]:
                if addr != self.addr and addr != '0x0000000000000000000000000000000000000000':
                    # prog_addr, prog_name, editable = self.chain.get_progenitor_name(addr)
                    prog_name, prog_addr, editable = self.chain.get_progenitor_entity(user,addr)
                    if prog_addr is None or prog_addr == 'None':
                        prog_addr = addr
                    # log("Looked up progenitor for", addr, "got", prog_addr, prog_name, editable)
                    if prog_name is not None:
                        if input_len > 0:
                            decoded_sig, sig = signatures.lookup_signature(input)
                            counter_parties[prog_addr] = (prog_name, sig, decoded_sig, editable, addr)
                        else:
                            potentates[prog_addr] = (prog_name,None,None, editable, addr)
                        cp_found = True

            if not cp_found and is_custom_op:
                counter_parties[addr] = ('UNKNOWN', decustomed_input, decustomed_input, 0, addr)

            if len(counter_parties) == 0:
                counter_parties = potentates
        self.counter_parties = counter_parties

        if len(self.counter_parties):
            cp_name = list(self.counter_parties.values())[0][0]
        else:
            cp_name = "UNKNOWN "
        for transfer in self.transfers:
            if transfer.outbound:
                transfer.vault_id = cp_name[:6] + " " + transfer.to[2:8]  # to
            else:
                transfer.vault_id = cp_name[:6] + " " + transfer.fr[2:8]  # fr


        # _, self.main_asset_rate, rate_source = coingecko_rates.lookup_rate(self.main_asset, self.ts)
        _, self.main_asset_rate, self.main_asset_rate_source = self.lookup_rate(user, coingecko_rates, self.main_asset, self.ts, -1)

        self.amounts = dict(amounts)

        out_cnt = 0
        in_cnt = 0
        out_tokens = set()
        in_tokens = set()
        for k, v in self.amounts.items():
            if v > 0:
                in_tokens.add(k)
                in_cnt += 1
            if v < 0:
                out_tokens.add(k)
                out_cnt += 1
        self.in_cnt = in_cnt
        self.out_cnt = out_cnt
        self.in_tokens = in_tokens
        self.out_tokens = out_tokens



    #finds all matching transfers by a dictionary of AND-ed field=value pairs
    def lookup(self,fv_pairs, count_only=False):
        matching_indexes = None
        for field, value in fv_pairs.items():
            assert field in Transaction.MAPPED_FIELDS
            mapping = self.mappings[field]
            if isinstance(value,list): #find everyone that's in the list
                subset = set()
                value_list = value
                for val in mapping.keys():
                    if val in value_list:
                        subset = subset.union(mapping[val])
                # print('mapping',mapping,'val list',value_list,"SUBSET",subset)
                if matching_indexes is None:
                    matching_indexes = subset
                else:
                    matching_indexes = matching_indexes.intersection(subset)
            else:
                if value not in mapping:
                    matching_indexes = set()
                    break
                if matching_indexes is None:
                    matching_indexes = set(mapping[value])
                else:
                    matching_indexes = matching_indexes.intersection(mapping[value])
            if len(matching_indexes) == 0:
                break
        if count_only:
            return len(matching_indexes)
        outs = []
        for index in matching_indexes:
            outs.append(self.transfers[index])
        return outs

    def tval(self,transfer, field):
        return transfer[Transaction.ALL_FIELDS[field]]


    def __str__(self):
        if self.hash is not None:
            rv = "HASH:"+str(self.hash)+", TIMESTAMP:"+str(self.ts)
            for transfer in self.transfers:
                rv += str(transfer)+"\n"
        else:
            return str(self.grouping)
        return rv

    def __repr__(self):
        return self.__str__()


    def get_contracts(self):
        contract_list = set()
        counterparty_list = set()
        input_list = set()
        # for transfer in self.transfers:
        #     if transfer.what is not None:
        #         contract_list.add(transfer.what)
        #     if transfer.input_len > 2:
        #         if transfer.input is not None:
        #             input_list.add(transfer.input)
        #         if transfer.outbound:
        #             counterparty_list.add(transfer.to)
        #         else:
        #             counterparty_list.add(transfer.fr)
        for type,sub_data,_,_,_,_ in self.grouping:
            hash, ts, fr, to, val, token, token_contract, token_nft_id, base_fee, input_len, input = sub_data
            if token_contract is not None:
                contract_list.add(token_contract)
            if input_len > 2: #ignore 0x
                if input is not None:
                    input_list.add(input)
                if to != self.addr:
                    counterparty_list.add(to)
                    # log("counter",hash,to,input_len)
                if fr != self.addr:
                    counterparty_list.add(fr)
                    # log("counter", hash, fr, input_len)
        return contract_list, counterparty_list, input_list


    # def calc_totals(self):
    #     total_fee = 0
    #     addr = self.addr
    #     # outs = defaultdict(float)
    #     # ins = defaultdict(float)
    #     amounts = defaultdict(float)
    #     amounts_read = defaultdict(float)
    #     for type, sub_data in self.grouping:
    #         hash, ts, fr, to, val, token, token_contract, base_fee, input_len = sub_data
    #
    #         if token_contract is None:
    #             token_contract = self.main_asset
    #
    #         vabs = None
    #         if val != 0:
    #             if fr == addr:
    #                 amounts[token_contract] -= val
    #                 amounts_read[token] -= val
    #                 vabs = -val
    #             if to == addr:
    #                 amounts[token_contract] += val
    #                 amounts_read[token] += val
    #                 vabs = val
    #             if vabs is None:
    #                 log("EXITING", fr, to, addr, sub_data)
    #                 exit(1)
    #         total_fee += base_fee
    #     self.amounts = dict(amounts)
    #     self.amounts_read = dict(amounts_read)
    #     self.total_fee = total_fee

    # def calc_usd_totals(self,coingecko_rates):
    #     try:
    #         ts = self.grouping[0][1][1]
    #     except:
    #         log("calc_usd_totals failed",self.grouping)
    #         exit(1)
    #     usd_in = 0
    #     usd_out = 0
    #     rate_in_good = 1
    #     rate_out_good = 1
    #     for contract,v in self.amounts.items():
    #
    #         rate_found,rate = coingecko_rates.lookup_rate(contract, ts)
    #         # print('rate',contract, v, ts, rate)
    #
    #         if v > 0:
    #             if rate is not None:
    #                 usd_in += rate * v
    #                 if not rate_found:
    #                     rate_in_good = 0
    #             else:
    #                 rate_in_good = 0
    #         else:
    #             if rate is not None:
    #                 usd_out -= rate*v
    #                 if not rate_found:
    #                     rate_out_good = 0
    #             else:
    #                 rate_out_good = 0
    #
    #     _, main_rate = coingecko_rates.lookup_rate(self.main_asset, ts)
    #     self.usd_fee = main_rate * self.total_fee
    #     self.transaction_value = 0
    #     if rate_in_good == rate_out_good:
    #         self.transaction_value = max(usd_in,usd_out)
    #     elif rate_in_good:
    #         self.transaction_value = usd_in
    #     else:
    #         self.transaction_value = usd_out
    #     # print('transaction_value',self.transaction_value,'fee',self.usd_fee, rate_in_good, rate_out_good)


    def infer_and_adjust_rates(self,user, coingecko_rates, skip_adjustment=False):
        if not self.balanced:
            return

        in_cnt = 0
        out_cnt = 0
        amounts = defaultdict(float)
        symbols = {}
        for transfer in self.transfers:
            val = transfer.amount
            if val > 0:
                if transfer.treatment == 'buy':
                    in_cnt += 1
                    amounts[transfer.what] += val
                    symbols[transfer.what] = {'symbol': transfer.symbol, 'rate': transfer.rate, 'rate_found': transfer.rate_found, 'rate_source':transfer.rate_source}
                elif transfer.treatment == 'sell':
                    out_cnt += 1
                    amounts[transfer.what] -= val
                    symbols[transfer.what] = {'symbol': transfer.symbol, 'rate': transfer.rate, 'rate_found': transfer.rate_found, 'rate_source':transfer.rate_source}
        if self.hash == self.chain.hif:
            log('infer_and_adjust_rates symbols',symbols)
        combo = (out_cnt, in_cnt)

        do_print = False
        if self.hash == self.chain.hif:
            do_print = True

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
                source = symbols[contract]['rate_source']
                if rate == 0 or rate is None:
                    good = 0
                # good, rate = coingecko_rates.lookup_rate(contract, ts)
                if do_print:
                    print("Rate lookup result",contract,symbols[contract],good,rate)
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
                print('stats',bad_in,bad_out,iffy_in,iffy_out, add_rate_for)

            if add_rate_for:
                if do_print:
                    print('add_rate_for', add_rate_for,'unaccounted_total',unaccounted_total,'bad_total',bad_total, 'rate',unaccounted_total / bad_total)


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
                    for transfer in self.lookup({'what':add_rate_for}):
                        transfer.rate = inferred_rate
                        transfer.rate_found = worst_inferrer
                        transfer.rate_source = rate_source
                        log("changing rate ",self.hash,transfer.index)
                        # user.add_rate(self.txid, transfer.index, inferred_rate, 'inferred', 1)
                    # coingecko_rates.add_rate(add_rate_for, symbols[add_rate_for]['symbol'], self.ts, inferred_rate)


                    # if self.type not in ['remove liquidity', 'add liquidity']:
                    coingecko_rates.add_rate(add_rate_for, self.ts, inferred_rate, worst_inferrer, rate_source)

                except:
                    print('EXCEPTION','contract', add_rate_for)
                    print(traceback.format_exc())
                    pprint.pprint(self)
                    exit(0)


            #don't adjust rates for receipt tokens
            # if self.type in ['remove liquidity','add liquidity']:
            #     return

            if bad_in + bad_out == 0 and add_rate_for is None and not skip_adjustment:

            # if good_count == len(self.amounts):
                total_avg = (total_in + total_out) / 2.
                try:
                    mult_adjustment_in = total_avg / total_in
                except:
                    print("ADJUSTMENT FAIL",traceback.format_exc())
                    print('bad inout',bad_in,bad_out)
                    print(self)
                    exit(1)
                mult_adjustment_out = total_avg / total_out
                adjustment_factor = abs(mult_adjustment_in - 1)
                # if self.hash.lower() == '0x6833b6ecf00860bb7d32048367305fc0c5d6ca156843e15d4a23c3cc262e1423':
                #     log('adjust', in_cnt, out_cnt,total_in,total_out)
                if adjustment_factor > 0.05:
                    rate_fluxes = []
                    for contract, amt in amounts.items():
                        good = symbols[contract]['rate_found']
                        if contract == self.main_asset:
                            sid1 = -2
                            sid2 = -3
                        else:
                            sid1 = -int(contract[-4:],16)
                            sid2 = -int(contract[-8:-4],16)


                        # _, rate_pre, _ = coingecko_rates.lookup_rate(contract, int(self.ts) - 3600)
                        _, rate_pre, _ = self.lookup_rate(user,coingecko_rates,contract,int(self.ts) - 3600,sid1)

                        rate = symbols[contract]['rate']
                        # _, rate_aft, _ = coingecko_rates.lookup_rate(contract, int(self.ts) + 3600)
                        _, rate_aft, _ = self.lookup_rate(user, coingecko_rates, contract, int(self.ts) + 3600, sid2)
                        if rate_pre is None or rate_aft is None:
                            log("Couldn't find nearby rates, txid",self.txid,"hash",self.hash,"sid1",sid1,"sid2",sid2)
                            return
                        rate_flux = abs(rate_aft / rate_pre - 1)
                        if good == 0.5: #if rate is inferred, assume it's much more likely to be the wrong one
                            rate_flux *= 100
                        rate_fluxes.append((contract, rate_flux, rate, amt))
                        # log(self.hash,contract,rate_flux,rate, rate_aft, rate_pre,amt,good)
                    min_flux = min(rate_fluxes, key=lambda t: t[1])
                    if min_flux[3] > 0:
                        mult_adjustment_in = 1
                        mult_adjustment_out = total_in / total_out
                    else:
                        mult_adjustment_in = total_out / total_in
                        mult_adjustment_out = 1


                for contract, amt in amounts.items():
                    rate = symbols[contract]['rate']
                    local_adjustment = 1
                    if amt < 0:
                        local_adjustment = mult_adjustment_out
                    if amt > 0:
                        local_adjustment = mult_adjustment_in
                    if local_adjustment != 1:
                        adjusted_rate = rate * local_adjustment
                        if do_print:
                            print('adjusted rate',contract,rate,'->',adjusted_rate)
                        for transfer in self.lookup({'what':contract}):
                            transfer.rate = adjusted_rate
                            transfer.rate_source += ", adjusted by "+str(local_adjustment)
                        # transfer.good_rate = 1
                        # user.add_rate(self.txid, transfer.index, adjusted_rate, 'adjusted', 1)
                    # self.local_rates[contract][self.ts] = adjusted_rate

                    # coingecko_rates.add_rate(self, contract, symbols[contract]['symbol'], self.ts, adjusted_rate)

                self.rate_adjusted = adjustment_factor


    def add_fee_transfer(self):
        # log("AFT")
        if self.total_fee != 0:
            treatment = 'fee'
            if len(self.transfers) == 1:
                treatment = 'loss'
            # log("AFT loss")
            # if isinstance(self.type, Category) and \
            #         (self.type.category in [Category.SWAP, Category.ADD_LIQUIDITY, Category.REMOVE_LIQUIDITY, Category.MINT_NFT, Category.CLAIM] or self.type.claim):
            #     treatment = 'fee'
                # log("AFT fee",self.type.category,self.type.claim)


            extra_transfer = Transfer(len(self.transfers),1, self.addr, None, self.total_fee, self.main_asset, self.main_asset, None, -1, 1, self.main_asset_rate, 'normal', 0, treatment=treatment, outbound=True, synthetic=True)
            self.transfers.append(extra_transfer)

    def to_json(self):
        ts = self.ts
        counter_parties = self.counter_parties

        type = self.type
        nft = False
        typestr = None
        if isinstance(type,Category):
            typestr = str(type)
            if type.nft:
                nft = True
        elif isinstance(type,list):
            typestr = 'NOT SURE:'+str(type)





        js = {'txid':self.txid,'type': typestr, 'ct_id':self.custom_type_id, 'nft':nft,'hash':self.hash,'ts':ts,'classification_certainty':self.classification_certainty_level,'counter_parties':counter_parties}

        if self.custom_color_id is not None:
            js['custom_color_id'] = self.custom_color_id

        if self.manual:
            js['manual'] = self.manual

        rows = []
        for t in self.transfers:
            if t.amount != 0:
                row = t.to_dict()
                rows.append(row)

        if self.hash == self.chain.hif:
            print("json transaction",rows)

        js['rows'] = rows

        return js





    # def to_rows(self,chain):
    #     rows = []
    #     #ID,Timestamp,Quote,Base,Side,Base amount,Quote Amount
    #     #JEX20191231-090001-569215F,1577836801,JPY,BTC-PERP,Sell,0.200000,159506.00
    #     tid = self.grouping[0][1][0]
    #     ts = self.grouping[0][1][1]
    #
    #     # tokens_in = []
    #     contracts_in = []
    #
    #     # tokens_out = []
    #     contracts_out = []
    #     # for token, val in self.amounts_read.items():
    #     #     if val > 0:
    #     #         tokens_in.append(token)
    #     #     if val < 0:
    #     #         tokens_out.append(token)
    #     token_names = {}
    #     for g in self.grouping:
    #         contract = g[1][6]
    #         if contract is not None:
    #             token_names[contract] = contract#g[1][5]
    #         else:
    #             token_names[g[1][5]] = g[1][5]
    #
    #     for contract, val in self.amounts.items():
    #         if val > 0:
    #             contracts_in.append(contract)
    #         if val < 0:
    #             contracts_out.append(contract)
    #     self.token_names = token_names
    #     self.contracts_in = contracts_in
    #     self.contracts_out = contracts_out
    #
    #     type = self.type
    #     if type == 'deposit' or type == 'deposit from bridge':
    #         row = [tid,ts,token_names[contracts_in[0]],'','deposit','',self.amounts[contracts_in[0]]]
    #         rows.append(row)
    #
    #     elif type == 'remove liquidity from vault' or type == 'remove liquidity [leveraged]':
    #         for contract_in in contracts_in:
    #             row = [tid, ts, token_names[contract_in], '', 'gift', '', self.amounts[contract_in]]
    #             rows.append(row)
    #
    #     elif type == 'withdrawal' or type == 'withdraw to bridge':
    #         row = [tid,ts,token_names[contracts_out[0]],'','withdrawal','',-self.amounts[contracts_out[0]]]
    #         rows.append(row)
    #
    #     elif type == 'add liquidity to vault' or type == 'add liquidity [leveraged]':
    #         for contract_out in contracts_out:
    #             row = [tid, ts, token_names[contract_out], '', 'burn', '', -self.amounts[contract_out]]
    #             rows.append(row)
    #
    #     elif type == 'add liquidity':
    #         for contract_in in contracts_in:
    #             for contract_out in contracts_out:
    #                 in_amt = self.amounts[contract_in] / len(contracts_out)
    #                 out_amt = -self.amounts[contract_out] / len(contracts_in)
    #                 row = [tid, ts, token_names[contract_out], token_names[contract_in], 'buy', in_amt, out_amt]
    #                 rows.append(row)
    #         # lp_contract = contracts_in[0]
    #         # lp_amt_per_asset = self.amounts[lp_contract] / len(contracts_out)
    #         # for contract_out in contracts_out:
    #         #     row = [tid, ts, token_names[contract_out], lp_contract, 'buy', lp_amt_per_asset, -self.amounts[contract_out]]
    #         #     rows.append(row)
    #
    #     elif type == 'remove liquidity':
    #         for contract_in in contracts_in:
    #             for contract_out in contracts_out:
    #                 in_amt = self.amounts[contract_in] / len(contracts_out)
    #                 out_amt = -self.amounts[contract_out] / len(contracts_in)
    #                 row = [tid, ts, token_names[contract_in], token_names[contract_out], 'sell', out_amt, in_amt]
    #                 rows.append(row)
    #         #
    #         # lp_contract = contracts_out[0]
    #         # lp_amt_per_asset = self.amounts[lp_contract] / len(contracts_in)
    #         # for contract_in in contracts_in:
    #         #     row = [tid, ts, token_names[contract_in], token_names[lp_contract], 'sell', -lp_amt_per_asset, self.amounts[contract_in]]
    #         #     rows.append(row)
    #
    #     elif type == 'swap':
    #         row = [tid, ts, token_names[contracts_out[0]], token_names[contracts_in[0]], 'buy', self.amounts[contracts_in[0]], -self.amounts[contracts_out[0]]]
    #         rows.append(row)
    #
    #     elif type == 'airdrop':
    #         row = [tid, ts, contracts_in[0], '', 'gift', '', self.amounts[contracts_in[0]]]
    #         rows.append(row)
    #
    #     elif type == 'stake':
    #         #ignore staking
    #         pass
    #
    #     elif type == 'unstake':
    #         #ignore staking
    #         pass
    #
    #     elif type == 'claim reward':
    #         for contract_in in contracts_in:
    #             row = [tid, ts, token_names[contract_in], '', 'gift', '', self.amounts[contract_in]]
    #             rows.append(row)
    #
    #     elif type == 'unstake with reward':
    #         for contract_in in contracts_in:
    #             if contract_in not in chain.stake_addresses and contract_in not in chain.lp_token_addresses:
    #                 row = [tid, ts, token_names[contract_in], '', 'gift', '', self.amounts[contract_in]]
    #                 rows.append(row)
    #     elif type == 'multi-transaction swap send':
    #         pass
    #     elif type == 'multi-transaction swap receive':
    #         source_addr = self.grouping[0][1][2]
    #         send = chain.swap_addresses[source_addr]
    #         row = [tid, ts, send.token_names[send.contracts_out[0]], token_names[contracts_in[0]], 'buy', self.amounts[contracts_in[0]], -send.amounts[send.contracts_out[0]]]
    #         rows.append(row)
    #
    #     elif type is None:
    #         # print("WARNING: UNCLASSIFIED TRANSACTION")
    #         # if len(contracts_in) > 0 and len(contracts_out) > 0:
    #         #     for contract_in in contracts_in:
    #         #         for contract_out in contracts_out:
    #         #             in_amt = self.amounts[contract_in] / len(contracts_out)
    #         #             out_amt = -self.amounts[contract_out] / len(contracts_in)
    #         #             row = [tid, ts, token_names[contract_out], token_names[contract_in], 'buy', in_amt, out_amt]
    #         #             rows.append(row)
    #         # else:
    #         if 1:
    #             for contract_in in contracts_in:
    #                 row = [tid, ts, token_names[contract_in], '', 'gift', '', self.amounts[contract_in]]
    #                 rows.append(row)
    #             for contract_out in contracts_out:
    #                 row = [tid, ts, token_names[contract_out], '', 'burn', '', -self.amounts[contract_out]]
    #                 rows.append(row)
    #
    #     if self.total_fee != 0:
    #         row = [tid+"-FEE",ts,self.main_asset,'','burn','',self.total_fee]
    #         rows.append(row)
    #     # print('rows')
    #     # print(rows)
    #     return rows

    #
    # def record(self,db):
    #     # self.db.create_table('transactions', 'id integer primary key autoincrement, chain, hash, timestamp, total_fee NUMERIC, '
    #     #                                      'in_cnt INTEGER, out_cnt INTEGER, category INTEGER, certainty INTEGER, claim INTEGER, nft INTEGER, '
    #     #                                      'rate_inferred, rate_adjusted, balanced INTEGER, interacted', drop=False)
    #     # self.db.create_table('transaction_transfers',
    #     #                      'id integer primary key autoincrement, transaction_id integer, type integer, from, to, amount, what, symbol, input, rate NUMERIC, rate_found, treatment',
    #     #                      drop=False)
    #     # self.db.create_table('transaction_amounts', 'transaction_id integer, contract, amount NUMERIC', drop=False
    #     # self.db.create_table('transaction_counterparties', 'transaction_id integer, address, progenitor_address, progenitor_name, signature, decoded_signature', drop=False)
    #     # self.db.create_table('transaction_local_rates', 'transaction_id integer, contract, timestamp, rate NUMERIC', drop=False)
    #     type = self.type
    #     category = None
    #     claim = 0
    #     nft = 0
    #     certainty = 0
    #
    #     if isinstance(type, Category):
    #         category = type.category
    #         claim = type.claim
    #         certainty = type.certainty
    #         if type.nft:
    #             nft = 1
    #     elif isinstance(type, list):
    #         category = 'NOT SURE:' + str(type)
    #
    #
    #     db.insert_kw('transactions',chain=self.chain.name, hash=self.hash,timestamp=self.ts, total_fee=self.total_fee, in_cnt=self.in_cnt, out_cnt = self.out_cnt,
    #                  category=category,certainty=certainty,claim=claim,nft=nft,
    #                  rate_inferred = self.rate_inferred, rate_adjusted=self.rate_adjusted, balanced=self.balanced, interacted=self.interacted)
    #
    #     row = db.select("SELECT id FROM transactions WHERE hash = '"+self.hash+"'")
    #     ts_id = row[0][0]
    #
    #     # self.db.create_table('transaction_transfers',
    #     #                      'id integer primary key autoincrement, transaction_id integer, type integer, from, to, amount, what, symbol, input, rate NUMERIC, rate_found, treatment',
    #     #                      drop=False)
    #     for transfer in self.transfers:
    #         db.insert_kw('transaction_transfers',transaction_id=ts_id, type=transfer.type, from_addr=transfer.fr, to_addr = transfer.to,
    #                      amount=transfer.amount, what=transfer.what, symbol=transfer.symbol, input=transfer.input, rate=transfer.rate,
    #                      rate_found=transfer.rate_found, treatment=transfer.treatment)


