from collections import defaultdict
from .transaction import Transaction, Transfer
from .util import *
import pprint
import traceback

class Pool:
    LIQUIDITY = 0
    STAKING = 1
    VAULT = 2

    mapping = {
        LIQUIDITY:"liquidity",
        STAKING:"staking",
        VAULT:"vault"
    }

    def __init__(self,pools,origin):
        self.addresses = set()
        self.deposited = defaultdict(float)
        self.receipts_issued = defaultdict(float)
        self.rewards_issued = defaultdict(float)

        self.in_symbols = {}
        self.out_symbols = {}

        self.type = Pool.LIQUIDITY
        self.origin = origin
        self.pools = pools
        self.stable = True

    def add(self,transfer):
        self.addresses.add(transfer.to)
        what = self.pools.unwrap(transfer.what)
        self.in_symbols[what] = transfer.symbol
        self.deposited[what] += transfer.amount

    def remove(self, transfer):
        what = self.pools.unwrap(transfer.what)
        self.deposited[what] -= transfer.amount
        # if self.deposited[transfer.what] < 0:
        #     print("WARNING, AMOUNT OF "+transfer.symbol+" IN POOL "+str(self)+" IS NOW "+str(self.deposited[transfer.what]))

    def issue_receipt(self, transfer):
        what = self.pools.unwrap(transfer.what)
        self.receipts_issued[what] += transfer.amount
        self.out_symbols[what] = transfer.symbol

    def issue_reward(self, transfer):
        what = self.pools.unwrap(transfer.what)
        self.rewards_issued[what] += transfer.amount

    def process_receipt(self, transfer):
        what = self.pools.unwrap(transfer.what)
        self.receipts_issued[what] -= transfer.amount

    def __str__(self):
        addresses = sorted(list(self.addresses))
        shorts = []
        for address in addresses:
            shorts.append(address[:5]+"..."+address[-3:])
        tr = self.origin.hash[:5]+"..."+self.origin.hash[-3:]
        st = ""
        if self.stable:
            st = " stablecoins"
        return "POOL:<"+Pool.mapping[self.type]+st+" in:"+ str(list(self.in_symbols.values()))+" receipt:"+str(list(self.out_symbols.values()))+", "+str(shorts)+", "+tr+">"

    def __repr__(self):
        return self.__str__()

    # def __hash__(self):
        # return self.id()
        # addresses = sorted(list(self.addresses))

    @classmethod
    def cls_id(cls, addresses, deposited, receipts_issued=None):
        addresses = sorted(list(addresses))
        deposited = sorted(list(deposited))

        if receipts_issued is not None:
            receipts_issued = sorted(list(receipts_issued))
        else:
            receipts_issued = []
        id = str(addresses+deposited+receipts_issued)
        return id


    def id(self):
        return Pool.cls_id(self.addresses, self.deposited.keys(), self.receipts_issued.keys())



class Pools:
    stable_list = ['USDT','USDC','DAI','TUSD']

    def __init__(self,chain):
        self.chain = chain
        self.pools = set()
        self.stable_pools = set()
        # self.map = {'A':defaultdict(set),'I':defaultdict(set),'O':defaultdict(set)}
        self.map = {
            'A': {
                Pool.LIQUIDITY:defaultdict(set),
                Pool.STAKING:defaultdict(set),
                Pool.VAULT:defaultdict(set)
            },
            'I': {
                Pool.LIQUIDITY: defaultdict(set),
                Pool.STAKING: defaultdict(set),
                Pool.VAULT: defaultdict(set)
            },
            'O': {
                Pool.LIQUIDITY: defaultdict(set),
                # Pool.STAKING: defaultdict(set)
            }
        }

        self.staking_pools = set()
        self.vaults = set()
        # self.map = {'A': {}, 'I': {}, 'O': {}}

    def __str__(self):
        rv = "POOL LIST:\n"
        for pool in list(self.pools):
            rv += str(pool)
            rv+='\n'
        return rv

    def __repr__(self):
        return self.__str__()

    # def is_receipt_token(self,what):
    #     return what in self.map['O']

    # def receipt_token_list(self):
    #     return list(self.map['O'][Pool.STAKING].keys())

    def pool_address_list(self, pool_type=None):
        return self.map_keys('A',pool_type=pool_type)

    def receipt_token_list(self, pool_type=None):
        return self.map_keys('O',pool_type=pool_type)

    def input_token_list(self, pool_type=None):
        return self.map_keys('I',pool_type=pool_type)

    def map_keys(self,map_type,pool_type=None):
        if pool_type is not None:
            return list(self.map[map_type][pool_type].keys())
        all_pools = []
        for pool_type in self.map[map_type].keys():
            all_pools.extend(list(self.map[map_type][pool_type].keys()))
        return all_pools

    def pool_list(self,map_type,entry,pool_type=None):
        if pool_type is not None:
            if entry in self.map[map_type][pool_type]:
                return self.map[map_type][pool_type]
            else:
                return set()

        all_pools = set()
        for pool_type in self.map[map_type].keys():
            if entry in self.map[map_type][pool_type]:
                all_pools = all_pools.union(self.map[map_type][pool_type][entry])
        return all_pools

    # def pool_list(self,map_type,pool_type=None,):


    def unwrap(self, what):
        return self.chain.unwrap(what)

    def matches(self,entry,map_type,pool_type=None):
        res = set()
        if pool_type is not None:
            if entry in self.map[map_type][pool_type]:
                res = self.map[map_type][pool_type][entry]
        else:
            for pool_type in self.map[map_type].keys():
                if entry in self.map[map_type][pool_type]:
                    res = res.union(self.map[map_type][pool_type][entry])
        return res

    def deposit(self, transaction, deposits, receipts=None):
        # print("DEPOSITING")
        # pprint.pprint(deposits)
        new_pool = False
        candidates = self.pools
        depositing_symbols = set()
        if transaction.hash == self.chain.hif:
            log("DEPOSITS", deposits)
            log("RECEIPTS", receipts)
        for deposit in deposits:
            what = self.unwrap(deposit.what)
            depositing_symbols.add(what)

            matches = self.matches(deposit.to,'A')
            if transaction.interacted:
                matches = matches.union(self.matches(transaction.interacted,'A'))

            if transaction.hash == self.chain.hif:
                log("matches1", matches)
            candidates = candidates.intersection(matches)
            if transaction.hash == self.chain.hif:
                log("candidates1", candidates)

            matches = self.matches(what,'I')
            if transaction.hash == self.chain.hif:
                log("matches2", matches)
            if deposit.symbol in Pools.stable_list:
                candidates = candidates.intersection(self.stable_pools.union(matches))
            else:
                candidates = candidates.intersection(matches)
                if transaction.hash == self.chain.hif:
                    log("candidates2", candidates)
            # if len(candidates) == 0:
            #     pool = Pool()
            #     self.pools.add(pool)
            #     break

        if len(candidates):
            if receipts is not None and len(receipts):
                candidates_w_receipts = candidates.copy()
                for receipt in receipts:
                    what = self.unwrap(receipt.what)
                    if what not in depositing_symbols:
                        matches = self.matches(what, 'O')
                        if len(matches):
                            candidates_w_receipts = candidates_w_receipts.intersection(matches)
                        else:
                            candidates_w_receipts = set()
                            break
                    # if what not in depositing_symbols and what in self.map['O']:
                    #     candidates_w_receipts = candidates_w_receipts.intersection(self.map['O'][what])
                    # else:
                    #     candidates_w_receipts = set()
                    #     break
                if len(candidates_w_receipts):
                    candidates = candidates_w_receipts



        selected = []
        for candidate in candidates:
            if len(candidate.in_symbols) == len(depositing_symbols) or candidate.stable:
                selected.append(candidate)

        if len(selected) == 0:
            if transaction.hash == self.chain.hif:
                log("CREATING POOL, candidates",candidates)
            pool = Pool(self,transaction)
            new_pool = True
            self.pools.add(pool)
        elif len(selected) == 1:
            pool = list(selected)[0]
            if transaction.hash == self.chain.hif:
                log("FOUND POOL",pool)
            # log("POOL FOUND",pool)
        elif len(selected) > 1:
            log("DEPOSIT: FOUND MULTIPLE POOLS, BAILING",transaction.hash)
            log(deposits)
            log(receipts)
            log(candidates)
            exit(1)

        depositing_receipt_token = False
        for deposit in deposits:
            what = self.unwrap(deposit.what)

            pool.add(deposit)
            # if what in self.map['O']:
            if len(self.matches(what,'O')):
                depositing_receipt_token = True


        # if '0xc2edad668740f1aa35e4d8f227fb8e17dca888cd' in pool.addresses:
        #     if receipts is not None and len(receipts) > 0:
        #         log('issuing receipt for 0xc2edad668740f1aa35e4d8f227fb8e17dca888cd',transaction.hash)

        if new_pool:


            if receipts is not None and len(receipts) > 0:
                for receipt in receipts:
                    what = self.unwrap(receipt.what)

                    if what not in depositing_symbols: #uniswap sometimes provides change?
                        pool.issue_receipt(receipt)
                        self.map['O'][pool.type][what].add(pool)
                        if what == 'ETH':
                            log("RECEIPT IS ETH",pool)
            else:
                if depositing_receipt_token:
                    pool.type = Pool.STAKING
                    self.staking_pools.add(pool)
                else:
                    pool.type = Pool.VAULT
                    self.vaults.add(pool)

            for deposit in deposits:
                what = self.unwrap(deposit.what)
                self.map['A'][pool.type][deposit.to].add(pool)
                self.map['I'][pool.type][what].add(pool)
                if deposit.symbol not in Pools.stable_list:
                    pool.stable = False


            if pool.stable:
                self.stable_pools.add(pool)
        else:
            if receipts is not None and len(receipts) > 0:
                for receipt in receipts:
                    what = self.unwrap(receipt.what)
                    if what in pool.receipts_issued:
                        pool.issue_receipt(receipt)
                    else:
                        pool.issue_reward(receipt)





    def withdraw(self,transaction,withdrawals,returned_receipts=None,pool_type=None):
        withdrawing_tokens = set()
        candidates = set()
        candidates_wide = set()
        for withdrawal in withdrawals:
            what = self.unwrap(withdrawal.what)
            withdrawing_tokens.add(what)

            matches_adr = self.matches(withdrawal.fr,'A',pool_type=pool_type)
            if transaction.interacted:
                matches_adr = matches_adr.union(self.matches(transaction.interacted,'A',pool_type=pool_type))
            matches_what = self.matches(what,'I',pool_type=pool_type)


            if withdrawal.symbol in Pools.stable_list:
                C = matches_adr.intersection(self.stable_pools.union(matches_what))
            else:
                C = matches_adr.intersection(matches_what)
            candidates = candidates.union(C)
            if transaction.hash == self.chain.hif:
                print("withdraw xi1", candidates)
            # candidates = candidates.intersection(self.map['A'][withdrawal.fr])
            # candidates = candidates.intersection(self.map['I'][withdrawal.what])


        if returned_receipts is not None and len(returned_receipts) > 0:
            ret_candidates = self.pools
            for receipt in returned_receipts:
                what = self.unwrap(receipt.what)
                matches_what = self.matches(what, 'O')
                # if transaction.chain.hif == transaction.hash:
                #     log('receipt map',self.map['O'])
                ret_candidates = ret_candidates.intersection(matches_what)
            candidates = candidates.union(ret_candidates)

        selected = []
        for candidate in candidates:
            if set(candidate.in_symbols.keys()).issubset(withdrawing_tokens) or candidate.stable:
                selected.append(candidate)

        if transaction.hash == self.chain.hif:
            print("withdraw xi2", selected)

        #liquidity>staking>vault for removals
        if len(selected) > 1:
            priority_map = {}
            for pool in selected:
                if pool.type not in priority_map:
                    priority_map[pool.type] = []
                priority_map[pool.type].append(pool)
            selected = priority_map[min(list(priority_map.keys()))]

            if transaction.hash == self.chain.hif:
                print("withdraw xi3", selected)

        if len(selected) == 1:
            pool = list(selected)[0]
            if transaction.hash == self.chain.hif:
                log("FOUND POOL",pool)


            # log("POOL FOUND", pool)
        elif len(selected) > 1:
            # return None
            log("WITHDRAW: FOUND MULTIPLE POOLS, BAILING")
            log("transaction",transaction)
            # log(withdrawals)
            # log(returned_receipts)
            log(selected)
            # log(self)
            # exit(1)
            return None
        else:
            log("WITHDRAW: POOL NOT FOUND, BAILING")
            log("transaction", transaction)
            log("receipts",returned_receipts)
            # log(withdrawals)
            # log(returned_receipts)
            log('candidates',candidates)
            # log(self)
            # exit(1)
            # log(self)
            return None
            # exit(1)
            # return None
            # log("POOL NOT FOUND")
            # exit(1)

        for withdrawal in withdrawals:
            what = self.unwrap(withdrawal.what)
            if what in pool.deposited:
                pool.remove(withdrawal)
            else:
                pool.issue_reward(withdrawal)

        if returned_receipts is not None:
            for receipt in returned_receipts:
                pool.process_receipt(receipt)

        return pool


        # pool = self.map['A'][address]
        #
        # for what, amount in out_pairs.items():
        #     pool.remove(what,amount)
        #
        # if in_pairs is not None:
        #     for what, amount in in_pairs.items():
        #         pool.process_receipt(what,amount)



    def add_liquidity(self, transaction, ignore_tokens=()):
        deposits = []
        receipts = []
        for transfer in transaction.transfers:
            if transfer.what in ignore_tokens:
                continue
            if transfer.amount > 0:
                if transfer.fr == transaction.addr:
                    deposits.append(transfer)
                else:
                    receipts.append(transfer)
        self.deposit(transaction,deposits,receipts)

        # print("added liquidity",deposits)
        # print(self.map['A'])

    def remove_liquidity(self,transaction, ignore_tokens=(),pool_type=None):
        withdrawals = []
        receipts = []
        for transfer in transaction.transfers:
            if transfer.what in ignore_tokens:
                continue
            if transfer.amount > 0:
                if transfer.to == transaction.addr:
                    withdrawals.append(transfer)
                else:
                    receipts.append(transfer)

        pool = self.withdraw(transaction,withdrawals, receipts,pool_type=pool_type)
        # try:
        #     pool = self.withdraw(withdrawals, receipts)
        # except:
        #     log(transaction)
        #     exit(1)
        if pool is not None:
            return 1
        else:
            # log("FAILED TO WITHDRAW", traceback.format_exc())
            # log(transaction)
            # log("CURRENT MAP", self.map['A'])
            return 0



    #
    # def stake(self, address, what, amount):
    #     self.deposit('staking',address, what,amount)


    # def deposit(self,address, in_pairs, out_pairs=None):
    #     if address not in self.holdings:
    #         self.holdings[address] = defaultdict(float)
    #
    #     for what, amount in in_pairs.items():
    #         self.holdings[address][what] += amount
    #
    #     if out_pairs is not None:
    #         if address not in self.receipts:
    #             self.receipts[address] = defaultdict(float)
    #
    #         for what, amount in out_pairs.items():
    #             self.receipts[address][what] += amount
    #             self.receipt_tokens.add(what)



