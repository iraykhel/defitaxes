import pprint
from .util import log
from .category import Category
from .transaction import Transfer
from .pool import Pool, Pools
from collections import defaultdict
import copy


class Classifier:
    NULL = '0x0000000000000000000000000000000000000000'

    def __init__(self, chain):
        self.chain = chain
        self.generic_library = {
            (0,0):[self.cl_fee],
            # ('?','?'):[self.cl_borrow,self.cl_repay],
            (0,1):[self.cl_deposit, self.cl_unwrap,  self.cl_mint_nft, self.cl_compound],
            (1,1):[self.cl_swap,self.cl_stake, self.cl_mint_nft,self.cl_borrow,self.cl_repay],  #swap needs to be before others
            (1,0):[self.cl_withdraw, self.cl_wrap],
            ('?',1):[self.cl_add],
            (1,'?'): [self.cl_remove],
            ('?',0):[self.cl_stake,self.cl_vault,self.cl_repay],
            (0, '?'): [self.cl_unstake, self.cl_unvault, self.cl_borrow]
            # (1,1):[self.cl_11],
            # (0,1):[self.cl_01],
            # (0,2):[self.cl_02],
            # (0,3):[self.cl_03],
            # (2,1):[self.cl_21],
            # (2,2): [self.cl_22],
            # (1,2): [self.cl_12],
            # (1,0): [self.cl_10]
        }

        self.address_to_cp_mapping = {
            # '0x7be8076f4ea4a4ad08075c2508e481d6c946d12b':'OpenSea',
            '0xc36442b4a4522e871399cd717abdd847ab11fe88':'Uniswap V3'
        }



        self.cp_mapping = {
            'OPENSEA': {
                'atomicMatch_':Category(Category.SWAP,nft=True, protocol='OPENSEA'),
                'proxyAssert':Category(Category.WITHDRAW,nft=True, protocol='OPENSEA'),
                'CATCHMENT':Category(Category.SWAP,nft=True, protocol='OPENSEA',certainty=3)
            },
            # '1Inch':{
            #     (2,1):self.cl_1inch_chiswap,
            #     (0,2):self.cl_1inch_doubleclaim,
            #     (1,3):self.cl_1inch_doubleexit,
            #     (1,1):[self.cl_1inch_chiswap,self.cl_1inch_stake,self.cl_1inch_unstake]
            # },
            #
            #
            # 'Balancer': {
            #     (1, '?'): [self.cl_balancer_remove],
            #     ('?', 1): [self.cl_balancer_add],
            # },
            #
            'COMPOUND':{
                'mint':Category(Category.SWAP, protocol='COMPOUND'),
                'redeem': Category(Category.SWAP, protocol='COMPOUND'),
                'redeemUnderlying': Category(Category.SWAP, protocol='COMPOUND'),
                'repayBorrow': Category(Category.REPAY, protocol='COMPOUND'),
                'repayBehalf': self.cl_compound_repaybehalf,
                'borrow': Category(Category.BORROW, protocol='COMPOUND'),
                'REWARD_TOKEN': '0xc00e94cb662c3520282e6f5717214004a7f26888'

                # (1,1):[self.cl_compound_addremove, self.cl_compound_repay],
                # (0,1):[self.cl_compound_borrow,self.cl_compound_claim],
                # (1,0):[self.cl_compound_repay],
                # (1,2):[self.cl_compound_addremove],
                # (0,2):[self.cl_compound_borrow],
            },
            'Uniswap V3':{
                'multicall':self.cl_uniswap_all,
                'increaseLiquidity':self.cl_uniswap_all,
                'collect':self.cl_uniswap_all,
                'mint':self.cl_uniswap_all
            }
            #
            # 'Yearn': {
            #     (1,0):[self.cl_yearn_add],
            #     (0,1):[self.cl_yearn_claim],
            #     (0, '?'): [self.cl_yearn_remove]
            # },
            #
            # 'Ygov': {
            #     (1, 0): [self.cl_yearn_add],
            #     (0, 1): [self.cl_yearn_claim],
            #     (0, '?'): [self.cl_yearn_remove]
            # },

            # 'Curve': {
            #     'add_liquidity':Category(Category.ADD_LIQUIDITY),
            #     'remove_liquidity_one_coin':Category(Category.REMOVE_LIQUIDITY),
            #     'remove_liquidity':Category(Category.REMOVE_LIQUIDITY),
            #     'mint_many':Category(Category.CLAIM),
            #     'mint': Category(Category.CLAIM),
            #     'deposit':Category(Category.STAKE),
            #     'withdraw':Category(Category.UNSTAKE)
            # },

            # 'Sushiswap': {
            #     ('?',1): [self.cl_sushi_claim],
            #     (0,2): [self.cl_sushi_claim]
            # }
        }

        self.uniswap_vaults = defaultdict(set)

    def add_liquidity(self,transaction, ignore_tokens=()):
        return self.chain.pools.add_liquidity(transaction, ignore_tokens=ignore_tokens)
    def remove_liquidity(self,transaction, ignore_tokens=(),pool_type=None):
        return self.chain.pools.remove_liquidity(transaction, ignore_tokens=ignore_tokens, pool_type=pool_type)

    # def stake(self, transfer):
    #     address, what, amount = transfer.to, transfer.what, transfer.amount
    #     self.chain.pools.stake(address, what, amount)

    def check_sig(self,sig,what):
        if sig is not None and what in sig.lower():
            return True
        return False

    def classify(self, transaction):
        transaction.type = None
        transaction.classification_certainty_level = 0
        combo = (transaction.out_cnt, transaction.in_cnt)


        interacted = transaction.lookup({'fr': transaction.addr,'input_non_zero':True})
        if len(interacted) == 1:
            transaction.interacted = interacted[0].to
        else:
            transaction.interacted = None

        sent_nonzero = transaction.lookup({'fr': transaction.addr,'amount_non_zero':True})
        received_nonzero = transaction.lookup({'to': transaction.addr,'amount_non_zero':True})

        error = transaction.lookup({'to': transaction.addr, 'fr': transaction.addr, 'amount_non_zero': True})
        minted = transaction.lookup({'to': transaction.addr, 'fr': Classifier.NULL,'amount_non_zero':True})
        burned = transaction.lookup({'fr': transaction.addr, 'to': Classifier.NULL,'amount_non_zero':True})

        nfts_in = transaction.lookup({'to': transaction.addr, 'type':4})
        nfts_out = transaction.lookup({'fr': transaction.addr, 'type':4})

        from_bridge = transaction.lookup({'to': transaction.addr, 'fr': self.chain.inbound_bridges})
        to_bridge = transaction.lookup({'fr': transaction.addr, 'to': self.chain.outbound_bridges})

        unstaked = transaction.lookup({'to': transaction.addr,'amount_non_zero':True, 'what': self.chain.pools.receipt_token_list(), 'fr':self.chain.pools.pool_address_list()})
        from_pools = transaction.lookup({'to': transaction.addr,'amount_non_zero':True, 'fr':self.chain.pools.pool_address_list()})





        staked = transaction.lookup({'fr': transaction.addr, 'amount_non_zero': True, 'what': self.chain.pools.receipt_token_list()})
        staked = list(set(staked) - set(burned))
        # unstaked = transaction.lookup({'to': transaction.addr,'amount_non_zero':True, 'what': self.chain.pools.receipt_token_list(), 'fr':self.chain.pools.pool_address_list(Pool.STAKING)})
        unvaulted = transaction.lookup({'to': transaction.addr,'amount_non_zero':True, 'what': self.chain.pools.input_token_list(Pool.VAULT), 'fr':self.chain.pools.pool_address_list(Pool.VAULT)})


        if transaction.hash == self.chain.hif:
            log('from_pools',from_pools)
            log('unstaked', unstaked)
            log('unvaulted', unvaulted)
            log('receipts',self.chain.pools.receipt_token_list())
        rewards = list(set(from_pools) - set(unstaked) - set(unvaulted))

        minted_nfts = transaction.lookup({'to': transaction.addr, 'fr': Classifier.NULL,'amount_non_zero':True,'type':4})

        # if transaction.hash == self.chain.hif:
        #     log('unstaked data',unstaked)
        #     log(unstaked[0].what+' in receipt_token_list', unstaked[0].what in self.chain.pools.receipt_token_list())
        #     log('self.chain.pools.receipt_token_list()',self.chain.pools.receipt_token_list())
        #     log(unstaked[0].fr + ' in pool_address_list', unstaked[0].fr in self.chain.pools.pool_address_list())

        # redeemed = transaction.lookup({'fr': transaction.addr,'amount_non_zero':True, 'what': self.chain.pools.receipt_token_list(), 'to':self.chain.pools.pool_address_list()})


        # staked = transaction.lookup({'fr': transaction.addr,'amount_non_zero':True, 'what': self.chain.pools.receipt_token_list()})
        # staked = list(set(staked)-set(burned))
        transaction.categorized_transfers = {
            Transfer.MINTED:minted,
            Transfer.RECEIVED:received_nonzero,
            # Transfer.SENT:sent,
            Transfer.SENT: sent_nonzero,
            Transfer.FROM_BRIDGE:from_bridge,
            Transfer.TO_BRIDGE:to_bridge,
            Transfer.STAKED_LP: staked,
            Transfer.UNSTAKED_LP:unstaked,
            Transfer.UNVAULTED: unvaulted,
            Transfer.REWARDS_LP:rewards,
            Transfer.BURNED:burned,
            Transfer.NFT_IN:nfts_in,
            Transfer.NFT_OUT: nfts_out,
            Transfer.ERROR: error,
            Transfer.INTERACTED: interacted,
            Transfer.MINTED_NFT: minted_nfts
            # Transfer.REDEEMED_LP: redeemed
        }




        cp = None
        sig = None
        if len(transaction.counter_parties) == 1:
            cp_list = list(transaction.counter_parties.values())
            cp, hex_sig, sig, _, cp_addr = cp_list[0]

            if cp_addr in self.address_to_cp_mapping:
                cp = self.address_to_cp_mapping[cp_addr]


        # if cp is not None:
        #     for custom_code_option in self.cp_mapping.keys():
        #         if custom_code_option.lower() in cp.lower():
        #             cp = custom_code_option
        #             break

        err = self.check_error(transaction,sig)
        if err is None:
            types = []
            specific_checkers = None
            if cp is not None and cp in self.cp_mapping:
                specific_checkers = self.cp_mapping[cp]
                if 'REWARD_TOKEN' in self.cp_mapping[cp]:
                    transaction.reward_token = self.cp_mapping[cp]['REWARD_TOKEN']

                checkers = []
                combo_options = [combo, ('?',combo[1]), (combo[0],'?')]
                for combo_opt in combo_options:
                    if combo_opt in specific_checkers:
                        combo_checkers = specific_checkers[combo_opt]
                        if not isinstance(combo_checkers,list):# or type(combo_checkers) is not list:
                            combo_checkers = [combo_checkers]
                        # print(checkers,combo_checkers)
                        checkers.extend(combo_checkers)



                if sig in specific_checkers:
                    if isinstance(specific_checkers[sig],Category):
                        types.append(copy.deepcopy(specific_checkers[sig])) #god that was an insane bug. Must have copy here, or it'll overwrite .claim on types used in different transactions.
                    else:
                        sig_checkers = specific_checkers[sig]
                        if not isinstance(sig_checkers,list):
                            sig_checkers = [sig_checkers]
                        checkers.extend(sig_checkers)



                for checker in checkers:
                    rv = checker(transaction, sig)
                    if rv is not None:
                        types.append(rv)


                        # break

            if len(types) == 0:
                combo_options = [combo, ('?', combo[1]), (combo[0], '?'), ('?','?')]
                generic_checkers = []
                for combo_opt in combo_options:
                    if combo_opt in self.generic_library:
                        generic_checkers.extend(self.generic_library[combo_opt])

                for checker in generic_checkers:
                    rv = checker(transaction, sig)
                    if rv is not None:
                        # tp, accuracy = rv
                        types.append(rv)

            if self.chain.hif == transaction.hash:
                print("CATCHMENT?",cp_addr,cp,len(types) == 0, specific_checkers is not None)

            if len(types) == 0 and specific_checkers is not None and 'CATCHMENT' in specific_checkers:
                checker = specific_checkers['CATCHMENT']
                if isinstance(checker, Category):
                    types.append(copy.deepcopy(checker))
                else:
                    rv = checker(transaction, sig)
                    if rv is not None:
                        types.append(rv)


            #swaps are below in priority
            if len(types) > 1:
                certainty_mapping = defaultdict(list)
                for type in types:
                    certainty_mapping[type.certainty].append(type)
                types = certainty_mapping[max(list(certainty_mapping.keys()))]

                types_noswap = []
                for type in types:
                    if type.category == Category.SWAP and type.certainty != 10:
                        continue
                    else:
                        types_noswap.append(type)
                types = types_noswap

            if len(types) == 1:
                transaction.type = types[0]
                if self.NFT_check(transaction, sig):
                    transaction.type.nft=True


                # print("tt",transaction.type)

                if transaction.type.category in [Category.SWAP, Category.ADD_LIQUIDITY, Category.REMOVE_LIQUIDITY, Category.BORROW, Category.REPAY, Category.MINT_NFT] \
                        and transaction.type.certainty >= 0 and len(received_nonzero) and len(sent_nonzero):
                    transaction.balanced = True



            if len(types) <= 1:
                self.check_reward(transaction, sig)



        # print('\n\n', combo, types)
            if len(types) > 1:
                print(transaction.hash, "MULTIPLE TYPES")
                transaction.type = types

        if isinstance(transaction.type,Category):
            transaction.classification_certainty_level = transaction.type.certainty
        else:
            transaction.classification_certainty_level = 0
            # transaction.type = None

        self.process_classification(transaction)

        if transaction.hash == transaction.chain.hif:
            log('CL RESULT', transaction.hash, transaction.counter_parties, cp, transaction.type, transaction.classification_certainty_level, combo, len(transaction.transfers))
            for cat in transaction.categorized_transfers.keys():
                log("transfer type",Transfer.name_map[cat],len(transaction.categorized_transfers[cat]))
            log(transaction.counter_parties)
            pprint.pprint(transaction.transfers)
            log('\n\n\n')
            # exit(1)

    def check_error(self,transaction,sig):
        CT = transaction.categorized_transfers
        if len(CT[Transfer.ERROR]) > 0:
            return Category(Category.ERROR,certainty=0)

    def check_reward(self, transaction, sig):
        CT = transaction.categorized_transfers

        if hasattr(transaction,'reward_token'):
            reward_token = transaction.reward_token
            reward_transfers = transaction.lookup({'what':reward_token,'to':transaction.addr})
            if transaction.hash == transaction.chain.hif:
                log("cr check reward token",len(reward_transfers))
            if len(reward_transfers) > 0:
                if transaction.hash == transaction.chain.hif:
                    log("cr crw ",len(reward_transfers))
                for rew in reward_transfers:
                    rew.treatment = 'income'
                if len(reward_transfers) == len(CT[Transfer.RECEIVED]) and len(CT[Transfer.SENT]) == 0:
                    if transaction.type is None:
                        transaction.type = Category(Category.CLAIM,certainty=10)
                    elif isinstance(transaction.type,Category):
                        if transaction.type.category is None:
                            transaction.type.category = Category.CLAIM

                        if transaction.hash == transaction.chain.hif:
                            log("cr crw setting cf", len(reward_transfers))
                        log("cr crw setting", transaction.hash, len(reward_transfers))
                        transaction.type.claim = True
            return


        if isinstance(transaction.type, Category):
            if not (transaction.type.category in [Category.STAKE, Category.UNSTAKE, Category.ADD_LIQUIDITY, Category.REMOVE_LIQUIDITY, Category.REMOVE_LIQUIDITY_NO_RECEIPT, Category.ADD_LIQUIDITY_NO_RECEIPT, Category.EXIT_VAULT]):
                return

            #no space for a reward
            if len(CT[Transfer.RECEIVED])+len(CT[Transfer.SENT]) == 1:
                return



        do_claim = False
        if transaction.hash == transaction.chain.hif:
            print("cr XI1")

        pools = []
        max_certainty = 5
        rew = None
        rew2 = None
        if len(CT[Transfer.REWARDS_LP]) > 0:
            rew = CT[Transfer.REWARDS_LP][0]

            pools = self.chain.pools.pool_list('A',rew.fr)
            if len(CT[Transfer.REWARDS_LP]) == 2:
                rew2 = CT[Transfer.REWARDS_LP][1]
                pools = pools.intersection(self.chain.pools.pool_list('A', rew2.fr))

            if transaction.hash == transaction.chain.hif:
                print("cr XI2",rew,pools)
            if len(pools) > 1: #sometimes there's a 0-valued transfer specifying which LP token was used to stake
                lp_indicator = transaction.lookup({'fr': transaction.addr, 'amount_non_zero': False, 'type':3})
                if transaction.hash == transaction.chain.hif:
                    print("cr XI21", lp_indicator)
                if len(lp_indicator) == 1:
                    lp_token = lp_indicator[0].what
                    # pools_by_token = self.chain.pools.map['I'][lp_token]
                    pools_by_token = self.chain.pools.pool_list('I',lp_token)
                    pools = pools.intersection(pools_by_token)
                    if transaction.hash == transaction.chain.hif:
                        print("cr XI22", pools)
                else:
                    #is there a staking or unstaking transaction?
                    pool_transfers = CT[Transfer.UNSTAKED_LP] + CT[Transfer.STAKED_LP] + CT[Transfer.UNVAULTED]
                    transferred_tokens = set()
                    for pt in pool_transfers:
                        transferred_tokens.add(pt.what)
                    if len(transferred_tokens) == 1:
                        # pools_by_token = self.chain.pools.map['I'][list(transferred_tokens)[0]]
                        pools_by_token = self.chain.pools.pool_list('I',list(transferred_tokens)[0])
                        pools = pools.intersection(pools_by_token)
                        if transaction.hash == transaction.chain.hif:
                            print("cr XI23", pools)

        #reward token is minted, not sent from pool
        elif len(CT[Transfer.MINTED]) > 0 and transaction.interacted is not None and not self.check_sig(sig,'mint') and not self.check_sig(sig,'borrow'):
            max_certainty = 3
            interactor = transaction.interacted
            pools = self.chain.pools.pool_list('A', interactor)
            rew = CT[Transfer.MINTED][0]
            if transaction.hash == transaction.chain.hif:
                print("cr XI241",'pools', len(pools), pools)
                print("cr XI242",'interactor', interactor)
            # pools = self.chain.pools.map['A'][rew.fr]

        # reward token is sent from a different address
        elif (len(CT[Transfer.RECEIVED]) >= 1 and (self.check_sig(sig,'claim') or self.check_sig(sig,'reward') or self.check_sig(sig,'withdraw') or self.check_sig(sig,'harvest')))\
                or (len(CT[Transfer.UNVAULTED]) >= 1 and self.check_sig(sig, 'deposit')):
            if self.check_sig(sig, 'deposit'):
                max_certainty = 3
            if transaction.hash == transaction.chain.hif:
                print("cr XI243", 'pools', len(pools), pools)
            interactor = transaction.interacted
            reward_transfers = set(CT[Transfer.RECEIVED])-set(CT[Transfer.UNSTAKED_LP])-set(CT[Transfer.UNVAULTED])
            if len(reward_transfers) > 0:
                if transaction.hash == transaction.chain.hif:
                    print("cr XI244")
                prew_list = sorted(list(reward_transfers), key=lambda x: x.index, reverse=False) #need consistent order
                # rew = list(reward_transfers)[0]
                for prew in prew_list:
                    if prew.treatment is None:
                        rew = prew
                        break
                pools = self.chain.pools.pool_list('A', interactor)


        if len(pools) >= 1:
            pool = list(pools)[0]
            if transaction.hash == transaction.chain.hif:
                print("cr XI3",pool,rew.what, pool.deposited)
            if (pool.type in [Pool.STAKING,Pool.VAULT] or self.check_sig(sig,'reward')) or self.check_sig(sig,'claim') and rew.what not in pool.deposited:
                if transaction.hash == transaction.chain.hif:
                    print("cr XI4")
                do_claim = True


        if do_claim:
            if len(pools) == 1:
                list(pools)[0].issue_reward(rew)
                certainty = max_certainty
            else:
                certainty = 3
            if rew is not None:
                rew.treatment = 'income'
            if rew2 is not None:
                rew2.treatment = 'income'

            if transaction.hash == transaction.chain.hif:
                print("cr XI5")

            full_cat = False
            if transaction.type is None:
                if transaction.hash == transaction.chain.hif:
                    print("cr XI6")

                transaction.type = Category(Category.CLAIM,certainty=certainty)
                full_cat = True
            elif isinstance(transaction.type,Category):
                if transaction.type.category is None:
                    transaction.type.category = Category.CLAIM
                transaction.type.claim = True
                full_cat = True

            if full_cat and rew is not None:
                found_transfers = transaction.lookup({'to': transaction.addr,'amount_non_zero':True,'what':rew.what})
                for transfer in found_transfers:
                    transfer.treatment = 'income'


    # def cl_00(self,transaction,sig):
    #     return Category(Category.FEE)

    def cl_fee(self, transaction, sig):
        return Category(Category.FEE)

    def cl_deposit(self,transaction,sig):

        # if sig in ['unstake','withdraw']:
        #     return
        # if self.check_sig(sig,'borrow'):
        #     return
        CT = transaction.categorized_transfers
        if len(CT[Transfer.RECEIVED]) and not len(CT[Transfer.SENT]) and not len(CT[Transfer.MINTED]) and not len(CT[Transfer.UNSTAKED_LP]):
            if len(CT[Transfer.FROM_BRIDGE]):
                return Category(Category.DEPOSIT_FROM_BRIDGE, certainty=10)
            else:
                if len(CT[Transfer.INTERACTED]) == 0:
                    return Category(Category.DEPOSIT, certainty=5)

    def cl_unwrap(self, transaction, sig):
        # print("UNWRAP?", CT[Transfer.RECEIVED][0].fr, transaction.chain.wrapper)
        # print("CL_UNWRAP SIG",sig)
        if sig != 'withdraw':
            return
        CT = transaction.categorized_transfers

        if len(CT[Transfer.RECEIVED]) and CT[Transfer.RECEIVED][0].fr == transaction.chain.wrapper:
            return Category(Category.UNWRAP, certainty=10)


    def cl_withdraw(self,transaction,sig):
        if sig in ['stake','deposit']:
            return
        if self.check_sig(sig,'repay'):
            return
        CT = transaction.categorized_transfers
        if len(CT[Transfer.SENT]) and not len(CT[Transfer.RECEIVED]) and not len(CT[Transfer.BURNED]) and not len(CT[Transfer.STAKED_LP]):
            if len(CT[Transfer.TO_BRIDGE]):
                return Category(Category.WITHDRAW_TO_BRIDGE, certainty=10)
            else:
                if len(CT[Transfer.INTERACTED]) == 0:
                    return Category(Category.WITHDRAW, certainty=5)

    def cl_wrap(self, transaction, sig):
        if sig != 'deposit':
            return
        CT = transaction.categorized_transfers
        if len(CT[Transfer.SENT]) and CT[Transfer.SENT][0].to == transaction.chain.wrapper:
            return Category(Category.WRAP, certainty=10)



    def cl_swap(self,transaction,sig):
        if sig is not None:
            if sig in ['mint']:# or self.check_sig(sig,'liquidity'):
                return

            if self.check_sig(sig,'swap') or self.check_sig(sig,'exchange'):
                return Category(Category.SWAP, certainty=10)
            elif self.check_sig(sig,'migrate'):
                return Category(Category.SWAP, certainty=5)

        CT = transaction.categorized_transfers
        if len(CT[Transfer.RECEIVED]) == 1 and len(CT[Transfer.SENT]) == 1 \
                and not len(CT[Transfer.MINTED]) and not len(CT[Transfer.BURNED]):
            if not len(CT[Transfer.STAKED_LP]) and not len(CT[Transfer.UNSTAKED_LP]) and not len(CT[Transfer.REWARDS_LP]):
                return Category(Category.SWAP, certainty=10)
            else:
                return Category(Category.SWAP, certainty=5)


    def cl_add(self, transaction, sig):
        if transaction.total_fee == 0 and self.chain.name != 'Polygon':
            return
        if sig is not None and 'migrate' in sig:
            return

        if self.check_sig(sig,'swap'):
            return

        if transaction.hash == self.chain.hif:
            print("ADD PASS 1")

        certainty = None
        CT = transaction.categorized_transfers
        if len(CT[Transfer.SENT]) >= 1 and not len(CT[Transfer.BURNED]) and not len(CT[Transfer.NFT_OUT]):# and not len(CT[Transfer.MINTED_NFT]):
            do_add = False
            if transaction.hash == self.chain.hif:
                print("ADD PASS 2")
            if self.check_sig(sig, 'add') and self.check_sig(sig,'liquidity') and not len(CT[Transfer.MINTED_NFT]):
                do_add = True
                if transaction.hash == self.chain.hif:
                    print("ADD PASS 21")
            elif len(CT[Transfer.MINTED]) == 1:
                if abs(transaction.amounts[CT[Transfer.MINTED][0].what]) == CT[Transfer.MINTED][0].amount:
                    if len(CT[Transfer.SENT]) >= 2: #minted liquidity pool token
                        certainty = 5
                    elif len(CT[Transfer.SENT]) == 1:
                        certainty = 3
                    if certainty is not None:
                        do_add = True
                        if transaction.hash == self.chain.hif:
                            print("ADD PASS 22")
            elif len(CT[Transfer.RECEIVED]) >= 1 and len(CT[Transfer.SENT]) >= 2: #issued liquidity pool token from the same address we deposited to

                    # do_add = True
                received_addresses = set()
                sent_addresses = set()
                for received_transfer in CT[Transfer.RECEIVED]:
                    received_addresses.add(received_transfer.fr)

                for sent_transfer in CT[Transfer.SENT]:
                    sent_addresses.add(sent_transfer.to)

                if transaction.hash == self.chain.hif:
                    print("ADD PASS 3",sent_addresses,received_addresses)

                if len(sent_addresses) == 1 and list(sent_addresses)[0] in received_addresses:
                    if transaction.hash == self.chain.hif:
                        print("ADD PASS 4")
                    do_add = True
                    # if sent_transfer.to != CT[Transfer.RECEIVED][0].fr:
                    #     do_add = False
                    #     break



            if do_add:
                if self.check_sig(sig,'addliquidity'):
                    self.add_liquidity(transaction)
                    return Category(Category.ADD_LIQUIDITY, certainty=10)
                else:
                    if len(CT[Transfer.SENT]) >= 2 or not self.check_sig(sig,'execute'):
                    # if 1:
                        if certainty is None:
                            certainty = 5
                        self.add_liquidity(transaction)
                        return Category(Category.ADD_LIQUIDITY, certainty=certainty)



    def cl_remove(self, transaction, sig):
        if transaction.total_fee == 0 and self.chain.name != 'Polygon':
            return
        if sig is not None and 'migrate' in sig:
            return
        if self.check_sig(sig,'swap'):
            return
        CT = transaction.categorized_transfers
        if len(CT[Transfer.RECEIVED]) >= 1 and not len(CT[Transfer.MINTED]) and not len(CT[Transfer.NFT_IN]):
            do_remove = False
            max_certainty = 10
            if len(CT[Transfer.BURNED]) == 1: #burned liquidity pool token
                if abs(transaction.amounts[CT[Transfer.BURNED][0].what]) == CT[Transfer.BURNED][0].amount:
                    do_remove = True
            elif len(CT[Transfer.STAKED_LP]) == 1: #returned LP token to the pool?
                if transaction.hash == self.chain.hif:
                    print("cl_remove 1")
                destination = CT[Transfer.STAKED_LP][0].to
                token = CT[Transfer.STAKED_LP][0].what
                # pools_by_token = self.chain.pools.receipt_token_list()

                # if token in pools_by_token:
                # pbt = pools_by_token[token]
                pbt = self.chain.pools.pool_list('O',token)

                # pools_by_address = self.chain.pools.pool_address_list()
                # if destination in pools_by_address:
                #     if len(pools_by_address[destination].intersection(pbt)):
                #         do_remove = True

                if len(pbt):
                    pba = self.chain.pools.pool_list('A', destination)
                    if len(pbt.intersection(pba)):
                        do_remove = True

                    else: #returned LP token to a different address, but got back everything matching an existing
                        for transfer in CT[Transfer.RECEIVED]:
                            in_token = self.chain.unwrap(transfer.what)
                            if transaction.hash == self.chain.hif:
                                print("cl_remove 3", in_token)
                            if in_token in self.chain.pools.input_token_list():
                                if transaction.hash == self.chain.hif:
                                    print("cl_remove 4", self.chain.pools.pool_list('I',in_token))
                                pbt = pbt.intersection(self.chain.pools.pool_list('I',in_token))

                                # pbt = pbt.intersection(self.chain.pools.map['I'][in_token])

                        if len(pbt):
                            max_certainty = 3
                            do_remove = True



            if do_remove:
                if self.remove_liquidity(transaction):
                    return Category(Category.REMOVE_LIQUIDITY, certainty=max_certainty)
                else:
                    return Category(Category.REMOVE_LIQUIDITY, certainty=3)
                # if self.check_sig(sig, 'removeliquidity'):
                #     if self.remove_liquidity(transaction):
                #         return Category(Category.REMOVE_LIQUIDITY, certainty=10)
                # else:
                #     if len(CT[Transfer.RECEIVED]) == 2:
                #         if self.remove_liquidity(transaction):
                #             return Category(Category.REMOVE_LIQUIDITY, certainty=5)


    def cl_stake(self, transaction, sig):
        if transaction.total_fee == 0 and self.chain.name != 'Polygon':
            return
        if self.check_sig(sig,'repay') or self.check_sig(sig,'swap'):
            return
        CT = transaction.categorized_transfers
        if len(CT[Transfer.STAKED_LP]):
            #returned token to the pool?
            if transaction.hash == self.chain.hif:
                print("cl_stake 1")
            destination = CT[Transfer.STAKED_LP][0].to
            token = CT[Transfer.STAKED_LP][0].what

            # if transaction.hash == self.chain.hif:
            #     log('token',token,'pools_by_token',pools_by_token[token])
            #     log('destination',destination,'pools_by_address', pools_by_address[destination])
            #     log('intersect',pools_by_address[destination].intersection(pools_by_token[token]))

            # pools_by_token = self.chain.pools.receipt_token_list()
            # pools_by_address = self.chain.pools.pool_address_list()
            # if destination in pools_by_address and token in pools_by_token:
            #     if len(pools_by_address[destination].intersection(pools_by_token[token])):
            #         return
            pbt = self.chain.pools.pool_list('O',token)
            pba = self.chain.pools.pool_list('A',destination)
            if len(pbt.intersection(pba)):
                if transaction.hash == self.chain.hif:
                    print("cl_stake 2 BAIL")
                return

            if transaction.in_cnt == 0:
                if transaction.hash == self.chain.hif:
                    print("cl_stake 3")
                self.add_liquidity(transaction)
                return Category(Category.STAKE, certainty=5)
            elif len(CT[Transfer.REWARDS_LP]) or len(CT[Transfer.UNVAULTED]): #claim reward as well?
                if transaction.hash == self.chain.hif:
                    print("cl_stake 4")
                self.add_liquidity(transaction)
                return Category(Category.STAKE, certainty=5)

    def cl_vault(self, transaction, sig):
        if transaction.hash == self.chain.hif:
            print("cl_vault XI0")
        CT = transaction.categorized_transfers
        if len(CT[Transfer.SENT]) and self.check_sig(sig, 'stake') and not self.check_sig(sig, 'unstake'):  # who am I to argue?
            self.add_liquidity(transaction)
            return Category(Category.ADD_LIQUIDITY_NO_RECEIPT, certainty=3)

        if len(CT[Transfer.SENT]) == 1 and len(transaction.transfers) <= 3 and self.check_sig(sig, 'deposit') and len(CT[Transfer.TO_BRIDGE]) == 0:
            if transaction.hash == self.chain.hif:
                print("cl_vault XI1")
            self.add_liquidity(transaction)
            return Category(Category.ADD_LIQUIDITY_NO_RECEIPT, certainty=3)
            # return Category(Category.STAKE, certainty=0)


    def cl_unstake(self, transaction, sig):
        if transaction.total_fee == 0 and self.chain.name != 'Polygon':
            return
        if sig is not None and 'borrow' in sig:
            return
        if self.check_sig(sig,'exit'):
            cat = Category.EXIT_VAULT
        else:
            cat = Category.UNSTAKE

        CT = transaction.categorized_transfers
        if len(CT[Transfer.UNSTAKED_LP]):
            if self.remove_liquidity(transaction, pool_type=Pool.STAKING):
                return Category(cat, certainty=5)

    def cl_unvault(self, transaction, sig):
        if transaction.total_fee == 0 and self.chain.name != 'Polygon':
            return
        if sig is not None and 'borrow' in sig:
            return

        if self.check_sig(sig,'exit'):
            cat = Category.EXIT_VAULT
        else:
            cat = Category.REMOVE_LIQUIDITY_NO_RECEIPT

        CT = transaction.categorized_transfers
        if len(CT[Transfer.UNSTAKED_LP]):
            return

        if len(CT[Transfer.UNVAULTED]) and not self.check_sig(sig,'deposit') and not self.check_sig(sig,'harvest') and len(CT[Transfer.FROM_BRIDGE]) == 0:
            if transaction.hash == self.chain.hif:
                print("cl_unvault xi1")

            # cnt_unvault = transaction.lookup({'what': CT[Transfer.UNVAULTED][0].what},count_only=True)
            # if cnt_unvault > 1:
            #     return

            if self.remove_liquidity(transaction, pool_type=Pool.VAULT):
                return Category(cat, certainty=5)
            else:
                return

        # did we receive tokens from a different address than the deposited?
        if len(CT[Transfer.RECEIVED]) > 0 and len(CT[Transfer.SENT]) == 0 \
                and len(CT[Transfer.UNSTAKED_LP]) == 0 \
                and len(CT[Transfer.UNVAULTED]) == 0 \
                and len(CT[Transfer.REWARDS_LP]) == 0:
            if transaction.hash == self.chain.hif:
                print("cl_unvault XI1")

            if transaction.interacted:
                pools_for_interactor = self.chain.pools.matches(transaction.interacted, 'A')

                pools_for_tokens = set()
                for transfer in CT[Transfer.RECEIVED]:
                    pools_for_tokens = pools_for_tokens.union(self.chain.pools.matches(transfer.what, 'I'))

                pools = pools_for_interactor.intersection(pools_for_tokens)
                if transaction.hash == self.chain.hif:
                    print("cl_unvault XI2",pools)

                if len(pools):
                    if self.remove_liquidity(transaction, pool_type=Pool.VAULT):
                        if transaction.hash == self.chain.hif:
                            print("cl_unvault XI3", pools)
                        return Category(cat, certainty=3)
                    else:
                        return


    def cl_borrow(self, transaction, sig):
        if self.check_sig(sig,'borrow') and not self.check_sig(sig,'repay'):
            return Category(Category.BORROW, certainty=5)

    def cl_repay(self, transaction, sig):
        if self.check_sig(sig,'repay'):
            return Category(Category.REPAY, certainty=5)


    def cl_mint_nft(self, transaction, sig):
        CT = transaction.categorized_transfers
        if len(CT[Transfer.MINTED_NFT]) > 0:
            if len(CT[Transfer.SENT]) > 0:
                transaction.balanced = True

            return Category(Category.MINT_NFT, certainty=5)


    def cl_compound(self,transaction,sig):
        CT = transaction.categorized_transfers
        if len(CT[Transfer.SENT]) == 1 and len(CT[Transfer.RECEIVED]) == 1:
            if CT[Transfer.SENT][0].what == CT[Transfer.RECEIVED][0].what:
                # print('comp div',CT[Transfer.SENT][0].amount / CT[Transfer.RECEIVED][0].amount)
                ratio = CT[Transfer.SENT][0].amount / CT[Transfer.RECEIVED][0].amount
                if ratio > 0.5 and ratio <= 1:
                    return Category(Category.COMPOUND, certainty=5)


    def NFT_check(self, transaction,sig):
        found_count = transaction.lookup({'type': 4}, count_only=True) + transaction.lookup({'type': 5}, count_only=True)
        if found_count > 0:
            return True
        return False



    def cl_uniswap_all(self, transaction,sig):
        CT = transaction.categorized_transfers

        tokens = set()
        for tr in transaction.transfers:
            if tr.to == None or tr.amount == 0:
                continue
            symbol = tr.symbol
            if symbol == 'ETH':
                qw = tr.to
            if symbol == 'WETH':
                symbol = 'ETH'
            if tr.token_nft_id is None:
                if tr.outbound:
                    tr.treatment = 'deposit'
                else:
                    tr.treatment = 'withdraw'
                tokens.add(symbol)
            else:
                tr.treatment = 'ignore'

        tokens = list(tokens)
        if len(tokens) == 2:
            certainty = 5

            self.uniswap_vaults[tokens[0]].add(tokens[1])
            self.uniswap_vaults[tokens[1]].add(tokens[0])
        else:
            certainty = 0
            if len(tokens) == 1:
                if len(self.uniswap_vaults[tokens[0]]) == 1:
                    certainty = 3
                    tokens.append(list(self.uniswap_vaults[tokens[0]])[0])

                transaction.protocol_note = 'We are not sure about the vault ID on the '+tokens[0]+' transfer'



        vault_id = 'UNI_'+'_'.join(sorted(tokens))
        for tr in transaction.transfers:
            tr.vault_id = vault_id

        if len(CT[Transfer.SENT]) > 0:
            return Category(Category.ADD_LIQUIDITY_NO_RECEIPT,certainty=certainty, protocol='UNISWAP V3')
        else:
            return Category(Category.REMOVE_LIQUIDITY_NO_RECEIPT, certainty=certainty, protocol='UNISWAP V3')

    def cl_compound_repaybehalf(self, transaction,sig):
        CT = transaction.categorized_transfers
        for tr in CT[Transfer.SENT]:
            tr.treatment = 'repay'
        for tr in CT[Transfer.RECEIVED]:
            tr.treatment = 'income'
        return Category(Category.REPAY, certainty=10, protocol='COMPOUND')


    # def cl_1inch_chiswap(self, transaction,sig):
    #     if len(transaction.transfers) in [4,3]:
    #         found_transfers = transaction.lookup({'to': Classifier.NULL,'symbol':'CHI'})
    #         if len(found_transfers) == 1:
    #             found_transfers[0].treatment = 'burn' #burn the CHIs
    #             return Category(Category.SWAP)
    #
    # def cl_1inch_doubleclaim(self, transaction,sig):
    #     if len(transaction.transfers) == 3:
    #         return Category(Category.CLAIM)
    #
    # def cl_1inch_doubleexit(self, transaction,sig):
    #     found_transfers = transaction.lookup({'to': Classifier.NULL})
    #     if len(found_transfers) == 0:
    #         return
    #     found_transfers[0].treatment = 'sell'
    #     stake_receipt_symbol = found_transfers[0].symbol
    #     liquidity_receipt_symbol = stake_receipt_symbol[5:]
    #     found_transfers = transaction.lookup({'symbol': liquidity_receipt_symbol})
    #     if len(found_transfers) == 0:
    #         return
    #     found_transfers[0].treatment = 'buy'
    #     return Category(Category.REMOVE_LIQUIDITY, claim_reward=True)
    #
    # def cl_1inch_stake(self, transaction,sig):
    #     if len(transaction.transfers) == 3:
    #         found_count = transaction.lookup({'fr':Classifier.NULL},count_only=True)
    #         if found_count == 1:
    #             transaction.transfers[1].treatment = 'sell'
    #             transaction.transfers[2].treatment = 'buy'
    #             self.add_liquidity(transaction)
    #             return Category(Category.ADD_LIQUIDITY)
    #             # return 'stake',10
    #
    # def cl_1inch_unstake(self, transaction,sig):
    #     if len(transaction.transfers) == 3:
    #         found_transfers = transaction.lookup({'to':Classifier.NULL})
    #         if len(found_transfers) == 1 and found_transfers[0].symbol != 'CHI':
    #             transaction.transfers[1].treatment = 'buy'
    #             transaction.transfers[2].treatment = 'sell'
    #             if self.remove_liquidity(transaction):
    #                 return Category(Category.REMOVE_LIQUIDITY)
    #             # return 'unstake',10
    #
    #
    #
    #
    # def cl_balancer_add(self,transaction,sig):
    #     # if len(transaction.transfers) in [5,6,7,8]:
    #     if 1:
    #         found_count = transaction.lookup({'symbol':'BPT','to':transaction.addr},count_only=True)
    #         if found_count == 1:
    #             found_transfers = transaction.lookup({'symbol': 'COMP', 'to': transaction.addr})
    #             for transfer in found_transfers:
    #                 transfer.treatment = 'gift'
    #
    #             return Category(Category.ADD_LIQUIDITY)
    #
    # def cl_balancer_remove(self,transaction,sig):
    #     # if len(transaction.transfers) in [4,5,6]:
    #     if 1:
    #         found_count = transaction.lookup({'symbol':'BPT','fr':transaction.addr},count_only=True)
    #         if found_count == 1:
    #             return Category(Category.REMOVE_LIQUIDITY)
    #
    #
    #
    #
    # def cl_compound_mint_redeem(self,transaction,sig):
    #     found_comp_transfers = transaction.lookup({'symbol': 'COMP', 'to':transaction.addr})
    #     for transfer in found_comp_transfers:
    #         transfer.treatment = 'income'
    #     return Category(Category.SWAP)
    #
    # def cl_compound_addremove(self, transaction,sig):
    #     if len(transaction.transfers) == 4:
    #         found_comp_transfers = transaction.lookup({'symbol': 'COMP', 'to':transaction.addr})
    #         if len(found_comp_transfers) == 1:
    #             symbol = transaction.transfers[2].symbol
    #             found_transfers = transaction.lookup({'symbol': 'c' + symbol})
    #             if len(found_transfers) == 1:
    #                 found_comp_transfers[0].treatment = 'gift'
    #                 if found_transfers[0].to == transaction.addr:
    #                     return Category(Category.ADD_LIQUIDITY)
    #                 if found_transfers[0].fr == transaction.addr:
    #                     return Category(Category.REMOVE_LIQUIDITY)
    #
    #     if len(transaction.transfers) == 3:
    #         symbol = transaction.transfers[1].symbol
    #         found_transfers = transaction.lookup({'symbol':'c'+symbol})
    #         if len(found_transfers) == 1:
    #             if found_transfers[0].to == transaction.addr:
    #                 return Category(Category.ADD_LIQUIDITY)
    #             if found_transfers[0].fr == transaction.addr:
    #                 return Category(Category.REMOVE_LIQUIDITY)
    #
    #     if len(transaction.transfers) in [2,3]:
    #         found_transfers = transaction.lookup({'symbol': 'c'+transaction.main_asset})
    #         if len(found_transfers) == 1:
    #             if found_transfers[0].to == transaction.addr:
    #                 return Category(Category.ADD_LIQUIDITY)
    #             if found_transfers[0].fr == transaction.addr:
    #                 return Category(Category.REMOVE_LIQUIDITY)
    #
    #     if sig == 'redeemUnderlying' or sig == 'redeem':
    #         found_transfers = transaction.lookup({'symbol': 'COMP'})
    #         for transfer in found_transfers:
    #             transfer.treatment = 'gift'
    #         return Category(Category.REMOVE_LIQUIDITY)
    #
    #
    #
    # def cl_compound_borrow(self, transaction,sig):
    #     if len(transaction.transfers) == 3:
    #         if transaction.transfers[0].input_len == 74:
    #             if transaction.transfers[1].symbol == 'COMP':
    #                 transaction.transfers[1].treatment = 'gift'
    #                 transaction.transfers[2].treatment = 'gift'
    #                 return Category(Category.BORROW)
    #
    #     if len(transaction.transfers) == 2:
    #         if transaction.transfers[0].input_len == 74 and transaction.transfers[1].free:
    #             if transaction.transfers[1].symbol != 'COMP':
    #                 transaction.transfers[1].treatment = 'gift'
    #                 return Category(Category.BORROW)
    #
    # def cl_compound_claim(self, transaction,sig):
    #     found_comp_count = transaction.lookup({'symbol': 'COMP','to':transaction.addr},count_only=True)
    #     if len(transaction.transfers) == found_comp_count + 1:
    #         return Category(Category.CLAIM)
    #
    # def cl_compound_repay(self, transaction,sig):
    #     if len(transaction.transfers) == 2:
    #         if transaction.transfers[0].input_len == 74:
    #             transaction.transfers[1].treatment = 'burn'
    #             return Category(Category.REPAY)
    #
    #     if len(transaction.transfers) == 3:
    #         if transaction.transfers[0].input_len == 74:
    #             if transaction.transfers[1].symbol == 'COMP' and transaction.transfers[1].to == transaction.addr:
    #                 transaction.transfers[1].treatment = 'burn'
    #                 return Category(Category.REPAY)
    # #
    # def cl_yearn_add(self, transaction,sig):
    #     if len(transaction.transfers) == 2:
    #         if transaction.transfers[0].input_len == 74:
    #             # print("ADDING TO YEARN VAULT",transaction.transfers[1].what)
    #             transaction.chain.vault_holds[transaction.transfers[1].to] = transaction.transfers[1].what
    #             return Category(Category.ADD_LIQUIDITY_NO_RECEIPT)
    #
    # def cl_yearn_claim(self, transaction,sig):
    #     if sig == 'getReward':
    #         return Category(Category.CLAIM)
    #     # if len(transaction.transfers) == 2:
    #     #     if transaction.transfers[0].input_len == 10 and transaction.transfers[1].symbol == 'YFI':
    #     #         return "claim reward", 10
    #
    # def cl_yearn_remove(self, transaction,sig):
    #
    #     if len(transaction.transfers) in [2,3]:
    #         # if transaction.transfers[0].input_len == 10:
    #         vaults = transaction.chain.vault_holds
    #         # print("VAULT CHECK",vaults)
    #         # found_count = transaction.lookup({'to':transaction.addr,'what':list(transaction.chain.vault_holds.values()),'fr':list(transaction.chain.vault_holds.keys())},count_only=True)
    #         found_transfers = transaction.lookup({'to': transaction.addr, 'fr': list(vaults.keys()), 'what':list(vaults.values())})
    #
    #         for transfer in found_transfers:
    #             if vaults[transfer.fr] == transfer.what:
    #                 return Category(Category.REMOVE_LIQUIDITY_NO_RECEIPT)
    #
    #             # transfers[1].fr in transaction.chain.vault_holds and transaction.chain.vault_holds[transfers[1].fr] == transfers[1].what:
    #             # return "claim reward", 10
    #
    # def cl_sushi_claim(self, transaction, sig):
    #     found_sushi_in = transaction.lookup({'symbol':'SUSHI','to':transaction.addr})
    #     if len(found_sushi_in) == 1:
    #         found_stake = transaction.lookup({'symbol': ['UNI-V2','SLP']})
    #         # if len(found_stake) == 0:
    #         #     found_stake = transaction.lookup({'symbol': 'SLP'})
    #         if len(found_stake) == 1:
    #             found_sushi_in[0].treatment = 'gift'
    #             if found_stake[0].fr == transaction.addr:
    #                 if found_stake[0].amount == 0:
    #                     return Category(Category.CLAIM)
    #                 else:
    #                     found_stake[0].treatment = 'ignore'
    #                     return Category(Category.STAKE, claim_reward=True, certainty=5)
    #             elif found_stake[0].to == transaction.addr:
    #                 found_stake[0].treatment = 'ignore'
    #                 return Category(Category.UNSTAKE, claim_reward=True, certainty=5)










    def process_classification(self,transaction):
        if len(transaction.counter_parties):
            cp_name = list(transaction.counter_parties.values())[0][0]
        elif transaction.interacted is not None:
            cp_name = transaction.interacted
        else:
            cp_name = "UNKNOWN "

        if not isinstance(transaction.type, Category):
            return

        cat = transaction.type.category
        CT = transaction.categorized_transfers

        def set_treatment(transfer, treatment):
            if transfer.treatment is None or transfer.vault_id is None:
                if treatment in ['borrow','repay','full_repay']:
                    transfer.vault_id = cp_name[:6] + " " + transfer.symbol
                else:
                    if transfer.outbound:
                        transfer.vault_id = cp_name[:6] + " " + transfer.to[2:8]  # to
                    else:
                        transfer.vault_id = cp_name[:6] + " " + transfer.fr[2:8]  # fr
            if transfer.treatment is None:
                transfer.treatment = treatment


        if cat == Category.FEE:
            for t in CT[Transfer.SENT]:
                set_treatment(t,'burn')

        elif cat in [Category.DEPOSIT, Category.DEPOSIT_FROM_BRIDGE]:
            for t in CT[Transfer.RECEIVED]:
                set_treatment(t,'buy')

        elif cat in [Category.WITHDRAW, Category.WITHDRAW_TO_BRIDGE]:
            for t in CT[Transfer.SENT]:
                set_treatment(t,'sell')

        elif cat in [Category.SWAP, Category.ADD_LIQUIDITY, Category.REMOVE_LIQUIDITY, Category.MINT_NFT]:
            for t in CT[Transfer.RECEIVED]:
                set_treatment(t,'buy')
            for t in CT[Transfer.SENT]:
                set_treatment(t,'sell')

        elif cat in [Category.ADD_LIQUIDITY_NO_RECEIPT, Category.REMOVE_LIQUIDITY_NO_RECEIPT, Category.STAKE, Category.UNSTAKE]:
            for t in CT[Transfer.RECEIVED]:
                set_treatment(t,'withdraw')
            for t in CT[Transfer.SENT]:
                set_treatment(t,'deposit')

        elif cat in [Category.COMPOUND]:
            for t in CT[Transfer.RECEIVED]:
                set_treatment(t,'income')
            for t in CT[Transfer.SENT]:
                set_treatment(t,'deposit')

        elif cat in [Category.EXIT_VAULT]:
            for t in CT[Transfer.RECEIVED]:
                set_treatment(t,'exit')

        elif cat in [Category.BORROW, Category.REPAY]:
            for t in CT[Transfer.RECEIVED]:
                set_treatment(t,'borrow')
            for t in CT[Transfer.SENT]:
                set_treatment(t,'repay')

        # elif cat in [Category.STAKE, Category.UNSTAKE]:
        #     for t in transaction.transfers:
        #         set_treatment(t, 'ignore')

        elif cat == Category.WRAP:
            t = CT[Transfer.SENT][0]
            set_treatment(t, 'sell')
            extra_transfer = Transfer(len(transaction.transfers), 3, transaction.chain.wrapper, transaction.addr, t.amount, transaction.chain.wrapper, 'W' + t.what, None, -1, 1, t.rate, t.rate_source, 0, treatment='buy', outbound=False, synthetic=True)
            transaction.transfers.append(extra_transfer)

        elif cat == Category.UNWRAP:
            t = CT[Transfer.RECEIVED][0]
            set_treatment(t, 'buy')
            extra_transfer = Transfer(len(transaction.transfers), 3, transaction.addr, transaction.chain.wrapper, t.amount, transaction.chain.wrapper, 'W' + t.what, None, -1, 1, t.rate, t.rate_source, 0,
                                      treatment='sell', outbound=True, synthetic=True)
            transaction.transfers.append(extra_transfer)

        else:
            for t in CT[Transfer.RECEIVED]:
                set_treatment(t,'gift')
            for t in CT[Transfer.SENT]:
                set_treatment(t,'burn')

