from sortedcontainers import *
import pprint
from .util import log, clog
from .category import Category
from .transaction import Transfer
from .pool import Pool, Pools
from collections import defaultdict
import copy
import traceback

class Classifier:
    NULL = '0x0000000000000000000000000000000000000000'
    DEAD = '0x000000000000000000000000000000000000dead'

    def __init__(self):
        self.chain = None #init in classify()
        self.generic_library = {
            (0,0):[self.cl_fee],
            (0,1):[self.cl_adjustment, self.cl_deposit,  self.cl_mint_nft, self.cl_compound],
            (1,1):[self.cl_stake, self.cl_mint_nft,self.cl_borrow,self.cl_repay,self.cl_wrap_unwrap],
            (1,0):[self.cl_adjustment, self.cl_withdraw], #, self.cl_wrap, self.cl_unwrap
            ('?',1):[self.cl_swap,self.cl_add],
            (1,'?'): [self.cl_remove],
            ('?',0):[self.cl_stake,self.cl_vault,self.cl_repay],
            (0, '?'): [self.cl_unstake, self.cl_unvault, self.cl_borrow, self.cl_spam]

        }

        self.address_to_cp_mapping = {
            # '0x7be8076f4ea4a4ad08075c2508e481d6c946d12b':'OpenSea',
            # '0xc36442b4a4522e871399cd717abdd847ab11fe88':'Uniswap V3',
            # '0xe592427a0aece92de3edee1f18e0157c05861564':'Uniswap V3',
            # '0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45':'Uniswap V3',
            # '0xd1c5966f9f5ee6881ff6b261bbeda45972b1b5f3':'Multichain',
            '0xfa9da51631268a30ec3ddd1ccbf46c65fad99251':['MULTICHAIN'],


            #solana
            '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8':['Raydium','Pool/Swap'],
            '5quBtoiQqxF9Jv6KYKctB59NT3gtJD2Y65kdnB1Uev3h': ['Raydium','Pool/Swap'],
            'routeUGWgWzqBWFcrCfv8tritsqukccJPu3q5GPP3xS': ['Raydium','Pool/Swap'],
            'EhhTKczWMGQt46ynNeRX1WfeagwwJd7ufHvCDjRxjo5Q': ['Raydium','Staking-related'],
            '9KEPoZmtHUrBbhWN1v1KWLMkkvwY6WLtAVUCPRtRjP4z': ['Raydium','Staking-related'],
            '9HzJyW1qZsEiSfMUf6L2jo3CcTKAyBmSyKdwQeYisHrC': ['Raydium','Accel'],

            'FC81tbGt6JWRXidaWYFXxGnTk4VgobhJHATvTRVMqgWj':['Francium','Lending'],
            '3Katmm9dhvLQijAvomteYMo6rfVbY5NaCRNq9ZBqBgr6': ['Francium','Reward'],
            '2nAAsYdXF3eTQzaeUQS3fr4o782dDg8L28mX39Wr5j8N': ['Francium','Farms'],
            'DmzAmomATKpNp2rCBfYLS7CSwQqeQTsgRYJA1oSSAJaP': ['Francium','Farms'],

            'Bt2WPMmbwHPk36i4CRucNDyLcmoGdC7xEdrVuxgJaNE6':['Tulip','Farms'],
            '4bcFeLv4nydFrsZqV5CgwCVrPhkQKsXtzfy2KyMz7ozM': ['Tulip','Lending'],
            'FoNqK2xudK7TfKjPFxpzAcTaU2Wwyt81znT4RjJBLFQp': ['Tulip','Orca vaults'],
            'EzSXQ2BXf8m4y4jcQQGeZ6nnwXB3ARXP3YQ5SwjKLj82': ['Tulip','Saber vaults'],
            '7vxeyaXGLqcp66fFShqUdHxdacp4k4kwUpRSSeoZLCZ4': ['Tulip','V1 Radium vaults'],
            '5JQ8Mhdp2wv3HWcfjq9Ts8kwzCAeBADFBDAgBznzRsE4': ['Tulip','Pyth price feed'],
            'TLPv2tuSVvn3fSk8RgW3yPddkp5oFivzZV3rA9hQxtX': ['Tulip','V2 vaults'],
            'stkTLPiBsQBUxDhXgxxsTRtxZ38TLqsqhoMvKMSt8Th': ['Tulip','Tulip staking'],

            'BBbD1WSjbHKfyE3TSFWF6vx1JV51c8msKSQy4ess6pXp':['Allbridge'],
            'bb1XfNoER5QC3rhVDaVz3AJp9oFKoHNHG6PHfZLcCjj': ['Allbridge'],
            'stk8xj8cygGKnFoLE1GL8vHABcHUbYrnPCkxdL5Pr2q':['Allbridge'],

            'cjg3oHmg9uuPsP8D6g29NWvhySJkdYdAo9D25PRbKXJ':['Chainlink'],

            'Zo1ggzTUKMY5bYnDvT5mtVeZxzf2FaLTbKkmvGUhUQk':['Serum','DEX-related'],
            '9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin':['Serum','DEX (v3)'],

            'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K':['Magic Eden', 'Account-related (V2)'],
            'MEisE1HzehtrDpAAT8PnLHjpSSkRYakotTuJRPjTpo8': ['Magic Eden', 'Account-related (V1)'],

            '6UeJYTLU1adaoHWeApWsoj1xNEDbWA2RhM2DLc8CrDDi':['Apricot'],

            'wormDTUJ6AWPNvk59vGQbDvGJmqbDTdgWgAqcLBCgUb':['Wormhole'],
            'worm2ZoG2kUd4vFXhvjh93UUH596ayRfgQ2MgjNMTth':['Wormhole'],

            'mv3ekLzLbnVPNxjSKvqBpU3ZeZXPQdEC3bp5MDEBG68':['Mango','Account-related'],

            'CJsLwbP1iu5DuUikHEJnLfANgKy6stB2uFgvBBHoyxwz':['Solanart','Account-related']
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
            # 'UNISWAP V3': { 'CATCHMENT':self.cl_uniswap_all },
            'UNISWAP V3':{
                'CATCHMENT':self.cl_uniswap_all,
                'multicall':self.cl_uniswap_all,
                'increaseLiquidity':self.cl_uniswap_all,
                'collect':self.cl_uniswap_all,
                'mint':self.cl_uniswap_all
            },
            'MULTICHAIN':{
                'ALL':self.cl_multichain_all
            },

            'Raydium':{
                'Pool/Swap':Category(Category.SWAP, protocol='Raydium'),
                'Staking-related':self.cl_raydium_staking
                # 'REWARD_TOKEN':'4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R'
            },

            'Tulip': {
                'Farms':self.cl_tulip_farms
            },

            'Francium': {
                'Farms':self.cl_francium_farms
            },

            'JUPITER': {
                'CATCHMENT':self.cl_jupiter_swap
            },

            'Mango': {
                'Account': self.cl_mango
            },

            'Magic Eden': {
                'Account-related (V2)': self.cl_magiceden_v2,
                'Account-related (V1)': self.cl_magiceden_v1
            },

            'Serum': {
                'DEX (v3)':self.cl_serum_all
            },

            'Solanart': {
                'Account-related': self.cl_solanart
            }


            # 'TETHER': {
            #     'transfer':self.cl_tether_transfer,
            # }
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

        self.outgoing_transfers = SortedDict() #bridge?
        self.spam_data = {
            'previously_interacted':set(),
            'token_tx_map':{},
            'confirmed_not_spam':set()
        }

        self.prior_transaction = None
        self.prior_withdraw = {}


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

    def match_sig(self,sig,what):
        if sig is not None:
            if self.chain.name == 'Solana':
                if what.lower() in sig.lower():
                    return True
            else:
                if what.lower() == sig.lower():
                    return True
        return False

    def compare(self, transaction):
        if transaction.derived_data is not None and transaction.changed is not None and transaction.changed != "NEW":
            dd = transaction.derived_data
            try:
                if transaction.type is None:
                    category = None
                else:
                    category = Category.mapping[transaction.type.category]

                if dd['category'] is None:
                    old_cat = None
                else:
                    old_cat = Category.mapping[dd['category']]

                if category != old_cat:
                    transaction.changed['Category'] = [old_cat,category]


                for transfer in transaction.transfers.values():
                    if transfer.treatment != transfer.derived_data['treatment']:
                        transfer.changed['Tax treatment'] = [transfer.derived_data['treatment'], transfer.treatment]
                    if transfer.vault_id != transfer.derived_data['vault_id']:
                        transfer.changed['Vault/Loan ID'] = [transfer.derived_data['vault_id'], transfer.vault_id]
            except:
                pass

    def classify(self, transaction):
        logtx = False
        if transaction.hash == transaction.chain.hif or (transaction.hash is not None and transaction.hash in transaction.chain.hif):
            log("Classifying ", transaction.hash)
            logtx = True

        # log("Classifying",transaction.hash,transaction.chain.hif)
        if transaction.derived_data is not None and transaction.changed is None:
            if logtx:
               log("Found ", transaction.hash, "in derived data, not reclassifying")
            dd = transaction.derived_data
            if dd['category'] is not None:
                transaction.type = Category(dd['category'],claim_reward=dd['claim'],nft=dd['nft'],certainty=dd['certainty'],protocol=dd['protocol'])
            else:
                transaction.type = None
            transaction.balanced = dd['balanced']
            transaction.classification_certainty_level = dd['certainty']
            if dd['protocol_note'] is not None:
                transaction.type.protocol_note = dd['protocol_note']
            for transfer in transaction.transfers.values():
                transfer.treatment = transfer.derived_data['treatment']
                transfer.vault_id = transfer.derived_data['vault_id']
            return

        self.chain = transaction.chain
        transaction.type = None
        transaction.classification_certainty_level = 0
        # fee = transaction.lookup({'to':transaction.chain.name +" network"})

        combo = (transaction.out_cnt, transaction.in_cnt)

        # user_addresses_ids = transaction.user.relevant_address_ids


        if transaction.interacted is None:
            interacted = transaction.lookup({'from_me': True,'input_non_zero':True})
            if len(interacted) == 1:
                transaction.interacted = interacted[0].to
            clog(transaction,"assigned interacted in classify",transaction.interacted)

        if transaction.interacted is not None and transaction.my_address(transaction.originator):
            self.spam_data['previously_interacted'].add(transaction.interacted)

        if self.chain.is_upload:
            self.classify_upload(transaction)
            cp = self.chain.name
        else:

            sent_nonzero = transaction.lookup({'from_me': True,'amount_non_zero':True})


            received_nonzero = transaction.lookup({'to_me': True,'amount_non_zero':True})

            self_transfers = transaction.lookup({'to_me': True, 'from_me': True, 'amount_non_zero': True})
            minted = transaction.lookup({'to_me': True, 'fr': Classifier.NULL,'amount_non_zero':True})
            burned = transaction.lookup({'from_me': True, 'to': Classifier.NULL,'amount_non_zero':True})+\
                     transaction.lookup({'from_me': True, 'to': Classifier.DEAD, 'amount_non_zero': True})

            nfts_in = transaction.lookup({'to_me': True, 'type':4})
            nfts_out = transaction.lookup({'from_me': True, 'type':4})

            from_bridge = transaction.lookup({'to_me': True, 'fr': self.chain.inbound_bridges})
            to_bridge = transaction.lookup({'from_me': True, 'to': self.chain.outbound_bridges})

            unstaked = transaction.lookup({'to_me': True,'amount_non_zero':True, 'what': self.chain.pools.receipt_token_list(), 'fr':self.chain.pools.pool_address_list()})
            from_pools = transaction.lookup({'to_me': True,'amount_non_zero':True, 'fr':self.chain.pools.pool_address_list()})

            fees = transaction.lookup({'to': 'network'})



            staked = transaction.lookup({'from_me': True, 'amount_non_zero': True, 'what': self.chain.pools.receipt_token_list()})
            staked = list(set(staked) - set(burned))
            # unstaked = transaction.lookup({'to': transaction.addr,'amount_non_zero':True, 'what': self.chain.pools.receipt_token_list(), 'fr':self.chain.pools.pool_address_list(Pool.STAKING)})
            unvaulted = transaction.lookup({'to_me': True,'amount_non_zero':True, 'what': self.chain.pools.input_token_list(Pool.VAULT), 'fr':self.chain.pools.pool_address_list(Pool.VAULT)})


            if logtx:
                log('from_pools',from_pools)
                log('unstaked', unstaked)
                log('unvaulted', unvaulted)
                log('receipts',self.chain.pools.receipt_token_list())
                log('self', self_transfers)
            rewards = list(set(from_pools) - set(unstaked) - set(unvaulted))

            minted_nfts = transaction.lookup({'to_me': True, 'fr': Classifier.NULL,'amount_non_zero':True,'type':4})

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
                Transfer.SELF: self_transfers,
                # Transfer.INTERACTED: interacted,
                Transfer.MINTED_NFT: minted_nfts
                # Transfer.REDEEMED_LP: redeemed
            }

            #did user send out a token previously identified as spam? Means it wasn't spam.
            for tr in sent_nonzero:
                if tr.what in self.spam_data['token_tx_map']:
                    self.spam_data['confirmed_not_spam'].add(tr.what)
                    for tx in self.spam_data['token_tx_map'][tr.what]:
                        tx.type = None
                        self.classify(tx) #reclassify spam transaction
                        clog(transaction,'reclassifying spam')
                    del self.spam_data['token_tx_map'][tr.what]




            cp_pair = None
            cp = None
            sig = None
            if len(transaction.counter_parties) == 1:
                cp_list = list(transaction.counter_parties.values())
                prog_addr = list(transaction.counter_parties.keys())[0]
                cp, hex_sig, sig, _, cp_addr = cp_list[0]

                if cp_addr in self.address_to_cp_mapping:
                    cp_pair = self.address_to_cp_mapping[cp_addr]
                    transaction.counter_parties[prog_addr][0] = cp = cp_pair[0]


                if prog_addr in self.address_to_cp_mapping:
                    cp_pair = self.address_to_cp_mapping[prog_addr]
                    transaction.counter_parties[prog_addr][0] = cp = cp_pair[0]
            if cp is None and transaction.interacted is not None and transaction.interacted in self.address_to_cp_mapping:
                cp_pair = self.address_to_cp_mapping[transaction.interacted]
                cp = cp_pair[0]
                transaction.counter_parties = {transaction.interacted:[cp, None, None, None, transaction.interacted]}

            if cp_pair is not None and len(cp_pair) > 1:
                k = list(transaction.counter_parties.keys())[0]
                if transaction.function is not None:
                    current_sig = transaction.function
                else:
                    current_sig = transaction.counter_parties[k][1]
                if current_sig is None:
                    current_sig = ""
                transaction.counter_parties[k][1] = transaction.counter_parties[k][2] = sig = cp_pair[1]+" "+current_sig




            if logtx:
                log("counterparty", cp,"sig",sig)


            # if cp is not None:
            #     for custom_code_option in self.cp_mapping.keys():
            #         if custom_code_option.lower() in cp.lower():
            #             cp = custom_code_option
            #             break

            # err = self.check_error(transaction,sig)
            err = None
            if err is None:
                types = []
                rv = self.check_self_transfers(transaction)
                if rv is not None:
                    types.append(rv)
                else:
                    specific_checkers = None
                    if cp is not None and cp in self.cp_mapping:
                        clog(transaction,"specific_checkers",self.cp_mapping[cp])
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



                        # if sig in specific_checkers:
                        for specific_checker_sig in list(specific_checkers.keys()):
                            if self.match_sig(sig,specific_checker_sig):
                                if isinstance(specific_checkers[specific_checker_sig],Category):
                                    types.append(copy.deepcopy(specific_checkers[specific_checker_sig])) #god that was an insane bug. Must have copy here, or it'll overwrite .claim on types used in different transactions.
                                else:
                                    sig_checkers = specific_checkers[specific_checker_sig]
                                    if not isinstance(sig_checkers,list):
                                        sig_checkers = [sig_checkers]
                                    checkers.extend(sig_checkers)


                        clog(transaction,"checkers",checkers)
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
                                # print(combo_opt,self.generic_library[combo_opt])
                                generic_checkers.extend(self.generic_library[combo_opt])

                        for checker in generic_checkers:
                            rv = checker(transaction, sig)
                            if rv is not None:
                                # tp, accuracy = rv
                                types.append(rv)


                    if specific_checkers is not None and 'ALL' in specific_checkers:
                        checker = specific_checkers['ALL']
                        if isinstance(checker, Category):
                            types.append(copy.deepcopy(checker))
                        else:
                            rv = checker(transaction, sig)
                            if rv is not None:
                                types.append(rv)

                    #CATCHMENT is below in priority than generics. For example, "TRANSFER IN" overrides anything from CATCHMENT
                    #this only fires if no other types were discovered earlier
                    if len(types) == 0 and specific_checkers is not None and 'CATCHMENT' in specific_checkers:
                        checker = specific_checkers['CATCHMENT']
                        if isinstance(checker, Category):
                            types.append(copy.deepcopy(checker))
                        else:
                            rv = checker(transaction, sig)
                            clog(transaction, "Catchment outcome", rv)
                            if rv is not None:
                                types.append(rv)


                    #swaps are below in priority
                    if len(types) > 1:
                        certainty_mapping = defaultdict(list)
                        for type in types:
                            certainty_mapping[type.certainty+int(type.protocol is not None)].append(type) #advantage given to identified protocol
                        clog(transaction, "types certainty_mapping", certainty_mapping)
                        types = certainty_mapping[max(list(certainty_mapping.keys()))]

                        types_noswap = []
                        for type in types:
                            if type.category in [Category.WRAP, Category.UNWRAP] and type.certainty == 10:
                                types_noswap = [type]
                                break

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


                if len(types) > 1:
                    log(transaction.hash, "MULTIPLE TYPES")
                    transaction.type = types



            # transaction.type = None

        self.process_classification(transaction)

        self.prior_transaction = transaction

        if logtx:
            log('CL RESULT', transaction.hash, transaction.interacted, transaction.counter_parties, cp, 'category',transaction.type, transaction.classification_certainty_level, combo, len(transaction.transfers))
            log('tx amounts',transaction.amounts)
            for cat in transaction.categorized_transfers.keys():
                log("transfer type",Transfer.name_map[cat],len(transaction.categorized_transfers[cat]))
            log('counterparties',transaction.counter_parties)
            log('transfers')
            for transfer_id, transfer in transaction.transfers.items():
                log(transfer_id,transfer)
            log('\n\n\n')
            # exit(1)

    def check_error(self,transaction,sig):
        CT = transaction.categorized_transfers
        if len(CT[Transfer.ERROR]) > 0:
            return Category(Category.ERROR,certainty=0)

    def check_self_transfers(self,transaction):
        clog(transaction,'check_self_transfers')
        CT = transaction.categorized_transfers
        if len(CT[Transfer.SELF]) > 0:
            certainty = 0
            if len(CT[Transfer.SELF]) == len(CT[Transfer.RECEIVED])+len(CT[Transfer.SENT]):
                certainty = 10
            elif len(CT[Transfer.SELF]) == len(CT[Transfer.RECEIVED])+len(CT[Transfer.SENT])-1 and transaction.fee_transfer is not None:
                certainty = 10
            for transfer in CT[Transfer.SELF]:
                transfer.treatment = 'ignore'
            clog(transaction, 'check_self_transfers',len(CT[Transfer.SELF]),len(transaction.transfers), 'self cat assigned',certainty)
            return Category(Category.SELF, certainty=certainty)

    def check_reward(self, transaction, sig):
        CT = transaction.categorized_transfers

        if hasattr(transaction,'reward_token'):
            reward_token = transaction.reward_token
            reward_transfers = transaction.lookup({'what':reward_token,'to_me': True})

            clog(transaction,"cr check reward token",len(reward_transfers))
            if len(reward_transfers) > 0:
                clog(transaction,"cr crw ",len(reward_transfers))
                for rew in reward_transfers:
                    rew.treatment = 'income'
                if len(reward_transfers) == len(CT[Transfer.RECEIVED]) and len(CT[Transfer.SENT]) == 0:
                    if transaction.type is None:
                        transaction.type = Category(Category.CLAIM,certainty=10)
                    elif isinstance(transaction.type,Category):
                        if transaction.type.category is None:
                            transaction.type.category = Category.CLAIM

                        clog(transaction,"cr crw setting cf", len(reward_transfers))
                        transaction.type.claim = True
            return


        if isinstance(transaction.type, Category):
            if not (transaction.type.category in [Category.STAKE, Category.UNSTAKE, Category.ADD_LIQUIDITY, Category.REMOVE_LIQUIDITY, Category.REMOVE_LIQUIDITY_NO_RECEIPT, Category.ADD_LIQUIDITY_NO_RECEIPT, Category.EXIT_VAULT]):
                return

            #no space for a reward
            if len(CT[Transfer.RECEIVED])+len(CT[Transfer.SENT]) == 1:
                return



        do_claim = False
        clog(transaction,"cr XI1")

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

            clog(transaction,"cr XI2",rew,pools)
            if len(pools) > 1: #sometimes there's a 0-valued transfer specifying which LP token was used to stake
                lp_indicator = transaction.lookup({'from_me': True, 'amount_non_zero': False, 'type':3})
                clog(transaction,"cr XI21", lp_indicator)
                if len(lp_indicator) == 1:
                    lp_token = lp_indicator[0].what
                    # pools_by_token = self.chain.pools.map['I'][lp_token]
                    pools_by_token = self.chain.pools.pool_list('I',lp_token)
                    pools = pools.intersection(pools_by_token)
                    clog(transaction,"cr XI22", pools)
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
                        clog(transaction,"cr XI23", pools)

        #reward token is minted, not sent from pool
        elif len(CT[Transfer.MINTED]) > 0 and transaction.interacted is not None and not self.check_sig(sig,'mint') and not self.check_sig(sig,'borrow'):
            max_certainty = 3
            interactor = transaction.interacted
            pools = self.chain.pools.pool_list('A', interactor)
            rew = CT[Transfer.MINTED][0]
            clog(transaction,"cr XI241",'pools', len(pools), pools,interactor)

            # pools = self.chain.pools.map['A'][rew.fr]

        # reward token is sent from a different address
        elif (len(CT[Transfer.RECEIVED]) >= 1 and (self.check_sig(sig,'claim') or self.check_sig(sig,'reward') or self.check_sig(sig,'withdraw') or self.check_sig(sig,'harvest')))\
                or (len(CT[Transfer.UNVAULTED]) >= 1 and self.check_sig(sig, 'deposit')):
            if self.check_sig(sig, 'deposit'):
                max_certainty = 3
            clog(transaction,"cr XI243", 'pools', len(pools), pools)
            interactor = transaction.interacted
            reward_transfers = set(CT[Transfer.RECEIVED])-set(CT[Transfer.UNSTAKED_LP])-set(CT[Transfer.UNVAULTED])
            if len(reward_transfers) > 0:
                clog(transaction,"cr XI244")
                prew_list = sorted(list(reward_transfers), key=lambda x: x.id, reverse=False) #need consistent order
                # rew = list(reward_transfers)[0]
                for prew in prew_list:
                    if prew.treatment is None:
                        rew = prew
                        break
                pools = self.chain.pools.pool_list('A', interactor)


        if len(pools) >= 1:
            pool = list(pools)[0]
            clog(transaction,"cr XI3",pool,rew.what, pool.deposited)
            if (pool.type in [Pool.STAKING,Pool.VAULT] or self.check_sig(sig,'reward')) or self.check_sig(sig,'claim') and rew.what not in pool.deposited:
                clog(transaction,"cr XI4")
                do_claim = True


        if do_claim:
            if len(pools) == 1 and rew is not None:
                list(pools)[0].issue_reward(rew)
                certainty = max_certainty
            else:
                certainty = 3
            if rew is not None:
                rew.treatment = 'income'
            if rew2 is not None:
                rew2.treatment = 'income'

            clog(transaction,"cr XI5")
            clog(transaction,"rew1",rew)
            clog(transaction,"rew2",rew2)

            full_cat = False
            if transaction.type is None:
                clog(transaction,"cr XI61")

                transaction.type = Category(Category.CLAIM,certainty=certainty)
                full_cat = True
            elif isinstance(transaction.type,Category):
                clog(transaction,"cr XI62",transaction.type,transaction.type.category)
                if transaction.type.category is None:
                    transaction.type.category = Category.CLAIM
                    full_cat = True
                transaction.type.claim = True

            clog(transaction,"cr XI7, full_cat",full_cat)

            # if full_cat and rew is not None:
            #     found_transfers = transaction.lookup({'to': transaction.addr,'amount_non_zero':True,'what':rew.what})
            #     for transfer in found_transfers:
            #         transfer.treatment = 'income'
            if full_cat and rew is not None:
                found_transfers = transaction.lookup({'to_me': True,'amount_non_zero':True})
                for transfer in found_transfers:
                    transfer.treatment = 'income'


    # def cl_00(self,transaction,sig):
    #     return Category(Category.FEE)

    def cl_fee(self, transaction, sig):
        return Category(Category.FEE)

    def cl_adjustment(self,transaction,sig):
        CT = transaction.categorized_transfers
        if self.check_sig(sig,'mismatch fix'):
            return Category(Category.BALANCE_ADJUSTMENT)

    def cl_deposit(self,transaction,sig):
        CT = transaction.categorized_transfers

        transfers = list(transaction.transfers.values())
        if transaction.hash == transaction.chain.hif:
            log("dep check bridge",len(transaction.transfers),transfers[0].rate_found,len(CT[Transfer.NFT_IN]))




        if len(CT[Transfer.RECEIVED]) and not len(CT[Transfer.SENT]) and not len(CT[Transfer.UNSTAKED_LP]):# and not len(CT[Transfer.NFT_IN]):
            if (len(CT[Transfer.FROM_BRIDGE]) or len(CT[Transfer.RECEIVED]) == 1) and (len(CT[Transfer.MINTED]) == 0 or (len(transfers) == 1 and transfers[0].rate_found == 1 and not len(CT[Transfer.NFT_IN]))):

                certainty = 5
                match_found = False
                for transfer in CT[Transfer.RECEIVED]:
                    if transaction.hash == transaction.chain.hif:
                        log('bridge 2')
                    match_found, match_certainty = self.find_outgoing_bridge_match(transaction,transfer)
                    if match_found:
                        certainty = match_certainty
                        break
                if match_found or len(CT[Transfer.FROM_BRIDGE]):
                    return Category(Category.DEPOSIT_FROM_BRIDGE, certainty=certainty)

            if not len(CT[Transfer.NFT_IN]):
                # if transaction.interacted in [None,'11111111111111111111111111111111','TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA','ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL']:
                if transaction.interacted is None or transaction.function is None or transaction.chain.name == 'Solana':
                    certainty = 5
                    if transaction.interacted not in [None, '11111111111111111111111111111111', 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA', 'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL']:
                        certainty = 3
                    return Category(Category.DEPOSIT, certainty=certainty)



    def cl_wrap_unwrap(self, transaction, sig):
        CT = transaction.categorized_transfers
        if transaction.interacted == transaction.chain.wrapper and len(CT[Transfer.SENT]) == 1 and len(CT[Transfer.RECEIVED]) == 1:
            if sig == 'withdraw':
                return Category(Category.UNWRAP, certainty=10)
            if sig == 'deposit':
                return Category(Category.WRAP, certainty=10)
        # if sig != 'withdraw':
        #     return
        # CT = transaction.categorized_transfers
        # if transaction.interacted == transaction.chain.wrapper:
        #     return Category(Category.UNWRAP, certainty=10)
        #
        # if len(CT[Transfer.RECEIVED]) and (CT[Transfer.RECEIVED][0].fr == transaction.chain.wrapper):
        #     return Category(Category.UNWRAP, certainty=10)


    def cl_withdraw(self,transaction,sig):
        if sig in ['stake','deposit']:
            return
        if self.check_sig(sig,'repay'):
            return
        CT = transaction.categorized_transfers
        if len(CT[Transfer.SENT]) and not len(CT[Transfer.RECEIVED]) and not len(CT[Transfer.STAKED_LP]):
            if len(CT[Transfer.TO_BRIDGE]) or len(CT[Transfer.SENT]) == 1:
                for transfer in CT[Transfer.SENT]:
                    if transfer.amount != 0:
                        clog(transaction,"Adding to outgoing transfers",transfer)
                        self.outgoing_transfers[transaction.ts] = {'chain':self.chain,'transfer':transfer,'matched':False,'transaction':transaction}

                if len(CT[Transfer.TO_BRIDGE]):
                    return Category(Category.WITHDRAW_TO_BRIDGE, certainty=5)

            # if transaction.interacted in [None,'11111111111111111111111111111111','TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA','ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL']:
            if transaction.interacted is None or transaction.chain.name == 'Solana':
                certainty = 5
                if transaction.interacted not in [None, '11111111111111111111111111111111', 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA', 'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL']:
                    certainty = 3
                return Category(Category.WITHDRAW, certainty=certainty)

    # def cl_wrap(self, transaction, sig):
    #     if sig == 'deposit' and transaction.interacted == transaction.chain.wrapper:
    #         return Category(Category.WRAP, certainty=10)
        # if sig != 'deposit':
        #     return None
        # CT = transaction.categorized_transfers
        # if len(CT[Transfer.SENT]) and CT[Transfer.SENT][0].to == transaction.chain.wrapper:
        #     return Category(Category.WRAP, certainty=10)



    def cl_swap(self,transaction,sig):
        if sig is not None:
            if sig in ['mint']:# or self.check_sig(sig,'liquidity'):
                return

            if self.check_sig(sig,'swap') or self.check_sig(sig,'exchange'):
                return Category(Category.SWAP, certainty=10)
            elif self.check_sig(sig,'migrate'):
                return Category(Category.SWAP, certainty=5)

        CT = transaction.categorized_transfers
        if len(CT[Transfer.RECEIVED]) == 1 and (len(CT[Transfer.SENT]) == 1 or (len(CT[Transfer.SENT]) >= 1 and self.chain.name == 'Solana') ) \
                and not len(CT[Transfer.MINTED]) and not len(CT[Transfer.BURNED]):
            if len(CT[Transfer.SENT]) > 1:
                return Category(Category.SWAP, certainty=5)
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
        CT = transaction.categorized_transfers
        if self.check_sig(sig,'borrow') and not self.check_sig(sig,'repay'):
            minted = CT[Transfer.MINTED]
            for transfer in minted:
                transfer.treatment='ignore'
            return Category(Category.BORROW, certainty=5)

    def cl_repay(self, transaction, sig):
        CT = transaction.categorized_transfers
        if self.check_sig(sig,'repay'):
            burned = CT[Transfer.BURNED]
            for transfer in burned:
                transfer.treatment='ignore'
            incoming = CT[Transfer.RECEIVED]
            claim = False
            if len(incoming) > 0:
                for transfer in incoming:
                    transfer.treatment='income'
                claim = True
            return Category(Category.REPAY, certainty=5, claim_reward=claim)

    def cl_spam(self, transaction, sig):
        CT = transaction.categorized_transfers
        received = CT[Transfer.RECEIVED]

        clog(transaction,'spam? 0',transaction.interacted, len(received) == len(transaction.transfers),transaction.interacted is not None,not transaction.my_address(transaction.originator),transaction.interacted not in self.spam_data['previously_interacted'])

        if len(received) == len(transaction.transfers) \
                and transaction.interacted is not None \
                and not transaction.my_address(transaction.originator) \
                and transaction.interacted not in self.spam_data['previously_interacted']:
            for tr in received:
                if tr.what in self.spam_data['confirmed_not_spam']:
                    return
                if tr.fr in self.spam_data['previously_interacted'] or transaction.my_address(tr.fr):
                    return
                if tr.coingecko_id is not None:
                    return
                if tr.rate_found != 0:
                    return
                clog(transaction, 'spam? 1',tr.token_nft_id is not None,
                     transaction.user.current_tokens is not None,
                     self.chain.name in transaction.user.current_tokens,
                     tr.what in transaction.user.current_tokens[self.chain.name][tr.to])

                clog(transaction, 'spam 2?', self.chain.name, tr.what, transaction.user.current_tokens[self.chain.name][tr.to])

                if tr.token_nft_id is not None:
                    if transaction.user.current_tokens is not None:
                        try:
                            floor = transaction.user.current_tokens[self.chain.name][tr.to][tr.what]['rate'][1]
                            clog(transaction, 'floor',floor)
                            if floor > 1.:
                                return
                        except:
                            pass

            processed_tokens = set()
            for tr in received:
                tok = tr.what
                if tok not in processed_tokens:
                    if tok not in self.spam_data['token_tx_map']:
                        self.spam_data['token_tx_map'][tok] = []
                    self.spam_data['token_tx_map'][tok].append(transaction)
                    processed_tokens.add(tok)

            return Category(Category.SPAM, certainty=10)



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
        clog(transaction,"uniswap cl_uniswap_all")

        CT = transaction.categorized_transfers

        if len(transaction.amounts) == 2 and len(CT[Transfer.SENT]) >= 1 and len(CT[Transfer.RECEIVED]) >= 1 \
                and not len(CT[Transfer.MINTED]) and not len(CT[Transfer.BURNED])\
                and not len(CT[Transfer.NFT_IN]) and not len(CT[Transfer.NFT_OUT]):
            return Category(Category.SWAP, certainty=10, protocol='UNISWAP V3')

        tokens = set()
        for tr in transaction.transfers.values():
            if tr.to == 'network' or tr.amount == 0:
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
        for tr in transaction.transfers.values():
            tr.vault_id = vault_id

        if len(CT[Transfer.SENT]) > 0:
            return Category(Category.ADD_LIQUIDITY_NO_RECEIPT,certainty=certainty, protocol='UNISWAP V3')
        else:
            return Category(Category.REMOVE_LIQUIDITY_NO_RECEIPT, certainty=certainty, protocol='UNISWAP V3')



    def find_outgoing_bridge_match(self,transaction,transfer,protocol=None):
        outgoing_timestamps = list(self.outgoing_transfers.keys())
        for outgoing_idx in range(0, len(self.outgoing_transfers)):
            outgoing_ts = outgoing_timestamps[outgoing_idx]
            # print(outgoing_ts,transaction.ts)
            if outgoing_ts > transaction.ts + 30: #leeway for some chains that have wrong scanner times. See also "timing_adjustment" in user.py
                break
            if outgoing_ts < transaction.ts - 86400 * 7:
                continue

            if self.outgoing_transfers[outgoing_ts]['matched']:
                continue

            tdiff = transaction.ts - outgoing_ts

            candidate = self.outgoing_transfers[outgoing_ts]['transfer']
            candidate_tx = self.outgoing_transfers[outgoing_ts]['transaction']

            clog(transaction,'bridge 3')
            # print("CANDIDATE",candidate)
            # print(candidate.symbol, transfer.symbol, candidate.amount, transfer.amount)
            proceed = 0
            if transaction.chain.name != candidate_tx.chain.name and candidate.type not in [Transfer.ERC721,Transfer.ERC1155] and transfer.type not in [Transfer.ERC721,Transfer.ERC1155]:
                if candidate.symbol == transfer.symbol or \
                        'W' + candidate.symbol == transfer.symbol or \
                        candidate.symbol == 'W' + transfer.symbol or \
                        (candidate.coingecko_id == transfer.coingecko_id and transfer.coingecko_id != None):
                    proceed = 1

                elif (candidate.coingecko_id is not None and transfer.coingecko_id is not None and
                        ('bridged-' in candidate.coingecko_id or 'bridged-' in transfer.coingecko_id or 'wrapped-' in candidate.coingecko_id or 'wrapped-' in transfer.coingecko_id)
                      and ((transfer.rate != 0 and transfer.rate is not None and candidate.rate != 0 and candidate.rate is not None and abs(transfer.rate/candidate.rate-1) < 0.03) or candidate.coingecko_id in transfer.coingecko_id
                      or transfer.coingecko_id in candidate.coingecko_id) ):
                    proceed = 1

                if proceed:
                    clog(transaction,'bridge 4')
                    if transfer.amount <= candidate.amount and transfer.amount > 0.97 * candidate.amount:
                        clog(transaction,'bridge 5')
                        self.outgoing_transfers[outgoing_ts]['matched'] = True
                        for tr in candidate_tx.transfers.values():
                            if tr.coingecko_id is None and 'ANY' in tr.symbol: #multichain
                                tr.treatment = 'ignore'
                        if transfer.amount != candidate.amount or transfer.coingecko_id != candidate.coingecko_id:
                            candidate.treatment = 'deposit'
                            candidate.vault_id = 'bridge_' + str(outgoing_idx)
                            transfer.treatment = 'exit'
                            transfer.vault_id = 'bridge_' + str(outgoing_idx)
                            if protocol is None:
                                certainty = 5
                            else:
                                certainty = 10
                        else:
                            certainty = 10
                            candidate.treatment = 'ignore'
                            transfer.treatment = 'ignore'
                        candidate_tx.type = Category(Category.WITHDRAW_TO_BRIDGE,certainty=certainty,protocol=protocol)
                        self.process_classification(candidate_tx)
                        return True, certainty
        return False, 0

    def cl_multichain_all(self,transaction,sig):
        CT = transaction.categorized_transfers
        send_to_bridge = 0
        receive_from_bridge = 0
        clog(transaction, "multichain 0")
        if len(CT[Transfer.SENT]) == 1 and len(CT[Transfer.RECEIVED]) == 0:
            self.outgoing_transfers[transaction.ts] = {'chain': self.chain, 'transfer': CT[Transfer.SENT][0], 'matched': False,'transaction':transaction}

        elif len(CT[Transfer.RECEIVED]) == 1 and len(CT[Transfer.SENT]) == 0:
            certainty = 0
            candidate = CT[Transfer.RECEIVED][0]
            match_found, match_certainty = self.find_outgoing_bridge_match(transaction, candidate, protocol='MULTICHAIN')
            clog(transaction, "multichain 4", match_found, match_certainty)
            if match_found:
                certainty = match_certainty
            return Category(Category.DEPOSIT_FROM_BRIDGE, certainty=certainty, protocol='MULTICHAIN')

        elif len(CT[Transfer.MINTED]) == 1 and len(CT[Transfer.BURNED]) == 1: #ANY*
            mint = CT[Transfer.MINTED][0]
            burn = CT[Transfer.BURNED][0]
            mint.treatment = 'ignore'
            burn.treatment = 'ignore'
            candidate = None
            clog(transaction,"multichain 1")
            if mint.what == burn.what and mint.amount == burn.amount:
                for transfer in transaction.transfers.values():
                    if transfer.to_me and transfer.fr != Classifier.NULL:
                        receive_from_bridge += 1
                        candidate = transfer
                    if transfer.from_me and transfer.to != Classifier.NULL and transfer.to != 'network' and transfer.amount > 0:
                        candidate = transfer
                        send_to_bridge += 1
            clog(transaction, "multichain 2","recv",receive_from_bridge,"send",send_to_bridge)

            #this is done in find_outgoing_bridge_match
            if send_to_bridge == 1 and receive_from_bridge == 0:
                self.outgoing_transfers[transaction.ts] = {'chain': self.chain, 'transfer': candidate, 'matched': False,'transaction':transaction}
                # return Category(Category.WITHDRAW_TO_BRIDGE, certainty=10, protocol='MULTICHAIN')

            if receive_from_bridge ==1 and send_to_bridge == 0:
                certainty = 0
                clog(transaction, "multichain 2")
                match_found, match_certainty = self.find_outgoing_bridge_match(transaction, candidate, protocol='MULTICHAIN')
                clog(transaction, "multichain 3",match_found,match_certainty)
                if match_found:
                    certainty = match_certainty

                return Category(Category.DEPOSIT_FROM_BRIDGE, certainty=certainty, protocol='MULTICHAIN')








    def cl_compound_repaybehalf(self, transaction,sig):
        CT = transaction.categorized_transfers
        for tr in CT[Transfer.SENT]:
            tr.treatment = 'repay'
        for tr in CT[Transfer.RECEIVED]:
            tr.treatment = 'income'
        return Category(Category.REPAY, certainty=10, protocol='COMPOUND')

    def cl_tether_transfer(self, transaction,sig):
        CT = transaction.categorized_transfers
        if len(transaction.lookup({'from_me':True,'symbol':'USDT'})):
            return Category(Category.WITHDRAW, certainty=10, protocol='TETHER')
        if len(transaction.lookup({'to_me': True,'symbol':'USDT'})):
            return Category(Category.DEPOSIT, certainty=10, protocol='TETHER')

    def cl_raydium_staking(self, transaction, sig):
        CT = transaction.categorized_transfers
        if len(CT[Transfer.SENT]) == 1 and len(CT[Transfer.RECEIVED]) == 0:
            tr = CT[Transfer.SENT][0]
            if '-' in tr.symbol:
                return Category(Category.STAKE, certainty=5, protocol='RAYDIUM')
        if len(CT[Transfer.RECEIVED]) in [1,2] and len(CT[Transfer.SENT]) == 0:
            cat = None
            reward = False
            for tr in CT[Transfer.RECEIVED]:
                if '-' in tr.symbol:
                    cat = Category(Category.UNSTAKE, certainty=5, protocol='RAYDIUM')
                elif tr.symbol == 'RAY':
                    tr.treatment = 'income'
                    reward = True
            if reward:
                clog(transaction, 'cr Raydium claim')
                if cat is not None:
                    cat.claim = True
                else:
                    cat = Category(Category.CLAIM,certainty=5,protocol='RAYDIUM')
            return cat

    def cl_tulip_farms(self,transaction,sig):
        CT = transaction.categorized_transfers
        if len(CT[Transfer.SENT]) in [1,2] and len(CT[Transfer.RECEIVED]) == 0:
            for tr in CT[Transfer.SENT]:
                tr.vault_id = 'Tulip '+tr.to[:6]
            return Category(Category.ADD_LIQUIDITY_NO_RECEIPT, certainty=5, protocol='TULIP')

        if len(CT[Transfer.RECEIVED]) == 1 and len(CT[Transfer.SENT]) == 0:# and not self.check_sig(sig,'repay'):
            if CT[Transfer.RECEIVED][0].symbol == 'TULIP':
                clog(transaction, 'cr Tulip claim')
                return Category(Category.CLAIM, certainty=5, protocol='TULIP')

        if len(CT[Transfer.RECEIVED]) in [1,2] and len(CT[Transfer.SENT]) == 0:# and self.check_sig(sig,'repay'):
            for tr in CT[Transfer.RECEIVED]:
                tr.vault_id = 'Tulip '+tr.fr[:6]
            return Category(Category.EXIT_VAULT, certainty=5, protocol='TULIP')



    def cl_francium_farms(self,transaction,sig):
        CT = transaction.categorized_transfers
        if len(CT[Transfer.SENT]) in [1,2] and len(CT[Transfer.RECEIVED]) == 0:
            for tr in CT[Transfer.SENT]:
                tr.vault_id = 'Francium '+tr.to[:6]
            return Category(Category.ADD_LIQUIDITY_NO_RECEIPT, certainty=5, protocol='FRANCIUM')

        if len(CT[Transfer.RECEIVED]) in [1,2] and len(CT[Transfer.SENT]) == 0:
            for tr in CT[Transfer.RECEIVED]:
                tr.vault_id = 'Francium '+tr.fr[:6]
            return Category(Category.EXIT_VAULT, certainty=5, protocol='FRANCIUM')

    def cl_jupiter_swap(self, transaction, sig):
        CT = transaction.categorized_transfers
        if len(CT[Transfer.RECEIVED]) >= 1 and len(CT[Transfer.SENT]) >= 1:
            return Category(Category.SWAP, certainty=5, protocol='JUPITER')

    def cl_mango(self, transaction, sig):
        CT = transaction.categorized_transfers
        if len(CT[Transfer.RECEIVED]) == 1 and len(CT[Transfer.SENT]) == 0:
            return Category(Category.REMOVE_LIQUIDITY_NO_RECEIPT, certainty=5, protocol='MANGO')

        if self.check_sig(sig, 'createaccount'):
            for tr in CT[Transfer.SENT]:
                if tr.what == 'SOL' and tr.amount < 0.05:
                    tr.treatment = 'sell'
            if len(CT[Transfer.SENT]) == 1:
                return Category(Category.FEE, certainty=5, protocol='MANGO')

        if self.check_sig(sig,'transfer') and (len(CT[Transfer.RECEIVED]) == 0 and (len(CT[Transfer.SENT]) == 1 or self.check_sig(sig,'createaccount'))):
            return Category(Category.ADD_LIQUIDITY_NO_RECEIPT, certainty=5, protocol='MANGO')

    def cl_magiceden_v1(self, transaction, sig):
        CT = transaction.categorized_transfers
        if len(CT[Transfer.SENT]) == 0:
            for tr in CT[Transfer.RECEIVED]:
                tr.vault_id = 'Magic Eden V1'
                tr.treatment = 'withdraw'
            return Category(Category.REMOVE_LIQUIDITY_NO_RECEIPT, certainty=3, protocol='MAGIC EDEN')
        if len(CT[Transfer.RECEIVED]) == 0:
            for tr in CT[Transfer.SENT]:
                tr.vault_id = 'Magic Eden V1'
                tr.treatment = 'deposit'
            return Category(Category.ADD_LIQUIDITY_NO_RECEIPT, certainty=3, protocol='MAGIC EDEN')

    def cl_magiceden_v2(self, transaction, sig):
        CT = transaction.categorized_transfers
        if len(CT[Transfer.RECEIVED]) == 1 and len(CT[Transfer.SENT]) == 0:
            return Category(Category.REMOVE_LIQUIDITY_NO_RECEIPT, certainty=5, protocol='MAGIC EDEN')
        if len(CT[Transfer.RECEIVED]) == 0 and len(CT[Transfer.SENT]) == 1:
            return Category(Category.ADD_LIQUIDITY_NO_RECEIPT, certainty=5, protocol='MAGIC EDEN')

    def cl_solanart(self, transaction, sig):
        CT = transaction.categorized_transfers
        if len(CT[Transfer.SENT]) == 0:
            for tr in CT[Transfer.RECEIVED]:
                tr.vault_id = 'Solanart'
                tr.treatment = 'withdraw'
            return Category(Category.REMOVE_LIQUIDITY_NO_RECEIPT, certainty=3, protocol='Solanart')
        if len(CT[Transfer.RECEIVED]) == 0:
            for tr in CT[Transfer.SENT]:
                tr.vault_id = 'Solanart'
                tr.treatment = 'deposit'
            return Category(Category.ADD_LIQUIDITY_NO_RECEIPT, certainty=3, protocol='Solanart')


    def cl_serum_all(self, transaction, sig):

        CT = transaction.categorized_transfers
        log(transaction.hash, "in cl_serum_all",len(CT[Transfer.RECEIVED]), len(CT[Transfer.SENT]))
        if len(CT[Transfer.RECEIVED]) == 1 and len(CT[Transfer.SENT]) == 2:
            return Category(Category.SWAP, certainty=5, protocol='SERUM')

        if len(CT[Transfer.RECEIVED]) == 0 and len(CT[Transfer.SENT]) in [1,2]:
            return Category(Category.ADD_LIQUIDITY_NO_RECEIPT, certainty=5, protocol='SERUM')

        # make the final withdrawal from the vault an exit, otherwise withdraw
        if len(CT[Transfer.RECEIVED]) in [1,2] and len(CT[Transfer.SENT]) == 0:
            vault_id = CT[Transfer.RECEIVED][0].fr
            if vault_id in self.prior_withdraw and self.prior_withdraw[vault_id].type.category == Category.EXIT_VAULT:
                prior_tx = self.prior_withdraw[vault_id]
                prior_tx.type.category = Category.REMOVE_LIQUIDITY_NO_RECEIPT
                CT = prior_tx.categorized_transfers
                for tr in CT[Transfer.RECEIVED]:
                    tr.treatment = None
                self.process_classification(prior_tx)

            self.prior_withdraw[vault_id] = transaction
            return Category(Category.EXIT_VAULT, certainty=3, protocol='SERUM')

        # if self.prior_transaction is None:
        #     return
        #
        # prior = self.prior_transaction


        # if transaction.ts - prior.ts < 120:
        #     cp_name = cp_name_prior = None
        #     if len(transaction.counter_parties):
        #         cp_name = list(transaction.counter_parties.values())[0][0]
        #     if len(prior.counter_parties):
        #         cp_name_prior = list(prior.counter_parties.values())[0][0]
        #     if cp_name is not None and cp_name == cp_name_prior:
        #         CT_prior = prior.categorized_transfers
        #         if len(CT[Transfer.SENT]) == 0 and len(CT[Transfer.RECEIVED]) in [1,2,3] and len(CT_prior[Transfer.SENT]) in [1,2] and len(CT_prior[Transfer.RECEIVED]) == 0:
        #             prior.type = Category(Category.MULTISWAP, certainty=5, protocol='SERUM')
        #             return Category(Category.MULTISWAP, certainty=5, protocol='SERUM')





    def process_classification(self,transaction):
        if isinstance(transaction.type,Category):
            transaction.classification_certainty_level = transaction.type.certainty
        else:
            transaction.classification_certainty_level = 0

        cp_name = "unknown"
        if len(transaction.counter_parties):
            cp_name = list(transaction.counter_parties.values())[0][0]
        if cp_name == "unknown" and transaction.interacted is not None:
            cp_name = transaction.interacted

        def set_treatment(transfer, treatment):
            if transfer.treatment is None or transfer.vault_id is None:
                if treatment in ['borrow','repay','full_repay']:
                    transfer.vault_id = cp_name[:6] + " " + transfer.symbol
                else:
                    transfer.set_default_vaultid(cp_name)
            if transfer.treatment is None:
                transfer.treatment = treatment


        #
        # if transaction.hash == transaction.chain.hif:
        #     print("proc clf",cp_name,transaction.interacted)

        if not isinstance(transaction.type, Category):
            if transaction.fee_transfer is not None:
                set_treatment(transaction.fee_transfer, 'fee')
            return

        cat = transaction.type.category
        CT = transaction.categorized_transfers
        deductible_fee = True



        # clog(transaction,"in process_classification","category",cat,"sent transfer",CT[Transfer.SENT])
        if cat == Category.FEE:
            deductible_fee = False
            for t in CT[Transfer.SENT]:
                set_treatment(t,'fee')
            for t in transaction.transfers.values():
                if t.synthetic == Transfer.FEE: #these are not in CT[Transfer.SENT] -- they are not mapped
                    set_treatment(t, 'fee')

        elif cat == Category.BALANCE_ADJUSTMENT:
            for t in CT[Transfer.SENT]:
                set_treatment(t,'burn')
            for t in CT[Transfer.RECEIVED]:
                set_treatment(t,'gift')

        elif cat in [Category.DEPOSIT, Category.DEPOSIT_FROM_BRIDGE]:
            for t in CT[Transfer.RECEIVED]:
                set_treatment(t,'buy')

        elif cat in [Category.WITHDRAW, Category.WITHDRAW_TO_BRIDGE]:
            for t in CT[Transfer.SENT]:
                set_treatment(t,'sell')

        elif cat in [Category.SWAP, Category.ADD_LIQUIDITY, Category.REMOVE_LIQUIDITY, Category.MINT_NFT, Category.WRAP, Category.UNWRAP]:
            for t in CT[Transfer.RECEIVED]:
                set_treatment(t,'buy')
            for t in CT[Transfer.SENT]:
                set_treatment(t,'sell')

        elif cat in [Category.ADD_LIQUIDITY_NO_RECEIPT, Category.REMOVE_LIQUIDITY_NO_RECEIPT, Category.STAKE, Category.UNSTAKE]:
            deductible_fee = False
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
            deductible_fee = False
            for t in CT[Transfer.RECEIVED]:
                set_treatment(t,'exit')

        elif cat in [Category.BORROW, Category.REPAY]:
            deductible_fee = False
            for t in CT[Transfer.RECEIVED]:
                set_treatment(t,'borrow')
            for t in CT[Transfer.SENT]:
                set_treatment(t,'repay')
        elif cat == Category.DEDUCTIBLE_LOSS:
            for t in CT[Transfer.SENT]:
                set_treatment(t,'burn')
        elif cat == Category.LOAN_INTEREST:
            for t in CT[Transfer.SENT]:
                set_treatment(t,'interest')
        elif cat == Category.BUSINESS_EXPENSE:
            for t in CT[Transfer.SENT]:
                set_treatment(t,'expense')
        elif cat == Category.INCOME:
            for t in CT[Transfer.RECEIVED]:
                set_treatment(t,'income')

        elif cat == Category.SPAM:
            for t in CT[Transfer.RECEIVED]:
                set_treatment(t,'ignore')

        else:
            for t in CT[Transfer.RECEIVED]:
                set_treatment(t,'gift')
            for t in CT[Transfer.SENT]:
                set_treatment(t,'burn')

        if transaction.fee_transfer is not None:
            set_treatment(transaction.fee_transfer, 'fee')
            # if deductible_fee:
            #     set_treatment(transaction.fee_transfer,'fee')
            # else:
            #     set_treatment(transaction.fee_transfer, 'sell')





    def classify_upload(self,transaction):
        function = transaction.function
        sent_nonzero = transaction.lookup({'from_me': True, 'amount_non_zero': True})
        received_nonzero = transaction.lookup({'to_me': True, 'amount_non_zero': True})
        sold = transaction.lookup({'to': 'counter-trader', 'amount_non_zero': True})
        bought = transaction.lookup({'fr': 'counter-trader', 'amount_non_zero': True})

        # fee transfers (the ones with synthetic=fee) aren't mapped
        # additional_fees = transaction.lookup({'from_me': True, 'to':transaction.chain.name+ ' fee', 'amount_non_zero': True})

        transaction.categorized_transfers = {
            Transfer.RECEIVED: received_nonzero,
            Transfer.SENT: sent_nonzero,
        }

        type = None
        if function == 'trade' and len(sold) > 0 and len(bought) > 0:
            transaction.balanced = True
            certainty = 5
            if len(transaction.transfers) - len(sold)-len(bought) in [0,1]:
                certainty = 10
            for transfer in transaction.transfers.values():
                if transfer['to'] == transaction.chain.name+' fee':
                    transfer.treatment = 'fee'

            type = Category(Category.SWAP, certainty=certainty)



        elif function in ['income','chain-split','interest','mining','airdrop','cashback','royalty','royalties','staking']:
            if len(sent_nonzero) == 0:
                type = Category(Category.INCOME, certainty=10)

        elif function in ['send','fiat-withdrawal'] or (function == 'trade' and len(sold) > 0):
            if len(received_nonzero) == 0:
                type = Category(Category.WITHDRAW,  certainty=5)
                if function == 'send' and len(sent_nonzero) == 1:
                    self.outgoing_transfers[transaction.ts] = {'chain': transaction.chain, 'transfer': sent_nonzero[0], 'matched': False,'transaction':transaction}

        elif function in ['receive','fiat-deposit','gift'] or (function == 'trade' and len(bought) > 0):
            if len(sent_nonzero) == 0:
                clog(transaction, "upload bridge 0")
                type = Category(Category.DEPOSIT, certainty=5)
                if function == 'receive' and len(received_nonzero) == 1:
                    clog(transaction, "upload bridge 1")
                    match_found, match_certainty = self.find_outgoing_bridge_match(transaction, received_nonzero[0])
                    if match_found:
                        type = Category(Category.DEPOSIT_FROM_BRIDGE, certainty=match_certainty)



        elif function in ['stolen','lost','burn','liquidate']:
            if len(received_nonzero) == 0:
                type = Category(Category.DEDUCTIBLE_LOSS, certainty=5)

        elif function == 'borrow':
            if len(sent_nonzero) == 0:
                certainty = 3
                if len(received_nonzero) == 1:
                    certainty = 10

                for transfer in received_nonzero:
                    transfer.treatment = 'borrow'
                    transfer.vault_id = transaction.chain.name+' loan'
                type = Category(Category.BORROW, certainty=certainty)

        elif function == 'loan-repayment':
            if len(received_nonzero) == 0:
                certainty = 3
                if len(sent_nonzero) == 1:
                    certainty = 10

                for transfer in sent_nonzero:
                    transfer.treatment = 'repay'
                    transfer.vault_id = transaction.chain.name + ' loan'
                type = Category(Category.REPAY, certainty=certainty)

        elif function == 'to-vault':
            if len(received_nonzero) == 0:
                certainty = 5
                if len(sent_nonzero) == 1:
                    certainty = 10

                for transfer in sent_nonzero:
                    transfer.treatment = 'deposit'
                    transfer.vault_id = transaction.chain.name + ' vault'
                type = Category(Category.ADD_LIQUIDITY_NO_RECEIPT, certainty=certainty)

        elif function == 'from-vault':
            if len(sent_nonzero) == 0:
                certainty = 5
                if len(received_nonzero) == 1:
                    certainty = 10

                for transfer in received_nonzero:
                    transfer.treatment = 'withdraw'
                    transfer.vault_id = transaction.chain.name+' vault'
                type = Category(Category.REMOVE_LIQUIDITY_NO_RECEIPT, certainty=certainty)

        elif function == 'margin-fee':
            if len(received_nonzero) == 0:
                certainty = 3
                if len(sent_nonzero) == 1:
                    certainty = 5
                type = Category(Category.LOAN_INTEREST, certainty=certainty)

        elif function == 'expense':
            if len(received_nonzero) == 0:
                certainty = 3
                if len(sent_nonzero) == 1:
                    certainty = 5
                type = Category(Category.BUSINESS_EXPENSE, certainty=certainty)

        elif function in ['realized-profit','realized-loss']:
            margin_fees = transaction.lookup({'to': transaction.chain.name+" margin fee"})

            ft = None
            for rtr in received_nonzero:
                for str in sent_nonzero:
                    if rtr.what == str.what:
                        ft = str.what
                        break
            if ft != None:
                if function == 'realized-profit':
                    for transfer in received_nonzero:
                        if transfer.what == ft:
                            transfer.treatment = 'gift'
                        else:
                            transfer.treatment = 'buy'
                    for transfer in sent_nonzero:
                        transfer.treatment = 'sell'
                else:
                    for transfer in received_nonzero:
                        transfer.treatment = 'buy'
                    for transfer in sent_nonzero:
                        if transfer.what == ft:
                            transfer.treatment = 'burn'
                        else:
                            transfer.treatment = 'sell'
                type = Category(Category.PNL_CHANGE, certainty=5)
                transaction.balanced = True

            for mf in margin_fees:
                mf.treatment = 'interest'
        elif function == 'fee':
            certainty = 0
            if len(received_nonzero) == 0:
                certainty = 10
            type = Category(Category.FEE, certainty=certainty)


        transaction.type = type