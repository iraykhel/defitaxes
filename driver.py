
import os
import traceback
import time
import json
from code.coingecko import Coingecko
from code.chain import Chain
from code.util import log
from code.sqlite import *
import pickle
from code.main import *
from code.signatures import *
from code.user import User
from code.tax_calc import Calculator


def update_rates_db():
    C = Coingecko()
    # C.download_symbols_to_db()
    C.download_all_coingecko_rates()

if __name__ == "__main__":
    # update_rates_db()
    # exit(0)
    # S = Signatures()
    # S.download_signatures_to_db()
    # exit(0)
    # formalize_names('BSC')
    # exit(0)

    # C = Coingecko()
    # # C.download_symbols_to_db()
    # C.download_all_coingecko_rates()
    address = '0xd603a49886c9b500f96c0d798aed10068d73bf7c'

    # chain = Chain('ETH', 'https://api.etherscan.io/api', 'ETH', 'ABGDZF9A4GIPCHYZZS4FVUBFXUPXRDZAKQ',
    #               outbound_bridges=['0XA0C68C638235EE32657E8F720A23CEC1BFC77C77',  # polygon
    #                                 '0X40EC5B33F54E0E8A33A975908C5BA1C14E5BBBDF',  # polygon
    #                                 '0X59E55EC322F667015D7B6B4B63DC2DE6D4B541C3'],  # bsc
    #               inbound_bridges=['0X56EDDB7AA87536C09CCC2793473599FD21A8B17F'], addr=address)

    # address = '0x5fe41f8e36b1b2c72aa0091626841c989d90b0d9'
    # address = '0x712d0f306956a6a4b4f9319ad9b9de48c5345996'
    # address = '0x22fa8cc33a42320385cbd3690ed60a021891cb32'
    # address = '0x083fc10ce7e97cafbae0fe332a9c4384c5f54e45' #k06a

    # address = '0x6867115787080d4e95cbcb6471fa85a9458a5e43' #subvert
    # address = '0x3401ea5a8d91c5e3944962c0148b08ac4a77f153' #so many nfts
    name = 'BSC'
    address_db = SQLite('addresses')
    chain = Chain.from_name(name,address_db,address)

    # rs = chain.scrape_address_info('0xd603a49886c9b500f96c0d798aed10068d73bf7c')
    # print(rs)
    # chain.update_address_from_scan('0x7e9997a38a439b2be7ed9c9c4628391d3e055d48')
    # exit(0)

    # r = chain.get_progenitor('0x694351f6dafe5f2e92857e6a3c0578b68a8c1435')
    # print(r)
    # exit(0)


    #
    # progenitor = chain.get_progenitor('0x97efe8470727fee250d7158e6f8f63bb4327c8a2', address_db)
    # print(progenitor)
    # exit(0)
    user = User(address)
    transactions = chain.get_transactions()
    user.store_transactions(chain,transactions)


    transactions = user.load_transactions(chain)

    contract_list, counterparty_list, input_list = chain.get_contracts(transactions)

    chain.update_progenitors(user, counterparty_list)
    # print("counter list",counterparty_list)

    # for contract in counterparty_list:
    #     progenitor = chain.get_progenitor(contract.lower())
    #     if progenitor is None:
    #         print("No progenitor for",contract.lower())

    C = Coingecko()

    t = time.time()
    C.init_from_db(chain.main_asset, contract_list, address)
    C.dump(chain)
    log('timing:coingecko init_from_db', time.time() - t)

    S = Signatures()
    S.init_from_db(input_list)

    tl = time.time()
    log('timing:coingecko save', time.time() - tl)


    transactions = chain.transactions_to_log(user, C,S, transactions,mode='js')
    # print('all transactions')
    # print(transactions)

    C = Coingecko.init_from_cache(chain)
    calculator = Calculator(user,chain,C,mtm=True)
    tax_info = calculator.process_transactions(transactions)
    calculator.matchup()

    # calculator.summary(CA_short)