# from numpy import hstack
# import sys
# sys.path.append('/home/ubuntu/.local/lib/python3.6/site-packages')
# import numpy

from flask import Flask, render_template, request, send_file, session, g
import os



import traceback
import time
import json
from code.coingecko import Coingecko
from code.signatures import Signatures
from code.chain import Chain
from code.solana import Solana
from code.util import log, ProgressBar, persist, sql_in, normalize_address, is_ethereum, log_error
from code.sqlite import SQLite
from code.fiat_rates import Twelve
# from code.category import Typing
from code.user import User, Import
from code.tax_calc import Calculator
import pickle
import html
import sys,pip
import redis
from code.redis_wrap import Redis
import secrets
import threading
import copy
from collections import defaultdict
import atexit
from dotenv import load_dotenv

FLASK_ENV = 'production'

app = Flask(__name__)

os.environ['debug'] = '0'
os.environ['version'] = '1.42'
os.environ['app_path'] = '/home/ubuntu/hyperboloid'

def init():
    os.chdir(os.environ.get('app_path')) if FLASK_ENV == "production" else False
    load_dotenv()
    log('env check',os.environ.get('api_key_etherscan'),filename='env_check.txt')

@app.route('/')
@app.route('/main')
def main():
    init()
    address_cookie = request.cookies.get('address')
    address = ""
    if address_cookie is not None:
        # log("address_cookie",address_cookie)
        if "|" in address_cookie:
            address,_ = address_cookie.split("|")
        else:
            address = address_cookie


        primary = normalize_address(address)#Address(address)
        persist(primary)

    blockchain_count = len(Chain.CONFIG)
    # log('cookie',address,chain_name)
    return render_template('main.html', title='Blockchain transactions to US tax form', address=address, blockchain_count=blockchain_count, version=os.environ.get('version'))

@app.route('/services.html')
def services_page():
    return render_template('services.html', title='Services we provide', version=os.environ.get('version'))

@app.route('/chains.html')
def chain_support():
    init()
    chains_support_info = []
    support_level_text_map = {10:'High',5:'Medium',3:'Low',0:'None'}
    for chain_name in Chain.list(alphabetical=True):
        conf = Chain.CONFIG[chain_name]
        support_level = conf['support']
        support_level_text = support_level_text_map[support_level]

        data_source_url = "https://"+conf['scanner']
        data_source_name = conf['scanner']

        erc1155_support = 0
        if '1155_support' in conf:
            erc1155_support = conf['1155_support']

        balance_token_support = 'Available'
        if 'debank_mapping' in conf and conf['debank_mapping'] is None:
            balance_token_support = 'Not available'

        balance_nft_support = 'Not available'
        if 'simplehash_mapping' in conf:
            balance_nft_support = 'Available'

        cp_availability = 3
        if 'blockscout' in conf:
            cp_availability = 0
        if 'cp_availability' in conf:
            cp_availability = conf['cp_availability']

        if chain_name == 'Solana':
            data_source_url = "https://www.quicknode.com/"
            data_source_name = "QuickNode RPC"
            balance_nft_support = 'Available'

        chains_support_info.append(
            {'name':chain_name,
             'support_level':support_level,
             'support_level_text':support_level_text,
             'data_source_name':data_source_name,
             'data_source_url': data_source_url,
             'cp_availability': support_level_text_map[cp_availability],
             'erc1155_support':support_level_text_map[erc1155_support],
             'balance_token_support':balance_token_support,
             'balance_nft_support': balance_nft_support
             })
    log('chains_support_info',chains_support_info,filename='chain_support.txt')
    return render_template('chains.html', title='Blockchain transactions to US tax form', chains=chains_support_info, version=os.environ.get('version'))

@app.route('/last_update')
def last_update():
    init()
    address = request.args.get('address')	
    primary = normalize_address(address)#Address(address)
    if primary is None:
        persist(primary)
        data = {'last_transaction_timestamp': 0,'update_import_needed':False}
    else:
        # redis = Redis(primary, None)
        # qpos = redis.qpos()
        # if qpos is not None:
        #     data = {'queue_position': qpos}
        # else:
        persist(primary)
        user = User(primary)
        update_import_needed = False
        try:
            last = last_update_inner(user)
            # update_import_needed = user.check_info('update_import_needed')
            data_version = float(user.get_info('data_version'))
            log('version comp', data_version,user.version)
            # if data_version != user.version:
            if user.version - data_version >= 0.1:
                update_import_needed = True
        except:
            last = user.last_db_modification_timestamp
            update_import_needed = True
        data = {'last_transaction_timestamp':last,'update_import_needed':update_import_needed}
    data = json.dumps(data)
    return data

def last_update_inner(user):
    query = "SELECT max(last_update) FROM user_addresses WHERE address='"+user.address+"'"
    rows = user.db.select(query)
    log('last_update_inner',rows)
    if len(rows) == 0 or rows[0][0] == None:
        return 0
    return int(rows[0][0])



@app.route('/process')
def process():
    init()
    address = request.args.get('address')
    primary = normalize_address(address)#Address(address)
    # chain_name = request.args.get('chain')
    uid = request.args.get('uid')
    # import_new = int(request.args.get('import_new'))

    # U = Users()
    # primary = U.lookup(address)
    # if primary is None:
    #     primary = address
    # all_previous_addresses = U.all_addresses(primary)
    # U.disconnect()



    persist(primary)
    redis = Redis(primary)



    #currently importing transactions for this user -- wait to finish, then recreate from stored
    if redis.waitself():
        return recreate_data_from_caches(primary)

    redis.start()


    # progress_bar_update(address, 'Starting', 0)

    active_address = None
    pb = None
    try:
        # accepted_chains_str = request.args.get('accepted_chains')
        accepted_chains_str = None

        user = User(primary, do_logging=False)
        all_previous_addresses = list(user.all_addresses.keys())
        log('all_previous_addresses 1', all_previous_addresses)
        # if len(all_previous_addresses) == 0:
        #     all_previous_addresses = [primary]

        all_chains = {}
        for chain_name in Chain.list():
            chain = user.chain_factory(chain_name)
            all_chains[chain_name] = {'chain': chain, 'import_addresses': [], 'display_addresses': set(), 'is_upload':False}

        if 'my account' in all_previous_addresses: #uploads
            for chain_name in user.all_addresses['my account']:
                chain = user.chain_factory(chain_name,is_upload=True)
                all_chains[chain_name] = {'chain': chain, 'import_addresses': [], 'display_addresses': set(), 'is_upload': True}

        log('all chains',all_chains)



        import_addresses = request.args.get('import_addresses')
        log('import_addresses provided',import_addresses)
        if import_addresses is not None and import_addresses != '':
            if import_addresses == 'all':
                if len(all_previous_addresses) == 0:
                    import_addresses = [primary]
                else:
                    import_addresses = all_previous_addresses
            else:
                import_addresses = import_addresses.split(",")
        # else:
        #     import_addresses = [primary]
        elif len(all_previous_addresses) == 0:
            import_addresses = [primary]
        else:
            import_addresses = []
        log('import_addresses processed',import_addresses)


        use_previous = True
        try:
            ac_str = request.args.get('ac_str')
            log('ac_str',ac_str)
            if ac_str is not None and ac_str != '':
                ac_spl = ac_str.split(",")
                for entry in ac_spl:
                    chain_name,address = entry.split(":")
                    all_chains[chain_name]['display_addresses'].add(normalize_address(address))
                use_previous = False
        except:
            pass

        if use_previous:
            for address in user.all_addresses:
                for chain_name in user.all_addresses[address]:
                    if user.all_addresses[address][chain_name]['used']:
                        all_chains[chain_name]['display_addresses'].add(address)



        # display_addresses = request.args.get('display_addresses')
        # display_addresses = None
        # if display_addresses is not None and display_addresses != '':
        #     display_addresses = display_addresses.split(",")
        # else:
        #     #if no explicit display address request, get the ones displayed previously
        #     display_addresses = []
        #     for previous_address in all_previous_addresses:
        #         for chain_name, chain_data in all_chains.items():
        #             chain = chain_data['chain']
        #             if chain.check_validity(previous_address):
        #                 if user.check_address_used(previous_address,chain_name):
        #                     display_addresses.append(previous_address)
        #                     break

        #everything that's imported must be displayed
        for address in import_addresses:
            address = normalize_address(address)
            if address not in all_previous_addresses:
                all_previous_addresses.append(address)

            for chain_name, chain_data in all_chains.items():
                chain = chain_data['chain']
                if chain.check_validity(address):
                    chain_data['display_addresses'].add(address)



            # if address not in display_addresses:
            #     display_addresses.append(address)

        # if len(display_addresses) == 0:
        #     display_addresses = all_previous_addresses

        #everything that's displayed must have been previously imported
        # relevant_addresses = []
        # for address in display_addresses:
        #     address = normalize_address(address)
        #     if address not in import_addresses and address not in all_previous_addresses:
        #         log("appending display address",address,"to import addresses", "previous", all_previous_addresses)
        #         import_addresses.append(address)
        #     relevant_addresses.append(normalize_address(address))
        #
        # user.relevant_addresses = relevant_addresses
        # user.all_addresses = all_previous_addresses

        all_display_addresses = set()
        for chain_name, chain_data in all_chains.items():
            chain_data['display_addresses'] = list(chain_data['display_addresses'])
            log('display addresses for', chain_name, chain_data['display_addresses'])
            for address in chain_data['display_addresses']:
                if address not in import_addresses and address not in all_previous_addresses:
                    import_addresses.append(address)
                all_display_addresses.add(address)
        all_display_addresses = list(all_display_addresses)



        # chain_list = Chain.list()
        log('req args',request.args)
        log('all_previous_addresses 2',all_previous_addresses)
        if 'my account' in import_addresses:
            import_addresses.remove('my account')
        log('import_addresses',import_addresses)
        log('all_chains',all_chains)


        import_new = len(import_addresses) > 0

        # if import_new:
        #     display_chains = all_chains
        # if accepted_chains_str is None or len(accepted_chains_str) == 0: #not specified -- use previously used chains for these addresses
        #     display_chains = {}
        #     for chain_name, chain_data in all_chains.items():
        #         chain = chain_data['chain']
        #         for address in display_addresses:
        #             if chain.check_validity(address):
        #                 if user.check_address_used(address, chain_name):
        #                     display_chains[chain_name] = all_chains[chain_name]
        #                     break
        # else:
        #     accepted_chains = accepted_chains_str.split(",")
        #     display_chains = {}
        #     for chain_name in Chain.list():
        #         if chain_name in accepted_chains:
        #             display_chains[chain_name] = all_chains[chain_name]
        #
        # if len(display_chains) == 0:
        #     display_chains = all_chains
        #
        # log('display chains', display_chains)

        S = Signatures()






        # chain = Chain.from_name(chain_name,address_db,address)

        pb = ProgressBar(redis)
        pb.set('Starting',0)
        t = time.time()


        user.get_custom_rates()
        #
        # try:
        #     Coingecko.init_from_cache(user)
        # except:
        #     log("coingecko cache fail")
        #     import_new = True
        #     import_addresses = all_display_addresses

        non_fatal_errors = set()

        use_derived = False
        force_forget_derived = user.check_info('force_forget_derived')
        log('force_forget_derived',force_forget_derived)
        if import_new:
            # user.set_info('update_import_needed',0)
            user.set_info('data_version',user.version)
            user.start_import(all_chains)

            redis.enq(reset=False)
            redis.wait(pb=0)

            pb.update('Updating FIAT rates',0.1)
            user.fiat_rates.download_all_rates()

            # chain_sets_to_check = defaultdict(dict)
            for chain_name, chain_data in all_chains.items():
                chain = chain_data['chain']
                chain_data['addresses_to_check'] = {} #going to send a request to scanner for each address in here

                for active_address in import_addresses:
                    active_address = normalize_address(active_address)
                    if not chain.is_upload and not chain.check_validity(active_address):
                        continue

                    present = user.check_address_present(active_address, chain_name)
                    if present:
                        user.set_address_used(active_address, chain_name)
                        chain_data['import_addresses'].append(active_address)
                    else:
                        chain_data['addresses_to_check'][active_address] = False


                        # chain_sets_to_check[chain_name][active_address] = False




            #called in threads, to check in parallel against all scanners
            def check_chain_for_addresses(chain_data):
                chain = chain_data['chain']
                address_dict = chain_data['addresses_to_check']
                for active_address in address_dict.keys():
                    try:
                        log('checking address present on chain', chain.name, active_address)
                        present = chain.check_presence(active_address)
                        log('checked address present on chain',chain.name,active_address,'present?', present)
                        if present:
                            address_dict[active_address] = True
                    except:
                        user.current_import.add_error(Import.PRESENCE_CHECK_FAILURE, chain=chain, address=active_address, debug_info=traceback.format_exc())
                        log('failed to check chain',chain.name,'for address',active_address,traceback.format_exc())
                        chain_data['failure'] = True
                        return

            pb.update('Checking supported chains for your addresses')
            threads = []
            for chain_name, chain_data in all_chains.items():
                if not chain_data['is_upload'] and len(chain_data['addresses_to_check']) > 0:
                    t = threading.Thread(target=check_chain_for_addresses, args=(chain_data,))
                    threads.append(t)
                    t.start()

            joined_cnt = 0
            for t in threads:
                t.join()
                joined_cnt += 1
                pb.update('Checking supported chains for your addresses: '+str(joined_cnt)+'/'+str(len(threads)), 5. / len(threads))

            for chain_name, chain_data in all_chains.items():
                chain = chain_data['chain']
                chain.progress_bar = pb
                if 'failure' in chain_data:

                    err = "We were not able to retrieve transactions from " + chain_name + ", " + chain.domain + " might be down or API non-functional. Transactions from " + chain.name + \
                                              " may be missing or outdated."
                    non_fatal_errors.add(err)
                else:
                    for checked_address in chain_data['addresses_to_check'].keys():
                        if chain_data['addresses_to_check'][checked_address]:
                            chain_data['import_addresses'].append(checked_address)
                            user.set_address_present(checked_address, chain_name)
                            user.set_address_used(checked_address, chain_name)

                log("import addresses per chain",chain_name, chain_data['import_addresses'])


                # chain = chain_data['chain']
                #
                # for active_address in import_addresses:
                #     active_address = normalize_address(active_address)
                #     if not chain.check_validity(active_address):
                #         continue
                #
                #     log('importing address',chain_name,active_address)
                #
                #     pb.update('Checking ' + chain_name + ' for ' + active_address, 5. / (len(all_chains) * len(import_addresses)))
                #     present = False
                #     if chain_name not in faulty_chains:
                #         chain.progress_bar = pb
                #         # present = user.check_info(chain_name + "_" + active_address + "_presence")
                #         present = user.check_address_present(active_address,chain_name)
                #
                #         if not present:
                #             try:
                #                 present = chain.check_presence(active_address)
                #                 log('present?',present)
                #             except:
                #                 present = False
                #                 faulty_chains.append(chain_name)
                #                 err = "We were not able to retrieve transactions from "+chain_name+", "+chain.domain+" might be down or API non-functional. Transactions from "+chain.name+\
                #                       " may be missing or outdated."
                #                 non_fatal_errors.add(err)
                #     if present:
                #         chain_data['import_addresses'].append(active_address)
                #         user.set_address_present(active_address,chain_name)
                #     if chain_name in display_chains:
                #         user.set_address_used(active_address,chain_name)

        else:
            previous_use = set()
            for address in user.all_addresses:
                for chain_name in user.all_addresses[address]:
                    if user.all_addresses[address][chain_name]['used']:
                        previous_use.add(chain_name + ":" + address)
                        user.set_address_used(address, chain_name, value=0) #unset all address use


            current_use = set()
            for chain_name, chain_data in all_chains.items():
                for address in chain_data['display_addresses']:
                    current_use.add(chain_name+":"+address)
                    user.set_address_used(address, chain_name) #set current address use


            if not force_forget_derived:
                log("comparing previous use vs current use",str(previous_use),str(current_use))
                rows = user.db.select("SELECT id FROM transactions_derived LIMIT 1")
                if previous_use == current_use and len(rows) > 0:
                    use_derived = True

        log('import_new',import_new)
        log('use_derived',use_derived)




        total_request_count = 0
        total_request_count_disp = 0
        for chain_name, chain_data in all_chains.items():
            chain = chain_data['chain']
            if chain.is_upload:
                continue

            addresses = chain_data['import_addresses']
            for active_address in addresses:
                if chain.check_validity(active_address):
                    total_request_count += 1

            disp_addresses = list(chain_data['display_addresses'])
            for active_address in disp_addresses:
                if chain.check_validity(active_address):
                    total_request_count_disp += 1
        log('total_request_count',total_request_count,total_request_count_disp)


        if import_new or not use_derived:
            C = Coingecko(verbose=True)
            C.make_contracts_map()


        if import_new:
            pb.set('Importing transactions',5)
            try:
                redis.cleanup()
            except:
                log_error("EXCEPTION trying to cleanup redis", primary)

            user.current_import.populate_addresses(user,all_chains)



            def threaded_transaction_processing(chain_data):
                addresses = chain_data['import_addresses']
                chain = chain_data['chain']
                for active_address in addresses:
                    active_address = normalize_address(active_address)
                    log('checking validity',chain.name,active_address)
                    if chain.check_validity(active_address):
                        log('is valid')
                        try:
                            transactions = chain.get_transactions(user, active_address, 28. / total_request_count)
                        except:
                            log_error("FAILED TO GET TRANSACTIONS FROM "+chain.name+" FOR ADDRESS "+active_address)
                            user.current_import.add_error(Import.UNKNOWN_ERROR,chain=chain,address=active_address,debug_info=traceback.format_exc())
                            # chain_data['errors'][active_address] = set(["failed to get transactions, code problem"])
                            continue
                        log('retrieved transactions',chain.name,active_address,len(transactions))
                        chain.correct_transactions(active_address, transactions, 2. / total_request_count)
                        current_tokens = chain.get_current_tokens(active_address)
                        # chain_data['transactions'].update(transactions)  #PROBLEM IF CROSS-WALLET TRANSFERS! Newly-downloaded overwrite previous. Need to merge transfers by only adding new ones.
                        for txhash, transaction in transactions.items():
                            if txhash not in chain_data['transactions']:
                                chain_data['transactions'][txhash] = transaction
                            else:
                                chain.merge_transaction(transaction,chain_data['transactions'][txhash])



                        # if len(errors) > 0:
                        #     chain_data['errors'][active_address] = errors
                        if current_tokens is not None:
                            chain_data['current_tokens'][active_address] = current_tokens
                            log("populated current_tokens",chain.name,active_address, len(current_tokens),filename='solana.txt')
                        else:
                            log("populated current_tokens - NONE!", chain.name, active_address)

            def threaded_covalent(all_chains):
                rq_cnt = 0
                for chain_name, chain_data in all_chains.items():
                    if not chain_data['is_upload'] and 'covalent_mapping' in Chain.CONFIG[chain_name]:
                        rq_cnt += len(chain_data['import_addresses'])

                if rq_cnt > 0:
                    for chain_name, chain_data in all_chains.items():
                        chain = chain_data['chain']
                        if chain.is_upload:
                            continue
                        chain.covalent_download(chain_data, pb_alloc=5/float(rq_cnt))

            t_covalent = threading.Thread(target=threaded_covalent, args=(all_chains,)) #this asshole is the longest
            t_covalent.start()


            threads = []
            user.load_solana_nfts()


            for chain_name, chain_data in all_chains.items():
                chain_data['transactions'] = {}
                chain_data['current_tokens'] = {}
                # chain_data['errors'] = {}
                if len(chain_data['import_addresses']) > 0 and not chain_data['is_upload']:
                    log('calling threaded_transaction_processing', chain_name)
                    t = threading.Thread(target=threaded_transaction_processing, args=(chain_data,))
                    threads.append(t)
                    t.start()



            joined_cnt = 0
            for t in threads:
                t.join()
                joined_cnt += 1





            def threaded_balances(all_chains):
                user.get_thirdparty_data(all_chains, progress_bar=pb) #alloc 5

            # pb.set('Loading additional data', 40)

            t_balances = threading.Thread(target=threaded_balances, args=(all_chains,))
            t_balances.start()

            t_balances.join()
            t_covalent.join()

            for chain_name, chain_data in all_chains.items():
                chain = chain_data['chain']
                if chain.is_upload:
                    continue
                chain.covalent_correction(chain_data)
                chain.balance_provider_correction(chain_data)


            pb.update('Loading coingecko symbols', 0)
            try:
                C.download_symbols_to_db(drop=True, progress_bar=pb)  # alloc 3
            except:
                log_error("Failed to download coingecko symbols", primary)

            pb.update('Storing transactions,',0)

            for chain_name, chain_data in all_chains.items():
                user.store_transactions(chain_data['chain'], chain_data['transactions'], chain_data['import_addresses'],C)
                log('storing transactions', chain_name, len(chain_data['transactions']))
                user.store_current_tokens(chain_data['chain'], chain_data['current_tokens'])

                for active_address in chain_data['import_addresses']:
                    active_address = normalize_address(active_address)
                    user.set_address_update(active_address,chain_name)

            user.store_solana_nfts()


        pb.set('Loading transactions from database',50)

        user.load_addresses()
        user.load_tx_counts()
        if import_new or not use_derived:

            pb.update('Loading transactions')
            transactions, _ = user.load_transactions(all_chains, load_derived=True)
            log("loaded transactions",len(transactions),filename='derived.txt')
            pb.update('Loading known counterparties')
            contract_dict, counterparty_by_chain, input_list = user.get_contracts(transactions)

            address_db = SQLite('addresses', read_only=True)
            for chain_name, chain_data in all_chains.items():
                if not chain_data['is_upload'] and len(chain_data['display_addresses']):
                    pb.update('Loading known counterparties for '+chain_name)
                    chain_data['chain'].init_addresses(address_db,counterparty_by_chain[chain_name])
            address_db.disconnect()

            pb.set('Looking up unknown counterparties')
            if total_request_count == 0:
                total_request_count = total_request_count_disp
            def threaded_update_progenitors(chain_name,chain_data,filtered_counterparty_list):
                chain = chain_data['chain']
                try:
                    chain_db_writes = chain.update_progenitors(filtered_counterparty_list, 10. / total_request_count)  # alloc 10
                    # all_db_writes.extend(chain_db_writes)
                    chain_data['progenitor_db_writes'] = chain_db_writes
                    log('new writes', chain_name, len(chain_db_writes), filename='address_update.txt')
                except:
                    log_error('error updating progenitors', primary, chain_name, traceback.format_exc())

            threads = []
            for chain_name, chain_data in all_chains.items():
                chain = chain_data['chain']
                if not chain.blockscout and not chain.is_upload:
                    filtered_counterparty_list = chain.filter_progenitors(list(counterparty_by_chain[chain_name]))
                    log('filtered_counterparty_list', chain_name, filtered_counterparty_list, filename='address_update.txt')
                    if len(filtered_counterparty_list) > 0:
                        t = threading.Thread(target=threaded_update_progenitors, args=(chain_name,chain_data,filtered_counterparty_list))
                        threads.append(t)
                        t.start()

            joined_cnt = 0
            for t in threads:
                t.join()
                joined_cnt += 1

            all_db_writes = []
            for chain_name, chain_data in all_chains.items():
                if 'progenitor_db_writes' in chain_data:
                    all_db_writes.extend(chain_data['progenitor_db_writes'])

            if len(all_db_writes):
                insert_cnt = 0
                address_db = SQLite('addresses')
                for write in all_db_writes:
                    chain_name, values = write
                    cn = chain_name.upper().replace(" ","_")
                    entity = values[-2]
                    address_to_add = values[0]
                    rc = address_db.insert_kw(cn + '_addresses', values=values, ignore=(entity == 'unknown'))
                    if rc > 0:
                        address_db.insert_kw(cn + '_labels', values=[address_to_add, 'auto'], ignore=True)
                        insert_cnt += 1
                if insert_cnt > 0:
                    address_db.commit()
                    log('New addresses added',insert_cnt, filename='address_lookups.txt')
                address_db.disconnect()




            t = time.time()
            log('contract_dict',contract_dict)
            S.init_from_db(input_list)

            pb.set('Loading coingecko rates', 63)
            needed_token_times = user.get_needed_token_times(transactions)
            log("needed_token_times",needed_token_times)

            C.init_from_db_2(all_chains,needed_token_times, progress_bar=pb)

            # C.init_from_db(all_chains, contract_dict, progress_bar=pb) #alloc 17
        else:
            pb.update('Loading transactions')
            transactions, _ = user.load_transactions(all_chains, load_derived=True)
            needed_token_times = user.get_needed_token_times(transactions)
            try:
                C = Coingecko.init_from_cache(user)
                for coingecko_id in needed_token_times:
                    assert coingecko_id in C.rates
            except:
                C = Coingecko(verbose=True)
                pb.set('Loading coingecko symbols', 60)
                try:
                    C.download_symbols_to_db(drop=True, progress_bar=pb)  # alloc 3
                except:
                    log_error("Failed to download coingecko symbols", primary)

                pb.set('Loading coingecko rates', 63)
                C.make_contracts_map()
                C.init_from_db_2(all_chains, needed_token_times, progress_bar=pb)
            S = None

        # if import_new:
        #     pb.set('Loading coingecko rates', 63)
        #     needed_token_times = user.get_needed_token_times(transactions)
        #     C.init_from_db_2(all_chains, needed_token_times, progress_bar=pb)
        # else:
        #     transactions, _ = user.load_transactions(all_chains, load_derived=True)
        #     try:
        #         C = Coingecko.init_from_cache(user)
        #     except:
        #         C = Coingecko(verbose=True)
        #         pb.set('Loading coingecko symbols', 60)
        #         try:
        #             C.download_symbols_to_db(drop=True, progress_bar=pb)  # alloc 3
        #         except:
        #             log_error("Failed to download coingecko symbols", primary)
        #
        #         pb.set('Loading coingecko rates', 63)
        #         needed_token_times = user.get_needed_token_times(transactions)
        #         C.init_from_db_2(all_chains, needed_token_times, progress_bar=pb)
        #     S = None

        if import_new:
            user.finish_import()
        current_tokens = user.load_current_tokens(C)
        log("loaded current tokens",current_tokens)

        # user.load_last_import(all_chains)



        if import_new:
            redis.deq()


        tl = time.time()


        log("coingecko initialized",C.initialized)
        pb.set('Classifying transactions', 80)
        store_derived = import_new or not use_derived
        if store_derived:
            user.wipe_derived_data()
        # if len(transactions) == 0:
        #     data = {'error': 'We didn\'t find any transactions for this address.'}
        #     data = json.dumps(data)
        #     return data

        transactions_js = user.transactions_to_log(C,S, transactions,progress_bar=pb, store_derived=store_derived) #alloc 10
        log("all transactions", transactions_js)


        # T = Typing()
        # builtin_types = T.load_builtin_types()
        pb.set('Loading custom types', 90)
        custom_types = user.load_custom_types()

        pb.set('Calculating taxes', 90)
        calculator = Calculator(user, C)
        calculator.process_transactions(transactions_js, user) #alloc

        #process_transactions affects coingecko rates! Need to cache it after, not before.
        C.dump(user)

        pb.set('Calculating taxes', 95)
        calculator.matchup()
        pb.set('Calculating taxes', 97)
        calculator.cache()

        js_file = open('data/users/' + primary + '/transactions.json', 'w', newline='')
        js_file.write(json.dumps(transactions_js,indent=2, sort_keys=True))
        js_file.close()

        info_fields = ['tx_per_page','high_impact_amount','dc_fix_shutup','matchups_visible','fiat','opt_tx_costs','opt_vault_gain','opt_vault_loss']
        info = {}
        for field in info_fields:
            value = user.get_info(field)
            if value is not None:
                info[field] = value


        # address_info = {}
        # for address in all_previous_addresses:
        #     address_info[address] = {'displayed':address in display_addresses,'imported':address in import_addresses}

        # for chain_name, chain_data in all_chains.items():
        #     if 'errors' in chain_data:
        #         for address, errors in chain_data['errors'].items():
        #             if len(all_previous_addresses) == 1:
        #                 prefix = "Problem on " + chain_name
        #             else:
        #                 prefix = "Problem with " + address + " on " + chain_name
        #             for err in list(errors):
        #                 non_fatal_errors.add(prefix+": "+err)

        non_fatal_errors = non_fatal_errors.union(set(user.load_relevant_errors()))
        data_version = float(user.get_info('data_version'))
        if user.version - data_version >= 0.1:
            non_fatal_errors.add('Software has been updated since your previous import. We recommend importing new transactions to enable all the features.')

        user.load_import_versions()

        data = {'info':info,'transactions':transactions_js,'custom_types':custom_types,
                'CA_long':calculator.CA_long,'CA_short':calculator.CA_short,'CA_errors':calculator.errors,'incomes':calculator.incomes,'interest':calculator.interest_payments,'expenses':calculator.business_expenses,
                'vaults':calculator.vaults_json()
                ,'loans':calculator.loans_json(),
                'tokens':calculator.tokens_json(),
                'non_fatal_errors':list(non_fatal_errors),
                # 'address_info':address_info, 'chain_list':list(display_chains.keys()),
                'latest_tokens':current_tokens,
                'fiat_info':Twelve.FIAT_SYMBOLS,
                'all_address_info':user.all_addresses,
                'chain_config':Chain.config_json(),
                'version':{'software':user.version,'data':data_version}
                }

        pb.set('Uploading to your browser', 98)
        # data = {'placeholder':'stuff'}
        user.done()
        dump = json.dumps(data)

        to_empty_in_cache = ['transactions','CA_long','CA_short','CA_errors','incomes','interest','vaults','loans','tokens']
        for entry in to_empty_in_cache:
            data[entry] = ''
    except:

        log("EXCEPTION in process", primary,active_address, traceback.format_exc())
        log_error("EXCEPTION in process", primary,active_address, request.args)
        data = {'error':'An error has occurred while processing transactions. '
                        'Please let us know on Discord if you received this message.'}
        dump = json.dumps(data)
        try:
            pb.set('Uploading to your browser', 98)
        except:
            pass
    js_file = open('data/users/' + primary + '/data_cache.json', 'w', newline='')
    js_file.write(json.dumps(data, indent=2, sort_keys=True))
    js_file.close()
    # data = json.dumps(data)
    if pb is not None:
        pb.set('Uploading to your browser', 100)
    redis.finish()
    # data.set_cookie('address', address + "|" + chain_name)
    return dump


def recreate_data_from_caches(primary):
    js_file = open('data/users/' + primary + '/data_cache.json', 'r')
    js = js_file.read()
    js_file.close()
    data = json.loads(js)
    if not 'error' in data:
        js_file = open('data/users/' + primary + '/transactions.json', 'r')
        js = js_file.read()
        js_file.close()
        data['transactions'] = json.loads(js)

        user = User(primary)

        calculator = Calculator(user, None)
        calculator.from_cache()
        data['CA_long'] = calculator.CA_long
        data['CA_short'] = calculator.CA_short
        data['CA_errors'] = calculator.errors
        data['incomes'] = calculator.incomes
        data['interest'] = calculator.interest_payments
        data['expenses'] = calculator.business_expenses
        data['vaults'] = calculator.vaults_json()
        data['loans'] = calculator.loans_json()
        data['tokens'] = calculator.tokens_json()
    data = json.dumps(data)
    return data




@app.route('/calc_tax',methods=['GET', 'POST'])
def calc_tax():
    init()
    address = normalize_address(request.args.get('address'))
    # chain_name = request.args.get('chain')
    persist(address)
    try:
        mtm = request.args.get('mtm')
        if mtm == 'false':
            mtm = False
        else:
            mtm = True


        data = request.get_json()
        # log('data',data)
        transactions_js = json.loads(data)
        # js_file = open('data/users/' + address + '/transactions.json', 'w', newline='')
        # js_file.write(json.dumps(transactions_js,indent=2, sort_keys=True))
        # js_file.close()

        # log('tran0',transactions_js[0])
        log("all transactions", transactions_js)

        user = User(address)
        # address_db = SQLite('addresses')
        # chain = Chain.from_name(chain_name, address_db, address)
        user.get_custom_rates()
        C = Coingecko.init_from_cache(user)

        calculator = Calculator(user, C, mtm=mtm)
        calculator.process_transactions(transactions_js, user)
        calculator.matchup()
        calculator.cache()



        js = {'CA_long': calculator.CA_long, 'CA_short': calculator.CA_short, 'CA_errors': calculator.errors, 'incomes': calculator.incomes, 'interest': calculator.interest_payments,
              'expenses':calculator.business_expenses,'vaults':calculator.vaults_json(),'loans':calculator.loans_json(),'tokens':calculator.tokens_json()}

        user.done()
    except:
        log("EXCEPTION in calc_tax", traceback.format_exc())
        log_error("EXCEPTION in calc_tax", address, request.args)
        js = {'error':'An error has occurred while calculating taxes'}
    data = json.dumps(js)
    return data


@app.route('/save_type',methods=['GET', 'POST'])
def save_type():
    init()
    address = normalize_address(request.args.get('address'))
    # chain_name = request.args.get('chain')
    persist(address)
    try:
        form = request.form

        name = form['tc_name']
        chain_specific = False
        if 'tc_chain' in form:
            chain_specific = True
        description = form['tc_desc']
        balanced = 0
        if 'tc_balanced' in form:
            balanced = int(form['tc_balanced'] == 'on')


        rules = []
        idx = 0
        while 'from_addr'+str(idx) in form:
            sidx = str(idx)
            rule = [form['from_addr'+sidx],form['from_addr_custom'+sidx],form['to_addr'+sidx],form['to_addr_custom'+sidx],
                    form['rule_tok'+sidx],form['rule_tok_custom'+sidx],form['rule_treatment' + sidx],form['vault_id'+sidx],form['vault_id_custom'+sidx]]
            rules.append(rule)
            idx += 1

        type_id = None
        if 'type_id' in form:
            type_id = form['type_id']

        log('create_type', address, name, chain_specific, type_id, rules)

        # T = Typing()
        user = User(address)
        user.save_custom_type(name,description, balanced, rules,id=type_id)

        custom_types = user.load_custom_types()
        user.done()
        js = {'custom_types': custom_types}
    except:
        log("EXCEPTION in save_type", traceback.format_exc())
        log_error("EXCEPTION in save_type", address, request.args, request.form)
        js = {'error':'An error has occurred while saving a type'}
    data = json.dumps(js)
    return data


@app.route('/delete_type',methods=['GET', 'POST'])
def delete_type():
    init()
    address = normalize_address(request.args.get('address'))
    # chain_name = request.args.get('chain')
    persist(address)
    try:
        form = request.form

        type_id = form['type_id']


        log('delete_type', address, type_id)

        # T = Typing()
        user = User(address)
        processed_transactions = user.unapply_custom_type(type_id)
        user.delete_custom_type(type_id)

        custom_types = user.load_custom_types()
        user.done()
        js = {'custom_types': custom_types, 'transactions': processed_transactions}
    except:
        log("EXCEPTION in delete_type", traceback.format_exc())
        log_error("EXCEPTION in delete_type", address, request.args, request.form)
        js = {'error':'An error has occurred while deleting a type'}
    data = json.dumps(js)
    return data


@app.route('/apply_type',methods=['GET', 'POST'])
def apply_type():
    init()
    address = normalize_address(request.args.get('address'))
    # chain_name = request.args.get('chain')
    persist(address)
    try:
        form = request.form

        type_id = form['type_id']
        transactions = form['transactions']

        log('apply_type', address, type_id, transactions)
        user = User(address)
        user.get_custom_rates()
        processed_transactions = user.apply_custom_type(type_id, transactions.split(","))
        user.done()
        js = {'success':1,'transactions':processed_transactions}
    except:
        log("EXCEPTION in apply_type", traceback.format_exc())
        log_error("EXCEPTION in apply_type", address, request.args, request.form)
        js = {'error':'An error has occurred while applying a type'}
    data = json.dumps(js)
    return data

@app.route('/unapply_type',methods=['GET', 'POST'])
def unapply_type():
    init()
    address = normalize_address(request.args.get('address'))
    # chain_name = request.args.get('chain')
    persist(address)
    try:
        form = request.form

        type_id = form['type_id']
        transactions = form['transactions']

        log('unapply_type', address, type_id, transactions)
        user = User(address)
        user.get_custom_rates()
        processed_transactions = user.unapply_custom_type(type_id, transactions.split(","))
        user.done()
        js = {'success':1,'transactions':processed_transactions}
    except:
        log("EXCEPTION in unapply_type", traceback.format_exc())
        log_error("EXCEPTION in unapply_type", address, request.args, request.form)
        js = {'error':'An error has occurred while unapplying a type'}
    data = json.dumps(js)
    return data


@app.route('/save_custom_val',methods=['GET', 'POST'])
def save_custom_val():
    init()
    address = normalize_address(request.args.get('address'))
    # chain_name = request.args.get('chain')
    persist(address)
    try:
        form = request.form

        transfer_id_str = form['transfer_id']
        transaction = form['transaction']
        prop = form['prop']
        val = form['val']


        log('apply_custom_val', address, transaction, transfer_id_str,prop,val,)
        user = User(address)
        user.save_custom_val(transaction, transfer_id_str, prop, val)
        user.done()
        # user.save_custom_val(chain_name,address,transaction, transfer_idx, treatment=custom_treatment, rate=custom_rate, vaultid=custom_vaultid)
        js = {'success':1}
    except:
        log("EXCEPTION in save_custom_val", traceback.format_exc())
        log_error("EXCEPTION in save_custom_val", address, request.args, request.form)
        js = {'error':'An error has occurred while saving custom value'}
    data = json.dumps(js)
    return data


@app.route('/undo_custom_changes',methods=['GET', 'POST'])
def undo_custom_changes():
    init()
    address = normalize_address(request.args.get('address'))
    # chain_name = request.args.get('chain')
    persist(address)

    try:
        form = request.form

        transaction = form['transaction']

        log('undo_custom_changes', address, transaction)
        user = User(address)
        user.get_custom_rates()
        transaction_js = user.undo_custom_changes(transaction)
        js = {'success':1,'transactions':[transaction_js]}
        user.done()
    except:
        log("EXCEPTION in undo_custom_changes", traceback.format_exc())
        log_error("EXCEPTION in undo_custom_changes", address, request.args, request.form)
        js = {'error':'An error has occurred while undoing custom changes'}
    data = json.dumps(js)
    return data

@app.route('/recolor',methods=['GET', 'POST'])
def recolor():
    t = time.time()
    init()
    address = normalize_address(request.args.get('address'))
    # chain_name = request.args.get('chain')
    persist(address)
    try:
        form = request.form

        color_id = form['color_id']
        transactions = form['transactions']

        log('recolor', address, color_id, transactions)
        log('recolor timing 1',time.time()-t)
        user = User(address)
        log('recolor timing 2', time.time() - t)
        user.recolor(color_id, transactions.split(","))
        log('recolor timing 3', time.time() - t)
        user.done()
        log('recolor timing 4', time.time() - t)
        js = {'success':1}
    except:
        log("EXCEPTION in recolor", traceback.format_exc())
        log_error("EXCEPTION in recolor", address, request.args, request.form)
        js = {'error':'An error has occurred while recoloring'}
    data = json.dumps(js)
    return data

@app.route('/save_note',methods=['GET', 'POST'])
def save_note():
    init()
    address = normalize_address(request.args.get('address'))
    persist(address)
    try:
        form = request.form

        note = form['note']
        txid = request.args.get('transaction')

        log('save_note', address, txid, note)
        user = User(address)
        user.save_note(note, txid)
        user.done()
        js = {'success':1}
    except:
        log("EXCEPTION in save_note", traceback.format_exc())
        log_error("EXCEPTION in save_note", address, request.args, request.form)
        js = {'error':'An error has occurred while saving note'}
    data = json.dumps(js)
    return data

@app.route('/save_manual_transaction',methods=['GET', 'POST'])
def save_manual_transaction():
    init()
    address = normalize_address(request.args.get('address'))
    chain_name = request.args.get('chain')
    persist(address,chain_name)

    try:

        form = request.form
        done = False
        idx = 0
        all_tx_blobs = []
        while not done:
            s_idx = str(idx)
            # try:
            #     date = form['mt'+s_idx+'_date']
            # except:
            #     break
            # time = form['mt'+s_idx+'_time']
            log('form',form)
            try:
                ts = form['mt'+s_idx+'_ts']
            except: #out of transactions
                break
            hash = form['mt'+s_idx+'_hash']
            op = form['mt'+s_idx+'_op']
            # cp = form['mt_cp']
            cp = None

            max_tr_disp_idx = int(form['mt'+s_idx+'_tr_disp_idx'])
            transfers = []
            for tr_disp_idx in range(max_tr_disp_idx):
                s_tr_idx = str(tr_disp_idx)
                if 'mt'+s_idx+'_from'+s_tr_idx in form:
                    transfers.append([form['mt'+s_idx+'_transfer_id'+s_tr_idx],form['mt'+s_idx+'_from'+s_tr_idx],form['mt'+s_idx+'_to'+s_tr_idx],form['mt'+s_idx+'_what'+s_tr_idx],form['mt'+s_idx+'_amount'+s_tr_idx],form['mt'+s_idx+'_nft_id'+s_tr_idx]])
                    if form['mt'+s_idx+'_from'+s_tr_idx] == 'my account' or form['mt'+s_idx+'_to'+s_tr_idx] == 'my account':
                        raise Exception('Using "my account" is not allowed')

            # froms = form.getlist('mt_from')
            # tos = form.getlist('mt_to')
            # whats = form.getlist('mt_what')
            # amounts = form.getlist('mt_amount')
            # nft_ids = form.getlist('mt_nft_id')
            # transfers = list(zip(froms,tos,whats,amounts,nft_ids))

            txid = None
            if 'mt'+s_idx+'_txid' in form:
                txid = form['mt'+s_idx+'_txid']
            # tx_blob = [date,time,hash,op,cp,transfers,txid]
            tx_blob = [ts, hash, op, cp, transfers, txid]
            all_tx_blobs.append(tx_blob)
            idx += 1


        user = User(address)
        C = Coingecko.init_from_cache(user)
        transactions_js = user.save_manual_transactions(chain_name,address,all_tx_blobs,C)
        user.done()
        js = {'success': 1, 'transactions':transactions_js}
    except:
        log("EXCEPTION in save_manual_transaction", traceback.format_exc())
        log_error("EXCEPTION in save_manual_transaction", address, request.args, request.form)
        js = {'error':'An error has occurred while saving manual transaction'}
    data = json.dumps(js)
    return data

@app.route('/delete_manual_transaction',methods=['GET', 'POST'])
def delete_manual_transaction():
    init()
    address = normalize_address(request.args.get('address'))
    # chain_name = request.args.get('chain')
    persist(address)
    try:
        form = request.form

        txid = form['txid']


        log('delete manual transaction', address, txid)

        user = User(address)
        user.delete_manual_transaction(txid)
        user.done()
        js = {'success': 1}
    except:
        log("EXCEPTION in delete_manual_transaction", traceback.format_exc())
        log_error("EXCEPTION in delete_manual_transaction", address, request.args, request.form)
        js = {'error':'An error has occurred while deleting a transaction'}
    data = json.dumps(js)
    return data





@app.route('/progress_bar')
def get_progress_bar():
    init()
    address = normalize_address(request.args.get('address'))
    # chain_name = request.args.get('chain_name')
    uid = request.args.get('uid')
    persist(address)
    try:

        redis = Redis(address)
        pb = ProgressBar(redis)

        phase, progress = pb.retrieve()
        if progress is None:
            progress = 0
            phase = 'Starting'
        js = {'phase': phase, 'pb': float(progress)}
    except:
        log("EXCEPTION in progress_bar", traceback.format_exc())
        log_error("EXCEPTION in progress_bar", address)
        js = {'phase': 'Progressbar error', 'pb': 100}

    return json.dumps(js)

@app.route('/update_progenitors')
def update_progenitors():
    init()
    address = request.args.get('user')
    chain_name = request.args.get('chain')
    persist(address,chain_name)
    try:

        progenitor = request.args.get('progenitor')
        counterparty = request.args.get('counterparty')
        if (len(address) == 42) or chain_name == 'Solana':
            counterparty = html.escape(counterparty[:30])
            user = User(address)
            db = user.db
            # address_db = SQLite('addresses')
            db.insert_kw("custom_names",values=[chain_name,progenitor.lower(),counterparty])
            db.commit()
            db.disconnect()
            # address_db.commit()
            # address_db.disconnect()
        js = {'success': 'true'}
    except:
        log("EXCEPTION in update_progenitors", traceback.format_exc())
        log_error("EXCEPTION in update_progenitors", address, request.args)
        js = {'error': 'An error has occurred while updating counterparty'}
    return json.dumps(js)


@app.route('/wipe',methods=['GET', 'POST'])
def wipe():
    init()
    address = normalize_address(request.args.get('address'))
    persist(address)
    try:
        user = User(address)
        user.wipe_transactions()
        user.done()
        js = {'success': 1}
    except:
        log("EXCEPTION in wipe", traceback.format_exc())
        log_error("EXCEPTION in wipe", address, request.args)
        js = {'error':'An error has occurred while wiping transactions'}
    data = json.dumps(js)
    return data

@app.route('/restore',methods=['GET', 'POST'])
def restore():
    init()
    address = normalize_address(request.args.get('address'))
    persist(address)
    try:
        user = User(address)
        user.restore_backup()
        user.done()
        js = {'success': 1}
    except:
        log("EXCEPTION in restore", traceback.format_exc())
        log_error("EXCEPTION in restore", address, request.args)
        js = {'error':'An error has occurred while restoring from backup'}
    data = json.dumps(js)
    return data

@app.route('/save_info',methods=['GET', 'POST'])
def save_info():
    init()
    address = normalize_address(request.args.get('address'))
    persist(address)
    try:
        user = User(address)
        field = request.args.get('field')
        value = request.args.get('value')
        assert field in ['tx_per_page','high_impact_amount','dc_fix_shutup','matchups_visible']
        user.set_info(field, value)
        user.done()
        js = {'success': 1}
    except:
        log("EXCEPTION in save_info", traceback.format_exc())
        log_error("EXCEPTION in save_info", address, request.args)
        js = {'error': 'An error has occurred while saving information'}
    data = json.dumps(js)
    return data

@app.route('/download')
def download():
    init()
    address = normalize_address(request.args.get('address'))
    # chain_name = request.args.get('chain')
    persist(address)
    try:

        type = request.args.get('type')



        if type == 'transactions_json':
            path = 'data/users/'+address+'/transactions.json'
            return send_file(path, as_attachment=True, cache_timeout=0)

        if type == 'transactions_csv':
            user = User(address)
            user.json_to_csv()
            user.done()
            path = 'data/users/' + address + '/transactions.csv'
            return send_file(path, as_attachment=True, cache_timeout=0)

        if type == 'tax_forms':
            year = request.args.get('year')
            user = User(address)
            C = Coingecko.init_from_cache(user)
            calculator = Calculator(user,C)
            calculator.from_cache()

            calculator.make_forms(year)
            user.done()
            path = 'data/users/'+address+'/tax_forms_'+str(year)+'.zip'
            return send_file(path, as_attachment=True, cache_timeout=0)

        if type == 'turbotax':
            year = request.args.get('year')
            user = User(address)
            C = Coingecko.init_from_cache(user)
            calculator = Calculator(user,C)
            calculator.from_cache()

            batched = calculator.make_turbotax(year)
            user.done()
            if batched:
                path = 'data/users/'+address+'/turbotax_8949_'+str(year)+'.zip'
            else:
                path = 'data/users/'+address+'/turbotax_8949_'+str(year)+'.csv'
            return send_file(path, as_attachment=True, cache_timeout=0)
    except:
        log_error("EXCEPTION in download", address, request.args)
        log("EXCEPTION in download", traceback.format_exc())
        return "EXCEPTION " + str(traceback.format_exc())


@app.route('/save_js',methods=['GET', 'POST'])
def save_js():
    init()
    address = normalize_address(request.args.get('address'))
    persist(address)
    try:
        data = request.get_json()
        transactions_js = json.loads(data)
        js_file = open('data/users/' + address + '/transactions.json', 'w', newline='')
        js_file.write(json.dumps(transactions_js,indent=2, sort_keys=True))
        js_file.close()
        js = {'success': 1}

    except:
        log("EXCEPTION in download_current", traceback.format_exc())
        log_error("EXCEPTION in download_current", address,  request.args)
        js = {'error':'An error has occurred while downloading a file'}
    data = json.dumps(js)
    return data

    # if type == 'transactions_csv':
    #     path = 'data/users/' + address + '/transactions.csv'
    # else:
    #     path = 'data/users/'+address+'/transactions.json'
    # return send_file(path, as_attachment=True, cache_timeout=0)


@app.route('/upload_csv', methods=['GET', 'POST'])
def upload_csv():
    init()
    address = normalize_address(request.args.get('address'))
    persist(address)
    redis = Redis(address)
    redis.start()
    pb = ProgressBar(redis)
    pb.set('Starting', 0)
    try:
        source = request.args.get('source')
        file = request.files['up_input']
        user = User(address)
        C = Coingecko.init_from_cache(user)
        C.make_contracts_map()
        error, transactions_js = user.upload_csv(source, file,C,pb)
        C.dump(user)
        user.done()

        if error is None:
            js = {'success': 1, 'transactions': transactions_js, 'all_address_info':user.all_addresses}
        else:
            js = {'error': error}
    except:
        log_error("EXCEPTION in upload_csv", address, request.args)
        js = {'error': 'An error has occurred while uploading a file'}
    pb.set('Uploading to your browser', 100)
    redis.finish()
    data = json.dumps(js)
    return data

@app.route('/delete_upload',methods=['GET', 'POST'])
def delete_upload():
    init()
    address = normalize_address(request.args.get('address'))
    # chain_name = request.args.get('chain')
    persist(address)
    try:
        form = request.form

        upload_source = form['chain']


        log('delete upload', address, upload_source)

        user = User(address)
        txids_to_delete = user.delete_upload(upload_source)
        user.load_addresses()
        user.load_tx_counts()
        user.done()
        js = {'success': 1, 'txids':txids_to_delete,'all_address_info':user.all_addresses}
    except:
        log("EXCEPTION in delete_upload", traceback.format_exc())
        log_error("EXCEPTION in delete_upload", address, request.args, request.form)
        js = {'error':'An error has occurred while deleting an upload'}
    data = json.dumps(js)
    return data

@app.route('/update_coingecko_id',methods=['GET', 'POST'])
def update_coingecko_id():
    init()
    address = normalize_address(request.args.get('address'))
    persist(address)
    redis = Redis(address)
    redis.start()
    pb = ProgressBar(redis)
    pb.set('Starting', 0)
    try:
        chain_name = request.args.get('chain')
        contract = request.args.get('contract')
        new_id = request.args.get('new_id')


        user = User(address)
        C = Coingecko.init_from_cache(user)
        C.make_contracts_map()
        error, transactions_js = user.update_coingecko_id(chain_name,contract,new_id, C, pb)
        user.done()
        if error is None:
            js = {'success': 1, 'transactions': transactions_js}
        else:
            js = {'error': error}
    except:
        log_error("EXCEPTION in update_coingecko_id", address, request.args)
        js = {'error': 'An error has occurred while updating coingecko ID'}
    pb.set('Uploading to your browser', 100)
    redis.finish()
    data = json.dumps(js)
    return data

@app.route('/save_options',methods=['GET', 'POST'])
def save_options():
    init()
    address = normalize_address(request.args.get('address'))
    persist(address)
    try:
        form = request.form
        recalc_needed = False
        reproc_needed = False

        fiat = form['opt_fiat']
        adjust_custom = False
        if 'opt_fiat_update_custom' in form:
            adjust_custom = form['opt_fiat_update_custom'] in ['on','checked']
        log('adjust_custom',adjust_custom)

        js = {}
        user = User(address)
        if fiat != user.fiat:
            reproc_needed = True
            user.load_fiat()
            user.set_info('fiat',fiat)
            js['fiat'] = fiat
            if adjust_custom:
                user.adjust_custom_rates(fiat)

        radio_options = ['tx_costs','vault_gain','vault_loss']
        for opt in radio_options:
            opt_code = 'opt_'+opt
            if opt_code in form:
                current_val = user.get_info(opt_code)
                new_val = form[opt_code]
                log('opt',opt_code,current_val,new_val)
                if current_val != new_val:
                    js[opt_code] = new_val
                    user.set_info(opt_code,new_val)
                    recalc_needed = True


        user.done()
        js.update({'success': 1, 'reproc_needed':reproc_needed, 'recalc_needed':recalc_needed})
    except:
        log("EXCEPTION in save_options", traceback.format_exc())
        log_error("EXCEPTION in save_options", address, request.args)
        js = {'error': 'An error has occurred while saving options'}
    data = json.dumps(js)
    return data

@app.route('/minmax_transactions',methods=['GET','POST'])
def minmax_transactions():
    init()
    address = normalize_address(request.args.get('address'))
    persist(address)
    try:
        form = request.form

        minimized = form['minimized']
        transactions = form['transactions']

        log('minmax_transaction', address, minimized, transactions)
        user = User(address)
        user.db.do_logging=True
        user.db.update_kw('transactions',"id IN "+sql_in(transactions),minimized=minimized)
        user.db.commit()
        user.done()
        js = {'success': 1}
    except:
        log("EXCEPTION in minmax_transaction", traceback.format_exc())
        log_error("EXCEPTION in minmax_transaction", address, request.args)
        js = {'error': 'An error has occurred while saving information'}
    data = json.dumps(js)
    return data

@app.route('/delete_address',methods=['GET', 'POST'])
def delete_address():
    init()
    address = normalize_address(request.args.get('address'))
    # chain_name = request.args.get('chain')
    persist(address)
    try:
        form = request.form

        address_to_delete = form['address_to_delete']


        log('delete address', address, address_to_delete)

        user = User(address)
        need_reproc = user.delete_address(address_to_delete)
        if need_reproc:
            user.set_info('force_forget_derived',1)
        user.load_addresses()
        user.load_tx_counts()
        user.done()
        js = {'success': 1, 'all_address_info':user.all_addresses, 'reproc_needed':need_reproc}
    except:
        log("EXCEPTION in delete_address", traceback.format_exc())
        log_error("EXCEPTION in delete_address", address, request.args, request.form)
        js = {'error':'An error has occurred while deleting an address'}
    data = json.dumps(js)
    return data



@app.route('/cross_user')
def cross_user():
    init()
    dirs = os.listdir('data/users/')

    query = "SELECT count(id) FROM custom_types_rules WHERE token=='base'"
    count = 0

    non_fail_count = 0
    dump = ""
    for address in dirs:
        if len(address) == 42:
            path = 'users/'+address+'/db'
            try:
                user_db = SQLite(path,read_only=True)
                res = user_db.select(query)
                stat = res[0][0]
                user_db.disconnect()
                if stat > 0:
                    count += 1
                non_fail_count += 1
            except:
                dump+=" "+traceback.format_exc()
    return str(count)+" "+str(non_fail_count)+" "+dump



if __name__ == "__main__":
    app.run()