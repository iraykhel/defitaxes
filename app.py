# from numpy import hstack
# import sys
# sys.path.append('/home/ubuntu/.local/lib/python3.6/site-packages')
# import numpy

from flask import Flask, render_template, request, send_file
import os
import traceback
import time
import json
from code.coingecko import Coingecko
from code.signatures import Signatures
from code.chain import Chain
from code.util import log, progress_bar_update
from code.main import process_web_json
from code.sqlite import SQLite
# from code.category import Typing
from code.user import User
from code.tax_calc import Calculator
import pickle
import html

# TODO: Fix the below error, and change this to use python-dotenv and .env file
#   File "app.py", line 457, in <module>
#     app.run()
#   File "/Users/stevenli/Documents/github/taxes/venv/lib/python3.8/site-packages/flask/app.py", line 920, in run
#     run_simple(t.cast(str, host), port, self, **options)
#   File "/Users/stevenli/Documents/github/taxes/venv/lib/python3.8/site-packages/werkzeug/serving.py", line 907, in run_simple
#     from .debug import DebuggedApplication
#   File "/Users/stevenli/Documents/github/taxes/venv/lib/python3.8/site-packages/werkzeug/debug/__init__.py", line 21, in <module>
#     from .console import Console
#   File "/Users/stevenli/Documents/github/taxes/venv/lib/python3.8/site-packages/werkzeug/debug/console.py", line 129, in <module>
#     class _InteractiveConsole(code.InteractiveInterpreter):
# AttributeError: module 'code' has no attribute 'InteractiveInterpreter'
FLASK_ENV = 'production'

# from code import symbols
app = Flask(__name__)

@app.route('/')
@app.route('/main')
def main():
    os.chdir('/home/ubuntu/hyperboloid') if FLASK_ENV == "production" else False  
    address_cookie = request.cookies.get('address')
    address = ""
    chain_name = ""
    last = 0
    if address_cookie is not None:
        address,chain_name = request.cookies.get('address').split("|")
        last = last_update_inner(address,chain_name)

    log('cookie',address,chain_name)
    return render_template('main.html', title='Blockchain transactions to US tax form', address=address, chain=chain_name, last=last)


@app.route('/last_update')
def last_update():
    os.chdir('/home/ubuntu/hyperboloid') if FLASK_ENV == "production" else False
    address = request.args.get('address').lower()
    chain_name = request.args.get('chain')
    last = last_update_inner(address, chain_name)
    data = {'last_transaction_timestamp':last}
    data = json.dumps(data)
    return data

def last_update_inner(address,chain_name):
    last = 0
    user = User(address)
    query = "select max(timestamp) from transactions where chain='" + chain_name + "'"
    row = user.db.select(query)
    log('last', query, row)
    ts = row[0][0]
    if ts is not None:
        last = ts
    return last

@app.route('/process')
def process():
    os.chdir('/home/ubuntu/hyperboloid') if FLASK_ENV == "production" else False
    log('xi1')
    address = request.args.get('address').lower()
    chain_name = request.args.get('chain')
    import_new = int(request.args.get('import_new'))

    log('xi2')
    progress_bar_update(address, 'Starting', 0)
    log('xi3')

    try:

        S = Signatures()
        log('xi4')

        address_db = SQLite('addresses')
        chain = Chain.from_name(chain_name,address_db,address)


        user = User(address)

        if import_new:
            transactions = chain.get_transactions()
            # user.wipe_rates()
            user.store_transactions(chain, transactions)

        transactions = user.load_transactions(chain)

        progress_bar_update(address, 'Looking up counterparties', 20)
        contract_list, counterparty_list, input_list = chain.get_contracts(transactions)
        # log("counterparty_list",counterparty_list)
        chain.update_progenitors(user, counterparty_list)
        log("Ancestor lookups", chain.scrapes)
        address_db.disconnect()


        progress_bar_update(address, 'Loading coingecko rates', 35)
        t = time.time()
        # if import_new:

        S.init_from_db(input_list)
        if import_new:
            C = Coingecko()
            C.init_from_db(chain.main_asset, contract_list, address)
        else:
            C = Coingecko.init_from_cache(chain)
        log('timing:coingecko init_from_db',time.time()-t)

        tl = time.time()
        # C.dump(address)
        # log('timing:coingecko save', time.time() - tl)

        # rates_dump_file = open('data/'+address+"_rates","wb")
        # pickle.dump(C, rates_dump_file)
        # rates_dump_file.close()

        log("coingecko initialized",C.initialized)
        transactions_js = chain.transactions_to_log(user, C,S, transactions,mode='js')
        log("all transactions", transactions_js)


        # T = Typing()
        # builtin_types = T.load_builtin_types()
        progress_bar_update(address, 'Loading custom types', 85)
        custom_types = user.load_custom_types(chain_name)

        progress_bar_update(address, 'Calculating taxes', 87)
        calculator = Calculator(user, chain, C)
        calculator.process_transactions(transactions_js)

        #process_transactions affects coingecko rates! Need to cache it after, not before.
        C.dump(chain)

        progress_bar_update(address, 'Calculating taxes', 90)
        calculator.matchup()
        calculator.cache()

        js_file = open('data/users/' + address + '/transactions.json', 'w', newline='')
        js_file.write(json.dumps(transactions_js))
        js_file.close()


        data = {'transactions':transactions_js,'custom_types':custom_types,
                'CA_long':calculator.CA_long,'CA_short':calculator.CA_short,'CA_errors':calculator.errors,'incomes':calculator.incomes,'interest':calculator.interest_payments,
                'vaults':calculator.vaults_json(),'loans':calculator.loans_json()}
        progress_bar_update(address, 'Uploading to your browser', 95)
        # data = {'placeholder':'stuff'}
        log('timing:coingecko lookups 1', C.time_spent_looking_up, C.shortcut_hits)
    except:
        log("EXCEPTION in process", address, chain_name, traceback.format_exc())
        data = {'error':'An error has occurred while processing transactions'}
    data = json.dumps(data)
    progress_bar_update(address, 'Finished', 100)
    # data.set_cookie('address', address + "|" + chain_name)
    return data


@app.route('/calc_tax',methods=['GET', 'POST'])
def calc_tax():
    os.chdir('/home/ubuntu/hyperboloid') if FLASK_ENV == "production" else False
    try:
        mtm = request.args.get('mtm')
        if mtm == 'false':
            mtm = False
        else:
            mtm = True
        address = request.args.get('address').lower()
        chain_name = request.args.get('chain')

        data = request.get_json()
        # log('data',data)
        transactions_js = json.loads(data)
        js_file = open('data/users/' + address + '/transactions.json', 'w', newline='')
        js_file.write(json.dumps(transactions_js))
        js_file.close()

        log('tran0',transactions_js[0])
        log("all transactions", transactions_js)

        user = User(address)
        address_db = SQLite('addresses')
        chain = Chain.from_name(chain_name, address_db, address)
        C = Coingecko.init_from_cache(chain)

        calculator = Calculator(user, chain, C, mtm=mtm)
        calculator.process_transactions(transactions_js)
        calculator.matchup()
        calculator.cache()

        js = {'CA_long': calculator.CA_long, 'CA_short': calculator.CA_short, 'CA_errors': calculator.errors, 'incomes': calculator.incomes, 'interest': calculator.interest_payments,
              'vaults':calculator.vaults_json(),'loans':calculator.loans_json()}
    except:
        log("EXCEPTION in calc_tax", traceback.format_exc())
        js = {'error':'An error has occurred while calculating taxes'}
    data = json.dumps(js)
    return data


@app.route('/save_type',methods=['GET', 'POST'])
def save_type():
    os.chdir('/home/ubuntu/hyperboloid') if FLASK_ENV == "production" else False
    try:
        form = request.form
        address = request.args.get('address').lower()
        chain_name = request.args.get('chain')
        name = form['tc_name']
        description = form['tc_desc']
        balanced = 0
        if 'tc_balanced' in form:
            balanced = int(form['tc_balanced'] == 'on')

        froms = form.getlist('from_addr')
        from_custom_addrs = form.getlist('from_addr_custom')

        tos = form.getlist('to_addr')
        tos_custom_addrs = form.getlist('to_addr_custom')

        toks = form.getlist('rule_tok')
        custom_toks = form.getlist('rule_tok_custom')
        treatments = form.getlist('rule_treatment')
        vault_ids = form.getlist('vault_id')
        vault_ids_custom = form.getlist('vault_id_custom')
        rules = list(zip(froms,from_custom_addrs,tos,tos_custom_addrs,toks,custom_toks,treatments,vault_ids,vault_ids_custom))
        type_id = None
        if 'type_id' in form:
            type_id = form['type_id']

        log('create_type', address, name, type_id, rules)

        # T = Typing()
        user = User(address)
        user.save_custom_type(chain_name,address,name,description, balanced, rules,id=type_id)

        custom_types = user.load_custom_types(chain_name)
        js = {'custom_types': custom_types}
    except:
        log("EXCEPTION in save_type", traceback.format_exc())
        js = {'error':'An error has occurred while saving a type'}
    data = json.dumps(js)
    return data


@app.route('/delete_type',methods=['GET', 'POST'])
def delete_type():
    os.chdir('/home/ubuntu/hyperboloid') if FLASK_ENV == "production" else False
    try:
        form = request.form
        address = request.args.get('address').lower()
        chain_name = request.args.get('chain')
        type_id = form['type_id']


        log('delete_type', address, type_id)

        # T = Typing()
        user = User(address)
        processed_transactions = user.unapply_custom_type(chain_name, address, type_id)
        user.delete_custom_type(type_id)

        custom_types = user.load_custom_types(chain_name)
        js = {'custom_types': custom_types, 'transactions': processed_transactions}
    except:
        log("EXCEPTION in delete_type", traceback.format_exc())
        js = {'error':'An error has occurred while deleting a type'}
    data = json.dumps(js)
    return data


@app.route('/apply_type',methods=['GET', 'POST'])
def apply_type():
    os.chdir('/home/ubuntu/hyperboloid') if FLASK_ENV == "production" else False
    try:
        form = request.form
        address = request.args.get('address').lower()
        chain_name = request.args.get('chain')
        type_id = form['type_id']
        transactions = form['transactions']

        log('apply_type', address, type_id, transactions)
        user = User(address)
        processed_transactions = user.apply_custom_type(chain_name,address,type_id, transactions.split(","))
        js = {'success':1,'transactions':processed_transactions}
    except:
        log("EXCEPTION in apply_type", traceback.format_exc())
        js = {'error':'An error has occurred while applying a type'}
    data = json.dumps(js)
    return data

@app.route('/unapply_type',methods=['GET', 'POST'])
def unapply_type():
    os.chdir('/home/ubuntu/hyperboloid') if FLASK_ENV == "production" else False
    try:
        form = request.form
        address = request.args.get('address').lower()
        chain_name = request.args.get('chain')
        type_id = form['type_id']
        transactions = form['transactions']

        log('unapply_type', address, type_id, transactions)
        user = User(address)
        processed_transactions = user.unapply_custom_type(chain_name,address,type_id, transactions.split(","))
        js = {'success':1,'transactions':processed_transactions}
    except:
        log("EXCEPTION in unapply_type", traceback.format_exc())
        js = {'error':'An error has occurred while unapplying a type'}
    data = json.dumps(js)
    return data


@app.route('/save_custom_val',methods=['GET', 'POST'])
def save_custom_val():
    os.chdir('/home/ubuntu/hyperboloid') if FLASK_ENV == "production" else False
    try:
        form = request.form
        address = request.args.get('address').lower()
        chain_name = request.args.get('chain')
        transfer_idx = form['transfer_idx']
        transaction = form['transaction']

        custom_treatment = custom_rate = custom_vaultid = None
        if 'custom_treatment' in form:
            custom_treatment = form['custom_treatment']
        if 'custom_rate' in form:
            custom_rate = form['custom_rate']
        if 'custom_vaultid' in form:
            custom_vaultid = form['custom_vaultid']

        log('apply_custom_val', address, transaction, transfer_idx,custom_treatment,custom_rate,custom_vaultid)
        user = User(address)
        user.save_custom_val(chain_name,address,transaction, transfer_idx, treatment=custom_treatment, rate=custom_rate, vaultid=custom_vaultid)
        js = {'success':1}
    except:
        log("EXCEPTION in save_custom_val", traceback.format_exc())
        js = {'error':'An error has occurred while saving custom value'}
    data = json.dumps(js)
    return data


@app.route('/undo_custom_changes',methods=['GET', 'POST'])
def undo_custom_changes():
    os.chdir('/home/ubuntu/hyperboloid') if FLASK_ENV == "production" else False
    try:
        form = request.form
        address = request.args.get('address').lower()
        chain_name = request.args.get('chain')
        transaction = form['transaction']

        log('undo_custom_changes', address, transaction)
        user = User(address)
        transaction_js = user.undo_custom_changes(chain_name,address,transaction)
        js = {'success':1,'transactions':[transaction_js]}
    except:
        log("EXCEPTION in undo_custom_changes", traceback.format_exc())
        js = {'error':'An error has occurred while undoing custom changes'}
    data = json.dumps(js)
    return data

@app.route('/recolor',methods=['GET', 'POST'])
def recolor():
    os.chdir('/home/ubuntu/hyperboloid') if FLASK_ENV == "production" else False
    try:
        form = request.form
        address = request.args.get('address').lower()
        chain_name = request.args.get('chain')
        color_id = form['color_id']
        transactions = form['transactions']

        log('recolor', address, color_id, transactions)
        user = User(address)
        user.recolor(chain_name,address,color_id, transactions.split(","))
        js = {'success':1}
    except:
        log("EXCEPTION in recolor", traceback.format_exc())
        js = {'error':'An error has occurred while recoloring'}
    data = json.dumps(js)
    return data


@app.route('/progress_bar')
def progress_bar():
    os.chdir('/home/ubuntu/hyperboloid') if FLASK_ENV == "production" else False
    good = 0
    for i in range(5):
        try:
            address = request.args.get('address').lower()
            js = pickle.load(open('data/' + address + "_pb", "rb"))
            good = 1
            break
        except:
            exc = traceback.format_exc()
            time.sleep(0.03)
            # log("EXCEPTION in progress_bar", traceback.format_exc())
            # js = {'phase':'Progressbar error','pb':100}
    if not good:
        log("EXCEPTION in progress_bar", exc)
        js = {'phase': 'Progressbar error', 'pb': 100}
    return json.dumps(js)

@app.route('/update_progenitors')
def update_progenitors():
    os.chdir('/home/ubuntu/hyperboloid') if FLASK_ENV == "production" else False
    try:
        address = request.args.get('user')
        chain = request.args.get('chain')
        progenitor = request.args.get('progenitor')
        counterparty = request.args.get('counterparty')
        if len(address) == 42 and len(progenitor) == 42 and chain in ['ETH','Polygon','BSC','HECO']:
            counterparty = html.escape(counterparty[:30])
            user = User(address)
            db = user.db
            # address_db = SQLite('addresses')
            db.insert_kw("custom_names",values=[chain,progenitor.lower(),counterparty])
            db.commit()
            db.disconnect()
            # address_db.commit()
            # address_db.disconnect()
        js = {'success': 'true'}
    except:
        log("EXCEPTION in update_progenitors", traceback.format_exc())
        js = {'error': 'An error has occurred while updating counterparty'}
    return json.dumps(js)

@app.route('/download')
def download():
    os.chdir('/home/ubuntu/hyperboloid') if FLASK_ENV == "production" else False
    try:
        address = request.args.get('address').lower()
        type = request.args.get('type')
        # if type == 'transactions_csv':
        #     path = 'data/'+address+'_transactions.csv'
        #     return send_file(path, as_attachment=True, cache_timeout=0)

        if type == 'transactions_json':
            path = 'data/users/'+address+'/transactions.json'
            return send_file(path, as_attachment=True, cache_timeout=0)

        if type == 'tax_forms':
            address = request.args.get('address').lower()
            chain_name = request.args.get('chain')
            year = request.args.get('year')
            address_db = SQLite('addresses')
            chain = Chain.from_name(chain_name, address_db, address)
            user = User(address)
            C = Coingecko.init_from_cache(chain)
            calculator = Calculator(user,chain,C)
            calculator.from_cache()

            calculator.make_forms(year)

            path = 'data/users/'+address+'/'+chain_name+'_'+year+'_tax_forms.zip'
            return send_file(path, as_attachment=True, cache_timeout=0)
    except:
        log("EXCEPTION in download", traceback.format_exc())
        return "EXCEPTION " + str(traceback.format_exc())

def wrapper(func,*args):
    try:
        return func(*args)
    except:
        return "EXCEPTION "+ str(traceback.format_exc())

if __name__ == "__main__":
    app.run()