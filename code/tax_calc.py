from .coingecko import *
from .chain import *
import datetime
from decimal import Decimal
import decimal
import copy
import calendar
import traceback
import json
from .util import *
import zipfile
import zlib


def timestamp_to_year(ts):
    return datetime.datetime.fromtimestamp(ts).year


def rate_pick(what, timestamp, running_rates, coingecko_rates):
    running_rate, running_ts = running_rates[what]
    if running_rate != 0 and timestamp - running_ts < 3600:
        return running_rate
    good, coingecko_rate, source = coingecko_rates.lookup_rate(what,timestamp)
    if good >= 1:
        return coingecko_rate
    else:
        return running_rate

def decustom(val):
    custom = False
    if val is not None and val[:7] == 'custom:':
        val = val[7:]
        custom = True
    return val, custom


class Vault:
    def __init__(self,id):
        self.id = id
        self.holdings = {}
        self.symbols = {}
        self.usd_total = 0
        self.usd_max = 0
        self.history = []
        self.warnings = []

    def __str__(self):
        return "VAULT "+str(self.id)+" holdings "+str(self.holdings)

    def __repr__(self):
        return self.__str__()

    def total_usd(self, timestamp, running_rates, coingecko_rates):
        total = 0
        empty = True
        bad = False
        for what, amt in self.holdings.items():
            rate = rate_pick(what, timestamp, running_rates, coingecko_rates)
            if amt > 0:
                empty = False
            if rate == 0 or rate is None:
                bad = True
            total += amt * rate
        return total, empty, bad

    def deposit(self,transaction,tridx,what, symbol,amount):
        if what not in self.holdings:
            self.holdings[what] = 0
            self.symbols[what] = symbol
        self.holdings[what] += amount
        self.history.append({'txid':transaction['txid'],'tridx':tridx,'action':'deposit','what':what,'amount':amount})
        print("DEPOSIT",self.id,symbol,amount)

    def withdraw(self,transaction,tridx, what,symbol,amount, running_rates, coingecko_rates, usd_fee, exit=False):
        orig_amount = amount
        orig_what = what
        timestamp = transaction['ts']
        txid = transaction['txid']
        print("WITHDRAW", self.id, symbol, amount)
        usd_total, empty, bad = self.total_usd(timestamp, running_rates, coingecko_rates)


        if usd_total > self.usd_max:
            self.usd_max = usd_total

        warning_issued = False
        if what not in self.holdings:
            warning_issued = True
            self.warnings.append({'txid': transaction['txid'], 'tridx': tridx, 'text': 'Trying to withdraw ' + symbol+' which was not previously deposited into the vault', 'level': 5})
            self.symbols[what] = symbol
            self.holdings[what] = 0


        trades = []
        incomes = []

        #first, withdraw from matching investment. If there's enough, we're done
        if self.holdings[what] >= amount*0.9999:
            self.holdings[what] -= amount
            if self.holdings[what] < 0:
                self.holdings[what] = 0
            amount = 0
        else:
            amount_requested = amount
            amount_available = self.holdings[what]
            performed_conversion = False

            amount -= self.holdings[what]
            self.holdings[what] = 0

            #next, withdraw from other tokens and record swapping transactions
            holding_keys = list(self.holdings.keys())
            # key_idx = 0
            # rate = rates[what]
            rate = rate_pick(what,timestamp,running_rates,coingecko_rates)

            holding_keys_reordered = []
            for key_idx in range(len(holding_keys)): #convert similar named tokens first
                other_tok = holding_keys[key_idx]
                other_symbol = self.symbols[other_tok]
                log("comp",other_symbol,symbol)
                if symbol in other_symbol or other_symbol in symbol:
                    holding_keys_reordered.insert(0,other_tok)
                    log("do reorder ",other_tok,"in front")
                else:
                    holding_keys_reordered.append(other_tok)

            # holding_keys_reordered = holding_keys

            log("original",holding_keys,"reordered",holding_keys_reordered)


            key_idx = 0
            while key_idx < len(holding_keys_reordered):
                usd_amt = amount*rate
                other_tok = holding_keys_reordered[key_idx]
                if other_tok == what:
                    key_idx += 1
                    continue
                other_symbol = self.symbols[other_tok]
                # other_rate = rates[other_tok]
                other_rate = rate_pick(other_tok, timestamp, running_rates, coingecko_rates)
                other_available = self.holdings[other_tok]
                other_usd_amt = other_available * other_rate
                if other_usd_amt > 0.01:
                    if other_usd_amt > usd_amt:
                        other_sold = usd_amt / other_rate
                        if amount * rate > 0.01:
                            print("WITHDRAW:CONVERSION: bought", amount,'of',symbol,', sold',other_sold,'of',other_symbol)
                            self.history.append({'txid': transaction['txid'], 'tridx': tridx, 'action': 'conversion',
                                                 'from': {'what':other_tok, 'amount': other_sold}, 'to':{'what':what, 'amount': amount}})
                            trades.append(CA_transaction(timestamp, what, symbol, amount, rate, txid, tridx))
                            trades.append(CA_transaction(timestamp, other_tok, other_symbol, -other_sold, other_rate, txid, tridx))
                            self.holdings[other_tok] -= other_sold
                            performed_conversion = True
                        amount = 0
                        break
                        # return trades, [], 0
                    else:
                        try:
                            amount_bought = other_usd_amt / rate
                        except:
                            log("EXCEPTION",traceback.format_exc(),what,symbol,running_rates[what])
                            log(transaction)
                            amount_bought = other_usd_amt
                            # exit(1)
                        print("WITHDRAW:CONVERSION: bought", amount_bought, 'of', symbol, ', sold', other_available, 'of', other_symbol)
                        self.history.append({'txid': transaction['txid'], 'tridx': tridx, 'action': 'conversion',
                                             'from': {'what': other_tok, 'amount': other_available}, 'to': {'what': what, 'amount': amount_bought}})
                        trades.append(CA_transaction(timestamp, what, symbol, amount_bought, rate, txid, tridx))
                        trades.append(CA_transaction(timestamp, other_tok, other_symbol, -other_available, other_rate, txid, tridx))
                        performed_conversion = True
                        # trades.append([amount_bought, rate, other_available, other_rate])
                        self.holdings[other_tok] = 0
                        amount -= amount_bought

                key_idx += 1

            if performed_conversion and not warning_issued:
                self.warnings.append({'txid': transaction['txid'], 'tridx': tridx,
                                  'text': 'Withdrawing ' + str(amount_requested) + ' of ' + self.symbols[what] + ', but only ' + str(amount_available) + ' was deposited. Converting from other deposits.',
                                  'level': 5})



        #if we're out of money, the rest is profit
        self.history.append({'txid': transaction['txid'], 'tridx': tridx, 'action': 'withdraw', 'what': orig_what, 'amount': orig_amount})
        if amount > 0:
            print("WITHDRAW:NOT ENOUGH HOLDINGS")
            incomes.append({'timestamp':timestamp,'text':'Income upon closing a vault', 'amount':amount*rate, 'txid':txid,'tridx':tridx})
            print("WITHDRAW:profit ", amount, 'of', symbol)
            self.history.append({'txid': transaction['txid'], 'tridx': tridx, 'action': 'income on exit', 'what': what, 'amount': amount})
            trades.append(CA_transaction(timestamp, what, symbol, amount, rate, txid, tridx, usd_fee))
            close = 1
            # if self.usd_max == 0:
            #     self.warnings.append({'txid': transaction['txid'], 'tridx': tridx, 'text': 'Withdrawing from an empty vault', 'level':0})

            if amount * rate > self.usd_max * 0.3:
                self.warnings.append({'txid': transaction['txid'], 'tridx': tridx, 'text':'Vault income on exit is over 30% of maximum investment', 'level':3})
        else:
            close = 0
            # remaining_usd = self.total_usd(rates)

            remaining_usd, vault_empty, vault_bad_rate = self.total_usd(timestamp, running_rates, coingecko_rates)
            log('withdrawal', self.id,remaining_usd,vault_empty,vault_bad_rate,exit)
            if (remaining_usd < 0.001 * self.usd_max and not vault_bad_rate) or vault_empty or exit:
                close = 1
                for transfer in transaction['rows']: #make sure it's the last transfer in transaction mentionining this vault
                    other_vault_id,_ = decustom(transfer['vault_id'])
                    other_treatment,_ = decustom(transfer['treatment'])

                    if transfer['index'] > tridx and other_vault_id == self.id and other_treatment in ['withdraw','deposit','exit']:
                        close = 0
                        break

                if close:
                    for loss_tok,loss_amt in self.holdings.items():
                        if loss_amt > 0:
                            print("WITHDRAW:fee loss on exit ", loss_amt, 'of', self.symbols[loss_tok])
                            self.history.append({'txid': transaction['txid'], 'tridx': tridx, 'action': 'loss on exit', 'what': loss_tok, 'amount': loss_amt})
                            trades.append(CA_transaction(timestamp, loss_tok, self.symbols[loss_tok], -loss_amt, 0, txid, tridx))

        if close:
            self.history.append({'txid': transaction['txid'], 'tridx': tridx, 'action': 'vault closed'})
            self.holdings = {}
            self.usd_total = 0
            self.usd_max = 0

        return trades, incomes, close



    def to_json(self):
        js = {
            'history':self.history,
            'warnings':self.warnings,
            'symbols':self.symbols,
            'holdings':self.holdings
        }
        return js

class Loan:
    def __init__(self, id):
        self.id = id
        self.loaned = {}
        self.symbols = {}
        self.usd_total = 0
        self.usd_max = 0
        self.history = []
        self.warnings = []

    def __str__(self):
        return "LOAN " + self.id + " loaned " + str(self.loaned)

    def __repr__(self):
        return self.__str__()

    # def total_usd(self, rates):
    #     total = 0
    #     for what, amt in self.loaned.items():
    #         total += amt * rates[what]
    #     return total

    def total_usd(self, timestamp, running_rates, coingecko_rates):
        total = 0
        empty = True
        for what, amt in self.loaned.items():
            rate = rate_pick(what, timestamp, running_rates, coingecko_rates)
            if amt > 0:
                empty = False
            if rate == 0 or rate is None:
                total = None
            if total is not None:
                total += amt * rate
        return total, empty

    def borrow(self, transaction,tridx, what, symbol, amount):
        txid = transaction['txid']
        if what not in self.loaned:
            self.loaned[what] = 0
            self.symbols[what] = symbol
        self.loaned[what] += amount
        self.history.append({'txid': txid, 'tridx': tridx, 'action': 'borrow', 'what': what, 'amount': amount})
        print("LOAN", self.id, symbol, amount)

    def repay(self, transaction,tridx, what,symbol,amount, running_rates, coingecko_rates, usd_fee, exit=False):
        txid = transaction['txid']

        if what not in self.loaned:
            self.symbols[what] = symbol
            self.loaned[what] = 0
            self.warnings.append({'txid': txid, 'tridx': tridx, 'text':'Trying to repay ' + symbol+', which was not previously loaned', 'level': 3})

        interest_payments = []
        self.history.append({'txid': txid, 'tridx': tridx, 'action': 'repay', 'what': what, 'amount': amount})

        # first, repay with capital. If there's enough, we're done
        if self.loaned[what] >= amount:
            self.loaned[what] -= amount
            amount = 0
        else:
            amount -= self.loaned[what]
            self.loaned[what] = 0

        # if we're out of money, the rest is profit
        timestamp = transaction['ts']
        # rate = rates[what]
        rate = rate_pick(what, timestamp, running_rates, coingecko_rates)

        if amount > 0:
            print("REPAY LOAN:REPAYING MORE THAN LOANED")
            interest_payments.append({'timestamp': timestamp, 'text': 'Interest on a loan', 'amount': amount * rate, 'txid': transaction['txid'],'tridx':tridx})
            self.history.append({'txid': txid, 'tridx': tridx, 'action': 'pay interest', 'what': what, 'amount': amount})

            print("REPAY:interest ", amount, 'of', symbol)


        trades = []
        if exit:  #assuming we were liquidated, acquire the rest of the loan at market prices
            for what, amt in self.loaned.items():
                if amt != 0:
                    rate = rate_pick(what, timestamp, running_rates, coingecko_rates)
                    self.history.append({'txid': txid, 'tridx': tridx, 'action': 'buy loaned', 'what': what, 'amount': amt, 'rate': rate})
                    trades.append(CA_transaction(timestamp, what, self.symbols[what], amt, rate, txid, tridx))
            self.loaned = {}

        for what, amt in self.loaned.items():
            if amt != 0:
                break
        else:
            self.history.append({'txid': txid, 'tridx': tridx, 'action': 'loan repaid'})

        return interest_payments, trades

    def liquidate(self,transaction, tridx, what,symbol,amount, running_rates, coingecko_rates):
        txid = transaction['txid']
        self.symbols[what] = symbol
        self.loaned = {}
        self.usd_total = 0
        self.history.append({'txid': txid, 'tridx': tridx, 'action': 'liquidation', 'what': what, 'amount': amount})
        self.history.append({'txid': txid, 'tridx': tridx, 'action': 'loan repaid'})
        self.warnings.append({'txid': txid, 'tridx': tridx, 'text': 'Liquidated ' + str(amount)+' of '+ symbol + ' collateral, loan considered repaid ', 'level': 5})

    def to_json(self):
        js = {
            'history':self.history,
            'warnings':self.warnings,
            'symbols':self.symbols,
            'loaned':self.loaned
        }
        return js






class CA_transaction:
    def __init__(self,timestamp,what,symbol,amount,rate, txid, tridx, usd_fee=0):
        if rate is None:
            rate = 0
        self.timestamp = timestamp
        self.what = what
        self.symbol = symbol
        self.amount = amount
        self.rate = rate
        # if fee_amount is None:
        #     fee_amount = 0
        # if fee_rate is None:
        #     fee_rate = 0
        # self.fee_amount = fee_amount
        # self.fee_rate = fee_rate
        if usd_fee is None:
            usd_fee = 0
        self.usd_fee = usd_fee
        if amount > 0:
            self.basis = amount*rate
        else:
            self.sale = -amount*rate
        self.txid = txid
        self.tridx = tridx

    def __str__(self):
        s = str(self.timestamp)+" "
        if self.amount > 0:
            s += "acquire "+str(self.amount)
        else:
            s += "dispose " + str(-self.amount)
        s += " of "+self.symbol + " for "+str(self.rate)+" each"
        return s

    def __repr__(self):
        return self.__str__()

class Calculator:
    def __init__(self, user, chain, coingecko_rates, mtm=False):
        self.user = user
        self.mtm = mtm
        self.chain = chain
        self.coingecko_rates=coingecko_rates

        self.hash=None#'0x13c3c4e3177c116cc7a11af87b9ca1e6e3fa0e7a48b2b298cd739533562ae46e'

        self.ca_transactions = []
        self.incomes = []
        self.interest_payments = []
        self.CA_long = []
        self.CA_short = []
        self.vaults = {}
        self.loans = {}
        self.errors = {}
        self.eoy_mtm = None

    def buysell_everything(self,timestamp,totals, running_rates,sell=True,eoy=True):
        transactions = []
        if sell and eoy:
            self.eoy_mtm = copy.deepcopy(totals)
        for what,entry in totals.items():
            rate = rate_pick(what,timestamp,running_rates,self.coingecko_rates)
            amount = entry['amount']
            if sell:
                amount = -amount
            sell_transaction = CA_transaction(timestamp, what, entry['symbol'], amount, rate, -10, -1)
            transactions.append(sell_transaction)
        return transactions


    def process_transactions(self,transactions_js):
        # print(transactions_js)
        print("PROCESS TRANSACTIONS")

        running_rates = {}

        totals = {}
        vaults = self.vaults
        loans = self.loans
        # print('all transactions',transactions_js)

        prev_timestamp = transactions_js[0]['ts']
        for tidx, transaction in enumerate(transactions_js):
            hash = transaction['hash']
            txid = transaction['txid']
            timestamp = transaction['ts']

            current_year = timestamp_to_year(timestamp)
            if self.mtm:

                if current_year != timestamp_to_year(prev_timestamp):
                    dt = datetime.date(current_year, 1, 1)
                    new_year_ts = calendar.timegm(dt.timetuple())
                    mtm_dispose_all = self.buysell_everything(new_year_ts-1,totals, running_rates, sell=True)
                    print("MTM DISP")
                    pprint.pprint(mtm_dispose_all)
                    self.ca_transactions.extend(mtm_dispose_all)
                    mtm_rebuy_all = self.buysell_everything(new_year_ts, totals, running_rates, sell=False)
                    self.ca_transactions.extend(mtm_rebuy_all)

            if hash == self.hash:
                pprint.pprint(transaction)
            transfers = transaction['rows']


            # vaults_to_inspect = set()
            fee_amount_per_transaction = fee_rate = fee_amount = fee_transfer = None
            if len(transfers) > 1:
                cnt = 0
                usd_fee_amount = 0
                for transfer in transfers:
                    treatment,_ = decustom(transfer['treatment'])
                    if treatment == 'fee':
                        fee_transfer = transfer
                        fee_amount = fee_transfer['amount']
                        fee_rate = fee_transfer['rate']
                        usd_fee_amount += fee_amount*fee_rate
                    elif treatment in ['buy','sell','burn','gift','income']:
                        cnt += 1
                if cnt > 0 and fee_amount is not None:
                    fee_amount_per_transaction = usd_fee_amount / cnt
                elif fee_transfer is not None:
                    fee_transfer['treatment'] = 'loss'



            for transfer in transfers:
                treatment, _ = decustom(transfer['treatment'])
                if treatment is None or treatment == 'ignore': #ignore means IGNORE!
                    continue

                tridx = transfer['index']
                outbound = False
                try:
                    what = transfer['what']
                except:
                    log('bad transfer',transfer)
                    exit(1)
                symbol = transfer['symbol']
                # if symbol == 'W' + self.chain.main_asset:
                #     what = self.chain.main_asset

                rate = transfer['rate']
                if rate is None:
                    rate = 0
                rate = str(rate)
                if 'custom' in rate:
                    rate = rate[7:]
                rate = float(rate)

                running_rates[what] = (rate,timestamp)



                if what not in totals:
                    totals[what] = {'symbol':transfer['symbol'],'amount':0,'nft_ids':set()}
                amount = transfer['amount']
                nft_id = transfer['token_nft_id']
                if transfer['outbound']:
                    outbound = True
                    totals[what]['amount'] -= amount
                    if nft_id is not None:
                        totals[what]['nft_ids'].remove(nft_id)

                else:
                    totals[what]['amount'] += amount
                    if nft_id is not None:
                        totals[what]['nft_ids'].add(nft_id)
                if abs(totals[what]['amount']) < 1e-4:
                    del totals[what]



                # treatment = transfer['treatment']
                # custom = False
                #
                # if treatment is not None and treatment[:7] == 'custom:':
                #     treatment = treatment[7:]
                #     custom = True


                to = transfer['to']
                fr = transfer['fr']

                if treatment in ['buy','sell']:
                    if treatment == 'sell':
                        amount = -amount
                    self.ca_transactions.append(CA_transaction(timestamp,what,symbol,amount,rate,txid,tridx,usd_fee=fee_amount_per_transaction))

                if treatment in ['gift','burn']:
                    if treatment == 'burn':
                        amount = -amount
                    self.ca_transactions.append(CA_transaction(timestamp,what,symbol,amount,0,txid,tridx,usd_fee=fee_amount_per_transaction))

                if treatment == 'income':
                    self.incomes.append({'timestamp':timestamp,'text':'Yield farming or similar income', 'amount':amount*rate, 'txid':txid,'tridx':tridx})
                    self.ca_transactions.append(CA_transaction(timestamp, what, symbol, amount, rate, txid,tridx, usd_fee=fee_amount_per_transaction))



                if treatment in ['deposit','withdraw','borrow','repay','full_repay','exit','liquidation']:
                    vault_id, _ = decustom(transfer['vault_id'])

                    vaddr = vault_id
                    # if vault_id is None:
                    #     cp_name = list(transaction['counter_parties'].values())[0][0]
                    #     if outbound:
                    #         vaddr = cp_name[:6]+" "+to[2:8] #to
                    #     else:
                    #         vaddr = cp_name[:6]+" "+fr[2:8] #fr
                    # if vault_id == 'type_name':
                    #     vaddr = transaction['type']
                    # else:
                    #     vaddr = vault_id

                    if vaddr == '':
                        print('wtf vault',transaction)

                    # if txid not in self.vaddr_info:
                    #     self.vaddr_info[txid] = {}
                    # self.vaddr_info[txid][tridx] = vaddr

                if treatment in ['borrow','repay','full_repay','liquidation']:
                    if vaddr not in loans:
                        loans[vaddr] = Loan(vaddr)
                    loan = loans[vaddr]



                    if treatment == 'borrow':
                        loan.borrow(transaction,tridx,what,symbol,amount)

                    if treatment == 'repay' or treatment == 'full_repay':
                        print(transfer)
                        interest_payments, v_trades = loan.repay(transaction,tridx,what,symbol,amount,running_rates,self.coingecko_rates,usd_fee=fee_amount_per_transaction,exit=treatment=='full_repay')
                        self.interest_payments.extend(interest_payments)
                        self.ca_transactions.extend(v_trades)



                    # if treatment == 'liquidation':
                    #     self.ca_transactions.append(CA_transaction(timestamp, what, symbol, -amount, rate, txid, tridx))
                    #     loan.liquidate(transaction, tridx, what,symbol,amount,running_rates, self.coingecko_rates)

                if treatment in ['deposit', 'withdraw','exit']:
                    if vaddr not in vaults:
                        vaults[vaddr] = Vault(vaddr)


                    vault = vaults[vaddr]
                    if outbound:
                        vault.deposit(transaction,tridx,what,symbol,amount)
                    else:
                        print(transfer)
                        v_trades, v_incomes, close = vault.withdraw(transaction,tridx,what,symbol,amount,running_rates,self.coingecko_rates,usd_fee=fee_amount_per_transaction,exit=treatment=='exit')
                        self.ca_transactions.extend(v_trades)
                        self.incomes.extend(v_incomes)
                        # if close:
                        #     del vaults[vaddr]

            prev_timestamp = timestamp


            # for vaddr in vaults_to_inspect:
            #     vault = vaults[vaddr]
            #     vault_close_res = vault.inspect(running_rates)
            #     if vault_close_res is not None:
            #         for what, vault_rem_amount in vault_close_res.items():
            #             symbol = vault.symbols[what]
            #             if vault_rem_amount > 0: #capital gains loss
            #                 self.ca_transactions.append(CA_transaction(timestamp, what, symbol, -vault_rem_amount, 0, fee_amount_per_transaction, fee_rate))
            #             elif vault_rem_amount < 0:
            #                 self.incomes.append({'timestamp': timestamp, 'text': 'Income upon closing a vault', 'amount': -vault_rem_amount * running_rates[what], 'hash':hash})
            #                 self.ca_transactions.append(CA_transaction(timestamp, what, symbol, -vault_rem_amount, running_rates[what], fee_amount_per_transaction, fee_rate))
            #
            #
            #
            #         del vaults[vaddr]

        # print("Totals")
        # pprint.pprint(totals)
        # print("\n\n")
        # for ca_trans in self.ca_transactions:
        #     print(ca_trans)
        dt = datetime.date(current_year + 1, 1, 1)
        new_year_ts = calendar.timegm(dt.timetuple())
        if self.mtm:
            mtm_dispose_all = self.buysell_everything(new_year_ts-1, totals, running_rates, sell=True,eoy=False)
            self.ca_transactions.extend(mtm_dispose_all)


        #check non-empty vaults
        # for vault_id,vault in vaults.items():
        #     total, empty, bad = vault.total_usd(self, new_year_ts, running_rates, self.coingecko_rates)


        print("Vaults")
        pprint.pprint(vaults)

        print("\n\n\nIncomes")
        pprint.pprint(self.incomes)

        print("\n\n\nInterest")
        pprint.pprint(self.interest_payments)

    def matchup(self):
        print("YOYO", len(self.ca_transactions))
        pprint.pprint(self.ca_transactions)
        queues = {}
        modes = {}
        CA_short = []
        CA_long = []
        CA_all = []
        errors = {}
        for idx, ca_trans in enumerate(self.ca_transactions):
            what = ca_trans.what
            symbol = ca_trans.symbol
            if what not in queues:
                queues[what] = []
                modes[what] = 1
            q = queues[what]
            mode = modes[what]

            amount = ca_trans.amount
            txid = ca_trans.txid
            tridx = ca_trans.tridx
            done = False
            while not done:
                switch = False
                if (amount > 0 and mode == 1) or (amount < 0 and mode == -1):
                    if mode == -1:
                        print("Putting short on q", ca_trans)
                    q.append(ca_trans)
                    done = True
                else:
                    initial_amount = amount = -amount
                    fees = initial_fees = ca_trans.usd_fee
                    rate = ca_trans.rate
                    pos_amount = amount * mode
                    while pos_amount > 0 and ((pos_amount * rate > 0.01 and rate != 0) or (pos_amount > 0.0001 and rate == 0)):

                        if len(q) == 0:
                            modes[what] = mode = -mode
                            amount = -amount
                            ca_trans.amount = amount
                            ca_trans.usd_fee = fees
                            if mode == 1:
                                ca_trans.basis = ca_trans.amount * ca_trans.rate
                            else:
                                ca_trans.sale = -ca_trans.amount * ca_trans.rate
                            switch = True
                            print("switch to",mode,ca_trans,'amt rem',pos_amount,amount)
                            # exit(1)
                            if mode == -1:
                                errors[txid] = {'level':3,'error':'going short','amount':amount,'what':what,'symbol':symbol}
                            else:
                                errors[txid] = {'level': 5, 'error': 'going long','amount':amount,'what':what,'symbol':symbol}
                            break

                        CA_in = q[0]

                        if mode == 1:
                            if CA_in.amount > amount:
                                prop_in = amount / CA_in.amount
                                basis_spent_in = CA_in.basis * prop_in
                                fees_spent_in = CA_in.usd_fee * prop_in
                                CA_in.amount -= amount
                                CA_in.basis -= basis_spent_in
                                CA_in.usd_fee -= fees_spent_in
                                basis = basis_spent_in + fees + fees_spent_in

                                CA_line = {'symbol':symbol,'what':what,'amount':amount,'in_ts':CA_in.timestamp,'out_ts':ca_trans.timestamp,
                                           'basis':basis,'sale':amount*rate,'out_txid':txid,'out_tridx':tridx, 'in_txid':CA_in.txid, 'in_tridx':CA_in.tridx}

                                fees = 0
                                amount = 0

                                if CA_in.amount*CA_in.rate < 0.01 and CA_in.rate != 0:
                                    del q[0]
                            else:
                                prop_out = CA_in.amount / amount
                                fees_spent = fees * prop_out
                                basis = CA_in.basis + fees_spent + CA_in.usd_fee
                                fees -= fees_spent
                                del q[0]
                                amount -= CA_in.amount
                                CA_line = {'symbol': symbol, 'what': what, 'amount': CA_in.amount, 'in_ts': CA_in.timestamp, 'out_ts': ca_trans.timestamp,
                                           'basis': basis, 'sale': CA_in.amount * rate, 'out_txid': txid, 'out_tridx': tridx, 'in_txid':CA_in.txid, 'in_tridx':CA_in.tridx}

                        else: #short, all amounts negative
                            if CA_in.amount < amount:
                                prop_in = amount / CA_in.amount

                                sale = CA_in.sale * prop_in
                                fees_spent_in = CA_in.usd_fee * prop_in
                                basis = -amount * rate + fees + fees_spent_in
                                CA_in.amount -= amount
                                CA_in.usd_fee -= fees_spent_in
                                CA_in.sale -= sale


                                CA_line = {'symbol': symbol, 'what': what, 'amount': -amount, 'out_ts': CA_in.timestamp, 'in_ts': ca_trans.timestamp,
                                           'basis': basis, 'sale': sale, 'in_txid': txid, 'in_tridx': tridx, 'out_txid': CA_in.txid, 'out_tridx': CA_in.tridx}

                                fees = 0
                                amount = 0

                                if -CA_in.amount * CA_in.rate < 0.01 and CA_in.rate != 0:
                                    del q[0]
                            else:
                                prop_out = CA_in.amount / amount
                                fees_spent = fees * prop_out
                                sale = CA_in.sale
                                basis = -CA_in.amount*rate + fees_spent + CA_in.usd_fee
                                fees -= fees_spent
                                del q[0]
                                amount -= CA_in.amount
                                CA_line = {'symbol': symbol, 'what': what, 'amount': -CA_in.amount, 'out_ts': CA_in.timestamp, 'in_ts': ca_trans.timestamp,
                                           'basis': basis, 'sale': sale, 'in_txid': txid, 'in_tridx': tridx, 'out_txid': CA_in.txid, 'out_tridx': CA_in.tridx}

                        pos_amount = amount * mode

                        CA_line['gain'] = CA_line['sale'] - CA_line['basis']
                        print('cl', pos_amount, amount, CA_line)
                        CA_all.append(CA_line)
                        if abs(CA_line['out_ts'] - CA_line['in_ts']) > 365 * 86400:
                            CA_long.append(CA_line)
                        else:
                            CA_short.append(CA_line)
                    if not switch:
                        done = True

        pprint.pprint(CA_long)
        self.CA_long = CA_long
        self.CA_short = CA_short
        self.errors = errors

    def cache(self):
        cache_file = open('data/users/' + self.chain.addr +"/"+self.chain.name+ "_calculator_cache", "wb")
        user = self.user
        self.user = None
        chain = self.chain
        self.chain = None
        coingecko_rates = self.coingecko_rates
        self.coingecko_rates = None

        pickle.dump(self, cache_file)
        cache_file.close()
        self.coingecko_rates = coingecko_rates
        self.user=user
        self.chain=chain

    def from_cache(self):
        C = pickle.load(open('data/users/' + self.chain.addr + "/" + self.chain.name + "_calculator_cache", "rb"))
        self.CA_long = C.CA_long
        self.CA_short = C.CA_short
        self.errors = C.errors
        self.eoy_mtm = C.eoy_mtm
        self.ca_transactions = C.ca_transactions
        self.incomes = C.incomes
        self.interest_payments = C.interest_payments
        self.mtm = C.mtm
        self.vaults = C.vaults
        self.loans = C.loans


    def make_forms(self, year):
        year = int(year)
        path = 'data/users/' + self.chain.addr +"/"+self.chain.name+ "_" +str(year)+"_"

        def timestamp_to_date(ts):
            return datetime.datetime.fromtimestamp(ts).strftime('%m/%d/%y')



        def CA_to_form(CA):
            rows = []
            total_proceeds = 0
            total_cost = 0
            for ca_line in CA:
                if timestamp_to_year(ca_line['out_ts']) == year:
                    row = [str(ca_line['amount']) +' units of '+str(ca_line['symbol']),
                           timestamp_to_date(ca_line['in_ts']),
                           timestamp_to_date(ca_line['out_ts']),
                           round(ca_line['sale'],2),
                           round(ca_line['basis'],2),
                           round(ca_line['gain'],2)
                           ]
                    rows.append(row)
                    total_proceeds += ca_line['sale']
                    total_cost += ca_line['basis']
            return rows, round(total_proceeds), round(total_cost)

        form_8949_short, short_total_proceeds, short_total_cost = CA_to_form(self.CA_short)
        form_8949_long, long_total_proceeds, long_total_cost = CA_to_form(self.CA_long)


        file_list = []

        if not self.mtm:
            if len(form_8949_short) > 0 or len(form_8949_long) > 0:
                form_file = open(path + 'form_1040_schedule_D.txt', 'w')
                if short_total_proceeds != 0:
                    gain = short_total_proceeds-short_total_cost
                    form_file.write("\nRow 3, column (d): "+str(short_total_proceeds))
                    form_file.write("\nRow 3, column (e): " + str(short_total_cost))
                    form_file.write("\nRow 3, column (h): " + str(gain))
                    form_file.write("\nRow 7, column (h): " + str(gain))

                if long_total_proceeds != 0:
                    gain = long_total_proceeds-long_total_cost
                    form_file.write("\nRow 10, column (d): "+str(long_total_proceeds))
                    form_file.write("\nRow 10, column (e): " + str(long_total_cost))
                    form_file.write("\nRow 10, column (h): " + str(gain))
                    form_file.write("\nRow 15, column (h): " + str(gain))

                form_file.write("\n\nYou will need to complete the rest of the form yourself")
                form_file.close()
                file_list.append('form_1040_schedule_D.txt')
        else:
            if len(form_8949_short) > 0:
                form_file = open(path + 'form_4797_part_2.csv', 'w')
                file_list.append('form_4797_part_2.csv')
                writer = csv.writer(form_file)
                writer.writerow(['Description of property', 'Date acquired', 'Date sold or disposed of', 'Proceeds/Gross sales price', 'Cost basis', 'Gain or loss'])
                rows = [
                    ['Trader - see attached','','',short_total_proceeds,short_total_cost,short_total_proceeds-short_total_cost]
                ]
                writer.writerows(rows)
                form_file.close()

                if self.eoy_mtm is not None:
                    form_file = open(path + 'mark_to_market_eoy_holdings.txt', 'w')
                    file_list.append('mark_to_market_eoy_holdings.txt')

                    writer = csv.writer(form_file)
                    writer.writerow(['Description of property'])
                    #totals[what] = {'symbol':transfer['symbol'],'amount':0,'nft_ids':set()}
                    rows = []
                    for what,entry in self.eoy_mtm.items():
                        desc = str(entry['amount'])+' units of '+entry['symbol']+' ('+what+')'
                        if len(entry['nft_ids']) != 0:
                            desc += ', NFT IDs:'+str(list(entry['nft_ids']))
                        rows.append([desc])
                    writer.writerows(rows)
                    form_file.close()




        if len(form_8949_short) > 0:
            if self.mtm:
                form_file = open(path+'form_4797_attachment.csv', 'w', newline='')
                file_list.append('form_4797_attachment.csv')
            else:
                form_file = open(path + 'form_8949_part_1.csv', 'w', newline='')
                file_list.append('form_8949_part_1.csv')
            writer = csv.writer(form_file)
            writer.writerow(['Description of property', 'Date acquired', 'Date sold or disposed of', 'Proceeds/Gross sales price', 'Cost basis', 'Gain or loss'])
            writer.writerows(form_8949_short)
            form_file.close()

        if not self.mtm:
            if len(form_8949_long) > 0:
                form_file = open(path+'form_8949_part_2.csv', 'w', newline='')
                file_list.append('form_8949_part_2.csv')
                writer = csv.writer(form_file)
                writer.writerow(['Description of property', 'Date acquired', 'Date sold or disposed of', 'Proceeds', 'Cost basis', 'Gain or loss'])
                writer.writerows(form_8949_long)
                form_file.close()

        log('incomes',self.incomes)
        if len(self.incomes) > 0:


            #{'timestamp':timestamp,'text':'Income upon closing a vault', 'amount':amount*rate, 'hash':transaction['hash']}
            income_types = {}
            for income in self.incomes:
                if timestamp_to_year(income['timestamp']) == year:
                    income_type = income['text']
                    if income_type not in income_types:
                        income_types[income_type] = 0
                    income_types[income_type] += income['amount']

            if len(income_types) > 0:
                form_file = open(path + 'form_1040_schedule_1.txt', 'w')
                file_list.append('form_1040_schedule_1.txt')
                total = 0
                for type, amount in income_types.items():
                    form_file.write("\nRow 8 income type: " + type+", amount: "+str(round(amount)))
                    total += amount
                form_file.write("\nRow 8 total: " + str(round(total)))
                form_file.close()


        compression = zipfile.ZIP_DEFLATED
        zf = zipfile.ZipFile(path+ "tax_forms.zip",mode='w')
        for filename in file_list:
            zf.write(path+filename,arcname=filename, compress_type=compression)
        zf.close()


    def vaults_json(self):
        js = {}
        for vault_id, vault in self.vaults.items():
            js[vault_id] = vault.to_json()
        return js

    def loans_json(self):
        js = {}
        for loan_id, loan in self.loans.items():
            js[loan_id] = loan.to_json()
        return js


    # def summary(self,CA):
    #     total_gain = 0
    #     for CA_line in CA:
    #         # print(CA_line)
    #         gain = CA_line['gain']
    #         # if gain < -1000:
    #         #     print("BIG LOSS",gain)
    #         #     print(CA_line)
    #         # if gain > 10000:
    #         #     print("BIG GAIN",gain)
    #         #     print(CA_line)
    #         total_gain += gain
    #     # print(total_gain)
    #     return total_gain

