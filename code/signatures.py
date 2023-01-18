import pprint
import traceback
from .util import *
from .sqlite import  SQLite
import requests
import time

class Signatures:

    def __init__(self):
        self.signatures = {}

    def download_signatures_to_db(self, start_page=None, endid=None):
        db = SQLite('db',do_logging=False)
        db.create_table('signatures', 'id INTEGER PRIMARY KEY, created_at, text_signature, hex_signature', drop=False)
        db.create_index('signatures_i1', 'signatures', 'hex_signature')

        if endid is None:
            rows = db.select("SELECT MAX(id) FROM signatures")
            endid = rows[0][0]

        done = False
        if start_page is None:
            page = 1
        else:
            page = start_page
        rep_cnt = 0
        while not done:

            url = 'https://www.4byte.directory/api/v1/signatures/?page='+str(page)
            resp = requests.get(url)
            try:
                data = resp.json()
                rep_cnt = 0
            except:
                print("FAILURE TO PARSE PAGE ",page)
                time.sleep(1)
                rep_cnt += 1
                if rep_cnt == 5:
                    print("SKIPPING PAGE ", page)
                    page += 1
                    rep_cnt = 0
                continue
            print('page', page, data['results'][0])
            try:
                for entry in data['results']:
                    id = entry['id']
                    db.insert_kw('signatures',values=[id,entry['created_at'],entry['text_signature'],entry['hex_signature']])
                db.commit()
                if id < endid:
                    done = True
            except:
                print('done',data)
                done = True
            time.sleep(0.25)
            page += 1

        db.disconnect()

    def input_to_sig(self,input):
        if input is None or len(input) < 10 or not isinstance(input,str) or input[:2].lower() != '0x':
            return None
        hex_sig = input[:10]
        return hex_sig

    def init_from_db(self, input_list):
        db = SQLite('db',do_logging=False, read_only=True)
        mapping = {}
        # log("input list",input_list)
        for input in input_list:
            hex_sig = self.input_to_sig(input)
            # log("hex_sig",hex_sig)
            if hex_sig is not None:
                rows = db.select("SELECT text_signature FROM signatures WHERE hex_signature='"+hex_sig+"' order by id ASC")
                if len(rows) >= 1:
                    text_sig = rows[0][0]
                    try:
                        obr_idx = text_sig.index('(')
                        text_sig = text_sig[:obr_idx]
                        # log("text_sig", text_sig)
                    except:
                        pass
                    mapping[hex_sig] = text_sig, len(rows) == 1
        db.disconnect()
        self.signatures = mapping

    def lookup_signature(self,input):
        decustomed, is_custom = decustom(input)
        if is_custom:
            return decustomed, decustomed

        hex_sig = self.input_to_sig(input)
        if hex_sig is None or hex_sig not in self.signatures:
            return None, 0, hex_sig
        return self.signatures[hex_sig][0], self.signatures[hex_sig][1], hex_sig

