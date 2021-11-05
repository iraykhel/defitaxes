import pprint
import traceback
from .util import *
from .sqlite import  SQLite
import requests
import time

class Signatures:

    def __init__(self):
        self.signatures = {}

    def download_signatures_to_db(self, endid=197245):
        db = SQLite('db')
        db.create_table('signatures', 'id INTEGER PRIMARY KEY, created_at, text_signature, hex_signature', drop=False)
        db.create_index('signatures_i1', 'signatures', 'hex_signature')

        done = False
        page = 1
        while not done:

            url = 'https://www.4byte.directory/api/v1/signatures/?page='+str(page)
            resp = requests.get(url)
            data = resp.json()
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
        if input is None or len(input) < 10:
            return None
        hex_sig = input[:10]
        return hex_sig

    def init_from_db(self, input_list):
        db = SQLite('db')
        mapping = {}
        for input in input_list:
            hex_sig = self.input_to_sig(input)
            if hex_sig is not None:
                rows = db.select("SELECT text_signature FROM signatures WHERE hex_signature='"+hex_sig+"' order by id ASC")
                if len(rows) >= 1:
                    text_sig = rows[0][0]
                    try:
                        obr_idx = text_sig.index('(')
                        text_sig = text_sig[:obr_idx]
                    except:
                        pass
                    mapping[hex_sig] = text_sig
        db.disconnect()
        self.signatures = mapping

    def lookup_signature(self,input):
        hex_sig = self.input_to_sig(input)
        if hex_sig is None or hex_sig not in self.signatures:
            return None, hex_sig
        return self.signatures[hex_sig], hex_sig

