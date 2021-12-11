from code.sqlite import SQLite

class Category:
    FEE = -1
    SWAP = 0
    ADD_LIQUIDITY = 1
    REMOVE_LIQUIDITY = 2
    STAKE = 3
    UNSTAKE = 4
    DEPOSIT = 5
    AIRDROP = 6
    WITHDRAW = 7
    DEPOSIT_FROM_BRIDGE = 8
    WITHDRAW_TO_BRIDGE = 9
    WRAP = 10
    UNWRAP = 11
    ADD_LIQUIDITY_NO_RECEIPT = 12
    REMOVE_LIQUIDITY_NO_RECEIPT = 13
    BORROW = 14
    REPAY = 15
    MINT_NFT = 16
    ERROR = 17
    COMPOUND = 18
    EXIT_VAULT = 19



    CLAIM = 99
    NFT = 100

    mapping = {
        FEE: "fee",
        SWAP: "swap",
        ADD_LIQUIDITY: "deposit with receipt",
        REMOVE_LIQUIDITY: "withdraw with receipt",
        STAKE: "stake",
        UNSTAKE: "unstake",
        DEPOSIT: "transfer in",
        AIRDROP: "airdrop",
        WITHDRAW: "transfer out",
        DEPOSIT_FROM_BRIDGE: "transfer from bridge",
        WITHDRAW_TO_BRIDGE: "transfer to bridge",
        WRAP: "wrap",
        UNWRAP: "unwrap",
        ADD_LIQUIDITY_NO_RECEIPT: "deposit",
        REMOVE_LIQUIDITY_NO_RECEIPT: "withdraw",
        EXIT_VAULT:"exit vault",
        # NFT: "NFT-related",
        CLAIM: "claim reward",
        BORROW: "borrow",
        REPAY: "repay",
        MINT_NFT: "mint NFT",
        ERROR:"execution error",
        COMPOUND:"compound"
    }


    def __init__(self,category=0, claim_reward=False, nft=False, certainty=10, custom_type=None,protocol=None):
        self.category = category
        self.claim = claim_reward
        self.certainty = certainty
        if category == Category.CLAIM:
            self.claim = True
        self.nft = nft
        self.custom_type = custom_type
        self.protocol = protocol


    def __str__(self):
        if self.custom_type != None:
            return self.custom_type

        rv = ""
        if self.category is not None and self.category != Category.CLAIM:
            rv += Category.mapping[self.category]
            if self.claim:
                rv += " & "
        if self.claim:
            rv += "claim reward"

        if self.nft:
            rv += " (NFT-related)"
        if self.protocol:
            rv += " ON "+self.protocol
        return rv

    def __repr__(self):
        return self.__str__()


# class Typing:
#     def __init__(self):
#         self.db = SQLite('typing')
#         self.db.create_table('builtin_types', 'id integer primary key, name', drop=False)
#         for id, name in Category.mapping.items():
#             self.db.insert_kw('builtin_types', values=[id, name])
#
#         self.db.create_table('custom_types', 'id integer primary key autoincrement, user, name', drop=False)
#         self.db.create_index('custom_types_idx','custom_types','user, name', unique=True)
#         self.db.create_table('custom_types_rules', 'id integer primary key autoincrement, type_id integer, from_addr, from_addr_custom, to_addr, to_addr_custom, token, token_custom, treatment', drop=False)
#         self.db.create_table('custom_types_applied','type_id integer,transaction_hash',drop=False)
#         self.db.create_index('custom_types_applied_idx', 'custom_types_applied', 'type_id, transaction_hash', unique=True)
#         self.db.commit()


    # def load_builtin_types(self):
    #     rows = self.db.select("SELECT * from builtin_types")
    #     return rows
    #
    # def save_custom_type(self,address,name,rules, id=None):
    #     if id is not None:
    #         self.db.query("DELETE FROM custom_types WHERE id="+id)
    #         self.db.query("DELETE FROM custom_types_rules WHERE type_id=" + id)
    #
    #     self.db.insert_kw('custom_types',user=address,name=name)
    #     rows = self.db.select("SELECT id FROM custom_types WHERE user='"+address+"' and name='"+name+"'")
    #     type_id = rows[0][0]
    #     for rule in rules:
    #         from_addr, from_addr_custom, to_addr, to_addr_custom, token, token_custom, treatment = rule
    #         self.db.insert_kw('custom_types_rules',type_id=type_id,from_addr=from_addr,from_addr_custom=from_addr_custom,
    #                           to_addr=to_addr,to_addr_custom=to_addr_custom,
    #                           token=token,token_custom=token_custom, treatment=treatment)
    #     self.db.commit()
    #
    # def delete_custom_type(self,address,id):
    #     self.db.query("DELETE FROM custom_types WHERE id=" + id)
    #     self.db.query("DELETE FROM custom_types_rules WHERE type_id=" + id)
    #     self.db.commit()
    #
    # def load_custom_types(self,address):
    #     rows = self.db.select("SELECT t.id, t.name, r.* FROM custom_types as t, custom_types_rules as r WHERE t.user='"+address+"' and t.name != '' and t.id = r.type_id ORDER BY t.name ASC, r.id ASC")
    #     if len(rows) == 0:
    #         return []
    #
    #     js = []
    #     cur_type = {'id':rows[0][0], 'name':rows[0][1], 'rules':[]}
    #     for row in rows:
    #         type_id, type_name, rule_id, _, from_addr, from_addr_custom, to_addr, to_addr_custom, token, token_custom, treatment = row
    #         if cur_type['id'] != type_id:
    #             js.append(cur_type)
    #             cur_type = {'id':type_id,'name':type_name,'rules':[]}
    #         cur_type['rules'].append([rule_id,from_addr,from_addr_custom,to_addr,to_addr_custom,token,token_custom,treatment])
    #     js.append(cur_type)
    #     return js
    #
    # def apply_custom_type(self,address,type_id,transaction_list):
    #     for transaction_hash in transaction_list:
    #         self.db.insert_kw('custom_types_applied', type_id=type_id, transaction_hash=transaction_hash)
    #     self.db.commit()