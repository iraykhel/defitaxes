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
    SELF = 20
    MULTISWAP = 21
    BALANCE_ADJUSTMENT = 22



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
        COMPOUND:"compound",
        SELF:"interaction between your accounts",
        MULTISWAP:"multi-transactional swap",
        BALANCE_ADJUSTMENT:"balance adjustment"
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

