function make_help_strings() {
    help_strings = {
        'mtm':{'header':"Mark to market",'explanation':
            'Mark-to-market accounting treats everything as income and disposes of all holdings at the end of the year. '+
            'Leave this off unless you had it enabled with the IRS by jumping through a million hoops.'
        },
        'createcustomtype':{'header':'Custom types','explanation':
            '<p>This powerful feature allows you to manually classify multiple transactions with one click.'+
            'If we failed to classify something correctly, and you made many of that kind of transactions, you should create a custom type to get them all classified without needing to go one-by-one.</p>'+
            '<p>Each custom type consists of one or more rules. We will check each transfer in your selected transactions against all the rules you defined. '+
            'We will apply the tax treatment from the first rule that matches each transfer. You can check the provided sample types.</p>'+
            '<p>To select transactions you want to apply the rule to, find one of them, and use "Select transactions with the same" checkboxes on the bottom.</p>'
        },
        'balanced':{'header':'Balanced transactions','explanation':
            '<p>If a transaction is balanced we assume that the total amount of '+fiat+' that you sent out is the same as you received. '+
            'More precisely, total dollar amount spent on transfers that are "Buy at market price" is the same as amount received from transfers that are "Sell at market price".</p>'+
            '<p>This allows us to infer dollar rates for tokens that are not on Coingecko, for example Uniswap pool tokens. '+
            'If all tokens are on Coingecko it allows us to use a more precise exhange rate for them.</p>'+
            '<p>The checkbox has no effect if there are only inbound transfers, or only outbound transfers.</p>'
        },
        'vaultid':{'header':'Vault and loan ID','explanation':
            '<p>Vaults and loans are concepts we use to improve your tax filing. A vault is anywhere you send your money for temporary storage without receiving anything in return. '+
            'Uniswap V2, for example, is not a vault, because you get a pool token in return. On the other hand, when you stake that pool token, you do not get anything back, so '+
            'the staking farm is a vault. A loan is when you borrow tokens from somewhere. </p>'+
            '<p>These are necessary because depositing money into a vault (or borrowing) is NOT a taxable event, but we must keep track of this money, because you might get back more money '+
            'from the vault than you put in (which is a taxable event), or you may pay loan interest (which may also be reported to the IRS).</p>'+
            '<p>The purpose of these IDs is to make sure we are correctly tracking the tokens that you deposited or borrowed. Make sure to use the same vault id when depositing and withdrawing '+
            'tokens from the same vault. Same for loan IDs.</p>'+
            '<p>Claiming rewards from a staking farm is not "withdraw from vault", it\'s just "income".</p>'
        },
        'tokenrule':{'header':'','explanation':'You can specify currency symbol, contract, or coingecko ID here.'},
        'issues':{'header':'Known issues and bugs','explanation':
            '<h4>Some scanners work intermittently</h4><p>We do not get our data straight from the blockchain, we get it from the third parties like Etherscan or Blockscout. '+
            '(full list of data providers is <a href="https://defitaxes.us/chains.html" target=_blank>here</a>). Some of these third parties work better than others. '+
            'When one of those does not work, you\'ll get an error message on top of the screen for that specific blockchain. '+
            'You can try importing transactions again, it might work. This problem should only happen with smaller chains like Kava or HECO.</p>'+
            '<h4>ERC-1155 support</h4><p>ERC-1155 is a type of token on some blockchains that combines the features of a regular coin and an NFT. Some NFTs will be this kind '+
            'of token, especially in gaming. We support it fully on Ethereum, and support it partially on Polygon, Avalanche, Arbitrum, and Fantom. Some transactions with these '+
            'tokens might be missing.</p>'+
            '<h4>Missing transactions</h4><p>On smaller chains with blockscout-type scanners, deposit transactions seem to often be missing. '+
            'Balance inspection should detect the discrepancy on most of them and provide an approximate fix. But not on Oasis, Velas, or SXnetwork.</p>'+
            '<h4>Optimism</h4><p>Transaction fees we display on Optimism are significantly lower than the real deal. This is what the scanner\'s API gives us. It\'s because '+
            'Optimism is an L2 chain and its fees consist of L1 part and L2 part, and we only see the L2 part. Balance inspection should detect the discrepancy and provide an '+
            'approximate fix.</p>'+
            '<h4>Other issues</h4><p>We can promise you that there are plenty. Please report whatever bugs you find on <a href="https://discord.gg/E7yuUZ3W4X" target="_blank">Discord</a>.<p>'
        },
        'mt':{'header':'Manual transactions','explanation':
            '<p>This lets you manually add a transaction to your list. It may be helpful, for example, if you want to preserve the long-term capital gain status when transferring tokens '+
            'from one place to another. By default we treat inbound transfers as purchases and outbound transfers as sales, which may break your long-term status. For instance, '+
            'if you want to keep the long term status when transferring tokens into your blockchain address, switch tax treatment for that transfer to "ignore", and add a manual '+
            'transaction acquiring these tokens at the correct date and price.</p>'+
            '<p>Another usage would be to add transfers and transactions that the scanner has missed. For example, Blockscout-based scanners (Kava, Canto, many other smaller chains) '+
            'often miss bridging deposits.</p>'
        },
        'cpop':{'header':'Counterparty and operation','explanation':
            '<p>We try to infer the counterparty you transacted with and what function you called from third-party sources. Counterparty is coming from Etherscan (or another scanner), and may '+
            'sometimes be very wrong. You can rename it; it will fix it in every transaction with the same counterparty. Operation is '+
            'typically correct, this is the function you are calling when interacting with the smart contract.</p>'
        },

        'start':{'header':'How do I use this thing?','explanation':
            '<p>We built this service to allow DeFi users to turn their blockchain transactions into tax forms, with the intention that these forms are then taken to a CPA '+
            '(although you may be able to manually enter the numbers in your tax-filing service, at this point we don\'t integrate with any). Unlike other blockchain tax processors '+
            'we don\'t claim to be able to magically turn all the wild variety of blockchain transactions into tax forms. While we have custom code for some of the most '+
            'popular DeFi protocols, it is a hopeless task for us to try to keep up with the innovations in the field. Your participation will be necessary if you '+
            'want your filing to be remotely close to correct. We aim to provide you with tools to do it in a reasonable amount of time (measured in hours), even if you have '+
            'a very large number of transactions.</p>'+
            '<p>The main, ahem, game loop is as follows:</p><p>Scroll down your transactions, looking for any that aren\'t green. Found one? Check if we processed it correctly. Don\'t know '+
            'if we did? Read "Kinds of transactions you might have" and find this kind of transaction. If we processed it correctly, recolor it green so you don\'t notice it anymore '+
            '(you don\'t actually have to recolor it, it doesn\'t do anything besides change colors). If we got it wrong, adjust the tax treatments. '+
            'If you performed this kind of transaction many times, create a custom type for it so you can get them all corrected at once.</p>'+
            '<p>After you processed all your transactions this way, hit "Recalculate taxes". Check the vaults and loans with potential problems and fix if they need fixing. '+
            'Hit "Recalculate taxes" again, pick your tax year, and download your tax forms.</p>'
        },

        'treatments':{'header':'Tax treatment options','explanation':
            '<h4>Ignore</h4><p>This transfer is ignored entirely, including in calculations of your available assets. If you ignore an inbound transfer and then spend it, '+
            'that action will open a short position. If you ignore one transfer of a token, we recommend ignoring all transfers involving that token.</p>' +
            '<h4>Buy</h4><p>Buy some tokens, spend '+fiat+'. Default price is '+fiat+' market price as provided by Coingecko (which may occasionally be wrong). You can adjust the price yourself. '+
            'This is typically not a taxable event and by itself has no effect on your tax forms (except when closing a short position).</p>'+
            '<h4>Sell</h4><p>Sell some tokens, receive '+fiat+'. Opposite of "Buy" treatment. Selling tokens is a taxable event and will add one or several lines to 8949 form. '+
            '<p>Anytime you spend your crypto on some service or product is also a "Sell".</p>'+
            '<h4>Acquire for free</h4><p>Same as "buy", but for price of 0. Do not use it for airdrops, rewards, mining, or anything else that gets you free money, use "Income" instead.'+
            ' Use sparingly for special situations.</p>'+
            '<h4>Dispose for free</h4><p>Same as "sell", but for price of 0. This makes it a tax-deductible capital loss. Use sparingly, '+
            'if you think this spending really is tax-deductible.</p>'+
            '<h4>Income</h4><p>Use this whenever you get free money, such as claiming staking rewards or receiving airdrops. The value is added to "ordinary income", and the asset is '+
            'considered purchased at the market price.</p>'+
            '<h4>Transaction cost</h4><p>This is typically the fee you pay to the network. It is added to the cost basis of assets acquired or disposed in the same transaction.</p>'+
//            '<h4>Non-deductible loss</h4><p>This is identical to "Sell".</p> '+
//            'Use this when you are spending your tokens on something other than buying other tokens. It is also a network fee for transactions that don\'t involve trading tokens.</p>'+
            '<h4>Borrow</h4><p>Use this when borrowing money from somewhere. See vaults and loans section for more info.</p>'+
            '<h4>Repay loan</h4><p>Use this when repaying your loan. Make sure the loan ID is the same as in the corresponding "Borrow" transfers. '+
            'See vaults and loans section for more info.</p>' +
            '<h4>Fully repay loan</h4><p>Use this with your final loan payment, if the total amount repaid is less than the total amount borrowed. This is used for liquidations, or for '+
            'self-repaying loans. There is no need to manually change "Repay loan" to "Fully repay loan" if total repaid amount is the same or larger than the amount borrowed.</p>'+
            '<h4>Deposit to vault</h4><p>Use this when depositing your tokens somewhere without getting a receipt. '+
            'See vaults and loans section for more info.</p>'+
            '<h4>Withdraw from vault</h4><p>Use this when receiving your tokens back from the vault. Make sure to use the same Vauld ID as in the corresponding "Deposit to vault" transfers. '+
            'See vaults and loans section for more info.</p>'+
            '<h4>Exit vault</h4><p>Use this when fully exiting a vault (nothing left in it), if the total amount of tokens you received is less than what you put in, '+
            'for example if the vault charged you a deposit fee. No need to use it when you got back same or more than you invested. '+
            'Non-closed vaults will show up as "vaults with potential problems" on the right side of the screen.</p>'
        },

        'examples':{'header':'Kinds of transactions you might have','explanation':
            '<h4>Transferring money to/from your blockchain address</h4><p>Normally, this is not a taxable event and shouldn\'t show up on the tax forms. However, that is not how we treat '+
            'it. We treat transfers into your address as purchases at market price, and transfers out as sales at market price. This is because otherwise we have no idea when you bought '+
            'your token and for how much. If the counterparty (wherever your source/destination address is) treats it the same way, it should not present a problem. There are two '+
            'potential issues with this approach. First, you may have some realized gains or losses when you should not have any. Second, this may break your long-term status for your '+
            'capital gains.</p>If you want to improve the filing for this kind of transactions: set the treatment on the transfer to "ignore", '+
            'and manually create a transaction where you are buying/selling this token at the time and price you specified.</p>' +
            '<h4>Spending crypto on a service or a product</h4><p>For tax purposes, this is a sale of that crypto for '+fiat+' and should be assigned a "sell" treatment.</p>' +
            '<h4>Bridging tokens between the same address on different blockchains</h4><p>We have some built-in handling for this, if both blockchains are supported. If sent amount is '+
            'exactly the same as received amount, and sent token and received token are the same as per Coingecko, we set both transfers to "ignore". If there is any '+
            'difference between the sent transfer and the received transfer, we set the sent transfer to "deposit to vault" and received transfer to "exit vault".</p>'+
            '<p>If you bridged to/from a different wallet address, or to/from a blockchain we don\'t support, we treat it as a usual external transfer described above.</p>'+
            '<h4>Exchange token A for token B</h4><p>This is a sale of token A, and a simultaneous purchase of token B. It is a balanced transaction, meaning total amount of '+fiat+' going out and '+
            'coming in is the same.</p>'+
            '<h4>Provide liquidity into a liquidity pool</h4><p>Typically you would deposit some amount of token A and some amount of token B into a pool, in return getting receipt token LP. '+
            'We treat this as exchange of tokens A and B for LP: this is a sale of token A, a sale of token B, and a purchase of token LP. The transaction is balanced, allowing us to infer the '+
            ''+fiat+' rate for LP even though it is typically not available on Coingecko.</p><p>An alternate way to treat this is depositing tokens A and B into a common vault, and ignoring '+
            'transfers of token LP. This may help decrease your number of taxable events, but is more error-prone.</p>'+
            '<h4>Provide liquidity elsewhere</h4><p>If you get something back, it\'s better treat it as an exchange of one thing for the other (apply '+
            'provided "Swap" custom type). If you didn\'t get anything back, treat it as "deposit to vault".</p>'+
            '<h4>Stake a token</h4><p>If you\'re staking a token in a farm without getting a receipt, this should be a deposit to a vault.</p><p>An alternate way to treat it is ignoring '+
            'this transfer altogether, but you can only do this if you get exactly the same amount of tokens when unstaking.</p>'+
            '<h4>Claim farming reward</h4><p>A farming reward is "income".</p>'+
            '<h4>Unstake a token</h4><p>This should be "withdraw from vault". If you\'re getting back less than what you put in (for example, if the vault charged you a deposit fee), use '+
            '"exit vault" instead.</p>'+
            '<h4>Provide collateral</h4><p>Typically you would provide collateral of token A and receive back token cA. We treat is as a simple exchange of A for cA. An alternate way is to '+
            'deposit A into a vault and ignore transfers involving cA. You do not need to specify loan ID when providing collateral, we don\'t connect collateral to the loan.'+
            '<h4>Take out a loan</h4><p>Taking out a loan is "borrow". Default loan ID is the counterparty you received the loan from + the address you borrowed from.</p>'+
            '<h4>Repay loan</h4><p>This is "Repay loan" tax treatment. Make sure to use the same loan ID as when borrowing. If you end up repaying less than what you borrowed '+
            '(for example with auto-repaying loan), use "Fully repay loan" in your last repaying transfer.</p>'+
            '<h4>Liquidation</h4><p>We do not have built-in support for liquidations. This is how you can manually process it: usually there will be a forcible transfer of '+
            'tokens representing collateral out of your address. Set this transfer to "dispose for free" to treat as capital loss. On your last transaction repaying the loan, '+
            'instead of "Repay loan" use "Fully repay loan" to let the software know the loan should be considered closed.</p>'+
            '<h4>Airdrop</h4><p>If this is a spammy worthless airdrop, you can just set it to "ignore". If it\'s a real airdrop, treat is as "income". You may need to specify '+
            'the price of airdropped tokens manually if you got the airdrop before they were listed on Coingecko.</p>'+
            '<h4>Wrap/unwrap</h4><p>Those are surprisingly annoying because they don\'t look like your normal exchange on the blockchain. We treat it as exchange of wrapped version '+
            'for unwrapped, or vice-versa. However, if we failed to automatically classify a wrap, you may need to create a manual transaction making sure you spend the tokens '+
            'to get the other version of them.</p>'+
            '<h4>Rebase</h4><p>Rebases are very hard because they do not generate a transaction on the blockchain. Our Balance Inspection functionality will '+
            'detect your rebasing assets and the total amount of rebase by comparing your current token holdings provided by a third-party to what we '+
            'calculated by going through all your transactions, but we will not know when the rebases occurred. If you are selling a rebasing '+
            'asset, you may need to manually create a transaction just before the sale. If a rebase was positive (number of tokens grew in your wallet), you will need to '+
            'create a transfer acquiring the difference for free. If negative, a transfer disposing the difference for free.</p>'+
            '<h4>Mint an NFT</h4><p>If you paid the mint price and got the NFT(s) in the same transaction, it\'s an exchange of one for the other. We should be able to infer '+
            'the mint price from the amount you spent. If you paid first, and received the NFT(s) later, you will need to set the payment transfer to "sell" and '+
            'manually provide the mint price in the minting transaction.</p>'
        },

        'dataimport':{'header':'Balance inspection','explanation':'<p>We check if we imported the data correctly from the scanners by comparing your current token balances '+
        '(provided by debank.com or Solana RPC) and NFTs (provided by simplehash.com or Solana RPC) '+
            'to what we calculated by going through every one of your transactions (including the ones you created manually). ' +
            'Tokens with positive balances not supported by debank.com are ignored. Tokens for which no exchange rate can be found are ignored. '+
            'NFTs not supported by simplehash.com are ignored. ERC-1155 NFTs are only supported on Ethereum. </p>'+
            '<p>A mismatch would indicate a rebasing token (most likely), missing data in the scanner (possible), an error in our code, or in debank/simplehash (unlikely). Tax treatment '+
            'has no effect. This means that even if you set tax treatment to "ignore", the transfer would still be counted.</p>'+
             '<p>This automatically detects rebasing tokens and other tokens that adjust your balance without a blockchain transaction. '+
             'Blockscout-based scanners for smaller chains (Kava, Canto, Aurora, some others) seem to often miss bridging transactions. '+
            'If it detects a sizable discrepancy for some other reason, please report the issue on discord.</p>'
        },

        'dc_fix':{'header':'Sorta kinda fix the discrepancy','explanation':'<p>There is no way for us to detect when rebases happen, or when your token balance changes without '+
            'a blockchain transaction for some other reason. However, we can try to approximate it. Using this will create manual transactions (as if you created them yourself) by '+
            'following these rules:</p><p>1) Whenever we detect your token balance going negative, we will create a transaction just before that moment to make sure your token balance goes '+
            'to zero instead.</p><p>2) After your very last transaction with the token, we will create another one to make sure your final balance matches the one we retrieved from debank.com.</p>'
        },

        'turbotax':{'header':'Instructions for TurboTax Online','explanation':
        '<h4>Capital gains & losses</h4>'+
        '<p>You can download the 8949 form in a TurboTax-compatible format. On TurboTax, you can upload it if you go spelunking into their UI:</p>'+
        '<ol><li>On the left side, click "Wages & Income"</li><li>Go to "Investments and Saving (1099-B, 1099-INT, 1099-DIV, 1099-K, Crypto)"</li>'+
        '<li>Click through their interface until you get to their list of financial institutions.</li>'+
        '<li>Click "Enter a different way" in the bottom-right corner</li><li>Click "Cryptocurrency"</li><li>Click "Upload it from my computer"</li>'+
        '<li>Choose "Other" in crypto service menu. <b>Not "Other (Transactions CSV)"</b></li>'+
        '<li>Enter "Defitaxes" in "Name". Or whatever you want, they don\'t care</li>'+
        '<li>Browse and upload the form we provided</li><li>It might say "needs review", but you can ignore it</li></ol></p>'+
        '<p>TurboTax has a limitation of no more than 4000 lines per upload. If you have more than that, we will batch them for you and have you download '+
        'a .zip file. In that case, you\'ll need to upload the batches one by one as described above.</p>'+


        '<h4>Ordinary income</h4><p>Besides the 8949 form, you will also need to report your income from yield farms etc. Time for another spelunking expedition!</p>'+
        '<ol><li>Go to "Wages & Income" as before</li><li>Go to "Less Common Income" on the very bottom</li>'+
        '<li>Go to "Miscellaneous Income, 1099-A, 1099-C" on the bottom of that</li><li>Go to "Other reportable income" on the bottom of THAT</li>'+
        '<li>Click "Yes". Enter "Income from decentralized cryptocurrency operations" in "Description"</li>'+
        '<li>In the "Amount" field enter the ordinary income reported on defitaxes.us for your tax year</li></ol></p>'+


        '<h4>Loan interest paid</h4><p>You may also consider deducting loan interest paid, but we advise against it. Consult with a CPA about it. If you want to, it is under '+
        'Deductions & Credits -> Retirement and Investments -> Investment Interest Expenses</p>'+

        '<h4>Business expenses</h4><p>If you want to deduct business expenses, you will need to use their search to search for "Schedule C". Note that filing it '+
        'requires an upgrade to Live Self-Employed version of TurboTax, may change how you would file you income, and you might be on the hook for additional '+
        'self-employment taxes.</p>'
        },

        'upload_csv':{'header':'Instructions for CSV uploads','explanation':
        '<p>You can upload your transactions from a CSV file. This is most useful when adding transactions from centralized exchanges. Download your order and transfer histories '+
        'from an exchange, convert them to defitaxes format, and upload it here.</p>'+
        '<p>Our upload format mimics that of CryptoTaxCalculator, you can read their guide <a href="https://cryptotaxcalculator.io/guides/advanced-manual-csv-import/" target=_blank>here</a>, '+
        'but we might process the same input in a different way. Specific instructions are in the template itself, <a target=_blank href="https://docs.google.com/spreadsheets/d/1dm-41zxpfS1BQUYgEhC1-kmt8egYJwu3IICCXomnN68/edit?usp=sharing">here</a>.</p>'+
        '<p>We have also prepared helpful conversion kits to convert from a few exchange formats to ours. To use them, first make a copy of the kit by going to File->Make a copy on Google Sheets, then '+
        'paste your data in the corresponding sheet and get it in our format in the next sheet over. You may need to make some modifications, and the conversion kits might not cover all the '+
        'kinds of data.</p><ul>'+
        '<li><a href="https://docs.google.com/spreadsheets/d/1ZFsCyleTYEfebS2i5wVdcpAMQ6I0b0vPzSQPoSN-kyY/edit?usp=sharing" target=_blank>Binance</a></li>'+
        '<li><a href="https://docs.google.com/spreadsheets/d/1bEiWQO_oo0GgnFlW35Cry3NxWIeIh9GrW_7cGdxd5zc/edit?usp=sharing" target=_blank>Coinbase Pro</a></li>'+
        '<li><a href="https://docs.google.com/spreadsheets/d/1JgnLDTJe72yK_eo-AmbKyuLh27xPp7UBS6HMHywK0xo/edit?usp=sharing" target=_blank>Kraken</a></li>'+
        '<li><a href="https://docs.google.com/spreadsheets/d/1FDcah9faGS3Hiu4aO3mPx2tmtaOxsNaESQWrqBaRRCc/edit?usp=sharing" target=_blank>Bittrex</a></li>'+
        '<li><a href="https://docs.google.com/spreadsheets/d/1-ENDeAINNh6e5g8zUX2D5eBfxhwQHgvI7_OS9k0jnaY/edit?usp=sharing" target=_blank>Kucoin</a></li>'+
        '</ul>'+
        '<p>Let us know if these kits are incomplete or out of date. If you make a conversion kit for a different exchange, we would love to add it.</p>'

        },
    }
}

$('body').on('click','.help',function() {
//    console.log('help click');
    for (let help_id in help_strings) {
        if ($(this).hasClass('help_'+help_id))
            help_popup(help_id);
    }
});

$('body').on('click','#close_help_popup',function() {
    $('#help_popup').remove();
    $('#overlay').remove();
});

$('body').on('click','#more_help',function() {
    window.open("https://discord.gg/E7yuUZ3W4X","_blank");
    $('#help_popup').remove();
    $('#overlay').remove();
});

function help_popup(id) {
//    console.log('help_popup',id);
    html ="<div id='overlay'></div><div id='help_popup' class='popup'>";
    html += "<div class='help_content'>"
    html += "<div class='help_header'>"+help_strings[id]['header']+"</div>";
    html += "<div class='help_text'>"+help_strings[id]['explanation']+"</div>";
    html += "</div>"
    html += "<div class='sim_buttons'>";
    html += "<div id='close_help_popup'>OK</div>";
    html += "<div id='more_help' title='Ask in our discord'>WUT?</div>";
    html += "</div>";
    html += "</div>";
    $('#content').append(html);
}




$('body').on('click','#help_main',function() {
//    console.log("main help");
    html ="<div id='overlay'></div><div id='help_main_popup' class='popup'>";
    html += "<div class='help_main_content'>"
    html += "<div id='close_help_main'></div>"

    html += "<div class='help_topics'>"

//    html += "<div class='help_topics_header'>Topics</div>"
    html += "<ul class='help_topics_list'>"
    html += "<li id='help_topic_start' class='help_topic help_topic_selected'>How do I use this thing?</li>"
    html += "<li id='help_topic_treatments' class='help_topic'>Tax treatment options</li>"
    html += "<li id='help_topic_examples' class='help_topic'>Kinds of transactions you might have</li>"
    html += "<li id='help_topic_vaultid' class='help_topic'>Vaults and loans</li>"
    html += "<li id='help_topic_turbotax' class='help_topic'>TurboTax integration</li>"
    html += "<li id='help_topic_issues' class='help_topic'>Known issues</li>"
    html += "</ul></div>";

    let header = help_strings['start']['header'];
    let content = help_strings['start']['explanation'];
    html += "<div class='help_topic_content'>"
    html += "<div class='help_header'>"+header+"</div>";
    html += "<div class='help_text'>"+content+"</div>";
    html += "</div>";

    html += "</div>";
    $('#content').append(html);
});

$('body').on('click','.help_topic',function() {
    $('.help_topic_selected').removeClass('help_topic_selected');
    $(this).addClass('help_topic_selected');
    let topic = $(this).attr('id').substr(11);
    let header = help_strings[topic]['header'];
    let content = help_strings[topic]['explanation'];
    $('.help_header').html(header);
    $('.help_text').html(content);
});

$('body').on('click','#close_help_main',function() {
    $('#help_main_popup').remove();
    $('#overlay').remove();
});