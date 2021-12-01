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
            '<p>If a transaction is balanced we assume that the total amount of USD that you sent out is the same as you received. '+
            'More precisely, total dollar amount spent on transfers that are "Buy at market price" is the same as amount received from transfers that are "Sell at market price".</p>'+
            '<p>This allows us to infer dollar rates for tokens that are not on Coingecko, for example Uniswap pool tokens. '+
            'If all tokens are on Coingecko it allows us to use a more precise exhange rate for them.</p>'+
            '<p>The checkbox has no effect if there are only inbound transfers, or only outbound transfers.</p>'
        },
        'vaultid':{'header':'Vault and loan ID','explanation':
            '<p>Vaults and loans are concepts we use to improve your tax filing. A vault is anywhere you send your money for temporary storage without receiving anything in return. '+
            'Uniswap, for example, is not a vault, because you get a pool token in return. On the other hand, when you stake that pool token, you do not get anything back, so '+
            'the staking farm is a vault. A loan is when you borrow tokens from somewhere. </p>'+
            '<p>These are necessary because depositing money into a vault (or borrowing) is NOT a taxable event, but we must keep track of this money, because you might get back more money '+
            'from the vault than you put in (which is a taxable event), or you may pay loan interest (which may also be reported to the IRS).</p>'+
            '<p>The purpose of these IDs is to make sure we are correctly tracking the tokens that you deposited or borrowed. Make sure to use the same vault id when depositing and withdrawing '+
            'tokens from the same vault. Same for loan IDs.</p>'+
            '<p>Claiming rewards from a staking farm is not "withdraw from vault", it\'s just "income".</p>'
        },
        'mt':{'header':'Manual transactions','explanation':
            '<p>This lets you manually add a transaction to your list. It may be helpful, for example, if you want to preserve the long-term capital gain status when transferring tokens '+
            'from one place to another. By default we treat inbound transfers as purchases and outbound transfers as sales, which may break your long-term status. For instance, '+
            'if you want to keep the long term status when transferring tokens into your blockchain address, switch tax treatment for that transfer to "ignore", and add a manual '+
            'transaction acquiring these tokens at the correct date and price.</p>'+
            '<p>Another usage would be to add transfers and transactions that etherscan missed. Right now we do not support ERC-1155 transfers; if you have any you might want to add them.</p>'
        },

        'start':{'header':'How do I use this thing?','explanation':
            '<p>We built this service to allow DeFi users to turn their blockchain transactions into tax forms, with the intention that these forms are then taken to a CPA '+
            '(although you may be able to manually enter the numbers in your tax-filing service, at this point we don\'t integrate with any). Unlike other blockchain tax processors '+
            'we don\'t claim to be able to magically turn all the wild variety of blockchain transactions into tax forms. While we have custom code for some of the most '+
            'popular DeFi protocols, it is a hopeless task for us to try to keep up with the innovations in the field. Your participation will be necessary if you '+
            'want your filing to be remotely close to correct. We aim to provide you with tools to do it in a reasonable amount of time (measured in hours), even if you have '+
            'a very large number of transactions.</p>'+
            '<p>The main, ahem, game loop is as follows:</p><p>Scroll down your transactions, looking for any that isn\'t green. Found one? Check if we processed it correctly. Don\'t know '+
            'if we did? Read "Kinds of transactions you might have" and find this kind of transaction. If we processed it correctly, recolor it green. If not, adjust tax treatments. '+
            'If you performed this kind of transaction many times, create a custom type for it so you can get them all corrected at once.</p>'+
            '<p>After you processed all your transactions this way, hit "Recalculate taxes". Check the vaults and loans with potential problems and fix if they need fixing. '+
            'Hit "Recalculate taxes" again, pick your tax year, and download your tax forms.</p>'
        },

        'treatments':{'header':'Tax treatment options','explanation':
            '<h4>Ignore</h4><p>This transfer is ignored entirely, including in calculations of your available assets. If you ignore an inbound transfer and then spend it, '+
            'that action will open a short position. If you ignore one transfer of a token, we recommend ignoring all transfers involving that token.</p>' +
            '<h4>Buy</h4><p>Buy some tokens, spend USD. Default price is USD market price as provided by Coingecko (which may occasionally be wrong). You can adjust the price yourself. '+
            'This is typically not a taxable event and by itself has no effect on your tax forms (except when closing a short position).</p>'+
            '<h4>Sell</h4><p>Sell some tokens, receive USD. Opposite of "Buy" treatment. Selling tokens is a taxable event and will add one or several lines to 8949 form.</p>'+
            '<h4>Acquire for free</h4><p>Same as "buy", but for price of 0. Do not use it for airdrops, rewards, mining, or anything else that gets you free money, use "Income" instead.'+
            ' Use sparingly for special situations.</p>'+
            '<h4>Dispose for free</h4><p>Same as "sell", but for price of 0. This makes it a tax-deductible capital loss. Use sparingly, '+
            'if you think this spending really is tax-deductible.</p>'+
            '<h4>Income</h4><p>Use this whenever you get free money, such as claiming staking rewards or receiving airdrops. The value is added to "ordinary income", and the asset is '+
            'considered purchased at the market price.</p>'+
            '<h4>Transaction cost</h4><p>This is typically the fee you pay to the network. It is added to the cost basis of assets acquired or disposed in the same transaction.</p>'+
            '<h4>Non-deductible loss</h4><p>This transfer is not reflected in tax forms in any way, but is accounted for in your total asset calculation. '+
            'Use this when you are spending your tokens on something other than buying other tokens. It is also a network fee for transactions that don\'t involve trading tokens.</p>'+
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
            '<h4>You transferring money to/from your blockchain address</h4><p>Normally, this is not a taxable event and shouldn\'t show up on the tax forms. However, that is not how we treat '+
            'it. We treat transfers into your address as purchases at market price, and transfers out as sales at market price. This is because otherwise we have no idea when you bought '+
            'your token and for how much. If the counterparty (wherever your source/destination address is) treats it the same way, it should not present a problem. There are two '+
            'potential issues with this approach. First, you may have some realized gains (losses) when you should not have any. Second, this may break your long-term status for your '+
            'capital gains.</p>If you want to improve the filing for this kind of transactions: for transfers into your account you can set the treatment on the inbound transfer to "ignore", '+
            'and manually create a transaction where you are buying this token at the time and price you specified. For outbound transfers, you can set the treatment to "non-deductible loss".</p>' +
            '<h4>Exchange token A for token B</h4><p>This is a sale of token A, and a simultaneous purchase of token B. It is a balanced transaction, meaning total amount of USD going out and '+
            'coming in is the same.</p>'+
            '<h4>Provide liquidity into a liquidity pool</h4><p>Typically you would deposit some amount of token A and some amount of token B into a pool, in return getting receipt token LP. '+
            'We treat this as exchange of tokens A and B for LP: this is a sale of token A, a sale of token B, and a purchase of token LP. The transaction is balanced, allowing us to infer the '+
            'USD rate for LP even though it is typically not available on Coingecko.</p><p>An alternate way to treat this is depositing tokens A and B into a common vault, and ignoring '+
            'transfers of token LP. This may help decrease your number of taxable events, but is more error-prone.</p>'+
            '<h4>Provide liquidity elsewhere</h4><p>If you get something back, it\'s better treat it as an exchange of one thing for the other (create a custom type to get '+
            'it treated as a balanced transaction and infer the rate for the receipt token). If you didn\'t get anything back, treat it as "deposit to vault".</p>'+
            '<h4>Stake a token</h4><p>If you\'re staking a token in a farm without getting a receipt, this should be a deposit to a vault.</p><p>An alternate way to treat it is ignoring '+
            'this transfer altogether, but you can only do this if you get exactly the same amount of tokens when unstaking.</p>'+
            '<h4>Claim farming reward</h4><p>A farming reward is "income".</p>'+
            '<h4>Unstake a token</h4><p>This should be "withdraw from vault". If you\'re getting back less than what you put in (for example, if the vault charged you a deposit fee), use '+
            '"exit vault" instead.</p>'+
            '<h4>Provide collateral</h4><p>Typically you would provide collateral of token A and receive back token cA. We treat is as a simple exchange of A for cA. An alternate way is to '+
            'deposit A into a vault and ignore transfers involving cA. You do not need to specify loan ID when providing collateral, we don\'t connect collateral to the loan.'+
            '<h4>Take out a loan</h4><p>Taking out a loan is "borrow". Default loan ID is tied to the address you received the loan from.</p>'+
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
            '<h4>Mint an NFT</h4><p>If you paid the mint price and got the NFT(s) in the same transaction, it\'s an exchange of one for the other. We should be able to infer '+
            'the mint price from the amount you spent. If you paid first, and received the NFT(s) later, you will need to set the payment transfer to "sell" and '+
            'manually provide the mint price in the minting transaction.</p>'+
            '<h4>Anything dealing with ERC-1155</h4><p>There is a small subset of NFT-related Ethereum transfers that Etherscan API currently does not support. You will need to '+
            'manually create transaction for them. Your list is <a target=_blank href="https://etherscan.io/address/'+addr+'#tokentxnsErc1155">here</a>.</p>'
        }
    }
}

$('body').on('click','.help',function() {
    console.log('help click');
    for (let help_id in help_strings) {
        if ($(this).hasClass('help_'+help_id))
            help_popup(help_id);
    }
});

$('body').on('click','#close_help_popup',function() {
    $('#help_popup').remove();
    $('#overlay').remove();
});

function help_popup(id) {
    console.log('help_popup',id);
    html ="<div id='overlay'></div><div id='help_popup' class='popup'>";
    html += "<div class='help_content'>"
    html += "<div class='help_header'>"+help_strings[id]['header']+"</div>";
    html += "<div class='help_text'>"+help_strings[id]['explanation']+"</div>";
    html += "</div>"
    html += "<div class='sim_buttons'>";
    html += "<div id='close_help_popup'>OK</div>";
    html += "<div id='more_help'>WUT?</div>";
    html += "</div>";
    html += "</div>";
    $('#content').append(html);
}


$('body').on('click','#help_main',function() {
    console.log("main help");
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