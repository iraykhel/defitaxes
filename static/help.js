var help_strings = {
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

    'treatments':{'header':'Tax treatment options','explanation':
        '<h4>Ignore</h4><p>This transfer is ignored entirely, including in calculations of your available assets. If you ignore an inbound transfer and then spend it, '+
        'that action will open a short position. If you ignore a transfer with some token, we would recommend ignoring all transfers with that token.</p>' +
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
        'for example when the vault charged you a deposit fee. No need to use it when you got back same or more than you invested. '+
        'Non-closed vaults will show up as "vaults with potential problems" on the right side of the screen.</p>'
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
    html += "<li id='help_topic_treatments' class='help_topic help_topic_selected'>Tax treatment options</li>"
    html += "<li id='help_topic_examples' class='help_topic'>Kinds of transactions you might have</li>"
    html += "<li id='help_topic_vaultid' class='help_topic'>Vaults and loans</li>"
    html += "</ul></div>";

    let header = help_strings['treatments']['header'];
    let content = help_strings['treatments']['explanation'];
    html += "<div class='help_topic_content'>"
    html += "<div class='help_header'>"+header+"</div>";
    html += "<div class='help_text'>"+content+"</div>";
    html += "</div>";




    html += "</div>";
    $('#content').append(html);
});

$('body').on('click','#close_help_main',function() {
    $('#help_main_popup').remove();
    $('#overlay').remove();
});