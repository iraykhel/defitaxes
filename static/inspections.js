function show_inspections(js) {
    $('#inspections_block').remove();
    let html = "<div id='inspections_block'>";
    html += show_inspections_dc(js);
    html += show_inspections_sub(js,'vault');
    html += show_inspections_sub(js,'loan');
    html += "</div>";
    $('#tax_block').append(html);
}

function show_inspections_sub(js,which) {
    let html = "<div id='"+which+"s_inspections_block'>";
    let data = js[which+'s'];
    let mwr_counts = {};
    let ids_by_mwr = {};
    let total_problematic_cnt = 0;
    let total_cnt = 0;
    mwr_mapping = {0:'critical',3:'moderate',5:'potential',10:'non-empty'}

    for (vault_id in data) {
        total_cnt += 1;
        let entry = data[vault_id];
        /*let history = entry['history'];*/
        let holdings = []
        if (which == 'vault')
            holdings = entry['holdings'];
        else
            holdings = entry['loaned'];

        let warnings = entry['warnings'];
        let mwr = 10;
        /*if (!check_vault_empty(holdings))
            mwr = 5;*/

        let warning_map = {};
        for (warning of warnings) {
            if (warning['level'] < mwr)
                mwr = warning['level'];
            let txid = warning['txid'];
            if (!(txid in warning_map))
                warning_map[txid] = [];
            warning_map[txid].push(warning);
        }
        data[vault_id]['warning_map'] = warning_map;
        data[vault_id]['mwr'] = mwr;
//        console.log('mwr',vault_id,mwr)
        if (mwr != 10 || !check_vault_empty(holdings)) {
            if (!(mwr in mwr_counts)) {
                mwr_counts[mwr] = 0;
                ids_by_mwr[mwr] = []
            }
            mwr_counts[mwr] += 1;
            ids_by_mwr[mwr].push(vault_id);
            total_problematic_cnt += 1;
        }

    }

    if (which == 'vault') {
        vault_data = data;
        vault_ids_by_mwr = ids_by_mwr;
    }

    if (which == 'loan') {
        loan_data = data;
        loan_ids_by_mwr = ids_by_mwr;
    }




    if (total_problematic_cnt > 0) {
        let s = "";
        if (total_problematic_cnt > 1)
            s = "s";
        html += "<div class='inspect_summary'><div class='inspect_summary_text'>"+total_problematic_cnt+" "+which+s+" may have problems</div>"
        html += "<a id='"+which+"s_inspect'>Inspect problematic</a>"
        html += "<a id='"+which+"s_inspect_all'>Inspect all</a>";
        html += "</div>"
    }
    else if (total_cnt > 0)
        html += "<a id='"+which+"s_inspect_all'>Inspect all "+which+"s</a>";
    html += "</div>";
    return html;

}

function compare_vaultids(a,b) {
     if (a.toLowerCase() < b.toLowerCase()) return -1;
    if (a.toLowerCase() > b.toLowerCase()) return 1;
    return 0;
}

$('body').on('click','#vaults_inspect,#loans_inspect', function() {
    $('.item_list').remove();
    let html = "<ul class='item_list'>";
    let ids_by_mwr = loan_ids_by_mwr;
    let which = 'loan';
    if ($(this).attr('id') == 'vaults_inspect') {
        ids_by_mwr = vault_ids_by_mwr;
        which = 'vault';
    }
    for (mwr in mwr_mapping) {
        if (mwr in ids_by_mwr) {
            vault_ids = ids_by_mwr[mwr];
            vault_ids.sort(compare_vaultids);
            for (let vault_id of vault_ids) {
                html += "<li class='"+which+"'><div class='item_header t_class_"+mwr+"'><div class='item_id'>"+vault_id+"</div><div class='inspect_item_ic'></div></div></li>";
            }
        }
    }
    html += "</ul>";
    $('#'+which+'s_inspections_block').append(html);
});

$('body').on('click','#vaults_inspect_all,#loans_inspect_all', function() {
    $('.item_list').remove();
    let lst = loan_data;
    let which = 'loan';
    if ($(this).attr('id') == 'vaults_inspect_all') {
        lst = vault_data;
        which = 'vault';
    }
    vault_list = [];
    for (let vault_id in lst) {
        vault_list.push(vault_id)
    }
    vault_list.sort(compare_vaultids)
    let html = "<ul class='item_list'>";
    for (let vault_id of vault_list) {
        mwr = lst[vault_id]['mwr'];
        html += "<li class='"+which+"'><div class='item_header t_class_"+mwr+"'><div class='item_id'>"+vault_id+"</div><div class='inspect_item_ic'></div></div></li>";
    }
    html += "</ul>";
    $('#'+which+'s_inspections_block').append(html);
});




function display_action(history_entry) {
    let action = history_entry['action'];
    let html = "<div class='action'>";
    let chain_name = all_transactions[history_entry['txid']]['chain'];
    let symbol = '???'
    if ('token' in history_entry)
        symbol = get_symbol(history_entry['token'],chain_name)

    if (action == 'deposit') {
//        let symbol = symbols[history_entry['lookup']][0];
        html += "deposit "+round(history_entry['amount'])+" "+symbol;
    }

    if (action == 'withdraw') {
        html += "withdraw "+round(history_entry['amount'])+" "+symbol;
    }

    if (action == 'conversion') {
        let from = history_entry['from'];
        let to = history_entry['to'];
        let symbol_from = get_symbol(from['token'],chain_name)
        let symbol_to = get_symbol(to['token'],chain_name)
        let amt_from = from['amount'];
        let amt_to = to['amount'];
        html += "convert "+round(amt_from)+" "+symbol_from +" to "+round(amt_to)+" "+symbol_to;
    }

    if (action == 'vault closed') {
        html += "vault drained of all contents"
    }

    if (action == 'income on exit') {
        html += "generated "+round(history_entry['amount'])+" "+symbol +" of income";
    }

    if (action == 'loss on exit') {
        html += "deductible loss of "+round(history_entry['amount'])+" "+symbol;
    }

    if (action == 'sell on exit') {
        html += "non-deductible loss of "+round(history_entry['amount'])+" "+symbol;
    }

    if (action == 'expense on exit') {
        html += "business expense of "+round(history_entry['amount'])+" "+symbol;
    }

    if (action == 'borrow') {
        html += "Borrow "+round(history_entry['amount'])+" "+symbol;
    }

    if (action == 'repay') {
        html += "Repay "+round(history_entry['amount'])+" "+symbol;
    }

    if (action == 'pay interest') {
        html += "Pay interest of "+round(history_entry['amount'])+" "+symbol;
    }

    if (action == 'liquidation') {
        html += "Liquidate "+round(history_entry['amount'])+" "+symbol+" of collateral";
    }

    if (action == 'loan repaid') {
        html += "Loan repaid";
    }

    if (action == 'buy loaned') {
        html += "Buy remaining " + round(history_entry['amount']) + " "+symbol+" for "+print_fiat(round_usd(history_entry['amount']*history_entry['rate']));
    }

    if (action == 'capgain on exit') {
        html += "acquired "+round(history_entry['amount'])+" "+symbol +" for free";
    }

    html += "</div>";
    return html;
}

function display_warnings(warning_list) {
    html = "";
    for (warning of warning_list) {
        html +="<div class='vault_warning t_class_"+warning['level']+"'>Warning: "+warning['text']+"</div>";
    }
    return html;
}

function check_vault_empty(holdings) {
    for (let pair of holdings) {
        if (pair[1] > 0)
            return false
    }
    return true
}

function display_holdings(holdings) {
    html = "<table class='vault_table'><tr class='vault_table_header'><td class='vault_table_tok'>Token</td><td>Amount</td></tr>";
    for (let pair of holdings) {
        if (pair[1] > 0) {
            let symbol = get_symbol(pair[0])
            html += "<tr><td class='vault_table_tok'>"+symbol+"</td><td>"+round(pair[1])+"</td></tr>";
        }
    }
    html += "</table>";
    return html;
}

function display_history(history,warning_map) {
    html = "<table class='vault_table'><tr class='vault_table_header'><td class='vault_table_txnum'>TX #</td><td>Actions</td></tr>";
    let txid = null;
    for (let entry of history) {
        let new_txid = entry['txid'];
        if (new_txid != txid) {
            if (txid != null) {
                html += "</td></tr>";
            }
            txid = new_txid;

            let txnum = all_transactions[txid]['num'];
            html += "<tr><td class='vault_table_txnum'><div class='txnum' id='txid_scroll_"+txid+"'>"+txnum+"</div></td><td>";
            if (txid in warning_map) {
                html += display_warnings(warning_map[txid]);
            }
        }

        html += display_action(entry);
    }
    html += "</td></tr></table>";
    return html
}

glob_history = 'bla';
$('body').on('click','.vault .item_header, .loan .item_header', function() {
    $('#opened_item').remove();
    if ($(this).hasClass('opened')) {
        $(this).removeClass('opened');
        return
    }

    let which = 'vault';
    let data = vault_data;
    let is_vault = true;
    if ($(this).closest('li').hasClass('loan')) {
        which = 'loan';
        data = loan_data;
        is_vault = false;
    }

     $('.opened').removeClass('opened');

    let item_id = $(this).find('.item_id').html();
    let info = data[item_id];
    let history = info['history'];
    glob_history = history;
    let holdings = [];
    if (is_vault)
        holdings = info['holdings'];
    else
        holdings = info['loaned'];
    let warnings = info['warnings'];
//    let symbols = info['symbols'];
    let warning_map = info['warning_map'];
//    console.log('warning_map',warning_map);
    let mwr = info['mwr'];

    html = "<div id='opened_item'>";
    html += "<div class='opened_subheader'>"
    if (is_vault) html += "Vault history"; else html += "Loan history";
    html += "</div>";
    html += display_history(history,warning_map)

    if (!check_vault_empty(holdings)) {
        html += "<div class='opened_spacer'></div>";
        html += "<div class='opened_subheader'>"
        if (is_vault) html += "Current vault holdings"; else html += "Currently on loan"
        html += "</div>";
        html += display_holdings(holdings);
    }

    html += "</div>";
    $(this).closest('li').append(html);
    $(this).addClass('opened');
});

$('body').on('click','.txnum', function() {
    let txid = parseInt($(this).attr('id').substr(12));
    go_to_transaction(txid);
//    el = $('#t_'+txid);
//    scroll_to(el);
});


function addup_running_tokens() {
    rv = {}
    for (let txid of transaction_order) {
        let tx = all_transactions[txid]
        let chain = tx['chain']
        let ts = tx['ts']
        if (!(chain in rv)) {
            rv[chain] = {}
        }
        let fiat_rate = tx['fiat_rate']

        let transfers = tx['rows']
        for (let trid in transfers) {
            let transfer = transfers[trid]
            let fr = transfer['fr']
            let to = transfer['to']
            let type = transfer['type']
            let contract = transfer['what']
            let symbol = transfer['symbol']
            let amount = transfer['amount']



            let rate = null
            let transfer_rate = transfer['rate']
            if (transfer_rate != null)
                transfer_rate *= fiat_rate
            rate = [transfer['rate_found'],transfer_rate,transfer['rate_source']]

            //if transfer is from my address to my other address, we need to account it twice
            let subs = []
            if (fr in all_address_info && chain in all_address_info[fr] && all_address_info[fr][chain]['used']) {
                subs.push([fr,-1])
            }

//            console.log('missing chain?',to,chain)
//            console.log(txid,'transfer',transfer, to, chain)
            if (to in all_address_info && chain in all_address_info[to] && all_address_info[to][chain]['used']) {
                subs.push([to,1])
            }
//            console.log('subs',subs, fr, to, all_addresses)


            for (sub of subs) {
                let adr = sub[0]
                let mult = sub[1]
                if (!(adr in rv[chain]))
                    rv[chain][adr] = {}

                let subdict = rv[chain][adr];
                let subamt = amount * mult;


                if (contract == '0xa02d547512bb90002807499f05495fe9c4c3943f') {
                    console.log('contract 0xa02d547512bb90002807499f05495fe9c4c3943f tx',tx,transfer)
                }

                if (type == 1 || type == 2 || type == 3) {
                    if (!(contract in subdict)) {
                        subdict[contract] = {'symbol':symbol, 'amount':0, 'adjusted_amount':0, 'negative_balance':[], 'last_ts':0}
                    }



                    subdict[contract]['amount'] += subamt
                    subdict[contract]['adjusted_amount'] += subamt
                    subdict[contract]['last_ts'] = ts
                    if (subdict[contract]['adjusted_amount'] < 0) {
                        negative_balance_entry = {'amount':-subdict[contract]['adjusted_amount'],'txid':txid,'ts':tx['ts'],'rate':rate}
                        subdict[contract]['negative_balance'].push(negative_balance_entry)
                        subdict[contract]['adjusted_amount'] = 0
                    }

                    if (chain == 'ETH' && contract == 'ETH') {
                        console.log(contract+" on "+ chain, tx['num'],txid,subamt,'running total',subdict[contract]['amount'])
                    }
                    subdict[contract]['rate'] = rate
                } else if (type == 4 || type == 5) {
                    if (!(contract in subdict)) {
                        subdict[contract] = {'symbol':symbol, 'nft_amounts':{}}
                    }

//                    if (!('nft_amounts' in subdict[contract]))
//                        subdict[contract]['nft_amounts'] = {}

                    let nft_id = transfer['token_nft_id']
//                    console.log('looking for nft id',nft_id,'contract',contract,'chain',chain,subdict[contract],'txid',txid)
                    if (nft_id == null || !('nft_amounts' in  subdict[contract])) {
                        console.log("THE SCANNER CAN'T DECIDE IF "+contract+" IS AN NFT OR NOT, SKIPPING",tx,transfer)
                        continue
                    }

                    if (!(nft_id in subdict[contract]['nft_amounts'])) {
                        subdict[contract]['nft_amounts'][nft_id] = 0
                    }
                    subdict[contract]['nft_amounts'][nft_id] += subamt
                }
            }




        }
    }
    return rv
}



function calc_token_amount_differences(data) {
    if ('latest_tokens' in data)
        latest_tokens = data['latest_tokens'];
    running_tokens = addup_running_tokens();
    diff = {}



    function add_contract(chain,address,contract, nft=false,symbol=null) {
        if (!(chain in diff))
            diff[chain] = {}
        if (!(address in diff[chain]))
            diff[chain][address] = {}
        if (nft) {
            if (!(contract in diff[chain][address]))
                diff[chain][address][contract] = {'symbol':symbol}
        }
    }

    for (let chain in latest_tokens) {
        for (let address in latest_tokens[chain]) {
            subdict = latest_tokens[chain][address]
            for (let contract in subdict) {
                if ('amount' in subdict[contract]) {
                    current_amount = subdict[contract]['amount']
                    try {
                        running_amount = running_tokens[chain][address][contract]['amount']
                        running_rate = running_tokens[chain][address][contract]['rate']
                    } catch(error) {
                        running_amount = 0
                        running_rate = [0,null,'none']
                    }
                    if (running_amount != current_amount) {
                        add_contract(chain, address, contract)
                        diff[chain][address][contract] = {'latest':current_amount,'running':running_amount,'symbol':subdict[contract]['symbol']}
                        latest_rate = subdict[contract]['rate']
//                        console.log('adding diff',chain,address,contract,subdict[contract],'running_rate',running_rate)
                        if (latest_rate[0] >= running_rate[0])
                            selected_rate = latest_rate
                        else
                            selected_rate = running_rate
                        diff[chain][address][contract]['rate'] = selected_rate

//                        if ('rate' in subdict[contract])
//                            diff[chain][address][contract]['rate'] = subdict[contract]['rate']
//                        else if (running_rate != null)
//                            diff[chain][address][contract]['rate'] = running_rate
                    }
                }

                if ('nft_amounts' in subdict[contract]) {
                    for (let nft_id in subdict[contract]['nft_amounts']) {
                        current_amount = subdict[contract]['nft_amounts'][nft_id]
                        try {
                            running_amount = running_tokens[chain][address][contract]['nft_amounts'][nft_id]
                        } catch(error) {
                            running_amount = 0
                        }
                        if (running_amount != current_amount) {
                            add_contract(chain, address, contract, nft=true, symbol=subdict[contract]['symbol'])
                            diff[chain][address][contract][nft_id] = {'latest':current_amount,'running':running_amount}
                        }
                    }
                }
            }
        }
    }

    for (let chain in running_tokens) {
        for (let address in running_tokens[chain]) {
            subdict = running_tokens[chain][address]
            for (let contract in subdict) {
                if (!(chain in latest_tokens))
                    latest_tokens[chain] = {}
                if (!(address in latest_tokens[chain]))
                    latest_tokens[chain][address] = {}
                if ('amount' in subdict[contract] && subdict[contract]['amount'] != 0) {
                    if (!(contract in latest_tokens[chain][address])) {
                        let running_amount = subdict[contract]['amount']
                        if (running_amount < 0) {
                            add_contract(chain, address, contract)
                            diff[chain][address][contract] = {'latest':0,'running':running_amount,'symbol':subdict[contract]['symbol']}
                            if ('rate' in subdict[contract])
                                diff[chain][address][contract]['rate'] = subdict[contract]['rate']
                        }
                    }
                }

//                if ('nft_amounts' in subdict[contract]) {
//                    if (!(contract in latest_tokens[chain][address])) {
//                        latest_tokens[chain][address][contract] = {}
//                    }
//                    for (let nft_id in subdict[contract]['nft_amounts']) {
//                        let amount = subdict[contract]['nft_amounts'][nft_id]
//                        if (amount != 0) {
////                            console.log('check',chain,address,contract,nft_id)
//                            if (!('nft_amounts' in latest_tokens[chain][address][contract]) || !(nft_id in latest_tokens[chain][address][contract]['nft_amounts'])) {
//                                add_contract(chain, address, contract, nft=true, symbol=subdict[contract]['symbol'])
//                                diff[chain][address][contract][nft_id] = {'latest':0,'running':amount}
//                            }
//                        }
//                    }
//                }

            }
        }
    }
    return diff
}

function show_inspections_dc(data) {
//    console.log('show_inspections_dc')
    let diff = calc_token_amount_differences(data)

    level = 10
    let symbol_list = []
    let display_symbol_list = []

    function single_asset_calc(chain, contract, entry, nft_id=null) {
//        console.log('single_asset_calc',entry)
        let local_level = 10;
        let symbol = entry['symbol']
        is_nft = false
        if (nft_id != null)  {
            entry = entry[nft_id]
            is_nft = true
        }
        let latest = entry['latest']
        let running = entry['running']
        let abs_d = Math.abs(latest - running)
        let rel_d = abs_d / (Math.abs(latest)+Math.abs(running))
        let abs_d_usd = 0
        if ('rate' in entry) {
            let rate_level = entry['rate'][0]
            if (rate_level >= 0.5) {
                abs_d_usd = abs_d*entry['rate'][1]
            }
        }
        if (rel_d > 0.001 && (abs_d_usd > 10 || is_nft)) {

            if (!(symbol_list.includes(symbol))) {
                symbol_list.push(symbol)
                display_symbol_list.push(display_token(symbol, contract, nft_id=nft_id, copiable=false)+" on "+chain)
            }
            local_level = 5

            if (rel_d > 0.01 && (abs_d_usd > 100 || is_nft)) {
                local_level = 3

                if (rel_d > 0.1 && (abs_d_usd > 1000 || is_nft)) {
                    local_level = 0
                }

            }

            if (local_level < level) level = local_level;
        }
        return local_level
    }

    token_amount_differences_problems = {}
    function add_mismatch(chain, address, contract, local_level,entry,nft_id=null) {
        if (!(address in token_amount_differences_problems))
            token_amount_differences_problems[address] = {}
        if (!(chain in token_amount_differences_problems[address]))
            token_amount_differences_problems[address][chain] = {'tokens':{},'nfts':{}}

        let symbol = entry['symbol']
        let rate_mult = null
        if ('rate' in entry)
            rate_mult = entry['rate'][1]

        if (nft_id != null)
            entry = entry[nft_id]
        let latest = entry['latest']
        let running = entry['running']
        let usd_diff = null
        if (rate_mult != null)
            usd_diff = (latest-running) * rate_mult
        if (nft_id != null) {
            if (!(contract in token_amount_differences_problems[address][chain]['nfts'])) {
                token_amount_differences_problems[address][chain]['nfts'][contract] = {'symbol':symbol,'nft_ids':{}}
            }
            token_amount_differences_problems[address][chain]['nfts'][contract]['nft_ids'][nft_id] = {'latest':latest,'running':running,'local_level':local_level,'usd_diff':usd_diff}
        } else {
            token_amount_differences_problems[address][chain]['tokens'][contract] = {'symbol':symbol,'latest':latest,'running':running,'local_level':local_level,'usd_diff':usd_diff,'rate':entry['rate']}
        }


    }

    for (let chain in diff) {
        for (let address in diff[chain]) {
            subd = diff[chain][address]

            for (let contract in subd) {
                if ('latest' in subd[contract]) {
//                    console.log(chain,contract, subd[contract])
                    local_level = single_asset_calc(chain, contract, subd[contract])
                    if (local_level < 10)
                        add_mismatch(chain,address,contract,local_level,subd[contract])


                } else {
                    for (let nft_id_or_symbol in subd[contract]) {
                        if (nft_id_or_symbol != 'symbol') {
//                            console.log(chain,contract, subd[contract], nft_id_or_symbol, subd[contract][nft_id_or_symbol])
                            local_level = single_asset_calc(chain, contract, subd[contract],nft_id=nft_id_or_symbol)
                            if (local_level < 10)
                                add_mismatch(chain,address,contract,local_level,subd[contract],nft_id=nft_id_or_symbol)
                        }
                    }
                }
            }
        }
    }

//    function compare_mismatches(m1,m2) {
//        return m1['usd_diff']-m2['usd_diff']
//    }
//
//    for (let address in token_amount_differences_problems) {
//        for (let chain in token_amount_differences_problems[address]) {
//            token_amount_differences_problems[address][chain]['tokens'].sort(compare_mismatches)
//        }
//     }

//    supported_chains = new Set(['ETH','Polygon','Arbitrum','Avalanche','Fantom','BSC','HECO','Moonriver','Cronos','Gnosis','Optimism','Celo','Doge','Songbird','Metis'])
    debank_unsupported_chains = []
    for (let chain of ordered_chains) {
        if (!chain_config[chain]['debank'])
            debank_unsupported_chains.push(chain)
    }

    let summary = "No problems detected"
    if (level < 10) {
        summary = "Problems with "

//        console.log('symbol_list',symbol_list)
        if (symbol_list.length > 10) {
            summary += symbol_list.length.toString()+" tokens"
        } else{
            summary += display_symbol_list.join(', ')
        }
    }
    if (debank_unsupported_chains.length > 0) {
        summary += "<br>("+debank_unsupported_chains.join(", ") + " not supported by Debank.com)"
    }

    if (level < 10)
        summary += "<a id='dc_inspect'>Inspect mismatches</a>"
    let html = "<div id='dc_inspections_block'>Balance check:<div class='help help_dataimport'></div>";
    if (data_version < 1.3)
        summary = "Import transactions to enable this feature"
    html += "<div id='inspect_summary'>"+summary+"</div>"
    html += "</div>"
    return html

}

$('body').on('click','#dc_inspect', function() {
    $('.item_list').remove();
    let html = "<ul class='item_list'>";
    let diff = token_amount_differences_problems
    let chain_cnt = dict_len(diff)

    function compare_mismatches(m1,m2) {
        return Math.abs(m2['usd_diff'])-Math.abs(m1['usd_diff'])
    }

    for (let address in diff) {

        for (let chain in diff[address]) {
            html += "<li class='dc_adr'>"+display_hash(address, name='address', copiable=true, replace_users_address=false)+":"+chain+"</li>";
            subd = diff[address][chain]
            let tok_list = subd['tokens']

            tok_ar = []
            for (let contract in tok_list) {
                tok_list[contract]['contract'] = contract
                tok_ar.push(tok_list[contract])
            }
            tok_ar.sort(compare_mismatches)


            for (let tok_data of tok_ar) {
                let contract = tok_data['contract']
                let symbol = tok_data['symbol']
                let usd_diff = tok_data['usd_diff']
                let disp_token = display_token(symbol,contract,null)
                let level = tok_data['local_level']
                html += "<li class='dc_token'><div class='item_header t_class_"+level+"'><div class='token_header' contract_info='"+address+"|"+chain+"|"+contract+"'>"+disp_token
                if (usd_diff != null)
                    html += ", "+print_fiat(round_usd(Math.abs(usd_diff)))
                html += "</div><div class='inspect_item_ic'></div></div></li>"
            }


            let nft_list = subd['nfts']
            for (let contract in nft_list) {
                let symbol = nft_list[contract]['symbol']
                for (let nft_id in nft_list[contract]['nft_ids']) {
                    let disp_token = display_token(symbol,contract,nft_id)
                    let nft_entry = nft_list[contract]['nft_ids'][nft_id]
                    let level = nft_entry['local_level']
                    let usd_diff = nft_entry['usd_diff']
                    html += "<li class='dc_token'><div class='item_header t_class_"+level+"'><div class='token_header' contract_info='"+address+"|"+chain+"|"+contract+"|"+nft_id+"'>NFT: "+disp_token
                    if (usd_diff != null)
                        html += ", "+print_fiat(round_usd(Math.abs(usd_diff)))
                    html += "</div><div class='inspect_item_ic'></div></div></li>"
                }
            }
        }
    }


    html += "</ul>";
    $('#dc_inspections_block').append(html);
});


$('body').on('click','.dc_token .item_header', function() {
    $('#opened_item').remove();
    if ($(this).hasClass('opened')) {
        $(this).removeClass('opened');
        return
    }

    $('.opened').removeClass('opened');

    let contract_info = $(this).find('.token_header').attr('contract_info')
//    console.log('contract_info',contract_info)
    let contract_info_ar = contract_info.split("|")

    let html = "<div id='opened_item'><table class='dc_info'>"
    let chain_name = contract_info_ar[1]
    if (contract_info_ar.length == 3) {
        let diff_data = token_amount_differences_problems[contract_info_ar[0]][chain_name]['tokens'][contract_info_ar[2]]
        let usd_diff = diff_data['usd_diff']
        let latest = diff_data['latest']
        let running = diff_data['running']
        let source = "Debank.com"
        if (chain_name == 'Solana')
            source = "Solana RPC"
        else if (debank_unsupported_chains.includes(chain_name))
            source = "Expected at least"
        html += "<tr><td>We calculated</td><td>"+round(running)+"</td></tr>"
        html += "<tr><td>"+source+" returned</td><td>"+round(latest)+"</td></tr>"
        html += "<tr><td>Difference</td><td>"+round(running-latest)+"</td></tr>"
        if (usd_diff != null)
            html += "<tr><td>Difference in "+fiat+"</td><td>"+round_usd(-usd_diff)+"</td></tr>"
    } else {
        let diff_data = token_amount_differences_problems[contract_info_ar[0]][chain_name]['nfts'][contract_info_ar[2]]['nft_ids'][contract_info_ar[3]]
        let usd_diff = diff_data['usd_diff']
        let latest = diff_data['latest']
        let running = diff_data['running']
        html += "<tr><td>We calculated</td><td>"+round(running)+"</td></tr>"
        let source = "Simplehash API"
        if (chain_name == 'Solana')
            source = "Solana RPC"
        html += "<tr><td>"+source+" returned</td><td>"+round(latest)+"</td></tr>"
        if (usd_diff != null)
            html += "<tr><td>Difference in "+fiat+" (at floor price)</td><td>"+round_usd(-usd_diff)+"</td></tr>"
    }



    html += "</table>"
    html += "<a id='dc_select_txs'>Show relevant transactions</a>";
    if (contract_info_ar.length == 3)
        html += "<a id='dc_fix'>Magically fix this</a><div class='help help_dc_fix'></div></div>"

    $(this).closest('li').append(html);
    $(this).addClass('opened');
});

$('body').on('click','#dc_select_txs', function() {
    let contract_info = $(this).closest('.dc_token').find('.token_header').attr('contract_info')
    let contract_info_ar = contract_info.split("|")
    let address = contract_info_ar[0]
    let chain = contract_info_ar[1]
    let contract = contract_info_ar[2]
    let nft_id = null
    if (contract_info_ar.length == 4)
        nft_id = contract_info_ar[3]

    let txids = []

    try {
        let single_lookup_set = lookup_info["chain_mapping"][chain]
        single_lookup_set = set_intersect(single_lookup_set,lookup_info["address_mapping"][address])
        if (nft_id == null)
            single_lookup_set = set_intersect(single_lookup_set,lookup_info["token_address_mapping"][contract])
        else
            single_lookup_set = set_intersect(single_lookup_set,lookup_info["token_nft_mapping"][contract+"_"+nft_id])

        txids = Array.from(single_lookup_set)
    } catch (error) {
        console.log("err", error)
        txids = []
    }
//    console.log('txids',txids)

    mark_all_deselected()
    for (let o_id of txids) {
//        o_id = parseInt(o_id)
        selected_transactions.add(o_id)
    }
    $('#sel_opt_sel').click();
    update_selections_block();
});


$('body').on('click','#dc_fix', function() {
    let contract_info = $(this).closest('.dc_token').find('.token_header').attr('contract_info')
    let contract_info_ar = contract_info.split("|")
    dc_fix_adjustments = []
    let html = "<div id='opened_item'><table class='dc_info'>"
    if (contract_info_ar.length == 3) {
        let adjusted_tally = 0
        dc_fix_address = contract_info_ar[0]
        dc_fix_chain = contract_info_ar[1]
        dc_fix_contract = contract_info_ar[2]
        let running_subd = running_tokens[dc_fix_chain][dc_fix_address][dc_fix_contract]
        if (typeof running_subd !== 'undefined' && 'negative_balance' in running_subd) {
            nb_entries = running_subd['negative_balance']
            last_ts = running_subd['last_ts']


            for (entry of nb_entries) {
                adjustment_amount = entry['amount']
                rate_mult = entry['rate'][1]
//                console.log("dc_fix_adjustment",rate_mult,adjustment_amount,rate_mult*adjustment_amount)
                if (rate_mult == null || Math.abs(rate_mult * adjustment_amount) > 0.01) {
                    dc_fix_adjustments.push({'amount':adjustment_amount,'ts':entry['ts']-1})
                    adjusted_tally += adjustment_amount
                }
            }

        } else {
            //find last chain ts
            last_ts = 0
            let chain_adr_ar = running_tokens[dc_fix_chain][dc_fix_address]
            for (let scontract in chain_adr_ar) {
                if (chain_adr_ar[scontract]['last_ts'] > last_ts)
                    last_ts = chain_adr_ar[scontract]['last_ts']
            }
        }


        final_diff_subd = diff[dc_fix_chain][dc_fix_address][dc_fix_contract]
        symbol = final_diff_subd['symbol']
        rate_mult = final_diff_subd['rate'][1]
        final_adjustment_amount = final_diff_subd['latest']-final_diff_subd['running']-adjusted_tally
        if (Math.abs(final_adjustment_amount) > 1e-9 && (rate_mult == null || Math.abs(rate_mult*final_adjustment_amount) > 0.01))
            dc_fix_adjustments.push({'amount':final_adjustment_amount,'ts':last_ts+1})
//        console.log("Adjustments",dc_fix_chain,symbol, dc_fix_adjustments)

        if (params['dc_fix_shutup']) {
            let form_data = adjustments_to_formdata(dc_fix_adjustments)
            post_manual_transactions(dc_fix_chain,form_data, function(new_txids) {$('#dc_select_txs').click(); $('#dc_select_txs').closest('.dc_token').remove();})
        } else {

            let html ="<div id='overlay'></div><div id='dc_fix_popup' class='popup'>";
    //        html += "<div class='dc_fix_popup_header'>What this will do</div>"
            html += "<div class='dc_fix_popup_header'>This will manually create the following transactions:</div>"
            html += "<ul class='dc_fix_popup_list'>"
            for (entry of dc_fix_adjustments) {
                amount = entry['amount']
                ts = entry['ts']
                ttime = timeConverter(ts);
                html += "<li>On "+ttime+" transfer "+round(Math.abs(amount))+" of "+display_token(symbol, dc_fix_contract, nft_id=null, copiable=false)+" from "
                address_disp = display_hash(dc_fix_address, name='address', copiable=false, replace_users_address=true, capitalize=false)
                if (amount > 0)
                    html += "null address to "+address_disp
                else
                    html += address_disp+" to null address"
                html += "</li>"
            }
            html += "</ul>"

            if (dc_fix_adjustments.length == 1)
                html+= "That will fix the balance mismatch. You will be able to delete or edit the transaction afterward."
            else
                html+= "That will fix the balance mismatches. You will be able to delete or edit them one by one afterward."
            html += " Go ahead?"
            html += "<div class='sim_buttons'><div id='dc_fix_proceed'>Proceed</div>"
            html += "<div id='dc_fix_proceed_shutup'>Proceed and don't ask again</div>"
            html += "<div id='dc_fix_cancel'>Cancel</div></div>";
            html += "</div>";
            $('#content').append(html);
        }
    }

});

function adjustments_to_formdata(dc_fix_adjustments) {
    let null_addr = "0x0000000000000000000000000000000000000000";
    let form_data = new FormData();
    idx = 0
    for (entry of dc_fix_adjustments) {
        form_data.append("mt"+idx+"_ts",entry['ts'])
        form_data.append("mt"+idx+"_chain",dc_fix_chain)
        form_data.append("mt"+idx+"_hash","")
        form_data.append("mt"+idx+"_op","Mismatch fix")
        form_data.append("mt"+idx+"_transfer_id0","-1")
        amount = entry['amount']
        if (amount > 0) {
            form_data.append("mt"+idx+"_from0",null_addr)
            form_data.append("mt"+idx+"_to0",dc_fix_address)
        } else {
            form_data.append("mt"+idx+"_from0",dc_fix_address)
            form_data.append("mt"+idx+"_to0",null_addr)
        }
        form_data.append("mt"+idx+"_what0",dc_fix_contract)
        form_data.append("mt"+idx+"_amount0",Math.abs(amount))
        form_data.append("mt"+idx+"_nft_id0","")
        form_data.append("mt"+idx+"_tr_disp_idx","1")
        idx += 1
    }
    return form_data
}

$('body').on('click','#dc_fix_proceed, #dc_fix_proceed_shutup', function() {
    if (dc_fix_adjustments.length < 1)
        return


    let form_data = adjustments_to_formdata(dc_fix_adjustments)
    if ($(this).attr('id') == 'dc_fix_proceed_shutup')
    {
        function followup(new_txids) {
            save_info('dc_fix_shutup',1)
            $('#dc_select_txs').click()
            $('.popup').remove();
            $('#overlay').remove();
            $('#dc_select_txs').closest('.dc_token').remove();
        }
    }else {
        function followup(new_txids) {
            $('#dc_select_txs').click()
            $('.popup').remove();
            $('#overlay').remove();
            $('#dc_select_txs').closest('.dc_token').remove();
        }
    }

    post_manual_transactions(dc_fix_chain,form_data, followup)

});