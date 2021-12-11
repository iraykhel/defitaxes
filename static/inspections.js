function show_inspections(js) {
    $('#inspections_block').remove();
    let html = "<div id='inspections_block'>";
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
        let holdings = {}
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
        console.log('mwr',vault_id,mwr)
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
        html += "<div class='inspect_summary'>You have "+total_problematic_cnt+" "+which+s+" with potential problems<a id='"+which+"s_inspect'>Inspect</a></div>"
    }
    if (total_cnt > 0)
        html += "<a id='"+which+"s_inspect_all'>Inspect all "+which+"s</a>";
    html += "</div>";
    return html;

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
            vault_ids.sort();
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
    vault_list.sort()
    let html = "<ul class='item_list'>";
    for (let vault_id of vault_list) {
        mwr = lst[vault_id]['mwr'];
        html += "<li class='"+which+"'><div class='item_header t_class_"+mwr+"'><div class='item_id'>"+vault_id+"</div><div class='inspect_item_ic'></div></div></li>";
    }
    html += "</ul>";
    $('#'+which+'s_inspections_block').append(html);
});


function display_action(history_entry, symbols) {
    let action = history_entry['action'];
    let html = "<div class='action'>";
    if (action == 'deposit') {
        let symbol = symbols[history_entry['what']];
        html += "deposit "+round(history_entry['amount'])+" "+symbol;
    }

    if (action == 'withdraw') {
        let symbol = symbols[history_entry['what']];
        html += "withdraw "+round(history_entry['amount'])+" "+symbol;
    }

    if (action == 'conversion') {
        let from = history_entry['from'];
        let to = history_entry['to'];
        let symbol_from = symbols[from['what']];
        let symbol_to = symbols[to['what']];
        let amt_from = from['amount'];
        let amt_to = to['amount'];
        html += "convert "+round(amt_from)+" "+symbol_from +" to "+round(amt_to)+" "+symbol_to;
    }

    if (action == 'vault closed') {
        html += "vault drained of all contents"
    }

    if (action == 'income on exit') {
        let symbol = symbols[history_entry['what']];
        html += "generated "+round(history_entry['amount'])+" "+symbol +" of income";
    }

    if (action == 'loss on exit') {
        let symbol = symbols[history_entry['what']];
        html += "deductible loss of "+round(history_entry['amount'])+" "+symbol;
    }

    if (action == 'borrow') {
        let symbol = symbols[history_entry['what']];
        html += "Borrow "+round(history_entry['amount'])+" "+symbol;
    }

    if (action == 'repay') {
        let symbol = symbols[history_entry['what']];
        html += "Repay "+round(history_entry['amount'])+" "+symbol;
    }

    if (action == 'pay interest') {
        let symbol = symbols[history_entry['what']];
        html += "Pay interest of "+round(history_entry['amount'])+" "+symbol;
    }

    if (action == 'liquidation') {
        let symbol = symbols[history_entry['what']];
        html += "Liquidate "+round(history_entry['amount'])+" "+symbol+" of collateral";
    }

    if (action == 'loan repaid') {
        html += "Loan repaid";
    }

    if (action == 'buy loaned') {
        let symbol = symbols[history_entry['what']];
        html += "Buy remaining " + round(history_entry['amount']) + " "+symbol+" for $"+round_usd(history_entry['amount']*history_entry['rate']);
    }

    html += "</div>";
    return html;
}

function display_warnings(warning_list, symbols) {
    html = "";
    for (warning of warning_list) {
        html +="<div class='vault_warning t_class_"+warning['level']+"'>Warning: "+warning['text']+"</div>";
    }
    return html;
}

function check_vault_empty(holdings) {
    for (what in holdings) {
        if (holdings[what] > 0)
            return false
    }
    return true
}

function display_holdings(holdings, symbols) {
    html = "<table class='vault_table'><tr class='vault_table_header'><td class='vault_table_tok'>Token</td><td>Amount</td></tr>";
    for (what in holdings) {
        if (holdings[what] > 0)
            html += "<tr><td class='vault_table_tok'>"+symbols[what]+"</td><td>"+round(holdings[what])+"</td></tr>";
    }
    html += "</table>";
    return html;
}

function display_history(history,warning_map,symbols) {
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
                html += display_warnings(warning_map[txid], symbols);
            }
        }

        html += display_action(entry, symbols);
    }
    html += "</td></tr></table>";
    return html
}

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
    let holdings = {};
    if (is_vault)
        holdings = info['holdings'];
    else
        holdings = info['loaned'];
    let warnings = info['warnings'];
    let symbols = info['symbols'];
    let warning_map = info['warning_map'];
    console.log('warning_map',warning_map);
    let mwr = info['mwr'];

    html = "<div id='opened_item'>";
    html += "<div class='opened_subheader'>"
    if (is_vault) html += "Vault history"; else html += "Loan history";
    html += "</div>";
    html += display_history(history,warning_map,symbols)

    if (!check_vault_empty(holdings)) {
        html += "<div class='opened_spacer'></div>";
        html += "<div class='opened_subheader'>"
        if (is_vault) html += "Current vault holdings"; else html += "Currently on loan"
        html += "</div>";
        html += display_holdings(holdings,symbols);
    }

    html += "</div>";
    $(this).closest('li').append(html);
    $(this).addClass('opened');
});

$('body').on('click','.txnum', function() {
    let txnum = $(this).attr('id').substr(12);
    el = $('#t_'+txnum);
    scroll_to(el);
});