function show_inspections(js) {
    vault_data = js['vaults'];
    mwr_counts = {};
    vault_ids_by_mwr = {};
    let total_problematic_cnt = 0;
    mwr_mapping = {0:'critical',3:'moderate',5:'potential'}
    for (vault_id in vault_data) {
        let entry = vault_data[vault_id];
        /*let history = entry['history'];
        let holdings = entry['holdings'];*/
        let warnings = entry['warnings'];
        let mwr = 10;
        let warning_map = {};
        for (warning of warnings) {
            if (warning['level'] < mwr)
                mwr = warning['level'];
            let txid = warning['txid'];
            if (!(txid in warning_map))
                warning_map[txid] = [];
            warning_map[txid].push(warning);
        }
        vault_data[vault_id]['warning_map'] = warning_map;
        vault_data[vault_id]['mwr'] = mwr;
        console.log('mwr',vault_id,mwr)
        if (mwr != 10) {
            if (!(mwr in mwr_counts)) {
                mwr_counts[mwr] = 0;
                vault_ids_by_mwr[mwr] = []
            }
            mwr_counts[mwr] += 1;
            vault_ids_by_mwr[mwr].push(vault_id);
            total_problematic_cnt += 1;
        }

    }

    $('#inspections_block').remove();
    let html = "<div id='inspections_block'>";

    if (total_problematic_cnt > 0) {
        let s = "";
        if (total_problematic_cnt > 1)
            s = "s";
        html += "<div class='vaults_summary'>You have "+total_problematic_cnt+" vault"+s+" with potential problems<a id='vaults_inspect'>Inspect</a></div>"
    }
    html += "<a id='vaults_inspect_all'>Inspect all vaults</a>";
    /*for (mwr in mwr_counts) {
        let s = "";
        if (mwr_counts[mwr] > 1)
            s = "s";
        html += "<div class='mwr mwr_"+mwr+"'>You have "+mwr_counts[mwr]+" vault"+s+"<a class='vault_inspect vault_inspect_"+mwr+"'>Inspect</a></div>";
    }*/

    html += "</div>";
    $('#tax_block').append(html);

}

$('body').on('click','#vaults_inspect', function() {
    $('.vault_list').remove();
    let html = "<ul class='vault_list'>";
    for (mwr in mwr_mapping) {
        if (mwr in vault_ids_by_mwr) {
            vault_ids = vault_ids_by_mwr[mwr];
            for (let vault_id of vault_ids) {
                html += "<li class='vault'><div class='item_header t_class_"+mwr+"'><div class='item_id'>"+vault_id+"</div><div class='inspect_item_ic'></div></div></li>";
            }
        }
    }
    html += "</ul>";
    $('#tax_block').append(html);
});

$('body').on('click','#vaults_inspect_all', function() {
    $('.vault_list').remove();
    let html = "<ul class='vault_list'>";
    for (let vault_id in vault_data) {
        mwr = vault_data[vault_id]['mwr'];
        html += "<li class='vault'><div class='item_header t_class_"+mwr+"'><div class='item_id'>"+vault_id+"</div><div class='inspect_item_ic'></div></div></li>";
    }
    html += "</ul>";
    $('#tax_block').append(html);
});


function display_vault_action(history_entry, symbols) {
    let action = history_entry['action'];
    let html = "<div class='vault_action'>";
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

    html += "</div>";
    return html;
}

function display_vault_warnings(warning_list, symbols) {
    html = "";
    for (warning of warning_list) {
        html +="<div class='vault_warning t_class_"+warning['level']+"'>Warning: "+warning['text']+"</div>";
    }
    return html;
}

$('body').on('click','.vault .item_header', function() {
    $('#opened_item').remove();
    if ($(this).hasClass('opened')) {
        $(this).removeClass('opened');
        return
    }

     $('.opened').removeClass('opened');

    vault_id = $(this).find('.item_id').html();
    let vault_info = vault_data[vault_id];
    let history = vault_info['history'];
    let holdings = vault_info['holdings'];
    let warnings = vault_info['warnings'];
    let symbols = vault_info['symbols'];
    let warning_map = vault_info['warning_map'];
    console.log('warning_map',warning_map);
    let mwr = vault_info['mwr'];

    html = "<div id='opened_item'>";
    html += "<table class='vault_table'><tr class='vault_table_header'><td class='vault_table_txnum'>TX #</td><td>Actions</td></tr>";
    let txid = null;
    for (let entry of history) {
        let new_txid = entry['txid'];
        if (new_txid != txid) {
            if (txid != null) {
                if (txid in warning_map) {
                    html += display_vault_warnings(warning_map[txid], symbols);
                }
                html += "</td></tr>";
            }
            txid = new_txid;
            let txnum = all_transactions[txid]['num'];
            html += "<tr";
            /*if (txid in warning_map) {
                let local_mwr = 10;
                for (warning of warning_map[txid]) {
                    if (warning['level'] < local_mwr)
                        local_mwr = warning['level'];
                }
                if (local_mwr < 10)
                    html += " class='t_class_"+local_mwr+"'";
            }*/
            html += "><td class='vault_table_txnum'><div class='txnum' id='txid_scroll_"+txid+"'>"+txnum+"</div></td><td>";
        }

        html += display_vault_action(entry, symbols);
    }
    html += "</td></tr></table></div>";
    $(this).closest('li').append(html);
    $(this).addClass('opened');

});

$('body').on('click','.txnum', function() {
    let txnum = $(this).attr('id').substr(12);
    el = $('#t_'+txnum);
    scroll_to(el);
});