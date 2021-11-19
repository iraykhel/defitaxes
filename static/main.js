scroll_position = null;

all_transactions = {}

lookup_info = {
    counterparty_mapping: {

    },
    counterparty_name_mapping: {

    },
    signature_mapping: {

    },
    inbound_count_mapping: {

    },
    outbound_count_mapping: {

    },
    inbound_token_mapping: {

    },
    outbound_token_mapping: {

    },
    address_mapping: {

    },
    last_index: 0,

    transactions: {

    }
}

options_in = {'ignore':'Ignore','buy':'Buy','gift':'Acquire for free','income':'Income','borrow':'Borrow','withdraw':'Withdraw from vault','exit':'Exit vault'};
options_out = {'ignore':'Ignore','sell':'Sell','burn':'Dispose for free','fee':'Transaction cost','loss':'Non-deductible loss','repay':'Repay loan','full_repay':'Fully repay loan','deposit':'Deposit to vault'};

var prev_selection = null;



function timeConverter(UNIX_timestamp){
  var a = new Date(UNIX_timestamp * 1000);
  var months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  var year = a.getFullYear();
  var month = months[a.getMonth()];
  var date = a.getDate();
  var hour = a.getHours();
  var min = ("0" +a.getMinutes()).substr(-2);
  var sec = ("0" +a.getSeconds()).substr(-2);
  var time = hour + ':' + min+':'+sec + ', ' + month + ' ' + date+' '+year;
  return time;
}

function copyToClipboard(element) {
    var $temp = $("<input>");
    $("body").append($temp);
    $temp.val($(element).text()).select();
    document.execCommand("copy");
    $temp.remove();
}


function startend(hash) {
    return hash.substring(0,5)+"..."+hash.substring(hash.length-3);
}

function display_hash(hash, my_addr=null, name='address') {
    if (hash == null) {
        return "<span class='hash'></span>";
    }

    hash = hash.toLowerCase();


    if (my_addr != null && my_addr.toUpperCase() == hash.toUpperCase()) {
        var html = "<span class='hash'>My address</span>";
//        var disp = "My address";
    } else {
        var html = "<span class='hash copiable' title='Copy full "+name+" to clipboard' full='"+hash+"'>"+startend(hash)+"</span>";
//        var disp = startend(hash);
    }
//    html += disp;
//    html += "</span>";
    return html;
}

function isNumeric(str) {
  return !isNaN(str) && // use type coercion to parse the _entirety_ of the string (`parseFloat` alone does not do this)...
         !isNaN(parseFloat(str)) // ...and ensure strings of whitespace fail
}

function round(rate) {
    if (isNumeric(rate) && !Number.isInteger(rate)) {
        if (rate == 0) {
            return 0;
        }
        return rate.toFixed(Math.max(0,Math.round(4-Math.log10(rate))));
    } else {
        return rate;
    }
}

function show_ajax_transactions(data) {
    selected_id = null;
    if ($('.primary_selected').length > 0) selected_id = $('.primary_selected').attr('id');
    for (let idx in data['transactions']) {
        let transaction = data['transactions'][idx];
        let txid = transaction['txid'];
        idx_html = $('#t_'+txid).find('.t_idx').html();
        transaction_html = make_transaction_html(transaction);
        $('#t_'+txid).replaceWith(transaction_html)
        $('#t_'+txid).addClass('secondary_selected');
        $('#t_'+txid).find('.t_idx').html(idx_html);
        process_errors(CA_errors, txid=txid)
//        populate_vault_info(vault_info=null,txid=txid);
    }
    if (selected_id != null) {
        $('#'+selected_id).click();
    }
}

$( document ).ready(function() {
    $('body').on('click', '.copiable', function() {
        var hash = $(this).attr('full');
        el = $(this);
        el.css({'background-color':'#8065f7','color':'white'});
        console.log(hash);
        var $temp = $("<input>");
        $("body").append($temp);
        $temp.val(hash).select();
        document.execCommand("copy");
        $temp.remove();
        setTimeout(function(){ el.css({'background-color':'','color':''}); }, 50);
    });

    //from cookie
    if (address != "" && chain != "") {
        $('#your_address').val(address);
        $('#chain').val(chain);
        show_last_update(last_transaction_timestamp);
    }

    $('#your_address').on('paste',function(e) {
        address = e.originalEvent.clipboardData.getData('text');
        chain = $('#chain').val();

        get_last_update();
    });


    $('#your_address, #chain').on('change',function() {
        address = $('#your_address').val();
        chain = $('#chain').val();
        get_last_update();
    });

});

function get_last_update() {
    if (address[0] != 0 || address.length != 42)
        return
    $.get("last_update?address="+address+"&chain="+chain, function(js) {
        var data = JSON.parse(js);
        last_transaction_timestamp = data['last_transaction_timestamp'];
        show_last_update(last_transaction_timestamp);
    });

}

function show_last_update(last_transaction_timestamp) {
    html = "";
    if (last_transaction_timestamp != 0) {
        ttime = timeConverter(last_transaction_timestamp);
        html = "Your last imported transaction was on "+ttime+".<br>";
        html += "<label>Import new transactions that you made since then? <input type=checkbox id='import_new_transactions'></label>";
    }
    $('#initial_options').html(html);
}



var pb_interval = null;
function start_progress_bar() {
    pb_html = "<div id='progressbar'></div><div id='pb_phase'>Processing...<div>"
    $('#content').html(pb_html);
    $( "#progressbar" ).progressbar({
      value: 0
    })
    pb_interval = setInterval(function() {
        addr = $('#your_address').val().toUpperCase();

        $.get("progress_bar?address="+addr, function(js) {
            var data = JSON.parse(js);
            current_phase = data['phase'];
            pb = data['pb'];
            $('#progressbar').progressbar({value: pb});
            $('#pb_phase').html(current_phase);

            if (pb == 100) {
                clearInterval(pb_interval);
            }

        });
    }, 1000);



}

function map_lookups(transaction) {
    txid = transaction['txid'];
    transaction_counterparties = transaction['counter_parties'];
    for (let progenitor in transaction_counterparties) {
        add_to_mapping('counterparty',progenitor,txid);
        hex_sig = transaction_counterparties[progenitor][1]
        add_to_mapping('signature',hex_sig,txid);
        cp_name = transaction_counterparties[progenitor][0];
        if (cp_name == 'unknown')
            cp_name = progenitor
        add_to_mapping('counterparty_name',cp_name,txid);
    }
    rows = transaction['rows'];
    outbound_count = 0;
    inbound_count = 0;
    for (let ridx in rows) {
        let row = rows[ridx];
        if (row['to'] != null) { //network fee
            token_contract = row['what'];
            if (row['outbound']) {
                add_to_mapping('outbound_token',token_contract,txid);
                outbound_count += 1;
                other_address = row['to'];
            } else {
                add_to_mapping('inbound_token',token_contract,txid);
                inbound_count += 1;
                other_address = row['fr'];
            }
            add_to_mapping('token',token_contract,txid);
            if (other_address != '0x0000000000000000000000000000000000000000')
                add_to_mapping('address',other_address,txid);
        }
    }

    add_to_mapping('outbound_count',outbound_count,txid);
    add_to_mapping('inbound_count',inbound_count,txid);
}

function add_to_mapping(mapping_type, value, txid) {
    if (value == null) return;
    mcode = mapping_type +"_mapping"
    if (!(mcode in lookup_info))
        lookup_info[mcode] = {}

    mapping = lookup_info[mcode];
    if (!(value in mapping))
        mapping[value] = new Set();
    mapping[value].add(txid);

    transaction_list = lookup_info['transactions'];
    lookups = ['counterparty','counterparty_name','signature','outbound_count','token','inbound_count','outbound_token','inbound_token','address'];
    if (!(txid in transaction_list)) {
        transaction_list[txid] = {}
        for (i = 0; i < lookups.length; i++)
            transaction_list[txid][lookups[i]] = new Set();
    }

    transaction_list[txid][mapping_type].add(value);
}

function remove_from_mapping(mapping_type,value,txid) {
    if (value == null) return;
    mapping = lookup_info[mapping_type+"_mapping"];
    mapping[value].delete(txid);
    transaction_list[txid][mapping_type].delete(value);
}


function display_counterparty(transaction, editable=false) {
    html = "<div class='tx_row_2'>"
    transaction_counterparties = transaction['counter_parties'];
    cp_len = Object.keys(transaction_counterparties).length
    if (cp_len > 0) {
        cp_idx = 0;
        for (let progenitor in transaction_counterparties) {
            cp = transaction_counterparties[progenitor][0]
            hex_sig = transaction_counterparties[progenitor][1]
            signature = transaction_counterparties[progenitor][2]

//            if (cp != 'unknown' && cp.length > 0) {
//                counterparty_list.push(cp)
//            }

            if (editable) {
                if (signature != null)
                    html += "Operation: <span class='signature'>"+signature+"</span> @ ";
                else
                    html += "Counterparty: "


                html += "<span class='cp prog_"+progenitor+" cp_"+addr+"' progenitor='"+progenitor+"' counter='"+addr+"' title='Update counterparty'>"+cp+"</span>";
            } else {
                if (signature != null) {
                    html += "Operation: <span class='op'>"+signature+" @ "+cp+"</span>";
                } else
                    html += "Counterparty: <span class='op'>"+cp+"</span>";
                transaction_html += cp+"</span>";
            }

            if (cp_idx != cp_len -1) transaction_html += ","
            cp_idx += 1
        }
    }
    html += "</div>";
    return html;

}

function del_transfer_val(transaction_id, transfer_idx, val_to_delete) {
    transfers = all_transactions[transaction_id]['rows'];
    for (let transfer of transfers) {
        if (transfer['index']  == transfer_idx) {
            delete transfer[val_to_delete];
            break;
        }
    }
}

function set_transfer_val(transaction_id, transfer_idx, what, val, append=false) {
    transfers = all_transactions[transaction_id]['rows'];
    for (let transfer of transfers) {
        if (transfer['index']  == transfer_idx) {
            if (append) {
                if (!(what in transfer))
                    transfer[what] = [];
                transfer[what].push(val);
            } else
                transfer[what] = val;
            break;
        }
    }

}

function display_transfers(transaction, editable=false) {

    rows = transaction['rows'];
    rows_table = "<div class='transfers'><table class='rows'>";
    rows_table += "<tr class='transfers_header'><td class=r_from_addr>From</td><td></td><td class=r_to_addr>To</td>"
    rows_table += "<td class=r_amount>Amount</td><td class=r_token>Token</td><td class=r_treatment>Tax treatment</td>";
    if (editable) {
        show_vaultid_col = true;
        rows_table += "<td class=r_vaultid>Vault/loan ID<div class='help help_vaultid'></div></td>";
    } else {
        show_vaultid_col = false;
        for (let ridx in rows) {
            let row = rows[ridx];
            treatment = row['treatment'];
            if (treatment != null && treatment.includes('custom:')) {
                treatment = treatment.substr(7);
            }
            if (['repay','deposit','borrow','withdraw','exit','liquidation','full_repay'].includes(treatment)) {
                show_vaultid_col = true;
                rows_table += "<td class=r_vaultid>Vault/loan ID<div class='help help_vaultid'></div></td>";
                break;
            }
        }
    }
    rows_table += "<td class=r_rate>USD rate</td></tr>";

    let txid = transaction['txid'];

    for (let ridx in rows) {
        let cust_treatment_class = "";
        let cust_rate_class = "";
        let cust_vaultid_class = "";
        let row = rows[ridx];
        from = row['fr'];
        to = row['to'];
        if (to == null)
            editable = false;
        amount = row['amount'];
        token_name = row['symbol'];
        token_contract = row['what'];
        treatment = row['treatment'];
        index = row['index'];

        if (treatment != null && treatment.includes('custom:')) {
            cust_treatment_class = " custom";
            treatment = treatment.substr(7);
        }
        good_rate = row['rate_found'];
        rate = row['rate'];
        if (rate != null && rate.toString().includes('custom:')) {
            cust_rate_class = " custom";
            rate = parseFloat(rate.substr(7));
        }


        vault_id = row['vault_id'];
        if (vault_id != null && vault_id.toString().includes('custom:')) {
            cust_vaultid_class = " custom";
            vault_id = vault_id.toString().substr(7);
        }

        if (to != null) {
            disp_to = display_hash(to,addr);
        } else {
            disp_to = 'Network fee';
        }

        row_html = "<tr index="+index+"><td class='r_from_addr'>"+display_hash(from,addr)+"</td><td class='r_arrow'><div></div></td><td class='r_to_addr'>"+disp_to+"</td><td class='r_amount'>"+round(amount)+"</td><td class='r_token'>"+token_name+"</td>";


        row_html+="<td class='r_treatment"+cust_treatment_class+"'>"
        if (editable)
            row_html += "<select class='treatment'>";


        if (to != null && to.toUpperCase() == addr) {
            options = options_in;
        } else {
            options = options_out;
        }

        hidden_vaultid_class = " class='hidden'";
        treatment_found = 0;
        for (let option in options) {
            opt_exp = options[option];
            if (editable)
                row_html += "<option ";
            /*if ((option == 'buy' || option == 'sell')) {
                if (rate == null) {
                    continue;
                } else {
                  opt_exp += ' for '+round(rate)+' USD/'+token_name;
                }
            }*/
            if (option == treatment) {
                if (['repay','deposit','borrow','withdraw','exit','liquidation','full_repay'].includes(option)) {
                    console.log(transaction['num'],index,'show vaultid')
                    hidden_vaultid_class = "";
                }

                if (editable)
                    row_html += " selected ";
                else {
                    row_html += opt_exp;
                    treatment_found = 1;
                    break;
                }

            }
            if (editable)
                row_html +="value='"+option+"'>"+opt_exp+"</option>\n";
        }
        if (editable) {
            row_html += "</select>"
//            row_html += "<input type=text class=row_rate value="+round(rate)+" default="+rate+">";
        } else if (!treatment_found)
            row_html += Object.values(options)[0];

        row_html +="</td>";

        if (show_vaultid_col) {
            row_html += "<td class='r_vaultid"+cust_vaultid_class+"'><span"+hidden_vaultid_class+">";
            if (editable)
                row_html += "<input type=text class=row_vaultid value='"+vault_id+"' default='"+vault_id+"'>";
            else
                row_html += vault_id;
            row_html += "</span></td>";
        }

//        if (show_vaultid) {
//            if (editable)
//                row_html += "<td class='r_vaultid"+cust_vaultid_class+"'><input type=text class=row_vaultid value='"+vault_id+"' default='"+vault_id+"'></td>";
//            else
//                row_html += "<td class='r_vaultid"+cust_vaultid_class+"'>"+vault_id+"</td>";
//        } else
//            row_html += "<td class='r_vaultid'></td>";


        rounded_rate = round(rate);
        if (rounded_rate == null)
            rounded_rate = 0;
        if (editable) {
            row_html += "<td class='r_rate"+cust_rate_class+"'><input type=text class=row_rate value="+rounded_rate+" default="+rate+"></td>";
        } else
            row_html += "<td class='r_rate"+cust_rate_class+"'>"+rounded_rate+"</td>";


        row_html+="</tr>";

        if (typeof matchup_texts !== 'undefined')
            row_html += make_matchup_html(txid,index);

        rows_table += row_html;



    }
    rows_table += "</table></div>";
    return rows_table

}

function make_transaction_html(transaction,idx=null,len=null) {

    transaction_color = transaction['classification_certainty']
    if (transaction['rate_adjusted'] != false && transaction['rate_adjusted'] > 0.05 && !transaction['nft']) {
       transaction_color = 3;
       bad_rate_adjustment = true;
    } else bad_rate_adjustment = false;




    missing_rates = [];
    txid = transaction['txid'];
    transaction['num'] = parseInt(idx)+1;
    all_transactions[txid] = transaction;

    rows = transaction['rows'];
    for (let ridx in rows) {
        let row = rows[ridx];
        rate = row['rate'];
        if (rate == null) {
            to = row['to'];
            treatment = row['treatment'];
            if (to != null && to.toUpperCase() == addr) {
                options = options_in;
            } else {
                options = options_out;
            }
            for (let option in options) {
                if ((option == treatment)) { // && ['buy','sell'].includes(option)) {

                    if (transaction['type'] != 'transfer in') //airdrops don't have rates
                        transaction_color = 0;
                    if (!missing_rates.includes(row['symbol']))
                        missing_rates.push(row['symbol']);
                }
            }
        }
    }

    type_class = "";
    type = transaction['type'];
    if (type == null) {
        type = 'unknown';
        type_class = 't_class_unknown';
    }

    if (type.includes("NOT SURE"))
        type_class = 't_class_unknown';

    ct_id = transaction['ct_id'];
    transaction['original_color'] = transaction_color;
    transaction_html = "<div id='t_"+txid+"' class='transaction t_class_"+transaction_color+" "+type_class;
    if ('custom_color_id' in transaction)
        transaction_html += " custom_recolor custom_recolor_"+transaction['custom_color_id'];
    if (ct_id != null)
        transaction_html += " custom_type custom_type_"+ct_id;
    transaction_html += "'>";
    ts = transaction['ts'];
    ttime = timeConverter(ts);



//                                transaction_html += "<input type=checkbox class='t_sel'>";
    transaction_html += "<div class='top_section'><div class='tx_row_0'><span class='t_time copiable' title='Copy timestamp to clipboard' full='"+ts+"'>"+ttime+"</span>";
    transaction_html += "<span class='tx_hash'>TX hash: "+display_hash(transaction['hash'], null, "hash");
//                                transaction_html += "<input type=hidden name='tx_hash' class='tx_hash_inp' value="+transaction['hash']+">";
    transaction_html += "<a class='open_scan' title='Open in scanner' href='https://"+scanner+"/tx/"+transaction['hash']+"' target=_blank></a></span>";
    if (idx == null)
        transaction_html += "<span class='t_idx'></span>";
    else
        transaction_html += "<span class='t_idx'>"+(parseInt(idx)+1)+"/"+len+"</span>";
    transaction_html += "</div>";

    ct_id = transaction['ct_id'];
    if (ct_id != null)
        transaction_html += "<div class='tx_row_1'><input type=hidden name=ct_id class=ct_id value="+ct_id+"><span class='t_class'>Your classification: "+type.toUpperCase()+"</span>";
    else
        transaction_html += "<div class='tx_row_1'><span class='t_class'>Our classification: "+type.toUpperCase()+"</span>";

    transaction_html += "</div>";
    transaction_counterparties = transaction['counter_parties'];
    cp_len = Object.keys(transaction_counterparties).length

    transaction_html += display_counterparty(transaction,false);

    if (transaction['rate_inferred'] != false) {
        transaction_html += "<div class='note'>Note: Exchange rate for "+transaction['rate_inferred']+" is inferred from the other currencies in this transaction</div>";
    }
    if (bad_rate_adjustment) {
        transaction_html += "<div class='note note_5'>Note: Rates for currencies in this transaction might be wrong</div>";
    }
    if (missing_rates.length > 0) {
        transaction_html += "<div class='note note_0'>Note: Could not find required rate ("+missing_rates.join(', ')+") for this transaction, assuming 0</div>";
    }
    transaction_html += "</div>";

    rows_table = display_transfers(transaction,false);

    transaction_html += rows_table;

    transaction_html += "</div>\n";
    return transaction_html
}




$(function() {
    $( document ).ready(function() {
        activate_clickables();
//            $('a#submit_address').click(function() {
        $('#main_form').submit( function(e) {
                e.preventDefault();
//                counterparty_list = []
//                progenitor_counts = {}
                addr = $('#your_address').val();
                chain = $('#chain').find(':selected').val();
                document.cookie = "address="+addr+"|"+chain+";path=/;expires=Fri, 31 Dec 9999 23:59:59 GMT";
                addr = addr.toUpperCase();
                scanner = 'etherscan.io'; base_token = 'ETH';
                if (chain == 'Polygon') { scanner = 'polygonscan.com'; base_token = 'MATIC'; }
                if (chain == 'BSC') { scanner = 'bscscan.com'; base_token = 'BNB'; }
                if (chain == 'HECO') { scanner = 'hecoinfo.com'; base_token = 'HT'; }
                if (addr[0] !=0 || addr[1] != 'X' || addr.length != 42) {
                    $('#content').html('Not a valid address');
                    return
                } else {
                    $(document.body).css({'cursor' : 'wait'});
//                    $('#content').html('Processing...');
                    start_progress_bar()

                    import_new=1;
                    if ($('#import_new_transactions').length) {
                        if ($('#import_new_transactions').is(':checked')) import_new=1;
                        else import_new = 0;
                    }

                    $.get("process?address="+addr+"&chain="+chain+"&import_new="+import_new, function( js ) {
                        $('#address_form').css({'margin-top':'0px'});
                        $('#main_form').css({'margin-top':'0px','padding':'5px','border-radius':'5px'});
                        $('#initial_options').css({'display':'none'});
                        $('#submit_address').css({'width':'auto','margin-top':'0px'});
                        try {
                            var data = JSON.parse(js);
                        } catch (error) {
                            $('#content').html(data);
                            return;
                        }
                        if (data.hasOwnProperty('error')) {
                            $('#content').html(data['error']);
                            return;
                        }
                        all_transactions = {}
                        len = data['transactions'].length;
                        if (len == 0) {
                            $('#content').html('No transactions found');
                        } else {
//                            window.sessionStorage.setItem('js',js); #DOES NOT ALWAYS FIT!
                            window.sessionStorage.setItem('address',addr);
                            window.sessionStorage.setItem('chain',chain);

                            var all_html = "<div id='top_text'>Make sure to check red, orange, and yellow transactions.</div>";

                            for (let idx in data['transactions']) {
                                let transaction = data['transactions'][idx];
                                map_lookups(transaction);
                                transaction_html = make_transaction_html(transaction, idx, len);


                                all_html += transaction_html;
                            }
//                            $('#content').html("<form id='transaction_list'>"+all_html+"</form>");
                            $('#content').html("<div id='transaction_list'>"+all_html+"</div>");


                            display_tax_block();
                            process_tax_js(data);
                            show_inspections(data);
                            lookup_info['last_index'] = len;

                            console.log('custom types?',data['custom_types']);
                            selection_operations(data['builtin_types'],data['custom_types']);
                        }

                        $(document.body).css({'cursor' : 'default'});





//                        counterparty_list = Array.from(new Set(counterparty_list))
//                        counterparty_list.sort()

//                        console.log([counterparty_list])

//                        set_autocompletes();

//                        activate_clickables();


                    });


                }


//                console.log(addr);
//                submit_address();
        });
    });
});


$('body').on('click',function() {
    console.log('gc');
});

function select_transaction(txel) {
    t1 = performance.now();
    if (txel.hasClass('primary_selected')) {
        return;
    }

    t_id = txel.attr('id');
    console.log('selected',t_id);
    let txid = t_id.substr(2);
    deselect_primary();
    prev_selection = txid;
    console.log('prev_selection',prev_selection);
    if (!event.ctrlKey)
        $('.secondary_selected').removeClass('secondary_selected');

    txel.addClass('primary_selected').addClass('secondary_selected');


    t2 = performance.now();
    el = txel.find('.select_similar');

    txel.find('.tx_row_2').replaceWith(display_counterparty(all_transactions[txid],true));
    txel.find('.transfers').replaceWith(display_transfers(all_transactions[txid],true));
    if (el.length)
        show(el);
    else {
        var html = "<div class='select_similar'><div class='header'>Select transactions with the same:</div>";
        let lookups = {
            counterparty_name:['counterparty','checked'],
            signature:['operation','checked'],
            address:['addresses'],
            outbound_count:['number of sent transfers',''],
            inbound_count:['number of received transfers','']
//            outbound_token:['sent tokens',''],
//            inbound_token:['received tokens','']
        };
        for (let lookup in lookups) {
            console.log('initial lookup',lookup);
            lookup_vals = lookup_info['transactions'][txid][lookup];
            if (lookup_vals.size > 0)
                html += "<label><input type=checkbox "+lookups[lookup][1]+" class='sim_"+lookup+"'>"+lookups[lookup][0]+"</label>";
        }

        let tokens = {};
        for (let transfer of all_transactions[txid]['rows']) {
            if (transfer['to'] != null)
                tokens[transfer['symbol']] = transfer['what'];
        }

        for (let token_symbol in tokens)
            html += "<label><input type=checkbox class='sim_token' token_address='"+tokens[token_symbol]+"'>token:"+token_symbol+"</label>";

        html += "<div class='sim_buttons'><div class='select_similar_button'></div>";
        html += "<div class='undo_changes'>Undo custom changes</div>"
        html += "<div class='deselect_this_button'>Deselect this transaction</div></div>";
        html += "<input type=hidden class='current_sims' value=''>";
        html += "</div>";
        txel.append(html);
        if (txel.find('.custom').length > 0)
            showib(txel.find('.undo_changes'))
        t3 = performance.now();
        similar_transactions = find_similar_transactions(txid);


    }
    t4 = performance.now();
    update_selections_block();
    t5 = performance.now();
    console.log('perf',t2-t1,t3-t2,t4-t3,t5-t4,'total',t5-t1);
}

function activate_clickables() {
//    col = document.getElementsByClassName('transaction');
//    $(col).on('click',function(event) {

    $('body').on('click','div.transaction',function(event) {
//    $('body').on('click','div#t_3, div#t_2',function(event) {
//      $('#t_3,#t_2').on('click',function(event) {
//    $('div.transaction').on('click',function(event) {
        console.log('!');
        el = $(this);
        select_transaction(el);
    });

    $('body').on('click','.select_similar input',function() {
        txid = $(this).closest('.transaction').attr('id').substr(2);
        similar_transactions = find_similar_transactions(txid);
    });

    $('body').on('click','.deselect_this_button',function(event) {
        deselect_primary(true);
        update_selections_block();
        event.stopPropagation();
    });

    $('body').on('click','.select_similar_button',function() {
        if ($(this).hasClass('grayed')) return;
        t_id = $(this).closest('.transaction').attr('id');
        str = $('#'+t_id+' .current_sims').val();
        o_ids = str.split(',');
        id = t_id.substr(2);
        cnt = 0;
        $('.secondary_selected').removeClass('secondary_selected');
        for (let o_id of o_ids) {
            $('#t_'+o_id).addClass('secondary_selected');
            cnt += 1;
        }
        $('#sel_opt_sel').click();
        update_selections_block();
//        $(this).html("Deselect "+cnt+" additional transactions").removeClass('select_similar_button').addClass('deselect_similar_button');
    });

    $('body').on('click','.cp',function() {
        if ($(this).find('input').length == 0) {
            console.log('yo');
            progenitor = $(this).attr('progenitor');
            txid = $(this).closest('.transaction').attr('id').substr(2);
            matches = lookup_info["counterparty_mapping"][progenitor];
//            count = progenitor_counts[progenitor];
            count = matches.size;
            ac_html = "<form class='ac_wrapper'><input type=text class='cp_enter'><input type=submit class='apply_ac' value='Apply to "+count+" transactions'/><button class='cancel'>Cancel</button><form>";
            cp_name_el = $(this)
            current_name = cp_name_el.text()
            cp_name_el.empty()
            cp_name_el.append(ac_html);
//            cp_name_el.find('.autocomplete_list').autocomplete({source: counterparty_list});
//            cp_name_el.find('.autocomplete_list').focus()

//            $(document).on('keyup', function(e) {
//              if (e.key == "Escape") $('.cancel').click();
//            });

            cp_name_el.find('.cancel').on('click',function(e) {
                e.preventDefault();
                cp_name_el.text(current_name);
                cp_name_el.find('form').remove();
                $(document).off('keyup');
                e.stopPropagation()
            });

            cp_name_el.find('form').submit( function(e) {
                e.preventDefault();
                cp = $(this).find('.cp_enter').val()
                if (cp.length > 0) {
//                    $('.prog_'+progenitor).html(cp).css({'background-color':getColor()});
                    address = window.sessionStorage.getItem('address');
                    chain = window.sessionStorage.getItem('chain');
                    $.get("update_progenitors?chain="+chain+"&user="+address+"&progenitor="+progenitor+"&counterparty="+cp, function(js) {
                        var data = JSON.parse(js);
                        console.log(data);
                        if (data.hasOwnProperty('error')) {
                            $(this).append("<div class='err_mes'>"+data['error']+"</div>");
                        } else {
                            console.log("match len",matches.size,"prog",progenitor);
                            for (o_txid of matches) {

                                console.log("o_txid",o_txid);
                                transaction = all_transactions[o_txid];
                                transaction_counterparties = transaction['counter_parties'];
                                console.log(transaction_counterparties)
                                for (let o_progenitor in transaction_counterparties) {
                                    if (o_progenitor == progenitor)
                                        transaction_counterparties[progenitor][0] = cp;
                                }
                                console.log("remove_from_mapping",current_name,o_txid);
                                remove_from_mapping('counterparty_name',current_name,o_txid);
                                console.log("add_to_mapping",cp,o_txid);
                                add_to_mapping('counterparty_name',cp,o_txid);
                                $('#t_'+o_txid).find('.tx_row_2').replaceWith(display_counterparty(transaction,o_txid == txid));
                                if (o_txid != txid)
                                    $('#t_'+o_txid).find('.select_similar').remove();
                            }
                            find_similar_transactions(txid);
                        }

                    });
                }
                $(this).remove();
//                $(document).off('keyup');
            });
        }
    });

    $('body').on('change','.treatment, .row_rate, .row_vaultid',function() {
        el = $(this);
        td = $(this).closest('td')
        td.addClass('custom');
        txel = $(this).closest('.transaction');
        t_id = txel.attr('id');
        txid = t_id.substr(2);
        this_row = $(this).closest('tr');
        tr_idx = this_row.attr('index');
        val = $(this).val();

        if ($(this).hasClass('treatment')) {
            console.log("update treatment, transaction",txid,'transfer index',tr_idx,'val',val);
            data = 'transaction='+txid+"&transfer_idx="+tr_idx+"&custom_treatment="+val;
            prop = 'treatment'
        } else if ($(this).hasClass('row_rate')) {
            console.log("update rate, transaction",txid,'transfer index',tr_idx,'val',val);
            data = 'transaction='+txid+"&transfer_idx="+tr_idx+"&custom_rate="+val;
            prop = 'rate'
        } else if ($(this).hasClass('row_vaultid')) {
            console.log("update vault id, transaction",txid,'transfer index',tr_idx,'val',val);
            data = 'transaction='+txid+"&transfer_idx="+tr_idx+"&custom_vaultid="+val;
            prop = 'vault_id'
        }

         $.post("save_custom_val?chain="+chain+"&address="+addr, data, function(resp) {
            console.log(resp);
            var data = JSON.parse(resp);
            if (data.hasOwnProperty('error')) {
                td.append("<div class='err_mes'>"+data['error']+"</div>");
            } else {
                set_transfer_val(txid, tr_idx, prop, "custom:"+val);
                need_recalc();
                showib(txel.find('.undo_changes'));
            }
        });


    });

    $('body').on('click','.undo_changes',function() {
        el = $(this);
        txel = $(this).closest('.transaction');
        t_id = txel.attr('id');
        txid = t_id.substr(2);
        console.log("undo changes",txid);
        data = 'transaction='+txid;
        $.post("undo_custom_changes?chain="+chain+"&address="+addr, data, function(resp) {
            console.log(resp);
            var data = JSON.parse(resp);
            if (data.hasOwnProperty('error')) {
                txel.append("<div class='err_mes'>"+data['error']+"</div>");
            } else {
                show_ajax_transactions(data)
                need_recalc();
            }
        });
    });

}

function set_intersect(set1,set2) {
    if (set1 == null)
        return set2;
    return new Set([...set1].filter(x => set2.has(x)));
}

function set_union(setA, setB) {
    let _union = new Set(setA)
    for (let elem of setB) {
        _union.add(elem)
    }
    return _union
}

function find_similar_transactions(txid) {
    console.log("txid "+txid);
    jqid = '#t_'+txid;
    tfst1 = performance.now();

//    all = [...Array(lookup_info['last_index']).keys()];
    all = null;
    $(jqid).find('input:checked').each(function() {
        tin1 = performance.now();
        lookup = $(this).attr('class').substr(4);
        if (lookup != 'token') {
            lookup_vals = lookup_info['transactions'][txid][lookup];

            if (lookup_vals.size == 0) {
                all = new Set();
                $(jqid).find('.current_sims').val('');
                return;
            }
        } else
            lookup_vals = [$(this).attr('token_address')];
        console.log('lookup',lookup,'vals',lookup_vals);

        single_lookup_set = new Set();
        for (let lookup_val of lookup_vals) {
//            all = set_intersect(all,lookup_info[lookup+"_mapping"][lookup_val]);
//            console.log('lookup',lookup,'lookup_val',lookup_val,lookup_info[lookup+"_mapping"][lookup_val].size,all.size)
            single_lookup_set = set_union(single_lookup_set,lookup_info[lookup+"_mapping"][lookup_val])
            console.log('lookup',lookup,'lookup_val',lookup_val,lookup_info[lookup+"_mapping"][lookup_val].size)
        }
        all = set_intersect(all,single_lookup_set);
        tin2 = performance.now();
        console.log('fst inner',(tin2-tin1));
    });
    tfst2 = performance.now();
    if (all == null) {
        hide($(jqid).find('.select_similar_button'));
        $(jqid).find('.current_sims').val('');
        return all
    } else showib($(jqid).find('.select_similar_button'));

    cnt = all.size - 1;
    console.log("find_similar_transactions",jqid,cnt);
    if (cnt == 0) {
        $(jqid).find('.select_similar_button').addClass('grayed').html('There are no matching transactions');
    } else {
        $(jqid).find('.select_similar_button').removeClass('grayed').html('Select '+cnt+' additional transactions');
    }
    tfst3 = performance.now();

    str = Array.from(all).join(',');
    tfst4 = performance.now();
    $(jqid).find('.current_sims').val(str);
    tfst5 = performance.now();
    console.log('fst all',tfst2-tfst1,tfst3-tfst2,tfst4-tfst3,tfst5-tfst4);
    return all
}

function deselect_primary(and_secondary=false) {
    if (prev_selection != null) {
        jqid = '#t_'+prev_selection;
        $(jqid).removeClass('primary_selected');
        if (and_secondary)
            $(jqid).removeClass('secondary_selected');
        $(jqid).find('.tx_row_2').replaceWith(display_counterparty(all_transactions[prev_selection]));
        $(jqid).find('.transfers').replaceWith(display_transfers(all_transactions[prev_selection]));
        hide($(jqid).find('.select_similar'));
        hide($(jqid).find('.save_changes'));
        prev_selection = null;
    }
}

function selection_operations(builtin_types,custom_types) {
    var html = "<div id='operations_block'>";
    html += "<div id='selections_placeholder'>Nothing selected. Click a transaction to select it. CTRL+click to select multiple.</div>";

    html += "<div id='scroll_block'><div class='header'>Scroll to:</div>";
    html += "<div class='scroll_row'>Top <a id='scr_top' class='prev_ic'></a></div>";
    html += "<div class='scroll_row'>Selected <a id='scr_selected_next' class='next_ic'></a><a id='scr_selected_prev' class='prev_ic'></a></div>";
//    html += "<div class='scroll_row'>Unknown <a id='scr_unknown_next'>Next</a><a id='scr_unknown_prev'>Previous</a></div>";
    html += "<div class='scroll_row'>Red <a id='scr_red_next' class='next_ic'></a><a id='scr_red_prev' class='prev_ic'></a></div>";
    html += "<div class='scroll_row'>Red or orange <a id='scr_orange_next' class='next_ic'></a><a id='scr_orange_prev' class='prev_ic'></a></div>";
    html += "<div class='scroll_row'>Red, orange, or yellow <a id='scr_yellow_next' class='next_ic'></a><a id='scr_yellow_prev' class='prev_ic'></a></div>";
    html += "</div>";

    html += "<div id='recolor_block'><div class='header'>Recolor selected transactions:</div>";
    html += "<div id='color_options'><div class='colopt t_class_10' id='colopt_10'></div><div class='colopt t_class_5' id='colopt_5'></div><div class='colopt t_class_3' id='colopt_3'></div><div class='colopt t_class_0' id='colopt_0'></div><div id='color_undo' title='Undo custom recoloring'></div></div>";
    html += "</div>";

    html += "<div id='selections_block'>";
    html += "<div id='selections_count'><span>0</span> transactions selected <a id='deselect_all'>Deselect all transactions</a></div>";
    html += "<div id='show_block'><div class='header'>Show:</div>";
    html += "<a id='sel_opt_all' class='sel_opt sel_opt_chosen'>All</a>";
    html += "<a id='sel_opt_sel' class='sel_opt'>Selected</a>";
    html += "<a id='sel_opt_desel' class='sel_opt'>Deselected</a>";
    html += "</div></div>";

    html += "<div id='types_block'>";
    html += show_custom_types(custom_types);
    html += "<a id='types_create'>Create new custom type</a>";
    html += "<div id='types_list'></div>";
    address = window.sessionStorage.getItem('address');
    html +="<a href='download?address="+address+"&type=transactions_json' id='download_transactions_json'>Download all transactions (json)</a>";
    html += "</div>";

    html +="</div>";
//    $('body').append(html);
    $('#content').append(html);

    $('#deselect_all').on('click',function() {
        deselect_primary();
        $('.secondary_selected').removeClass('secondary_selected');
        update_selections_block();
    });
    update_selections_block();

    $('#scroll_block a').on('click', function() {
        which = $(this).attr('id');

        var bottom_of_screen = $(window).scrollTop() + window.innerHeight;
        var top_of_screen = $(window).scrollTop();
        if (which.includes('top')) {
            window.scrollTo(0,0);
            return;
        }


        var mid_screen = (top_of_screen+bottom_of_screen)/2;
        console.log('scroll',which, top_of_screen, bottom_of_screen);

        if (which.includes('selected')) collection = $('.secondary_selected');
        if (which.includes('unknown')) collection = $('.t_class_unknown');
//        if (which.includes('red')) collection = $('.t_class_0:not(.custom_recolor) .custom_recolor_0');
//        if (which.includes('orange')) collection = $('.t_class_0,.t_class_3');
//        if (which.includes('yellow')) collection = $('.t_class_0,.t_class_3,.t_class_5');

        if (which.includes('red')) collection = $('.t_class_0:not(.custom_recolor),.custom_recolor_0');
        if (which.includes('orange')) collection = $('.t_class_0:not(.custom_recolor),.t_class_3:not(.custom_recolor),.custom_recolor_0,.custom_recolor_3');
        if (which.includes('yellow')) collection = $('.t_class_0:not(.custom_recolor),.t_class_3:not(.custom_recolor),.t_class_5:not(.custom_recolor),.custom_recolor_0,.custom_recolor_5,.custom_recolor_5');

        next = true;
        if (which.includes('_prev')) {
            next = false;
            jQuery.fn.reverse = [].reverse;
            collection = $(collection).reverse();
        }


        $(collection).each(function() {
            el = $(this)

            var top_of_element = el.offset().top;
            var bottom_of_element = el.offset().top + el.outerHeight();
            console.log('col',$(this).attr('id'),top_of_element,bottom_of_element);

            if (next) {
                if (top_of_element > mid_screen+20) {
                    console.log('scroll down to ',$(el).attr('id'));
                    scroll_to(el);
                    return false;
                }
            } else {
                if (top_of_element < top_of_screen) {
                    console.log('scroll up to ',$(el).attr('id'));
                    scroll_to(el);
                    return false;
                }
            }
        });


    });

    $('#sel_opt_all').on('click',function() {
        if ($('#sel_opt_all').hasClass('sel_opt_chosen'))
            return;
//        $(document.body).css({'cursor' : 'wait'});
        $('.sel_opt_chosen').removeClass('sel_opt_chosen');
        $(this).addClass('sel_opt_chosen');
        showib($('.transaction'));
        if (scroll_position != null)
            window.scrollTo(0,scroll_position);
//        $(document.body).css({'cursor' : 'default'});
    });

    $('#sel_opt_sel').on('click',function() {
        if ($('#sel_opt_all').hasClass('sel_opt_chosen'))
            scroll_position = window.scrollY;
//        $(document.body).css({'cursor' : 'wait'});
        $('.sel_opt_chosen').removeClass('sel_opt_chosen');
        $(this).addClass('sel_opt_chosen');
        showib($('.secondary_selected'));
        hide($('.transaction:not(.secondary_selected)'));

//        $(document.body).css({'cursor' : 'default'});
    });

    $('#sel_opt_desel').on('click',function() {
//        $(document.body).css({'cursor' : 'wait'});
        $('.sel_opt_chosen').removeClass('sel_opt_chosen');
        $(this).addClass('sel_opt_chosen');
        hide($('.secondary_selected'));
        showib($('.transaction:not(.secondary_selected)'));
//        $(document.body).css({'cursor' : 'default'});
    });

}

function update_selections_block() {
    cnt = $('#transaction_list').find('div.secondary_selected').length;
//    cnt = $('.secondary_selected').length;
    if (cnt > 0) {
        $('#selections_count').children('span').html(cnt);
        show($('#selections_count'));
        hide($('#selections_placeholder'));
        show($('#scr_selected_next').closest('.scroll_row'));
        $('#custom_types_list .ct_name').addClass('applicable').attr('title','Apply to selected transactions');
    } else {
        hide($('#selections_count'));
        show($('#selections_placeholder'));
        hide($('#scr_selected_next').closest('.scroll_row'));
        $('#custom_types_list .ct_name').removeClass('applicable').removeAttr('title');
    }
}


function scroll_to(el) {
    var elem_position = el.offset().top;
    var elem_height = el.outerHeight();
    var window_height = window.innerHeight;
    var y = elem_position - window_height/2+elem_height/2;
    window.scrollTo(0,y);

}

function show(el) {
    el.css({'display':'block'});
}

function showib(el) {
    el.css({'display':'inline-block'});
}

function hide(el) {
    el.css({'display':'none'});
}



$('body').on('click','.colopt',function() {
   color_id = $(this).attr('id').substr(7);
   txids = [];
   transactions = $('div.secondary_selected');
   if (transactions.length == 0)
        return;
   transactions.each(function() {
        txid = $(this).attr('id').substr(2);
        txids.push(txid)
   });
   console.log("recolor",color_id,"to transactions",txids);
   data = 'color_id='+color_id+'&transactions='+txids.join(',');
   $.post("recolor?chain="+chain+"&address="+addr, data, function(resp) {
        console.log(resp);
        var data = JSON.parse(resp);
        if (data.hasOwnProperty('error')) {
            $('#recolor_block').append("<div class='err_mes'>"+data['error']+"</div>");
        } else {
            transactions.removeClass('custom_recolor_0 custom_recolor_3 custom_recolor_5 custom_recolor_10').addClass('custom_recolor custom_recolor_'+color_id);
        }
    });
});

$('body').on('click','#color_undo',function() {
   txids = [];
   transactions = $('div.secondary_selected');
   if (transactions.length == 0)
        return;
   transactions.each(function() {
        txid = $(this).attr('id').substr(2);
        txids.push(txid)
   });
   console.log("recolor",'undo',"to transactions",txids);
   data = 'color_id=undo&transactions='+txids.join(',');
   $.post("recolor?chain="+chain+"&address="+addr, data, function(resp) {
        console.log(resp);
        var data = JSON.parse(resp);
        if (data.hasOwnProperty('error')) {
            $('#recolor_block').append("<div class='err_mes'>"+data['error']+"</div>");
        } else {
            transactions.removeClass('custom_recolor custom_recolor_0 custom_recolor_3 custom_recolor_5 custom_recolor_10');
        }
    });
});


$('body').on('change','select.treatment',function() {
    let selected_treatment = $(this).val();
    let vault_id_el = $(this).closest('tr').find('.r_vaultid').find('span');
    if (['repay','deposit','borrow','withdraw','exit','liquidation','full_repay'].includes(selected_treatment))
        vault_id_el.removeClass('hidden')
    else
        vault_id_el.addClass('hidden')
});