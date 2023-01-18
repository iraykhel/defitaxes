$.validator.addMethod('atleastonetransfer', function(val,el) {
        if ($('#mt_form').find('.mt_transfer').length == 0)
            return false
        return true
    },'You need at least one transfer')

$.validator.addMethod('useraddresspresent', function(val,el) {
        let fail = false
        $('#mt_form').find('.mt_transfer').each(function() {
            let trel = $(this)
            if (!displayed_addresses.includes(normalize_address(trel.find('.mt_to').val())) && !displayed_addresses.includes(normalize_address(trel.find('.mt_from').val()))) {
                fail = true
                return
            }
        });
        return !fail
    },'Each transfer be from or to your address')

$.validator.addMethod('mmddyyyy', function(value, element) {
    let re = new RegExp("^((0?[1-9]|1[012])[- /.](0?[1-9]|[12][0-9]|3[01])[- /.](19|20)?[0-9]{2})*$")
    return this.optional(element) || re.test(value);
},
"Needs to be MM/DD/YYYY")

$('body').on('click','#mt_create',function() {
    create_edit_custom_transaction(null);
});

$('body').on('click','.mt_edit',function() {
    let txid = parseInt($(this).closest('.transaction').attr('id').substr(2));
//    console.log('edit tx',txid);
    create_edit_custom_transaction(txid);
});

$('body').on('click','#mt_r_add',function() {
    html = make_mt_transfer_html(null);
    $('#mt_r_add').before(html);
    set_ac_addr();
    $('.mt_what').autocomplete({  source: ac_token_list });

//    resetFormValidator();
});

$('body').on('click','.mt_r_rem div',function() {
    $(this).closest('div.mt_transfer').remove();
});

$('body').on('click','#mt_cancel',function() {
    $('#mt').remove();
    $('.transaction').removeClass('shifted');
});


function post_manual_transactions(chain,data,followup=null) {
//    $.post("save_manual_transaction?address="+primary+"&chain="+chain, data, function(resp) {
//            console.log(resp);
    $.ajax({
        method: 'post',
        processData: false,
        contentType: false,
        cache: false,
        data: data,
        enctype: 'multipart/form-data',
        url: "save_manual_transaction?address="+primary+"&chain="+chain,
        success: function (resp) {

            var data = JSON.parse(resp);
            if (data.hasOwnProperty('error')) {
                $(".sim_buttons").before("<div class='err_mes'>"+rv['error']+"</div>");
            } else {
                var new_txids = []
                for (transaction of data['transactions']) {
    //            let transaction = data['transactions'];


                    let txid = parseInt(transaction['txid']);
                    new_txids.push(txid)
                    if (txid in all_transactions) {
                        delete_transaction(txid);
                    }

                    {
                        let ts = transaction['ts'];
                        let idx = 0
                        insert_idx = -1
                        for (idx in transaction_order) {
                            txid_loop = transaction_order[idx];
                            let o_tx = all_transactions[txid_loop];
                            let ts_loop = o_tx['ts'];
                            if (ts_loop > ts) {
                                if (insert_idx == -1) {
                                    insert_idx = parseInt(idx)
        //                                console.log("splicing at idx",idx)
                                }
                                o_tx['num'] = o_tx['num'] + 1;

                            }
                        }


                        all_transactions[txid] = transaction;
                        if (insert_idx == -1) {
                            transaction_order.push(txid)
                            transaction['num'] = transaction_order.length;
                        } else {
                            transaction['num'] = insert_idx+1;
                            transaction_order.splice(insert_idx,0,txid)
                        }

                        map_lookups(transaction);

                        all_transactions[txid] = transaction;

                    }
                }


                $('#mt').remove();
                $('.transaction').removeClass('shifted');
                make_pagination();
    //            go_to_transaction(txid)
    //            select_transaction($('#t_'+txid),keep_secondary=false);
                need_recalc();
//                console.log('new_txids',new_txids)
                if (followup != null)
                    followup(new_txids);
//                return {'error':null,'txids':txids}
            }
        }
    });
}

$('body').on('click','#mt_save',function() {
    $('.err_mes').remove();

    //jquery validation plugin is an utter piece of shit.
    $('.mt_from,.mt_to,.mt_what').each(function() {
        $(this).rules("add", {
            required:true,
            messages: {
                required:'required'
            }
        });
    });

    $('.mt_amount').each(function() {
        $(this).rules("add", {
            required:true,
            number:true,
            messages: {
                required:'required',
                number:'must be a number'
            }
        });
    });

    $('.mt_nft_id').each(function() {
        $(this).rules("add", {
            digits:true,
            messages: {
                digits:'must be an integer'
            }
        });
    });

    let is_valid = $('#mt_form').valid();

    if (is_valid) {
        $('#mt_form').append("<input type=hidden name=mt0_tr_disp_idx value="+tr_disp_idx+">");
        let dt = $('#mt_date').val()
        let tm = $('#mt_time').val()
        let dt_ar = dt.split("/")
        let tm_ar = tm.split(":")
        let date = new Date(Date.UTC(dt_ar[2],dt_ar[0]-1,dt_ar[1], tm_ar[0],tm_ar[1],tm_ar[2]));
        let ts = date.getTime()/1000;

//        let ts2 = Date.parse(dt+" "+tm+" GMT")/1000
//        console.log('converted timestamps',dt_ar,tm_ar,ts,ts2)

//        data = $('#mt_form').serialize();
//        data += "&mt0_ts="+ts
         let form_data = new FormData(document.querySelector('#mt_form'))
         form_data.append("mt0_ts",ts)
//        console.log(addr,data);
        let chain = $('#mt_chain').val();

        function followup(new_txids) {
            let txid = new_txids[0]
//            console.log('new txid',txid)
            go_to_transaction(txid)
            select_transaction($('#t_'+txid),keep_secondary=false);
        }
        rv = post_manual_transactions(chain,form_data,followup)

    }
});



$('body').on('click','.mt_delete_popup',function() {
    let txel = $(this).closest('.transaction');
    let txid = parseInt(txel.attr('id').substr(2));
    delete_manual_transaction_popup(txid);
});

function delete_transaction(txid) {
    let idx = transaction_order.indexOf(parseInt(txid))
    for (let l_idx = idx+1; l_idx < transaction_order.length; l_idx++) {
        let txid_loop = transaction_order[l_idx];
        let o_tx = all_transactions[txid_loop];
        o_tx['num'] = o_tx['num'] - 1;
    }

    map_lookups(all_transactions[txid], unmap_instead=true)

    delete all_transactions[txid];
//    console.log("delete tx",txid,"at idx",transaction_order.indexOf(parseInt(txid)),transaction_order.length)
    transaction_order.splice(idx,1)

//    $('#t_'+txid).remove();
}

$('body').on('click','#mt_delete',function() {
    data = $('#mt_delete_form').serialize();

//    console.log('del trans',addr,data);

    $.post("delete_manual_transaction?address="+primary, data, function(resp) {
//        console.log(resp);
        var data = JSON.parse(resp);
        if (data.hasOwnProperty('error')) {
            $("#mt_delete_form").after("<div class='err_mes'>"+data['error']+"</div>");
        } else {
            let txid = $('#txid_to_delete').val();
            delete_transaction(txid)
            prev_selection = null;
            $('#popup').remove();
            $('#overlay').remove();
            need_recalc();
//            console.log("delete - go to page",current_page_idx)
            make_pagination(go_to_page=current_page_idx);

//            $('.pagination_list').pagination('go',current_page_idx+1)
        }
    });
});

$('body').on('click','#mt_delete_cancel',function() {
    $('#popup').remove();
    $('#overlay').remove();
});

function delete_manual_transaction_popup(id) {
    html ="<div id='overlay'></div><div id='popup' class='popup'><form id='mt_delete_form'><input type=hidden name=txid id=txid_to_delete value="+id+"> ";
    html += "Really delete this transaction?";
    html += "<div class='sim_buttons'>";
    html += "<div id='mt_delete'>Delete transaction</div>";
    html += "<div id='mt_delete_cancel'>Cancel</div></div>";
    html += "</form></div>";
    $('#content').append(html);
}


function create_edit_custom_transaction(id) {
    $('#mt').remove();

    $('.transaction').addClass('shifted');

//    console.log(ac_token_list);

    html = "<div id='mt'><form id='mt_form'>";
    let mt_date = "";
    let mt_time = " value='12:00:00'";
    let mt_hash = "";
    let mt_op = "";
    let transfer_list = [null]
    let chain_name = 'ETH';
    if (id == null) {

        html += "<div class='header'>Add a new transaction<div class='help help_mt'></div></div>";

     } else {
        html += "<input type=hidden name=mt0_txid id=mt_txid value="+id+"><div class='header'>Edit transaction<div class='help help_mt'></div></div>";
        let transaction = all_transactions[id];
        chain_name = transaction['chain'];

        let ts = transaction['ts'];

        let dateobj = new Date(ts * 1000);
        mt_date = " value='"+dateobj.toLocaleDateString('en-US',{timeZone:'UTC',month:'2-digit',day:'2-digit',year:'numeric'})+"'"
        mt_time = " value='"+dateobj.toLocaleTimeString('en-GB',{timeZone:'UTC',hour:'2-digit',minute:'2-digit',second:'2-digit'})+"'"
        if (transaction['hash'] != null)
            mt_hash = " value='"+transaction['hash']+"'"

        let rows = transaction['rows'];
//        let op = rows[0]['input'];
        for (let transfer_id in rows) {
            let op = rows[transfer_id]['input']
            if (op != null) {
                mt_op = " value='"+op.substr(7)+"'"
                break;
            }
        }

//        console.log('edit','ts',ts,mt_date,mt_time)
        transfer_list = transaction['rows'];


     }
    html += "<div class='mt_row'><div class='mt_field'><div class='mt_field_header'>Date:</div><input type=text id='mt_date' name='mt0_date' placeholder='required'"+mt_date+"></div>";
    html += "<div class='mt_field'><div class='mt_field_header'>Time:</div><input type=text id='mt_time' required name='mt0_time' placeholder='optional'"+mt_time+"></div></div>";


    html += "<div class='mt_row'><div class='mt_field'><div class='mt_field_header'>Chain:</div><select id='mt_chain' name='mt0_chain'>"
    for (let chain_option of ordered_chains) {
        html += "<option value='"+chain_option+"'"
//        console.log(id, chain_option,chain_name)
        if (chain_option == chain_name)
            html += " selected "
        html +=">"+chain_option+"</option>";
    }
    html += "</select></div>";
    html += "<div class='mt_field'><div class='mt_field_header'>TX hash:</div><input type=text id='mt_hash' name='mt0_hash' placeholder='optional'"+mt_hash+"></div></div>";
    html += "<div class='mt_row'><div class='mt_field'><div class='mt_field_header'>Operation:</div><input type=text id='mt_op' name='mt0_op' placeholder='optional'"+mt_op+"></div>";
//    html += "<div class='mt_field'><div class='mt_field_header'>Counterparty:</div><input type=text id='mt_cp' name='mt_cp' placeholder='optional'></div></div>";


    html += "<div id='mt_transfers' class='transfers'>";
    tr_disp_idx = 0;
    for (let trid in transfer_list) {
        let transfer = transfer_list[trid];
        html += make_mt_transfer_html(transfer)
    }
//    html += make_mt_transfer_html();
    html+="<div class='mt_r_add' id='mt_r_add'><div></div>Add a transfer</div>";
    html += "</div>";
    html += "<input type=hidden name=validation_hook>";


    html += "<div class='sim_buttons'>";
    html += "<div id='mt_save'>Save transaction</div>";
    html += "<div id='mt_cancel'>Cancel</div></div>";
    html += "</form></div>";
    ac_token_list = prep_ac_token_list(chain_name);

    $('#content').append(html);
    $('#mt_date').datepicker();
    $('#mt_time').timepicker({ timeFormat: 'HH:mm:ss', interval:60, dropdown:false });
    $('.mt_what').autocomplete({  source: ac_token_list });
    set_ac_addr();


    $('#mt_form').validate({
        ignore:[],
        messages: {

            mt_time:'required',
            mt_date: {
                required:'required'
            }
        },
        rules: {
            mt_date: {
                required:true,
                mmddyyyy:true
            },
            validation_hook: { //this is an ugly hack because jquery validation plugin is a POS
                atleastonetransfer:true,
                useraddresspresent:true
            },
        }
    });
}

function prep_ac_token_list(chain_name) {
    let ac_list = [];
    let chain_symbols = all_symbols[chain_name]
    for (let symbol in chain_symbols) {
        for (let address of chain_symbols[symbol]) {
            if (address == symbol)
                ac_list.push(symbol)
            else
                ac_list.push(symbol+" ("+address+")")
        }
    }
    return ac_list;
}

function make_mt_transfer_html(transfer) {
    let mt_from = '';
    let mt_to = '';
    let mt_what = '';
    let mt_amount = '';
    let mt_nft_id = '';
    let mt_transfer_id = '';



    if (transfer != null) {
        mt_from = " value='"+transfer['fr']+"'";
        mt_to = " value='"+transfer['to']+"'";

        let symbol = transfer['symbol'];
        let what = transfer['what'];
        if (symbol != null && symbol != what)
            mt_what = " value='"+symbol+" ("+what+")'";
        else
            mt_what = " value='"+what+"'";
        mt_amount = " value='"+transfer['amount']+"'";
        if (transfer['token_nft_id'] != null)
            mt_nft_id = " value='"+transfer['token_nft_id']+"'";
        mt_transfer_id = " value='"+transfer['id']+"'";
    } else {
        mt_transfer_id = " value='-1'";
    }

    let html = "<div class='mt_transfer'>";
    html += "<input name='mt0_transfer_id"+tr_disp_idx+"' type=hidden"+mt_transfer_id+">";
    html += "<input class='mt_from' name='mt0_from"+tr_disp_idx+"' type=text placeholder='From address'"+mt_from+">";
    html += "<span class='mt_r_arrow'><div></div></span>";
    html += "<input class='mt_to' name='mt0_to"+tr_disp_idx+"' type=text placeholder='To address'"+mt_to+">";
    html += "<input class='mt_what' name='mt0_what"+tr_disp_idx+"' type=text placeholder='Token name or address'"+mt_what+">";
    html += "<input class='mt_amount' name='mt0_amount"+tr_disp_idx+"' type=text placeholder='Amount'"+mt_amount+">";
    html += "<input class='mt_nft_id' name='mt0_nft_id"+tr_disp_idx+"' type=text placeholder='NFT ID'"+mt_nft_id+">";
    html += "<span class='mt_r_rem' title='Delete transfer'><div></div></span>";
    html += "</div>";
    tr_disp_idx += 1;
    return html;
}

$('body').on('change','#mt_chain',function() {
    ac_token_list = prep_ac_token_list($(this).val());
    $('.mt_what').autocomplete({  source: ac_token_list });
});

function set_ac_addr() {
    let null_addr = "0x0000000000000000000000000000000000000000";
//    let address_list = Array.from(all_addresses);
    let address_list = Object.keys(all_address_info)
    address_list.push(null_addr)
    $('.mt_to, .mt_from').autocomplete({  source: address_list, minLength:0 }).focus(function() {   $(this).autocomplete("search", $(this).val());});
}