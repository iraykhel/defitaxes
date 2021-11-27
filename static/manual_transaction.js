$('body').on('click','#mt_create',function() {
    create_edit_custom_transaction(null);
});

$('body').on('click','.mt_edit',function() {
    let txid = $(this).closest('.transaction').attr('id').substr(2);
    console.log('edit tx',txid);
    create_edit_custom_transaction(txid);
});

$('body').on('click','#mt_r_add',function() {
    html = make_mt_transfer_html(null);
    $('#mt_r_add').before(html);
    $('.mt_what').autocomplete({  source: ac_token_list });
});

$('body').on('click','.mt_r_rem div',function() {
    $(this).closest('div.mt_transfer').remove();
});

$('body').on('click','#mt_cancel',function() {
    $('#mt').remove();
    $('.transaction').removeClass('shifted');
});

$('body').on('click','#mt_save',function() {
    $('.err_mes').remove();

    let err = null;
    let dt = $('#mt_date').val();
    console.log('dt',dt)
    if (dt.length == 0) {
        err = "Please enter date"
        $('#mt_date').css({'background-color':'#FF9E9E'});
    }

    if ($('#mt div.mt_transfer').length == 0) {
        err = "Please enter at least one transfer"
    }

    if (err != null) {
        $("#mt .sim_buttons").before("<div class='err_mes'>"+err+"</div>");
    } else {
        data = $('#mt_form').serialize();
        console.log(addr,data);
        $.post("save_manual_transaction?address="+addr+"&chain="+chain, data, function(resp) {
            console.log(resp);
            var data = JSON.parse(resp);
            if (data.hasOwnProperty('error')) {
                console.log("ERROR",data['error']);
                $("#mt .sim_buttons").before("<div class='err_mes'>"+data['error']+"</div>");
            } else {
                let transaction = data['transactions'][0];


                let txid = transaction['txid'];
                if (txid in all_transactions) {
                    show_ajax_transactions(data);
                } else {
                    let ts = transaction['ts'];
                    let len = Object.keys(all_transactions).length;
                    let min_num = len+1;
                    let insert_before_txid = null;
                    for (let txid_loop in all_transactions) {
                        let o_tx = all_transactions[txid_loop];
                        let ts_loop = o_tx['ts'];
                        let num_loop = o_tx['num'];
                        if (ts_loop > ts) {
                            o_tx['num'] = num_loop + 1;
                            $('#t_'+txid_loop).find('.t_num').text('#'+o_tx['num']);

                            if (num_loop < min_num) {
                                min_num = num_loop;
                                insert_before_txid = txid_loop;
                            }
                        }

                    }
                    $('.len').text(len+1);
    //                transaction['num'] = min_num;
                    all_transactions[transaction['txid']] = transaction;
                    map_lookups(transaction);
                    transaction_html = make_transaction_html(transaction,idx=min_num-1, len=len+1);

                    $('#sel_opt_all').click();

                    if (insert_before_txid == null)
                        $('#transaction_list').append(transaction_html)
                    else
                        $('#t_'+insert_before_txid).before(transaction_html);

                    console.log("Found num",transaction['num'],'insert_before_txid',insert_before_txid);
                }


                $('#mt').remove();
                $('.transaction').removeClass('shifted');
                scroll_to($('#t_'+txid));
                select_transaction($('#t_'+txid),keep_secondary=false);
                need_recalc();
            }
        });
    }
});

$('body').on('click','.mt_delete_popup',function() {
    let txel = $(this).closest('.transaction');
    let txid = txel.attr('id').substr(2);
    delete_manual_transaction_popup(txid);
});

$('body').on('click','#mt_delete',function() {
    data = $('#mt_delete_form').serialize();

    console.log('del trans',addr,data);

    $.post("delete_manual_transaction?address="+addr+"&chain="+chain, data, function(resp) {
        console.log(resp);
        var data = JSON.parse(resp);
        if (data.hasOwnProperty('error')) {
            $("#mt_delete_form").after("<div class='err_mes'>"+data['error']+"</div>");
        } else {
            let txid = $('#txid_to_delete').val();
            let transaction = all_transactions[txid];
            let ts = transaction['ts'];


            let len = Object.keys(all_transactions).length;
            for (let txid_loop in all_transactions) {
                let o_tx = all_transactions[txid_loop];
                let ts_loop = o_tx['ts'];
                if (ts_loop > ts) {
                    o_tx['num'] = o_tx['num'] - 1;
                    $('#t_'+txid_loop).find('.t_num').text('#'+o_tx['num']);
                }
            }

            $('.len').text(len-1);

            map_lookups(all_transactions[txid], unmap_instead=true)

            delete all_transactions[txid];
            $('#t_'+txid).remove();
            prev_selection = null;
            $('#popup').remove();
            $('#overlay').remove();
            need_recalc();
        }
    });
});

$('body').on('click','#mt_delete_cancel',function() {
    $('#popup').remove();
    $('#overlay').remove();
});

function delete_manual_transaction_popup(id) {
    html ="<div id='overlay'></div><div id='popup'><form id='mt_delete_form'><input type=hidden name=txid id=txid_to_delete value="+id+"> ";
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

    ac_token_list = prep_ac_token_list();
//    console.log(ac_token_list);

    html = "<div id='mt'><form id='mt_form'>";
    let mt_date = "";
    let mt_time = " value='12:00:00'";
    let mt_hash = "";
    let mt_op = "";
    let transfer_list = [null]
    if (id == null) {
        html += "<div class='header'>Add a new transaction<div class='help help_mt'></div></div>";

     } else {
        html += "<input type=hidden name=mt_txid id=mt_txid value="+id+"><div class='header'>Edit transaction<div class='help help_mt'></div></div>";
        let transaction = all_transactions[id];
        let ts = transaction['ts'];

        let dateobj = new Date(ts * 1000);
        mt_date = " value='"+dateobj.toLocaleDateString('en-US',{timeZone:'UTC',month:'2-digit',day:'2-digit',year:'numeric'})+"'"
        mt_time = " value='"+dateobj.toLocaleTimeString('en-GB',{timeZone:'UTC',hour:'2-digit',minute:'2-digit',second:'2-digit'})+"'"
        if (transaction['hash'] != null)
            mt_hash = " value='"+transaction['hash']+"'"

        let rows = transaction['rows'];
        let op = rows[0]['input'];
        if (op != null)
            mt_op = " value='"+op.substr(7)+"'"

        console.log('edit','ts',ts,mt_date,mt_time)
        transfer_list = transaction['rows'];


     }
    html += "<div class='mt_row'><div class='mt_field'><div class='mt_field_header'>Date:</div><input type=text id='mt_date' name='mt_date' placeholder='required'"+mt_date+"></div>";
    html += "<div class='mt_field'><div class='mt_field_header'>Time:</div><input type=text id='mt_time' name='mt_time' placeholder='optional'"+mt_time+"></div></div>";
    html += "<div class='mt_row'><div class='mt_field'><div class='mt_field_header'>TX hash:</div><input type=text id='mt_hash' name='mt_hash' placeholder='optional'"+mt_hash+"></div></div>";
    html += "<div class='mt_row'><div class='mt_field'><div class='mt_field_header'>Operation:</div><input type=text id='mt_op' name='mt_op' placeholder='optional'"+mt_op+"></div>";
//    html += "<div class='mt_field'><div class='mt_field_header'>Counterparty:</div><input type=text id='mt_cp' name='mt_cp' placeholder='optional'></div></div>";


    html += "<div id='mt_transfers' class='transfers'>";
    for (let transfer of transfer_list)
        html += make_mt_transfer_html(transfer)
//    html += make_mt_transfer_html();
    html+="<div class='mt_r_add' id='mt_r_add'><div></div>Add a transfer</div>";
    html += "</div>";


    html += "<div class='sim_buttons'>";
    html += "<div id='mt_save'>Save transaction</div>";
    html += "<div id='mt_cancel'>Cancel</div></div>";
    html += "</form></div>";
    $('#content').append(html);
    $('#mt_date').datepicker();
    $('#mt_time').timepicker({ timeFormat: 'HH:mm:ss', interval:60, dropdown:false });
    $('.mt_what').autocomplete({  source: ac_token_list });
}

function prep_ac_token_list() {
    let ac_list = [];
    for (let symbol in all_symbols) {
        for (let address of all_symbols[symbol]) {
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
    }

    let html = "<div class='mt_transfer'>";
    html += "<input class='mt_from' name='mt_from' type=text placeholder='From address'"+mt_from+">";
    html += "<span class='mt_r_arrow'><div></div></span>";
    html += "<input class='mt_to' name='mt_to' type=text placeholder='To address'"+mt_to+">";
    html += "<input class='mt_what' name='mt_what' type=text placeholder='Token name or address'"+mt_what+">";
    html += "<input class='mt_amount' name='mt_amount' type=text placeholder='Amount'"+mt_amount+">";
    html += "<input class='mt_nft_id' name='mt_nft_id' type=text placeholder='NFT ID'"+mt_nft_id+">";
    html += "<span class='mt_r_rem' title='Delete transfer'><div></div></span>";
    html += "</div>";
    return html;
}

