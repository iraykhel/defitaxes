mtm = false;




function display_tax_block() {

    var html = "<div id='tax_block'>";
    year_html ="<div id='year_selector'>Tax year:<select id='tax_year'>";
    let current_year = new Date().getFullYear();
    year = 2019;
    while (year != current_year + 1) {
        if (year == current_year-1) {
            selected = " selected ";
        } else {
            selected = "";
        }
        year_html += "<option value="+year+selected+">"+year+"</option>";
        year += 1;
    }
    year_html += "</select></div>\n";
    html += "<div id='matchups_selector'><label>Show gains & losses<input type=checkbox id='matchups_visible' "
    if (params['matchups_visible']) html += " checked "
    html += "></label></div>";
//    html +="<div id='mtm_selector'><label>Mark-to-market <div class='help help_mtm'></div><input type=checkbox id='mtm'></label></div>";
//    html += "<div id='download_transactions_block'><div class='header'>Download all transactions:</div>";
//    html +="<a id='download_transactions_json'>json (raw data)</a>";
//    html +="<a id='download_transactions_csv'>csv (easier to read)</a>";
//    html += "</div>";



//    html += "<a href='download?address="+address+"&type=transactions_csv' id='download_transactions_csv'>csv</a></div>";
    html += "<div id='tax_data'>";

    html += year_html;

//    html +="<a id='download_tax_forms'>Download tax forms</a>";
    html +="<a id='options'>Tax options</a>";
    html +="<a id='downloads'>Download stuff</a>";
    html += "</div>";
    html +="<a id='calc_tax'>Recalculate taxes</a>";


    html += "</div>";



    $('#content').append(html);
//    $('#calc_tax').click(calc_tax);
//    calc_tax();


}

//$('body').on('click','#download_transactions_json, #download_transactions_csv',function() {
////    let address = window.sessionStorage.getItem('address');
////    let chain = window.sessionStorage.getItem('chain');
//    let type = $(this).attr('id').substr(22);
//    window.open("download?type=transactions_"+type+"&address="+primary,'_blank');
////    $.post("download?chain="+chain+"&address="+address+"&type=transactions_json", data, function(resp) {
////        console.log(resp);
////        var data = JSON.parse(resp);
////        if (data.hasOwnProperty('error')) {
////            $('#recolor_block').append("<div class='err_mes'>"+data['error']+"</div>");
////        } else {
////            transactions.removeClass('custom_recolor_0 custom_recolor_3 custom_recolor_5 custom_recolor_10').addClass('custom_recolor custom_recolor_'+color_id);
////        }
////    });
//});

function process_tax_js(js) {
    year = $('#tax_year').val();
//'CA_long':CA_long,'CA_short':CA_short,'CA_errors':CA_errors,'incomes':calculator.incomes,'interest':calculator.interest_payments
    CA_long = js['CA_long'];
    CA_short = js['CA_short'];
    CA_errors = js['CA_errors'];
    tokens = js['tokens'];
    incomes = js['incomes'];
    interest = js['interest'];
    expenses = js['expenses'];
//    console.log('process_tax_js',year);
    show_sums(CA_long, CA_short, incomes, interest, expenses, year);
    reset_impact();
    indicate_matchups(CA_long, CA_short, incomes, interest, expenses);

//    populate_vault_info(js['vault_info']);
}

//run this after make_pagination
function show_tax_related_html() {
    make_all_matchup_html();
    process_tax_errors();
    highlight_impact();
}

function sum_up(lines,year,field='amount',timestamp_field='timestamp') {
    let total = 0;
    for (line of lines) {
        let date = new Date(line[timestamp_field]*1000);
        let line_year = date.getFullYear();
//        console.log(line[timestamp_field],line[timestamp_field]*1000,date);
        if (line_year == year)
            total += line[field];
    }
    return total;
}

function show_sums(CA_long, CA_short, incomes, interest, expenses, year) {
    $('#tax_sums').remove();
    total_gain_long = sum_up(CA_long,year,field='gain',timestamp_field='out_ts');
    total_gain_short = sum_up(CA_short,year,field='gain',timestamp_field='out_ts');
    total_income = sum_up(incomes,year);
    total_interest = sum_up(interest,year);
    total_expenses = sum_up(expenses,year);
    html = "<table id='tax_sums'>";
    if (mtm) {
        html += "<tr><td class='tax_sum_header'>Ordinary income</td><td class='tax_sum_val'>"+print_fiat(round_usd(total_income+total_gain_short))+"</td></tr>";
    } else {
        html += "<tr><td class='tax_sum_header'>Long-term cap gains</td><td class='tax_sum_val'>"+print_fiat(round_usd(total_gain_long))+"</td></tr>";
        html += "<tr><td class='tax_sum_header'>Short-term cap gains</td><td class='tax_sum_val'>"+print_fiat(round_usd(total_gain_short))+"</td></tr>";
        html += "<tr><td class='tax_sum_header'>Ordinary income</td><td class='tax_sum_val'>"+print_fiat(round_usd(total_income))+"</td></tr>";
    }
    html += "<tr><td class='tax_sum_header'>Loan interest paid</td><td class='tax_sum_val'>"+print_fiat(round_usd(total_interest))+"</td></tr>";
    html += "<tr><td class='tax_sum_header'>Business expenses</td><td class='tax_sum_val'>"+print_fiat(round_usd(total_expenses))+"</td></tr>";
    html += "</table>";
    $('#year_selector').after(html);
}


function define_matchups(lines) {


    for (line of lines) {
        let in_txid = line['in_txid'];
        let in_trid = line['in_trid'];

        let out_txid = line['out_txid'];
        let out_trid = line['out_trid'];

        let tok_id = line['token'];

        let gain = line['gain'];

        if (!(in_txid in matchups_basis))
            matchups_basis[in_txid] = {};
        if (!(in_trid in matchups_basis[in_txid]))
            matchups_basis[in_txid][in_trid] = {}
        if (!(tok_id in matchups_basis[in_txid][in_trid]))
            matchups_basis[in_txid][in_trid][tok_id] = {'txids':[],'gain':0}

        matchups_basis[in_txid][in_trid][tok_id]['txids'].push(out_txid);
        matchups_basis[in_txid][in_trid][tok_id]['gain'] += gain;

        if (!(out_txid in matchups_sales))
            matchups_sales[out_txid] = {};
        if (!(out_trid in matchups_sales[out_txid]))
            matchups_sales[out_txid][out_trid] = {}
        if (!(tok_id in matchups_sales[out_txid][out_trid]))
            matchups_sales[out_txid][out_trid][tok_id] = {'txids':[],'gain':0}

        matchups_sales[out_txid][out_trid][tok_id]['txids'].push(in_txid);
        matchups_sales[out_txid][out_trid][tok_id]['gain'] += gain;
    }
}

function define_matchups_ii(lines,target) {
    for (line of lines) {
        let txid = line['txid'];
        let trid = line['trid'];

        if (!(txid in target))
            target[txid] = {};
        if (!(trid in target[txid]))
            target[txid][trid] = {'text':line['text'],'amount':line['amount']}
    }
}

function add_matchup_text(txid,trid,text) {
    if (!(txid in matchup_texts))
        matchup_texts[txid] = {}
    if (!(trid in matchup_texts[txid]))
        matchup_texts[txid][trid] = []
    matchup_texts[txid][trid].push(text);
}

function make_matchup_html(txid,trid,check=true) {
    if (check) {
        if (!(txid in matchup_texts))
            return "";
        if (!(trid in matchup_texts[txid]))
            return "";
    }

    matchup_html = "<tr class='matchup";
    if (!params['matchups_visible'])
        matchup_html += ' hidden';
    matchup_html += "'><td colspan=7>";
    for (matchup_text of matchup_texts[txid][trid]) {
        matchup_html += ("<p>"+matchup_text +"</p>");
    }
    matchup_html += "</td></tr>";
    return matchup_html
}

function make_all_matchup_html() {
    $('.matchup').remove();
    for (let txid in matchup_texts) {
        for (let trid in matchup_texts[txid]) {
            let transfer = $('#t_'+txid).find("tr[id='"+trid+"']");
            transfer.after(make_matchup_html(txid,trid,check=false));
        }
    }
}

function indicate_matchups(CA_long, CA_short, incomes, interest, expenses) {
    lines = CA_short;
    matchups_basis = {}
    matchups_sales = {}
    matchups_incomes = {}
    matchups_interest = {}
    matchups_expenses = {}
    matchup_gains = {}
    define_matchups(CA_long);
    define_matchups(CA_short);
    define_matchups_ii(incomes,matchups_incomes)
    define_matchups_ii(interest,matchups_interest)
    define_matchups_ii(expenses,matchups_expenses)

    matchup_texts = {}

//    $('.matchup').remove();

    for (let txid in matchups_basis) {

        if (txid == -10) //mtm eoy
            continue;
        let chain_name = all_transactions[txid]['chain'];
        let cur_num = all_transactions[txid]['num']
        for (let trid in matchups_basis[txid]) {
            text = "";
            let short = false;
            for (tok in matchups_basis[txid][trid]) {
                let tok_symbol = get_symbol(tok,chain_name);
                text += "This "+tok_symbol+" is disposed ";
                cnt = matchups_basis[txid][trid][tok]['txids'].length;
                if (cnt <=5) {
                    if (cnt == 1)
                        text += "in transaction ";
                    else
                        text += "in transactions ";
                    subs = [];
                    for (o_txid of matchups_basis[txid][trid][tok]['txids']) {
                        if (o_txid == -10)
                            subs.push('at end of year (mark-to-market)')
                        else {
                            let num = all_transactions[o_txid]['num'];
                            if (!(subs.includes("#"+num)))
                                subs.push("#"+num);
                            if (num < cur_num) short = true;
                        }
                    }
                    text += subs.join(', ');
                } else {
                    text += "in more than 5 transactions";
                }
                let cap_gain = round_usd(matchups_basis[txid][trid][tok]['gain']);
                text += ", total cap gain is "+print_fiat(cap_gain);
                if (short) text += "<br><b>This involves a short sale</b>";

                set_impact(txid,cap_gain)
            }

            add_matchup_text(txid,trid,text)


        }
    }

    for (let txid in matchups_sales) {

        if (txid == -10) //mtm eoy
            continue;
        let cur_num = all_transactions[txid]['num']
        let chain_name = all_transactions[txid]['chain']
        for (let trid in matchups_sales[txid]) {

            let transfer = find_transfer(txid,trid)
            let tr_tok = transfer['coingecko_id']
            if (tr_tok == null)
                tr_tok = chain_name+":"+transfer['what']

            if (transfer['token_nft_id'] != null)
                tr_tok += "_"+transfer['token_nft_id']
            text = "";
            let short = false;
            for (tok in matchups_sales[txid][trid]) {
                let tok_symbol = get_symbol(tok,chain_name);
                if (tok != tr_tok) {
                    let tr_tok_symbol = get_symbol(tr_tok,chain_name);
//                    console.log('tok vault convo',tok,'tr_tok',tr_tok,'tok_symbol',tok_symbol,'tr_tok_symbol',tr_tok_symbol)
                    text += "The "+tr_tok_symbol+" was converted inside the vault from "+tok_symbol+" originally acquired ";
                } else
                    text += "This "+tok_symbol+" was acquired ";
                cnt = matchups_sales[txid][trid][tok]['txids'].length;
                if (cnt <= 5) {
                    if (cnt == 1)
                        text += "in transaction ";
                    else
                        text += "in transactions ";
                    subs = [];
                    for (o_txid of matchups_sales[txid][trid][tok]['txids']) {
                        if (o_txid == -10)
                            subs.push('at start of year (mark-to-market)')
                        else {
                            let num = all_transactions[o_txid]['num'];
                            if (!(subs.includes("#"+num)))
                                subs.push("#"+num);
                            if (num > cur_num) short = true;
                        }
                    }
                    text += subs.join(', ');
                } else {
                    text += "in more than 5 transactions";
                }
                cap_gain = round_usd(matchups_sales[txid][trid][tok]['gain']);
                text += ", total cap gain is "+print_fiat(cap_gain);
                if (short) text += "<br><b>This involves a short sale</b>";
                text += "</p>";

                set_impact(txid,cap_gain);
            }

            add_matchup_text(txid,trid,text)

        }
    }

    for (let txid in matchups_incomes) {
        if (txid == -10) //mtm eoy
            continue;
        for (let trid in matchups_incomes[txid]) {
            entry = matchups_incomes[txid][trid];
            if (entry['amount'] > 1) {
                text = entry['text'] + ": "+print_fiat(round_usd(entry['amount']));
                add_matchup_text(txid,trid,text)
                set_impact(txid,round_usd(entry['amount']));
            }
        }
    }

    for (let txid in matchups_interest) {
        if (txid == -10) //mtm eoy
            continue;
        for (let trid in matchups_interest[txid]) {
            entry = matchups_interest[txid][trid];
            if (entry['amount'] > 1) {
                text = entry['text'] + ": "+print_fiat(round_usd(entry['amount']));
                add_matchup_text(txid,trid,text)
                set_impact(txid,round_usd(entry['amount']));
            }
        }
    }

    for (let txid in matchups_expenses) {
        if (txid == -10) //mtm eoy
            continue;
        for (let trid in matchups_expenses[txid]) {
            entry = matchups_expenses[txid][trid];
            if (entry['amount'] > 1) {
                text = entry['text'] + ": "+print_fiat(round_usd(entry['amount']));
                add_matchup_text(txid,trid,text)
                set_impact(txid,round_usd(entry['amount']));
            }
        }
    }






}

function reset_impact() {
    for (let txid in all_transactions) {
        all_transactions[txid].impact = 0;
    }
}

function set_impact(txid, gain) {
    if (Math.abs(gain) > all_transactions[txid].impact) {
        all_transactions[txid].impact = Math.abs(gain);
    }
}

function highlight_impact() {
    if (typeof visible_order === 'undefined')
        return
    for (let txid of visible_order) {
        el = $('#t_'+txid);
        if (all_transactions[txid].impact > params['high_impact_amount']) {
            el.addClass('high_impact');
//            console.log("add high_impact "+txid)
        } else {
            el.removeClass('high_impact');
//            console.log("remove high_impact "+txid)
        }
    }
}

function process_tax_errors(txid=null) {
    if (txid==null && typeof visible_order === 'undefined')
        return
    if (typeof CA_errors === 'undefined')
        return
//    console.log("processing tax calc errors")
    let level_options = [0,3,5,10]
    let txid_list = []
    if (txid != null)
        txid_list.push(txid)
    else {
        txid_list = visible_order;
        $('.ca_error').remove();
        $('.t_class_ca').removeClass('t_class_ca t_class_ca_0 t_class_ca_3 t_class_ca_5');
    }

//    let err_list = {}
//    if (txid != null) {
//        if (txid in CA_errors)
//            err_list[txid] = CA_errors[txid]
//    } else {
//        err_list = CA_errors
//        $('.ca_error').remove();
//        $('.t_class_ca').removeClass('t_class_ca t_class_ca_0 t_class_ca_3 t_class_ca_5');
//    }
    for (let txid of txid_list) {
        if (parseInt(txid) < 0)
            continue;
        if (!(txid in CA_errors))
            continue;
//        console.log('err txid',txid)
        let chain_name = all_transactions[txid]['chain'];
        let el = $('#t_'+txid);
//        console.log(el.length);
        let tx_below_level = false;
        let entry = CA_errors[txid];
        let error = entry['error'];
        let level = entry['level'];

        el.removeClass('t_class_ca t_class_ca_0 t_class_ca_3 t_class_ca_5')
//        console.log("orig color",txid)
        if (level < all_transactions[txid]['original_color'])
            el.addClass('t_class_ca t_class_ca_'+level)

//            el.removeClass('t_class_10 t_class_5 t_class_3').addClass('t_class_'+level);

        let note_text = "";
        let amount = entry['amount'];
        let symbol = get_symbol(entry['token'],chain_name);
        if (error == 'going short') {
            note_text = "Note: At this point you do not have "+round(Math.abs(amount))+" "+symbol+" to complete one of the transfers. This transaction opens a short position. "+
            "This is correct behaviour if you previously borrowed " +symbol+". If this is a rebasing asset, manually add a rebasing transaction just before this one. "+
            "Otherwise, find the earlier transaction where you acquired "+symbol+", change the \"ignore\" treatment, and click \"Recalculate taxes\"."
        }

        if (error == 'going long') {
            note_text = "Note: This transaction closes previously opened short position on "+symbol+".";
        }
//        console.log('making error note');
        let error_note = "<div class='note note_"+level+" ca_error'>"+note_text+"</div>";
        el.find('.tx_row_2').after(error_note);
    }
}


$('body').on('click','#calc_tax',function() {
    calc_tax();
});

$('body').on('click','#download_tax_forms, #download_turbotax',function() {
    let year = $('#tax_year').val();
    let mtm = false
    let dltype = $(this).attr('id').substr(9);
    window.open("download?type="+dltype+"&year="+year+"&mtm="+mtm+"&address="+primary,'_blank');
});

$('body').on('change','#tax_year',function() {
    year = $('#tax_year').val();
//    console.log(year);
    show_sums(CA_long, CA_short, incomes, interest, expenses, year);
});

$('body').on('change','#mtm',function() {
//    if (mtm != $('#mtm').is(":checked"))
    need_recalc();
//    else
//        need_recalc(false);
});

$('body').on('change','#matchups_visible',function() {
    let matchups_visible = $('#matchups_visible').is(':checked');
//    console.log('matchups_visible',matchups_visible)
//    $.get("save_info?address="+primary+"&field=matchups_visible&value="+parseInt(matchups_visible));
    save_info('matchups_visible',+matchups_visible)
    if (matchups_visible)
        $('.matchup').removeClass('hidden');
    else
        $('.matchup').addClass('hidden');
});



function compare_ts(tx1,tx2) {
    let ts1 = tx1.ts
    let ts2 = tx2.ts
    if (tx1.chain == 'Avalanche' || tx1.chain == 'Fantom')
        ts1 -= 30
    if (tx2.chain == 'Avalanche' || tx2.chain == 'Fantom')
        ts2 -= 30
    if (ts1 < ts2) return -1;

    if (ts1 > ts2) return 1;

    if (tx1.nonce != null && tx2.nonce != null && tx1.nonce < tx2.nonce) return -1;
    return 1;
}

function calc_tax() {
    $(document.body).css({'cursor' : 'wait'});

    data = $.map(all_transactions, function(value, key) { return value });
    data.sort(compare_ts);
    data = JSON.stringify(data);

    year = $('#tax_year').val();
    mtm = $('#mtm').is(":checked");

    js = JSON.stringify(data);
    $('#tax_block').find('.err_mes').remove();
//    $.post("calc_tax?year="+year+"&mtm="+mtm+"&address="+address+"&chain="+chain,  js, function(resp) {
     $.ajax({type:'POST',url:"calc_tax?year="+year+"&mtm="+mtm+"&address="+primary, data:js, contentType : 'application/json', success:function(resp) {
        var data = JSON.parse(resp);
        if (data.hasOwnProperty('error')) {
            $('#tax_block').append("<div class='err_mes'>"+data['error']+"</div>");
            $(document.body).css({'cursor' : 'default'});
            return;
        }

        process_tax_js(data);
        show_tax_related_html();
        show_inspections(data);
        need_recalc(false);
        $(document.body).css({'cursor' : 'default'});

    }
    });
}

$('body').on('click','#download_transactions_json, #download_transactions_csv',function() {
    $(document.body).css({'cursor' : 'wait'});
    let type = $(this).attr('id').substr(22);
    data = $.map(all_transactions, function(value, key) { return value });
    data.sort(compare_ts);
    js = JSON.stringify(data);
    js = JSON.stringify(js); //don't ask me

    $('#tax_block').find('.err_mes').remove();

    $.ajax({type:'POST',url:"save_js?address="+primary, data:js, contentType : 'application/json',
        success:function(resp) {
            var data = JSON.parse(resp);
            if (data.hasOwnProperty('error')) {
                $('#tax_block').append("<div class='err_mes'>"+data['error']+"</div>");
                $(document.body).css({'cursor' : 'default'});
                return;
            }

            window.open("download?type=transactions_"+type+"&address="+primary,'_blank');

            $(document.body).css({'cursor' : 'default'});

        }
    });


//    window.open("download?type=transactions_"+type+"&address="+primary,'_blank');
//    $.post("download?chain="+chain+"&address="+address+"&type=transactions_json", data, function(resp) {
//        console.log(resp);
//        var data = JSON.parse(resp);
//        if (data.hasOwnProperty('error')) {
//            $('#recolor_block').append("<div class='err_mes'>"+data['error']+"</div>");
//        } else {
//            transactions.removeClass('custom_recolor_0 custom_recolor_3 custom_recolor_5 custom_recolor_10').addClass('custom_recolor custom_recolor_'+color_id);
//        }
//    });
});


function need_recalc(show=true) {
    if (show && $('#need_recalc').length == 0) {
        $('#calc_tax').before("<div id='need_recalc'>Recalculation needed</div>");
        $('#tax_data').addClass('outdated');
        $('#inspect_summary').addClass('outdated');
//        $('#dc_inspections_block').addClass('tax_data_outdated');
    }

    if (!show) {
        $('#need_recalc').remove();
        $('.outdated').removeClass('outdated');
    }
}


$('body').on('click','#downloads',function() {
    let html ="<div id='overlay'></div><div id='dl_popup' class='popup'>";
    html += "<div class='header'>What do you want to download?</div>";
    html += "<ul id='download_list'>"
    html +="<li><a id='download_transactions_json'>Transactions json</a> (raw data)</li>";
    html +="<li><a id='download_transactions_csv'>Transactions csv</a> (easier to read)</li>";
    html +="<li><a id='download_tax_forms'>Tax forms for your CPA</a></li>";
    html +="<li><a id='download_turbotax'>Form 8949 for TurboTax Online</a> (you will also need to report misc. income)<div class='help help_turbotax'></div></li>";
    html += "</ul>";
    html += "<div id='dl_cancel'>Cancel</div>";
    $('#content').append(html);
});

$('body').on('click','#options',function() {
    let html ="<div id='overlay'></div><div id='opt_popup' class='popup'>";
    html += "<form id='opt_form'>"
//    html += "<table id='option_list'>"
//    html +="<tr><td>Your currency:</td>";
//    html += "<td><select id='currency_select' name='opt_fiat'>";
//    for (symbol in fiat_info) {
//        let selected = ""
//        if (symbol == fiat)
//            selected = "selected"
//        html += "<option "+selected+" value='"+symbol+"'>"+symbol+"</option>";
//    }
//    html += "</select></td></tr>";
//    html += "</table>"
    html += "<ul id='option_list'>"

    html += "<li>Your currency:<select id='opt_fiat' name='opt_fiat'>"
    for (symbol in fiat_info) {
        let selected = ""
        if (symbol == fiat)
            selected = "selected"
        html += "<option "+selected+" value='"+symbol+"'>"+symbol+"</option>";
    }
    html += "</select>"
    html += "<div class='opt_note'>Note: this will only affect you currency and exchange rates. Tax forms are still going to be in the US format, but all values will be in the new currency.</div>"
    html += "</li>"


    html += "<li><div class='opt_left'>Your transaction costs are treated as part of your cost basis when the transaction involves a taxable event. How should they be treated if it doesn't? "
    html += "For example, when you approve a contract, or when you stake something without getting anything back?</div>"
    html += "<div class='opt_right'><ul class='opt_radio_list'>"
    let opts = [['sell','Sale (not tax-deductible)'],['expense','Business expense'],['loss','Capital loss']]
    for (let opt of opts) {
        let checked = ""
        if (global_options["opt_tx_costs"] == opt[0])
            checked = "checked"
        html += "<li><label><input type=radio "+checked+" name=opt_tx_costs value='"+opt[0]+"'>"+opt[1]+"</label></li>"
    }
    html += "</ul></div></li>"

    html += "<li><div class='opt_left'>If you deposited something to a vault, and later withdrew more than you deposited, how should we treat this extra money?</div>"
    html += "<div class='opt_right'><ul class='opt_radio_list'>"
    opts = [['income','Income (taxable immediately as income)'],['gain','Free acquisition (taxable as capital gain when sold)']]
    for (let opt of opts) {
        let checked = ""
        if (global_options["opt_vault_gain"] == opt[0])
            checked = "checked"
        html += "<li><label><input type=radio "+checked+" name=opt_vault_gain value='"+opt[0]+"'>"+opt[1]+"</label></li>"
    }
    html += "</ul></div></li>"

    html += "<li><div class='opt_left'>If you deposited something to a vault, but later exited the vault with a loss, how should we treat this loss?</div>"
    html += "<div class='opt_right'><ul class='opt_radio_list'>"
    opts = [['sell','Sale (not tax-deductible)'],['expense','Business expense'],['loss','Capital loss']]
    for (let opt of opts) {
        let checked = ""
        if (global_options["opt_vault_loss"] == opt[0])
            checked = "checked"
        html += "<li><label><input type=radio "+checked+" name=opt_vault_loss value='"+opt[0]+"'>"+opt[1]+"</label></li>"
    }
    html += "</ul></div></li>"


    html += "</ul>"
    html += "</form>";
    html += "<div class='sim_buttons'><div id='opt_process'>Save changes</div>";
    html += "<div id='opt_cancel'>Cancel</div></div>";
    $('#content').append(html);
});

$('body').on('change', '#opt_fiat', function() {
    let new_fiat = $(this).val();
    if (new_fiat != fiat) {
        if ($('#opt_fiat_update_custom').length == 0) {
            html = "<div id='opt_fiat_update_custom'><label>Adjust your custom rates to the new currency? <input type=checkbox checked name='opt_fiat_update_custom'></label></div>"
            $('#opt_fiat').after(html)
        }
    } else {
        $('#opt_fiat_update_custom').remove()
    }
});

$('body').on('click','#opt_process',function() {
    data = $('#opt_form').serialize();
    $.post("save_options?address="+primary, data, function(resp) {
        var data = JSON.parse(resp);
        if (data.hasOwnProperty('error')) {
            $("#opt_form").after("<div class='err_mes'>"+data['error']+"</div>");
        } else {
            if (data['reproc_needed']) {
                $('#main_form').submit();
                return
            } else if (data['recalc_needed']) {
                need_recalc();
            }

            let opt_fields = ['fiat','opt_vault_gain','opt_vault_loss','opt_tx_costs']
            for (opt of opt_fields) {
                if (opt in data)
                    global_options[opt] = data[opt]
            }

            $('#opt_popup').remove();
            $('#overlay').remove();
        }
    });
});