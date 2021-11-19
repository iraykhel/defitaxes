mtm = false;
matchups_visible = false;

function round_usd(amount) {
    return Math.round(amount);
}

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
    html += "<div id='matchups_selector'><label>Show tax outcomes<input type=checkbox id='matchups_visible'></label></div>";
    html +="<div id='mtm_selector'><label>Mark-to-market <div class='help help_mtm'></div><input type=checkbox id='mtm'></label></div>";
    html +="<a id='calc_tax'>Recalculate taxes</a>";

//    html += "<a href='download?address="+address+"&type=transactions_csv' id='download_transactions_csv'>csv</a></div>";
    html += "<div id='tax_data'>";
    html += year_html;
    html +="<a id='download_tax_forms'>Download tax forms</a>";
    html += "</div>";


    html += "</div>";



    $('#content').append(html);
//    $('#calc_tax').click(calc_tax);
//    calc_tax();


}

function process_tax_js(js) {
    year = $('#tax_year').val();
//'CA_long':CA_long,'CA_short':CA_short,'CA_errors':CA_errors,'incomes':calculator.incomes,'interest':calculator.interest_payments
    CA_long = js['CA_long'];
    CA_short = js['CA_short'];
    CA_errors = js['CA_errors'];
    incomes = js['incomes'];
    interest = js['interest'];
    console.log('process_tax_js',year);
    show_sums(CA_long, CA_short, incomes, interest, year);
    indicate_matchups(CA_long, CA_short, incomes, interest);
    process_errors(CA_errors);
//    populate_vault_info(js['vault_info']);



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

function show_sums(CA_long, CA_short, incomes, interest,year) {
    $('#tax_sums').remove();
    total_gain_long = sum_up(CA_long,year,field='gain',timestamp_field='out_ts');
    total_gain_short = sum_up(CA_short,year,field='gain',timestamp_field='out_ts');
    total_income = sum_up(incomes,year);
    total_interest = sum_up(interest,year);
    html = "<table id='tax_sums'>";
    if (mtm) {
        html += "<tr><td class='tax_sum_header'>Ordinary income</td><td class='tax_sum_val'>$"+round_usd(total_income+total_gain_short)+"</td></tr>";
    } else {
        html += "<tr><td class='tax_sum_header'>Long-term cap gains</td><td class='tax_sum_val'>$"+round_usd(total_gain_long)+"</td></tr>";
        html += "<tr><td class='tax_sum_header'>Short-term cap gains</td><td class='tax_sum_val'>$"+round_usd(total_gain_short)+"</td></tr>";
        html += "<tr><td class='tax_sum_header'>Ordinary income</td><td class='tax_sum_val'>$"+round_usd(total_income)+"</td></tr>";
    }
    html += "<tr><td class='tax_sum_header'>Loan interest paid</td><td class='tax_sum_val'>$"+round_usd(total_interest)+"</td></tr>";
    html += "</table>";
    $('#year_selector').after(html);
}


function define_matchups(lines) {
    for (line of lines) {
        let in_txid = line['in_txid'];
        let in_tridx = line['in_tridx'];

        let out_txid = line['out_txid'];
        let out_tridx = line['out_tridx'];

        let tok = line['symbol'];
        let gain = line['gain'];

        if (!(in_txid in matchups_basis))
            matchups_basis[in_txid] = {};
        if (!(in_tridx in matchups_basis[in_txid]))
            matchups_basis[in_txid][in_tridx] = {}
        if (!(tok in matchups_basis[in_txid][in_tridx]))
            matchups_basis[in_txid][in_tridx][tok] = {'txids':[],'gain':0}

        matchups_basis[in_txid][in_tridx][tok]['txids'].push(out_txid);
        matchups_basis[in_txid][in_tridx][tok]['gain'] += gain;

        if (!(out_txid in matchups_sales))
            matchups_sales[out_txid] = {};
        if (!(out_tridx in matchups_sales[out_txid]))
            matchups_sales[out_txid][out_tridx] = {}
        if (!(tok in matchups_sales[out_txid][out_tridx]))
            matchups_sales[out_txid][out_tridx][tok] = {'txids':[],'gain':0}

        matchups_sales[out_txid][out_tridx][tok]['txids'].push(in_txid);
        matchups_sales[out_txid][out_tridx][tok]['gain'] += gain;
    }
}

function define_matchups_ii(lines,target) {
    for (line of lines) {
        let txid = line['txid'];
        let tridx = line['tridx'];

        if (!(txid in target))
            target[txid] = {};
        if (!(tridx in target[txid]))
            target[txid][tridx] = {'text':line['text'],'amount':line['amount']}
    }
}

function add_matchup_text(txid,tridx,text) {
    if (!(txid in matchup_texts))
        matchup_texts[txid] = {}
    if (!(tridx in matchup_texts[txid]))
        matchup_texts[txid][tridx] = []
    matchup_texts[txid][tridx].push(text);
}

function make_matchup_html(txid,tridx,check=true) {
    if (check) {
        if (!(txid in matchup_texts))
            return "";
        if (!(tridx in matchup_texts[txid]))
            return "";
    }

    matchup_html = "<tr class='matchup";
    if (!matchups_visible)
        matchup_html += ' hidden';
    matchup_html += "'><td colspan=7>";
    for (matchup_text of matchup_texts[txid][tridx]) {
        matchup_html += ("<p>"+matchup_text +"</p>");
    }
    matchup_html += "</td></tr>";
    return matchup_html
}

function make_all_matchup_html() {
    $('.matchup').remove();
    for (let txid in matchup_texts) {
        for (let tridx in matchup_texts[txid]) {
            let transfer = $('#t_'+txid).find("tr[index='"+tridx+"']");
            transfer.after(make_matchup_html(txid,tridx,check=false));
        }
    }
}

function indicate_matchups(CA_long, CA_short, incomes, interest) {
    lines = CA_short;
    matchups_basis = {}
    matchups_sales = {}
    matchups_incomes = {}
    matchups_interest = {}
    matchup_gains = {}
    define_matchups(CA_long);
    define_matchups(CA_short);
    define_matchups_ii(incomes,matchups_incomes)
    define_matchups_ii(interest,matchups_interest)

    matchup_texts = {}

//    $('.matchup').remove();

    for (let txid in matchups_basis) {
        let cur_num = all_transactions[txid]['num']
        if (txid == -10) //mtm eoy
            continue;
        for (let tridx in matchups_basis[txid]) {
//            console.log('matchup basis',txid,tridx)
//            transfer = $('#t_'+txid).find("tr[index='"+tridx+"']");
            //tok = $(transfer).find('.r_token').html();
            text = "";
            let short = false;
            for (tok in matchups_basis[txid][tridx]) {
                text += "This "+tok+" is disposed ";
                cnt = matchups_basis[txid][tridx][tok]['txids'].length;
                if (cnt <=5) {
                    if (cnt == 1)
                        text += "in transaction ";
                    else
                        text += "in transactions ";
                    subs = [];
                    for (o_txid of matchups_basis[txid][tridx][tok]['txids']) {
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

                text += ", total cap gain is $"+round_usd(matchups_basis[txid][tridx][tok]['gain']);
                if (short) text += "<br><b>This involves a short sale</b>";
            }

            add_matchup_text(txid,tridx,text)

        }
    }

    for (let txid in matchups_sales) {
        let cur_num = all_transactions[txid]['num']
        if (txid == -10) //mtm eoy
            continue;
        for (let tridx in matchups_sales[txid]) {
//            console.log('matchup sale',txid,tridx)
            transfer = $('#t_'+txid).find("tr[index='"+tridx+"']");
            tr_tok = $(transfer).find('.r_token').html();
            text = "";
            let short = false;
            for (tok in matchups_sales[txid][tridx]) {
                if (tok != tr_tok)
                    text += "The "+tr_tok+" was converted inside the vault from "+tok+" originally acquired ";
                else
                    text += "This "+tok+" was acquired ";
                cnt = matchups_sales[txid][tridx][tok]['txids'].length;
                if (cnt <= 5) {
                    if (cnt == 1)
                        text += "in transaction ";
                    else
                        text += "in transactions ";
                    subs = [];
                    for (o_txid of matchups_sales[txid][tridx][tok]['txids']) {
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
                text += ", total cap gain is $"+round_usd(matchups_sales[txid][tridx][tok]['gain']);
                if (short) text += "<br><b>This involves a short sale</b>";
                text += "</p>";
            }

            add_matchup_text(txid,tridx,text)

        }
    }

    for (let txid in matchups_incomes) {
        if (txid == -10) //mtm eoy
            continue;
        for (let tridx in matchups_incomes[txid]) {
//            transfer = $('#t_'+txid).find("tr[index='"+tridx+"']");
            entry = matchups_incomes[txid][tridx];
            if (entry['amount'] > 1) {
                text = entry['text'] + ": $"+round_usd(entry['amount']);
                add_matchup_text(txid,tridx,text)
            }
        }
    }

    for (let txid in matchups_interest) {
        if (txid == -10) //mtm eoy
            continue;
        for (let tridx in matchups_interest[txid]) {
//            transfer = $('#t_'+txid).find("tr[index='"+tridx+"']");
            entry = matchups_interest[txid][tridx];
            if (entry['amount'] > 1) {
                text = entry['text'] + ": $"+round_usd(entry['amount']);
                add_matchup_text(txid,tridx,text)

            }
        }
    }

    make_all_matchup_html();




}

function process_errors(CA_errors, txid=null) {
    let level_options = [0,3,5,10]
    let err_list = {}
    if (txid != null) {
        if (txid in CA_errors)
            err_list[txid] = CA_errors[txid]
    } else {
        err_list = CA_errors
        $('.ca_error').remove();
        $('.t_class_ca').removeClass('t_class_ca t_class_ca_0 t_class_ca_3 t_class_ca_5');
    }
    for (let txid in err_list) {
        let el = $('#t_'+txid);
        let tx_below_level = false;
        let entry = err_list[txid];
        let error = entry['error'];
        let level = entry['level'];
//        for (lopt of level_options) {
//            if (el.hasClass('t_class_'+lopt)) {
//                if (lopt < level)
//                    tx_below_level = true;
//                all_transactions[txid]['original_level'] = lopt;
//                break
//            }
//        }

//        if (!('additional_notes' in all_transactions[txid]))
//            all_transactions[txid]['additional_notes'] = []
        el.removeClass('t_class_ca t_class_ca_0 t_class_ca_3 t_class_ca_5')
        if (level < all_transactions[txid]['original_color'])
            el.addClass('t_class_ca t_class_ca_'+level)

//            el.removeClass('t_class_10 t_class_5 t_class_3').addClass('t_class_'+level);

        let note_text = "";
        let amount = entry['amount'];
        let symbol = entry['symbol']
        if (error == 'going short') {
            note_text = "Note: At this point you do not have "+round(Math.abs(amount))+" "+symbol+" to complete one of the transfers. This transaction opens a short position. "+
            "This is correct behaviour if you previously borrowed " +symbol+"."
        }

        if (error == 'going long') {
            note_text = "Note: This transaction closes previously opened short position on "+symbol+".";
        }
        let error_note = "<div class='note note_"+level+" ca_error'>"+note_text+"</div>";
        el.find('.tx_row_2').after(error_note);
    }
}


$('body').on('click','#calc_tax',function() {
    calc_tax();
});

$('body').on('click','#download_tax_forms',function() {
    download_tax_forms();
});

$('body').on('change','#tax_year',function() {
    year = $('#tax_year').val();
    console.log(year);
    show_sums(CA_long, CA_short, incomes, interest, year);
});

$('body').on('change','#mtm',function() {
    if (mtm != $('#mtm').is(":checked"))
        need_recalc();
    else
        need_recalc(false);
});

$('body').on('change','#matchups_visible',function() {
    matchups_visible = $('#matchups_visible').is(':checked');
    if (matchups_visible)
        $('.matchup').removeClass('hidden');
    else
        $('.matchup').addClass('hidden');
});


//function print_holdings(holdings,id) {
//    if (Object.keys(holdings).length > 0) {
//        html = "<div class='holdings_holder'><table class='holdings' id='"+id+"'>";
//        html += "<th>Currency</th><th>Amount</th><th>USD amount</th>";
//        worthless = []
//        for (let i in holdings) {
//            tuple = holdings[i];
//            usd_value = tuple[1]*tuple[2];
//            if (usd_value >= 1) {
//                html += "<tr><td>"+tuple[0]+"</td><td>"+round(tuple[1])+"</td><td>"+round(usd_value)+"</td></tr>\n";
//            } else {
//                worthless.push(tuple);
//            }
//        }
//        for (let i in worthless) {
//            tuple = worthless[i];
//            usd_value = tuple[1]*tuple[2];
//            html += "<tr class='worthless'><td>"+tuple[0]+"</td><td>"+round(tuple[1])+"</td><td>"+round(usd_value)+"</td></tr>\n";
//        }
//        html += "</table></div>";
//        return html;
//    } else {
//        return "<div id='no_holdings'>None</div>";
//    }
//
//}


function calc_tax() {
    $(document.body).css({'cursor' : 'wait'});

    data = $.map(all_transactions, function(value, key) { return value });
    data = JSON.stringify(data);

    address = window.sessionStorage.getItem('address');
    chain = window.sessionStorage.getItem('chain');
    year = $('#tax_year').val();
    mtm = $('#mtm').is(":checked");

    js = JSON.stringify(data);
    $('#tax_block').find('.err_mes').remove();
//    $.post("calc_tax?year="+year+"&mtm="+mtm+"&address="+address+"&chain="+chain,  js, function(resp) {
     $.ajax({type:'POST',url:"calc_tax?year="+year+"&mtm="+mtm+"&address="+address+"&chain="+chain, data:js, contentType : 'application/json', success:function(resp) {
        var data = JSON.parse(resp);
        if (data.hasOwnProperty('error')) {
            $('#tax_block').append("<div class='err_mes'>"+data['error']+"</div>");
            $(document.body).css({'cursor' : 'default'});
            return;
        }

        process_tax_js(data);
        show_inspections(data);
        need_recalc(false);
        $(document.body).css({'cursor' : 'default'});

    }
    });
}

function download_tax_forms() {
    address = window.sessionStorage.getItem('address');
    chain = window.sessionStorage.getItem('chain');
    year = $('#tax_year').val();
    mtm = $('#mtm').is(":checked");
    window.open("download?type=tax_forms&year="+year+"&mtm="+mtm+"&address="+address+"&chain="+chain,'_blank');
}

function need_recalc(show=true) {
    if (show && $('#need_recalc').length == 0) {
        $('#calc_tax').before("<div id='need_recalc'>Recalculation needed</div>");
        $('#tax_data').addClass('tax_data_outdated');
    }

    if (!show) {
        $('#need_recalc').remove();
        $('#tax_data').removeClass('tax_data_outdated');
    }
}

//function populate_vault_info(vault_info=null,txid=null) {
//
//    if (vault_info == null)
//        vault_info = saved_vault_info
//    else
//        saved_vault_info = vault_info
//
//
//    if (txid != null) {
//        local_vault_info = {};
//        local_vault_info[txid] = vault_info[txid];
//        console.log('pop vault info',txid, local_vault_info);
//    } else
//        local_vault_info = vault_info
//
//    for (let txid in local_vault_info) {
//        for (let tridx in local_vault_info[txid]) {
//            let vault_id = local_vault_info[txid][tridx];
//            let transfer = $('#t_'+txid).find("tr[index='"+tridx+"']");
//            console.log("setting vault id",txid, tridx, 'display_vault_id', vault_id, transfer.length);
//
//            set_transfer_val(txid, tridx, 'display_vault_id', vault_id);
//
//            if (vault_id != null && vault_id.toString().includes('custom:'))
//                vault_id = vault_id.toString().substr(7);
//
//            transfer.find('.r_vaultid').html(vault_id);
//        }
//    }
//}