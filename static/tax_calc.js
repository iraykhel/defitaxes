function round_usd(amount) {
    return Math.round(amount);
}

function display_tax_block() {
    address = window.sessionStorage.getItem('address');

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
    html +="<div id='mtm_selector'><label>Mark-to-market <input type=checkbox id='mtm'></label></div>";
    html +="<a id='calc_tax'>Recalculate taxes</a>";
    html +="<a href='download?address="+address+"&type=transactions_json' id='download_transactions_json'>Download transactions (json)</a>";
//    html += "<a href='download?address="+address+"&type=transactions_csv' id='download_transactions_csv'>csv</a></div>";
    html += year_html;
    html +="<a id='download_tax_forms'>Download tax forms</a>";


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
    html += "<tr><td class='tax_sum_header'>Long-term cap gains</td><td class='tax_sum_val'>$"+round_usd(total_gain_long)+"</td></tr>";
    html += "<tr><td class='tax_sum_header'>Short-term cap gains</td><td class='tax_sum_val'>$"+round_usd(total_gain_short)+"</td></tr>";
    html += "<tr><td class='tax_sum_header'>Other income</td><td class='tax_sum_val'>$"+round_usd(total_income)+"</td></tr>";
    html += "<tr><td class='tax_sum_header'>Loan interest paid</td><td class='tax_sum_val'>$"+round_usd(total_interest)+"</td></tr>";
    html += "</table>";
    $('#year_selector').after(html);
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