True = true
False = false
pb_interval = null;
demo = false
primary = null;
chain = "";
//address_info = {}
//all_addresses = [];
all_address_info = {}
displayed_addresses = [];
//displayed_chains = [];

fast_mode = false;

params = {
    'tx_per_page':100,
    'high_impact_amount':10000,
    'dc_fix_shutup':0,
    'matchups_visible':1
}

global_options  = {
    fiat : 'USD',
    opt_tx_costs : 'sell',
    opt_vault_gain : 'income',
    opt_vault_loss : 'loss'
}


//tx_per_page = 100
//high_impact_amount = 10000;
//dc_fix_shutup = false;
//matchups_visible = true;

function normalize_address(address) {
    if (address.length == 42 && address[0] == '0' && address[1] == 'x')
        return address.toLowerCase();
    return address;
}

function setup() {
    all_transactions = {}
    transaction_order = []

    current_page_idx = 0
    saved_page_idx = null
    scroll_position = null

    display_mode = 'all'
    selected_transactions = new Set()
    all_symbols = {}

    var prev_selection = null;

    lookup_info = {
        last_index: 0,

        transactions: {

        }
    }
}

setup();



//chain_config = {
//    ETH:{scanner:'etherscan.io', base_token:'ETH', scanner_name:'Etherscan', debank:1},
//    ETC:{scanner:'blockscout.com/etc/mainnet',base_token:'ETC',scanner_name:'BlockScout', debank:0},
//    Polygon:{scanner:'polygonscan.com', base_token:'MATIC', scanner_name:'PolygonScan', debank:1},
//    BSC:{scanner:'bscscan.com', base_token:'BNB', scanner_name:'BscScan', debank:1},
//    HECO:{scanner:'hecoinfo.com', base_token:'HT', scanner_name:'HecoInfo', debank:1},
//    Arbitrum:{scanner:'arbiscan.io', base_token:'ETH', scanner_name:'ArbiScan', debank:1},
//    Avalanche:{scanner:'snowtrace.io', base_token:'AVAX', scanner_name:'SnowTrace', debank:1},
//    Fantom:{scanner:'ftmscan.com', base_token:'FTM', scanner_name:'FtmScan', debank:1},
//    Moonriver:{scanner:'moonscan.io', base_token:'MOVR', scanner_name:'MoonScan', debank:1},
//    Cronos:{scanner:'cronoscan.com',base_token:'CRO', scanner_name:'Cronoscan', debank:1},
//    Gnosis:{scanner:'blockscout.com',base_token:'XDAI',scanner_name:'BlockScout', debank:1},
//    Optimism:{scanner:'optimistic.etherscan.io', base_token:'ETH', scanner_name:'Etherscan', debank:1},
//    Celo:{scanner:'celoscan.io', base_token:'CELO', scanner_name:'Celoscan', debank:1},
//    Chiliz:{scanner:'explorer.chiliz.com', base_token:'ETH', scanner_name:'BlockScout', debank:0},
//    Oasis:{scanner:'explorer.emerald.oasis.dev', base_token:'ROSE', scanner_name:'BlockScout', debank:0},
//    Doge:{scanner:'explorer.dogechain.dog', base_token:'DOGE', scanner_name:'BlockScout',  debank:1},
//    Songbird:{scanner:'songbird-explorer.flare.network', base_token:'SGB', scanner_name:'BlockScout',  debank:1},
//    Metis:{scanner:'andromeda-explorer.metis.io', base_token:'METIS', scanner_name:'BlockScout',debank:1},
//    Boba:{scanner:'blockexplorer.bobabeam.boba.network', base_token:'BOBA', scanner_name:'BlockScout',  debank:1},
//    SXNetwork:{scanner:'explorer.sx.technology', base_token:'SX', scanner_name:'BlockScout', debank:0},
////    Astar:{scanner:'blockscout.com/astar', base_token:'ASTR', scanner_name:'BlockScout', used:1, debank:1},
//    Evmos:{scanner:'blockscout.evmos.org', base_token:'EVMOS', scanner_name:'BlockScout', debank:1},
//    Kava:{scanner:'explorer.kava.io', base_token:'KAVA', scanner_name:'BlockScout', debank:1},
//    Canto:{scanner:'evm.explorer.canto.io', base_token:'CANTO', scanner_name:'BlockScout', debank:1},
//    Aurora:{scanner:'explorer.mainnet.aurora.dev', base_token:'ETH', scanner_name:'BlockScout', debank:1},
//    Step:{scanner:'stepscan.io', base_token:'FITFI', scanner_name:'BlockScout', debank:1},
//    KCC:{scanner:'scan.kcc.io', base_token:'KCS', scanner_name:'BlockScout', debank:1},
//    Solana:{scanner:'solscan.io', base_token:'SOL', scanner_name:'Solscan', debank:1},
//}





function round_usd(amount) {
    return Math.round(amount);
}

function print_fiat(amount) {
    let fiat_data = fiat_info[fiat]
    if ('left_symbol' in fiat_data)
        return fiat_data['left_symbol']+amount
    else
        return amount+fiat_data['right_symbol']
}


function timeConverter(UNIX_timestamp){
  var a = new Date(UNIX_timestamp * 1000);
  var months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  var year = a.getUTCFullYear();
  var month = months[a.getUTCMonth()];
  var date = a.getUTCDate();
  var hour = a.getUTCHours();
  var min = ("0" +a.getUTCMinutes()).substr(-2);
  var sec = ("0" +a.getUTCSeconds()).substr(-2);
  var time = hour + ':' + min+':'+sec + ', ' + month + ' ' + date+' '+year;
//  return a.toUTCString();
  return time;
}



function startend(hash,minlen=12) {
    if (hash.length > minlen)
        return hash.substring(0,5)+"..."+hash.substring(hash.length-3);
    else return hash
}

function display_hash(zerox, name='address', copiable=true, replace_users_address=true, capitalize=true) {
    if (zerox == null) {
        return "<span class='hash'></span>";
    }

//    zerox = zerox.toLowerCase();

    let html = "";
    Y= (capitalize?"Y":"y")
    if (displayed_addresses.length == 1 && displayed_addresses[0] == zerox && replace_users_address) {

        html = "<span class='hash self_address'>"+Y+"our address</span>";
    } else if (displayed_addresses.length > 1 && displayed_addresses.includes(zerox) && replace_users_address) {
//        html = "<span class='hash self_address_one_of' title='One of your addresses'>"+startend(zerox)+"</span>";
        html = "<span class='hash self_address'>"+Y+"ou ("+startend(zerox)+")</span>";
    } else {
        if (copiable)
            html = "<span class='hash copiable' title='Copy full "+name+" to clipboard' full='"+zerox+"'>"+startend(zerox)+"</span>";
        else
            html = "<span class='hash' title='"+zerox+"'>"+startend(zerox)+"</span>";
    }
    return html;
}

function display_token(token_name, token_address, nft_id, copiable=true) {
    let html = "";
    if ((token_name == null || token_name.length == 0 || token_name.toLowerCase() == token_address.toLowerCase()) && token_address != null)
        html = display_hash(token_address)
    else {
        if (token_name == token_address || token_address == null)
            html = token_name;
        else {
            if (copiable)
                if (nft_id != null)
                    html += "<span class='token copiable' title='Copy NFT collection address' full='"+token_address+"'>";
                else
                    html += "<span class='token copiable' title='Copy token address' full='"+token_address+"'>"
            html += token_name
            if (copiable) html += "</span>";
        }
    }
    if (nft_id != null) {
        if (copiable) html += " <span class='copiable' title='Copy NFT ID to clipboard' full='"+nft_id+"'>"; else html += " ";
        html += startend(nft_id);
        if (copiable) html += "</span>"
    }
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
        if (rate > 0)
            return rate.toFixed(Math.max(0,Math.round(4-Math.log10(rate))));
        else
            return rate.toFixed(Math.max(0,Math.round(4-Math.log10(-rate))));
    } else {
        return rate;
    }
}

function show(el) {
    el.css({'display':'block'});
//    el.style.display = 'block';
}

function showib(el) {
    el.css({'display':'inline-block'});
//    el.style.display = 'inline-block';
}

function hide(el) {
    el.css({'display':'none'});
//    el.style.display = 'none';
}

function show_ajax_transactions(data) {
    selected_id = null;
    if ($('.primary_selected').length > 0) selected_id = $('.primary_selected').attr('id');
    for (let idx in data['transactions']) {
        let transaction = data['transactions'][idx];
        let txid = parseInt(transaction['txid']);
        let num = all_transactions[txid]['num'];
//        let len = dict_len(all_transactions);
//        console.log('ajax num',num, all_transactions.length, all_transactions.size, );
//        idx_html = $('#t_'+txid).find('.t_idx').html();
        transaction['num'] = num;
        all_transactions[txid] = transaction;
        transaction_html = make_transaction_html(transaction);
        $('#t_'+txid).replaceWith(transaction_html)
        process_tax_errors(txid=txid)
//        $('#t_'+txid).addClass('secondary_selected');
        selected_transactions.add(txid)
//        all_transactions[txid]['selected']=true
//        $('#t_'+txid).find('.t_idx').html(idx_html);

//        populate_vault_info(vault_info=null,txid=txid);
    }
    if (selected_id != null) {
        select_transaction($('#'+selected_id),keep_secondary=true);
//        $('#'+selected_id).click();
    }
    update_selections_block()
//    if (selected_secondary.length > 0) {
//        let secondary_id_list =
//    }

}

$( document ).ready(function() {
    $('body').on('click', '.copiable', function() {
        let hash = $(this).attr('full');
        let el = $(this);
        el.css({'background-color':'#8065f7','color':'white'});
//        console.log(hash);
        let temp = $("<input>");
        $("body").append(temp);
        temp.val(hash).select();
        document.execCommand("copy");
        temp.remove();
        setTimeout(function(){ el.css({'background-color':'','color':''}); }, 50);
    });

    //from cookie
    if (address != "") {
        $('#your_address').val(address);
        get_last_update(address);
        //show_last_update(last_transaction_timestamp);
    }

    $('#your_address').on('paste',function(e) {
        address = e.originalEvent.clipboardData.getData('text').trim();
//        chain = $('#chain').val();

        get_last_update(address);
    });


    $('#your_address, #chain').on('change',function() {
        address = $('#your_address').val().trim();
//        chain = $('#chain').val();
        get_last_update(address);
    });



});

function isAlphaNumeric(str) {
  var code, i, len;

  for (i = 0, len = str.length; i < len; i++) {
    code = str.charCodeAt(i);
    if (!(code > 47 && code < 58) && // numeric (0-9)
        !(code > 64 && code < 91) && // upper alpha (A-Z)
        !(code > 96 && code < 123)) { // lower alpha (a-z)
      return false;
    }
  }
  return true;
};

function get_last_update(address) {
    if (address.length < 32 || address.length > 44 || !isAlphaNumeric(address))
        return
    $.get("last_update?address="+address, function(js) {
        var data = JSON.parse(js);
        last_transaction_timestamp = data['last_transaction_timestamp'];
        update_import_needed = data['update_import_needed']
        show_last_update(last_transaction_timestamp,update_import_needed);
    });

}

function show_last_update(last_transaction_timestamp,update_import_needed) {
    html = "";
    if (last_transaction_timestamp != 0) {
        ttime = timeConverter(last_transaction_timestamp);
        html = "You last imported transactions at "+ttime+" (UTC).<br>";
        html += "<label>Import new transactions that you made since then? <input type=checkbox id='import_new_transactions'></label>";
        if (update_import_needed)
            html += "<div id='updated_note'>Software has been updated since your last visit. "+
            "Importing new transactions will also fix some issues with the old ones, and is strongly recommended. "
            +"It will preserve custom changes you made wherever possible; additionally your data has been backed up.</div>"
    }
    $('#initial_options').html(html);
}




function start_progress_bar(mode) {
//    console.log('pb position',position)
    pb_html = "<div id='progressbar_wrap'><div id='progressbar'></div><div id='pb_phase'>Processing...<div></div>"

    if (mode=='middle') {
        $('#address_form').css({'width':'95%'});
        $('#main_form').append(pb_html);
    }

    if (mode=='initial') {
        $('#content').html(pb_html);
        $('#progressbar_wrap').css({'top':'100px','padding-top':'0px','position':'fixed','left':'50%','transform':'translateX(-50%)'});
    }

    if (mode =='popup') {
        $('.popup').append(pb_html);
    }

    $( "#progressbar" ).progressbar({
      value: 0
    })
    pb_interval = setInterval(function() {
//        let address = window.sessionStorage.getItem('address');
//        let chain_name = window.sessionStorage.getItem('chain');
        let uid = window.sessionStorage.getItem('uid');

        $.get("progress_bar?address="+primary+"&uid="+uid, function(js) {
            var data = JSON.parse(js);
            current_phase = data['phase'];
            pb = data['pb'];
            $('#progressbar').progressbar({value: pb});
            $('#pb_phase').html(current_phase);

            if (pb >= 100) {
                clearInterval(pb_interval);
            }

        });
    }, 1000);
}

function stop_progress_bar() {
    if (pb_interval != null)
        clearInterval(pb_interval);
    $('#progressbar_wrap').remove()
}

function map_lookups(transaction, unmap_instead=false) {
    func = add_to_mapping
    if (unmap_instead)
        func = remove_from_mapping
    txid = transaction['txid'];
    transaction_counterparties = transaction['counter_parties'];
    for (let progenitor in transaction_counterparties) {
        func('counterparty',progenitor,txid);
        hex_sig = transaction_counterparties[progenitor][1]
        func('signature',hex_sig,txid);
        cp_name = transaction_counterparties[progenitor][0];
        if (cp_name == 'unknown')
            cp_name = progenitor
        func('counterparty_name',cp_name,txid);
    }

    if (transaction['upload_id'] != null && transaction['function'] != null)
        func('signature',transaction['function'],txid);

    func('chain',transaction['chain'],txid)
    rows = transaction['rows'];
    outbound_count = 0;
    inbound_count = 0;
    for (let trid in rows) {
        let transfer = rows[trid];
        if (transfer['to'] != 'network') //network fee
        {
            let token_address = transfer['what'];
            let token_name = transfer['symbol'];
            let token_id = transfer['coingecko_id'];

            func('address',transfer['fr'],txid);
            if (transfer['to'] != 'network') {
                func('address',transfer['to'],txid);
                if (transfer['outbound']) {
                    outbound_count += 1;
                } else {
                    inbound_count += 1;
                }
            }


            func('token_address',token_address,txid);
            func('token_name',token_name,txid);
            if (token_id != null)
                func('token_id',token_id,txid);
            else
                func('token_id',token_address,txid);
            if (transfer['token_nft_id'] != null)
                func('token_nft',token_address+"_"+transfer['token_nft_id'],txid)
//            func('address',other_address,txid);
        }
    }

    func('outbound_count',outbound_count,txid);
    func('inbound_count',inbound_count,txid);
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
    lookups = ['chain','counterparty','counterparty_name','signature','outbound_count','token_name','token_address','token_id','inbound_count','address','token_nft']; //'outbound_token','inbound_token'
    if (!(txid in transaction_list)) {
        transaction_list[txid] = {}
        for (i = 0; i < lookups.length; i++)
            transaction_list[txid][lookups[i]] = new Set();
    }

    transaction_list[txid][mapping_type].add(value);
}


function remove_from_mapping(mapping_type,value,txid) {
    if (value == null) return;
//    console.log('rfm',value)
    mapping = lookup_info[mapping_type+"_mapping"];
//    console.log('rfm 2',mapping[value])
    mapping[value].delete(txid);
    transaction_list[txid][mapping_type].delete(value);
}



function display_counterparty(transaction, editable=false) {
//    console.log('display_counterparty', transaction)
    html = "<div class='tx_row_2'>"
    transaction_counterparties = transaction['counter_parties'];
    cp_len = dict_len(transaction_counterparties)
    if (cp_len > 0) {
        cp_idx = 0;
        for (let progenitor in transaction_counterparties) {
            cp = transaction_counterparties[progenitor][0]
            hex_sig = transaction_counterparties[progenitor][1]
            signature = transaction_counterparties[progenitor][2]


            if (editable) {
                if (signature != null)
                    html += "Operation: <span class='signature'>"+signature+"</span> @ ";
                else
                    html += "Counterparty: "


                html += "<span class='cp prog_"+progenitor+"' progenitor='"+progenitor+"' title='Update counterparty'>"+cp+"</span>";
            } else {
                if (signature != null) {
                    html += "Operation: <span class='op'>"+signature+" @ "+cp+"</span>";
                } else
                    html += "Counterparty: <span class='op'>"+cp+"</span>";
            }


            if (cp_idx != cp_len -1) transaction_html += ","
            cp_idx += 1
        }
        if (editable)
            html += "<div class='help help_cpop'></div>";
    } else if (transaction['function'] != null && transaction['upload_id'] != null) {
        html += "Operation: <span class='signature'>"+transaction['function']+"</span>";
    }
    html += "</div>";
    return html;

}

function find_transfer(transaction_id, transfer_id) {
    if (!(transaction_id in all_transactions))
        return null
    let transfers =  all_transactions[transaction_id]['rows'];
    if (!(transfer_id in transfers))
        return null
    return transfers[transfer_id]
//    transfers = all_transactions[transaction_id]['rows'];
//    for (let transfer of transfers) {
//        if (transfer['index']  == transfer_idx) {
//            return transfer;
//            break;
//        }
//    }
//    return null
}

function set_transfer_val(transaction_id, transfer_id, what, val, append=false) {
    let transfer = find_transfer(transaction_id,transfer_id)
    if (transfer != null) {
        if (append) {
            if (!(what in transfer))
                transfer[what] = [];
            transfer[what].push(val);
        } else
            transfer[what] = val;
    }
}

function display_transfers(transaction, editable=false) {
    let options_in = {'ignore':'Ignore','buy':'Buy','gift':'Acquire for free','income':'Income','borrow':'Borrow','withdraw':'Withdraw from vault','exit':'Exit vault'};
    let options_out = {'ignore':'Ignore','sell':'Sell','burn':'Dispose for free','fee':'Transaction cost','repay':'Repay loan','full_repay':'Fully repay loan','deposit':'Deposit to vault','interest':'Loan interest','expense':'Business expense'};
    let chain = transaction['chain'];
    let fiat_rate = transaction['fiat_rate'];

//    if (editable == false && fast_mode)
//        return "<div class=transfers style='display:none;'></div>";

    rows = transaction['rows'];
    rows_table = "<div class='transfers'><table class='rows'>";
    rows_table += "<tr class='transfers_header'><td class=c>From</td><td></td><td class=c>To</td>"
    rows_table += "<td class=r_amount>Amount</td><td class=r_token>Token</td>"
    if (editable)
        rows_table += "<td class=r_coingecko_id>Coingecko ID</td>"
    rows_table += "<td class=r_treatment>Tax treatment</td>";
    if (editable) {
        show_vaultid_col = true;
        rows_table += "<td class=r_vaultid>Vault/loan ID<div class='help help_vaultid'></div></td>";
    } else {
        show_vaultid_col = false;
        for (let trid in rows) {
            let transfer = rows[trid];
            treatment = transfer['treatment'];
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
    rows_table += "<td class=r_rate>"+fiat+" rate</td></tr>";

    let txid = transaction['txid'];

    for (let trid in rows) {
        let transfer = rows[trid];
        let cust_treatment_class = "";
        let cust_rate_class = "";
        let cust_vaultid_class = "";
        from = transfer['fr'];
        to = transfer['to'];
        amount = transfer['amount'];
        let token_name = transfer['symbol'];
        let token_contract = transfer['what'];
        let nft_id = transfer['token_nft_id'];
        let input = transfer['input'];
        let input_len = transfer['input_len'];
        let coingecko_id = transfer['coingecko_id'];
        let changed = transfer['changed']

        treatment = transfer['treatment'];

        if (treatment != null && treatment.includes('custom:')) {
            cust_treatment_class = " custom";
            treatment = treatment.substr(7);
        }

        rate = transfer['rate'];
        if (rate != null) {
            if (rate.toString().includes('custom:')) {
                cust_rate_class = " custom";
                rate = parseFloat(rate.substr(7));
            } else {
                rate *= fiat_rate
            }
        }


        vault_id = transfer['vault_id'];
        if (vault_id != null && vault_id.toString().includes('custom:')) {
            cust_vaultid_class = " custom";
            vault_id = vault_id.toString().substr(7);
        }

        if (to != null) {
            disp_to = display_hash(to);
        } else {
            disp_to = 'Network fee';
        }

        row_html = "<tr id="+trid+"><td>"+display_hash(from)+"</td><td class='r_arrow'><div></div></td><td>"+disp_to+"</td><td class='r_amount'>"+round(amount)+"</td><td class='r_token'>"+display_token(token_name,token_contract,nft_id);
        if (chain == 'Solana' && input_len == 200) //solana nft address
            row_html += "<a class='open_scan' title='Open in scanner' target=_blank href='https://solscan.io/token/"+input+"'></a>";
        row_html += "</td>";

        if (editable) {
            if (coingecko_id == null)
                coingecko_id_text = 'not found'
            else
                coingecko_id_text = coingecko_id
            row_html += "<td class='r_coingecko_id'><a class='edit_coingecko_id' chain='"+chain+"' contract='"+token_contract+"' symbol='"+token_name+"' title='Change Coingecko ID'>"+coingecko_id_text+"</a></td>";
        }


        row_html+="<td class='r_treatment"+cust_treatment_class+"'>"
        if (editable)
            row_html += "<select class='treatment'>";

//        console.log('disp transfer',txid,row,to,addr)
        let options = {}
        if (displayed_addresses.includes(to) && displayed_addresses.includes(from))
            options = Object.assign({},options_in,options_out)
        else if (displayed_addresses.includes(to) || to == 'my account')
            options = options_in
        else
            options = options_out

//        if (to != null && to.toLowerCase() == addr) {
//            options = options_in;
//        } else {
//            options = options_out;
//        }

        hidden_vaultid_class = " class='hidden'";
        treatment_found = 0;
        if (treatment == 'loss')
            treatment = 'sell'
        for (let option in options) {
            opt_exp = options[option];
            if (editable)
                row_html += "<option ";

            if (option == treatment) {
                if (['repay','deposit','borrow','withdraw','exit','liquidation','full_repay'].includes(option)) {
//                    console.log(transaction['num'],index,'show vaultid')
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
            row_html += make_matchup_html(txid,trid);

        rows_table += row_html;



    }
    rows_table += "</table></div>";
    return rows_table

}

function rate_note(rate_struct,symbol,level,text) {
    if (!(symbol in rate_struct))
        rate_struct[symbol] = {'level':level,'text': [text]}
    else {
        if (level < rate_struct[symbol]['level']) {
            rate_struct[symbol]['level'] = level
        }
        if (!rate_struct[symbol]['text'].includes(text))
            rate_struct[symbol]['text'].push(text)
//            rate_struct[symbol] = {'level':level,'text': text}
    }
}

function make_transaction_html(transaction) {
    let min_color = transaction['classification_certainty']
    let chain_name = transaction['chain'];


    let spam = false
    if (transaction['type'] == 'spam' || transaction['type'] == 'spam (NFT-related)')
        spam = true

    let minimized = false
    if (transaction.hasOwnProperty('minimized') && transaction.minimized != null) {
        minimized = transaction.minimized
    } else if (spam) {
        transaction.minimized = true
        minimized = true
    }




    let rate_struct = {}

    txid = transaction['txid'];


    rows = transaction['rows'];
    let other_notes = []
//    if (chain_name == 'Arbitrum') {
//        other_notes.push("Arbiscan is extremely buggy. It misses ALL ETH transfers that you receive from smart contracts! You may need to create manual transactions if you want Arbitrum to be processed correctly.")
//    }
    let transaction_symbols_out = new Set()
    let transaction_symbols_in = new Set()
    for (let trid in rows) {
        let transfer = rows[trid];
        let symbol = transfer['symbol'];
        if (transfer['from_me'] && transfer.to != 'network')
            transaction_symbols_out.add(symbol)
        if (transfer['to_me'])
            transaction_symbols_in.add(symbol)
        let coingecko_id = transfer['coingecko_id'];
        let display_symbol = symbol;
//        if (coingecko_id != null) {
//            display_symbol += " ("+coingecko_id+")"
//        }
        let what = transfer['what'];
        let nft_id = transfer['token_nft_id'];
        if (!(chain_name in all_symbols))
            all_symbols[chain_name] = {}

        if (!(symbol in all_symbols[chain_name]))
            all_symbols[chain_name][symbol] = new Set();
        all_symbols[chain_name][symbol].add(what);

        synth = transfer['synthetic']
        if (synth == 3) {
            other_notes.push(display_token(symbol,what,null,copiable=false)+" appears to be a rebasing asset. The first transfer in this transaction represents all its rebases up to this point.")
            min_color = 0
        }

        if (synth == 4) {
//            other_notes.push("Etherscan seems to have missed where you acquired "+display_token(symbol,what,nft_id,copiable=false)+". We added a transfer simulating a mint to this transaction.");
            other_notes.push("Etherscan seems to have missed a mint in this transaction. We added it based on simplehash data. There's a small chance this is in a wrong transaction.");
            min_color = 3
        }

        if (synth == 5) {
            other_notes.push("Arbiscan screws up direction of transactions that bridge funds in. The transfer here has opposite direction from what Arbiscan shows.");
            min_color = Math.min(min_color,5)
        }


        rate = transfer['rate'];
        if (rate == null) {
            treatment = transfer['treatment'];
            if (treatment != 'ignore' && treatment != null) {
                if (transaction['type'] != 'transfer in' && !spam) //airdrops don't have rates
                    min_color = 0;
                if (!spam)
                    rate_note(rate_struct,symbol,0,"Could not find rate of "+symbol+", assuming 0")
            }
        } else if (!rate.toString().includes('custom:')) {
            let good_rate = transfer['rate_found'];
            let rate_source = transfer['rate_source'];
            if (rate_source.includes("inferred")) {
                if (rate_source.includes("inferred from ")) {
                    let source_trust = parseFloat(rate_source.substr(rate_source.indexOf('inferred from ')+14))
                    let level = 10
                    if (source_trust < 1)
                        level = 5
                    if (source_trust < 0.5)
                        level = 3
                    rate_note(rate_struct,symbol,level,"Rate for "+symbol+" is inferred from the other currencies and might be wrong")
                    min_color = Math.min(min_color,level)
                } else if (rate_source.includes("after")) {
                    let ts = rate_source.substr(rate_source.indexOf("after") + 11,10);
                    let ttime = timeConverter(ts);
                    rate_note(rate_struct,symbol,5,"Rate for "+symbol+" is inferred from earlier transactions (from "+ttime+")")
                    min_color = Math.min(min_color,5)
                } else if (rate_source.includes("before")) {
                    let ts = rate_source.substr(rate_source.indexOf("before") + 13,10);
                    let ttime = timeConverter(ts);
                    rate_note(rate_struct,symbol,3,"Rate for "+symbol+" is inferred from subsequent transactions (from "+ttime+") and is probably wrong!")
                    min_color = Math.min(min_color,3)
                } else
                    rate_note(rate_struct,symbol,10,"Rate for "+symbol+" is inferred from the other currencies")
            }
            else if (rate_source.includes("before")) {
                let ts = rate_source.substr(rate_source.indexOf("before") + 13,10);
                let ttime = timeConverter(ts);
                rate_note(rate_struct,symbol,3,"We don't have rates data for "+display_symbol+" at the time of this transaction, we are using the earliest rate we have (from "+ttime+"), and it's probably wrong")
                min_color = Math.min(min_color,3)
            } else if (rate_source.includes("after")) {
                let ts = rate_source.substr(rate_source.indexOf("after") + 11,10);
                let ttime = timeConverter(ts);
                rate_note(rate_struct,symbol,5,"We don't have rates data for "+display_symbol+" at the time of this transaction, we are using the latest rate we have (from "+ttime+"), and it may be wrong")
                min_color = Math.min(min_color,5)
            }

            if (rate_source.includes("adjusted by")) {
                let factor = Math.abs(parseFloat(rate_source.substr(rate_source.indexOf('adjusted by ')+12)))
                if (factor > 0.5) {
                    rate_note(rate_struct,symbol,0,"To balance this transaction, rate for "+symbol+" had to be adjusted by over 50% and is probably wrong.")
                    min_color = Math.min(min_color,0)
                }  else if (factor > 0.05) {
                    rate_note(rate_struct,symbol,3,"To balance this transaction, rate for "+symbol+" had to be adjusted by over 5% and might be wrong")
                    min_color = Math.min(min_color,3)
                }
            }
        }
    }





    if ('protocol_note' in transaction)
        other_notes.push(transaction['protocol_note'])



    type_class = "";
    type = transaction['type'];
    if (type == null) {
        type = 'unknown';
        type_class = 't_class_unknown';
    }

    if (type.includes("NOT SURE"))
        type_class = 't_class_unknown';

    ct_id = transaction['ct_id'];
    transaction['original_color'] = min_color;
    transaction_html = "<div id='t_"+txid+"' class='transaction t_class_"+min_color+" "+type_class;
    if ('custom_color_id' in transaction)
        transaction_html += " custom_recolor custom_recolor_"+transaction['custom_color_id'];
    if (ct_id != null)
        transaction_html += " custom_type custom_type_"+ct_id;
    if ('manual' in transaction && transaction['manual'])
        transaction_html += " manual";
    if (minimized)
        transaction_html += " t_minimized"
    transaction_html += "'>";
    ts = transaction['ts'];
    ttime = timeConverter(ts);



//                                transaction_html += "<input type=checkbox class='t_sel'>";
    transaction_html += "<div class='top_section'>"

    transaction_html += "<div class='tx_row_0'>"
    if (minimized) {
        transaction_html += "<span class='t_class' title='"+type.toUpperCase()+"'>"+type.toUpperCase()+"</span>";
        transaction_html += "<span class='t_time_icon' title='"+ttime+"'><div class=time_icon></div></span>"
    } else
        transaction_html += "<span class='t_time copiable' title='Copy timestamp to clipboard' full='"+ts+"'>"+ttime+"</span>";
    transaction_html += "<span class='tx_chain tx_chain_"+chain_name+"'>"+chain_name+"</span>";
    if (transaction['hash'] != null) {
        if (minimized)
            transaction_html += "<span class='tx_hash'>"+display_hash(transaction['hash'], "hash");
        else
            transaction_html += "<span class='tx_hash'>TX hash: "+display_hash(transaction['hash'], "hash");
        if (chain_name in chain_config) {
            let scanner = chain_config[chain_name]['scanner'];
            transaction_html += "<a class='open_scan' title='Open in scanner' href='https://"+scanner+"/tx/"+transaction['hash']+"' target=_blank></a>";
        }
        transaction_html += "</span>"
    }

    if (minimized) {
        transaction_symbols_out = Array.from(transaction_symbols_out)
        transaction_symbols_in = Array.from(transaction_symbols_in)
        if (transaction_symbols_in.length + transaction_symbols_out.length > 0) {
            transaction_html += "<span class='t_tokens'>"
            if (transaction_symbols_in.length > 0)
                transaction_html += "<span class='t_token_list'>"+transaction_symbols_in.toString()+"</span><span class='r_arrow'><div></div></span>"
            transaction_html += "you"
            if (transaction_symbols_out.length > 0)
                transaction_html += "<span class='r_arrow'><div></div></span><span class='t_token_list'>"+transaction_symbols_out.toString()+"</span>"
            transaction_html += "</span>";
        }
    }

    if (transaction['num'] == null)
        transaction_html += "<span class='t_idx'></span>";
    else
        transaction_html += "<span class='t_idx'><span class='t_num'>#"+transaction['num']+"</span>/<span class='len'>"+transaction_order.length+"</span></span>";

    if (minimized)
        transaction_html += "<span class='t_hide t_hide_hidden' title='Show transaction'></span>";
    else
        transaction_html += "<span class='t_hide t_hide_shown' title='Minimize transaction'></span>";
    transaction_html += "</div>";






    if (!minimized) {
        let changed = transaction['changed'];
        let previous_type = null
        if (changed != null && changed != "NEW" && 'Category' in changed) {
            old_type = changed['Category'][0]
            if (old_type == null)
                previous_type = "unclassified"
            else
                previous_type = old_type
        }

        transaction_html += "<div class='tx_row_1'>"

        ct_id = transaction['ct_id'];
        if (ct_id != null)
            transaction_html += "<input type=hidden name=ct_id class=ct_id value="+ct_id+"><span class='t_class'>Your classification: "+type.toUpperCase()+"</span>";
        else {
            transaction_html += "<span class='t_class'>Our classification: "+type.toUpperCase()+"</span>";
            if (previous_type != null)
                transaction_html += "<span class='t_cat_updated'>[UPDATED FROM "+previous_type.toUpperCase()+"]</span>"
        }

        transaction_html += "</div>";

        transaction_html += display_counterparty(transaction,false);
        transaction_html += "<div class='notes'>"
        for (let symbol in rate_struct) {
            for (let text of rate_struct[symbol]['text'])
                transaction_html += "<div class='note note_"+rate_struct[symbol]['level']+"'>Note: "+text+"</div>";
        }

        for (let note of other_notes) {
            transaction_html += "<div class='note'>Note: "+note+"</div>";
        }

        if ('custom_note' in transaction)
            transaction_html += "<div class='custom_note'>"+transaction['custom_note']+"</div><div class='add_note'>Edit note</div>";
        else
            transaction_html += "<div class='custom_note'></div><div class='add_note'>Add a note</div>";
        transaction_html += "</div>";

        transaction_html += "</div>";

        rows_table = display_transfers(transaction,false);

        transaction_html += rows_table;
    } else
        transaction_html += "</div>\n";

    transaction_html += "</div>\n";
    return transaction_html
}



function show_eula() {
    let eula_agreed = get_cookie('eula_agreed')
    if (!eula_agreed)
    {
        let html ="<div id='overlay'></div><div id='eula' class='popup'>";
        html += "<h3>BEWARE! DISCLAIMITY DISCLAIMER!</h3>";
        html += "<p>So, we could've hired a lawyer and written a regular 74-page terms agreement and asked you to say you read it but we both know it's bullshit.</p>";
        html += "<p>Here's what you need to know:</p><p>We made this service to help you with your blockchain tax filing. It will NOT magically turn your transactions "+
        "into tax forms. We will attempt to classify and pick the correct tax treatment for your transfers; we also absolutely 100% guarantee that it's going to "+
        "occasionally be wrong. Sometimes we will have no idea what this or that transfer is. You MUST go over your transactions and inspect them. "+
        "We will give you tools to make corrections, and those tools are better than what anyone else currently offers. Even after you make corrections, due to bugs "+
        "and half-assed programming the tax filing may still end up wrong.</p>"+
        "<p>So, to recap. We don't guarantee correctness in any way. This warning isn't here just to cover our asses,"+
        " we really mean it! You promise not to blame us if the IRS comes a knocking. Oh, and also we'll stick some cookies in your browser. We good?</p>"
        html += "<div class='sim_buttons'>";
        html += "<div id='agree_eula'>We good. I'm not gonna blame you for wrong tax filing.</div>";
        html += "<div id='disagree_eula'>Nah, I'm outta here!</div>";
        html += "</div>";
        html += "</div>";
        $('#content').append(html);

        $('#agree_eula').on('click',function() {
            document.cookie = "eula_agreed=1;path=/;expires=Fri, 31 Dec 9999 23:59:59 GMT";
            $('#overlay').remove();
            $('#eula').remove();
        });

        $('#disagree_eula').on('click',function() {
            window.open("https://www.google.com/search?q=hire+a+programmer+to+do+my+blockchain+taxes")
        });
    }
}


function uid() {
    return Math.floor(Math.random() * Date.now())
}


$('body').on('click','#demo_link',function() {
    $('#main_form').append("<input type=hidden name=demo id='demo' value=1>");
    $('#your_address').val('0x032b7d93aeed91127baa55ad570d88fd2f15d589');
    $('#main_form').submit()
});

//main
$(function() {
    $( document ).ready(function() {
        show_eula();
        activate_clickables();
//            $('a#submit_address').click(function() {
        $('#main_form').submit( function(e) {
            console.log('main')
            e.preventDefault();
            primary = $('#your_address').val().trim();

            if ($('#demo').length || primary == '0x032b7d93aeed91127baa55ad570d88fd2f15d589')
                demo = true

            if (primary.length < 32 || primary.length > 44 || !isAlphaNumeric(primary)) {
                $('#your_address').after('<div class=address_selector_error>Not a valid address</div>');
                return
            }



            if (!demo)
                document.cookie = "address="+primary+";path=/;expires=Fri, 31 Dec 9999 23:59:59 GMT";
            prev_selection = null;
            uniqueid = uid();
            window.sessionStorage.setItem('address',primary);
            window.sessionStorage.setItem('uid',uniqueid);





            let import_addresses = ''
//            let display_addresses = '';

            if ($('#import_new_transactions').length) {
                if ($('#import_new_transactions').is(':checked')) {
                    import_addresses = 'all';
                }
            }


            ac_str = ''
            if ($('#aa').length) {
                import_addresses = $('#aa').val();
            } else {
                ac_ar = []
                boxes = $('#address_matrix').find('.ac_cb:checked').each(function() {
                    ac_ar.push($(this).attr('chain')+":"+$(this).attr('address'))
                });
                ac_str = ac_ar.join(',')
            }




            $('#demo').remove()
            $('#demo_link').remove()
            $('#initial_options').remove()
            $('#supported_chain_list').remove()

            $(document.body).css({'cursor' : 'wait'});

            let pb_mode = 'initial'
            if ($('#address_form').css('position') == 'absolute')
                pb_mode = 'middle'

            start_progress_bar(pb_mode)

            close_top_menu();
//            force_forget_derived = 0
//            if ($('#force_forget_derived').length)
//                force_forget_derived = $('#force_forget_derived').val()
            need_reproc(false)
            $.ajax({
                url:"process?address="+primary+"&uid="+uniqueid+"&import_addresses="+import_addresses+"&ac_str="+ac_str,

                success: function( js ) {
                    $('#address_form').css({'margin-top':'0px','padding-top':'0px','position':'fixed','left':'0','right':'0','top':'0','transform':'none',
                        'background-color':'#FAFAFA','border-bottom':'1px solid #DFDFDF','padding':'0px'});
                    $('#main_form').css({'margin':'auto','margin-top':'0px','padding':'5px','border-width':'0px','width':'50%'});
                    $('.header').remove();
                    $('.footer').remove();

                    try {
                        data = JSON.parse(js);
                    } catch (error) {
                        $('#content').html("<div class='main_error'>"+js+"<div id='error_discord'>Get help on <a id='discord_link' href='https://discord.gg/E7yuUZ3W4X' target='_blank'>Discord</a></div></div>");
                        stop_progress_bar();
                        return;
                    }


                    if (data.hasOwnProperty('error')) {
                        $('#content').html("<div class='main_error'>"+data['error']+"<div id='error_discord'>Get help on <a id='discord_link' href='https://discord.gg/E7yuUZ3W4X' target='_blank'>Discord</a></div></div>");
                        $(document.body).css({'cursor' : 'default'});
                        stop_progress_bar();
                        return;
                    }



                    console.log("received main data, no errors")

                    version = parseFloat(data['version']['software']);
                    data_version = parseFloat(data['version']['data']);

//                    address_info = data['address_info'];
                    all_address_info = data['all_address_info'];
                    fiat_info = data['fiat_info'];
                    chain_config = data['chain_config'];



//                            let chain_selector_el = $('#chain_selector').css({'margin':'10px','display':'block'}).detach()
                    make_top()




                    all_transactions = {}
                    transaction_order = []


                    len = data['transactions'].length;
                    setup();
                    if (len == 0) {
                        $('#content').html("</div><div id='transaction_list'><div id='top_text'>No transactions found on selected chains.</div></div>");
                    } else {
                        let info = data['info']
                        for (let field in params) {
                            if (field in info)
                                params[field] = parseInt(info[field])
                        }

                        for (let field in global_options) {
                            if (field in info)
                                global_options[field] = info[field]
                        }
                        fiat = global_options['fiat']

                        make_help_strings();

                        var all_html = "";
                        let non_fatal_errors = data['non_fatal_errors'];
                        for (let nfe of non_fatal_errors) {
                            all_html += "<div class='non_fatal_error'>"+nfe+"</div>";
                        }

                        all_html += "<div id='top_text'>Make sure to check red, orange, and yellow transactions."
                        if (demo) {
                            all_html += "<div id='demo_warning'>You are running a demo address that made real transactions on several blockchains. "+
                             "Permission to use this address was obtained from the owner. "+
                             "Anybody can modify stuff here and it will be saved. Feel free to play with it.</div>"
                        }

                        all_html+="</div>";
                        mark_all_deselected()
                        for (let idx in data['transactions']) {
                            let transaction = data['transactions'][idx];
                            txid = parseInt(transaction['txid'])
//                                    transaction['selected']=false;
                            map_lookups(transaction);
                            transaction['num'] = parseInt(idx)+1;
                            all_transactions[txid] = transaction;
//                                    transaction_html = make_transaction_html(transaction, idx, len);
                            transaction_order.push(txid)
//                                    all_html += transaction_html;
                        }
                        $('#content').html("</div><div id='transaction_list'>"+all_html+
                        "<div id='current_page'></div><div class='pagination'></div></div>");


//                                render_page(0);

                        display_tax_block();
                        process_tax_js(data);
                        show_inspections(data);
                        lookup_info['last_index'] = len;

//                                console.log('custom types?',data['custom_types']);
                        selection_operations(data['builtin_types'],data['custom_types']);
                        assist_block();
                        br_block();
                        $('#sel_opt_all').click();
//                        console.log("make_pagination in main")
                        make_pagination();
                        show_tax_related_html();
                    }

                    $(document.body).css({'cursor' : 'default'});
                },

                error: function ( js) {
                    $('#content').html("<div class='main_error'>A fatal server error has occurred<div id='error_discord'>Get help on <a id='discord_link' href='https://discord.gg/E7yuUZ3W4X' target='_blank'>Discord</a></div></div>");
                }
            })


        });
    });
});




function select_transaction(txel,keep_secondary=false) {
    t1 = performance.now();
    if (txel.hasClass('primary_selected')) {
        return;
    }

    t_id = txel.attr('id');
//    console.log('selected',t_id);
    let txid = parseInt(t_id.substr(2));
//    console.log("select_transaction",txid);
    let transaction = all_transactions[txid]

    if (transaction.hasOwnProperty('minimized') && transaction.minimized) {
        transaction.minimized = false
        transaction_html = make_transaction_html(transaction);
        txel.replaceWith(transaction_html)
        txel = $('#t_'+txid)
        transaction.minimized = true
    }
//    console.log('trdisp1',transaction);
    deselect_primary();
    prev_selection = txid;
//    console.log('prev_selection',prev_selection);
    if (!event.ctrlKey && !keep_secondary) {
        mark_all_deselected();
    }

    txel.addClass('primary_selected').addClass('secondary_selected');
    selected_transactions.add(txid)
//    transaction['selected'] = true;


    t2 = performance.now();
    el = txel.find('.select_similar');
//    console.log('trdisp2',transaction);
    txel.find('.tx_row_2').replaceWith(display_counterparty(transaction,true));
    txel.find('.transfers').replaceWith(display_transfers(transaction,true));
    showib(txel.find('.add_note'))
    if (el.length)
        show(el);
    else {
        var html = "<div class='select_similar'><div class='header'>Select transactions with the same:</div>";
        let lookups = {
            chain: ['chain','checked'],
            counterparty_name:['counterparty','checked'],
            signature:['operation','checked'],
            outbound_count:['number of sent transfers',''],
            inbound_count:['number of received transfers','']
        };
        for (let lookup in lookups) {
//            console.log('initial lookup',lookup);
            lookup_vals = lookup_info['transactions'][txid][lookup];
            if (lookup_vals.size > 0)
                html += "<label><input type=checkbox "+lookups[lookup][1]+" class='sim_"+lookup+"'>"+lookups[lookup][0]+"</label>";
        }

        let local_addresses = []


        let local_tokens = {}
        let local_nfts = {}
        let local_nft_list = []
        for (let trid in all_transactions[txid]['rows']) {
            let transfer = all_transactions[txid]['rows'][trid];
            let local_address = transfer['fr'];
            if (!local_addresses.includes(local_address))
                local_addresses.push(local_address)
            local_address = transfer['to'];
            if (local_address != 'network' && !local_addresses.includes(local_address))
                local_addresses.push(local_address)

            if (transfer['to'] != 'network') {
//                local_tokens[transfer['symbol']] = transfer['what'];
                let token_id = transfer['coingecko_id'];
                if (token_id == null)
                    token_id = transfer['what'];
                local_tokens[transfer['symbol']] = token_id;
            }
            if (transfer['token_nft_id'] != null) {
                let nft_html = display_token(transfer['symbol'],transfer['what'],transfer['token_nft_id'],copiable=false);
                local_nft_list.push(nft_html)
                local_nfts[nft_html] = transfer['what']+"_"+transfer['token_nft_id'];
            }
        }
//        console.log(local_addresses)

        for (let local_address of local_addresses) {
            if (displayed_addresses.length == 1 && displayed_addresses[0] == local_address)
                continue
            html += "<label><input type=checkbox class='sim_address' address='"+local_address+"'>address:"+display_hash(local_address,'address',false)+"</label>";
        }

        for (let token_symbol in local_tokens)
            html += "<label><input type=checkbox class='sim_token_id' token_id='"+local_tokens[token_symbol]+"'>token:"+token_symbol+"</label>";

        local_nft_list.sort()
        for (let token_nft_symbol of local_nft_list) {
            html += "<label><input type=checkbox class='sim_token_nft' token_nft_address='"+local_nfts[token_nft_symbol]+"'>NFT:"+token_nft_symbol+"</label>";
        }

        html += "<div class='sim_buttons'><div class='select_similar_button'></div>";
        html += "<div class='undo_changes'>Undo custom changes</div>"
        html += "<div class='deselect_this_button'>Deselect this transaction</div>"
        if (txel.hasClass('manual')) {
            html += "<div class='mt_edit'>Edit this transaction</div>"
            html += "<div class='mt_delete_popup'>Delete this transaction</div>"
        }
        html += "</div>";
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
//    console.log('perf',t2-t1,t3-t2,t4-t3,t5-t4,'total',t5-t1);
}

function activate_clickables() {
//    col = document.getElementsByClassName('transaction');
//    $(col).on('click',function(event) {

    $('body').on('click','div.transaction',function(event) {
//    $('body').on('click','div#t_3, div#t_2',function(event) {
//      $('#t_3,#t_2').on('click',function(event) {
//    $('div.transaction').on('click',function(event) {
//        console.log('!');
        el = $(this);
        select_transaction(el, keep_secondary=false);
    });

    $('body').on('click','.select_similar input',function() {
        txid = parseInt($(this).closest('.transaction').attr('id').substr(2));
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
        id = parseInt(t_id.substr(2));
        cnt = 0;

        mark_all_deselected()
        for (let o_id of o_ids) {
            o_id = parseInt(o_id)
//            $('#t_'+o_id).addClass('secondary_selected');
            selected_transactions.add(o_id)
//            all_transactions[oid]['selected']=true
            cnt += 1;
        }
        $('#sel_opt_sel').click();
        update_selections_block();
//        $(this).html("Deselect "+cnt+" additional transactions").removeClass('select_similar_button').addClass('deselect_similar_button');
    });

    $('body').on('click','.cp',function() {
        if ($(this).find('input').length == 0) {
            let progenitor = $(this).attr('progenitor');
            let cp_name_el = $(this)
            let current_name = cp_name_el.text()


            txid = parseInt($(this).closest('.transaction').attr('id').substr(2));
            let chain = all_transactions[txid]['chain']
            matches = lookup_info["counterparty_mapping"][progenitor];
//            count = progenitor_counts[progenitor];
            count = matches.size;
            ac_html = "<form class='ac_wrapper'><input type=text class='cp_enter' value='"+current_name+"'><input type=submit class='apply_custom_cp' value='Apply to "+count+" transactions'/><button class='cancel_custom_cp'>Cancel</button><form>";

            cp_name_el.empty()
            cp_name_el.append(ac_html);
//            cp_name_el.find('.autocomplete_list').autocomplete({source: counterparty_list});
//            cp_name_el.find('.autocomplete_list').focus()

//            $(document).on('keyup', function(e) {
//              if (e.key == "Escape") $('.cancel').click();
//            });

            $('.cp_enter').focus();

            cp_name_el.find('.cancel_custom_cp').on('click',function(e) {
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
//                    address = window.sessionStorage.getItem('address');
//                    chain = window.sessionStorage.getItem('chain');
                    $.get("update_progenitors?chain="+chain+"&user="+primary+"&progenitor="+progenitor+"&counterparty="+cp, function(js) {
                        var data = JSON.parse(js);
//                        console.log(data);
                        if (data.hasOwnProperty('error')) {
                            $(this).append("<div class='err_mes'>"+data['error']+"</div>");
                        } else {
//                            console.log("match len",matches.size,"prog",progenitor);
                            for (o_txid of matches) {

//                                console.log("o_txid",o_txid);
                                transaction = all_transactions[o_txid];
                                transaction_counterparties = transaction['counter_parties'];
//                                console.log(transaction_counterparties)
                                for (let o_progenitor in transaction_counterparties) {
                                    if (o_progenitor == progenitor)
                                        transaction_counterparties[progenitor][0] = cp;
                                }
//                                console.log("remove_from_mapping",current_name,o_txid);
                                if (current_name == 'unknown')
                                    current_name = progenitor
                                remove_from_mapping('counterparty_name',current_name,o_txid);
//                                console.log("add_to_mapping",cp,o_txid);
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



    $('body').on('click','.add_note',function() {
        let txel = $(this).closest('.transaction');
        let txid = parseInt(txel.attr('id').substr(2));
        let note = '';
        if ('custom_note' in all_transactions[txid]) {
            note = all_transactions[txid]['custom_note'];
        }

        let html = "<div id='add_note_hold'><textarea class='note_area' placeholder='You can use HTML here'>"+note+"</textarea>";
        html += "<div class='note_buttons'><button class='button_save_note'>Save note</button>"
        if (note.length > 0)
            html += "<button class='button_delete_note'>Delete note</button>";
        html += "<button class='button_cancel_note'>Cancel</button></div></div>";
        $(this).after(html);
        hide($(this))
        hide(txel.find('.custom_note'))
    });

    $('body').on('click','.button_cancel_note',function() {
        let txel = $(this).closest('.transaction');
        showib(txel.find('.add_note'));
        $('#add_note_hold').remove()
        show(txel.find('.custom_note'))
    });

    $('body').on('click','.button_delete_note',function() {
        let txel = $(this).closest('.transaction');
        let txid = parseInt(txel.attr('id').substr(2));
        let clicker_el = txel.find('.add_note')

        if ('custom_note' in all_transactions[txid]) {
            let data = 'note='
            $.post("save_note?address="+primary+"&transaction="+txid, data, function(resp) {
                var data = JSON.parse(resp);
                if (data.hasOwnProperty('error')) {
                    $('#add_note_hold').append("<div class='err_mes'>"+data['error']+"</div>");
                } else {
                   delete all_transactions[txid]['custom_note'];
                    $(txel).find('.custom_note').html('');

                    clicker_el.html('Add a note')
                    showib(clicker_el);

                    $('#add_note_hold').remove()
                    show(txel.find('.custom_note'))
                }
            });

        }


    });

    $('body').on('click','.button_save_note',function() {
        let txel = $(this).closest('.transaction');
        let txid = parseInt(txel.attr('id').substr(2));
        let note = $('.note_area').val();
        let clicker_el = txel.find('.add_note')
        let data = 'note='+encodeURIComponent(note)

        $.post("save_note?address="+primary+"&transaction="+txid, data, function(resp) {
            var data = JSON.parse(resp);
            if (data.hasOwnProperty('error')) {
                $('#add_note_hold').append("<div class='err_mes'>"+data['error']+"</div>");
            } else {
                $(txel).find('.custom_note').html(note);
                if (note.length > 0) {
                    all_transactions[txid]['custom_note'] = note;
                    $(clicker_el).html('Edit note')
                } else {
                    delete all_transactions[txid]['custom_note']
                    $(clicker_el).html('Add note')
                }
                showib(clicker_el);
                $('#add_note_hold').remove()
                show(txel.find('.custom_note'))
            }
        });

    });



    $('body').on('change','.treatment, .row_rate, .row_vaultid',function() {
        el = $(this);
//        td = $(this).closest('td')
//        td.addClass('custom');
        txel = $(this).closest('.transaction');
        t_id = txel.attr('id');
        txid = parseInt(t_id.substr(2));
        this_row = $(this).closest('tr');
        trid = this_row.attr('id');
        val = $(this).val();

        if ($(this).hasClass('treatment')) {
            prop = 'treatment'
        } else if ($(this).hasClass('row_rate')) {
            prop = 'rate'
        } else if ($(this).hasClass('row_vaultid')) {
            prop = 'vault_id'
        }

        multiple = change_multiple(txid, trid, prop, val)


        if (!multiple)
            save_custom_val(txid,trid,prop,val)



    });

    $('body').on('click','.undo_changes',function() {
        el = $(this);
        txel = $(this).closest('.transaction');
        t_id = txel.attr('id');
        txid = parseInt(t_id.substr(2));
//        console.log("undo changes",txid);
        data = 'transaction='+txid;
//        let address = window.sessionStorage.getItem('address');
        $.post("undo_custom_changes?address="+primary, data, function(resp) {
//            console.log(resp);
            var data = JSON.parse(resp);
            if (data.hasOwnProperty('error')) {
                txel.append("<div class='err_mes'>"+data['error']+"</div>");
            } else {
                show_ajax_transactions(data)
                need_recalc();
                need_reproc();
            }
        });
    });

}

function change_multiple(txid, trid, prop, new_val) {
    let transfer = find_transfer(txid,trid)
    let token = transfer['what'];
    let symbol = transfer['symbol'];
    let fr = transfer['fr'];
    let to = transfer['to'];
    let similar_transfers = [];
    for (let other_trid in all_transactions[txid]['rows']) {
        let other_transfer = all_transactions[txid]['rows'][other_trid];

        if (other_trid != trid) {
            let other_token = other_transfer['what'];
            let other_fr = other_transfer['fr'];
            let other_to = other_transfer['to'];
//            console.log("sim check",tr_idx,other_transfer['index'],prop,token,other_token,fr,other_fr,to,other_to)
            if (prop == 'rate' && token == other_token && other_to != null) {
                similar_transfers.push(other_trid)
            }

            else if (token == other_token && other_fr == fr && other_to == to) {
                similar_transfers.push(other_trid)
            }
        }
    }
//    console.log("sim len",similar_transfers.length);

    if (similar_transfers.length > 0) {
        let prop_map = {'rate':'rate','treatment':'tax treatment','vault_id':'Vault/loan ID'}
        let html ="<div id='overlay'></div><div id='popup' class='popup'><form id='change_val_multi'>"
        let plural = "";
        if (similar_transfers.length > 1)
            plural = "s";
        html += "<input type=hidden id=cm_txid value="+txid+"> ";
        html += "<input type=hidden id=cm_trid value="+trid+"> ";
        html += "<input type=hidden id=cm_trid_str value='"+similar_transfers.join(',')+','+trid+"'> ";
        html += "<input type=hidden id=cm_prop value="+prop+"> ";
        html += "<input type=hidden id=cm_val value='"+new_val+"'> ";
        if (prop == 'rate')
            html += "Also change rate for "+similar_transfers.length+" other "+symbol+" transfer"+plural+" in this transaction?";
        else
            html += "Also change "+prop_map[prop]+" for "+similar_transfers.length+" similar other transfer"+plural+" in this transaction?";
        html += "<div class='sim_buttons'>";
        html += "<div id='change_val_multi_confirm'>Yes</div>";
        html += "<div id='change_val_multi_deny'>No, just this one</div></div>";
        html += "</form></div>";
        $('#content').append(html);
        return true;
    }
    return false;
}

$('body').on('click','#change_val_multi_confirm', function() {
    save_custom_val($('#cm_txid').val(),$('#cm_trid_str').val(),$('#cm_prop').val(),$('#cm_val').val())
    $('#overlay').remove();
    $('#popup').remove();
});

$('body').on('click','#change_val_multi_deny', function () {
    save_custom_val($('#cm_txid').val(),$('#cm_trid').val(),$('#cm_prop').val(),$('#cm_val').val())
    $('#overlay').remove();
    $('#popup').remove();
});


function save_custom_val(txid,trid_str,prop,value) {
    let data = 'transaction='+txid+"&transfer_id="+trid_str+"&prop="+prop+"&val="+value;
    let prop_map = {'rate':'row_rate','treatment':'treatment','vault_id':'row_vaultid'}

//    let address = window.sessionStorage.getItem('address');
    $.post("save_custom_val?address="+primary, data, function(resp) {
//        console.log(resp);
        var data = JSON.parse(resp);
        if (data.hasOwnProperty('error')) {
            $('#t_'+txid).append("<div class='err_mes'>"+data['error']+"</div>");
        } else {
            let trid_list = trid_str.split(",")
            for (let trid of trid_list) {
                set_transfer_val(txid, trid, prop, "custom:"+value);
            }
            let transfers_html = display_transfers(all_transactions[txid],true);
//            console.log('transfers_html',transfers_html);
            $('#t_'+txid).find('.transfers').replaceWith(transfers_html);
            need_recalc();
            showib(txel.find('.undo_changes'));
            if (prop == 'rate')
                need_reproc()
        }
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
//    console.log("txid "+txid);
    jqid = '#t_'+txid;
    tfst1 = performance.now();

//    all = [...Array(lookup_info['last_index']).keys()];
    all = null;
    $(jqid).find('input:checked').each(function() {
        tin1 = performance.now();
        lookup = $(this).attr('class').substr(4);
        if (lookup == 'address')
            lookup_vals = [$(this).attr('address')];
        else if (lookup == 'token_id')
            lookup_vals = [$(this).attr('token_id')];
        else if (lookup == 'token_nft')
            lookup_vals = [$(this).attr('token_nft_address')];
        else {
            lookup_vals = lookup_info['transactions'][txid][lookup];

            if (lookup_vals.size == 0) {
                all = new Set();
                $(jqid).find('.current_sims').val('');
                return;
            }
        }

//        console.log('lookup',lookup,'vals',lookup_vals);

        single_lookup_set = new Set();
        for (let lookup_val of lookup_vals) {
//            all = set_intersect(all,lookup_info[lookup+"_mapping"][lookup_val]);
//            console.log('lookup',lookup,'lookup_val',lookup_val,lookup_info[lookup+"_mapping"][lookup_val].size,all.size)
            single_lookup_set = set_union(single_lookup_set,lookup_info[lookup+"_mapping"][lookup_val])
//            console.log('lookup',lookup,'lookup_val',lookup_val,lookup_info[lookup+"_mapping"][lookup_val].size)
        }
        all = set_intersect(all,single_lookup_set);
        tin2 = performance.now();
//        console.log('fst inner',(tin2-tin1));
    });
    tfst2 = performance.now();
    if (all == null) {
        hide($(jqid).find('.select_similar_button'));
        $(jqid).find('.current_sims').val('');
        return all
    } else showib($(jqid).find('.select_similar_button'));

    cnt = all.size - 1;
//    console.log("find_similar_transactions",jqid,cnt);
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
//    console.log('fst all',tfst2-tfst1,tfst3-tfst2,tfst4-tfst3,tfst5-tfst4);
    return all
}

function deselect_primary(and_secondary=false) {
    if (prev_selection != null) {
        let jqid = '#t_'+prev_selection;
        let transaction = all_transactions[prev_selection];
        let txel = $(jqid)
        if (transaction.hasOwnProperty('minimized') && transaction.minimized) {
            transaction_html = make_transaction_html(transaction);
            txel.replaceWith(transaction_html)
            txel = $(jqid)
            if (!and_secondary)
                txel.addClass('secondary_selected');
            else
                selected_transactions.delete(prev_selection)
        } else {
            txel.removeClass('primary_selected');
            if (and_secondary) {
                txel.removeClass('secondary_selected');
                selected_transactions.delete(prev_selection)
            }
            txel.find('.tx_row_2').replaceWith(display_counterparty(all_transactions[prev_selection]));
            txel.find('.transfers').replaceWith(display_transfers(all_transactions[prev_selection]));
    //        $(jqid).find('.button_cancel_note').click()

            $('#add_note_hold').remove()
            show(txel.find('.custom_note'))
            hide(txel.find('.add_note'))
            hide(txel.find('.select_similar'));
            hide(txel.find('.save_changes'));
        }
        prev_selection = null;
    }
}




function selection_operations(builtin_types,custom_types) {
    let high_impact_amount = params['high_impact_amount'];
    var html = "<div id='operations_block'>";
    html += "<div id='selections_placeholder'>Nothing selected. Click a transaction to select it. CTRL+click to select multiple.</div>";

    html += "<div id='scroll_block'><div class='header'>Scroll to:</div>";
    html += "<div class='scroll_row'>Bottom/Top <a id='scr_bottom' class='next_ic'></a><a id='scr_top' class='prev_ic'></a></div>";
    html += "<div class='scroll_row'>Selected <a id='scr_selected_next' class='next_ic'></a><a id='scr_selected_prev' class='prev_ic'></a></div>";
//    html += "<div class='scroll_row'>Unknown <a id='scr_unknown_next'>Next</a><a id='scr_unknown_prev'>Previous</a></div>";
    html += "<div class='scroll_row'>High impact (<a id='hi_conf'>"+print_fiat(high_impact_amount)+"+</a>) <a id='scr_hi_next' class='next_ic'></a><a id='scr_hi_prev' class='prev_ic'></a></div>";
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
//    address = window.sessionStorage.getItem('address');
//    chain = window.sessionStorage.getItem('chain');
    html +="<a id='mt_create'>Manually add a transaction</a>";
//    html +="<a href='download?address="+address+"&chain="+chain+"&type=transactions_json' id='download_transactions_json'>Download all transactions (json)</a>";

    html += "</div>";

    html +="</div>";
//    $('body').append(html);
    $('#content').append(html);

    $('#deselect_all').on('click',function() {
        deselect_primary();
        mark_all_deselected();
        update_selections_block();
    });
    update_selections_block();

    $('#scroll_block a').on('click', function() {
        let high_impact_amount = params['high_impact_amount']
        which = $(this).attr('id');

        var bottom_of_screen = $(window).scrollTop() + window.innerHeight;
        var top_of_screen = $(window).scrollTop();
        if (which.includes('top')) {
            window.scrollTo(0,0);
            return;
        }

        if (which.includes('bottom')) {
            window.scrollTo(0,document.body.scrollHeight);
            return;
        }


        var mid_screen = (top_of_screen+bottom_of_screen)/2;
        let txels = $('.transaction')
        let current_idx = null
        $('.transaction').each(function() {
            let el = $(this)
            let bottom_of_element = el.offset().top + el.outerHeight();
            if (bottom_of_element > mid_screen) {
                let txid = parseInt(el.attr('id').substr(2));
//                console.log('tx in mid', txid)
                current_idx = visible_order.indexOf(txid);
                console.log('scroll current_idx',current_idx)
                return false
            }
        });

        let search_selected = which.includes('selected')
        let search_hi = which.includes('_hi_')
        let minimum_color = 0
        if (which.includes('red'))
            minimum_color = 0
        else if (which.includes('orange'))
            minimum_color = 3
        else if (which.includes('yellow'))
            minimum_color = 5

        let collection = null
        if (which.includes('_next')) {
            collection = visible_order.slice(current_idx+1)
        } else {
            collection = visible_order.slice(0,current_idx)
            collection.reverse()
        }

        for (let o_txid of collection) {
            if (search_selected) {
                if (selected_transactions.has(o_txid)) {
                    go_to_transaction(o_txid)
                    return
                }
            } else if (search_hi) {
                let transaction = all_transactions[o_txid]
                if (transaction.impact > high_impact_amount) {
                    go_to_transaction(o_txid)
                    return
                }
            }

            else {
                let transaction = all_transactions[o_txid]
//                let color = transaction['classification_certainty']
                let color = transaction['original_color']
                if ('custom_color_id' in transaction)
                    color = transaction['custom_color_id']
                if (color <= minimum_color) {
                    go_to_transaction(o_txid)
                    return
                }
            }

        }


    });

    $('#sel_opt_all').on('click',function() {
        display_mode = 'all'
        t1 = performance.now();
        if ($('#sel_opt_all').hasClass('sel_opt_chosen'))
            return;
//        $(document.body).css({'cursor' : 'wait'});
        $('.sel_opt_chosen').removeClass('sel_opt_chosen');
        $(this).addClass('sel_opt_chosen');
        t2 = performance.now();
//        showib($('.transaction'));
        t3 = performance.now();

        make_pagination(saved_page_idx)
        if (scroll_position != null) {
            window.scrollTo(0,scroll_position);
        }
        t4 = performance.now();
//        console.log("Timing sel_opt_all",t2-t1,t3-t2,t4-t3)

//        $(document.body).css({'cursor' : 'default'});
    });

    $('#sel_opt_sel').on('click',function() {
        display_mode = 'selected'
        t1 = performance.now();
        if ($('#sel_opt_all').hasClass('sel_opt_chosen')) {
            scroll_position = window.scrollY;
            saved_page_idx = current_page_idx;
        }
//        $(document.body).css({'cursor' : 'wait'});
        $('.sel_opt_chosen').removeClass('sel_opt_chosen');
        $(this).addClass('sel_opt_chosen');
        t2 = performance.now();
//        showib($('.secondary_selected'));
        t3 = performance.now();
//        hide($('.transaction:not(.secondary_selected)'));
        t4 = performance.now();
//        console.log("Timing sel_opt_sel",t2-t1,t3-t2,t4-t3)
        make_pagination()
//        $(document.body).css({'cursor' : 'default'});
    });

    $('#sel_opt_desel').on('click',function() {
        display_mode = 'deselected'
//        $(document.body).css({'cursor' : 'wait'});
        $('.sel_opt_chosen').removeClass('sel_opt_chosen');
        $(this).addClass('sel_opt_chosen');
//        hide($('.secondary_selected'));
//        showib($('.transaction:not(.secondary_selected)'));
//        $(document.body).css({'cursor' : 'default'});
        make_pagination()
    });

}



function update_selections_block() {
//    cnt = $('#transaction_list').find('div.secondary_selected').length;
//    cnt = $('.secondary_selected').length;
    let selected_cnt = selected_transactions.size
//    console.log('update_selections_block',selected_cnt)
    if (selected_cnt > 0) {
        $('#selections_count').children('span').html(selected_cnt);
        show($('#selections_count'));
        hide($('#selections_placeholder'));
        show($('#scr_selected_next').closest('.scroll_row'));
        $('#custom_types_list .ct_name').addClass('applicable').attr('title','Apply to selected transactions');
        $('.secondary_selected').removeClass('secondary_selected')
        mark_selected();
    } else {
        hide($('#selections_count'));
        show($('#selections_placeholder'));
        hide($('#scr_selected_next').closest('.scroll_row'));
        $('#custom_types_list .ct_name').removeClass('applicable').removeAttr('title');
    }
}


function scroll_to(el) {
    if (!el.is(':visible'))
        showib(el)
    let elem_position = el.offset().top;
    let elem_height = el.outerHeight();
    let window_height = window.innerHeight;
    let y = elem_position - window_height/2+elem_height/2;
    window.scrollTo(0,y);

}





$('body').on('click','.colopt',function() {
   color_id = $(this).attr('id').substr(7);
   txids = [];
   if (selected_transactions.size == 0)
        return;
//   console.log("recolor",color_id,"to transactions",txids);
   data = 'color_id='+color_id+'&transactions='+Array.from(selected_transactions).join(',');
//   let address = window.sessionStorage.getItem('address');
   $.post("recolor?address="+primary, data, function(resp) {
//        console.log(resp);
        var data = JSON.parse(resp);
        if (data.hasOwnProperty('error')) {
            $('#recolor_block').append("<div class='err_mes'>"+data['error']+"</div>");
        } else {
            for (let txid of selected_transactions) {
                all_transactions[txid]['custom_color_id'] = color_id;
                $('#t_'+txid).removeClass('custom_recolor_0 custom_recolor_3 custom_recolor_5 custom_recolor_10').addClass('custom_recolor custom_recolor_'+color_id);
            }

        }
    });
});

$('body').on('click','#color_undo',function() {
   txids = [];
   if (selected_transactions.size == 0)
        return;
//   console.log("recolor",'undo',"to transactions",txids);
   data = 'color_id=undo&transactions='+Array.from(selected_transactions).join(',');
//   let address = window.sessionStorage.getItem('address');
   $.post("recolor?address="+primary, data, function(resp) {
//        console.log(resp);
        var data = JSON.parse(resp);
        if (data.hasOwnProperty('error')) {
            $('#recolor_block').append("<div class='err_mes'>"+data['error']+"</div>");
        } else {
            for (let txid of selected_transactions) {
                delete all_transactions[txid]['custom_color_id'];
                $('#t_'+txid).removeClass('custom_recolor_0 custom_recolor_3 custom_recolor_5 custom_recolor_10');
            }
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

function assist_block() {
    let html = "<div id='assist_block'>"
    html += "<div class='assist_line'><div id='help_main'>HELP!</div></div>"
    html += "<div class='assist_line'><div id='social'>"
    html += "<a id='discord_link' href='https://discord.gg/E7yuUZ3W4X' target='_blank'>Discord</a>"
    html += "<a id='twitter_link' href='https://twitter.com/defitaxes' target='_blank'>Twitter</a>"
    html += "</div></div>"
    html += "<div class='assist_line'><div id='attribution'>Powered by <a href='chains.html' target=_blank>Etherscan and Blockscout scanners</a></div></div>"
    html += "</div>";
    $('#content').append(html);
}

function br_block() {
    let html = "<div id='br_block'>"
    html += "<div class='br_line'>Send me some $ (ETH, Polygon, BSC, Avalanche):<div id='donation_address' class='copiable' full='0xbf01E689Dd71206A47186f204afBf3b8e7bB8114' title='Copy donation address to clipboard'>0xbf01E689Dd71206A47186f204afBf3b8e7bB8114</div></div>"
    html += "</div>";
    $('#content').append(html);
}


function need_reproc(display=true, level=1, text=null) {
    if (show && $('#need_reproc').length == 0) {
        if (text == null)
            text = "Reprocessing recommended"
        $('#main_form').append("<div id='need_reproc' class='reproc_level_"+level+"'>"+text+"</div>");
        $('#main_form').addClass('main_form_outdated');
        show($('#submit_address'));
    }

    if (!display) {
        $('#need_reproc').remove();
        $('#main_form').removeClass('main_form_outdated');
        hide($('#submit_address'))
    }

//    $('#force_forget_derived').val(+display)
}

function isInViewport(element) {
    const rect = element.getBoundingClientRect();
    return (
        rect.top >= 0 &&
        rect.left >= 0 &&
        rect.bottom <= (window.innerHeight || document.documentElement.clientHeight) &&
        rect.right <= (window.innerWidth || document.documentElement.clientWidth)
    );
}

function dict_len(dct) {
    return Object.keys(dct).length
}

//function get_cookie(name) {
//    return document.cookie.split('; ').find(row => row.startsWith(name))
//}

function get_cookie(name) {
  const value = `; ${document.cookie}`;
  const parts = value.split(`; ${name}=`);
  if (parts.length === 2) return parseInt(parts.pop().split(';').shift());
  return 0
}

function mark_selected() {
    for (let txid of selected_transactions) {
        $('#t_'+txid).addClass('secondary_selected')
    }
}

function mark_all_deselected() {
    $('.secondary_selected').removeClass('secondary_selected');
    selected_transactions = new Set();
}



function render_page(page_idx) {
    let tx_per_page = params['tx_per_page']
    current_page_idx = page_idx
    console.log('render page',page_idx)
    $('#current_page').html('')
    let len = visible_order.length
    let start_idx = tx_per_page*page_idx
    let end_idx = start_idx + tx_per_page
    if (end_idx > len)
        end_idx = len

    let page_html = "";
//    console.log(start_idx,end_idx)
//    console.log('tx indexes',start_idx,end_idx)
    for (let idx = start_idx; idx < end_idx; idx++) {
        let txid = visible_order[idx];
//        console.log(txid)
        let transaction = all_transactions[txid]
        transaction_html = make_transaction_html(transaction);
        page_html += transaction_html;
    }

    $('#current_page').html(page_html);
    process_tax_errors()
    mark_selected();
    highlight_impact();

}

function make_pagination(go_to_page=null,init_render=true) {
    let tx_per_page = params['tx_per_page']
    console.log("make pagination")
    visible_order = transaction_order
    if (display_mode != 'all') {
        visible_order = []
        if (display_mode == 'selected') {
            for (let txid of transaction_order) {
                if (selected_transactions.has(txid))
                    visible_order.push(txid)
            }
        }

        if (display_mode == 'deselected') {
            for (let txid of transaction_order) {
                if (!selected_transactions.has(txid))
                    visible_order.push(txid)
            }
        }
    }
    let len = visible_order.length
    let total_pages = Math.ceil(len/tx_per_page)
//    if (total_pages > 1)
    {
        let pagination_html = ""
        if (total_pages > 1)
            pagination_html += "<div class='pagination_header'>Go to page</div><div class='pagination_list'></div>"
        let tx_per_page_options = [100,250,500,1000]
        pagination_html += "<div class='pagination_header'>Transactions per page</div><select class='pagination_per_page'>";
        for (let tx_per_page_option of tx_per_page_options) {
            pagination_html += "<option value='"+tx_per_page_option+"'"
            if (tx_per_page_option == tx_per_page)
                pagination_html += " selected "
            pagination_html += ">"+tx_per_page_option+"</option>"
        }
        pagination_html += "</select>";

        $('.pagination').html(pagination_html)

        if (total_pages > 1)
        {
            let pageNumber = 1;
            if (go_to_page != null)
                pageNumber = go_to_page+1;
            $('.pagination_list').pagination({
                pageSize:tx_per_page,
                dataSource: visible_order,
                pageNumber:pageNumber,
                triggerPagingOnInit: init_render,
                callback: function(data, pagination) {

                    let page_idx = pagination.pageNumber - 1;
//                    console.log('callback',page_idx);
                    console.log("callback render")
                    render_page(page_idx)
                }
            })
        } else {
            console.log("def render")
            render_page(0)
        }
    }


}

function save_info(field,value) {
    $.get("save_info?address="+primary+"&field="+field+"&value="+value);
    params[field] = value
}

$('body').on('change','.pagination_per_page', function() {
    let tx_per_page = parseInt($(this).val());
    save_info('tx_per_page',tx_per_page)
    make_pagination()
});

function go_to_transaction(txid) {
    let tx_per_page = params['tx_per_page']
    console.log('go_to_transaction txid',txid)
    if (isNaN(txid))
        return
    el = $('#t_'+txid)
    if (el.length > 0) {
        scroll_to(el);
        return;
    }

    let visible_idx = visible_order.indexOf(txid)
    if (visible_idx == -1) {
        $('#sel_opt_all').click()
        go_to_transaction(txid)
        return
    }

    let page_idx = Math.floor(visible_idx/tx_per_page)
    console.log('go_to_transaction page_idx',page_idx,current_page_idx)
    $('.pagination_list').pagination('go',page_idx+1)
    el = $('#t_'+txid)
    scroll_to(el);
}


function close_top_menu() {
    hide($('#top_menu'))
    $('#top_menu_icon').removeClass('top_menu_icon_open').addClass('top_menu_icon_closed')
}

$('body').on('click','#top_menu_icon',function() {
    if ($(this).hasClass('top_menu_icon_closed')) {
        show($('#top_menu'))
        $(this).removeClass('top_menu_icon_closed').addClass('top_menu_icon_open')
    } else {
        close_top_menu();
    }
});


function get_symbol(token_id,chain_name=null) {
//    console.log('get_symbol',token_id,chain_name)
//    if (!token_id.includes(":")) {
//        if ((token_id.length >= 42 || token_id == '-') && chain_name == 'Solana')
//            token_id = chain_name+":"+token_id
//        else if (token_id.length >= 42 && token_id[1] == 'x' && token_id[0] == '0' && chain_name != null)
//            token_id = chain_name+":"+token_id
//    }
    let token_entry = null
    if (token_id in tokens)
        token_entry = tokens[token_id];
    else if (chain_name != null) {
        token_id = chain_name +":"+token_id;
        token_entry = tokens[token_id];
    } /*else return "-"*/
//    console.log('get_symbol 2',token_id, chain_name, token_entry)
    if (chain_name != null && chain_name in token_entry['symbols'])
        return token_entry['symbols'][chain_name][0]
    else return token_entry['default_symbol']
}

$('body').on('click','#aa_clicker',function() {
    let html ="<div id='overlay'></div><div id='aa_popup' class='popup'>";
    if (demo) html += "<div>You can't add another address in a demo, sorry.</div>";
    else {
    html += "<input type=text placeholder='Ethereum or Solana address' id='aa_input'><div id='aa_process'>Import transactions</div>";
    }
    html += "<div id='aa_cancel'>Cancel</div></div>";
    $('#content').append(html);
});

$('body').on('click','#aa_cancel, #hi_cancel, #dc_fix_cancel, #dl_cancel, #up_cancel, #cg_cancel, #opt_cancel',function() {
    $('.popup').remove();
    $('#overlay').remove();
});

$('body').on('click','#aa_process',function() {
    let aa = $('#aa_input').val().trim();
//    aa = aa.toLowerCase()

    $('#aa_popup .error').remove();

    if (aa.length < 32 || aa.length > 44 || !aa.match(/^[0-9a-z]+$/i)) {
        $('#aa_input').after("<div class='error'>Not a valid address</div>");
        return
    }

    if (aa.toLowerCase() in all_address_info) {
        $('#aa_input').after("<div class='error'>Already imported</div>");
        return
    }

    $('.popup').remove();
    $('#overlay').remove();
    $('#main_form').append("<input type=hidden id='aa' value='"+aa+"'>");
    $('#main_form').submit();


});




$('body').on('click','#hi_conf',function() {
    let html ="<div id='overlay'></div><div id='hi_popup' class='popup'>";
    html += "<div class='hi_popup_explanation'>Transaction is considered high impact if it results in at least this much "+fiat+" gain or loss:</div>";
    html += "<input type=text value='"+params['high_impact_amount']+"' id='hi_input'>";
    html += "<div id='hi_update'>Update</div><div id='hi_cancel'>Cancel</div></div>";
    $('#content').append(html);
});

$('body').on('click','#hi_update',function() {
    let hi_val = $('#hi_input').val();

    $('#hi_popup .error').remove();

    if (!isNumeric(hi_val) || hi_val <= 0) {
        $('#hi_input').after("<div class='error'>Should be a positive number</div>");
        return
    }
    let high_impact_amount = Math.round(hi_val);
    save_info('high_impact_amount',high_impact_amount)

//    $.get("save_info?address="+primary+"&field=high_impact_amount&value="+high_impact_amount);

    $('#hi_conf').html('$'+high_impact_amount+'+');
    highlight_impact();

    $('.popup').remove();
    $('#overlay').remove();

});

function address_matrix_html() {
    let addresses = new Set()
    used_chains = new Set()
    for (let address in all_address_info) {
        if (address == 'my account')
            continue
        for (let chain in all_address_info[address]) {
            let addr_dict = all_address_info[address][chain]
            if (addr_dict['present']) {
                addresses.add(address)
                used_chains.add(chain)
            }
        }
    }
    console.log('addresses for matrix',addresses,addresses.size)
    console.log('chains for matrix',used_chains,used_chains.size)


    html = "<div id='address_matrix'>"
    if (used_chains.size == 1) {
        let chain = Array.from(used_chains)[0];
        ordered_chains = [chain]
        if (dict_len(all_address_info) > 1) {
            html += "<ul id='display_address_selector'>"
            for (let address in all_address_info) {
                if (address == 'my account')
                    continue
                let addr_dict = all_address_info[address][chain]
                let used = addr_dict['used']
                let tx_count = 0
                if ('tx_count' in addr_dict)
                    tx_count = addr_dict['tx_count']
                html += "<li><label><input type=checkbox class=ac_cb id="+address+"_displayed address="+address+" chain='"+chain+"'"
                if (used)
                    html += " checked "
                html += "><div class='display_address_option'>"+address+"</div></label> ("+tx_count+" transactions)"
                if (address.toLowerCase() != primary.toLowerCase()) {
                    html += "<div class='delete_address' addr='"+address+"' title='Delete this address'></div>"
                }
                html += "</li>"
            }
            html += "</ul>"
        }
    } else {
        html += "<table id='address_matrix_left'>"
        html += "<tr class='chain_names_row'><td></td><td class='all_chains_column'>All chains</td></tr>"
        if (addresses.size > 1)
            html += "<tr row_addr=all class='all_addresses_row'><td class='addresses_column'>All addresses</td><td class='all_chains_column'><input type=checkbox id='toggle_all'></td></tr>"
        for (let address in all_address_info) {
            if (address == 'my account')
                continue
            html += "<tr row_addr="+address+"><td class='addresses_column'>"+display_hash(address, name='address', copiable=true, replace_users_address=false)
            if (address.toLowerCase() != primary.toLowerCase()) {
                html += "<div class='delete_address' addr='"+address+"' title='Delete this address'></div>"
            }
            html += "</td>"
            html += "<td class='all_chains_column'><input type=checkbox class='toggle_all_chains'></td></tr>"
        }
        html += "</table>"
        html += "<table id='address_matrix_right'>"
        ordered_chains = []
        html += "<tr class='chain_names_row'>"
        for (let chain in chain_config) {
            if (used_chains.has(chain)) {
                ordered_chains.push(chain)
                html += "<td>"+chain+"</td>"
            }
        }
        html += "</tr>"
        console.log('ordered_chains',ordered_chains)


        if (addresses.size > 1) {
            html += "<tr row_addr=all class='all_addresses_row'>"
            for (let chain of ordered_chains) {
                html += "<td><input type=checkbox class='toggle_all_addresses' chain='"+chain+"'></td>"
            }
            html += "</tr>"
        }

        for (let address in all_address_info) {
            if (address == 'my account')
                continue
            html += "<tr row_addr="+address+">"
            for (let chain of ordered_chains) {
                if (chain in all_address_info[address]) {
                    addr_dict = all_address_info[address][chain]
                    if (addr_dict['present']) {
                        used = addr_dict['used']
                        checked = ""
                        if (used)
                            checked = " checked "
                        tx_count = 0
                        if ('tx_count' in addr_dict)
                            tx_count = addr_dict['tx_count']
                        let title = tx_count+" transactions for "+address+" on "+chain
                        html += "<td><input type=checkbox class=ac_cb chain='"+chain+"' address="+address+" "+checked+"><div class='adr_tx_count' title='"+title+"'>"+tx_count+"</div></td>"
                    } else {
                        html += "<td><input type=checkbox disabled></td>"
                    }
                } else{
                    html += "<td></td>"
                }
            }
            html += "</tr>"
        }
        html += "</table>"
    }

    if ('my account' in all_address_info) {
        html += "<ul id='upload_list'><div id='upload_list_header'>Your uploads:</div>"
        for (let upload in all_address_info['my account']) {
            let addr_dict = all_address_info['my account'][upload]
            let used = addr_dict['used']
            let tx_count = 0
            if ('tx_count' in addr_dict)
                tx_count = addr_dict['tx_count']
            html += "<li><label><input type=checkbox class=ac_cb id="+upload+"_displayed address='my account' chain='"+upload+"'"
            if (used)
                html += " checked "
            html += "><div class='upload_option'>"+upload+"</div> ("+tx_count+" transactions)</label>"
            html += "<div title='Add more transactions from the same source' class='up_icon up_add' chain='"+upload+"'></div>"
            html += "<div title='Delete this upload and all its transactions' class='up_icon up_delete' chain='"+upload+"'></div>"
            html += "</li>"
        }
        html += "</ul>"
    }

    html += "</div>"
    return html
}

$('body').on('change','.toggle_all_chains',function() {
    row_addr = $(this).closest('tr').attr('row_addr')
    checked = false
    if ($(this).is(':checked'))
        checked = true
    $('#address_matrix_right').find('input[address='+row_addr+']').prop('checked',checked)
});

$('body').on('change','.toggle_all_addresses',function() {
    chain = $(this).attr('chain')
    checked = false
    if ($(this).is(':checked'))
        checked = true
    $('#address_matrix_right').find('input[chain='+chain+']').prop('checked',checked)
});

$('body').on('change','#toggle_all',function() {
    checked = false
    if ($(this).is(':checked'))
        checked = true
    $('#address_matrix_right').find('input').prop('checked',checked)
    $('.toggle_all_chains').prop('checked',checked)
});







function make_top() {
    let submit_button_el = $('#submit_address').css({'width':'auto','margin-top':'20px','display':'block'}).detach()
    $('#demo_link').remove();
    let import_new_el = $('#import_new_transactions').prop('checked',false).css({'display':'none'}).detach()

    let inner_form_html = "";

    displayed_addresses_set = new Set()
    for (let address in all_address_info) {
        if (address == 'my account') {
            for (let upload in all_address_info[address])
                if (all_address_info[address][upload]['used'])
                    displayed_addresses_set.add(upload)
        } else {
            for (let chain in all_address_info[address])
                if (all_address_info[address][chain]['used'])
                    displayed_addresses_set.add(address)
        }
    }
    displayed_addresses = Array.from(displayed_addresses_set)
//                    console.log('displayed_addresses',displayed_addresses)
    inner_form_html += "<div id='displayed_address_list'>";

    if (displayed_addresses.length <= 3) {
        let idx = 0;
        for (let address of displayed_addresses) {
            idx += 1;

            inner_form_html += "<div class='displayed_address_top hash copiable' title='Copy to clipboard' full='"+address+"'>"+address+"</div>";
            if (idx < displayed_addresses.length)
                inner_form_html +=", "
        }
    }
    else inner_form_html +=displayed_addresses.length + " sources";

    inner_form_html += "</div>";
    inner_form_html += "<input type=hidden id='your_address' value='"+primary+"'>"
    inner_form_html += "<div id='top_menu_icon' class='top_menu_icon_closed'></div><div id='top_menu'></div>"


    $('#main_form').html(inner_form_html)

    let top_menu_html = "";

    top_menu_html += address_matrix_html()


    top_menu_html += "<div id='aa_clicker'>Add another address</div>";
    top_menu_html += "<div id='up_clicker'>Upload transactions from a CSV</div>";

//    top_menu_html += "<input type=hidden id=force_forget_derived value=0 />"
    $('#top_menu').html(top_menu_html)
    import_new_el.appendTo($('#top_menu'))
    submit_button_el.appendTo($('#top_menu'))
}

function add_transactions(transactions) {
    new_txids = []
    for (transaction of transactions) {
        let txid = parseInt(transaction['txid']);
        console.log("Adding tx",txid)
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
    return new_txids
}

function delete_transaction(txid) {
    console.log("Deleting tx "+txid)
    let idx = transaction_order.indexOf(parseInt(txid))
    for (let l_idx = idx+1; l_idx < transaction_order.length; l_idx++) {
        let txid_loop = transaction_order[l_idx];
        let o_tx = all_transactions[txid_loop];
        o_tx['num'] = o_tx['num'] - 1;
    }

    map_lookups(all_transactions[txid], unmap_instead=true)

    delete all_transactions[txid];
    transaction_order.splice(idx,1)
}

$('body').on('click','a.edit_coingecko_id',function() {
    let current_id = $(this).text()
    let chain = $(this).attr('chain')
    let contract = $(this).attr('contract')
    let symbol = $(this).attr('symbol')
    let l1 = lookup_info["token_address_mapping"][contract]
    let l2 = lookup_info["chain_mapping"][chain]
    let matches = set_intersect(l1,l2);


    let html ="<div id='overlay'></div><div id='cg_popup' class='popup'>";
    html += "<div class=header>Change Coingecko ID for "+symbol+"</div>"
    if (current_id == fiat) {
        html += "<div id='cg_popup_explanation'>Sorry, you can't change coingecko ID for "+fiat+". It will literally make the server explode.</div>"
        html += "<div class='sim_buttons'><div id='cg_cancel'>Cancel</div></div>"
    } else {

        html += "<div id='cg_popup_explanation'><p>This will change coingecko ID everywhere you transferred "+contract+" on "+chain+", in "+matches.size+" transactions. "
        html += "It will not affect other chains/uploads, or other tokens with the same symbol. It may cause the software to download additional rate data from coingecko, "
        html += "and it may affect your taxes. All tokens with the same Coingecko ID are treated as the same asset when calculating your taxes.</p>"
        html += "<p>You can find Coingecko ID in the browser address bar on Coingecko, or here:<br>"
        html += "<img src='static/coingecko_id.png'></p>"
        html += "</div>"

        if (current_id == 'not found')
            current_id = ''

        html += "New Coingecko ID (leave empty to return to default): <input type=text id='coingecko_id_input' value='"+current_id+"' chain='"+chain+"' current_coingecko_id='"+current_id+"' contract='"+contract+"' />"

        html += "<div class='sim_buttons'><div id='cg_process'>Change Coingecko ID</div>";
        html += "<div id='cg_cancel'>Cancel</div></div></div>";
    }
    $('#content').append(html);
});

$('body').on('click','#cg_process',function() {
    el = $('#coingecko_id_input')
    new_id = el.val()
    current_id = el.attr('current_coingecko_id')
    if (new_id == current_id) {
        $('.popup').remove();
        $('#overlay').remove();
        return
    }
    if (new_id == fiat) {
        $('#cg_popup').find('.sim_buttons').css({'display':''}).after("<div class='cg_error'>You can't set coingecko ID to "+fiat+"</div>");
        return
    }

    $('#cg_popup').find('.sim_buttons').css({'display':'none'})
    start_progress_bar('popup')
    $('.cg_error').remove()

    chain = el.attr('chain')
    contract = el.attr('contract')
    $.get("update_coingecko_id?chain="+chain+"&address="+primary+"&contract="+contract+"&new_id="+new_id, function(js) {
        stop_progress_bar();
        var data = JSON.parse(js);
        if (data.hasOwnProperty('error')) {
            $('#cg_popup').find('.sim_buttons').css({'display':''}).after("<div class='cg_error'>"+data['error']+"</div>");
        } else {
            new_txids = add_transactions(data['transactions'])
            make_pagination();
            need_recalc();
            $('.popup').remove();
            $('#overlay').remove();
        }
    });
})

$('body').on('click','.t_hide',function(event) {
    txel = $(this).closest('.transaction')
    t_id = txel.attr('id');
    let txid = parseInt(t_id.substr(2));
    let transaction = all_transactions[txid]
    if ($(this).hasClass('t_hide_shown'))
        transaction.minimized = true
    else
        transaction.minimized = false
    transaction_html = make_transaction_html(transaction);

    secondary_selected = primary_selected = false
    if (txel.hasClass('secondary_selected'))
        secondary_selected = true

    if (txel.hasClass('primary_selected'))
        primary_selected = true
    console.log(secondary_selected, primary_selected)

    txel.replaceWith(transaction_html)

    txel = $('#t_'+txid);
    if (transaction.minimized) {
        if (secondary_selected)
            txel.addClass('secondary_selected')
        if (primary_selected)
            txel.addClass('primary_selected')
    } else {
        if (primary_selected)
            select_transaction(txel,keep_secondary=true)
        if (secondary_selected)
            txel.addClass('secondary_selected')
    }
    data = 'minimized='+(transaction.minimized?1:0)+'&transactions='+txid;

    $.post("minmax_transactions?address="+primary, data, function(resp) {
        var data = JSON.parse(resp);
        if (data.hasOwnProperty('error')) {
            txel.append("<div class='err_mes'>"+data['error']+"</div>");
        }
    });
    event.stopPropagation()
});


$('body').on('click','.delete_address',function() {
    let address = $(this).attr('addr')
    html ="<div id='overlay'></div><div id='delete_address_popup' class='popup'><form id='delete_address_form'><input type=hidden name=address_to_delete value='"+address+"'> ";
    html += "Really delete "+address+"? This will also delete all transfers associated with it.";
    html += "<div class='sim_buttons'>";
    html += "<div id='delete_address_confirm'>Delete address</div>";
    html += "<div id='delete_address_cancel'>Cancel</div></div>";
    html += "</form></div>";
    $('#content').append(html);
});

$('body').on('click','#delete_address_cancel',function() {
    $('#delete_address_popup').remove();
    $('#overlay').remove();
});

$('body').on('click','#delete_address_confirm',function() {
    data = $('#delete_address_form').serialize();
    $.post("delete_address?address="+primary, data, function(resp) {
//        console.log(resp);
        var data = JSON.parse(resp);
        if (data.hasOwnProperty('error')) {
            $("#delete_address_form").after("<div class='err_mes'>"+data['error']+"</div>");
        } else {
            all_address_info = data['all_address_info'];
            make_top();
            if (data['reproc_needed']) {
                $('#main_form').submit();
                return
            }

            $('#delete_address_popup').remove();
            $('#overlay').remove();
        }
    });
});

$('body').on('click','#donations_main',function() {
    html ="<div id='overlay'></div><div id='donations_popup' class='popup'>";
    html += "<p>Thank you for clicking here. My name is Ilya Raykhel, and development of this website is my full-time job. Additionally, I spend about $7K/year on hosting costs and various paid APIs. ";
    html += "If you found my website useful and would like to contribute to its development, I would appreciate a donation to 0xbf01E689Dd71206A47186f204afBf3b8e7bB8114.</p> "
    html += "<p>If there's ever a token or an NFT project associated with this website in the future (no promises), all donators will be generously rewarded.</p>"
    html += "<div class='sim_buttons'>";
    html += "<div id='donations_popup_cancel'>Close popup</div></div>";
    html += "</div>";
    $('#content').append(html);
});


$('body').on('click','#donations_popup_cancel',function() {
    $('#donations_popup').remove();
    $('#overlay').remove();
});