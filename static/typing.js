$.validator.addMethod('atleastonerule', function(val,el) {
//        console.log('custom val method called',$('#tc_form').find('.rule').length);
        if ($('#tc_form').find('.rule').length == 0)
            return false
        return true
    },'You need at least one rule')

function my_address_rule_options(direction,addr_custom) {
    let cls = 'index_to_addr'
    if (direction == 'out') {
        cls = 'index_from_addr'
    }

    let html = "<input type=hidden value='my_address' class='"+cls+"'>"
    if (dict_len(all_address_info) == 1)
        html += "<input type=hidden class='"+cls+"_custom'>my address"
    else {
        let selected = ''
        html += "<select class='"+cls+"_custom tc_rule_sel'>";
        if (addr_custom == null || addr_custom == '')
            selected = ' selected'
        html += "<option value=''"+selected+">any of my addresses</option>";
        for (let address in all_address_info) {
            if (address == addr_custom) selected = ' selected'; else selected = '';
            html += "<option value='"+address+"'"+selected+">"+startend(address)+"</option>";
        }
        html += "</select>";
    }
    return html
}

function address_rule_options(direction,addr,addr_custom) {
//    console.log('aro',direction,addr,addr_custom)
    let cls = 'index_from_addr'
    if (direction == 'out') {
        cls = 'index_to_addr'
    }

    custom_val = "";
    hidden = " hidden_custom"
    if (addr_custom != "" && addr_custom != null) {
        custom_val = " value='"+addr_custom+"'";
        hidden = "";
    }

    html = "<div class='selspec_wrap"+hidden+"'><select class='tc_rule_addr tc_rule_sel "+cls+"'>";

    opt_list = [['any','anywhere','any'],['0x0000000000000000000000000000000000000000',startend('0x0000000000000000000000000000000000000000'),'0x0000000000000000000000000000000000000000'],
        ['specific','a specific address','specific'],['specific_excl','anywhere but a specific address','specific_excl']];

    for (pair of opt_list) {
        html += "<option ";
        html += "value='"+pair[0]+"'";
        html += " class='tc_addr_opt_"+pair[2]+"'"
        if (pair[2] == addr) html += " selected ";
        html += ">"+ pair[1] +"</option>";
    }



    html += "</select>";
    html += "<input type=text class='specific_val "+cls+"_custom' placeholder='paste address here'"+custom_val+"></div>";
    return html;
}

function token_rule_options(tok,tok_custom) {
    custom_val = "";
    hidden = " hidden_custom"
    if (tok_custom != "" && tok_custom != null) {
        custom_val = " value='"+tok_custom+"'";
        hidden = "";
    }

    html = "<div class='selspec_wrap"+hidden+"'><select class='tc_rule_tok tc_rule_sel index_rule_tok'>";


    opt_list = [['any','any token','any'], ['specific','specific token','specific'],['specific_excl','any token except','specific_excl']];

    for (pair of opt_list) {
        html += "<option ";
        if (pair[0] != null) html += "value='"+pair[0]+"'";
        html += " class='tc_tok_opt_"+pair[2]+"'"
        if (pair[2] == tok) html += " selected ";
        html += ">"+ pair[1] +"</option>";
    }

    html += "</select>";
    html += "<input type=text class='specific_val index_rule_tok_custom' "+custom_val+"><div class='help help_tokenrule'></div></div>";
    return html;
}

function treatment_rule_options(direction,def,vault_id=null,vault_id_custom=null) {
//    console.log('treatment rule',direction,def,vault_id,vault_id_custom);

    vault_id_hidden = '';


    if (direction == 'out') {
        opt_list = [['ignore','Ignore'], ['sell','Sell at market price'], ['burn','Dispose for free'],['fee','Transaction cost'],['repay','Repay loan'],['full_repay','Fully repay loan'],['deposit','Deposit to vault'],['interest','Loan interest'],['expense','Business expense']];
        vault_id_opt_list = [['address','Destination address'],['type_name','Name of this custom type'],['other','Other']];
    } else {
        opt_list = [['ignore','Ignore'], ['buy','Buy at market price'], ['gift','Acquire for free'], ['income','Income'], ['borrow','Borrow'], ['withdraw','Withdraw from vault'], ['exit','Exit vault']];
        vault_id_opt_list = [['address','Source address'],['type_name','Name of this custom type'],['other','Other']];
    }

    html ="<select class='tc_rule_sel tc_rule_treatment index_rule_treatment'>";

    for (pair of opt_list) {
//        console.log(pair);
        html += "<option ";
        if (pair[0] != null) html += "value='"+pair[0]+"'";
        html += " class='tc_treatment_opt_"+pair[0]+"'"
        if (pair[0] == def) {
            html += " selected ";
            //if (pair[0] == 'deposit' || pair[0] == 'withdraw' || pair[0] == 'deposit' || pair[0] == 'withdraw')
            if (['deposit','withdraw','repay','borrow','exit','liquidation','full_repay'].includes(pair[0]))
                vault_id_hidden = ' style="display: inline-block;"';
        }

        html += ">"+ pair[1] +"</option>";
    }
    html += "</select>";

//    if (direction == 'out')
//        help = "This tax treatment means you are depositing your tokens somewhere (into a \"vault\").\n"
//    else
//        help = "This tax treatment means you are withdrawing your tokens from somewhere (a \"vault\") you deposited them earlier.\n"

    hidden_custom = " hidden_custom";
    if (vault_id == 'other')
        hidden_custom = "";
//    help += "To be accounted correctly, deposits to and withdrawals from the same location must use the same vault ID. Deposits and withdrawals from different locations must use different vault IDs.";
    html += "<div class='vault_id_wrap selspec_wrap "+hidden_custom+"'"+vault_id_hidden+"><span class='vault_id_name'>Vault ID<div class='help help_vaultid'></div></span>:<select type=text class='tc_vault_id_field tc_rule_sel index_vault_id'>";

    custom_vault_id = "";
    for (pair of vault_id_opt_list) {
        html += "<option ";
        html += "value='"+pair[0]+"'";
        html += " class='tc_vault_id_opt'"
        if (pair[0] == vault_id) {
            html += " selected ";
            if (vault_id == 'other' && vault_id_custom != null) {
                custom_vault_id = " value='"+vault_id_custom+"'"
            }
        }
        html += ">"+ pair[1] +"</option>";
    }
    html += "</select><input type=text class='specific_val index_vault_id_custom'"+custom_vault_id+"></div>";
    return html;
}

function make_rule_html(direction,rule=null) {
    if (rule == null) { from_addr = 'any'; from_addr_custom=null; to_addr='any'; to_addr_custom=null; tok='any'; tok_custom=null; treatment='free'; vault_id=null; vault_id_custom=null;}
    else { from_addr = rule[1]; from_addr_custom=rule[2]; to_addr=rule[3]; to_addr_custom=rule[4]; tok=rule[5]; tok_custom=rule[6]; treatment=rule[7]; vault_id=rule[8]; vault_id_custom=rule[9];}
    html = "<div class=rule><span class='r_mov' title='Hold to move rule'><div></div></span>";
    html += "<div class='rule_conditions'>";
    if (direction == 'out') {
        html += "<span class='r_from_addr'>"+my_address_rule_options(direction,from_addr_custom)+"</span>";
    } else {
        html += "<span class='r_from_addr'>"+address_rule_options(direction,from_addr,from_addr_custom)+"</span>";
    }

    html += "<span class='r_arrow'><div></div></span>";
    if (direction == 'in')
        html += "<span class='r_to_addr'>"+my_address_rule_options(direction,to_addr_custom)+"</span>";
    else
        html += "<span class='r_to_addr'>"+address_rule_options(direction,to_addr,to_addr_custom)+"</span>";


    html += "<span class='r_token'>"+token_rule_options(tok,tok_custom)+"</span>";
    html += "</div>";
    html += "<div class='rule_treatment'><span class='r_expl'>Treatment:</span><span class='tc_r_treatment'>"+treatment_rule_options(direction,treatment, vault_id, vault_id_custom)+"</span></div>";
    html += "<span class='r_rem' title='Delete rule'><div></div></span></div>";
    return html;
}

$('body').on('click','#types_create',function() {
    create_edit_custom_type(null);
});

$('body').on('click','div.ct_edit',function() {
    id = $(this).closest('li').attr('id').substr(3);
//    console.log('type edit id',id);
    create_edit_custom_type(id);
});

$('body').on('click','div.ct_delete',function() {
    id = $(this).closest('li').attr('id').substr(3);
    delete_custom_type_popup(id);
});

$('body').on('click','div.ct_select',function(event) {
    id = $(this).closest('li').attr('id').substr(3);
    if (!event.ctrlKey) {
        deselect_primary();
        mark_all_deselected();
    }
//    $('.custom_type_'+id).addClass('secondary_selected');
    for (let txid in all_transactions) {
        let transaction = all_transactions[txid];
        if (transaction['ct_id'] == id)
            selected_transactions.add(parseInt(txid))
//            transaction['selected']=true;
    }
    $('#sel_opt_sel').click();
    update_selections_block();
    window.scrollTo(0,0);
//    update_selections_block();
});

function create_edit_custom_type(id) {
    if ($('#tc').length) return;

    $('.transaction').addClass('shifted');


    let html = "<div id='tc'><form id='tc_form'>";

    let name_val = "";
    let desc = "";
    let balanced = "checked";
//    let chain_specific="";
     if (id != null) {
        html += "<input type=hidden name=type_id id=del_type_id value="+id+"><div class='header'>Edit transaction type<div class='help help_createcustomtype'></div></div>";
        name_val = " value='"+custom_types_js[id]['name']+"'";
        desc = custom_types_js[id]['description'];
        rules = custom_types_js[id]['rules'];
        if (custom_types_js[id]['balanced'])
            balanced = "checked";
        else
            balanced = "";
//        if (custom_types_js[id]['chain_specific'])
//            chain_specific = " checked";
     } else
        html += "<div class='header'>Create new transaction type<div class='help help_createcustomtype'></div></div>";

    html += "<div class=top_section>";
//    html += "<div class='tx_row_0'><span class='t_class'><label>Only used on "+window.sessionStorage.getItem('chain')+" chain?<input type=checkbox"+chain_specific+" name=tc_chain></label></span></div>";
    html += "<div class='tx_row_1'><span class='t_class'><label for=tc_name class='tc_expl'>Your classification:</label><input type=text required placeholder='Name your type' id='tc_name' name=tc_name"+name_val+"></span></div>";
    html += "<div class='tx_row_2'><span class='t_class'><label for=tc_desc class='tc_expl'>Description (optional):</label><textarea id='tc_desc' name=tc_desc>"+desc+"</textarea></span></div>";
    html += "</div>";
    html += "<div id='tc_rules_expl'>Rules below are applied to every transfer in your selected transactions. If a transfer satisfies all the conditions on the left, tax treatment on the right is applied to it. "+
    "Default treatment is \"ignore\".</div>";
    html += "<div class='transfers'>";

//    html += "<table id='tc_rules_out' class='rows tc_rules'>";
    html += "<div id='tc_rules_out' class='rows tc_rules'>";
    if (id == null)
        html += make_rule_html('out');
    else {
        for (rule of rules) {
            if (rule[1] == 'my_address')
                html += make_rule_html('out',rule);
        }
    }
//    html += "</table>";
    html += "</div>";
    html+="<div class='r_add' id='r_add_out'><div></div>Add outbound rule</div>";

//    html+= "<table id='tc_rules_in' class='rows tc_rules'>";
    html+= "<div id='tc_rules_in' class='rows tc_rules'>";
    if (id == null)
        html += make_rule_html('in');
    else {
        for (rule of rules) {
            if (rule[3] == 'my_address')
                html += make_rule_html('in',rule);
        }
    }
//    html += "</table>
    html += "</div>";
    html += "<div class='r_add' id='r_add_in'><div></div>Add inbound rule</div>";
    html += "</div>";

//    help = "In a balanced transaction amount of money going out is the same as coming in. This helps us use precise exchange rates and derive USD rates for synthetic tokens.\n"
//    help += "For instance, a deposit into a Uniswap liquidity pool is balanced, and the monetary value of receipt token UNI-V2 is the same as the value of tokens you put in the pool.\n"
//    help += "On the other hand, a collateral deposit into Compound when you simultaneously receive some COMP rewards from earlier deposits is not balanced, since the value of the reward "
//    help += "has nothing to do with the value of your new deposit.\n"
//    help += "This setting is ignored if transaction only has incoming transfers, or only has outgoing transfers."
    html += "<div class='balance_config'><label>Transaction is balanced<div class='help help_balanced'></div><input type=checkbox name=tc_balanced "+balanced+"></label></div>";
    html += "<div class='sim_buttons'>";
    html += "<div id='tc_create'>Save type</div>";
    html += "<div id='tc_cancel'>Cancel</div></div>";
    html += "</form></div>";

    $('#content').append(html);

    $('#tc_rules_out').sortable({handle: ".r_mov", axis:"y", containment: "#tc_rules_out"});
    $('#tc_rules_in').sortable({handle: ".r_mov", axis:"y", containment: "#tc_rules_in"});
    $('#tc_form').validate({
        messages: {
            tc_name:'required',
        },
        rules: {
            tc_balanced: { //this is an ugly hack because jquery validation plugin is a POS
                atleastonerule:true
            }
        }
    });
}

function delete_custom_type_popup(id) {
    html ="<div id='overlay'></div><div id='popup' class='popup'><form id='tc_delete_form'><input type=hidden name=type_id value="+id+"> ";
    name = custom_types_js[id]['name'];
    html += "Really delete "+name+"? This will also unapply this type from all transactions it was previously applied to.";
    html += "<div class='sim_buttons'>";
    html += "<div id='tc_delete'>Delete type</div>";
    html += "<div id='tc_delete_cancel'>Cancel</div></div>";
    html += "</form></div>";
    $('#content').append(html);
}

$('body').on('click','#tc_delete_cancel, #tc_confirm_cancel',function() {
    $('#popup').remove();
    $('#overlay').remove();
});


$('body').on('click','.r_rem div',function() {
    $(this).closest('div.rule').remove();
});

$('body').on('click','#r_add_out',function() {
    html = make_rule_html('out');
    $('#tc_rules_out').append(html);
});

$('body').on('click','#r_add_in',function() {
    html = make_rule_html('in');
    $('#tc_rules_in').append(html);
});

$('body').on('click','#tc_cancel',function() {
    $('#tc').remove();
    $('.transaction').removeClass('shifted');
});

$('body').on('click','#tc_create',function() {
    $('.err_mes').remove();

    //fucking validation plugin requires different names for everything, so here we are
    let need_names = ['from_addr','to_addr','from_addr_custom','to_addr_custom','rule_tok','rule_tok_custom','rule_treatment','vault_id','vault_id_custom'];
    for (let to_index of need_names) {
        let idx = 0;
//        console.log('.index_'+to_index, $('index_'+to_index).length);
        $('.index_'+to_index).each(function() {
            $(this).attr('name',to_index+idx);
            idx += 1;
        });
    }



    $('.specific_val').each(function() {
        $(this).rules("add", {
            required:true,
            messages: {
                required:'required'
            }
        });
    });


    let is_valid = $('#tc_form').valid();

    if (is_valid) {
        data = $('#tc_form').serialize();
//        console.log(addr,data);
//        let address = window.sessionStorage.getItem('address');
        $.post("save_type?address="+primary, data, function(resp) {
//            console.log(resp);
            var data = JSON.parse(resp);
            if (data.hasOwnProperty('error')) {
//                console.log("ERROR",data['error']);
                $("#tc .sim_buttons").before("<div class='err_mes'>"+data['error']+"</div>");
            } else {
                $('#tc').remove();
                $('.transaction').removeClass('shifted');

                custom_types_html = show_custom_types(data['custom_types']);
                $('#custom_types_block').replaceWith(custom_types_html);
                update_selections_block();
            }
        });
    }
});

$('body').on('click','#tc_delete',function() {
    data = $('#tc_delete_form').serialize();
//    console.log(addr,data);


//    let address = window.sessionStorage.getItem('address');
    $.post("delete_type?address="+primary, data, function(resp) {
//        console.log(resp);
        var data = JSON.parse(resp);
        if (data.hasOwnProperty('error')) {
            $("#tc_delete_form").after("<div class='err_mes'>"+data['error']+"</div>");
        } else {
            $('#popup').remove();
            $('#overlay').remove();

            show_ajax_transactions(data);
            need_recalc();

            custom_types_html = show_custom_types(data['custom_types']);
            $('#custom_types_block').replaceWith(custom_types_html);
            update_selections_block();
        }
    });
});

$('body').on('change','.tc_rule_addr', function() {
    selected_option = $(this).find(':selected');
//    td = $(this).closest('td');
    wrap = $(this).closest('.selspec_wrap');
    if (selected_option.hasClass('tc_addr_opt_specific') || selected_option.hasClass('tc_addr_opt_specific_excl')) {
        wrap.removeClass('hidden_custom');
    } else {
        wrap.addClass('hidden_custom');
    }
});

$('body').on('change','.tc_rule_tok', function() {
    selected_option = $(this).find(':selected');
//    td = $(this).closest('td');
    wrap = $(this).closest('.selspec_wrap');
    if (selected_option.hasClass('tc_tok_opt_specific') || selected_option.hasClass('tc_tok_opt_specific_excl')) {
        wrap.removeClass('hidden_custom');
    } else {
        wrap.addClass('hidden_custom');
    }
});

$('body').on('change','.tc_rule_treatment', function() {
    selected_option = $(this).find(':selected');
    vault_id = $(this).next();
    val = selected_option.val();
    if (val == 'deposit' || val == 'withdraw' || val == 'exit') {
    //<span class='vault_id_name'>Vault ID<div class='help help_vaultid'></div></span>
    //html += "<div class='vault_id_wrap selspec_wrap "+hidden_custom+"'"+vault_id_hidden+">Vault ID<div class='help help_vaultid'></div>:<select type=text name=vault_id class='tc_vault_id_field tc_rule_sel'>";
        vault_id.find('.vault_id_name').html("Vault ID<div class='help help_vaultid'></div>");
        showib(vault_id);
    } else if (val == 'borrow' || val == 'repay' || val == 'liquidation' || val == 'full_repay') {
        vault_id.find('.vault_id_name').html("Loan ID<div class='help help_vaultid'></div>");
        showib(vault_id);
    } else {
        hide(vault_id);
    }
});

$('body').on('change','.tc_vault_id_field', function() {
    selected_option = $(this).find(':selected');
//    td = $(this).closest('td');
    wrap = $(this).closest('.selspec_wrap');
    if (selected_option.val() == 'other') {
        wrap.removeClass('hidden_custom');
    } else {
        wrap.addClass('hidden_custom');
    }
});


custom_types_js = {};
function show_custom_types(custom_types) {
    custom_types_js = {};
    html = "<div id='custom_types_block'>";
    if (custom_types.length > 0) {

        html += "<div class='header'>Custom types:</div><ul id='custom_types_list'>";
        for (let ct of custom_types) {
            id = ct['id'];
            name = ct['name'];
            html += "<li id=ct_"+id+"><span class='ct_name'>"+name+"</span>";
            html += "<div title='Select transactions with this type' class='ct_icon ct_select'></div>";
            html += "<div title='Unapply this custom type from selected transactions' class='ct_icon ct_unapply'></div>";
            html += "<div title='Edit this custom type' class='ct_icon ct_edit'></div>";
            html += "<div title='Delete this custom type' class='ct_icon ct_delete'></div>";
            html += "</li>";
            custom_types_js[id] = {'name':name,'rules':ct['rules'], 'description':ct['description'], 'balanced':ct['balanced']};
        }
        html += "</ul>";

    }
    html += "</div>";
//    console.log('ct html',html)
    return html;
}





$('body').on('click','#custom_types_list .applicable', function() {
   type_clicked = $(this).parent();
   ct_id = type_clicked.attr('id').substr(3);
   txids = [];
   if (selected_transactions.size == 0)
        return;
   if (selected_transactions.size > 20) {
        ct_name = $(this).html()
        confirm_apply_popup(ct_id,ct_name)
        return
   }
   apply_type(ct_id)

});


$('body').on('click','.ct_unapply', function() {
   type_clicked = $(this).parent();
   ct_id = type_clicked.attr('id').substr(3);
   txids = [];
   if (selected_transactions.size == 0)
        return;
   if (selected_transactions.size > 20) {
        ct_name = $(this).html()
        confirm_apply_popup(ct_id,ct_name,unapply=true)
        return
   }
   apply_type(ct_id,unapply=true)

//   data = 'type_id='+ct_id+'&transactions='+Array.from(selected_transactions).join(',');
//   $.post("unapply_type?address="+primary, data, function(resp) {
//        var data = JSON.parse(resp);
//        if (data.hasOwnProperty('error')) {
//            type_clicked.append("<div class='err_mes'>"+data['error']+"</div>");
//        } else {
//
//            show_ajax_transactions(data)
//            need_recalc();
//        }
//    });

});

function apply_type(ct_id,unapply=false) {
    data = 'type_id='+ct_id+'&transactions='+Array.from(selected_transactions).join(',');
    call = 'apply_type'
    if (unapply)
        call = 'unapply_type'
   $.post(call+"?address="+primary, data, function(resp) {
        var data = JSON.parse(resp);
        if (data.hasOwnProperty('error')) {
            type_clicked.append("<div class='err_mes'>"+data['error']+"</div>");
        } else {
            if (unapply)
                selected_id = null;
            show_ajax_transactions(data)
            need_recalc();
            $('#popup,#overlay').remove()
        }
    });
}


function confirm_apply_popup(id,name,unapply=false) {
    html ="<div id='overlay'></div><div id='popup' class='popup'><input type=hidden id=type_id value="+id+">"
    html += "<input type=hidden id=confirm_unapply value="+(unapply?1:0)+"> ";
    name = custom_types_js[id]['name'];
    if (unapply)
        html += "Are you sure you want to unapply custom type "+name+" from "+selected_transactions.size+" transactions?";
    else
        html += "Are you sure you want to apply custom type "+name+" to "+selected_transactions.size+" transactions?";
    html += "<div class='sim_buttons'>";
    html += "<div id='tc_confirm_apply'>Yes, do it</div>";
    html += "<div id='tc_confirm_cancel'>Cancel</div></div>";
    html += "</div>";
    $('#content').append(html);
}

$('body').on('click','#tc_confirm_apply', function() {
    let unapply = parseInt($('#confirm_unapply').val())
    let ct_id = $('#type_id').val()
    apply_type(ct_id,unapply=unapply)
});