
$('body').on('click','#up_clicker',function() {
    show_popup()
});

$('body').on('click','.up_add',function() {
    let source = $(this).attr('chain')
    show_popup(source)
});

function show_popup(source=null) {
    let html ="<div id='overlay'></div><div id='up_popup' class='popup'>";
    if (demo) html += "<div>You can't upload CSV in a demo, sorry.</div><div>";
    else {
        html += "<div class=header>Upload your transactions<div class='help help_upload_csv'></div></div>"
        html += "<form id=up_form enctype='multipart/form-data'><ol id=up_template_list>"
        html += "<li class=up_template><a id=up_template_link target=_blank href='https://docs.google.com/spreadsheets/d/1dm-41zxpfS1BQUYgEhC1-kmt8egYJwu3IICCXomnN68/edit?usp=sharing'>Use this Google Sheets template</a></li>"
        html += "<li><label id='up_input_label'><input type=file id='up_input' name=up_input /><div id='up_input_text'>Choose CSV file from your computer</div></label></li>"
        html += "<li>Specify source of transactions:<input type=text id=up_source placeholder='For example, Binance'"
        if (source != null)
            html += "value='"+source+"'"
        html += "/></li></ol></form>"

        html += "<div class='sim_buttons'><div id='up_process'>Upload transactions</div>";
    }
    html += "<div id='up_cancel'>Cancel</div></div></div>";
    $('#content').append(html);
}

$('body').on('change','#up_input', function() {
    let filename_spl = $(this).val().split("\\");
    let filename = filename_spl[filename_spl.length-1];
    if (filename.length > 0) {
        $('#up_input_text').html(filename);
        $('#up_error_file').remove()
    } else
        $('#up_input_text').html("Browse your computer");
});

$('body').on('click','#up_process', function() {
    let bad = false;
    let mode = "append";
    $('.up_error').remove()
    if ($('#up_input').get(0).files.length === 0) {
        if ($('#up_error_file').length == 0)
            $('#up_input_label').after("<div class='up_error' id=up_error_file>Please select a file</div>")
        bad = true;
    }

    let source = $('#up_source').val()
    if (source.length == 0) {
        if ($('#up_error_source').length == 0)
            $('#up_source').after("<div class='up_error' id=up_error_source>Please specify the source</div>")
        bad = true;
    }

    for (let chain in chain_config) {
        if (chain.toLowerCase() == source.toLowerCase()) {
            $('#up_source').after("<div class='up_error' id=up_error_source>"+chain+" is one of the chains we support. "+
            "To avoid a godawful confusion please pick a different name for your upload.</div>")
            bad = true;
            break;
        }
    }

    if (bad)
        return;

    $('#up_popup').children().css({'display':'none'})
    start_progress_bar('popup')



    $.ajax({
        url: "upload_csv?address="+primary+"&source="+source+"&mode="+mode,
        type: 'POST',
        data: new FormData($('#up_form')[0]),
        cache: false,
        contentType: false,
        processData: false,
        success: function( data, textStatus, jqXHR  ) {
            console.log("uploaded", textStatus, data)
            data = JSON.parse(data);
            stop_progress_bar();
            if (data.hasOwnProperty('error')) {
                let error = data['error'];
                $('#up_popup').children().css({'display':''})
                $('#up_popup').find('.sim_buttons').after("<div class='up_error'>"+error+"</div>");
            } else{
                all_address_info = data['all_address_info'];
                make_top()
                new_txids = add_transactions(data['transactions'])
                make_pagination();
                need_recalc();
                need_reproc(display=true,level=3,text="Reprocessing transactions required to detect cross-account transfers");
                $('.popup').remove();
                $('#overlay').remove();
                $('#top_menu_icon').click()
            }


        },
        error: function (jqXHR,textStatus,errorThrown ) {
            stop_progress_bar();
            console.log("error",textStatus,errorThrown)
            console.log(upload_config)
            $('#up_popup').children().css({'display':''})
            $('#up_popup').find('.sim_buttons').after("<div class='up_error'>Unknown upload error:"+errorThrown+"</div>");
        }
  });
});

$('body').on('click','.up_delete',function() {
    let upload = $(this).attr('chain')
    html ="<div id='overlay'></div><div id='up_delete_popup' class='popup'><form id='up_delete_form'><input type=hidden name=chain value='"+upload+"'> ";
    html += "Really delete "+upload+"? This will also delete all transactions from it.";
    html += "<div class='sim_buttons'>";
    html += "<div id='up_delete_confirm'>Delete upload</div>";
    html += "<div id='up_delete_cancel'>Cancel</div></div>";
    html += "</form></div>";
    $('#content').append(html);
});

$('body').on('click','#up_delete_cancel',function() {
    $('#up_delete_popup').remove();
    $('#overlay').remove();
});

$('body').on('click','#up_delete_confirm',function() {
    data = $('#up_delete_form').serialize();
    $.post("delete_upload?address="+primary, data, function(resp) {
//        console.log(resp);
        var data = JSON.parse(resp);
        if (data.hasOwnProperty('error')) {
            $("#up_delete_form").after("<div class='err_mes'>"+data['error']+"</div>");
        } else {
            txids_to_delete = data['txids']
            for (txid of txids_to_delete) {
                if (txid in all_transactions)
                    delete_transaction(txid);
            }
            all_address_info = data['all_address_info']
            make_top();
            prev_selection = null;
            $('#up_delete_popup').remove();
            $('#overlay').remove();


            need_recalc();
            need_reproc(display=true,level=3);
            make_pagination();
        }
    });
});