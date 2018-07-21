/*global WildRydes _config*/

var WildRydes = window.WildRydes || {};

// https://stackoverflow.com/questions/2937227/what-does-function-jquery-mean
(function rideScopeWrapper($) {
    var authToken;
    WildRydes.authToken.then(function setAuthToken(token) {
        if (token) {
            authToken = token;
        } else {
            window.location.href = '/index.html';
        }
    }).catch(function handleTokenError(error) {
        alert(error);
        window.location.href = '/index.html';
    });
    function fetchData(startDate, endDate) {
        $.ajax({
            method: 'POST',
            url: _config.api.invokeUrl + '/query',
            headers: {
                Authorization: authToken
                //, Origin: "http://energy.pettijohn.com"
            },
            data: JSON.stringify({
                start: startDate,
                end: endDate,
                format: 'total'
            }),
            contentType: 'application/json',
            success: completeRequest,
            error: function ajaxError(jqXHR, textStatus, errorThrown) {
                console.error('Error loading data: ', textStatus, ', Details: ', errorThrown);
                console.error('Response: ', jqXHR.responseText);
                console.error('Status: ', jqXHR.status)
                alert('An error occured while loading the page:\n' + jqXHR.responseText);
            }
        });
    }

    function completeRequest(result) {
        //console.log('Response received from API: ', result);
        // dt = $("#recentDays").DataTable( {
        //     data: result,
        //     columns: [
        //         {"data": "bucket_id", "title": "Date", "render": function ( data, type, row, meta ) {
        //             return data.substr( 0, 10 )
        //             }
        //         },
        //         {"data": "cost_usd", "title": "Cost (USD)", "render": $.fn.dataTable.render.number(',', '.', 2, '$')
        //             //function ( data, type, row, meta ) { return "$" + Math.round(data*100)/100 }
        //         }
        //     ]
        // });
        // //Sort table by recent days
        // dt.order([ 0, 'desc' ]).draw();

        var data = new google.visualization.DataTable();
        data.addColumn('string', 'Date');
        data.addColumn('number', 'Cost (USD)');
        result.forEach(row => {
            data.addRow([row['bucket_id'], Math.round(row['cost_usd']*100)/100])
        });

        // Set chart options
        var options = {'title':'Cost Explorer'};

        // Instantiate and draw our chart, passing in some options.
        var chart = new google.visualization.BarChart($('#chart')[0]);
        chart.draw(data, options);
    }

    // https://stackoverflow.com/questions/7642442/what-does-function-do
    $(function onDocReady() {
        $('#signOut').click(function() {
            WildRydes.signOut();
            alert("You have been signed out.");
            window.location = "index.html";
        });

        WildRydes.authToken.then(function updateAuthMessage(token) {
            if (token) {
                //displayUpdate('You are authenticated. Click to see your <a href="#authTokenModal" data-toggle="modal">auth token</a>.');
                $('.authToken').text(token);
            }
        });

        if (!_config.api.invokeUrl) {
            $('#noApiMessage').show();
        }


        // Load the Visualization API and the corechart package.
        google.charts.load('current', {'packages':['corechart']});

        // Set a callback to run when the Google Visualization API is loaded.
        google.charts.setOnLoadCallback(function() {
            // When the chart is ready to draw, fetch the data
            now = new Date();
            prev = new Date(now.getTime() - 1000*60*60*24*59); //59 days earlier

            fetchData(formatDate(prev), formatDate(now));
        });
        
    });

    function formatDate(d) {
        return d.getFullYear() + '-' + (d.getMonth() + 1) + '-' + d.getDate();
    }

}(jQuery));
