/*global WildRydes _config*/

var WildRydes = window.WildRydes || {};

// https://stackoverflow.com/questions/2937227/what-does-function-jquery-mean
(function rideScopeWrapper($) {
    var authToken;
    WildRydes.authToken.then(function setAuthToken(token) {
        if (token) {
            authToken = token;
        } else {
            window.location.href = 'index.html';
        }
    }).catch(function handleTokenError(error) {
        alert(error);
        window.location.href = 'index.html';
    });

    // Format a javascript date into a string that the backend can parse
    function formatDate(d) {
        return d.getFullYear() + '-' + ("00" + (d.getMonth() + 1)).slice(-2) + '-' + ("00" + d.getDate()).slice(-2);
    }

    // Fetch data between two dates
    function fetchRange(startDate, endDate) {
        var dataDict = {
            start: formatDate(startDate),
            end: formatDate(endDate),
            grain: 'day'
        };
        queryCore(dataDict);
    }

    // Fetch child data
    function fetchChildren(bucketID) {
        // On click of a bar, get the children
        
    }

    function queryCore(dataDict) {
        $.ajax({
            method: 'GET',
            url: _config.api.invokeUrl + '/range',
            headers: {
                Authorization: authToken
            },
            data: dataDict,
            //contentType: 'application/json',
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
        

        var data = new google.visualization.DataTable();
        data.addColumn('string', 'Date');
        data.addColumn('number', 'Cost (USD)');
        var totalUSD = 0.0;
        var totalWH = 0.0
        result.forEach(row => {
            data.addRow([row['bucket_id'], Math.round(row['cost_usd']*100)/100])
            totalUSD += row['cost_usd'];
            totalWH += row['watt_hours'];
        });

        //Render total data table
        dt = $("#totals").DataTable( {
            data: [
                ["Cost (USD)", Math.round(totalUSD*100)/100],
                ["Power (kWh)", Math.round(totalWH/1000*100)/100]
            ]
        });

        // Set chart options
        // https://developers.google.com/chart/interactive/docs/gallery/barchart#Configuration_Options
        var options = {'title':'Cost Explorer',
            'legend': {'position': 'none'},
            'chartArea': {'width': '70%', 'height': '90%'}
        };

        function selectHandler() {
            var selectedItem = chart.getSelection()[0];
            if (selectedItem) {
              var item = data.getValue(selectedItem.row, 0);
              console.log('The user selected ' + item);
            }
        }
  
        // Instantiate and draw our chart, passing in some options.
        var chart = new google.visualization.BarChart($('#chart')[0]);
        // Wire up select handler
        google.visualization.events.addListener(chart, 'select', selectHandler);
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
            prev = new Date(now.getTime() - 1000*60*60*24*30); //30 days earlier

            fetchRange(prev, now);
        });
        
    });

}(jQuery));
