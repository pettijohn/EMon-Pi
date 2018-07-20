/*global WildRydes _config*/

var WildRydes = window.WildRydes || {};

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
        dt = $("#recentDays").DataTable( {
            data: result,
            columns: [
                {"data": "bucket_id", "title": "Date", "render": function ( data, type, row, meta ) {
                    return data.substr( 0, 10 )
                    }
                },
                {"data": "cost_usd", "title": "Cost (USD)", "render": $.fn.dataTable.render.number(',', '.', 2, '$')
                    //function ( data, type, row, meta ) { return "$" + Math.round(data*100)/100 }
                }
            ]
        });
        //Sort table by recent days
        dt.order([ 0, 'desc' ]).draw();
    }

    // Register click handler for #request button
    $(function onDocReady() {
        $('#request').click(handleRequestClick);
        $('#signOut').click(function() {
            WildRydes.signOut();
            alert("You have been signed out.");
            window.location = "index.html";
        });

        WildRydes.authToken.then(function updateAuthMessage(token) {
            if (token) {
                displayUpdate('You are authenticated. Click to see your <a href="#authTokenModal" data-toggle="modal">auth token</a>.');
                $('.authToken').text(token);
            }
        });

        if (!_config.api.invokeUrl) {
            $('#noApiMessage').show();
        }

        now = new Date();
        prev = new Date(now.getTime() - 1000*60*60*24*59); //59 days earlier

        fetchData(formatDate(prev), formatDate(now));
    });

    function formatDate(d) {
        return d.getFullYear() + '-' + (d.getMonth() + 1) + '-' + d.getDate();
    }

    function handlePickupChanged() {
        var requestButton = $('#request');
        requestButton.text('Request Unicorn');
        requestButton.prop('disabled', false);
    }

    function handleRequestClick(event) {
        var pickupLocation = WildRydes.map.selectedPoint;
        event.preventDefault();
        requestUnicorn(pickupLocation);
    }

    function animateArrival(callback) {
        var dest = WildRydes.map.selectedPoint;
        var origin = {};

        if (dest.latitude > WildRydes.map.center.latitude) {
            origin.latitude = WildRydes.map.extent.minLat;
        } else {
            origin.latitude = WildRydes.map.extent.maxLat;
        }

        if (dest.longitude > WildRydes.map.center.longitude) {
            origin.longitude = WildRydes.map.extent.minLng;
        } else {
            origin.longitude = WildRydes.map.extent.maxLng;
        }

        WildRydes.map.animate(origin, dest, callback);
    }

    function displayUpdate(text) {
        $('#updates').append($('<li>' + text + '</li>'));
    }
}(jQuery));
