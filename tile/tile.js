var tile = {
  tileconfig:{},
  // Takes in the id for the button that has been clicked
  spinnerOn: function(id) {
    $(`#${id}`).hide();
    $(`#${id}`)
      .next("ons-progress-circular")
      .show();
  },
  popPanel: function () {
    consolelog("popPanel");
    tile.getNav().popPage();
  },
  // Takes in the id for the button that has been clicked
  spinnerOff: function(id) {
    $(`#${id}`)
      .next("ons-progress-circular")
      .hide();
    $(`#${id}`).show();
  },
  // Takes in the id for the relevant dialog box element
  createDialog: function(dialogId) {
    var dialog = $(`#${dialogId}-dialog`)[0];
    if (dialog) {
      if(dialogId === "subscribe") {
        tile.createCheckboxes();
      }
      if(dialogId === "sendRealtimeEvent"){
        tile.createSelectOptions();
      }
      dialog.show();
    } else {
      ons
        .createElement(`${dialogId}-dialog.html`, { append: true })
        .then(function(dialog) {
          if(dialogId ==="subscribe"){
            tile.createCheckboxes();
          }
          if(dialogId === "sendRealtimeEvent"){
            tile.createSelectOptions();
          }
          dialog.show();
        });
    }
  },
  // Takes in the relevant dialog box element
  closeDialog: function(dialog) {
    dialog.hide();
  },
  // Takes in the connector method and relevant input elements to retrieve params from
  getParams: function(method, inputs, checkboxes, selectLists) {
    var params = {};
    var filters = {};
    var accounts = [];
    // loops through input fields and retrieves any params entered
    if(method==="subscribe"){
      console.log(checkboxes);
      for (i = 0; i < checkboxes.length; i++) {
        const checked = checkboxes[i].checked;
        const value = checkboxes[i].value;
        params[value] = checked
      }
    }
    else {
      for (i = 0; i < inputs.length; i++) {
        var key = inputs[i].attributes.id.value;
        var value = inputs[i].value;
        if (value) {
          // Special switch statement for account filters
          if (method === "retrieveAccountList") {
            switch (key) {
              case "accountId":
                filters.accountIdFilter = value;
                break;
              case "type":
                filters.accountFilterType = value;
                break;
              default:
                filters[key] = value;
            }
            // looks for filter params which need to be stringified for the connector
          } else if (method === "retrieveTransactionList" && key !== "accountId") {
            filters[key] = value;
          } else if(method === "sendRealtimeEvent"){
            switch(key){
              case "eventName":
                params[key]=value;
                break;
              default:
                accounts.push(value);
                break;
            }

          } else {
            params[key] = value;
          }
        }
      }
      for (j=0; j<selectLists.length; j++){
        console.log(selectLists[j])
        const id = selectLists[j].attributes.id.value;
        const value = selectLists[j].value;
        params[id] = value;
      }
    }
    // add the stringified filters if necessary
    if (Object.keys(filters).length !== 0) {
      params.filters = JSON.stringify(filters);
    }
    if(accounts.length !==0){
      params.accounts = JSON.stringify(accounts);
    }
    return params;
  },
  // Takes in the connector method, params, and relevant dialog box to be closed
  sendRequest: function(method, params, dialog = null) {
    tile.spinnerOn(method);
    console.log("params", params);
    // Sends container request

      if(method==="subscribe"){
        if(window.container.platform && window.container.platform.events){
          for(const event in params) {
            if (params[event] === true) {
              console.log("subscribe " + event)
              window.container.platform.events.subscribe(event, (resp) => console.error(event + " event received " + JSON.stringify(resp)), (resp) => console.error(event + " event subscription response " + JSON.stringify(resp)));
            } else {
              console.log("unsubscribe " + event)
              window.container.platform.events.unsubscribe(event, (resp) => console.error(event + " event unsubscribe response " + JSON.stringify(resp)));
            }
          }
        } else{
          console.log("container DOES NOT have platform events api");
        }
      tile.spinnerOff(method);
      // Closes relevant dialog if one was passed in
      if (dialog) {
        tile.closeDialog(dialog);
      }
    }
    else {
      window.container.connectors.sendRequest(
          "workflowMethodExample",
          "1.0",
          method,
          params,
          function (res) {
            // Renders toast that describes the results of the call
            if (res.success) {
              ons.notification.toast("Check the console to see the response!", {
                timeout: 2000,
                animation: "fall"
              });
            } else {
              ons.notification.toast(
                  "An error has occurred, see console for details.",
                  {
                    timeout: 2000,
                    animation: "fall"
                  }
              );
            }

            tile.spinnerOff(method);
            // Closes relevant dialog if one was passed in
            if (dialog) {
              tile.closeDialog(dialog);
            }
            // Logs results
            console.log(`${method}: `, res);
          }
      );
    }
  },
  createCheckboxes: function(){
    const subscribeList = $("#subscribe-dialog-list");
    if (subscribeList) {
      subscribeList.find('ons-list-item').remove();
      tile.tileconfig.eventNames.forEach((name) => {
        const listItem = document.createElement("ons-list-item")
        listItem.innerHTML =
            `<label className="left">
            <ons-checkbox
                id=${name}
                modifier="underbar"
                value=${name}
            ></ons-checkbox>
          </label>
          <label htmlFor=${name}> ${name}</label>`

        subscribeList.append(listItem);
      })
    }
  },
  createSelectOptions: function(){
    const onsSelect = $("#eventName");
    if (onsSelect) {
      const select = onsSelect.find('select')
      select.empty();
      tile.tileconfig.eventNames.forEach((name) => {
        select.append(new Option(name, name));
      })
    }
  }
};

//#region listeners
// Listens for page init and grabs onsen page
document.addEventListener("init", function(event) {
  var page = event.target;

  window.container.tile.data.loadJsonFile("tileconfig.json",(resp)=>{
    tile.tileconfig = resp.data.filecontent;
    console.log("tileconfig ",tile.tileconfig);
  })

  // Handler for buttons tied to methods with no params
  $(page).on("click", ".execute", function(e) {
    var btnId = $(this).attr("id");
    tile.sendRequest(btnId, {});
  });

  // Handler for button that executes method after params are entered
  $(document).on("click", "ons-alert-dialog .execute", function() {
    var inputs = $(this)
      .parent()
      .find("ons-input");
    var checkboxes = $(this).parent().find("ons-checkbox")
    var selectLists = $(this).parent().find("ons-select")
    var btnId = $(this).attr("id");
    var dialog = $(this).closest("ons-alert-dialog");
    var params = tile.getParams(btnId, inputs, checkboxes, selectLists);
    tile.sendRequest(btnId, params, dialog);
  });

  // Handler that opens dialog box for methods that require parameters
  $(document).on("click", ".enter", function() {
    var btnId = $(this)
      .parent()
      .siblings(".left")
      .text();
    tile.createDialog(btnId);
  });

  // Handler for the cancel buttons that close the dialog boxes
  $(document).on("click", "ons-alert-dialog-button", function() {
    var dialog = $(this).closest("ons-alert-dialog");
    tile.closeDialog(dialog);
  });
  //#endregion
});
