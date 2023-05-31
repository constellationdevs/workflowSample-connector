package coop.constellation.connectorservices.workflowexample.controller;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xtensifi.connectorservices.common.events.RealtimeEventService;

import com.xtensifi.connectorservices.common.logging.ConnectorLogging;
import com.xtensifi.connectorservices.common.workflow.ConnectorHubService;
import com.xtensifi.connectorservices.common.workflow.ConnectorRequestData;
import com.xtensifi.connectorservices.common.workflow.ConnectorResponse;
import com.xtensifi.connectorservices.common.workflow.ConnectorState;
import com.xtensifi.dspco.ConnectorMessage;

import coop.constellation.connectorservices.workflowexample.handlers.EditTransactionHandler;
import coop.constellation.connectorservices.workflowexample.handlers.P2pTransferHandler;
import coop.constellation.connectorservices.workflowexample.handlers.RetrieveAccountListHandler;
import coop.constellation.connectorservices.workflowexample.handlers.RetrieveAccountListRefreshHandler;
import coop.constellation.connectorservices.workflowexample.handlers.RetrieveTransactionCategoriesHandler;
import coop.constellation.connectorservices.workflowexample.handlers.RetrieveTransactionListHandler;
import coop.constellation.connectorservices.workflowexample.handlers.RetrieveUserByIdHandler;
import coop.constellation.connectorservices.workflowexample.handlers.RetrieveUserBySocialHandler;
import coop.constellation.connectorservices.workflowexample.handlers.StartTransferHandler;
import coop.constellation.connectorservices.workflowexample.handlers.StopPaymentHandler;
import coop.constellation.connectorservices.workflowexample.handlers.ValidateMemberAccountInfoHandler;
import coop.constellation.connectorservices.workflowexample.helpers.ConnectorResponseEntityBuilder;
import coop.constellation.connectorservices.workflowexample.helpers.RealtimeEvents;
import coop.constellation.connectorservices.workflowexample.helpers.WorkflowHelpers;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import static coop.constellation.connectorservices.workflowexample.helpers.Constants.*;
import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;
@RestController
@CrossOrigin
@Controller
@RequiredArgsConstructor
@RequestMapping("/externalConnector/workflowMethodExample/1.0")
public class WorkflowExampleController extends ConnectorControllerBase {

    @Autowired
    // ConnectorHubService is required for workflow methods
    private ConnectorHubService connectorHubService;

    private final ConnectorLogging clog;
    private final ObjectMapper mapper;
    private final ConnectorResponseEntityBuilder responseEntityBuilder;
    private final RealtimeEventService realtimeEventService;
    private final RealtimeEvents realtimeEvents;
    private final BaseParamsSupplier baseParamsSupplier;


    private final RetrieveAccountListRefreshHandler retrieveAccountListRefreshHandler;
    private final RetrieveAccountListHandler retrieveAccountListHandler;
    private final RetrieveTransactionListHandler retrieveTransactionListHandler;
    private final RetrieveUserBySocialHandler retrieveUserBySocialHandler;
    private final RetrieveUserByIdHandler retrieveUserByIdHandler;
    private final RetrieveTransactionCategoriesHandler retrieveTransactionCategoriesHandler;
    private final EditTransactionHandler editTransactionsHandler;
    private final StartTransferHandler startTransferHandler;
    private final P2pTransferHandler p2pTransferHandler;
    private final StopPaymentHandler stopPaymentHandler;
    private final ValidateMemberAccountInfoHandler validateMemberAccountInfoHandler;


    



    /**
     * This method is required in order for your controller to pass health checks.
     * If the server cannot call awsping and get the expected response your app will
     * not be active.
     *
     * @return the required ping-pong string
     */
    @CrossOrigin
    @GetMapping("/awsping")
    public String getAWSPing() {
        return "{ping: 'pong'}";
    }

    // region retrieveAccountList

    // Workflow methods return a ResponseEntity
    @PostMapping(path = "/retrieveAccountListRefresh", produces = "application/json", consumes = "application/json")
    public ResponseEntity retrieveAccountListRefresh(@RequestBody final ConnectorMessage connectorMessage) {
    
      CompletableFuture<ConnectorMessage> future = connectorHubService
          .executeConnector(connectorMessage, new ConnectorRequestData("kivapublic", "1.0", "getAccountsRefresh"))
          .thenApply(this.handleResponseEntity(retrieveAccountListRefreshHandler))
          .thenApply(connectorHubService.completeAsync())
          .exceptionally(exception -> connectorHubService.handleAsyncFlowError(exception, connectorMessage,
              "Error running submitApplication future: " + exception.getMessage()));
  
      return responseEntityBuilder.build(HttpStatus.OK, future);
  
    }

    @PostMapping(path = "/retrieveAccountList", produces = "application/json", consumes = "application/json")
    public ResponseEntity retrieveAccountList(@RequestBody final ConnectorMessage connectorMessage) {
    
      CompletableFuture<ConnectorMessage> future = connectorHubService
          .executeConnector(connectorMessage, new ConnectorRequestData("kivapublic", "1.0", "getAccounts"))
          .thenApply(this.handleResponseEntity(retrieveAccountListHandler))
          .thenApply(this.handleResponseEntity(retrieveAccountListRefreshHandler))
          .thenApply(connectorHubService.completeAsync())
          .exceptionally(exception -> connectorHubService.handleAsyncFlowError(exception, connectorMessage,
              "Error running submitApplication future: " + exception.getMessage()));
  
      return responseEntityBuilder.build(HttpStatus.OK, future);}
    // endregion


    // region retrieveTransactionList

    // Workflow methods return a ResponseEntity

    @PostMapping(path = "/retrieveTransactionList", produces = "application/json", consumes = "application/json")
    public ResponseEntity retrieveTransactionList(@RequestBody final ConnectorMessage connectorMessage) {
    
      CompletableFuture<ConnectorMessage> future = connectorHubService
          .executeConnector(connectorMessage, new ConnectorRequestData("kivapublic", "1.0", "getTransactions"))
          .thenApply(this.handleResponseEntity(retrieveTransactionListHandler))
          .thenApply(connectorHubService.completeAsync())
          .exceptionally(exception -> connectorHubService.handleAsyncFlowError(exception, connectorMessage,
              "Error running submitApplication future: " + exception.getMessage()));
  
      return responseEntityBuilder.build(HttpStatus.OK, future);}
    // endregion
    

    // region retrieveUserBySocial

    // Workflow methods return a ResponseEntity

    @PostMapping(path = "/retrieveUserBySocial", produces = "application/json", consumes = "application/json")
    public ResponseEntity retrieveUserBySocial(@RequestBody final ConnectorMessage connectorMessage) {
    
      CompletableFuture<ConnectorMessage> future = connectorHubService
          .executeConnector(connectorMessage, new ConnectorRequestData("kivapublic", "1.0", "getPartyBySSN"))
          .thenApply(this.handleResponseEntity(retrieveUserBySocialHandler))
          .thenApply(connectorHubService.completeAsync())
          .exceptionally(exception -> connectorHubService.handleAsyncFlowError(exception, connectorMessage,
              "Error running submitApplication future: " + exception.getMessage()));
  
      return responseEntityBuilder.build(HttpStatus.OK, future);}

    
    // // endregion

    // // region retrieveUserById

    // // Workflow methods return a ResponseEntity

    @PostMapping(path = "/retrieveUserById", produces = "application/json", consumes = "application/json")
    public ResponseEntity retrieveUserById(@RequestBody final ConnectorMessage connectorMessage) {
    
      CompletableFuture<ConnectorMessage> future = connectorHubService
          .executeConnector(connectorMessage, new ConnectorRequestData("kivapublic", "1.0", "getPartyById"))
          .thenApply(this.handleResponseEntity(retrieveUserByIdHandler))
          .thenApply(connectorHubService.completeAsync())
          .exceptionally(exception -> connectorHubService.handleAsyncFlowError(exception, connectorMessage,
              "Error running submitApplication future: " + exception.getMessage()));
  
      return responseEntityBuilder.build(HttpStatus.OK, future);
  
    }
    // // endregion

    // // region retrieveTransactionCategories

    // // Workflow methods return a ResponseEntity

    @PostMapping(path = "/retrieveTransactionCategories", produces = "application/json", consumes = "application/json")
    public ResponseEntity retrieveTransactionCategories(@RequestBody final ConnectorMessage connectorMessage) {
    
      CompletableFuture<ConnectorMessage> future = connectorHubService
          .executeConnector(connectorMessage, new ConnectorRequestData("kivapublic", "1.0", "getTransactionCategories"))
          .thenApply(this.handleResponseEntity(retrieveTransactionCategoriesHandler))
          .thenApply(connectorHubService.completeAsync())
          .exceptionally(exception -> connectorHubService.handleAsyncFlowError(exception, connectorMessage,
              "Error running submitApplication future: " + exception.getMessage()));
  
      return responseEntityBuilder.build(HttpStatus.OK, future);
  
    }
    // // endregion

    // // region editTransaction

    // // Workflow methods return a ResponseEntity

    @PostMapping(path = "/editTransactions", produces = "application/json", consumes = "application/json")
    public ResponseEntity editTransactions(@RequestBody final ConnectorMessage connectorMessage) {
    
      CompletableFuture<ConnectorMessage> future = connectorHubService
          .executeConnector(connectorMessage, new ConnectorRequestData("kivapublic", "1.0", "updateTransactions"))
          .thenApply(this.handleResponseEntity(editTransactionsHandler))
          .thenApply(connectorHubService.completeAsync())
          .exceptionally(exception -> connectorHubService.handleAsyncFlowError(exception, connectorMessage,
              "Error running submitApplication future: " + exception.getMessage()));
  
      return responseEntityBuilder.build(HttpStatus.OK, future);
  
    }
    // // endregion


    // // region startTransfer

    // // Workflow methods return a ResponseEntity
 
    @PostMapping(path = "/startTransfer", produces = "application/json", consumes = "application/json")
    public ResponseEntity startTransfer(@RequestBody final ConnectorMessage connectorMessage) {
    
      CompletableFuture<ConnectorMessage> future = connectorHubService
          .executeConnector(connectorMessage, new ConnectorRequestData("kivapublic", "1.0", "createInternalTransfer"))
          .thenApply(this.handleResponseEntity(startTransferHandler))
          .thenApply(connectorHubService.completeAsync())
          .exceptionally(exception -> connectorHubService.handleAsyncFlowError(exception, connectorMessage,
              "Error running submitApplication future: " + exception.getMessage()));
  
      return responseEntityBuilder.build(HttpStatus.OK, future);
  
    }
    // // endregion

    // // region p2pTransfer
    @PostMapping(path = "/p2pTransfer", produces = "application/json", consumes = "application/json")
    public ResponseEntity p2pTransfer(@RequestBody final ConnectorMessage connectorMessage) {
    
      CompletableFuture<ConnectorMessage> future = connectorHubService
         .executeConnector(connectorMessage, new ConnectorRequestData("kivapublic", "1.0", "personToPersonTransfer"))
          .thenApply(this.handleResponseEntity(p2pTransferHandler))
          .thenApply(connectorHubService.completeAsync())
          .exceptionally(exception -> connectorHubService.handleAsyncFlowError(exception, connectorMessage,
              "Error running submitApplication future: " + exception.getMessage()));
  
      return responseEntityBuilder.build(HttpStatus.OK, future);
  
    }
    // // endregion

    // // region stopPayment
    @PostMapping(path = "/stopPayment", produces = "application/json", consumes = "application/json")
    public ResponseEntity stopPayment(@RequestBody final ConnectorMessage connectorMessage) {
    
      CompletableFuture<ConnectorMessage> future = connectorHubService
          .executeConnector(connectorMessage, new ConnectorRequestData("kivapublic", "1.0", "createStopPayment"))
          .thenApply(this.handleResponseEntity(stopPaymentHandler))
          .thenApply(connectorHubService.completeAsync())
          .exceptionally(exception -> connectorHubService.handleAsyncFlowError(exception, connectorMessage,
              "Error running submitApplication future: " + exception.getMessage()));
  
      return responseEntityBuilder.build(HttpStatus.OK, future);
  
    }
    // // endregion

    // region Validate Member Account Info
    @PostMapping(path = "/validateMemberAccountInfo", produces = "application/json", consumes = "application/json")
    public ResponseEntity validateMemberAccountInfo(@RequestBody final ConnectorMessage connectorMessage) {
    
      CompletableFuture<ConnectorMessage> future = connectorHubService
          .executeConnector(connectorMessage, new ConnectorRequestData("kivapublic", "1.0", "validateMemberAccountInfo"))
          .thenApply(this.handleResponseEntity(validateMemberAccountInfoHandler))
          .thenApply(connectorHubService.completeAsync())
          .exceptionally(exception -> connectorHubService.handleAsyncFlowError(exception, connectorMessage,
              "Error running submitApplication future: " + exception.getMessage()));
  
      return responseEntityBuilder.build(HttpStatus.OK, future);
  
    }

    // // region multiCall
    // // This call demonstrates an example of making two calls with logic in between
    @CrossOrigin
    @PostMapping(path = "/multiCall", consumes = "application/json", produces = "application/json")
    public ResponseEntity<String> multiCall(@RequestBody final ConnectorMessage connectorMessage) {
        ResponseEntity.BodyBuilder responseEntity = ResponseEntity.status(HttpStatus.OK);

        connectorHubService
                .executeConnector(connectorMessage, new ConnectorRequestData("kivapublic", "1.0", "getAccounts"))
                // .thenApplyAsync(connectorHubService.waitForConnectorResponse())
                .thenApplyAsync(connectorState -> connectorHubService.prepareNextConnector(
                        new ConnectorRequestData("kivapublic", "1.0", "getTransactions"), connectorState))
                .thenApply(this.getMultiCallParams()).thenApplyAsync(connectorHubService.callConnectorUsingState())
                .thenApplyAsync(connectorHubService.waitForConnectorResponse()).thenApply(this.processMultiCall())
                .thenApplyAsync(connectorHubService.completeAsync())
                .exceptionally(exception -> connectorHubService.handleAsyncFlowError(exception, connectorMessage,
                        "Error running multiCall: " + exception.getMessage()));

        return responseEntity.build();
    }

    private Function<ConnectorState, ConnectorState> processMultiCall() {
        return connectorState -> {
            clog.info(connectorState.getConnectorMessage(), "processMultiCall");

            // Gather the list of responses, when only making 1 kiva call there should only
            // be one reponse
            List<ConnectorResponse> connectorResponseList = connectorState.getConnectorResponseList().getResponses();

            // This is a placeholder incase no responses are returned, this should be
            // updated with a default message when responses are found
            String resp = "{\"response\": 1}";
            for (ConnectorResponse connectorResponse : connectorResponseList) {

                // This is how you retrieve the name of the connector
                String name = connectorResponse.getConnectorRequestData().getConnectorName();
                clog.info(connectorState.getConnectorMessage(), name);

                // This is how you capture the response
                String data = connectorResponse.getResponse();

                // Parse the response however you see fit
                resp = "{\"response\": " + data + "}";
                clog.info(connectorState.getConnectorMessage(), resp);
            }

            // This is required, and is how you set the response for a workflow method
            connectorState.setResponse(resp);
            return connectorState;
        };
    }

    private Function<ConnectorState, ConnectorState> getMultiCallParams() {
        return connectorState -> {
            ConnectorMessage connectorMessage = connectorState.getConnectorMessage();
            List<ConnectorResponse> responseList = connectorState.getConnectorResponseList().getResponses();
            // This example is only expecting 1 response
            if (responseList.size() == 1) {
                clog.info(connectorState.getConnectorMessage(), "Start Parsing");

                String response = responseList.get(0).getResponse();
                clog.info(connectorState.getConnectorMessage(), response);

                ObjectMapper jsonMap = new ObjectMapper();
                String accountID = "";
                try {
                    JsonNode component = jsonMap.readTree(response);
                    JsonNode depositArray = component.at("/accountContainer/depositMessage/depositList/deposit");
                    clog.info(connectorState.getConnectorMessage(), depositArray.toString());
                    accountID = depositArray.get(1).get("accountId").asText();
                } catch (Exception e) {
                    clog.error(connectorMessage, "failed to get accountid");
                }
                // add accountId as param for the getTransactions call
                connectorState.getConnectorRequestParams().addNameValue("accountId", accountID);
            }
            return connectorState;
        };
    }
    // endregion

    @CrossOrigin
    @PostMapping(path = "/sendRealtimeEvent", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
    public ConnectorMessage sendRealtimeEvent(@RequestBody final String connectorJson) throws IOException {
        ConnectorMessage connectorMessage = mapper.readValue(connectorJson, ConnectorMessage.class);
        Map<String, String> parms = ConnectorControllerBase.getAllParams(connectorMessage, baseParamsSupplier.get());
        String eventName = parms.get(EVENT_NAME);
        String accounts = parms.get(ACCOUNTS);
        boolean success = true;
        String message = "";
        if(eventName!=null && !eventName.isEmpty() && accounts!=null && !accounts.isEmpty()) {
            List<String> affectedItems = mapper.readValue(accounts, new TypeReference<>() {});
            String accountIdList = "";
            for(String id : affectedItems){
                accountIdList = accountIdList + id + ",";
            }
            try {
                realtimeEvents.send(CDP_SOURCE, eventName, affectedItems, connectorMessage, clog, realtimeEventService);
                message = String.format("Realtime event sent successfully for %s, affected items %s", eventName, accountIdList);
                clog.info(connectorMessage, message);
            }
            catch(Exception e){
                message = String.format("Realtime event was unsuccessful for %s, affected items %s ", eventName, accountIdList) + e.getMessage();
                clog.error(connectorMessage, message);
            }
        }
        else{
            message = String.format("Something is missing when sending event for event name: %s, affected items: %s ", eventName);
            clog.error(connectorMessage, message);
        }
        String response = "{\"response\":{\"success\":"+success+", \"message\": \""+message+"\"}}";
        clog.info(connectorMessage, "this is the response " + response);
        connectorMessage.setResponse(response);
        return connectorMessage;
    }

}