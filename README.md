# Workflow Method Sample

Tile and connector to test kivapublic endpoints and the sending of realtime events

## Connector Endpoints

| Connector Endpoint            | Kiva permissions required    |
| :---------------------------- | :--------------------------- |
| retrieveAccountList           | getAccounts                  |
| retrieveAccountList           | getAccountsRefresh           |
| retrieveTransactionList       | getTransactions              |
| retrieveTransactionCategories | getTransactionCategories     |
| editTransaction               | updateTransaction            |
| stopPayment                   | createStopPayment            |
| multiCall                     | getAccounts, getTransactions |
| retrieveUserBySocial          | getPartyBySSN                |
| retrieveUserById              | getPartyById                 |
| validateMemberAccountInfo     | validateMemberAccountInfo    |
| startTransfer                 | createInternalTransfer       |
| p2pTransfer                   | personToPersonTransfer       |
| subscribe                     | none                         |
| sendRealtimeEvent             | none                         |

## tileconfig.json

The following realtime events are currently supported in the platform -- you can add these to the tileconfig to allow them to be selected for subscribing to events or for sending events.

```json
{
  "eventNames": [
    "platform_account_transactionadded",
    "account_transactionadded",
    "account_balanceupdated",
    "account_nicknameupdated",
    "account_refreshall"
  ]
}
```

## How to test locally

When running the tile locally, uncomment the top section of the index.html file between the comments
`<!-- LOCAL DEVELOPMENT ONLY -->`

```html
<!--    <script src="https://assets.cdp.wiki/cdp_bundle.js?100"></script> -->
<!--    <link-->
<!--      rel="stylesheet"-->
<!--      href="https://assets.cdp.wiki/cdp_defaultTheme.css?100"-->
<!--      type="text/css"-->
<!--    />-->
<!--    <link rel="stylesheet" href="tile.css" />-->
<!--    <script src="tile.js"></script>-->
```

You cannot run the connector locally without a proxy because the endpoints require platform dependencies like kivapublic and redis.

## How to deploy

### Tile

Ensure that the script and link tags in the index.html file between `<!-- LOCAL DEVELOPMENT ONLY -->` are removed or commented out before deploying the tile
Upload the following files to the tile project in the portal:
tile.js
index.html
tileconfig.json
tilestrings-en.json
tile.less

### Connector

## Local Structure

basic_connector_sample/
├─ Dockerfile/
├─ lib/
├─ pom.xml/
├─ readme
├─ src/
├─ target/

- Make sure the name and version of your connector is reflected accurately in all of the connector assets

- The lib/ and src/ directory should be packaged(compressed) together at the same level, in a [externalconnector.zip] for upload

  - Any other connector assets _excluding_ those listed in the Local Connector Structure should also be included in this zipped folder.

- The portal is only expecting the following required files at the time of upload [externalconnector.zip] [pom.xml] [Dockerfile]

## Portal Upload Structure

externalconnector.zip/
pom.xml/
Dockerfile/

## Spring profiles

Spring Profiles are used to activate different implementations of ConnectorLogging Beans. If you choose to use VS Code as your IDE, a lauch.json file is included that will make use of the "local" profile to print logs to the IDE output console.
