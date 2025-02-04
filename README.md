# Zarban-Subgraph

This project is a subgraph for the Zarban blockchain protocol. It indexes and queries data from the Zarban protocol (the Liquidity Market and Stablecoin System both), providing a GraphQL API for accessing the protocol's data.

## Table of Contents

- [Zarban-Subgraph](#zarban-subgraph)
  - [Table of Contents](#table-of-contents)
  - [Introduction](#introduction)
  - [Installation](#installation)
  - [Usage](#usage)
    - [Query Example 1: Get User Collateral and Debt Positions](#query-example-1-get-user-collateral-and-debt-positions)
    - [Query Example 2: Get All Deposits for a User](#query-example-2-get-all-deposits-for-a-user)
  - [Schema](#schema)
  - [Contributing](#contributing)

## Introduction

The Zarban-Subgraph is designed to index and query data from the Zarban blockchain protocol. It provides a GraphQL API that allows developers to access various data points from the protocol, such as collateral and debt positions, user transactions, protocol revenues, and more.

## Installation

To install and run the subgraph locally, follow these steps:

1. Clone the repository:
    ```sh
    git clone https://github.com/zarbanio/subgraph.git
    cd subgraph
    ```

2. Install dependencies:
    ```sh
    npm install
    ```

3. Generate the subgraph code:
    ```sh
    npm codegen
    ```

4. Deploy the subgraph:
    ```sh
    graph auth --studio YOUR_DEPLOY_KEY
    graph deploy SUBGRAPH_SLUG
    ```

## Usage

Once the subgraph is deployed, you can query the data using the GraphQL API. Here are some example queries:

### Query Example 1: Get User Collateral and Debt Positions
```graphql
{
  urn(id:"userAddress-ilk") {
    ownerAddress
    collateralLocked
    debt
    availableToMint
    availableToWithdraw
    loanToValue
    liquidationPrice
    collateralizationRatio
    normalizedDebt
    ilk {
      name
      annualStabilityFee
      minimumCollateralizationRatio
      collateralToken {
        name
        lastPriceTOMAN
        lastPriceUSD
      }
    }
  }
}
```

### Query Example 2: Get All Deposits for a User
```graphql
{
  account(id: "userAddress") {
    deposits {
      id
      asset{
        id
        name
      }
      amount
      timestamp
    }
  }
}
```

## Schema

You can check out the data structures and relationships indexed from the Zarban protocol in the `schema.graphql` file.

These are the most important entities:

- Liquidity Market specific entities
  - _DefaultOracle
- Stablecoin System specific entities
  - _Ilk
  - _Urn
- Both
  - Account
  - Protocol
  - Market
  - UsageMetricsDailySnapshot
  - UsageMetricsHourlySnapshot
  - FinancialsDailySnapshot
  - MarketDailySnapshot
  - MarketHourlySnapshot
  - Event (Deposit, Withdraw, Borrow, Repay, Transfer, FlashLoan)
  - Position

There are some other entities, but they are mostly helpers and not as useful as the ones mentioned above. Feel free to use them if needed.
Entities whose names begin with “Lm” are specific to the liquidity market only.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request if you have any improvements or bug fixes.
