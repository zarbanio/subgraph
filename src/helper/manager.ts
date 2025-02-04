import {
  Address,
  Bytes,
  BigInt,
  ethereum,
  BigDecimal,
  log,
} from "@graphprotocol/graph-ts";
import {
  Borrow,
  Deposit,
  LmFEE,
  Flashloan,
  LmInterestRate,
  Protocol,
  Liquidate,
  Market,
  LmOracle,
  Repay,
  LmRevenueDetail,
  LmRewardToken,
  Token,
  Transfer,
  Withdraw,
} from "../../generated/schema";
import { AccountManager } from "./account";
import {
  activityCounter,
  BIGDECIMAL_ZERO,
  exponentToBigDecimal,
  FeeType,
  INT_ONE,
  PositionSide,
  Transaction,
  TransactionType,
  insert,
} from "./constants";
import { SnapshotManager } from "./snapshots";
import { TokenManager } from "./token";
import { PositionManager } from "./position";
import { getOrCreateLendingProtocol, getOrCreateMarket } from "./getters";
import { getAssetPriceInUSDC } from "../mapping";

/**
 * This file contains the DataManager, which is used to
 * make all of the storage changes that occur in a protocol.
 *
 * You can think of this as an abstraction so the developer doesn't
 * need to think about all of the detailed storage changes that occur.
 *
 **/

export class ProtocolData {
  constructor(
    public readonly protocolID: string,
    public readonly protocol: string,
    public readonly name: string,
    public readonly slug: string,
    public readonly network: string,
    public readonly lendingType: string,
    public readonly lenderPermissionType: string | null,
    public readonly borrowerPermissionType: string | null,
    public readonly poolCreatorPermissionType: string | null,
    public readonly collateralizationType: string | null,
    public readonly riskType: string | null
  ) {}
}

export class RewardData {
  constructor(
    public readonly rewardToken: LmRewardToken,
    public readonly rewardTokenEmissionsAmount: BigInt,
    public readonly rewardTokenEmissionsUSD: BigDecimal
  ) {}
}

export class DataManager {
  private event!: ethereum.Event;
  private protocol!: Protocol;
  private market!: Market;
  private inputToken!: TokenManager;
  private oracle!: LmOracle;
  private snapshots!: SnapshotManager;

  constructor(marketID: string, inputToken: string, event: ethereum.Event) {
    this.protocol = this.getOrCreateProtocol();
    this.inputToken = new TokenManager(inputToken, event);

    const name = "Lending Pool - ".concat(this.inputToken.getToken().name);
    let _market = getOrCreateMarket(
      marketID,
      name,
      this.inputToken.getToken().id,
      event.block.number,
      event.block.timestamp
    );
    this.market = _market;
    this.event = event;

    // load snapshots
    this.snapshots = new SnapshotManager(event, this.protocol, this.market);

    // load oracle and update asset price
    if (this.market.oracle) {
      this.oracle = LmOracle.load(this.market.oracle!)!;
      let assetPriceUSD =
        getAssetPriceInUSDC(
          Address.fromString(inputToken),
          this.getOracleAddress(),
        ) || BIGDECIMAL_ZERO;
      this.inputToken.updatePrice(assetPriceUSD);
      this.market.inputTokenPriceUSD = assetPriceUSD;
      this.saveMarket();
    }
  }

  /////////////////
  //// Getters ////
  /////////////////

  getOrCreateProtocol(): Protocol {
    return getOrCreateLendingProtocol();
  }

  getMarket(): Market {
    return this.market;
  }

  saveMarket(): void {
    this.market.save();
  }

  // only update when updating the supply/borrow index
  updateIndexLastMarketTimestamp(): void {
    this.market.indexLastUpdatedTimestamp = this.event.block.timestamp;
    this.saveMarket();
  }

  getProtocol(): Protocol {
    return this.protocol;
  }

  getInputToken(): Token {
    return this.inputToken.getToken();
  }

  getOrCreateOracle(
    oracleAddress: Address,
    isUSD: boolean,
    source?: string
  ): LmOracle {
    const oracleID = this.market.id.concat(this.market.inputToken);
    let oracle = LmOracle.load(oracleID);
    if (!oracle) {
      oracle = new LmOracle(oracleID);
      oracle.market = this.market.id;
      oracle.blockCreated = this.event.block.number;
      oracle.timestampCreated = this.event.block.timestamp;
      oracle.isActive = true;
    }
    oracle.oracleAddress = oracleAddress;
    oracle.isUSD = isUSD;
    if (source) {
      oracle.oracleSource = source;
    }
    oracle.save();
    this.oracle = oracle;

    return oracle;
  }

  getOracleAddress(): Address {
    return Address.fromBytes(this.oracle.oracleAddress);
  }

  getOrUpdateRate(
    rateSide: string,
    rateType: string,
    interestRate: BigDecimal
  ): LmInterestRate {
    const interestRateID = rateSide
      .concat("-")
      .concat(rateType)
      .concat("-")
      .concat(this.market.id);
    let rate = LmInterestRate.load(interestRateID);
    if (!rate) {
      rate = new LmInterestRate(interestRateID);
      rate.side = rateSide;
      rate.type = rateType;
    }
    rate.rate = interestRate;
    rate.save();

    let marketRates = this.market.rates;
    if (!marketRates) {
      marketRates = [];
    }

    if (marketRates.indexOf(interestRateID) == -1) {
      marketRates.push(interestRateID);
    }
    this.market.rates = marketRates;
    this.saveMarket();

    return rate;
  }

  getOrUpdateFee(feeType: string, rate: BigDecimal | null = null): LmFEE {
    let fee = LmFEE.load(feeType);
    if (!fee) {
      fee = new LmFEE(feeType);
      fee.type = feeType;
    }

    fee.rate = rate;
    fee.save();

    let protocolFees = this.protocol.fees;
    if (!protocolFees) {
      protocolFees = [];
    }

    if (protocolFees.indexOf(feeType) == -1) {
      protocolFees.push(feeType);
    }
    this.protocol.fees = protocolFees;
    this.protocol.save();

    return fee;
  }

  getAddress(): Address {
    return Address.fromString(this.market.id);
  }

  getOrCreateRevenueDetail(id: string, isMarket: boolean): LmRevenueDetail {
    let details = LmRevenueDetail.load(id);
    if (!details) {
      details = new LmRevenueDetail(id);
      details.sources = [];
      details.amountsUSD = [];
      details.save();

      if (isMarket) {
        this.market.revenueDetail = details.id;
        this.saveMarket();
      } else {
        this.protocol.revenueDetail = details.id;
        this.protocol.save();
      }
    }

    return details;
  }

  //////////////////
  //// Creators ////
  //////////////////

  createDeposit(
    asset: string,
    account: string,
    amount: BigInt,
    amountUSD: BigDecimal,
    newBalance: BigInt,
    interestType: string | null = null,
    principal: BigInt | null = null
  ): Deposit {
    const depositor = new AccountManager(account);
    if (depositor.isNewUser()) {
      this.protocol.cumulativeUniqueUsers += INT_ONE;
      this.protocol.save();
    }
    depositor.countDeposit();
    const position = new PositionManager(
      depositor.getAccount(),
      this.market,
      PositionSide.COLLATERAL,
      interestType
    );
    position.addPosition(
      this.event,
      asset,
      this.protocol,
      newBalance,
      TransactionType.DEPOSIT,
      this.market.inputTokenPriceUSD,
      principal
    );

    const deposit = new Deposit(
      this.event.transaction.hash
        .concatI32(this.event.logIndex.toI32())
        .concatI32(Transaction.DEPOSIT)
        .toHexString()
    );
    deposit.hash = this.event.transaction.hash;
    deposit.nonce = this.event.transaction.nonce;
    deposit.logIndex = this.event.logIndex.toI32();
    deposit.gasPrice = this.event.transaction.gasPrice;
    deposit.gasUsed = this.event.receipt ? this.event.receipt!.gasUsed : null;
    deposit.gasLimit = this.event.transaction.gasLimit;
    deposit.blockNumber = this.event.block.number;
    deposit.timestamp = this.event.block.timestamp;
    deposit.account = account;
    deposit.market = this.market.id;
    deposit.position = position.getPositionID()!;
    deposit.asset = asset;
    deposit.amount = amount;
    deposit.amountUSD = amountUSD;
    deposit.save();

    this.updateTransactionData(TransactionType.DEPOSIT, amount, amountUSD);
    this.updateUsageData(TransactionType.DEPOSIT, account);

    return deposit;
  }

  createWithdraw(
    asset: string,
    account: string,
    amount: BigInt,
    amountUSD: BigDecimal,
    newBalance: BigInt,
    interestType: string | null = null,
    principal: BigInt | null = null
  ): Withdraw | null {
    const withdrawer = new AccountManager(account);
    if (withdrawer.isNewUser()) {
      this.protocol.cumulativeUniqueUsers += INT_ONE;
      this.protocol.save();
    }
    withdrawer.countWithdraw();
    const position = new PositionManager(
      withdrawer.getAccount(),
      this.market,
      PositionSide.COLLATERAL,
      interestType
    );
    position.subtractPosition(
      this.event,
      this.protocol,
      newBalance,
      TransactionType.WITHDRAW,
      this.market.inputTokenPriceUSD,
      principal
    );
    const positionID = position.getPositionID();
    if (!positionID) {
      log.error(
        "[createWithdraw] positionID is null for market: {} account: {}",
        [this.market.id, account]
      );
      return null;
    }

    const withdraw = new Withdraw(
      this.event.transaction.hash
        .concatI32(this.event.logIndex.toI32())
        .concatI32(Transaction.WITHDRAW)
        .toHexString()
    );
    withdraw.hash = this.event.transaction.hash;
    withdraw.nonce = this.event.transaction.nonce;
    withdraw.logIndex = this.event.logIndex.toI32();
    withdraw.gasPrice = this.event.transaction.gasPrice;
    withdraw.gasUsed = this.event.receipt ? this.event.receipt!.gasUsed : null;
    withdraw.gasLimit = this.event.transaction.gasLimit;
    withdraw.blockNumber = this.event.block.number;
    withdraw.timestamp = this.event.block.timestamp;
    withdraw.account = account;
    withdraw.market = this.market.id;
    withdraw.position = positionID!;
    withdraw.asset = asset;
    withdraw.amount = amount;
    withdraw.amountUSD = amountUSD;
    withdraw.save();

    this.updateTransactionData(TransactionType.WITHDRAW, amount, amountUSD);
    this.updateUsageData(TransactionType.WITHDRAW, account);

    return withdraw;
  }

  createBorrow(
    asset: string,
    account: string,
    amount: BigInt,
    amountUSD: BigDecimal,
    newBalance: BigInt,
    tokenPriceUSD: BigDecimal, // used for different borrow token in CDP
    interestType: string | null = null,
    principal: BigInt | null = null
  ): Borrow {
    const borrower = new AccountManager(account);
    if (borrower.isNewUser()) {
      this.protocol.cumulativeUniqueUsers += INT_ONE;
      this.protocol.save();
    }
    borrower.countBorrow();
    const position = new PositionManager(
      borrower.getAccount(),
      this.market,
      PositionSide.BORROWER,
      interestType
    );
    position.addPosition(
      this.event,
      asset,
      this.protocol,
      newBalance,
      TransactionType.BORROW,
      tokenPriceUSD,
      principal
    );

    const borrow = new Borrow(
      this.event.transaction.hash
        .concatI32(this.event.logIndex.toI32())
        .concatI32(Transaction.BORROW)
        .toHexString()
    );
    borrow.hash = this.event.transaction.hash;
    borrow.nonce = this.event.transaction.nonce;
    borrow.logIndex = this.event.logIndex.toI32();
    borrow.gasPrice = this.event.transaction.gasPrice;
    borrow.gasUsed = this.event.receipt ? this.event.receipt!.gasUsed : null;
    borrow.gasLimit = this.event.transaction.gasLimit;
    borrow.blockNumber = this.event.block.number;
    borrow.timestamp = this.event.block.timestamp;
    borrow.account = account;
    borrow.market = this.market.id;
    borrow.position = position.getPositionID()!;
    borrow.asset = asset;
    borrow.amount = amount;
    borrow.amountUSD = amountUSD;
    borrow.save();

    this.updateTransactionData(TransactionType.BORROW, amount, amountUSD);
    this.updateUsageData(TransactionType.BORROW, account);

    return borrow;
  }

  createRepay(
    asset: string,
    account: string,
    amount: BigInt,
    amountUSD: BigDecimal,
    newBalance: BigInt,
    tokenPriceUSD: BigDecimal, // used for different borrow token in CDP
    interestType: string | null = null,
    principal: BigInt | null = null
  ): Repay | null {
    const repayer = new AccountManager(account);
    if (repayer.isNewUser()) {
      this.protocol.cumulativeUniqueUsers += INT_ONE;
      this.protocol.save();
    }
    repayer.countRepay();
    const position = new PositionManager(
      repayer.getAccount(),
      this.market,
      PositionSide.BORROWER,
      interestType
    );
    position.subtractPosition(
      this.event,
      this.protocol,
      newBalance,
      TransactionType.REPAY,
      tokenPriceUSD,
      principal
    );

    const positionID = position.getPositionID();
    if (!positionID) {
      log.error("[createRepay] positionID is null for market: {} account: {}", [
        this.market.id,
        account,
      ]);
      return null;
    }

    const repay = new Repay(
      this.event.transaction.hash
        .concatI32(this.event.logIndex.toI32())
        .concatI32(Transaction.REPAY)
        .toHexString()
    );
    repay.hash = this.event.transaction.hash;
    repay.nonce = this.event.transaction.nonce;
    repay.logIndex = this.event.logIndex.toI32();
    repay.gasPrice = this.event.transaction.gasPrice;
    repay.gasUsed = this.event.receipt ? this.event.receipt!.gasUsed : null;
    repay.gasLimit = this.event.transaction.gasLimit;
    repay.blockNumber = this.event.block.number;
    repay.timestamp = this.event.block.timestamp;
    repay.account = account;
    repay.market = this.market.id;
    repay.position = positionID!;
    repay.asset = asset;
    repay.amount = amount;
    repay.amountUSD = amountUSD;
    repay.save();

    this.updateTransactionData(TransactionType.REPAY, amount, amountUSD);
    this.updateUsageData(TransactionType.REPAY, account);

    return repay;
  }

  /**
   * Creates a Liquidate entity for a liquidation, update liquidatee and liquidator positions
   *
   * @param asset The collateral asset that is seized by the protocol and transfered to the liquidator.
   * @param liquidator The liquidator.
   * @param liquidatee The borrower that is liquidated.
   * @param amount The amount of asset being liquidated.
   * @param amountUSD The amount in USD.
   * @param profitUSD Liquidator's profit from the liquidation.
   * @param newCollateralBalance The liquidatee's new borrowing balance after the liquidation (usually ZERO).
   * @param interestType Optional - The InterestType of liquidatee's position (FIXED, VARIABLE, etc.).
   * @param subtractBorrowerPosition - whether to subtract borrower/debt position involved in the liquidation
   * @returns A Liquidate entity or null
   */
  createLiquidate(
    asset: string,
    debtTokenId: string,
    liquidator: Address,
    liquidatee: Address,
    amount: BigInt,
    amountUSD: BigDecimal,
    profitUSD: BigDecimal,
    newCollateralBalance: BigInt,
    newBorrowerBalance: BigInt,
    interestType: string | null = null,
    subtractBorrowerPosition: boolean = true,
    principal: BigInt | null = null
  ): Liquidate | null {
    const positions: string[] = []; // positions touched by this liquidation
    const liquidatorAccount = new AccountManager(liquidator.toHexString());
    if (liquidatorAccount.isNewUser()) {
      this.protocol.cumulativeUniqueUsers += INT_ONE;
      this.protocol.save();
    }
    liquidatorAccount.countLiquidate();
    // Note: Be careful, some protocols might give the liquidated collateral to the liquidator
    //       in collateral in the market. But that is not always the case so we don't do it here.

    const liquidateeAccount = new AccountManager(liquidatee.toHexString());
    liquidateeAccount.countLiquidation();
    const collateralPosition = new PositionManager(
      liquidateeAccount.getAccount(),
      this.market,
      PositionSide.COLLATERAL,
      interestType
    );

    const collateralPositionID = collateralPosition.subtractPosition(
      this.event,
      this.protocol,
      newCollateralBalance,
      TransactionType.LIQUIDATE,
      this.market.inputTokenPriceUSD,
      principal
    );
    if (!collateralPositionID) {
      log.error(
        "[createLiquidate] positionID is null for market: {} account: {}",
        [this.market.id, liquidatee.toHexString()]
      );

      return null;
    }
    positions.push(collateralPositionID!);
    // we may want to do call subtractPosition outside this function
    // to close both stable and variable borrowing positions, e.g.
    // in Zarban-forks
    if (subtractBorrowerPosition) {
      const debtMarket = Market.load(debtTokenId);
      if (!debtMarket) {
        log.error("[createLiquidate] market {} not found", [debtTokenId]);
        return null;
      }
      const borrowerPosition = new PositionManager(
        liquidateeAccount.getAccount(),
        debtMarket,
        PositionSide.BORROWER,
        interestType
      );

      const borrowerPositionID = borrowerPosition.subtractPosition(
        this.event,
        this.protocol,
        newBorrowerBalance,
        TransactionType.LIQUIDATE,
        debtMarket.inputTokenPriceUSD
      );
      if (!borrowerPositionID) {
        log.error(
          "[createLiquidate] positionID is null for market: {} account: {}",
          [debtMarket.id, liquidatee.toHexString()]
        );
        return null;
      }
      positions.push(borrowerPositionID!);
    }

    // Note:
    //  - liquidatees are not considered users since they are not spending gas for the transaction
    //  - It is possible in some protocols for the liquidator to incur a position if they are transferred collateral tokens
    const liquidate = new Liquidate(
      this.event.transaction.hash
        .concatI32(this.event.logIndex.toI32())
        .concatI32(Transaction.LIQUIDATE)
        .toHexString()
    );
    liquidate.hash = this.event.transaction.hash;
    liquidate.nonce = this.event.transaction.nonce;
    liquidate.logIndex = this.event.logIndex.toI32();
    liquidate.gasPrice = this.event.transaction.gasPrice;
    liquidate.gasUsed = this.event.receipt ? this.event.receipt!.gasUsed : null;
    liquidate.gasLimit = this.event.transaction.gasLimit;
    liquidate.blockNumber = this.event.block.number;
    liquidate.timestamp = this.event.block.timestamp;
    liquidate.liquidator = liquidator.toHexString();
    liquidate.liquidatee = liquidatee.toHexString();
    liquidate.market = this.market.id;
    liquidate.positions = positions;
    liquidate.asset = asset;
    liquidate.amount = amount;
    liquidate.amountUSD = amountUSD;
    liquidate.profitUSD = profitUSD;
    liquidate.save();

    this.updateTransactionData(TransactionType.LIQUIDATE, amount, amountUSD);
    this.updateUsageData(TransactionType.LIQUIDATEE, liquidatee.toHexString());
    this.updateUsageData(TransactionType.LIQUIDATOR, liquidator.toHexString());

    return liquidate;
  }

  createTransfer(
    asset: Bytes,
    sender: Address,
    receiver: Address,
    amount: BigInt,
    amountUSD: BigDecimal,
    senderNewBalance: BigInt,
    receiverNewBalance: BigInt,
    interestType: string | null = null,
    senderPrincipal: BigInt | null = null,
    receiverPrincipal: BigInt | null = null
  ): Transfer | null {
    const transferrer = new AccountManager(sender.toHexString());
    if (transferrer.isNewUser()) {
      this.protocol.cumulativeUniqueUsers += INT_ONE;
      this.protocol.save();
    }
    transferrer.countTransfer();
    const transferrerPosition = new PositionManager(
      transferrer.getAccount(),
      this.market,
      PositionSide.COLLATERAL,
      interestType
    );
    transferrerPosition.subtractPosition(
      this.event,
      this.protocol,
      senderNewBalance,
      TransactionType.TRANSFER,
      this.market.inputTokenPriceUSD,
      senderPrincipal
    );
    const positionID = transferrerPosition.getPositionID();
    if (!positionID) {
      log.error(
        "[createTransfer] positionID is null for market: {} account: {}",
        [this.market.id, receiver.toHexString()]
      );
      return null;
    }

    const recieverAccount = new AccountManager(receiver.toHexString());
    recieverAccount.countReceive();
    // receivers are not considered users since they are not spending gas for the transaction
    const receiverPosition = new PositionManager(
      recieverAccount.getAccount(),
      this.market,
      PositionSide.COLLATERAL,
      interestType
    );
    receiverPosition.addPosition(
      this.event,
      asset.toHexString(),
      this.protocol,
      receiverNewBalance,
      TransactionType.TRANSFER,
      this.market.inputTokenPriceUSD,
      receiverPrincipal
    );

    const transfer = new Transfer(
      this.event.transaction.hash
        .concatI32(this.event.logIndex.toI32())
        .concatI32(Transaction.TRANSFER)
        .toHexString()
    );
    transfer.hash = this.event.transaction.hash;
    transfer.nonce = this.event.transaction.nonce;
    transfer.logIndex = this.event.logIndex.toI32();
    transfer.gasPrice = this.event.transaction.gasPrice;
    transfer.gasUsed = this.event.receipt ? this.event.receipt!.gasUsed : null;
    transfer.gasLimit = this.event.transaction.gasLimit;
    transfer.blockNumber = this.event.block.number;
    transfer.timestamp = this.event.block.timestamp;
    transfer.sender = sender.toHexString();
    transfer.receiver = receiver.toHexString();
    transfer.market = this.market.id;
    transfer.positions = [receiverPosition.getPositionID()!, positionID!];
    transfer.asset = asset.toHexString();
    transfer.amount = amount;
    transfer.amountUSD = amountUSD;
    transfer.save();

    this.updateTransactionData(TransactionType.TRANSFER, amount, amountUSD);
    this.updateUsageData(TransactionType.TRANSFER, sender.toHexString());

    return transfer;
  }

  createFlashloan(
    asset: Address,
    account: Address,
    amount: BigInt,
    amountUSD: BigDecimal
  ): Flashloan {
    const flashloaner = new AccountManager(account.toHexString());
    if (flashloaner.isNewUser()) {
      this.protocol.cumulativeUniqueUsers += INT_ONE;
      this.protocol.save();
    }
    flashloaner.countFlashloan();

    const flashloan = new Flashloan(
      this.event.transaction.hash
        .concatI32(this.event.logIndex.toI32())
        .concatI32(Transaction.FLASHLOAN)
        .toHexString()
    );
    flashloan.hash = this.event.transaction.hash;
    flashloan.nonce = this.event.transaction.nonce;
    flashloan.logIndex = this.event.logIndex.toI32();
    flashloan.gasPrice = this.event.transaction.gasPrice;
    flashloan.gasUsed = this.event.receipt ? this.event.receipt!.gasUsed : null;
    flashloan.gasLimit = this.event.transaction.gasLimit;
    flashloan.blockNumber = this.event.block.number;
    flashloan.timestamp = this.event.block.timestamp;
    flashloan.account = account.toHexString();
    flashloan.market = this.market.id;
    flashloan.asset = asset.toHexString();
    flashloan.amount = amount;
    flashloan.amountUSD = amountUSD;
    flashloan.save();

    this.updateTransactionData(TransactionType.FLASHLOAN, amount, amountUSD);
    this.updateUsageData(TransactionType.FLASHLOAN, account.toHexString());

    return flashloan;
  }

  //////////////////
  //// Updaters ////
  //////////////////

  updateRewards(rewardData: RewardData): void {
    if (!this.market.rewardTokens) {
      this.market.rewardTokens = [rewardData.rewardToken.id];
      this.market.rewardTokenEmissionsAmount = [
        rewardData.rewardTokenEmissionsAmount,
      ];
      this.market.rewardTokenEmissionsUSD = [
        rewardData.rewardTokenEmissionsUSD,
      ];
      return; // initial add is manual
    }

    // update market reward tokens with rewardData so that it is in alphabetical order
    let rewardTokens = this.market.rewardTokens!;
    let rewardTokenEmissionsAmount = this.market.rewardTokenEmissionsAmount!;
    let rewardTokenEmissionsUSD = this.market.rewardTokenEmissionsUSD!;

    for (let i = 0; i < rewardTokens.length; i++) {
      const index = rewardData.rewardToken.id.localeCompare(rewardTokens[i]);
      if (index < 0) {
        // insert rewardData at index i
        rewardTokens = insert(rewardTokens, rewardData.rewardToken.id, i);
        rewardTokenEmissionsAmount = insert(
          rewardTokenEmissionsAmount,
          rewardData.rewardTokenEmissionsAmount,
          i
        );
        rewardTokenEmissionsUSD = insert(
          rewardTokenEmissionsUSD,
          rewardData.rewardTokenEmissionsUSD,
          i
        );
        break;
      } else if (index == 0) {
        // update the rewardData at index i
        rewardTokens[i] = rewardData.rewardToken.id;
        rewardTokenEmissionsAmount[i] = rewardData.rewardTokenEmissionsAmount;
        rewardTokenEmissionsUSD[i] = rewardData.rewardTokenEmissionsUSD;
        break;
      } else {
        if (i == rewardTokens.length - 1) {
          // insert rewardData at end of array
          rewardTokens.push(rewardData.rewardToken.id);
          rewardTokenEmissionsAmount.push(
            rewardData.rewardTokenEmissionsAmount
          );
          rewardTokenEmissionsUSD.push(rewardData.rewardTokenEmissionsUSD);
          break;
        }
      }
    }

    this.market.rewardTokens = rewardTokens;
    this.market.rewardTokenEmissionsAmount = rewardTokenEmissionsAmount;
    this.market.rewardTokenEmissionsUSD = rewardTokenEmissionsUSD;
    this.saveMarket();
  }

  // used to update tvl, borrow balance, etc. in market and protocol
  updateMarketAndProtocolData(
    inputTokenPriceUSD: BigDecimal,
    newInputTokenBalance: BigInt,
    newVariableBorrowBalance: BigInt | null = null,
    newStableBorrowBalance: BigInt | null = null,
    outputTokenSupply: BigInt | null = null
  ): void {
    const mantissaFactorBD = exponentToBigDecimal(
      this.inputToken.getDecimals()
    );
    this.inputToken.updatePrice(inputTokenPriceUSD);
    this.market.inputTokenPriceUSD = inputTokenPriceUSD;
    this.market.inputTokenBalance = newInputTokenBalance;
    this.market.outputTokenSupply = outputTokenSupply;
    if (newVariableBorrowBalance) {
      this.market.variableBorrowedTokenBalance = newVariableBorrowBalance;
    }
    if (newStableBorrowBalance) {
      this.market.stableBorrowedTokenBalance = newStableBorrowBalance;
    }

    const vBorrowAmount = this.market.variableBorrowedTokenBalance
      ? this.market
          .variableBorrowedTokenBalance!.toBigDecimal()
          .div(mantissaFactorBD)
      : BIGDECIMAL_ZERO;
    const sBorrowAmount = this.market.stableBorrowedTokenBalance
      ? this.market
          .stableBorrowedTokenBalance!.toBigDecimal()
          .div(mantissaFactorBD)
      : BIGDECIMAL_ZERO;
    const totalBorrowed = vBorrowAmount.plus(sBorrowAmount);
    this.market.totalValueLockedUSD = newInputTokenBalance
      .toBigDecimal()
      .div(mantissaFactorBD)
      .times(inputTokenPriceUSD);
    this.market.totalDepositBalanceUSD = this.market.totalValueLockedUSD;
    this.market.totalBorrowBalanceUSD = totalBorrowed.times(inputTokenPriceUSD);
    this.saveMarket();

    let totalValueLockedUSD = BIGDECIMAL_ZERO;
    let totalBorrowBalanceUSD = BIGDECIMAL_ZERO;
    const marketList = this.protocol.markets.load();
    for (let i = 0; i < marketList.length; i++) {
      const _market = Market.load(marketList[i].id);
      if (!_market) {
        log.error("[updateMarketAndProtocolData] Market not found: {}", [
          marketList[i].id,
        ]);
        continue;
      }
      totalValueLockedUSD = totalValueLockedUSD.plus(
        _market.totalValueLockedUSD
      );
      totalBorrowBalanceUSD = totalBorrowBalanceUSD.plus(
        _market.totalBorrowBalanceUSD
      );
    }
    this.protocol.totalValueLockedUSD = totalValueLockedUSD;
    this.protocol.totalDepositBalanceUSD = totalValueLockedUSD;
    this.protocol.totalBorrowBalanceUSD = totalBorrowBalanceUSD;
    this.protocol.save();
  }

  updateSupplyIndex(supplyIndex: BigInt): void {
    this.market.supplyIndex = supplyIndex;
    this.market.indexLastUpdatedTimestamp = this.event.block.timestamp;
    this.saveMarket();
  }

  updateBorrowIndex(borrowIndex: BigInt): void {
    this.market.borrowIndex = borrowIndex;
    this.market.indexLastUpdatedTimestamp = this.event.block.timestamp;
    this.saveMarket();
  }

  //
  //
  // Update the protocol revenue
  addProtocolRevenue(
    protocolRevenueDelta: BigDecimal,
    fee: LmFEE | null = null
  ): void {
    this.updateRevenue(protocolRevenueDelta, BIGDECIMAL_ZERO);

    if (!fee) {
      fee = this.getOrUpdateFee(FeeType.OTHER);
    }

    const marketRevDetails = this.getOrCreateRevenueDetail(
      this.market.id,
      true
    );
    const protocolRevenueDetail = this.getOrCreateRevenueDetail(
      this.protocol.id,
      false
    );

    this.insertInOrder(marketRevDetails, protocolRevenueDelta, fee.id);
    this.insertInOrder(protocolRevenueDetail, protocolRevenueDelta, fee.id);
  }

  //
  //
  // Update the protocol revenue
  addSupplyRevenue(
    supplyRevenueDelta: BigDecimal,
    fee: LmFEE | null = null
  ): void {
    this.updateRevenue(BIGDECIMAL_ZERO, supplyRevenueDelta);

    if (!fee) {
      fee = this.getOrUpdateFee(FeeType.OTHER);
    }

    const marketRevDetails = this.getOrCreateRevenueDetail(
      this.market.id,
      true
    );
    const protocolRevenueDetail = this.getOrCreateRevenueDetail(
      this.protocol.id,
      false
    );

    this.insertInOrder(marketRevDetails, supplyRevenueDelta, fee.id);
    this.insertInOrder(protocolRevenueDetail, supplyRevenueDelta, fee.id);
  }

  private updateRevenue(
    protocolRevenueDelta: BigDecimal,
    supplyRevenueDelta: BigDecimal
  ): void {
    const totalRevenueDelta = protocolRevenueDelta.plus(supplyRevenueDelta);

    // update market
    this.market.cumulativeTotalRevenueUSD =
      this.market.cumulativeTotalRevenueUSD.plus(totalRevenueDelta);
    this.market.cumulativeProtocolSideRevenueUSD =
      this.market.cumulativeProtocolSideRevenueUSD.plus(protocolRevenueDelta);
    this.market.cumulativeSupplySideRevenueUSD =
      this.market.cumulativeSupplySideRevenueUSD.plus(supplyRevenueDelta);
    this.saveMarket();

    // update protocol
    this.protocol.cumulativeTotalRevenueUSD =
      this.protocol.cumulativeTotalRevenueUSD.plus(totalRevenueDelta);
    this.protocol.cumulativeProtocolSideRevenueUSD =
      this.protocol.cumulativeProtocolSideRevenueUSD.plus(protocolRevenueDelta);
    this.protocol.cumulativeSupplySideRevenueUSD =
      this.protocol.cumulativeSupplySideRevenueUSD.plus(supplyRevenueDelta);
    this.protocol.save();

    // update revenue in snapshots
    this.snapshots.updateRevenue(protocolRevenueDelta, supplyRevenueDelta);
  }

  //
  //
  // this only updates the usage data for the entities changed in this class
  // (ie, market and protocol)
  private updateUsageData(transactionType: string, account: string): void {
    this.market.cumulativeUniqueUsers += activityCounter(
      account,
      transactionType,
      false,
      0,
      this.market.id
    );
    if (transactionType == TransactionType.DEPOSIT) {
      this.market.cumulativeUniqueDepositors += activityCounter(
        account,
        transactionType,
        true,
        0,
        this.market.id
      );
      this.protocol.cumulativeUniqueDepositors += activityCounter(
        account,
        transactionType,
        true,
        0
      );
    }
    if (transactionType == TransactionType.BORROW) {
      this.market.cumulativeUniqueBorrowers += activityCounter(
        account,
        transactionType,
        true,
        0,
        this.market.id
      );
      this.protocol.cumulativeUniqueBorrowers += activityCounter(
        account,
        transactionType,
        true,
        0
      );
    }
    if (transactionType == TransactionType.LIQUIDATOR) {
      this.market.cumulativeUniqueLiquidators += activityCounter(
        account,
        transactionType,
        true,
        0,
        this.market.id
      );
      this.protocol.cumulativeUniqueLiquidators += activityCounter(
        account,
        transactionType,
        true,
        0
      );
    }
    if (transactionType == TransactionType.LIQUIDATEE) {
      this.market.cumulativeUniqueLiquidatees += activityCounter(
        account,
        transactionType,
        true,
        0,
        this.market.id
      );
      this.protocol.cumulativeUniqueLiquidatees += activityCounter(
        account,
        transactionType,
        true,
        0
      );
    }
    if (transactionType == TransactionType.TRANSFER)
      this.market.cumulativeUniqueTransferrers += activityCounter(
        account,
        transactionType,
        true,
        0,
        this.market.id
      );
    if (transactionType == TransactionType.FLASHLOAN)
      this.market.cumulativeUniqueFlashloaners += activityCounter(
        account,
        transactionType,
        true,
        0,
        this.market.id
      );

    this.protocol.save();
    this.saveMarket();

    // update the snapshots in their respective class
    this.snapshots.updateUsageData(transactionType, account);
  }

  //
  //
  // this only updates the usage data for the entities changed in this class
  // (ie, market and protocol)
  private updateTransactionData(
    transactionType: string,
    amount: BigInt,
    amountUSD: BigDecimal
  ): void {
    if (transactionType == TransactionType.DEPOSIT) {
      this.protocol.transactionCount += INT_ONE;
      this.protocol.depositCount += INT_ONE;
      this.protocol.cumulativeDepositUSD =
        this.protocol.cumulativeDepositUSD.plus(amountUSD);
      this.market.cumulativeDepositUSD =
        this.market.cumulativeDepositUSD.plus(amountUSD);
      this.market.depositCount += INT_ONE;
    } else if (transactionType == TransactionType.WITHDRAW) {
      this.protocol.transactionCount += INT_ONE;
      this.protocol.withdrawCount += INT_ONE;
      this.market.withdrawCount += INT_ONE;
    } else if (transactionType == TransactionType.BORROW) {
      this.protocol.transactionCount += INT_ONE;
      this.protocol.borrowCount += INT_ONE;
      this.protocol.cumulativeBorrowUSD =
        this.protocol.cumulativeBorrowUSD.plus(amountUSD);
      this.market.cumulativeBorrowUSD =
        this.market.cumulativeBorrowUSD.plus(amountUSD);
      this.market.borrowCount += INT_ONE;
    } else if (transactionType == TransactionType.REPAY) {
      this.protocol.transactionCount += INT_ONE;
      this.protocol.repayCount += INT_ONE;
      this.market.repayCount += INT_ONE;
    } else if (transactionType == TransactionType.LIQUIDATE) {
      this.protocol.transactionCount += INT_ONE;
      this.protocol.liquidationCount += INT_ONE;
      this.protocol.cumulativeLiquidateUSD =
        this.protocol.cumulativeLiquidateUSD.plus(amountUSD);
      const reserveFactor = this.market.reserveFactor;
      if (reserveFactor)
        this.protocol._cumulativeProtocolSideLiquidationRevenue =
          this.protocol._cumulativeProtocolSideLiquidationRevenue!.plus(
            amountUSD.times(reserveFactor)
          );
      this.market.cumulativeLiquidateUSD =
        this.market.cumulativeLiquidateUSD.plus(amountUSD);
      this.market.liquidationCount += INT_ONE;
    } else if (transactionType == TransactionType.TRANSFER) {
      this.protocol.transferCount += INT_ONE;
      this.market.cumulativeTransferUSD =
        this.market.cumulativeTransferUSD.plus(amountUSD);
      this.market.transferCount += INT_ONE;
    } else if (transactionType == TransactionType.FLASHLOAN) {
      this.protocol.flashloanCount += INT_ONE;
      this.market.cumulativeFlashloanUSD =
        this.market.cumulativeFlashloanUSD.plus(amountUSD);
      this.market.flashloanCount += INT_ONE;
    } else {
      log.error("[updateTransactionData] Invalid transaction type: {}", [
        transactionType,
      ]);
      return;
    }
    this.market.transactionCount += INT_ONE;

    this.protocol.save();
    this.saveMarket();

    // update the snapshots in their respective class
    this.snapshots.updateTransactionData(transactionType, amount, amountUSD);
  }

  //
  //
  // Insert revenue in RevenueDetail in order (alphabetized)
  private insertInOrder(
    details: LmRevenueDetail,
    amountUSD: BigDecimal,
    associatedSource: string
  ): void {
    if (details.sources.length == 0) {
      details.sources = [associatedSource];
      details.amountsUSD = [amountUSD];
    } else {
      let sources = details.sources;
      let amountsUSD = details.amountsUSD;

      // upsert source and amount
      if (sources.includes(associatedSource)) {
        const idx = sources.indexOf(associatedSource);
        amountsUSD[idx] = amountsUSD[idx].plus(amountUSD);

        details.sources = sources;
        details.amountsUSD = amountsUSD;
      } else {
        sources = insert(sources, associatedSource);
        amountsUSD = insert(amountsUSD, amountUSD);

        // sort amounts by sources
        const sourcesSorted = sources.sort();
        let amountsUSDSorted: BigDecimal[] = [];
        for (let i = 0; i < sourcesSorted.length; i++) {
          const idx = sources.indexOf(sourcesSorted[i]);
          amountsUSDSorted = insert(amountsUSDSorted, amountsUSD[idx]);
        }

        details.sources = sourcesSorted;
        details.amountsUSD = amountsUSDSorted;
      }
    }
    details.save();
  }
}
