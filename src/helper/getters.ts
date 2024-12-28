import {
  Address,
  Bytes,
  ethereum,
  BigInt,
  log,
  BigDecimal,
} from "@graphprotocol/graph-ts";
import {
  Token,
  UsageMetricsDailySnapshot,
  FinancialsDailySnapshot,
  MarketDailySnapshot,
  Market,
  _Ilk,
  InterestRate,
  MarketHourlySnapshot,
  UsageMetricsHourlySnapshot,
  Liquidate,
  _Chi,
  _Urn,
  _Proxy,
  _PositionCounter,
  Position,
  Protocol,
} from "../../generated/schema";
import {
  BIGDECIMAL_ZERO,
  ProtocolType,
  SECONDS_PER_DAY,
  LendingType,
  VAT_ADDRESS,
  ZERO_ADDRESS,
  BIGDECIMAL_ONE,
  ZAR_ADDRESS,
  SECONDS_PER_HOUR,
  Network,
  BIGINT_ZERO,
  BIGINT_ONE_RAY,
  INT_ZERO,
  PositionSide,
  BIGDECIMAL_ONE_RAY,
  INT_ONE,
  ZAR_INIT_PRICE_USD,
  DAI_ADDRESS,
  ETH_ADDRESS,
  WSTETH_ADDRESS,
  WSTETH_INIT_PRICE_USD,
  ETH_INIT_PRICE_USD,
  DAI_INIT_PRICE_USD,
  InterestRateType,
  RAD,
  RAY,
  BIGINT_ONE_WAD,
  BIGINT_ONE_RAD,
  SECONDS_PER_YEAR,
  BIGINT_ONE,
  BIGDECIMAL_ONE_WAD,
  BIGDECIMAL_ONE_RAD,
} from "./constants";
import { getProtocolData } from "../mapping";

export function getOrCreateToken(
  tokenId: string,
  name: string = "unknown",
  symbol: string = "unknown",
  decimals: i32 = 18
): Token {
  let token = Token.load(tokenId.toLowerCase());
  // fetch info if null
  if (token == null) {
    token = new Token(tokenId);
    token.name = name;
    token.symbol = symbol;
    token.decimals = decimals;
    if (tokenId.toLowerCase() == ZAR_ADDRESS) {
      token.lastPriceUSD = ZAR_INIT_PRICE_USD;
    } else if (tokenId.toLowerCase() == DAI_ADDRESS) {
      token.lastPriceUSD = DAI_INIT_PRICE_USD;
    } else if (tokenId.toLowerCase() == ETH_ADDRESS) {
      token.lastPriceUSD = ETH_INIT_PRICE_USD;
    } else if (tokenId.toLowerCase() == WSTETH_ADDRESS) {
      token.lastPriceUSD = WSTETH_INIT_PRICE_USD;
    }
    token.save();
  }
  return token;
}

///////////////////////////
///////// Metrics /////////
///////////////////////////

export function getOrCreateUsageMetricsHourlySnapshot(
  event: ethereum.Event
): UsageMetricsHourlySnapshot {
  // Number of days since Unix epoch
  const id: i64 = event.block.timestamp.toI64() / SECONDS_PER_HOUR;

  // Create unique id for the day
  let usageMetrics = UsageMetricsHourlySnapshot.load(id.toString());
  const protocol = getOrCreateLendingProtocol();
  if (usageMetrics == null) {
    usageMetrics = new UsageMetricsHourlySnapshot(id.toString());
    usageMetrics.hours = event.block.timestamp.toI32() / SECONDS_PER_HOUR;
    usageMetrics.protocol = protocol.id;
    usageMetrics.hourlyActiveUsers = 0;
    usageMetrics.cumulativeUniqueUsers = protocol.cumulativeUniqueUsers;
    usageMetrics.hourlyTransactionCount = 0;
    usageMetrics.hourlyDepositCount = 0;
    usageMetrics.hourlyBorrowCount = 0;
    usageMetrics.hourlyWithdrawCount = 0;
    usageMetrics.hourlyRepayCount = 0;
    usageMetrics.hourlyLiquidateCount = 0;
    usageMetrics.blockNumber = BIGINT_ZERO;
    usageMetrics.timestamp = BIGINT_ZERO;
    usageMetrics.save();
  }
  return usageMetrics;
}

export function getOrCreateUsageMetricsDailySnapshot(
  event: ethereum.Event
): UsageMetricsDailySnapshot {
  // Number of days since Unix epoch
  const id: i64 = event.block.timestamp.toI64() / SECONDS_PER_DAY;

  // Create unique id for the day
  let usageMetrics = UsageMetricsDailySnapshot.load(id.toString());
  const protocol = getOrCreateLendingProtocol();

  if (usageMetrics == null) {
    usageMetrics = new UsageMetricsDailySnapshot(id.toString());
    usageMetrics.days = event.block.timestamp.toI32() / SECONDS_PER_DAY;
    usageMetrics.protocol = protocol.id;
    usageMetrics.dailyActiveUsers = 0;
    usageMetrics.dailyActiveDepositors = 0;
    usageMetrics.dailyActiveBorrowers = 0;
    usageMetrics.dailyActiveLiquidators = 0;
    usageMetrics.dailyActiveLiquidatees = 0;
    usageMetrics.cumulativeUniqueUsers = protocol.cumulativeUniqueUsers;
    usageMetrics.cumulativeUniqueDepositors =
      protocol.cumulativeUniqueDepositors;
    usageMetrics.cumulativeUniqueBorrowers = protocol.cumulativeUniqueBorrowers;
    usageMetrics.cumulativeUniqueLiquidators =
      protocol.cumulativeUniqueLiquidators;
    usageMetrics.cumulativeUniqueLiquidatees =
      protocol.cumulativeUniqueLiquidatees;
    usageMetrics.totalPoolCount = protocol.totalPoolCount;
    usageMetrics.dailyTransactionCount = INT_ZERO;
    usageMetrics.dailyDepositCount = INT_ZERO;
    usageMetrics.dailyBorrowCount = INT_ZERO;
    usageMetrics.dailyWithdrawCount = INT_ZERO;
    usageMetrics.dailyRepayCount = INT_ZERO;
    usageMetrics.dailyLiquidateCount = INT_ZERO;
    usageMetrics.dailyTransferCount = INT_ZERO;
    usageMetrics.dailyFlashloanCount = INT_ZERO;
    usageMetrics.cumulativePositionCount = protocol.cumulativePositionCount;
    usageMetrics.openPositionCount = protocol.openPositionCount;
    usageMetrics.blockNumber = BIGINT_ZERO;
    usageMetrics.timestamp = BIGINT_ZERO;
    usageMetrics.save();
  }
  return usageMetrics;
}

export function getOrCreateMarketHourlySnapshot(
  event: ethereum.Event,
  marketAddress: string
): MarketHourlySnapshot {
  const hours = event.block.timestamp.toI32() / SECONDS_PER_HOUR;
  const snapshotID = marketAddress.concat("-").concat(hours.toString());
  let marketMetrics = MarketHourlySnapshot.load(snapshotID);
  const market = getOrCreateMarket(marketAddress);

  if (marketMetrics == null) {
    marketMetrics = new MarketHourlySnapshot(snapshotID);
    marketMetrics.hours = hours;
    marketMetrics.protocol = getOrCreateLendingProtocol().id;
    marketMetrics.market = marketAddress;
    marketMetrics.inputTokenBalance = market.inputTokenBalance;
    marketMetrics.inputTokenPriceUSD = market.inputTokenPriceUSD;
    marketMetrics.outputTokenSupply = market.outputTokenSupply;
    marketMetrics.totalValueLockedUSD = market.totalValueLockedUSD;
    marketMetrics.totalDepositBalanceUSD = market.totalDepositBalanceUSD;
    marketMetrics.totalBorrowBalanceUSD = market.totalBorrowBalanceUSD;

    marketMetrics.cumulativeDepositUSD = market.cumulativeDepositUSD;
    marketMetrics.cumulativeBorrowUSD = market.cumulativeBorrowUSD;
    marketMetrics.cumulativeLiquidateUSD = market.cumulativeLiquidateUSD;

    marketMetrics.rates = market.rates;
    marketMetrics.blockNumber = event.block.number;
    marketMetrics.timestamp = event.block.timestamp;

    marketMetrics.hourlyDepositUSD = BIGDECIMAL_ZERO;
    marketMetrics.hourlyBorrowUSD = BIGDECIMAL_ZERO;
    marketMetrics.hourlyLiquidateUSD = BIGDECIMAL_ZERO;
    marketMetrics.hourlyWithdrawUSD = BIGDECIMAL_ZERO;
    marketMetrics.hourlyRepayUSD = BIGDECIMAL_ZERO;
    marketMetrics.hourlyTransferUSD = BIGDECIMAL_ZERO;
    marketMetrics.hourlyFlashloanUSD = BIGDECIMAL_ZERO;

    marketMetrics.cumulativeSupplySideRevenueUSD =
      market.cumulativeSupplySideRevenueUSD;
    marketMetrics.cumulativeProtocolSideRevenueUSD =
      market.cumulativeProtocolSideRevenueUSD;
    marketMetrics.cumulativeTotalRevenueUSD = market.cumulativeTotalRevenueUSD;

    marketMetrics.hourlySupplySideRevenueUSD = BIGDECIMAL_ZERO;
    marketMetrics.hourlyProtocolSideRevenueUSD = BIGDECIMAL_ZERO;
    marketMetrics.hourlyTotalRevenueUSD = BIGDECIMAL_ZERO;

    marketMetrics.save();
  }

  return marketMetrics;
}

export function getOrCreateMarketDailySnapshot(
  event: ethereum.Event,
  marketAddress: string
): MarketDailySnapshot {
  const days = event.block.timestamp.toI32() / SECONDS_PER_DAY;
  const snapshotID = marketAddress.concat("-").concat(days.toString());
  let marketMetrics = MarketDailySnapshot.load(snapshotID);
  const market = getOrCreateMarket(marketAddress);

  if (marketMetrics == null) {
    marketMetrics = new MarketDailySnapshot(snapshotID);
    marketMetrics.days = days;
    marketMetrics.protocol = getOrCreateLendingProtocol().id;
    marketMetrics.market = marketAddress;
    marketMetrics.inputTokenBalance = market.inputTokenBalance;
    marketMetrics.inputTokenPriceUSD = market.inputTokenPriceUSD;
    marketMetrics.outputTokenSupply = market.outputTokenSupply;
    marketMetrics.totalValueLockedUSD = market.totalValueLockedUSD;
    marketMetrics.totalDepositBalanceUSD = market.totalDepositBalanceUSD;
    marketMetrics.totalBorrowBalanceUSD = market.totalBorrowBalanceUSD;
    marketMetrics.dailyNativeDeposit = BIGINT_ZERO;
    marketMetrics.dailyNativeBorrow = BIGINT_ZERO;
    marketMetrics.dailyNativeLiquidate = BIGINT_ZERO;
    marketMetrics.dailyNativeWithdraw = BIGINT_ZERO;
    marketMetrics.dailyNativeRepay = BIGINT_ZERO;
    marketMetrics.dailyNativeTransfer = BIGINT_ZERO;
    marketMetrics.dailyNativeFlashloan = BIGINT_ZERO;

    marketMetrics.variableBorrowedTokenBalance = BIGINT_ZERO;
    marketMetrics.stableBorrowedTokenBalance = BIGINT_ZERO;
    marketMetrics.positionCount = market.positionCount;
    marketMetrics.openPositionCount = market.openPositionCount;
    marketMetrics.closedPositionCount = market.closedPositionCount;
    marketMetrics.lendingPositionCount = market.lendingPositionCount;
    marketMetrics.borrowingPositionCount = market.borrowingPositionCount;

    marketMetrics.dailyActiveUsers = INT_ZERO;
    marketMetrics.dailyActiveDepositors = INT_ZERO;
    marketMetrics.dailyActiveBorrowers = INT_ZERO;
    marketMetrics.dailyActiveLiquidators = INT_ZERO;
    marketMetrics.dailyActiveLiquidatees = INT_ZERO;
    marketMetrics.dailyActiveTransferrers = INT_ZERO;
    marketMetrics.dailyActiveFlashloaners = INT_ZERO;
    marketMetrics.dailyDepositCount = INT_ZERO;
    marketMetrics.dailyWithdrawCount = INT_ZERO;
    marketMetrics.dailyBorrowCount = INT_ZERO;
    marketMetrics.dailyRepayCount = INT_ZERO;
    marketMetrics.dailyLiquidateCount = INT_ZERO;
    marketMetrics.dailyTransferCount = INT_ZERO;
    marketMetrics.dailyFlashloanCount = INT_ZERO;

    marketMetrics.cumulativeDepositUSD = market.cumulativeDepositUSD;
    marketMetrics.cumulativeBorrowUSD = market.cumulativeBorrowUSD;
    marketMetrics.cumulativeLiquidateUSD = market.cumulativeLiquidateUSD;
    marketMetrics.cumulativeTransferUSD = BIGDECIMAL_ZERO;
    marketMetrics.cumulativeFlashloanUSD = BIGDECIMAL_ZERO;
    marketMetrics.rates = market.rates;
    marketMetrics.blockNumber = event.block.number;
    marketMetrics.timestamp = event.block.timestamp;

    marketMetrics.dailyDepositUSD = BIGDECIMAL_ZERO;
    marketMetrics.dailyBorrowUSD = BIGDECIMAL_ZERO;
    marketMetrics.dailyLiquidateUSD = BIGDECIMAL_ZERO;
    marketMetrics.dailyWithdrawUSD = BIGDECIMAL_ZERO;
    marketMetrics.dailyRepayUSD = BIGDECIMAL_ZERO;
    marketMetrics.dailyTransferUSD = BIGDECIMAL_ZERO;
    marketMetrics.dailyFlashloanUSD = BIGDECIMAL_ZERO;

    marketMetrics.cumulativeSupplySideRevenueUSD =
      market.cumulativeSupplySideRevenueUSD;
    marketMetrics.cumulativeProtocolSideRevenueUSD =
      market.cumulativeProtocolSideRevenueUSD;
    marketMetrics.cumulativeTotalRevenueUSD = market.cumulativeTotalRevenueUSD;

    marketMetrics.dailySupplySideRevenueUSD = BIGDECIMAL_ZERO;
    marketMetrics.dailyProtocolSideRevenueUSD = BIGDECIMAL_ZERO;
    marketMetrics.dailyTotalRevenueUSD = BIGDECIMAL_ZERO;

    marketMetrics.save();
  }

  return marketMetrics;
}

export function getOrCreateFinancials(
  event: ethereum.Event
): FinancialsDailySnapshot {
  // Number of days since Unix epoch
  const id: i64 = event.block.timestamp.toI64() / SECONDS_PER_DAY;

  let financialMetrics = FinancialsDailySnapshot.load(id.toString());
  const protocol = getOrCreateLendingProtocol();
  if (financialMetrics == null) {
    financialMetrics = new FinancialsDailySnapshot(id.toString());
    financialMetrics.days = event.block.timestamp.toI32() / SECONDS_PER_DAY;
    financialMetrics.protocol = getOrCreateLendingProtocol().id;
    financialMetrics.totalValueLockedUSD = protocol.totalValueLockedUSD;
    financialMetrics.totalBorrowBalanceUSD = protocol.totalBorrowBalanceUSD;
    financialMetrics.totalDepositBalanceUSD = protocol.totalDepositBalanceUSD;
    financialMetrics.mintedTokenSupplies = protocol.mintedTokenSupplies;

    financialMetrics.cumulativeSupplySideRevenueUSD =
      protocol.cumulativeSupplySideRevenueUSD;
    financialMetrics.cumulativeProtocolSideRevenueUSD =
      protocol.cumulativeProtocolSideRevenueUSD;
    financialMetrics._cumulativeProtocolSideStabilityFeeRevenue =
      protocol._cumulativeProtocolSideStabilityFeeRevenue;
    financialMetrics._cumulativeProtocolSideLiquidationRevenue =
      protocol._cumulativeProtocolSideLiquidationRevenue;
    financialMetrics.cumulativeTotalRevenueUSD =
      protocol.cumulativeTotalRevenueUSD;
    financialMetrics.cumulativeBorrowUSD = protocol.cumulativeBorrowUSD;
    financialMetrics.cumulativeDepositUSD = protocol.cumulativeDepositUSD;
    financialMetrics.cumulativeLiquidateUSD = protocol.cumulativeLiquidateUSD;

    financialMetrics.dailySupplySideRevenueUSD = BIGDECIMAL_ZERO;
    financialMetrics.dailyProtocolSideRevenueUSD = BIGDECIMAL_ZERO;
    financialMetrics._dailyProtocolSideStabilityFeeRevenue = BIGDECIMAL_ZERO;
    financialMetrics._dailyProtocolSideLiquidationRevenue = BIGDECIMAL_ZERO;
    financialMetrics.dailyTotalRevenueUSD = BIGDECIMAL_ZERO;
    financialMetrics.dailyBorrowUSD = BIGDECIMAL_ZERO;
    financialMetrics.dailyWithdrawUSD = BIGDECIMAL_ZERO;
    financialMetrics.dailyDepositUSD = BIGDECIMAL_ZERO;
    financialMetrics.dailyRepayUSD = BIGDECIMAL_ZERO;
    financialMetrics.dailyLiquidateUSD = BIGDECIMAL_ZERO;
    financialMetrics.dailyTransferUSD = BIGDECIMAL_ZERO;
    financialMetrics.dailyFlashloanUSD = BIGDECIMAL_ZERO;
    financialMetrics.blockNumber = event.block.number;
    financialMetrics.timestamp = event.block.timestamp;

    financialMetrics.save();
  }
  return financialMetrics;
}

////////////////////////////
///// Lending Specific /////
///////////////////////////

export function getOrCreateLendingProtocol(): Protocol {
  const protocolData = getProtocolData();
  let protocol = Protocol.load(VAT_ADDRESS);
  if (protocol == null) {
    protocol = new Protocol(VAT_ADDRESS);
    protocol.protocol = protocolData.protocol;
    protocol.name = protocolData.name;
    protocol.slug = protocolData.slug;
    protocol.schemaVersion = "1.0.0";
    protocol.subgraphVersion = "1.0.0";
    protocol.methodologyVersion = "1.0.0";
    protocol.network = Network.ARBITRUM_ONE;
    protocol.type = ProtocolType.LENDING;
    protocol.lendingType = LendingType.POOLED;
    protocol.lenderPermissionType = protocolData.lenderPermissionType;
    protocol.borrowerPermissionType = protocolData.borrowerPermissionType;
    protocol.poolCreatorPermissionType = protocolData.poolCreatorPermissionType;
    protocol.riskType = protocolData.riskType;
    protocol.collateralizationType = protocolData.collateralizationType;
    protocol.cumulativeUniqueUsers = INT_ZERO;
    protocol.cumulativeUniqueDepositors = INT_ZERO;
    protocol.cumulativeUniqueBorrowers = INT_ZERO;
    protocol.cumulativeUniqueLiquidators = INT_ZERO;
    protocol.cumulativeUniqueLiquidatees = INT_ZERO;
    protocol.totalValueLockedUSD = BIGDECIMAL_ZERO;
    protocol.cumulativeSupplySideRevenueUSD = BIGDECIMAL_ZERO;
    protocol.cumulativeProtocolSideRevenueUSD = BIGDECIMAL_ZERO;
    protocol.cumulativeTotalRevenueUSD = BIGDECIMAL_ZERO;
    protocol.totalDepositBalanceUSD = BIGDECIMAL_ZERO;
    protocol.cumulativeDepositUSD = BIGDECIMAL_ZERO;
    protocol.totalBorrowBalanceUSD = BIGDECIMAL_ZERO;
    protocol.cumulativeBorrowUSD = BIGDECIMAL_ZERO;
    protocol.cumulativeLiquidateUSD = BIGDECIMAL_ZERO;
    protocol.totalPoolCount = INT_ZERO;
    protocol.openPositionCount = INT_ZERO;
    protocol.cumulativePositionCount = INT_ZERO;
    protocol.transactionCount = INT_ZERO;
    protocol.depositCount = INT_ZERO;
    protocol.withdrawCount = INT_ZERO;
    protocol.borrowCount = INT_ZERO;
    protocol.repayCount = INT_ZERO;
    protocol.liquidationCount = INT_ZERO;
    protocol.transferCount = INT_ZERO;
    protocol.flashloanCount = INT_ZERO;
    protocol._cumulativeProtocolSideStabilityFeeRevenue = BIGDECIMAL_ZERO;
    protocol._cumulativeProtocolSideLiquidationRevenue = BIGDECIMAL_ZERO;
    protocol.rewardTokenEmissionsUSD = [BIGDECIMAL_ZERO];
    protocol.mintedTokens = [ZAR_ADDRESS];
    protocol.marketIDList = [];
    protocol.ilkIDList = [];
    protocol._par = BIGINT_ONE_RAY;
  }

  protocol.save();

  return protocol;
}

export function getOrCreateMarket(
  marketID: string,
  name: string = "unknown",
  inputToken: string = ZERO_ADDRESS,
  blockNumber: BigInt = BIGINT_ZERO,
  timeStamp: BigInt = BIGINT_ZERO,
  ilk: Bytes = Bytes.fromHexString("0x")
): Market {
  let market = Market.load(marketID);
  if (market == null) {
    if (marketID == ZERO_ADDRESS) {
      log.warning("[getOrCreateMarket]Creating a new Market with marketID={}", [
        marketID,
      ]);
    }
    const protocol = getOrCreateLendingProtocol();

    market = new Market(marketID);
    market.name = name;
    market.inputToken = inputToken;
    market.createdTimestamp = timeStamp;
    market.createdBlockNumber = blockNumber;
    market.protocol = protocol.id;
    market.ilk = ilk.toHexString();

    // set defaults
    market.totalValueLockedUSD = BIGDECIMAL_ZERO;
    market.inputTokenBalance = BIGINT_ZERO;
    market.inputTokenPriceUSD = BIGDECIMAL_ZERO;

    market.outputTokenSupply = BIGINT_ZERO;
    market.totalBorrowBalanceUSD = BIGDECIMAL_ZERO;
    market.cumulativeBorrowUSD = BIGDECIMAL_ZERO;
    market.totalDepositBalanceUSD = BIGDECIMAL_ZERO;
    market.cumulativeDepositUSD = BIGDECIMAL_ZERO;
    market.cumulativeLiquidateUSD = BIGDECIMAL_ZERO;
    market.cumulativeTransferUSD = BIGDECIMAL_ZERO;
    market.cumulativeFlashloanUSD = BIGDECIMAL_ZERO;
    market.cumulativeSupplySideRevenueUSD = BIGDECIMAL_ZERO;
    market.cumulativeProtocolSideRevenueUSD = BIGDECIMAL_ZERO;
    market.cumulativeTotalRevenueUSD = BIGDECIMAL_ZERO;
    market.isActive = true; // default
    market.canUseAsCollateral = true; // default
    market.canBorrowFrom = true; // default
    market.canIsolate = false; // default
    market.maximumLTV = BIGDECIMAL_ZERO; // default
    market.liquidationThreshold = BIGDECIMAL_ZERO; // default
    market.liquidationPenalty = BIGDECIMAL_ZERO; // default
    market.rates = [BIGDECIMAL_ZERO.toString()];
    market._mat = BIGINT_ONE_RAY;

    market.positionCount = INT_ZERO;
    market.openPositionCount = INT_ZERO;
    market.closedPositionCount = INT_ZERO;
    market.borrowingPositionCount = INT_ZERO;
    market.lendingPositionCount = INT_ZERO;

    market.cumulativeUniqueUsers = INT_ZERO;
    market.cumulativeUniqueDepositors = INT_ZERO;
    market.cumulativeUniqueBorrowers = INT_ZERO;
    market.cumulativeUniqueLiquidators = INT_ZERO;
    market.cumulativeUniqueLiquidatees = INT_ZERO;
    market.cumulativeUniqueTransferrers = INT_ZERO;
    market.cumulativeUniqueFlashloaners = INT_ZERO;

    market.transactionCount = INT_ZERO;
    market.depositCount = INT_ZERO;
    market.withdrawCount = INT_ZERO;
    market.borrowCount = INT_ZERO;
    market.repayCount = INT_ZERO;
    market.liquidationCount = INT_ZERO;
    market.transferCount = INT_ZERO;
    market.flashloanCount = INT_ZERO;

    market.save();

    const ilkIDList = protocol.ilkIDList;
    ilkIDList.push(ilk.toHexString());
    protocol.ilkIDList = ilkIDList;
    const marketIDList = protocol.marketIDList;
    marketIDList.push(marketID);
    protocol.marketIDList = marketIDList;
    protocol.totalPoolCount += INT_ONE;
    protocol.save();
  }

  return market;
}

export function getOrCreateIlk(
  ilk: Bytes,
  marketID: string = ZERO_ADDRESS,
  art: BigInt = BIGINT_ZERO,
  rate: BigInt = BIGINT_ONE_RAY,
  mat: BigInt = BIGINT_ZERO,
  line: BigInt = BIGINT_ZERO,
  duty: BigInt = BIGINT_ZERO,
  dust: BigInt = BIGINT_ZERO,
  hole: BigInt = BIGINT_ZERO,
  dirt: BigInt = BIGINT_ZERO,
  pip: string = "",
  median: string = "",
  clip: string = ""
): _Ilk | null {
  let _ilk = _Ilk.load(ilk.toHexString());
  if (_ilk == null && marketID != ZERO_ADDRESS) {
    const market = getOrCreateMarket(marketID);

    _ilk = new _Ilk(ilk.toHexString());
    _ilk.marketAddress = marketID;
    _ilk.name = market.name != null ? market.name! : "unknown";
    _ilk.collateralToken = market.inputToken;
    _ilk.minimumCollateralizationRatio = mat
      .toBigDecimal()
      .div(BIGDECIMAL_ONE_RAY);

    _ilk.maximumLTV = market.maximumLTV;
    _ilk.liquidationPenalty = market.liquidationPenalty;
    _ilk.debt = art.times(rate).toBigDecimal().div(BIGDECIMAL_ONE_RAD);
    _ilk.debtCeiling = line.toBigDecimal().div(BIGDECIMAL_ONE_RAD);
    _ilk.debtFloor = dust.toBigDecimal().div(BIGDECIMAL_ONE_RAD);

    let dutyBigDecimal = duty.toBigDecimal().div(BIGDECIMAL_ONE_RAY);
    let dutyFloat = parseFloat(dutyBigDecimal.toString());
    let annualStabilityFeeFloat = Math.pow(dutyFloat, SECONDS_PER_YEAR) - 1;
    let annualStabilityFee = BigDecimal.fromString(
      annualStabilityFeeFloat.toString()
    );
    _ilk.annualStabilityFee = annualStabilityFee;

    _ilk.clipper = clip;
    _ilk.pip = pip;
    _ilk.median = median;
    _ilk.hole = hole.toBigDecimal().div(BIGDECIMAL_ONE_RAD);
    _ilk.dirt = dirt.toBigDecimal().div(BIGDECIMAL_ONE_RAD);

    _ilk._urns = [];
    _ilk.availableToBorrow = _ilk.debtCeiling.minus(_ilk.debt);
    _ilk._art = art;
    _ilk._rate = rate;

    _ilk.save();
  }
  return _ilk;
}

export function getOrCreateInterestRate(
  marketAddress: string,
  side: string,
  type: string
): InterestRate {
  const interestRateID = side + "-" + type + "-" + marketAddress;
  let interestRate = InterestRate.load(interestRateID);
  if (interestRate) {
    return interestRate;
  }

  interestRate = new InterestRate(interestRateID);
  interestRate.side = side;
  interestRate.type = type;
  interestRate.rate = BIGDECIMAL_ONE;
  interestRate.save();

  return interestRate;
}

export function getLiquidateEvent(LiquidateID: string): Liquidate | null {
  const liquidate = Liquidate.load(LiquidateID);
  if (liquidate == null) {
    log.error("[getLiquidateEvent]Liquidate entity with id {} does not exist", [
      LiquidateID,
    ]);
    return null;
  }
  return liquidate;
}

export function getOrCreateLiquidate(
  LiquidateID: string,
  event: ethereum.Event | null = null,
  market: Market | null = null,
  liquidatee: string | null = null,
  liquidator: string | null = null,
  amount: BigInt | null = null,
  amountUSD: BigDecimal | null = null,
  profitUSD: BigDecimal | null = null
): Liquidate {
  let liquidate = Liquidate.load(LiquidateID);
  if (liquidate == null) {
    liquidate = new Liquidate(LiquidateID);
    liquidate.hash = event!.transaction.hash;
    liquidate.logIndex = event!.logIndex.toI32();
    liquidate.gasPrice = event!.transaction.gasPrice;
    liquidate.gasUsed = event!.receipt ? event!.receipt!.gasUsed : null;
    liquidate.gasLimit = event!.transaction.gasLimit;
    liquidate.nonce = event!.transaction.nonce;
    liquidate.liquidator = liquidator!;
    liquidate.liquidatee = liquidatee!;
    liquidate.blockNumber = event!.block.number;
    liquidate.timestamp = event!.block.timestamp;
    liquidate.market = market!.id;
    liquidate.asset = market!.inputToken;
    liquidate.amount = amount!;
    liquidate.amountUSD = amountUSD!;
    liquidate.profitUSD = profitUSD!;
    liquidate.positions = [];
    liquidate.save();
  }
  return liquidate;
}


export function getOrCreateChi(chiID: string): _Chi {
  let _chi = _Chi.load(chiID);
  if (_chi == null) {
    _chi = new _Chi(chiID);
    _chi.chi = BIGINT_ONE_RAY;
    _chi.rho = BIGINT_ZERO;
    _chi.save();
  }

  return _chi;
}

export function getOrCreatePosition(
  event: ethereum.Event,
  address: string,
  ilk: Bytes,
  side: string,
  newPosition: bool = false
): Position {
  const accountAddress = getOwnerAddress(address);
  const marketID = getMarketAddressFromIlk(ilk)!.toHexString();
  const market = getOrCreateMarket(marketID);
  const positionPrefix = `${address}-${marketID}-${side}`;
  const counterEnity = getOrCreatePositionCounter(event, address, ilk, side);
  let counter = counterEnity.nextCount;
  let positionID = `${positionPrefix}-${counter}`;
  let position = Position.load(positionID);

  if (newPosition && position != null) {
    // increase the counter to force a new position
    // this is necessary when receiving a transferred position
    counter += 1;
    positionID = `${positionPrefix}-${counter}`;
    position = Position.load(positionID);
    counterEnity.nextCount = counter;
    counterEnity.save();
  }

  if (position == null) {
    // new position
    position = new Position(positionID);
    position.market = marketID;
    position.asset = market.inputToken;
    position.account = accountAddress;
    position.hashOpened = event.transaction.hash;
    position.blockNumberOpened = event.block.number;
    position.timestampOpened = event.block.timestamp;
    position.side = side;
    position.balance = BIGINT_ZERO;
    position.depositCount = INT_ZERO;
    position.withdrawCount = INT_ZERO;
    position.borrowCount = INT_ZERO;
    position.repayCount = INT_ZERO;
    position.liquidationCount = INT_ZERO;
    position.transferredCount = INT_ZERO;
    position.receivedCount = INT_ZERO;

    if (side == PositionSide.COLLATERAL) {
      //isCollateral is always enabled for maker lender position
      position.isCollateral = true;
    } else {
      position.type = InterestRateType.VARIABLE;
    }

    position.save();
  }

  log.info(
    "[getOrCreatePosition]Get/create position positionID={}, account={}, balance={}",
    [positionID, accountAddress, position.balance.toString()]
  );
  return position;
}

// find the open position for the matching urn/ilk/side combination
// there should be only one or none
export function getOpenPosition(
  event: ethereum.Event,
  urn: string,
  ilk: Bytes,
  side: string
): Position | null {
  const marketID = getMarketAddressFromIlk(ilk)!.toHexString();
  const nextCounter = getNextPositionCounter(event, urn, ilk, side);
  log.info("[getOpenPosition]Finding open position for urn {}/ilk {}/side {}", [
    urn,
    ilk.toString(),
    side,
  ]);
  for (let counter = nextCounter; counter >= 0; counter--) {
    const positionID = `${urn}-${marketID}-${side}-${counter}`;
    const position = Position.load(positionID);
    if (position) {
      const hashClosed = position.hashClosed;
      const balance = position.balance.toString();
      const account = position.account;
      // position is open
      if (!hashClosed || hashClosed.length == 0) {
        log.info(
          "[getOpenPosition]found open position counter={}, position.id={}, account={}, balance={}, hashClosed={}",
          [counter.toString(), positionID, account, balance, "null"]
        );
        return position;
      } else {
        log.info(
          "[getOpenPosition]iterating counter={}, position.id={}, account={}, balance={}, hashClosed={}",
          [
            counter.toString(),
            positionID,
            account,
            balance,
            hashClosed.toString(),
          ]
        );
      }
    } else {
      log.info(
        "[getOpenPosition]iterating counter={}, position.id={} doesn't exist",
        [counter.toString(), positionID]
      );
    }
  }
  log.info("[getOpenPosition]No open position for urn {}/ilk {}/side {}", [
    urn,
    ilk.toString(),
    side,
  ]);

  return null;
}

export function getOrCreatePositionCounter(
  event: ethereum.Event,
  urn: string,
  ilk: Bytes,
  side: string
): _PositionCounter {
  const marketID = getMarketAddressFromIlk(ilk)!.toHexString();
  const ID = `${urn}-${marketID}-${side}`;
  let counterEnity = _PositionCounter.load(ID);
  if (!counterEnity) {
    counterEnity = new _PositionCounter(ID);
    counterEnity.nextCount = INT_ZERO;
    counterEnity.lastTimestamp = event.block.timestamp;
    counterEnity.save();
  }
  return counterEnity;
}

///////////////////////////
///////// Helpers /////////
///////////////////////////
export function getNextPositionCounter(
  event: ethereum.Event,
  urn: string,
  ilk: Bytes,
  side: string
): i32 {
  const counterEnity = getOrCreatePositionCounter(event, urn, ilk, side);
  return counterEnity.nextCount;
}

export function getMarketAddressFromIlk(ilk: Bytes): Address | null {
  const _ilk = getOrCreateIlk(ilk);
  if (_ilk) return Address.fromString(_ilk.marketAddress);

  log.warning("[getMarketAddressFromIlk]MarketAddress for ilk {} not found", [
    ilk.toString(),
  ]);
  return null;
}

export function getMarketFromIlk(ilk: Bytes): Market | null {
  const marketAddress = getMarketAddressFromIlk(ilk);
  if (marketAddress) {
    return getOrCreateMarket(marketAddress.toHexString());
  }
  return null;
}

export function getOwnerAddressFromUrn(urn: string): string | null {
  const _urn = _Urn.load(urn);
  if (_urn) {
    return _urn.ownerAddress;
  }
  return null;
}

export function getOwnerAddressFromProxy(proxy: string): string | null {
  const _proxy = _Proxy.load(proxy);
  if (_proxy) {
    return _proxy.ownerAddress;
  }
  return null;
}

// get owner address from possible urn or proxy address
// return itself if it is not an urn or proxy
export function getOwnerAddress(address: string): string {
  const urnOwner = getOwnerAddressFromUrn(address);
  let owner = urnOwner ? urnOwner : address;
  const proxyOwner = getOwnerAddressFromProxy(owner);
  owner = proxyOwner ? proxyOwner : owner;
  return owner;
}

// this is needed to prevent snapshot rates from being pointers to the current rate
export function getSnapshotRates(
  rates: string[],
  timeSuffix: string
): string[] {
  const snapshotRates: string[] = [];
  for (let i = 0; i < rates.length; i++) {
    const rate = InterestRate.load(rates[i]);
    if (!rate) {
      log.warning("[getSnapshotRates] rate {} not found, should not happen", [
        rates[i],
      ]);
      continue;
    }

    // create new snapshot rate
    const snapshotRateId = rates[i].concat("-").concat(timeSuffix);
    const snapshotRate = new InterestRate(snapshotRateId);
    snapshotRate.side = rate.side;
    snapshotRate.type = rate.type;
    snapshotRate.rate = rate.rate;
    snapshotRate.save();

    snapshotRates.push(snapshotRateId);
  }
  return snapshotRates;
}
