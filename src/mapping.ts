import {
  Address,
  BigDecimal,
  BigInt,
  dataSource,
  ethereum,
  log,
  Bytes,
  ByteArray,
  crypto,
} from "@graphprotocol/graph-ts";
import { PriceOracleUpdated } from "../generated/LendingPoolAddressesProvider/LendingPoolAddressesProvider";
import {
  BIGDECIMAL_ONE_RAD,
  BIGDECIMAL_ONE_RAY,
  BIGDECIMAL_ONE_WAD,
  BIGINT_ONE_RAD,
  DOG_ADDRESS,
  FLASHLOAN_PREMIUM_TOTAL,
  getNetworkSpecificConstant,
  JUG_ADDRESS,
  Protocol,
  SECONDS_PER_YEAR,
  SPOT_ADDRESS,
  supportedAssets,
  Transaction,
  VAT_ADDRESS,
  WETH_GATEWAY_ADDRESS,
} from "./helper/constants";
import {
  BorrowingDisabledOnReserve,
  BorrowingEnabledOnReserve,
  CollateralConfigurationChanged,
  ReserveActivated,
  ReserveDeactivated,
  ReserveFactorChanged,
  ReserveInitialized,
} from "../generated/LendingPoolConfigurator/LendingPoolConfigurator";
import {
  Borrow,
  Deposit,
  FlashLoan,
  LiquidationCall,
  Paused,
  Repay,
  ReserveDataUpdated,
  ReserveUsedAsCollateralDisabled,
  ReserveUsedAsCollateralEnabled,
  Unpaused,
  Withdraw,
  Swap,
} from "../generated/LendingPool/LendingPool";
import { ZToken } from "../generated/LendingPool/ZToken";
import { StableDebtToken } from "../generated/LendingPool/StableDebtToken";
import { VariableDebtToken } from "../generated/LendingPool/VariableDebtToken";

import {
  InterestRateMode,
  Network,
  SECONDS_PER_DAY,
  CollateralizationType,
  LendingType,
  PermissionType,
  RewardTokenType,
  RiskType,
  BIGDECIMAL_HUNDRED,
  BIGDECIMAL_ZERO,
  BIGINT_ZERO,
  DEFAULT_DECIMALS,
  IzarbanTokenType,
  INT_FOUR,
  RAY_OFFSET,
  ZERO_ADDRESS,
  BIGINT_ONE_RAY,
  BIGDECIMAL_NEG_ONE_CENT,
  InterestRateSide,
  InterestRateType,
  OracleSource,
  PositionSide,
  TransactionType,
  FeeType,
  TokenType,
  WAD,
  RAY,
  RAD,
  BIGDECIMAL_ONE,
  BIGDECIMAL_ONE_HUNDRED,
  SECONDS_PER_YEAR_BIGDECIMAL,
  VOW_ADDRESS,
  ZAR_ADDRESS,
  ProtocolSideRevenueType,
  BIGDECIMAL_NEG_ONE,
  BIGINT_NEGATIVE_ONE,
  INT_ZERO,
  INT_ONE,
  MIGRATION_ADDRESS,
} from "./helper/constants";
import {
  Account,
  Market,
  Token,
  Protocol as LendingProtocol,
  _DefaultOracle,
  _FlashLoanPremium,
  _ClipTakeStore,
  _Urn,
  _Cdpi,
  _Ilk,
} from "../generated/schema";
import { ZarbanIncentivesController } from "../generated/LendingPool/ZarbanIncentivesController";
import { StakedZarban } from "../generated/LendingPool/StakedZarban";
import {
  AssetSourceUpdated,
  IPriceOracleGetter,
} from "../generated/LendingPool/IPriceOracleGetter";
import { BalanceTransfer as CollateralTransfer } from "../generated/templates/ZToken/ZToken";

import {
  equalsIgnoreCase,
  readValue,
  getBorrowBalances,
  getCollateralBalance,
  getMarketByAuxillaryToken,
  exponentToBigDecimal,
  rayToWad,
  getMarketFromToken,
  getOrCreateFlashloanPremium,
  getInterestRateType,
  getFlashloanPremiumAmount,
  calcuateFlashLoanPremiumToLPUSD,
  getPrincipal,
  updateOrCreateUrn,
} from "./helper/helpers";

import { TokenManager } from "./helper/token";

import {
  ZToken as ZTokenTemplate,
  VariableDebtToken as VTokenTemplate,
  StableDebtToken as STokenTemplate,
  ZarbanOracle,
} from "../generated/templates";
import { ERC20 } from "../generated/LendingPool/ERC20";
import { DataManager, ProtocolData, RewardData } from "./helper/manager";
import { AccountManager } from "./helper/account";
import { PositionManager } from "./helper/position";
import { GemJoin } from "../generated/Vat/GemJoin";
import {
  Vat,
  Rely as VatRelyEvent,
  Cage as VatCageEvent,
  Frob as VatFrobEvent,
  Grab as VatGrabEvent,
  Fork as VatForkEvent,
  Fold as VatFoldEvent,
  File1 as VatFileEvent,
} from "../generated/Vat/Vat";
import {
  Dog,
  Bark as BarkEvent,
  File2 as DogFileChopAndHoleEvent,
  File3 as DogFileClipEvent,
  Digs as DogDigsEvent,
} from "../generated/Dog/Dog";
import { Clip, Pip } from "../generated/templates";
import {
  Take as TakeEvent,
  Yank as ClipYankEvent,
  Clip as ClipContract,
} from "../generated/templates/Clip/Clip";
import {
  Poke as PokeEvent,
  Spot,
  File as SpotFileMatEvent,
  File1 as SpotFilePipEvent,
  File2 as SpotFileParEvent,
} from "../generated/Spot/Spot";
import {
  LogValue as PipLogValueEvent,
  Pip as PipContract,
} from "../generated/templates/Pip/Pip";
import { Jug, File as JugFileEvent } from "../generated/Jug/Jug";
import { CdpManager, NewCdp } from "../generated/CdpManager/CdpManager";
import {
  bigIntToBDUseDecimals,
  bigDecimalExponential,
  bigIntChangeDecimals,
} from "./utils/numbers";
import { bytesToUnsignedBigInt } from "./utils/bytes";
import {
  updateUsageMetrics,
  updateFinancialsSnapshot,
  updateProtocol,
  createTransactions,
  updatePriceForMarket,
  updateRevenue,
  updateMarket,
  snapshotMarket,
  transferPosition,
  liquidatePosition,
  updatePosition,
} from "./helper/helpers";
import {
  getOwnerAddress,
  getOrCreateInterestRate,
  getOrCreateMarket,
  getOrCreateIlk,
  getOrCreateLendingProtocol,
  getMarketFromIlk,
  getMarketAddressFromIlk,
  getOrCreateLiquidate,
} from "./helper/getters";
import { createEventID } from "./utils/strings";

export function getProtocolData(): ProtocolData {
  const constants = getNetworkSpecificConstant();
  return new ProtocolData(
    constants.protocolAddress.toHexString(),
    Protocol.PROTOCOL,
    Protocol.NAME,
    Protocol.SLUG,
    constants.network,
    LendingType.POOLED,
    PermissionType.PERMISSIONLESS,
    PermissionType.PERMISSIONLESS,
    PermissionType.ADMIN,
    CollateralizationType.OVER_COLLATERALIZED,
    RiskType.GLOBAL
  );
}

const protocolData = getProtocolData();

///////////////////////////////////////////////
///// LendingPoolAddressProvider Handlers /////
///////////////////////////////////////////////

export function handlePriceOracleUpdated(event: PriceOracleUpdated): void {
  const newPriceOracle = event.params.newAddress;

  log.info("[_handlePriceOracleUpdated] New oracleAddress: {}", [
    newPriceOracle.toHexString(),
  ]);

  // since all Zarban markets share the same oracle
  // we will use _DefaultOracle entity for markets whose oracle is not set
  let defaultOracle = _DefaultOracle.load(protocolData.protocolID);
  if (!defaultOracle) {
    defaultOracle = new _DefaultOracle(protocolData.protocolID);
  }
  defaultOracle.oracle = newPriceOracle;
  defaultOracle.supportedAssets = [];
  defaultOracle.save();

  ZarbanOracle.create(newPriceOracle);

  const protocol = LendingProtocol.load(protocolData.protocolID);
  let markets: Market[];
  if (protocol === null) {
    log.warning("[_handleUnpaused] LendingProtocol does not exist", []);
    return;
  } else {
    markets = protocol.markets.load();
    if (!markets) {
      log.warning(
        "[_handlePriceOracleUpdated] marketList for {} does not exist",
        [protocolData.protocolID]
      );
      return;
    }

    for (let i = 0; i < markets.length; i++) {
      const _market = Market.load(markets[i].id);
      if (!_market) {
        log.warning("[_handlePriceOracleUpdated] Market not found: {}", [
          markets[i].id,
        ]);
        continue;
      }
      const manager = new DataManager(markets[i].id, _market.inputToken, event);
      _market.oracle = manager.getOrCreateOracle(
        newPriceOracle,
        true,
        OracleSource.CHAINLINK
      ).id;
      _market.oracle;
      _market.save();
    }
  }
}

//////////////////////////////////////
///// Lending Pool Configuration /////
//////////////////////////////////////

export function handleReserveInitialized(event: ReserveInitialized): void {
  const underlyingToken = event.params.asset;
  const outputToken = event.params.zToken;
  const variableDebtToken = event.params.variableDebtToken;
  const stableDebtToken =
    event.params.stableDebtToken || Address.fromString(ZERO_ADDRESS);

  // create VToken from template
  VTokenTemplate.create(variableDebtToken);
  // create ZToken from template
  ZTokenTemplate.create(outputToken);

  const manager = new DataManager(
    outputToken.toHexString(),
    underlyingToken.toHexString(),
    event
  );
  const market = manager.getMarket();

  const outputTokenManager = new TokenManager(outputToken.toHexString(), event);
  const vDebtTokenManager = new TokenManager(
    variableDebtToken.toHexString(),
    event,
    TokenType.REBASING
  );
  market.outputToken = outputTokenManager.getToken().id;
  market._vToken = vDebtTokenManager.getToken().id;

  // map tokens to market
  const inputToken = manager.getInputToken();
  inputToken._market = market.id;
  inputToken._izarbanTokenType = IzarbanTokenType.INPUTTOKEN;
  inputToken.save();

  const ZToken = outputTokenManager.getToken();
  ZToken._market = market.id;
  ZToken._izarbanTokenType = IzarbanTokenType.ZTOKEN;
  ZToken.save();

  const vToken = vDebtTokenManager.getToken();
  vToken._market = market.id;
  vToken._izarbanTokenType = IzarbanTokenType.VTOKEN;
  vToken.save();

  if (stableDebtToken != Address.zero()) {
    const sDebtTokenManager = new TokenManager(
      stableDebtToken.toHexString(),
      event
    );
    const sToken = sDebtTokenManager.getToken();
    sToken._market = market.id;
    sToken._izarbanTokenType = IzarbanTokenType.STOKEN;
    sToken.save();

    market._sToken = sToken.id;

    STokenTemplate.create(stableDebtToken);
  }

  const defaultOracle = _DefaultOracle.load(protocolData.protocolID);
  if (!market.oracle && defaultOracle) {
    market.oracle = manager.getOrCreateOracle(
      Address.fromBytes(defaultOracle.oracle),
      true,
      OracleSource.CHAINLINK
    ).id;
  }

  market.save();
}

export function handleCollateralConfigurationChanged(
  event: CollateralConfigurationChanged
): void {
  const asset = event.params.asset;
  const liquidationPenalty = event.params.liquidationBonus;
  const liquidationThreshold = event.params.liquidationThreshold;
  const maximumLTV = event.params.ltv;
  const market = getMarketFromToken(asset, protocolData);
  if (!market) {
    log.warning(
      "[_handleCollateralConfigurationChanged] Market for asset {} not found",
      [asset.toHexString()]
    );
    return;
  }

  market.maximumLTV = maximumLTV.toBigDecimal().div(BIGDECIMAL_HUNDRED);
  market.liquidationThreshold = liquidationThreshold
    .toBigDecimal()
    .div(BIGDECIMAL_HUNDRED);

  // The liquidation bonus value is equal to the liquidation penalty, the naming is a matter of which side of the liquidation a user is on
  // The liquidationBonus parameter comes out as above 100%, represented by a 5 digit integer over 10000 (100%).
  // To extract the expected value in the liquidationPenalty field: convert to BigDecimal, subtract by 10000 and divide by 100
  const bdLiquidationPenalty = liquidationPenalty.toBigDecimal();
  if (bdLiquidationPenalty.gt(exponentToBigDecimal(INT_FOUR))) {
    market.liquidationPenalty = bdLiquidationPenalty
      .minus(exponentToBigDecimal(INT_FOUR))
      .div(BIGDECIMAL_HUNDRED);
  }

  market.save();
}

export function handleBorrowingEnabledOnReserve(
  event: BorrowingEnabledOnReserve
): void {
  const asset = event.params.asset;
  const market = getMarketFromToken(asset, protocolData);
  if (!market) {
    log.warning("[_handleBorrowingEnabledOnReserve] Market not found {}", [
      asset.toHexString(),
    ]);
    return;
  }

  market.canBorrowFrom = true;
  market.save();
}

export function handleBorrowingDisabledOnReserve(
  event: BorrowingDisabledOnReserve
): void {
  const asset = event.params.asset;
  const market = getMarketFromToken(asset, protocolData);
  if (!market) {
    log.warning(
      "[_handleBorrowingDisabledOnReserve] Market for token {} not found",
      [asset.toHexString()]
    );
    return;
  }

  market.canBorrowFrom = false;
  market.save();
}

export function handleReserveActivated(event: ReserveActivated): void {
  const asset = event.params.asset;
  const market = getMarketFromToken(asset, protocolData);
  if (!market) {
    log.warning("[_handleReserveActivated] Market for token {} not found", [
      asset.toHexString(),
    ]);
    return;
  }

  market.isActive = true;
  market.save();
}

export function handleReserveDeactivated(event: ReserveDeactivated): void {
  const asset = event.params.asset;
  const market = getMarketFromToken(asset, protocolData);
  if (!market) {
    log.warning("[_handleReserveDeactivated] Market for token {} not found", [
      asset.toHexString(),
    ]);
    return;
  }

  market.isActive = false;
  market.save();
}

export function handleReserveFactorChanged(event: ReserveFactorChanged): void {
  const asset = event.params.asset;
  const reserveFactor = event.params.factor;
  const market = getMarketFromToken(asset, protocolData);
  if (!market) {
    log.warning("[_handleReserveFactorChanged] Market for token {} not found", [
      asset.toHexString(),
    ]);
    return;
  }

  market.reserveFactor = reserveFactor
    .toBigDecimal()
    .div(exponentToBigDecimal(INT_FOUR));
  market.save();
}

/////////////////////////////////
///// Lending Pool Handlers /////
/////////////////////////////////

export function handleReserveDataUpdated(event: ReserveDataUpdated): void {
  const liquidityRate = event.params.liquidityRate; // deposit rate in ray
  const liquidityIndex = event.params.liquidityIndex;
  const variableBorrowIndex = event.params.variableBorrowIndex;
  const variableBorrowRate = event.params.variableBorrowRate;
  const stableBorrowRate = event.params.stableBorrowRate;
  const asset = event.params.reserve;

  const market = getMarketFromToken(asset, protocolData);
  if (!market) {
    log.warning("[handleReserveDataUpdated] Market not found for reserve {}", [
      event.params.reserve.toHexString(),
    ]);
    return;
  }
  const manager = new DataManager(market.id, market.inputToken, event);
  updateRewards(manager, event);

  let assetPriceUSD =
    getAssetPriceInUSDC(
      Address.fromString(market.inputToken),
      manager.getOracleAddress()
    ) || BIGDECIMAL_ZERO;

  if (!market) {
    log.warning("[_handlReserveDataUpdated] Market for asset {} not found", [
      asset.toHexString(),
    ]);
    return;
  }

  const inputToken = manager.getInputToken();
  // get current borrow balance
  let trySBorrowBalance: ethereum.CallResult<BigInt> | null = null;
  if (market._sToken) {
    const stableDebtContract = StableDebtToken.bind(
      Address.fromString(market._sToken!)
    );
    trySBorrowBalance = stableDebtContract.try_totalSupply();
  }

  const variableDebtContract = VariableDebtToken.bind(
    Address.fromString(market._vToken!)
  );
  const tryVBorrowBalance = variableDebtContract.try_totalSupply();
  let sBorrowBalance = BIGINT_ZERO;
  let vBorrowBalance = BIGINT_ZERO;

  if (trySBorrowBalance != null && !trySBorrowBalance.reverted) {
    sBorrowBalance = trySBorrowBalance.value;
  }
  if (!tryVBorrowBalance.reverted) {
    vBorrowBalance = tryVBorrowBalance.value;
  }

  // broken if both revert
  if (
    trySBorrowBalance != null &&
    trySBorrowBalance.reverted &&
    tryVBorrowBalance.reverted
  ) {
    log.warning("[ReserveDataUpdated] No borrow balance found", []);
    return;
  }

  // update total supply balance
  const ZTokenContract = ZToken.bind(Address.fromString(market.outputToken!));
  const tryTotalSupply = ZTokenContract.try_totalSupply();
  if (tryTotalSupply.reverted) {
    log.warning(
      "[ReserveDataUpdated] Error getting total supply on market: {}",
      [market.id]
    );
    return;
  }

  if (assetPriceUSD.equals(BIGDECIMAL_ZERO)) {
    assetPriceUSD = market.inputTokenPriceUSD;
  }
  manager.updateMarketAndProtocolData(
    assetPriceUSD,
    tryTotalSupply.value,
    vBorrowBalance,
    sBorrowBalance,
    tryTotalSupply.value
  );

  const tryScaledSupply = ZTokenContract.try_scaledTotalSupply();
  if (tryScaledSupply.reverted) {
    log.warning(
      "[ReserveDataUpdated] Error getting scaled total supply on market: {}",
      [asset.toHexString()]
    );
    return;
  }

  // calculate new revenue
  // New Interest = totalScaledSupply * (difference in liquidity index)
  let currSupplyIndex = market.supplyIndex;
  if (!currSupplyIndex) {
    manager.updateSupplyIndex(BIGINT_ONE_RAY);
    currSupplyIndex = BIGINT_ONE_RAY;
  }
  const liquidityIndexDiff = liquidityIndex
    .minus(currSupplyIndex)
    .toBigDecimal()
    .div(exponentToBigDecimal(RAY_OFFSET));
  manager.updateSupplyIndex(liquidityIndex); // must update to current liquidity index
  manager.updateBorrowIndex(variableBorrowIndex);

  const newRevenueBD = tryScaledSupply.value
    .toBigDecimal()
    .div(exponentToBigDecimal(inputToken.decimals))
    .times(liquidityIndexDiff);
  let totalRevenueDeltaUSD = newRevenueBD.times(assetPriceUSD);

  const receipt = event.receipt;
  let FlashLoanPremiumToLPUSD = BIGDECIMAL_ZERO;
  if (!receipt) {
    log.warning(
      "[_handleReserveDataUpdated]No receipt for tx {}; cannot subtract Flashloan revenue",
      [event.transaction.hash.toHexString()]
    );
  } else {
    const flashLoanPremiumAmount = getFlashloanPremiumAmount(event, asset);
    const flashLoanPremiumUSD = flashLoanPremiumAmount
      .toBigDecimal()
      .div(exponentToBigDecimal(inputToken.decimals))
      .times(assetPriceUSD);
    const flashloanPremium = getOrCreateFlashloanPremium(protocolData);
    FlashLoanPremiumToLPUSD = calcuateFlashLoanPremiumToLPUSD(
      flashLoanPremiumUSD,
      flashloanPremium.premiumRateToProtocol
    );
  }

  // deduct flashloan premium that may have already been accounted for in
  // _handleFlashloan()
  totalRevenueDeltaUSD = totalRevenueDeltaUSD.minus(FlashLoanPremiumToLPUSD);
  if (
    totalRevenueDeltaUSD.lt(BIGDECIMAL_ZERO) &&
    totalRevenueDeltaUSD.gt(BIGDECIMAL_NEG_ONE_CENT)
  ) {
    // totalRevenueDeltaUSD may become a tiny negative number after
    // subtracting flashloan premium due to rounding
    totalRevenueDeltaUSD = BIGDECIMAL_ZERO;
  }
  let reserveFactor = market.reserveFactor;
  if (!reserveFactor) {
    log.warning(
      "[_handleReserveDataUpdated]reserveFactor = null for market {}, default to 0.0",
      [asset.toHexString()]
    );
    reserveFactor = BIGDECIMAL_ZERO;
  }

  const protocolSideRevenueDeltaUSD = totalRevenueDeltaUSD.times(reserveFactor);
  const supplySideRevenueDeltaUSD = totalRevenueDeltaUSD.minus(
    protocolSideRevenueDeltaUSD
  );

  const fee = manager.getOrUpdateFee(
    FeeType.PROTOCOL_FEE,
    market.reserveFactor
  );

  manager.addProtocolRevenue(protocolSideRevenueDeltaUSD, fee);
  manager.addSupplyRevenue(supplySideRevenueDeltaUSD, fee);

  manager.getOrUpdateRate(
    InterestRateSide.BORROWER,
    InterestRateType.VARIABLE,
    rayToWad(variableBorrowRate)
      .toBigDecimal()
      .div(exponentToBigDecimal(DEFAULT_DECIMALS - 2))
  );

  if (market._sToken) {
    // geist does not have stable borrow rates
    manager.getOrUpdateRate(
      InterestRateSide.BORROWER,
      InterestRateType.STABLE,
      rayToWad(stableBorrowRate)
        .toBigDecimal()
        .div(exponentToBigDecimal(DEFAULT_DECIMALS - 2))
    );
  }

  manager.getOrUpdateRate(
    InterestRateSide.LENDER,
    InterestRateType.VARIABLE,
    rayToWad(liquidityRate)
      .toBigDecimal()
      .div(exponentToBigDecimal(DEFAULT_DECIMALS - 2))
  );
}

export function handleReserveUsedAsCollateralEnabled(
  event: ReserveUsedAsCollateralEnabled
): void {
  // This Event handler enables a reserve/market to be used as collateral
  const asset = event.params.reserve;
  const accountID = event.params.user;
  const market = getMarketFromToken(asset, protocolData);
  if (!market) {
    log.warning(
      "[_handleReserveUsedAsCollateralEnabled] Market for token {} not found",
      [asset.toHexString()]
    );
    return;
  }
  const accountManager = new AccountManager(accountID.toHexString());
  const account = accountManager.getAccount();

  const markets = account._enabledCollaterals
    ? account._enabledCollaterals!
    : [];
  markets.push(market.id);
  account._enabledCollaterals = markets;

  account.save();
}

export function handleReserveUsedAsCollateralDisabled(
  event: ReserveUsedAsCollateralDisabled
): void {
  // This Event handler disables a reserve/market being used as collateral
  const asset = event.params.reserve;
  const accountID = event.params.user;
  const market = getMarketFromToken(asset, protocolData);
  if (!market) {
    log.warning(
      "[_handleReserveUsedAsCollateralEnabled] Market for token {} not found",
      [asset.toHexString()]
    );
    return;
  }
  const accountManager = new AccountManager(accountID.toHexString());
  const account = accountManager.getAccount();

  const markets = account._enabledCollaterals
    ? account._enabledCollaterals!
    : [];

  const index = markets.indexOf(market.id);
  if (index >= 0) {
    // drop 1 element at given index
    markets.splice(index, 1);
  }
  account._enabledCollaterals = markets;
  account.save();
}

// eslint-disable-next-line @typescript-eslint/no-unused-vars
export function handlePaused(event: Paused): void {
  const protocol = LendingProtocol.load(protocolData.protocolID);
  let markets: Market[];
  if (protocol === null) {
    log.warning("[_handleUnpaused] LendingProtocol does not exist", []);
    return;
  } else {
    markets = protocol.markets.load();
  }
  if (!markets) {
    log.warning("[_handlePaused]marketList for {} does not exist", [
      protocolData.protocolID,
    ]);
    return;
  }

  for (let i = 0; i < markets.length; i++) {
    const market = Market.load(markets[i].id);
    if (!market) {
      log.warning("[Paused] Market not found: {}", [markets[i].id]);
      continue;
    }

    market.isActive = false;
    market.canUseAsCollateral = false;
    market.canBorrowFrom = false;
    market.save();
  }
}

// eslint-disable-next-line @typescript-eslint/no-unused-vars
export function handleUnpaused(event: Unpaused): void {
  const protocol = LendingProtocol.load(protocolData.protocolID);
  let markets: Market[];
  if (protocol === null) {
    log.warning("[_handleUnpaused] LendingProtocol does not exist", []);
    return;
  } else {
    markets = protocol.markets.load();
  }
  if (!markets) {
    log.warning("[_handleUnpaused]marketList for {} does not exist", [
      protocolData.protocolID,
    ]);
    return;
  }

  for (let i = 0; i < markets.length; i++) {
    const market = Market.load(markets[i].id);
    if (!market) {
      log.warning("[_handleUnpaused] Market not found: {}", [markets[i].id]);
      continue;
    }
  }
}

export function handleDeposit(event: Deposit): void {
  const amount = event.params.amount;
  const asset = event.params.reserve;
  const accountID = event.params.onBehalfOf;
  const market = getMarketFromToken(asset, protocolData);
  if (!market) {
    log.warning("[_handleDeposit] Market for token {} not found", [
      asset.toHexString(),
    ]);
    return;
  }
  const manager = new DataManager(market.id, market.inputToken, event);
  const tokenManager = new TokenManager(
    asset.toHexString(),
    event,
    TokenType.REBASING
  );
  const amountUSD = tokenManager.getAmountUSD(amount);
  const newCollateralBalance = getCollateralBalance(market, accountID);
  const principal = getPrincipal(market, accountID, PositionSide.COLLATERAL);
  manager.createDeposit(
    asset.toHexString(),
    accountID.toHexString(),
    amount,
    amountUSD,
    newCollateralBalance,
    null,
    principal
  );
  const account = Account.load(accountID.toHexString());
  if (!account) {
    log.warning("[_handleDeposit]account {} not found", [
      accountID.toHexString(),
    ]);
    return;
  }
  const positionManager = new PositionManager(
    account,
    market,
    PositionSide.COLLATERAL
  );
  if (
    !account._enabledCollaterals ||
    account._enabledCollaterals!.indexOf(market.id) == -1
  ) {
    return;
  }
  positionManager.setCollateral(true);
}

export function handleWithdraw(event: Withdraw): void {
  const amount = event.params.amount;
  const asset = event.params.reserve;
  let accountID = event.params.user;
  if (accountID.toHexString().toLowerCase() == WETH_GATEWAY_ADDRESS) {
    accountID = event.transaction.from;
  }
  const market = getMarketFromToken(asset, protocolData);
  if (!market) {
    log.warning("[_handleWithdraw] Market for token {} not found", [
      asset.toHexString(),
    ]);
    return;
  }
  const manager = new DataManager(market.id, market.inputToken, event);

  const tokenManager = new TokenManager(
    asset.toHexString(),
    event,
    TokenType.REBASING
  );
  const amountUSD = tokenManager.getAmountUSD(amount);
  const newCollateralBalance = getCollateralBalance(market, accountID);
  const principal = getPrincipal(market, accountID, PositionSide.COLLATERAL);
  manager.createWithdraw(
    asset.toHexString(),
    accountID.toHexString(),
    amount,
    amountUSD,
    newCollateralBalance,
    null,
    principal
  );
}

export function handleBorrow(event: Borrow): void {
  const amount = event.params.amount;
  const asset = event.params.reserve;
  const accountID = event.params.onBehalfOf;
  const market = getMarketFromToken(asset, protocolData);
  if (!market) {
    log.warning("[_handleBorrow] Market for token {} not found", [
      asset.toHexString(),
    ]);
    return;
  }
  const manager = new DataManager(market.id, market.inputToken, event);
  const tokenManager = new TokenManager(
    asset.toHexString(),
    event,
    TokenType.REBASING
  );
  const amountUSD = tokenManager.getAmountUSD(amount);
  const newBorrowBalances = getBorrowBalances(market, accountID);
  const principal = getPrincipal(market, accountID, PositionSide.BORROWER);

  manager.createBorrow(
    asset.toHexString(),
    accountID.toHexString(),
    amount,
    amountUSD,
    newBorrowBalances[0].plus(newBorrowBalances[1]),
    market.inputTokenPriceUSD,
    null,
    principal
  );
}

export function handleRepay(event: Repay): void {
  const amount = event.params.amount;
  const asset = event.params.reserve;
  const accountID = event.params.user;
  const market = getMarketFromToken(asset, protocolData);
  if (!market) {
    log.warning("[_handleRepay] Market for token {} not found", [
      asset.toHexString(),
    ]);
    return;
  }
  const manager = new DataManager(market.id, market.inputToken, event);
  const tokenManager = new TokenManager(
    asset.toHexString(),
    event,
    TokenType.REBASING
  );
  const amountUSD = tokenManager.getAmountUSD(amount);
  const newBorrowBalances = getBorrowBalances(market, accountID);

  // use debtToken Transfer event for Burn/Mint to determine interestRateType of the Repay event
  const interestRateType = getInterestRateType(event);
  if (!interestRateType) {
    log.error(
      "[_handleRepay]Cannot determine interest rate type for Repay event {}-{}",
      [
        event.transaction.hash.toHexString(),
        event.transactionLogIndex.toString(),
      ]
    );
  }

  const principal = getPrincipal(
    market,
    accountID,
    PositionSide.BORROWER,
    interestRateType
  );

  manager.createRepay(
    asset.toHexString(),
    accountID.toHexString(),
    amount,
    amountUSD,
    newBorrowBalances[0].plus(newBorrowBalances[1]),
    market.inputTokenPriceUSD,
    interestRateType,
    principal
  );
}

export function handleLiquidationCall(event: LiquidationCall): void {
  const amount = event.params.liquidatedCollateralAmount; // amount of collateral liquidated
  const collateralAsset = event.params.collateralAsset; // collateral market
  const liquidator = event.params.liquidator;
  const liquidatee = event.params.user; // account liquidated
  const debtAsset = event.params.debtAsset; // token repaid to cover debt,
  const debtToCover = event.params.debtToCover; // the amount of debt repaid by liquidator

  const market = getMarketFromToken(collateralAsset, protocolData);
  if (!market) {
    log.warning("[_handleLiquidate] Market for token {} not found", [
      collateralAsset.toHexString(),
    ]);
    return;
  }
  const manager = new DataManager(market.id, market.inputToken, event);
  const inputToken = manager.getInputToken();
  let inputTokenPriceUSD = market.inputTokenPriceUSD;
  if (!inputTokenPriceUSD) {
    log.warning(
      "[_handleLiquidate] Price of input token {} is not set, default to 0.0",
      [inputToken.id]
    );
    inputTokenPriceUSD = BIGDECIMAL_ZERO;
  }
  const amountUSD = amount
    .toBigDecimal()
    .div(exponentToBigDecimal(inputToken.decimals))
    .times(inputTokenPriceUSD);

  const fee = manager.getOrUpdateFee(FeeType.LIQUIDATION_FEE, BIGDECIMAL_ZERO);
  manager.addProtocolRevenue(BIGDECIMAL_ZERO, fee);

  const debtTokenMarket = getMarketFromToken(debtAsset, protocolData);
  if (!debtTokenMarket) {
    log.warning("[_handleLiquidate] market for Debt token  {} not found", [
      debtAsset.toHexString(),
    ]);
    return;
  }
  let debtTokenPriceUSD = debtTokenMarket.inputTokenPriceUSD;
  if (!debtTokenPriceUSD) {
    log.warning(
      "[_handleLiquidate] Price of token {} is not set, default to 0.0",
      [debtAsset.toHexString()]
    );
    debtTokenPriceUSD = BIGDECIMAL_ZERO;
  }
  const profitUSD = amountUSD.minus(
    debtToCover.toBigDecimal().times(debtTokenPriceUSD)
  );
  const collateralBalance = getCollateralBalance(market, liquidatee);
  const debtBalances = getBorrowBalances(debtTokenMarket, liquidatee);
  const totalDebtBalance = debtBalances[0].plus(debtBalances[1]);
  const subtractBorrowerPosition = false;
  const collateralPrincipal = getPrincipal(
    market,
    liquidatee,
    PositionSide.COLLATERAL
  );
  const liquidate = manager.createLiquidate(
    collateralAsset.toHexString(),
    debtAsset.toHexString(),
    liquidator,
    liquidatee,
    amount,
    amountUSD,
    profitUSD,
    collateralBalance,
    totalDebtBalance,
    null,
    subtractBorrowerPosition,
    collateralPrincipal
  );
  if (!liquidate) {
    return;
  }

  const liquidatedPositions = liquidate.positions;
  const liquidateeAccount = new AccountManager(
    liquidatee.toHexString()
  ).getAccount();
  const protocol = manager.getOrCreateProtocol();
  // Use the Transfer event for debtToken to burn to determine the interestRateType for debtToken liquidated

  // Variable debt is liquidated first
  const vBorrowerPosition = new PositionManager(
    liquidateeAccount,
    debtTokenMarket,
    PositionSide.BORROWER,
    InterestRateType.VARIABLE
  );

  const vBorrowerPositionBalance = vBorrowerPosition._getPositionBalance();
  if (vBorrowerPositionBalance && vBorrowerPositionBalance.gt(BIGINT_ZERO)) {
    const vPrincipal = getPrincipal(
      market,
      Address.fromString(liquidateeAccount.id),
      PositionSide.BORROWER,
      InterestRateType.VARIABLE
    );
    vBorrowerPosition.subtractPosition(
      event,
      protocol,
      debtBalances[1],
      TransactionType.LIQUIDATE,
      debtTokenMarket.inputTokenPriceUSD,
      vPrincipal
    );
    liquidatedPositions.push(vBorrowerPosition.getPositionID()!);
  }

  const sBorrowerPosition = new PositionManager(
    liquidateeAccount,
    debtTokenMarket,
    PositionSide.BORROWER,
    InterestRateType.STABLE
  );

  const sBorrowerPositionBalance = sBorrowerPosition._getPositionBalance();
  // Stable debt is liquidated after exhuasting variable debt
  if (
    debtBalances[1].equals(BIGINT_ZERO) &&
    sBorrowerPositionBalance &&
    sBorrowerPositionBalance.gt(BIGINT_ZERO)
  ) {
    const sPrincipal = getPrincipal(
      market,
      Address.fromString(liquidateeAccount.id),
      PositionSide.BORROWER,
      InterestRateType.STABLE
    );
    sBorrowerPosition.subtractPosition(
      event,
      protocol,
      debtBalances[0],
      TransactionType.LIQUIDATE,
      debtTokenMarket.inputTokenPriceUSD,
      sPrincipal
    );
    liquidatedPositions.push(sBorrowerPosition.getPositionID()!);
  }

  liquidate.positions = liquidatedPositions;
  liquidate.save();
}

export function handleFlashloan(event: FlashLoan): void {
  const flashloanPremium = getOrCreateFlashloanPremium(protocolData);
  flashloanPremium.premiumRateTotal = FLASHLOAN_PREMIUM_TOTAL;
  flashloanPremium.save();

  const asset = event.params.asset;
  const amount = event.params.amount;
  const account = event.params.initiator;
  const premiumAmount = event.params.premium;

  const market = getMarketFromToken(asset, protocolData);
  if (!market) {
    log.warning("[_handleFlashLoan] market for token {} not found", [
      asset.toHexString(),
    ]);
    return;
  }
  const manager = new DataManager(market.id, market.inputToken, event);
  const tokenManager = new TokenManager(asset.toHexString(), event);
  const amountUSD = tokenManager.getAmountUSD(amount);
  const flashloan = manager.createFlashloan(asset, account, amount, amountUSD);
  const premiumUSDTotal = tokenManager.getAmountUSD(premiumAmount);
  flashloan.feeAmount = premiumAmount;
  flashloan.feeAmountUSD = premiumUSDTotal;
  flashloan.save();

  let premiumUSDToProtocol = BIGDECIMAL_ZERO;
  if (flashloanPremium.premiumRateToProtocol.gt(BIGDECIMAL_ZERO)) {
    // premium to protocol = total premium * premiumRateToProtocol
    premiumUSDToProtocol = premiumUSDTotal.times(
      flashloanPremium.premiumRateToProtocol
    );
    const feeToProtocol = manager.getOrUpdateFee(
      FeeType.FLASHLOAN_PROTOCOL_FEE,
      flashloanPremium.premiumRateToProtocol
    );
    manager.addProtocolRevenue(premiumUSDToProtocol, feeToProtocol);
  }

  // flashloan premium to LP is accrued in liquidityIndex and handled in
  // _handleReserveDataUpdated;
  const premiumRateToLP = flashloanPremium.premiumRateTotal.minus(
    flashloanPremium.premiumRateToProtocol
  );
  const feeToLP = manager.getOrUpdateFee(
    FeeType.FLASHLOAN_LP_FEE,
    premiumRateToLP
  );

  const premiumUSDToLP = premiumUSDTotal.minus(premiumUSDToProtocol);
  manager.addSupplyRevenue(premiumUSDToLP, feeToLP);
}

export function handleSwapBorrowRateMode(event: Swap): void {
  const interestRateMode = event.params.rateMode.toI32();
  if (
    ![InterestRateMode.STABLE, InterestRateMode.VARIABLE].includes(
      interestRateMode
    )
  ) {
    log.error(
      "[handleSwapBorrowRateMode]interestRateMode {} is not one of [{}, {}]",
      [
        interestRateMode.toString(),
        InterestRateMode.STABLE.toString(),
        InterestRateMode.VARIABLE.toString(),
      ]
    );
    return;
  }

  const interestRateType =
    interestRateMode === InterestRateMode.STABLE
      ? InterestRateType.STABLE
      : InterestRateType.VARIABLE;
  const market = getMarketFromToken(event.params.reserve, protocolData);
  if (!market) {
    log.error("[handleLiquidationCall]Failed to find market for asset {}", [
      event.params.reserve.toHexString(),
    ]);
    return;
  }

  const user = event.params.user;
  const newBorrowBalances = getBorrowBalances(market, event.params.user);
  const account = new AccountManager(user.toHexString()).getAccount();
  const manager = new DataManager(market.id, market.inputToken, event);
  const protocol = manager.getOrCreateProtocol();
  const sPositionManager = new PositionManager(
    account,
    market,
    PositionSide.BORROWER,
    InterestRateType.STABLE
  );
  const vPositionManager = new PositionManager(
    account,
    market,
    PositionSide.BORROWER,
    InterestRateType.VARIABLE
  );
  const stableTokenBalance = newBorrowBalances[0];
  const variableTokenBalance = newBorrowBalances[1];
  const vPrincipal = getPrincipal(
    market,
    Address.fromString(account.id),
    PositionSide.BORROWER,
    InterestRateType.VARIABLE
  );
  const sPrincipal = getPrincipal(
    market,
    Address.fromString(account.id),
    PositionSide.BORROWER,
    InterestRateType.STABLE
  );
  //all open position converted to STABLE
  if (interestRateType === InterestRateType.STABLE) {
    vPositionManager.subtractPosition(
      event,
      protocol,
      variableTokenBalance,
      TransactionType.SWAP,
      market.inputTokenPriceUSD,
      vPrincipal
    );
    sPositionManager.addPosition(
      event,
      market.inputToken,
      protocol,
      stableTokenBalance,
      TransactionType.SWAP,
      market.inputTokenPriceUSD,
      sPrincipal
    );
  } else {
    //all open position converted to VARIABLE
    vPositionManager.addPosition(
      event,
      market.inputToken,
      protocol,
      variableTokenBalance,
      TransactionType.SWAP,
      market.inputTokenPriceUSD,
      vPrincipal
    );
    sPositionManager.subtractPosition(
      event,
      protocol,
      stableTokenBalance,
      TransactionType.SWAP,
      market.inputTokenPriceUSD,
      sPrincipal
    );
  }

  //all open position converted to STABLE
  if (interestRateType === InterestRateType.STABLE) {
    vPositionManager.subtractPosition(
      event,
      protocol,
      variableTokenBalance,
      TransactionType.SWAP,
      market.inputTokenPriceUSD,
      vPrincipal
    );
    sPositionManager.addPosition(
      event,
      market.inputToken,
      protocol,
      stableTokenBalance,
      TransactionType.SWAP,
      market.inputTokenPriceUSD,
      sPrincipal
    );
  } else {
    //all open position converted to VARIABLE
    vPositionManager.addPosition(
      event,
      market.inputToken,
      protocol,
      variableTokenBalance,
      TransactionType.SWAP,
      market.inputTokenPriceUSD,
      vPrincipal
    );
    sPositionManager.subtractPosition(
      event,
      protocol,
      stableTokenBalance,
      TransactionType.SWAP,
      market.inputTokenPriceUSD,
      sPrincipal
    );
  }
}

/////////////////////////
//// Transfer Events ////
/////////////////////////

export function handleCollateralTransfer(event: CollateralTransfer): void {
  const positionSide = PositionSide.COLLATERAL;
  const to = event.params.to;
  const from = event.params.from;
  const amount = event.params.value;
  const asset = event.address;
  const market = getMarketByAuxillaryToken(asset, protocolData);
  if (!market) {
    log.warning("[_handleTransfer] market not found: {}", [
      asset.toHexString(),
    ]);
    return;
  }

  // if the to / from addresses are the same as the asset
  // then this transfer is emitted as part of another event
  // ie, a deposit, withdraw, borrow, repay, etc
  // we want to let that handler take care of position updates
  // and zero addresses mean it is a part of a burn / mint
  if (
    to == Address.fromString(ZERO_ADDRESS) ||
    from == Address.fromString(ZERO_ADDRESS) ||
    to == asset ||
    from == asset
  ) {
    return;
  }

  const tokenContract = ERC20.bind(asset);
  const senderBalanceResult = tokenContract.try_balanceOf(from);
  const receiverBalanceResult = tokenContract.try_balanceOf(to);
  if (senderBalanceResult.reverted) {
    log.warning(
      "[_handleTransfer]token {} balanceOf() call for account {} reverted",
      [asset.toHexString(), from.toHexString()]
    );
    return;
  }
  if (receiverBalanceResult.reverted) {
    log.warning(
      "[_handleTransfer]token {} balanceOf() call for account {} reverted",
      [asset.toHexString(), to.toHexString()]
    );
    return;
  }
  const tokenManager = new TokenManager(asset.toHexString(), event);
  const assetToken = tokenManager.getToken();
  let interestRateType: string | null;
  if (assetToken._izarbanTokenType! == IzarbanTokenType.STOKEN) {
    interestRateType = InterestRateType.STABLE;
  } else if (assetToken._izarbanTokenType! == IzarbanTokenType.VTOKEN) {
    interestRateType = InterestRateType.VARIABLE;
  } else {
    interestRateType = null;
  }

  const senderPrincipal = getPrincipal(
    market,
    from,
    positionSide,
    interestRateType
  );
  const receiverPrincipal = getPrincipal(
    market,
    to,
    positionSide,
    interestRateType
  );

  const inputTokenManager = new TokenManager(market.inputToken, event);
  const amountUSD = inputTokenManager.getAmountUSD(amount);
  const manager = new DataManager(market.id, market.inputToken, event);
  manager.createTransfer(
    asset,
    from,
    to,
    amount,
    amountUSD,
    senderBalanceResult.value,
    receiverBalanceResult.value,
    interestRateType,
    senderPrincipal,
    receiverPrincipal
  );
}

///////////////////
///// Helpers /////
///////////////////

export function getAssetPriceInUSDC(
  tokenAddress: Address,
  priceOracle: Address
): BigDecimal {
  if (!supportedAssets.includes(tokenAddress.toHexString().toLowerCase()))
    return BIGDECIMAL_ZERO;

  const oracle = IPriceOracleGetter.bind(priceOracle);
  let oracleResult = readValue<BigInt>(
    oracle.try_getAssetPrice(tokenAddress),
    BIGINT_ZERO
  );

  // if the result is zero or less, try the fallback oracle
  if (!oracleResult.gt(BIGINT_ZERO)) {
    const tryFallback = oracle.try_getFallbackOracle();
    if (tryFallback) {
      const fallbackOracle = IPriceOracleGetter.bind(tryFallback.value);
      oracleResult = readValue<BigInt>(
        fallbackOracle.try_getAssetPrice(tokenAddress),
        BIGINT_ZERO
      );
    }
  }

  // last resort, should not be touched
  const inputToken = Token.load(tokenAddress.toHexString().toLowerCase());
  if (!inputToken) {
    log.warning(
      "[getAssetPriceInUSDC]token {} not found in Token entity; return BIGDECIMAL_ZERO",
      [tokenAddress.toHexString()]
    );
    return BIGDECIMAL_ZERO;
  }
  return oracleResult.toBigDecimal().div(exponentToBigDecimal(8));
}

function updateRewards(manager: DataManager, event: ethereum.Event): void {
  // Reward rate (rewards/second) in a market comes from try_assets(to)
  // Supply side the to address is the ZToken
  // Borrow side the to address is the variableDebtToken
  const market = manager.getMarket();

  // FIX: Safety check for outputToken. If it doesn't exist, we can't process rewards.
  if (!market.outputToken) {
    log.warning("[updateRewards] Market {} has no outputToken, skipping rewards update", [market.id]);
    return;
  }

  const ZTokenContract = ZToken.bind(Address.fromString(market.outputToken!));
  const tryIncentiveController = ZTokenContract.try_getIncentivesController();
  if (tryIncentiveController.reverted) {
    log.warning(
      "[updateRewards]getIncentivesController() call for ZToken {} is reverted",
      [market.outputToken!]
    );
    return;
  }
  const incentiveControllerContract = ZarbanIncentivesController.bind(
    tryIncentiveController.value
  );

  // FIX: Handle _vToken separately. If it's null, we just skip the borrow rewards part
  // instead of crashing the subgraph.
  let tryBorrowRewards: ethereum.CallResult<ZarbanIncentivesController__assetsResult> | null = null;
  
  if (market._vToken) {
    tryBorrowRewards = incentiveControllerContract.try_assets(
      Address.fromString(market._vToken!)
    );
  }

  const trySupplyRewards = incentiveControllerContract.try_assets(
    Address.fromString(market.outputToken!)
  );
  const tryRewardAsset = incentiveControllerContract.try_REWARD_TOKEN();

  if (tryRewardAsset.reverted) {
    log.warning(
      "[updateRewards]REWARD_TOKEN() call for ZarbanIncentivesController contract {} is reverted",
      [tryIncentiveController.value.toHexString()]
    );
    return;
  }
  // create reward tokens
  const tokenManager = new TokenManager(
    tryRewardAsset.value.toHexString(),
    event
  );
  const rewardToken = tokenManager.getToken();
  const vBorrowRewardToken = tokenManager.getOrCreateRewardToken(
    RewardTokenType.VARIABLE_BORROW
  );
  const sBorrowRewardToken = tokenManager.getOrCreateRewardToken(
    RewardTokenType.STABLE_BORROW
  );
  const depositRewardToken = tokenManager.getOrCreateRewardToken(
    RewardTokenType.DEPOSIT
  );

  const rewardDecimals = rewardToken.decimals;
  const defaultOracle = _DefaultOracle.load(protocolData.protocolID);
  // get reward token price
  // get price of reward token (if stkZarban it is tied to the price of Zarban)
  let rewardTokenPriceUSD = BIGDECIMAL_ZERO;
  if (
    equalsIgnoreCase(dataSource.network(), Network.ARBITRUM_ONE) &&
    defaultOracle &&
    defaultOracle.oracle
  ) {
    // get staked token if possible to grab price of staked token
    const stakedTokenContract = StakedZarban.bind(tryRewardAsset.value);
    const tryStakedToken = stakedTokenContract.try_STAKED_TOKEN();
    if (!tryStakedToken.reverted) {
      rewardTokenPriceUSD = getAssetPriceInUSDC(
        tryStakedToken.value,
        Address.fromBytes(defaultOracle.oracle)
      );
    }
  }

  // if reward token price was not found then use old method
  if (
    rewardTokenPriceUSD.equals(BIGDECIMAL_ZERO) &&
    defaultOracle &&
    defaultOracle.oracle
  ) {
    rewardTokenPriceUSD = getAssetPriceInUSDC(
      tryRewardAsset.value,
      Address.fromBytes(defaultOracle.oracle)
    );
  }

  // we check borrow first since it will show up first in graphql ordering
  // see explanation in docs/Mapping.md#Array Sorting When Querying
  
  // FIX: Check if tryBorrowRewards is not null before checking reverted
  if (tryBorrowRewards && !tryBorrowRewards.reverted) {
    // update borrow rewards
    const borrowRewardsPerDay = tryBorrowRewards.value.value0.times(
      BigInt.fromI32(SECONDS_PER_DAY)
    );
    const borrowRewardsPerDayUSD = borrowRewardsPerDay
      .toBigDecimal()
      .div(exponentToBigDecimal(rewardDecimals))
      .times(rewardTokenPriceUSD);

    const vBorrowRewardData = new RewardData(
      vBorrowRewardToken,
      borrowRewardsPerDay,
      borrowRewardsPerDayUSD
    );
    const sBorrowRewardData = new RewardData(
      sBorrowRewardToken,
      borrowRewardsPerDay,
      borrowRewardsPerDayUSD
    );
    manager.updateRewards(vBorrowRewardData);
    manager.updateRewards(sBorrowRewardData);
  }

  if (!trySupplyRewards.reverted) {
    // update deposit rewards
    const supplyRewardsPerDay = trySupplyRewards.value.value0.times(
      BigInt.fromI32(SECONDS_PER_DAY)
    );
    const supplyRewardsPerDayUSD = supplyRewardsPerDay
      .toBigDecimal()
      .div(exponentToBigDecimal(rewardDecimals))
      .times(rewardTokenPriceUSD);
    const depositRewardData = new RewardData(
      depositRewardToken,
      supplyRewardsPerDay,
      supplyRewardsPerDayUSD
    );
    manager.updateRewards(depositRewardData);
  }
}


// It's the only way to get oracle's supported assets but since the AssetSources are usually set before setting the oracle to lending pool
// it's not working.  so it is commented temporarily.
export function handleZarbanOracleAssetSourceUpdated(
  event: AssetSourceUpdated
): void {
  const asset = event.params.asset.toHexString().toLowerCase();

  const defaultOracle = _DefaultOracle.load(getProtocolData().protocolID);
  if (!defaultOracle) {
    log.warning(
      "[handleZarbanOracleAssetSourceUpdated]DefaultOracle not found",
      []
    );
    return;
  }
  defaultOracle.supportedAssets.push(asset);
}

//////////////////////////////////////
///// Stablecoin system handlers /////
//////////////////////////////////////

// Authorizating Vat (CDP engine)
export function handleVatRely(event: VatRelyEvent): void {
  const someAddress = event.params.usr;
  log.debug("[handleVatRely]Input address = {}", [someAddress.toHexString()]);

  // We don't know whether the address passed in is a valid 'market' (gemjoin) address
  const marketContract = GemJoin.bind(someAddress);
  const ilkCall = marketContract.try_ilk(); // collateral type
  const gemCall = marketContract.try_gem(); // get market collateral token, referred to as 'gem'
  if (ilkCall.reverted || gemCall.reverted) {
    log.debug("[handleVatRely]Address {} is not a market", [
      someAddress.toHexString(),
    ]);
    log.debug(
      "[handleVatRely]ilkCall.revert = {} gemCall.reverted = {} at tx hash {}",
      [
        ilkCall.reverted.toString(),
        gemCall.reverted.toString(),
        event.transaction.hash.toHexString(),
      ]
    );

    return;
  }
  const ilk = ilkCall.value;
  const marketID = someAddress.toHexString();

  const tokenId = gemCall.value.toHexString();
  let tokenName = "unknown";
  let tokenSymbol = "unknown";
  let decimals = 18;
  const erc20Contract = ERC20.bind(gemCall.value);
  const tokenNameCall = erc20Contract.try_name();
  if (tokenNameCall.reverted) {
    log.warning("[handleVatRely]Failed to get name for token {}", [tokenId]);
  } else {
    tokenName = tokenNameCall.value;
  }
  const tokenSymbolCall = erc20Contract.try_symbol();
  if (tokenSymbolCall.reverted) {
    log.warning("[handleVatRely]Failed to get symbol for token {}", [tokenId]);
  } else {
    tokenSymbol = tokenSymbolCall.value;
  }
  const tokenDecimalsCall = erc20Contract.try_decimals();
  if (tokenDecimalsCall.reverted) {
    log.warning("[handleVatRely]Failed to get decimals for token {}", [
      tokenId,
    ]);
  } else {
    decimals = tokenDecimalsCall.value;
  }

  const vatContract = Vat.bind(Address.fromString(VAT_ADDRESS));
  const tryVatIlk = vatContract.try_ilks(ilk);
  if (tryVatIlk.reverted) {
    log.warning("[handleVatRely] Error getting ilk from vat ilk: {}", [
      ilk.toHexString(),
    ]);
    return;
  }

  const SpotContract = Spot.bind(Address.fromString(SPOT_ADDRESS));
  const trySpotIlk = SpotContract.try_ilks(ilk);
  if (trySpotIlk.reverted) {
    log.warning("[handleVatRely] Error getting ilk from spot ilk: {}", [
      ilk.toHexString(),
    ]);
    return;
  }

  const DogContract = Dog.bind(Address.fromString(DOG_ADDRESS));
  const tryDogIlk = DogContract.try_ilks(ilk);
  if (tryDogIlk.reverted) {
    log.warning("[handleVatRely] Error getting ilk from dog ilk: {}", [
      ilk.toHexString(),
    ]);
    return;
  }

  const JugContract = Jug.bind(Address.fromString(JUG_ADDRESS));
  const tryJugIlk = JugContract.try_ilks(ilk);
  if (tryJugIlk.reverted) {
    log.warning("[handleVatRely] Error getting ilk from jug ilk: {}", [
      ilk.toHexString(),
    ]);
    return;
  }

  const pip = trySpotIlk.value.getPip();
  let pipContract = PipContract.bind(pip);

  let tryPipSrc = pipContract.try_src();
  if (tryPipSrc.reverted) {
    log.error("[handleVatRely]Failed to get medianfrom OSM contract", []);
    return;
  }

  log.info(
    "[handleVatRely]ilk={}, market={}, token={}, name={}, symbol={}, decimals={}",
    [
      ilk.toString(),
      marketID, //join (market address)
      tokenId, //gem (token address)
      tokenName,
      tokenSymbol,
      decimals.toString(),
    ]
  );
  Pip.create(pip);

  getOrCreateMarket(
    marketID,
    "Stablecoin System - ".concat(ilk.toString()),
    tokenId,
    event.block.number,
    event.block.timestamp,
    ilk
  );
  getOrCreateIlk(
    ilk,
    marketID,
    tryVatIlk.value.getArt(),
    tryVatIlk.value.getRate(),
    trySpotIlk.value.getMat(),
    tryVatIlk.value.getLine(),
    tryJugIlk.value.getDuty(),
    tryVatIlk.value.getDust(),
    tryDogIlk.value.getHole(),
    tryDogIlk.value.getDirt(),
    pip.toHexString(),
    tryPipSrc.value.toHexString(),
    tryDogIlk.value.getClip().toHexString()
  );
  const tokenManager = new TokenManager(tokenId, event, TokenType.NON_REBASING);
  const token = tokenManager.getToken()
  token._market = marketID;
  token.save();
  // for protocol.mintedTokens
  new TokenManager(ZAR_ADDRESS, event, TokenType.NON_REBASING);
}

export function handleVatCage(event: VatCageEvent): void {
  const protocol = getOrCreateLendingProtocol();
  log.info("[handleVatCage]All markets paused with tx {}", [
    event.transaction.hash.toHexString(),
  ]);
  // Vat.cage pauses all markets
  for (let i: i32 = 0; i < protocol.marketIDList.length; i++) {
    const market = getOrCreateMarket(protocol.marketIDList[i]);
    market.isActive = false;
    market.canBorrowFrom = false;
    market.save();
  }
}

export function handleVatGrab(event: VatGrabEvent): void {
  // only needed for non-liquidations
  if (!event.receipt) {
    log.error("[handleVatGrab]no receipt found. Tx Hash: {}", [
      event.transaction.hash.toHexString(),
    ]);
    return;
  }

  const liquidationSigs = [
    crypto.keccak256(
      ByteArray.fromUTF8(
        "Bite(bytes32,address,uint256,uint256,uint256,address,uint256)"
      )
    ),

    crypto.keccak256(
      ByteArray.fromUTF8(
        "Bark(bytes32,address,uint256,uint256,uint256,address,uint256)"
      )
    ),
  ];

  for (let i = 0; i < event.receipt!.logs.length; i++) {
    const txLog = event.receipt!.logs[i];

    if (liquidationSigs.includes(txLog.topics.at(0))) {
      // it is a liquidation transaction; skip
      log.info("[handleVatGrab]Skip handle grab() for liquidation tx {}-{}", [
        event.transaction.hash.toHexString(),
        event.transactionLogIndex.toString(),
      ]);
      return;
    }
  }

  // Create a new VatFrobEvent
  let frobEvent = new VatFrobEvent(
    event.address,
    event.logIndex,
    event.transactionLogIndex,
    event.logType,
    event.block,
    event.transaction,
    event.parameters,
    event.receipt
  );
  handleVatFrob(frobEvent);
}

// Borrow/Repay/Deposit/Withdraw
export function handleVatFrob(event: VatFrobEvent): void {
  const ilk = event.params.i;
  if (ilk.toString() == "TELEPORT-FW-A") {
    log.info(
      "[handleVatSlip] Skip ilk={} (ZAR Teleport: https://github.com/makerdao/dss-teleport)",
      [ilk.toString()]
    );
    return;
  }
  let u = event.params.u.toHexString();
  let v = event.params.v.toHexString();
  // frob(bytes32 i, address u, address v, address w, int256 dink, int256 dart) call
  // 4th arg w: start = 4 (signature) + 3 * 32, end = start + 32
  let w = event.params.w.toHexString();
  // 5th arg dink: start = 4 (signature) + 4 * 32, end = start + 32
  const dink = event.params.dink; // change to collateral
  // 6th arg dart: start = 4 (signature) + 4 * 32, end = start + 32
  const dart = event.params.dart; // change to debt
  const tx = event.transaction.hash
    .toHexString()
    .concat("-")
    .concat(event.transactionLogIndex.toString());
  log.info(
    "[handleVatFrob]tx {} block {}: ilk={}, u={}, v={}, w={}, dink={}, dart={}",
    [
      tx,
      event.block.number.toString(),
      ilk.toString(),
      u,
      v,
      w,
      dink.toString(),
      dart.toString(),
    ]
  );

  const urn = u;
  const migrationCaller = getMigrationCaller(u, v, w, event);
  if (migrationCaller != null && ilk.toString() == "SAI") {
    // Ignore vat.frob calls not of interest
    // - ignore swapSaiToZar() and swapZarToSai() calls:
    //   https://github.com/makerdao/scd-mcd-migration/blob/96b0e1f54a3b646fa15fd4c895401cf8545fda60/src/ScdMcdMigration.sol#L76-L103
    // - ignore the two migration frob calls that move SAI/ZAR around to balance accounting:
    //   https://github.com/makerdao/scd-mcd-migration/blob/96b0e1f54a3b646fa15fd4c895401cf8545fda60/src/ScdMcdMigration.sol#L118-L125
    //   https://github.com/makerdao/scd-mcd-migration/blob/96b0e1f54a3b646fa15fd4c895401cf8545fda60/src/ScdMcdMigration.sol#L148-L155

    log.info(
      "[handleVatFrob]account migration tx {} for urn={},migrationCaller={} skipped",
      [tx, urn, migrationCaller!]
    );

    return;
  }

  // translate possible UrnHandler/DSProxy address to its owner address
  u = getOwnerAddress(u);
  v = getOwnerAddress(v);
  w = getOwnerAddress(w);

  const market = getMarketFromIlk(ilk);
  if (market == null) {
    log.warning("[handleVatFrob]Failed to get market for ilk {}/{}", [
      ilk.toString(),
      ilk.toHexString(),
    ]);
    return;
  }

  const token = new TokenManager(market.inputToken, event);
  const deltaCollateral = bigIntChangeDecimals(dink, WAD, token.getDecimals());
  const tokenPrice = token.getPriceUSD();
  const deltaCollateralUSD = bigIntToBDUseDecimals(
    deltaCollateral,
    token.getDecimals()
  ).times(tokenPrice);

  log.info(
    "[handleVatFrob]tx {} block {}: token.decimals={}, deltaCollateral={}, deltaCollateralUSD={}",
    [
      tx,
      event.block.number.toString(),
      token.getDecimals().toString(),
      deltaCollateral.toString(),
      deltaCollateralUSD.toString(),
    ]
  );

  market.inputTokenPriceUSD = tokenPrice;

  const zar = new TokenManager(ZAR_ADDRESS, event);
  let deltaDebt = BIGINT_ZERO;
  let deltaDebtBD = BIGDECIMAL_ZERO;

  const _ilk = getOrCreateIlk(ilk);
  if (_ilk) {
    // Calculate actual debt using rate
    _ilk._art = _ilk._art.plus(dart);

    deltaDebt = _ilk._art.times(_ilk._rate).div(BIGINT_ONE_RAY);

    deltaDebtBD = deltaDebt.toBigDecimal().div(BIGDECIMAL_ONE_WAD);

    _ilk.debt = deltaDebtBD;

    _ilk.availableToBorrow = _ilk.debtCeiling.minus(_ilk.debt);
    _ilk.save();
  }

  const deltaDebtUSD = deltaDebtBD.times(zar.getPriceUSD());

  log.info(
    "[handleVatFrob]inputTokenBal={}, inputTokenPrice={}, totalBorrowUSD={}",
    [
      market.inputTokenBalance.toString(),
      market.inputTokenPriceUSD.toString(),
      market.totalBorrowBalanceUSD.toString(),
    ]
  );

  updateOrCreateUrn(u, ilk, event, dink, dart);
  createTransactions(
    event,
    market,
    v,
    w,
    deltaCollateral,
    deltaCollateralUSD,
    deltaDebt,
    deltaDebtUSD
  );
  updateUsageMetrics(
    event,
    market,
    [u, v, w],
    deltaCollateralUSD,
    deltaDebtUSD
  );
  updatePosition(event, urn, ilk, market, deltaCollateral, deltaDebt);
  updateMarket(
    event,
    market,
    deltaCollateral,
    deltaCollateralUSD,
    deltaDebt,
    deltaDebtUSD
  );
  updateProtocol(deltaCollateralUSD, deltaDebtUSD);
  //this needs to after updateProtocol as it uses protocol to do the update
  updateFinancialsSnapshot(event, deltaCollateralUSD, deltaDebtUSD);
}

// function fork( bytes32 ilk, address src, address dst, int256 dink, int256 dart)
// needed for position transfer
export function handleVatFork(event: VatForkEvent): void {
  const ilk = event.params.ilk;
  const src = event.params.src.toHexString();
  const dst = event.params.dst.toHexString();

  // fork( bytes32 ilk, address src, address dst, int256 dink, int256 dart)
  // 4th arg dink: start = 4 (signature) + 3 * 32, end = start + 32
  const dink = event.params.dink; // change to collateral
  // 5th arg dart: start = 4 (signature) + 4 * 32, end = start + 32
  const dart = event.params.dart; // change to debt

  const market: Market = getMarketFromIlk(ilk)!;
  const token = new TokenManager(market.inputToken, event);
  const collateralTransferAmount = bigIntChangeDecimals(
    dink,
    WAD,
    token.getDecimals()
  );
  const debtTransferAmount = dart;

  log.info("[handleVatFork]ilk={}, src={}, dst={}, dink={}, dart={}", [
    ilk.toString(),
    src,
    dst,
    collateralTransferAmount.toString(),
    debtTransferAmount.toString(),
  ]);

  updateOrCreateUrn(
    src,
    ilk,
    event,
    dink.times(BIGINT_NEGATIVE_ONE),
    dart.times(BIGINT_NEGATIVE_ONE)
  );

  updateOrCreateUrn(dst, ilk, event, dink, dart);

  if (dink.gt(BIGINT_ZERO)) {
    transferPosition(
      event,
      ilk,
      src,
      dst,
      PositionSide.COLLATERAL,
      null,
      null,
      collateralTransferAmount
    );
  } else if (dink.lt(BIGINT_ZERO)) {
    transferPosition(
      event,
      ilk,
      dst,
      src,
      PositionSide.COLLATERAL,
      null,
      null,
      collateralTransferAmount.times(BIGINT_NEGATIVE_ONE)
    );
  }

  if (dart.gt(BIGINT_ZERO)) {
    transferPosition(
      event,
      ilk,
      src,
      dst,
      PositionSide.BORROWER,
      null,
      null,
      debtTransferAmount
    );
  } else if (dart.lt(BIGINT_ZERO)) {
    transferPosition(
      event,
      ilk,
      dst,
      src,
      PositionSide.BORROWER,
      null,
      null,
      debtTransferAmount.times(BIGINT_NEGATIVE_ONE)
    );
  }
}

// update total revenue (stability fee)
export function handleVatFold(event: VatFoldEvent): void {
  const ilk = event.params.i;
  if (ilk.toString() == "TELEPORT-FW-A") {
    log.info(
      "[handleVatSlip] Skip ilk={} (ZAR Teleport: https://github.com/makerdao/dss-teleport)",
      [ilk.toString()]
    );
    return;
  }
  const vow = event.params.u.toHexString();
  const rate = event.params.rate;
  const vatContract = Vat.bind(event.address);
  const ilkOnChain = vatContract.ilks(ilk);
  const revenue = ilkOnChain.getArt().times(rate);
  const newTotalRevenue = bigIntToBDUseDecimals(revenue, RAD);
  const tokenManager = new TokenManager(ZAR_ADDRESS, event);
  const newTotalRevenueUSD = newTotalRevenue.times(tokenManager.getPriceUSD());

  if (vow.toLowerCase() != VOW_ADDRESS.toLowerCase()) {
    log.warning(
      "[handleVatFold]Stability fee unexpectedly credited to a non-Vow address {}",
      [vow]
    );
  }

  const _ilk = getOrCreateIlk(ilk);
  if (_ilk) {
    _ilk._rate = _ilk._rate.plus(rate);
    const deltaDebt = _ilk._art
      .times(_ilk._rate)
      .toBigDecimal()
      .div(BIGDECIMAL_ONE_RAD);

    _ilk.debt = deltaDebt;
    _ilk.availableToBorrow = _ilk.debtCeiling.minus(_ilk.debt);
    _ilk.save();

    for (let i = 0; i < _ilk._urns.length; i++) {
      const owner = _ilk._urns[i].split("-")[0];
      updateOrCreateUrn(owner, ilk, event, BIGINT_ZERO, BIGINT_ZERO);
    }
  }

  const marketAddress = getMarketAddressFromIlk(ilk);
  if (marketAddress) {
    const marketID = marketAddress.toHexString();
    log.info("[handleVatFold]total revenue accrued from Market {}/{} = ${}", [
      ilk.toString(),
      marketID,
      newTotalRevenueUSD.toString(),
    ]);
    updateRevenue(
      event,
      marketID,
      newTotalRevenueUSD,
      BIGDECIMAL_ZERO,
      ProtocolSideRevenueType.STABILITYFEE
    );
  } else {
    log.warning(
      "[handleVatFold]Failed to find marketID for ilk {}/{}; revenue of ${} is ignored.",
      [ilk.toString(), ilk.toHexString(), newTotalRevenueUSD.toString()]
    );
  }
}

// update collateral data
export function handleVatFile(event: VatFileEvent): void {
  const what = event.params.what.toString();
  const ilk = event.params.ilk;
  const data = event.params.data;

  const _ilk = getOrCreateIlk(ilk);
  if (_ilk) {
    if (what == "line")
      _ilk.debtCeiling = data.toBigDecimal().div(BIGDECIMAL_ONE_RAD);
    else if (what == "dust")
      _ilk.debtFloor = data.toBigDecimal().div(BIGDECIMAL_ONE_RAD);
    _ilk.save();
  }

  log.info("[handleVatFile]ilk={}, what={}, data={}", [
    ilk.toString(),
    what,
    data.toString(),
  ]);
}

// New liquidation
export function handleDogBark(event: BarkEvent): void {
  const ilk = event.params.ilk; //market
  if (ilk.toString() == "TELEPORT-FW-A") {
    log.info(
      "[handleVatSlip] Skip ilk={} (ZAR Teleport: https://github.com/makerdao/dss-teleport)",
      [ilk.toString()]
    );
    return;
  }
  const urn = event.params.urn; //liquidatee
  const clip = event.params.clip; //auction contract
  const id = event.params.id; //auction id
  const lot = event.params.ink;
  const art = event.params.art;
  const due = event.params.due; //including interest, but not penalty

  const market = getMarketFromIlk(ilk)!;
  const token = new TokenManager(market.inputToken, event);
  const tokenPrice = token.getPriceUSD();
  const collateral = bigIntChangeDecimals(lot, WAD, token.getDecimals());
  const collateralUSD = bigIntToBDUseDecimals(
    collateral,
    token.getDecimals()
  ).times(tokenPrice);
  const deltaCollateral = collateral.times(BIGINT_NEGATIVE_ONE);
  const deltaCollateralUSD = collateralUSD.times(BIGDECIMAL_NEG_ONE);
  const deltaDebt = bigIntToBDUseDecimals(art, WAD).times(BIGDECIMAL_NEG_ONE);

  const tokenManager = new TokenManager(ZAR_ADDRESS, event);
  const deltaDebtUSD = deltaDebt.times(tokenManager.getPriceUSD());

  const _ilk = getOrCreateIlk(ilk);
  if (_ilk) {
    const dirt = _ilk.liquidationPenalty
      .div(BIGDECIMAL_ONE_HUNDRED)
      .plus(BIGDECIMAL_ONE)
      .times(new BigDecimal(due));
    _ilk.dirt = dirt.div(BIGDECIMAL_ONE_RAY);
    _ilk.save();
  }

  // Here we remove all collateral and close positions, even though partial collateral may be returned
  // to the urn, it is no longer "locked", the user would need to call `vat.frob` again to move the collateral
  // from gem to urn (locked); so it is clearer to remove all collateral at initiation of liquidation
  const liquidatedPositionIds = liquidatePosition(
    event,
    urn.toHexString(),
    ilk,
    collateral,
    art
  );
  updateMarket(
    event,
    market,
    deltaCollateral,
    deltaCollateralUSD,
    art,
    deltaDebtUSD
  );
  updateProtocol();
  updateFinancialsSnapshot(event);

  const liquidationRevenue = bigIntToBDUseDecimals(due, RAD).times(
    market.liquidationPenalty.div(BIGDECIMAL_ONE_HUNDRED)
  );
  const liquidationRevenueUSD = liquidationRevenue.times(
    tokenManager.getPriceUSD()
  );
  market.save();

  updateRevenue(
    event,
    market.id,
    liquidationRevenueUSD,
    BIGDECIMAL_ZERO,
    ProtocolSideRevenueType.LIQUIDATION
  );

  const storeID = clip.toHexString().concat("-").concat(id.toString());
  log.info(
    "[handleDogBark]storeID={}, ilk={}, urn={}: lot={}, art={}, due={}, liquidation revenue=${}",
    [
      storeID,
      ilk.toString(),
      urn.toHexString(),
      lot.toString(),
      art.toString(),
      due.toString(),
      liquidationRevenueUSD.toString(),
    ]
  );

  //let debt = bigIntChangeDecimals(due, RAD, WAD);
  const clipTakeStore = new _ClipTakeStore(storeID);
  clipTakeStore.slice = INT_ZERO;
  clipTakeStore.ilk = ilk.toHexString();
  clipTakeStore.market = market.id;
  clipTakeStore.urn = urn.toHexString();
  clipTakeStore.lot = lot;
  clipTakeStore.art = art;
  clipTakeStore.tab = due; //not including penalty
  clipTakeStore.tab0 = due;
  clipTakeStore.positions = liquidatedPositionIds;
  clipTakeStore.save();

  Clip.create(clip);
}

// Update liquidate penalty for the Dog contract
export function handleDogFile(event: DogFileChopAndHoleEvent): void {
  const ilk = event.params.ilk;
  if (ilk.toString() == "TELEPORT-FW-A") {
    log.info(
      "[handleVatSlip] Skip ilk={} (ZAR Teleport: https://github.com/makerdao/dss-teleport)",
      [ilk.toString()]
    );
    return;
  }
  const what = event.params.what.toString();
  const market = getMarketFromIlk(ilk);
  const _ilk = getOrCreateIlk(ilk);
  if (market == null || _ilk == null) {
    log.warning("[handleFileDog]Failed to get Market for ilk {}/{}", [
      ilk.toString(),
      ilk.toHexString(),
    ]);
    return;
  }
  if (what == "hole") {
    _ilk.hole = new BigDecimal(event.params.data.div(BIGINT_ONE_RAD));
    _ilk.save();
  } else if (what == "chop") {
    const chop = event.params.data;
    const liquidationPenalty = bigIntToBDUseDecimals(chop, WAD)
      .minus(BIGDECIMAL_ONE)
      .times(BIGDECIMAL_ONE_HUNDRED);
    if (liquidationPenalty.ge(BIGDECIMAL_ZERO)) {
      market.liquidationPenalty = liquidationPenalty;
      market.save();
      _ilk.liquidationPenalty = liquidationPenalty;
      _ilk.save();
    }

    log.info(
      "[handleDogFile]ilk={}, chop={}, liquidationPenalty={}, market.liquidationPenalty={}",
      [
        ilk.toString(),
        chop.toString(),
        liquidationPenalty.toString(),
        market.liquidationPenalty.toString(),
      ]
    );
  }
}

// update collateral data
export function handleDogClipFile(event: DogFileClipEvent): void {
  const what = event.params.what.toString();
  const ilk = event.params.ilk;
  const clip = event.params.clip;

  if (what != "clip") {
    const _ilk = getOrCreateIlk(ilk);
    if (_ilk) {
      _ilk.clipper = clip.toHexString();
      _ilk.save();
    }

    log.info("[handleDogClipFile]ilk={}, what={}, clip={}", [
      ilk.toString(),
      what,
      clip.toHexString(),
    ]);
  }
}

// update dirt
export function handleDogDigs(event: DogDigsEvent): void {
  const ilk = event.params.ilk;
  const rad = event.params.rad;

  const _ilk = getOrCreateIlk(ilk);
  if (_ilk) {
    _ilk.dirt = _ilk.dirt.minus(new BigDecimal(rad));
    _ilk.save();
  }

  log.info("[handleDogDigs]ilk={}", [ilk.toString()]);
}

// Auction used by Dog (new liquidation contract)
export function handleClipTakeBid(event: TakeEvent): void {
  const id = event.params.id;
  let liquidatee = event.params.usr.toHexString();
  const max = event.params.max;
  const lot = event.params.lot;
  const price = event.params.price;
  const tab = event.params.tab;
  const owe = event.params.owe;

  const liquidator = event.transaction.from.toHexString();
  // translate possible proxy/urn handler address to owner address
  liquidatee = getOwnerAddress(liquidatee);

  const storeID = event.address //clip contract
    .toHexString()
    .concat("-")
    .concat(id.toString());
  const clipTakeStore = _ClipTakeStore.load(storeID)!;
  clipTakeStore.slice += INT_ONE;
  const marketID = clipTakeStore.market;
  const market = getOrCreateMarket(marketID);
  const token = new TokenManager(market.inputToken, event);
  const tokenPrice = token.getPriceUSD();

  const value = bigIntToBDUseDecimals(lot, token.getDecimals()).times(
    tokenPrice
  );
  log.info(
    "[handleClipTakeBid]block#={}, storeID={}, clip.id={}, slice #{} event params: max={}, lot={}, price={}, value(lot*price)={}, art={}, tab={}, owe={}, liquidatee={}, liquidator={}",
    [
      event.block.number.toString(),
      storeID, //storeID
      id.toString(),
      clipTakeStore.slice.toString(),
      max.toString(),
      lot.toString(),
      price.toString(),
      value.toString(),
      clipTakeStore.art.toString(),
      tab.toString(),
      owe.toString(),
      liquidatee,
      liquidator,
    ]
  );

  const deltaLot = clipTakeStore.lot.minus(lot);
  const amount = bigIntChangeDecimals(deltaLot, WAD, token.getDecimals());
  const amountUSD = bigIntToBDUseDecimals(amount, token.getDecimals()).times(
    tokenPrice
  );

  const tokenManager = new TokenManager(ZAR_ADDRESS, event);

  const amountOwed = bigIntToBDUseDecimals(owe, RAD);
  const amountOwedUSD = amountOwed.times(tokenManager.getPriceUSD());
  const profitUSD = amountUSD.minus(amountOwedUSD);

  const liquidateID = createEventID(event, Transaction.LIQUIDATE);
  const liquidate = getOrCreateLiquidate(
    liquidateID,
    event,
    market,
    liquidatee,
    liquidator,
    amount,
    amountUSD,
    profitUSD
  );

  liquidate.positions = clipTakeStore.positions!;
  liquidate.save();

  if (
    liquidate.amount.le(BIGINT_ZERO) ||
    liquidate.amountUSD.le(BIGDECIMAL_ZERO) ||
    liquidate.profitUSD.le(BIGDECIMAL_ZERO)
  ) {
    log.warning(
      "[handleClipTakeBid]liquidateID={}, storeID={}, clip.id={} slice #{} problematic values: amount={}, amountUSD={}, profitUSD={}",
      [
        liquidateID,
        storeID,
        id.toString(),
        clipTakeStore.slice.toString(),
        liquidate.amount.toString(),
        liquidate.amountUSD.toString(),
        liquidate.profitUSD.toString(),
      ]
    );
  }

  clipTakeStore.lot = lot;
  clipTakeStore.tab = tab;
  clipTakeStore.save();

  log.info(
    "[handleClipTakeBid]liquidateID={}, storeID={}, clip.id={}, slice #{} final: amount={}, amountUSD={}, profitUSD={}",
    [
      liquidate.id,
      clipTakeStore.id, //storeID
      id.toString(),
      clipTakeStore.slice.toString(),
      liquidate.amount.toString(),
      liquidate.amountUSD.toString(),
      liquidate.profitUSD.toString(),
    ]
  );

  log.info(
    "[handleClipTakeBid]storeID={}, clip.id={} clipTakeStatus: lot={}, tab={}, price={}",
    [
      storeID, //storeID
      id.toString(),
      clipTakeStore.lot.toString(),
      clipTakeStore.tab.toString(),
      tokenPrice.toString(),
    ]
  );

  updateUsageMetrics(
    event,
    market,
    [],
    BIGDECIMAL_ZERO,
    BIGDECIMAL_ZERO,
    liquidate.amountUSD,
    liquidator,
    liquidatee
  );
  updateMarket(
    event,
    market,
    BIGINT_ZERO,
    BIGDECIMAL_ZERO,
    BIGINT_ZERO,
    BIGDECIMAL_ZERO,
    liquidate.amount,
    liquidate.amountUSD
  );
  updateProtocol(BIGDECIMAL_ZERO, BIGDECIMAL_ZERO, liquidate.amountUSD);
  updateFinancialsSnapshot(
    event,
    BIGDECIMAL_ZERO,
    BIGDECIMAL_ZERO,
    liquidate.amountUSD
  );
}

// cancel auction
export function handleClipYankBid(event: ClipYankEvent): void {
  const id = event.params.id;
  const storeID = event.address //clip contract
    .toHexString()
    .concat("-")
    .concat(id.toString());
  const clipTakeStore = _ClipTakeStore.load(storeID)!;

  const clipContract = ClipContract.bind(event.address);
  const ilk = clipContract.ilk();
  const sales = clipContract.sales(id);
  const lot = sales.getLot();
  const tab = sales.getTab();
  let liquidatee = sales.getUsr().toHexString();

  const liquidator = event.transaction.from.toHexString();
  // translate possible proxy/urn handler address to owner address
  liquidatee = getOwnerAddress(liquidatee);
  const market = getMarketFromIlk(ilk)!;
  const token = new TokenManager(market.inputToken, event);
  const tokenPrice = token.getPriceUSD();

  const liquidateID = createEventID(event, Transaction.LIQUIDATE);
  // convert collateral to its native amount from WAD
  const amount = bigIntChangeDecimals(lot, WAD, token.getDecimals());
  const amountUSD = bigIntToBDUseDecimals(amount, token.getDecimals()).times(
    tokenPrice
  );

  const tokenManager = new TokenManager(ZAR_ADDRESS, event);

  const outstandingDebt = bigIntToBDUseDecimals(tab, RAD);
  const outstandingDebtUSD = outstandingDebt.times(tokenManager.getPriceUSD());
  const profitUSD = amountUSD.minus(outstandingDebtUSD);

  const liquidate = getOrCreateLiquidate(
    liquidateID,
    event,
    market,
    liquidatee,
    liquidator,
    amount,
    amountUSD,
    profitUSD
  );

  liquidate.positions = clipTakeStore.positions!;
  liquidate.save();

  log.info(
    "[handleClipYankBid]auction for liquidation {} (id {}) cancelled, assuming the msg sender {} won at ${} (profit ${})",
    [
      liquidateID,
      id.toString(),
      liquidator,
      liquidate.amountUSD.toString(),
      liquidate.profitUSD.toString(),
    ]
  );

  if (
    liquidate.amount.le(BIGINT_ZERO) ||
    liquidate.amountUSD.le(BIGDECIMAL_ZERO) ||
    liquidate.profitUSD.le(BIGDECIMAL_ZERO)
  ) {
    log.warning(
      "[handleClipTakeBid]problematic values: amount={}, amountUSD={}, profitUSD={}",
      [
        liquidate.amount.toString(),
        liquidate.amountUSD.toString(),
        liquidate.profitUSD.toString(),
      ]
    );
  }

  updateUsageMetrics(
    event,
    market,
    [],
    BIGDECIMAL_ZERO,
    BIGDECIMAL_ZERO,
    liquidate.amountUSD,
    liquidator,
    liquidatee
  );
  updateMarket(
    event,
    market,
    BIGINT_ZERO,
    BIGDECIMAL_ZERO,
    BIGINT_ZERO,
    BIGDECIMAL_ZERO,
    liquidate.amount,
    liquidate.amountUSD
  );
  updateProtocol(BIGDECIMAL_ZERO, BIGDECIMAL_ZERO, liquidate.amountUSD);
  updateFinancialsSnapshot(
    event,
    BIGDECIMAL_ZERO,
    BIGDECIMAL_ZERO,
    liquidate.amountUSD
  );
}

// Setting mat & par in the Spot contract
export function handleSpotFileMat(event: SpotFileMatEvent): void {
  const what = event.params.what.toString();
  if (what == "mat") {
    const ilk = event.params.ilk;
    if (ilk.toString() == "TELEPORT-FW-A") {
      log.info(
        "[handleVatSlip] Skip ilk={} (ZAR Teleport: https://github.com/makerdao/dss-teleport)",
        [ilk.toString()]
      );
      return;
    }
    const market = getMarketFromIlk(ilk);
    if (market == null) {
      log.warning("[handleSpotFileMat]Failed to get Market for ilk {}/{}", [
        ilk.toString(),
        ilk.toHexString(),
      ]);
      return;
    }

    // 3rd arg: start = 4 + 2 * 32, end = start + 32
    const mat = event.params.data;

    log.info("[handleSpotFileMat]ilk={}, market={}, mat={}", [
      ilk.toString(),
      market.id,
      mat.toString(),
    ]);

    const protocol = getOrCreateLendingProtocol();
    const par = protocol._par!;
    market._mat = mat;
    if (mat != BIGINT_ZERO) {
      // mat for the SAI market is 0 and can not be used as deonimnator
      market.maximumLTV = BIGDECIMAL_ONE_HUNDRED.div(
        bigIntToBDUseDecimals(mat, RAY)
      ).div(bigIntToBDUseDecimals(par, RAY));
      market.liquidationThreshold = market.maximumLTV;
      const _ilk = getOrCreateIlk(ilk);
      if (_ilk) {
        _ilk.minimumCollateralizationRatio = mat
          .toBigDecimal()
          .div(BIGDECIMAL_ONE_RAY);

        _ilk.maximumLTV = market.maximumLTV;
        _ilk.save();
      }
    }
    market.save();
  }
}

export function handleSpotFilePar(event: SpotFileParEvent): void {
  const what = event.params.what.toString();
  if (what == "par") {
    const par = event.params.data;
    log.info("[handleSpotFilePar]par={}", [par.toString()]);
    const protocol = getOrCreateLendingProtocol();
    protocol._par = par;
    protocol.save();

    for (let i: i32 = 0; i <= protocol.marketIDList.length; i++) {
      const market = getOrCreateMarket(protocol.marketIDList[i]);
      const mat = market._mat;
      if (mat != BIGINT_ZERO) {
        // mat is 0 for the SAI market
        market.maximumLTV = BIGDECIMAL_ONE_HUNDRED.div(
          bigIntToBDUseDecimals(mat, RAY)
        ).div(bigIntToBDUseDecimals(par, RAY));
        market.liquidationThreshold = market.maximumLTV;
        market.save();
        if (market.ilk != null) {
          const _ilk = getOrCreateIlk(Bytes.fromHexString(market.ilk!));
          if (_ilk) {
            _ilk.maximumLTV = market.maximumLTV;
            _ilk.save();
          }
        }
      }
    }
  }
}

// update pip
export function handleSpotFilePip(event: SpotFilePipEvent): void {
  const what = event.params.what.toString();
  const ilk = event.params.ilk;
  const pip = event.params.pip_;

  const _ilk = getOrCreateIlk(ilk);
  if (_ilk) {
    _ilk.pip = pip.toHexString();
    _ilk.save();
    Pip.create(pip);
  }

  log.info("[handleSpotFilePip]ilk={}, what={}, pip={}", [
    ilk.toString(),
    what,
    pip.toString(),
  ]);
}

// update token price for ilk market
export function handleSpotPoke(event: PokeEvent): void {
  const ilk = event.params.ilk;
  if (ilk.toString() == "TELEPORT-FW-A") {
    log.info(
      "[handleVatSlip] Skip ilk={} (ZAR Teleport: https://github.com/makerdao/dss-teleport)",
      [ilk.toString()]
    );
    return;
  }
  const market = getMarketFromIlk(ilk);
  if (market == null) {
    log.warning("[handleSpotPoke]Failed to get Market for ilk {}/{}", [
      ilk.toString(),
      ilk.toHexString(),
    ]);
    return;
  }

  const tokenPriceToman = bigIntToBDUseDecimals(
    bytesToUnsignedBigInt(event.params.val),
    WAD
  ).times(BigDecimal.fromString("1000"));

  let tokenPriceUSD = BIGDECIMAL_ZERO;

  let defaultOracle = _DefaultOracle.load(getProtocolData().protocolID);
  if (defaultOracle) {
    const oracle = defaultOracle.oracle;
    if (oracle) {
      tokenPriceUSD =
        getAssetPriceInUSDC(
          Address.fromString(market.inputToken),
          Address.fromBytes(oracle)
        ) || BIGDECIMAL_ZERO;
    }
  }
  if (!tokenPriceUSD) {
    const zar = new TokenManager(ZAR_ADDRESS, event);
    tokenPriceUSD = tokenPriceToman.times(zar.getPriceUSD());
  }

  const tokenID = market.inputToken;
  const token = new TokenManager(tokenID, event);
  token.updatePrice(tokenPriceUSD, tokenPriceToman);
  market.inputTokenPriceUSD = tokenPriceUSD;
  market.save();

  updatePriceForMarket(market.id, event);
  log.info("[handleSpotPoke]Price of token {} in market {} is updated to {}", [
    tokenID,
    market.id,
    tokenPriceUSD.toString(),
  ]);
}

export function handlePipLogValue(event: PipLogValueEvent): void {
  const pip = event.address;

  let pipContract = PipContract.bind(pip);

  let tryPipSrc = pipContract.try_src();
  if (tryPipSrc.reverted) {
    log.error(
      "[handlePipLogValue]Failed to get median from OSM contract. pip:{}",
      [pip.toHexString()]
    );
    return;
  }

  const median = tryPipSrc.value.toHexString();

  // we don't have permission to call peep to fetch the nextPrice so it is commented temporarily

  // let tryPipPeep = pipContract.try_peep();
  // if (tryPipPeep.reverted) {
  //   log.error(
  //     "[handlePipLogValue]Failed to get next price from OSM contract. pip:{}",
  //     [pip.toHexString()]
  //   );
  //   return;
  // }

  // const nextPrice = BigInt.fromUnsignedBytes(
  //   tryPipPeep.value.value0
  // ).toBigDecimal();

  const protocol = getOrCreateLendingProtocol();
  const ilkIDlist = protocol.ilkIDList;

  for (let i = 0; i < ilkIDlist.length; i++) {
    const ilk = ilkIDlist[i];
    const _ilk = getOrCreateIlk(Bytes.fromHexString(ilk));
    if (_ilk) {
      if (pip.toHexString() == _ilk.pip) {
        _ilk.median = median;
        // _ilk.nextPrice = nextPrice;
      }
      _ilk.save();
    }
    log.info("[handlePipLogValue]ilk={}, median={}", [ilk.toString(), median]);
  }
}

export function handleJugFileDuty(event: JugFileEvent): void {
  const ilk = event.params.ilk;
  if (ilk.toString() == "TELEPORT-FW-A") {
    log.info(
      "[handleJugFileDuty] Skip ilk={} (ZAR Teleport: https://github.com/makerdao/dss-teleport)",
      [ilk.toString()]
    );
    return;
  }
  const what = event.params.what.toString();
  if (what == "duty") {
    const market = getMarketFromIlk(ilk);
    if (market == null) {
      log.error("[handleJugFileDuty]Failed to get market for ilk {}/{}", [
        ilk.toString(),
        ilk.toHexString(),
      ]);
      return;
    }

    const jugContract = Jug.bind(event.address);
    const base = jugContract.base();
    const duty = jugContract.ilks(ilk).value0;
    const rate = bigIntToBDUseDecimals(base.plus(duty), RAY).minus(
      BIGDECIMAL_ONE
    );
    let rateAnnualized = BIGDECIMAL_ZERO;
    if (rate.gt(BIGDECIMAL_ZERO)) {
      rateAnnualized = bigDecimalExponential(
        rate,
        SECONDS_PER_YEAR_BIGDECIMAL
      ).times(BIGDECIMAL_ONE_HUNDRED);
    }
    log.info(
      "[handleJugFileDuty] ilk={}, duty={}, rate={}, rateAnnualized={}",
      [
        ilk.toString(),
        duty.toString(),
        rate.toString(),
        rateAnnualized.toString(),
      ]
    );

    const _ilk = getOrCreateIlk(ilk);
    if (_ilk) {
      let dutyBigDecimal = duty.toBigDecimal().div(BIGDECIMAL_ONE_RAY);
      let dutyFloat = parseFloat(dutyBigDecimal.toString());
      let annualStabilityFeeFloat = Math.pow(dutyFloat, SECONDS_PER_YEAR) - 1;
      let annualStabilityFee = BigDecimal.fromString(
        annualStabilityFeeFloat.toString()
      );
      _ilk.annualStabilityFee = annualStabilityFee;
      _ilk.save();
    }

    const interestRateID =
      InterestRateSide.BORROWER +
      "-" +
      InterestRateType.STABLE +
      "-" +
      market.id;
    const interestRate = getOrCreateInterestRate(
      market.id,
      InterestRateSide.BORROWER,
      InterestRateType.STABLE
    );
    interestRate.rate = rateAnnualized;
    interestRate.save();

    market.rates = [interestRateID];
    market.save();
    snapshotMarket(event, market);
  }
}

// Store cdpi, UrnHandler, and owner address
export function handleNewCdp(event: NewCdp): void {
  const cdpi = event.params.cdp;
  const owner = event.params.own.toHexString().toLowerCase();
  // if owner is a DSProxy, get the EOA owner of the DSProxy
  const ownerEOA = getOwnerAddress(owner);
  const contract = CdpManager.bind(event.address);
  const urnhandlerAddress = contract.urns(cdpi).toHexString();
  const ilk = contract.ilks(cdpi);
  const _cdpi = new _Cdpi(cdpi.toString());
  _cdpi.urn = urnhandlerAddress.toString();
  _cdpi.ilk = ilk.toHexString();
  _cdpi.ownerAddress = ownerEOA;
  _cdpi.save();

  log.info("[handleNewCdp]cdpi={}, ilk={}, urn={}, owner={}, EOA={}", [
    cdpi.toString(),
    ilk.toString(),
    urnhandlerAddress,
    owner,
    ownerEOA,
  ]);
}

// detect if a frob is a migration transaction,
// if it is, return the address of the caller (owner)
// if it is not, return null
// Ref: https://github.com/makerdao/scd-mcd-migration/blob/96b0e1f54a3b646fa15fd4c895401cf8545fda60/src/ScdMcdMigration.sol#L107
export function getMigrationCaller(
  u: string,
  v: string,
  w: string,
  event: ethereum.Event
): string | null {
  if (!(u == v && u == w && w == v)) return null;
  const owner = event.transaction.from.toHexString();
  if (u.toLowerCase() == MIGRATION_ADDRESS) {
    return owner;
  }
  return null;
}
