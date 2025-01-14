// Helpers for the general mapping.ts file
import {
  Address,
  BigDecimal,
  BigInt,
  ByteArray,
  crypto,
  ethereum,
  log,
  Bytes,
} from "@graphprotocol/graph-ts";
import { ProtocolData } from "./manager";
import {
  Market,
  Token,
  _FlashLoanPremium,
  Protocol as LendingProtocol,
  Account,
  _ActiveAccount,
  Deposit,
  Withdraw,
  Borrow,
  Repay,
  Position,
  PositionSnapshot,
  _Urn,
  _Cdpi,
} from "../../generated/schema";
import {
  BIGINT_ZERO,
  BIGINT_ONE,
  BIGDECIMAL_ZERO,
  IzarbanTokenType,
  INT_TWO,
  BIGINT_THREE,
  INT_FIVE,
  INT_NINE,
  INT_TEN,
  INT_THREE,
  InterestRateType,
  PositionSide,
  SECONDS_PER_HOUR,
  SECONDS_PER_DAY,
  BIGINT_NEGATIVE_ONE,
  BIGDECIMAL_NEG_ONE,
  ZAR_ADDRESS,
  ProtocolSideRevenueType,
  INT_ONE,
  BIGINT_NEG_HUNDRED,
  Transaction,
  BIGDECIMAL_ONE_WAD,
  BIGDECIMAL_ONE_RAD,
  VAT_ADDRESS,
} from "./constants";
import { ZToken } from "../../generated/LendingPool/ZToken";
import { StableDebtToken } from "../../generated/LendingPool/StableDebtToken";
import { VariableDebtToken } from "../../generated/LendingPool/VariableDebtToken";
import {
  getOrCreateMarketHourlySnapshot,
  getOrCreateMarketDailySnapshot,
  getOrCreateFinancials,
  getOrCreateLendingProtocol,
  getOrCreateUsageMetricsHourlySnapshot,
  getOrCreateUsageMetricsDailySnapshot,
  getSnapshotRates,
  getOrCreateMarket,
  getOrCreatePosition,
  getMarketFromIlk,
  getOpenPosition,
  getOwnerAddress,
  getOrCreateIlk,
} from "./getters";
import { createEventID } from "../utils/strings";
import { bigIntToBDUseDecimals } from "../utils/numbers";
import { ZAR } from "../../generated/Vat/ZAR";
import { AccountManager } from "./account";
import { TokenManager } from "./token";
import { Vat } from "../../generated/Vat/Vat";

// returns the market based on any auxillary token
// ie, outputToken, vToken, or sToken
export function getMarketByAuxillaryToken(
  auxillaryToken: Address,
  protocolData: ProtocolData
): Market | null {
  const protocol = LendingProtocol.load(protocolData.protocolID);
  let markets: Market[];
  if (protocol === null) {
    log.warning("[_handleUnpaused] LendingProtocol does not exist", []);
    return null;
  } else {
    markets = protocol.markets.load();
  }

  if (!markets) {
    log.warning("[getMarketByAuxillaryToken]marketList not found for id {}", [
      protocolData.protocolID,
    ]);
    return null;
  }
  for (let i = 0; i < markets.length; i++) {
    const market = Market.load(markets[i].id);

    if (!market) {
      continue;
    }

    if (
      market.outputToken &&
      market.outputToken! == auxillaryToken.toString()
    ) {
      // we found a matching market!
      return market;
    }
    if (market._vToken && market._vToken! == auxillaryToken.toString()) {
      return market;
    }
    if (market._sToken && market._sToken! == auxillaryToken.toString()) {
      return market;
    }
  }

  return null; // no market found
}

// this is more efficient than getMarketByAuxillaryToken()
// but requires a token._market field
export function getMarketFromToken(
  tokenAddress: Address,
  protocolData: ProtocolData
): Market | null {
  const token = Token.load(tokenAddress.toHexString().toLowerCase());
  if (!token) {
    log.error("[getMarketFromToken] token {} not exist", [
      tokenAddress.toHexString(),
    ]);
    return null;
  }
  if (!token._market) {
    log.warning("[getMarketFromToken] token {} _market = null", [
      tokenAddress.toHexString(),
    ]);
    return getMarketByAuxillaryToken(tokenAddress, protocolData);
  }

  const marketId = token._market!;
  const market = Market.load(marketId);
  return market;
}
export function getBorrowBalances(market: Market, account: Address): BigInt[] {
  let sDebtTokenBalance = BIGINT_ZERO;
  let vDebtTokenBalance = BIGINT_ZERO;

  // get account's balance of variable debt
  if (market._vToken) {
    const vTokenContract = ZToken.bind(Address.fromString(market._vToken!));
    const tryVDebtTokenBalance = vTokenContract.try_balanceOf(account);
    vDebtTokenBalance = tryVDebtTokenBalance.reverted
      ? BIGINT_ZERO
      : tryVDebtTokenBalance.value;
  }

  // get account's balance of stable debt
  if (market._sToken) {
    const sTokenContract = ZToken.bind(Address.fromString(market._sToken!));
    const trySDebtTokenBalance = sTokenContract.try_balanceOf(account);
    sDebtTokenBalance = trySDebtTokenBalance.reverted
      ? BIGINT_ZERO
      : trySDebtTokenBalance.value;
  }

  return [sDebtTokenBalance, vDebtTokenBalance];
}

export function getCollateralBalance(market: Market, account: Address): BigInt {
  const collateralBalance = BIGINT_ZERO;
  const ZTokenContract = ZToken.bind(Address.fromString(market.outputToken!));
  const balanceResult = ZTokenContract.try_balanceOf(account);
  if (balanceResult.reverted) {
    log.warning(
      "[getCollateralBalance]failed to get ZToken {} balance for {}",
      [market.outputToken!.toString(), account.toHexString()]
    );
    return collateralBalance;
  }

  return balanceResult.value;
}

export function getPrincipal(
  market: Market,
  account: Address,
  side: string,
  interestRateType: InterestRateType | null = null
): BigInt | null {
  if (side == PositionSide.COLLATERAL) {
    const ZTokenContract = ZToken.bind(Address.fromString(market.outputToken!));
    const scaledBalanceResult = ZTokenContract.try_scaledBalanceOf(account);
    if (scaledBalanceResult.reverted) {
      log.warning(
        "[getPrincipal]failed to get ZToken {} scaledBalance for {}",
        [market.outputToken!.toString(), account.toHexString()]
      );
      return null;
    }
    return scaledBalanceResult.value;
  } else if (side == PositionSide.BORROWER && interestRateType) {
    if (interestRateType == InterestRateType.STABLE) {
      const stableDebtTokenContract = StableDebtToken.bind(
        Address.fromString(market._sToken!)
      );
      const principalBalanceResult =
        stableDebtTokenContract.try_principalBalanceOf(account);
      if (principalBalanceResult.reverted) {
        log.warning(
          "[getPrincipal]failed to get stableDebtToken {} principalBalance for {}",
          [market._sToken!.toString(), account.toHexString()]
        );
        return null;
      }
      return principalBalanceResult.value;
    } else if (interestRateType == InterestRateType.VARIABLE) {
      const variableDebtTokenContract = VariableDebtToken.bind(
        Address.fromString(market._vToken!)
      );
      const scaledBalanceResult =
        variableDebtTokenContract.try_scaledBalanceOf(account);
      if (scaledBalanceResult.reverted) {
        log.warning(
          "[getPrincipal]failed to get variableDebtToken {} scaledBalance for {}",
          [market._vToken!.toString(), account.toHexString()]
        );
        return null;
      }
      return scaledBalanceResult.value;
    }
  }

  return null;
}

export function getOrCreateFlashloanPremium(
  procotolData: ProtocolData
): _FlashLoanPremium {
  let flashloanPremium = _FlashLoanPremium.load(procotolData.protocolID);
  if (!flashloanPremium) {
    flashloanPremium = new _FlashLoanPremium(procotolData.protocolID);
    flashloanPremium.premiumRateTotal = BIGDECIMAL_ZERO;
    flashloanPremium.premiumRateToProtocol = BIGDECIMAL_ZERO;
    flashloanPremium.save();
  }
  return flashloanPremium;
}

export function readValue<T>(
  callResult: ethereum.CallResult<T>,
  defaultValue: T
): T {
  return callResult.reverted ? defaultValue : callResult.value;
}

export function rayToWad(a: BigInt): BigInt {
  const halfRatio = BigInt.fromI32(INT_TEN)
    .pow(INT_NINE as u8)
    .div(BigInt.fromI32(INT_TWO));
  return halfRatio.plus(a).div(BigInt.fromI32(INT_TEN).pow(INT_NINE as u8));
}

// n => 10^n
export function exponentToBigDecimal(decimals: i32): BigDecimal {
  let result = BIGINT_ONE;
  const ten = BigInt.fromI32(INT_TEN);
  for (let i = 0; i < decimals; i++) {
    result = result.times(ten);
  }
  return result.toBigDecimal();
}

export function equalsIgnoreCase(a: string, b: string): boolean {
  const DASH = "-";
  const UNDERSCORE = "_";
  return (
    a.replace(DASH, UNDERSCORE).toLowerCase() ==
    b.replace(DASH, UNDERSCORE).toLowerCase()
  );
}

// Use the Transfer event before Repay event to detect interestRateType
// We cannot use Burn event because a Repay event may actually mint
// new debt token if the repay amount is less than interest accrued

export function getInterestRateType(
  event: ethereum.Event
): InterestRateType | null {
  const TRANSFER = "Transfer(address,address,uint256)";
  const eventSignature = crypto.keccak256(ByteArray.fromUTF8(TRANSFER));
  const logs = event.receipt!.logs;
  // Transfer emitted at 4 or 5 index ahead of Repay's event.logIndex
  const logIndexMinus5 = event.logIndex.minus(BigInt.fromI32(INT_FIVE));
  const logIndexMinus3 = event.logIndex.minus(BigInt.fromI32(INT_THREE));

  for (let i = 0; i < logs.length; i++) {
    const thisLog = logs[i];
    if (thisLog.topics.length >= INT_THREE) {
      if (thisLog.logIndex.lt(logIndexMinus5)) {
        // skip event with logIndex < LogIndexMinus5
        continue;
      }
      if (thisLog.logIndex.equals(logIndexMinus3)) {
        // break if the logIndex = event.logIndex - 3
        break;
      }

      // topics[0] - signature
      const ADDRESS = "address";
      const logSignature = thisLog.topics[0];

      if (logSignature.equals(eventSignature)) {
        const from = ethereum
          .decode(ADDRESS, thisLog.topics.at(1))!
          .toAddress();
        const to = ethereum.decode(ADDRESS, thisLog.topics.at(2))!.toAddress();

        if (from.equals(Address.zero()) || to.equals(Address.zero())) {
          // this is a burn or mint event
          const tokenAddress = thisLog.address;
          const token = Token.load(tokenAddress.toHexString().toLowerCase());
          if (!token) {
            log.error("[getInterestRateType]token {} not found tx {}-{}", [
              tokenAddress.toHexString(),
              event.transaction.hash.toHexString(),
              event.transactionLogIndex.toString(),
            ]);
            return null;
          }

          if (token._izarbanTokenType == IzarbanTokenType.STOKEN) {
            return InterestRateType.STABLE;
          }
          if (token._izarbanTokenType == IzarbanTokenType.VTOKEN) {
            return InterestRateType.VARIABLE;
          }
        }
      }

      log.info(
        "[getInterestRateType]event at logIndex {} signature {} not match the expected Transfer signature {}. tx {}-{} ",
        [
          thisLog.logIndex.toString(),
          logSignature.toHexString(),
          eventSignature.toHexString(),
          event.transaction.hash.toHexString(),
          event.transactionLogIndex.toString(),
        ]
      );
    }
  }
  return null;
}

export function getFlashloanPremiumAmount(
  event: ethereum.Event,
  assetAddress: Address
): BigInt {
  let flashloanPremiumAmount = BIGINT_ZERO;
  const FLASHLOAN =
    "FlashLoan(address,address,address,uint256,uint8,uint256,uint16)";
  const eventSignature = crypto.keccak256(ByteArray.fromUTF8(FLASHLOAN));
  const logs = event.receipt!.logs;
  //ReserveDataUpdated emitted before Flashloan's event.logIndex
  // e.g. https://etherscan.io/tx/0xeb87ebc0a18aca7d2a9ffcabf61aa69c9e8d3c6efade9e2303f8857717fb9eb7#eventlog
  const ReserveDateUpdatedEventLogIndex = event.logIndex;
  for (let i = 0; i < logs.length; i++) {
    const thisLog = logs[i];
    if (thisLog.topics.length >= INT_THREE) {
      if (thisLog.logIndex.le(ReserveDateUpdatedEventLogIndex)) {
        // skip log before ReserveDataUpdated
        continue;
      }
      //FlashLoan Event equals ReserveDateUpdatedEventLogIndex + 2 or 3 (there may be an Approval event)
      if (
        thisLog.logIndex.gt(ReserveDateUpdatedEventLogIndex.plus(BIGINT_THREE))
      ) {
        // skip if no matched FlashLoan event at ReserveDateUpdatedEventLogIndex+3
        break;
      }

      // topics[0] - signature
      const ADDRESS = "address";
      const DATA_TYPE_TUPLE = "(address,uint256,uint8,uint256)";
      const logSignature = thisLog.topics[0];
      if (thisLog.address == event.address && logSignature == eventSignature) {
        log.info(
          "[getFlashloanPremiumAmount]tx={}-{} thisLog.logIndex={} thisLog.topics=(1:{},2:{}),thisLog.data={}",
          [
            event.transaction.hash.toHexString(),
            event.logIndex.toString(),
            thisLog.logIndex.toString(),
            thisLog.topics.at(1).toHexString(),
            thisLog.topics.at(2).toHexString(),
            thisLog.data.toHexString(),
          ]
        );
        const flashLoanAssetAddress = ethereum
          .decode(ADDRESS, thisLog.topics.at(2))!
          .toAddress();
        if (flashLoanAssetAddress.notEqual(assetAddress)) {
          //
          continue;
        }
        const decoded = ethereum.decode(DATA_TYPE_TUPLE, thisLog.data);
        if (!decoded) continue;

        const logData = decoded.toTuple();
        flashloanPremiumAmount = logData[3].toBigInt();
        break;
      }
    }
  }
  return flashloanPremiumAmount;
}

// flashLoanPremiumRateToProtocol is rate of flashLoan premium directly accrue to
// protocol treasury
export function calcuateFlashLoanPremiumToLPUSD(
  flashLoanPremiumUSD: BigDecimal,
  flashLoanPremiumRateToProtocol: BigDecimal
): BigDecimal {
  let premiumToLPUSD = BIGDECIMAL_ZERO;
  if (flashLoanPremiumRateToProtocol.gt(BIGDECIMAL_ZERO)) {
    premiumToLPUSD = flashLoanPremiumUSD.minus(
      flashLoanPremiumUSD.times(flashLoanPremiumRateToProtocol)
    );
  }
  return premiumToLPUSD;
}

export function updateProtocol(
  deltaCollateralUSD: BigDecimal = BIGDECIMAL_ZERO,
  deltaDebtUSD: BigDecimal = BIGDECIMAL_ZERO,
  liquidateUSD: BigDecimal = BIGDECIMAL_ZERO,
  newTotalRevenueUSD: BigDecimal = BIGDECIMAL_ZERO,
  newSupplySideRevenueUSD: BigDecimal = BIGDECIMAL_ZERO,
  protocolSideRevenueType: u32 = 0
): void {
  const protocol = getOrCreateLendingProtocol();

  // update Deposit
  if (deltaCollateralUSD.gt(BIGDECIMAL_ZERO)) {
    protocol.cumulativeDepositUSD =
      protocol.cumulativeDepositUSD.plus(deltaCollateralUSD);
  }

  // protocol.totalDepositBalanceUSD = protocol.totalDepositBalanceUSD.plus(deltaCollateralUSD);
  // instead, iterate over markets to get "mark-to-market" deposit balance
  let totalBorrowBalanceUSD = BIGDECIMAL_ZERO;
  let totalDepositBalanceUSD = BIGDECIMAL_ZERO;
  for (let i: i32 = 0; i < protocol.marketIDList.length; i++) {
    const marketID = protocol.marketIDList[i];
    const market = Market.load(marketID);
    totalBorrowBalanceUSD = totalBorrowBalanceUSD.plus(
      market!.totalBorrowBalanceUSD
    );
    totalDepositBalanceUSD = totalDepositBalanceUSD.plus(
      market!.totalDepositBalanceUSD
    );
  }
  protocol.totalBorrowBalanceUSD = totalBorrowBalanceUSD;
  protocol.totalDepositBalanceUSD = totalDepositBalanceUSD;
  protocol.totalValueLockedUSD = protocol.totalDepositBalanceUSD;

  /* alternatively, get total borrow (debt) from vat.debt
  // this would include borrow interest, etc
  // so they two will have some difference
  let vatContract = Vat.bind(Address.fromString(VAT_ADDRESS));
  let debtCall = vatContract.try_debt();
  if (debtCall.reverted) {
    log.warning("[updateProtocal]Failed to call Vat.debt; not updating protocol.totalBorrowBalanceUSD", []);
  } else {
    protocol.totalBorrowBalanceUSD = bigIntToBDUseDecimals(debtCall.value, RAD+);
  }
  */

  // update Borrow
  if (deltaDebtUSD.gt(BIGDECIMAL_ZERO)) {
    protocol.cumulativeBorrowUSD =
      protocol.cumulativeBorrowUSD.plus(deltaDebtUSD);
  }

  if (liquidateUSD.gt(BIGDECIMAL_ZERO)) {
    protocol.cumulativeLiquidateUSD =
      protocol.cumulativeLiquidateUSD.plus(liquidateUSD);
  }

  if (newTotalRevenueUSD.gt(BIGDECIMAL_ZERO)) {
    protocol.cumulativeTotalRevenueUSD =
      protocol.cumulativeTotalRevenueUSD.plus(newTotalRevenueUSD);
  }

  if (newSupplySideRevenueUSD.gt(BIGDECIMAL_ZERO)) {
    protocol.cumulativeSupplySideRevenueUSD =
      protocol.cumulativeSupplySideRevenueUSD.plus(newSupplySideRevenueUSD);
  }

  const newProtocolSideRevenueUSD = newTotalRevenueUSD.minus(
    newSupplySideRevenueUSD
  );
  if (newProtocolSideRevenueUSD.gt(BIGDECIMAL_ZERO)) {
    protocol.cumulativeProtocolSideRevenueUSD =
      protocol.cumulativeTotalRevenueUSD.minus(
        protocol.cumulativeSupplySideRevenueUSD
      );
    switch (protocolSideRevenueType) {
      case ProtocolSideRevenueType.STABILITYFEE:
        protocol._cumulativeProtocolSideStabilityFeeRevenue =
          protocol._cumulativeProtocolSideStabilityFeeRevenue!.plus(
            newProtocolSideRevenueUSD
          );
        break;
      case ProtocolSideRevenueType.LIQUIDATION:
        protocol._cumulativeProtocolSideLiquidationRevenue =
          protocol._cumulativeProtocolSideLiquidationRevenue!.plus(
            newProtocolSideRevenueUSD
          );
        break;
    }
  }

  // update mintedTokenSupplies
  const zarContract = ZAR.bind(Address.fromString(ZAR_ADDRESS));
  protocol.mintedTokens = [ZAR_ADDRESS];
  protocol.mintedTokenSupplies = [zarContract.totalSupply()];

  protocol.save();
}

export function updateMarket(
  event: ethereum.Event,
  market: Market,
  deltaCollateral: BigInt = BIGINT_ZERO,
  deltaCollateralUSD: BigDecimal = BIGDECIMAL_ZERO,
  deltaDebt: BigInt = BIGINT_ZERO,
  deltaDebtUSD: BigDecimal = BIGDECIMAL_ZERO,
  liquidate: BigInt = BIGINT_ZERO,
  liquidateUSD: BigDecimal = BIGDECIMAL_ZERO,
  newTotalRevenueUSD: BigDecimal = BIGDECIMAL_ZERO,
  newSupplySideRevenueUSD: BigDecimal = BIGDECIMAL_ZERO
): void {
  if (deltaCollateral != BIGINT_ZERO) {
    // deltaCollateral can be positive or negative
    market.inputTokenBalance = market.inputTokenBalance.plus(deltaCollateral);

    if (deltaCollateral.gt(BIGINT_ZERO)) {
      market.cumulativeDepositUSD =
        market.cumulativeDepositUSD.plus(deltaCollateralUSD);
    } else if (deltaCollateral.lt(BIGINT_ZERO)) {
      // ignore as we don't care about cumulativeWithdraw in a market
    }
  }
  const token = new TokenManager(market.inputToken, event);
  const tokenPrice = token.getPriceUSD();

  if (tokenPrice) {
    market.inputTokenPriceUSD = tokenPrice;
    // here we "mark-to-market" - re-price total collateral using last price
    market.totalDepositBalanceUSD = bigIntToBDUseDecimals(
      market.inputTokenBalance,
      token.getDecimals()
    ).times(market.inputTokenPriceUSD);
  } else if (deltaCollateralUSD != BIGDECIMAL_ZERO) {
    market.totalDepositBalanceUSD =
      market.totalDepositBalanceUSD.plus(deltaCollateralUSD);
  }

  market.totalValueLockedUSD = market.totalDepositBalanceUSD;

  if (deltaDebtUSD != BIGDECIMAL_ZERO) {
    let vatContract = Vat.bind(Address.fromString(VAT_ADDRESS));
  let debtCall = vatContract.try_ilks(Bytes.fromHexString(market.ilk!));
  if (debtCall.reverted) {
    log.warning("[updateProtocal]Failed to call Vat.debt; not updating protocol.totalBorrowBalanceUSD", []);
    market.totalBorrowBalanceUSD =
      market.totalBorrowBalanceUSD.plus(deltaDebtUSD);
  } else {
    const zar = new TokenManager(ZAR_ADDRESS, event);
    market.totalBorrowBalanceUSD = debtCall.value.getArt().times(debtCall.value.getRate()).toBigDecimal().times(zar.getPriceUSD()).div(BIGDECIMAL_ONE_RAD);
  }
    if (deltaDebtUSD.gt(BIGDECIMAL_ZERO)) {
      market.cumulativeBorrowUSD =
        market.cumulativeBorrowUSD.plus(deltaDebtUSD);
    } else if (deltaDebtUSD.lt(BIGDECIMAL_ZERO)) {
      // again ignore repay
    }
  }

  if (liquidateUSD.gt(BIGDECIMAL_ZERO)) {
    market.cumulativeLiquidateUSD =
      market.cumulativeLiquidateUSD.plus(liquidateUSD);
  }

  // update revenue
  if (newTotalRevenueUSD.gt(BIGDECIMAL_ZERO)) {
    market.cumulativeTotalRevenueUSD =
      market.cumulativeTotalRevenueUSD.plus(newTotalRevenueUSD);
  }

  if (newSupplySideRevenueUSD.gt(BIGDECIMAL_ZERO)) {
    market.cumulativeSupplySideRevenueUSD =
      market.cumulativeSupplySideRevenueUSD.plus(newSupplySideRevenueUSD);
  }

  if (
    newTotalRevenueUSD.gt(BIGDECIMAL_ZERO) ||
    newSupplySideRevenueUSD.gt(BIGDECIMAL_ZERO)
  ) {
    market.cumulativeProtocolSideRevenueUSD =
      market.cumulativeTotalRevenueUSD.minus(
        market.cumulativeSupplySideRevenueUSD
      );
  }

  market.save();

  snapshotMarket(
    event,
    market,
    deltaCollateral,
    deltaCollateralUSD,
    deltaDebt,
    deltaDebtUSD,
    liquidate,
    liquidateUSD,
    newTotalRevenueUSD,
    newSupplySideRevenueUSD
  );
}

export function snapshotMarket(
  event: ethereum.Event,
  market: Market,
  deltaCollateral: BigInt = BIGINT_ZERO,
  deltaCollateralUSD: BigDecimal = BIGDECIMAL_ZERO,
  deltaDebt: BigInt = BIGINT_ZERO,
  deltaDebtUSD: BigDecimal = BIGDECIMAL_ZERO,
  liquidate: BigInt = BIGINT_ZERO,
  liquidateUSD: BigDecimal = BIGDECIMAL_ZERO,
  newTotalRevenueUSD: BigDecimal = BIGDECIMAL_ZERO,
  newSupplySideRevenueUSD: BigDecimal = BIGDECIMAL_ZERO
): void {
  const marketID = market.id;
  const marketHourlySnapshot = getOrCreateMarketHourlySnapshot(event, marketID);
  const marketDailySnapshot = getOrCreateMarketDailySnapshot(event, marketID);
  if (marketHourlySnapshot == null || marketDailySnapshot == null) {
    log.error("[snapshotMarket]Failed to get marketsnapshot for {}", [
      marketID,
    ]);
    return;
  }
  const hours = event.block.timestamp.toI32() / SECONDS_PER_HOUR;
  const hourlySnapshotRates = getSnapshotRates(market.rates, hours.toString());

  const days = event.block.timestamp.toI32() / SECONDS_PER_DAY;
  const dailySnapshotRates = getSnapshotRates(market.rates, days.toString());

  marketHourlySnapshot.hours = hours;
  marketHourlySnapshot.totalValueLockedUSD = market.totalValueLockedUSD;
  marketHourlySnapshot.totalBorrowBalanceUSD = market.totalBorrowBalanceUSD;
  marketHourlySnapshot.cumulativeSupplySideRevenueUSD =
    market.cumulativeSupplySideRevenueUSD;
  marketHourlySnapshot.cumulativeProtocolSideRevenueUSD =
    market.cumulativeProtocolSideRevenueUSD;
  marketHourlySnapshot.cumulativeTotalRevenueUSD =
    market.cumulativeTotalRevenueUSD;
  marketHourlySnapshot.totalDepositBalanceUSD = market.totalDepositBalanceUSD;
  marketHourlySnapshot.cumulativeDepositUSD = market.cumulativeDepositUSD;
  marketHourlySnapshot.totalBorrowBalanceUSD = market.totalBorrowBalanceUSD;
  marketHourlySnapshot.cumulativeBorrowUSD = market.cumulativeBorrowUSD;
  marketHourlySnapshot.cumulativeLiquidateUSD = market.cumulativeLiquidateUSD;
  marketHourlySnapshot.inputTokenBalance = market.inputTokenBalance;
  marketHourlySnapshot.inputTokenPriceUSD = market.inputTokenPriceUSD;
  marketHourlySnapshot.rates = hourlySnapshotRates;
  //marketHourlySnapshot.outputTokenSupply = market.outputTokenSupply;
  //marketHourlySnapshot.outputTokenPriceUSD = market.outputTokenPriceUSD;

  marketHourlySnapshot.blockNumber = event.block.number;
  marketHourlySnapshot.timestamp = event.block.timestamp;

  if (deltaCollateral != BIGINT_ZERO) {
    if (deltaCollateral.gt(BIGINT_ZERO)) {
      marketDailySnapshot.dailyNativeDeposit.plus(deltaCollateral);
    } else if (deltaCollateral.lt(BIGINT_ZERO)) {
      marketDailySnapshot.dailyNativeWithdraw.plus(deltaCollateral);
    }
  }

  if (deltaDebt != BIGINT_ZERO) {
    if (deltaDebt.gt(BIGINT_ZERO)) {
      marketDailySnapshot.dailyNativeBorrow.plus(deltaDebt);
    } else if (deltaDebt.lt(BIGINT_ZERO)) {
      marketDailySnapshot.dailyNativeRepay.plus(deltaDebt);
    }
  }

  if (liquidate != BIGINT_ZERO) {
    marketDailySnapshot.dailyNativeLiquidate.plus(liquidate);
  }

  marketDailySnapshot.days = days;
  marketDailySnapshot.totalValueLockedUSD = market.totalValueLockedUSD;
  marketDailySnapshot.cumulativeSupplySideRevenueUSD =
    market.cumulativeSupplySideRevenueUSD;
  marketDailySnapshot.cumulativeProtocolSideRevenueUSD =
    market.cumulativeProtocolSideRevenueUSD;
  marketDailySnapshot.cumulativeTotalRevenueUSD =
    market.cumulativeTotalRevenueUSD;
  marketDailySnapshot.totalDepositBalanceUSD = market.totalDepositBalanceUSD;
  marketDailySnapshot.cumulativeDepositUSD = market.cumulativeDepositUSD;
  marketDailySnapshot.totalBorrowBalanceUSD = market.totalBorrowBalanceUSD;
  marketDailySnapshot.cumulativeBorrowUSD = market.cumulativeBorrowUSD;
  marketDailySnapshot.cumulativeLiquidateUSD = market.cumulativeLiquidateUSD;
  marketDailySnapshot.cumulativeTransferUSD = BIGDECIMAL_ZERO;
  marketDailySnapshot.cumulativeFlashloanUSD = BIGDECIMAL_ZERO;
  marketDailySnapshot.inputTokenBalance = market.inputTokenBalance;
  marketDailySnapshot.inputTokenPriceUSD = market.inputTokenPriceUSD;
  marketDailySnapshot.rates = dailySnapshotRates;
  //marketDailySnapshot.outputTokenSupply = market.outputTokenSupply;
  //marketDailySnapshot.outputTokenPriceUSD = market.outputTokenPriceUSD;

  marketDailySnapshot.positionCount = market.positionCount;
  marketDailySnapshot.openPositionCount = market.openPositionCount;
  marketDailySnapshot.closedPositionCount = market.closedPositionCount;
  marketDailySnapshot.lendingPositionCount = market.lendingPositionCount;
  marketDailySnapshot.borrowingPositionCount = market.borrowingPositionCount;

  marketDailySnapshot.blockNumber = event.block.number;
  marketDailySnapshot.timestamp = event.block.timestamp;

  if (deltaCollateralUSD.gt(BIGDECIMAL_ZERO)) {
    marketHourlySnapshot.hourlyDepositUSD =
      marketHourlySnapshot.hourlyDepositUSD.plus(deltaCollateralUSD);
    marketDailySnapshot.dailyDepositUSD =
      marketDailySnapshot.dailyDepositUSD.plus(deltaCollateralUSD);
  } else if (deltaCollateralUSD.lt(BIGDECIMAL_ZERO)) {
    // minus a negative number
    marketHourlySnapshot.hourlyWithdrawUSD =
      marketHourlySnapshot.hourlyWithdrawUSD.minus(deltaCollateralUSD);
    marketDailySnapshot.dailyWithdrawUSD =
      marketDailySnapshot.dailyWithdrawUSD.minus(deltaCollateralUSD);
  }

  if (deltaDebtUSD.gt(BIGDECIMAL_ZERO)) {
    marketHourlySnapshot.hourlyBorrowUSD =
      marketHourlySnapshot.hourlyBorrowUSD.plus(deltaDebtUSD);
    marketDailySnapshot.dailyBorrowUSD =
      marketDailySnapshot.dailyBorrowUSD.plus(deltaDebtUSD);
  } else if (deltaDebtUSD.lt(BIGDECIMAL_ZERO)) {
    // minus a negative number
    marketHourlySnapshot.hourlyRepayUSD =
      marketHourlySnapshot.hourlyRepayUSD.minus(deltaDebtUSD);
    marketDailySnapshot.dailyRepayUSD =
      marketDailySnapshot.dailyRepayUSD.minus(deltaDebtUSD);
  }

  if (liquidateUSD.gt(BIGDECIMAL_ZERO)) {
    marketHourlySnapshot.hourlyLiquidateUSD =
      marketHourlySnapshot.hourlyLiquidateUSD.plus(liquidateUSD);
    marketDailySnapshot.dailyLiquidateUSD =
      marketDailySnapshot.dailyLiquidateUSD.plus(liquidateUSD);
  }

  // update revenue
  if (newTotalRevenueUSD.gt(BIGDECIMAL_ZERO)) {
    marketHourlySnapshot.hourlyTotalRevenueUSD =
      marketHourlySnapshot.hourlyTotalRevenueUSD.plus(newTotalRevenueUSD);
    marketDailySnapshot.dailyTotalRevenueUSD =
      marketDailySnapshot.dailyTotalRevenueUSD.plus(newTotalRevenueUSD);
  }

  if (newSupplySideRevenueUSD.gt(BIGDECIMAL_ZERO)) {
    marketHourlySnapshot.hourlySupplySideRevenueUSD =
      marketHourlySnapshot.hourlySupplySideRevenueUSD.plus(
        newSupplySideRevenueUSD
      );
    marketDailySnapshot.dailySupplySideRevenueUSD =
      marketDailySnapshot.dailySupplySideRevenueUSD.plus(
        newSupplySideRevenueUSD
      );
  }

  if (
    newTotalRevenueUSD.gt(BIGDECIMAL_ZERO) ||
    newSupplySideRevenueUSD.gt(BIGDECIMAL_ZERO)
  ) {
    marketHourlySnapshot.hourlyProtocolSideRevenueUSD =
      marketHourlySnapshot.hourlyTotalRevenueUSD.minus(
        marketHourlySnapshot.hourlySupplySideRevenueUSD
      );
    marketDailySnapshot.dailyProtocolSideRevenueUSD =
      marketDailySnapshot.dailyTotalRevenueUSD.minus(
        marketDailySnapshot.dailySupplySideRevenueUSD
      );
  }

  marketHourlySnapshot.save();
  marketDailySnapshot.save();
}

export function updateFinancialsSnapshot(
  event: ethereum.Event,
  deltaCollateralUSD: BigDecimal = BIGDECIMAL_ZERO,
  deltaDebtUSD: BigDecimal = BIGDECIMAL_ZERO,
  liquidateUSD: BigDecimal = BIGDECIMAL_ZERO,
  newTotalRevenueUSD: BigDecimal = BIGDECIMAL_ZERO,
  newSupplySideRevenueUSD: BigDecimal = BIGDECIMAL_ZERO,
  protocolSideRevenueType: u32 = 0
): void {
  const protocol = getOrCreateLendingProtocol();
  const financials = getOrCreateFinancials(event);

  financials.totalValueLockedUSD = protocol.totalValueLockedUSD;
  financials.totalBorrowBalanceUSD = protocol.totalBorrowBalanceUSD;
  financials.totalDepositBalanceUSD = protocol.totalDepositBalanceUSD;
  financials.mintedTokenSupplies = protocol.mintedTokenSupplies;

  financials.cumulativeSupplySideRevenueUSD =
    protocol.cumulativeSupplySideRevenueUSD;
  financials.cumulativeProtocolSideRevenueUSD =
    protocol.cumulativeProtocolSideRevenueUSD;
  financials._cumulativeProtocolSideStabilityFeeRevenue =
    protocol._cumulativeProtocolSideStabilityFeeRevenue;
  financials._cumulativeProtocolSideLiquidationRevenue =
    protocol._cumulativeProtocolSideLiquidationRevenue;
  financials.cumulativeTotalRevenueUSD = protocol.cumulativeTotalRevenueUSD;
  financials.cumulativeBorrowUSD = protocol.cumulativeBorrowUSD;
  financials.cumulativeDepositUSD = protocol.cumulativeDepositUSD;
  financials.cumulativeLiquidateUSD = protocol.cumulativeLiquidateUSD;

  if (deltaCollateralUSD.gt(BIGDECIMAL_ZERO)) {
    financials.dailyDepositUSD =
      financials.dailyDepositUSD.plus(deltaCollateralUSD);
  } else if (deltaCollateralUSD.lt(BIGDECIMAL_ZERO)) {
    // minus a negative number
    financials.dailyWithdrawUSD =
      financials.dailyWithdrawUSD.minus(deltaCollateralUSD);
  }

  if (deltaDebtUSD.gt(BIGDECIMAL_ZERO)) {
    financials.dailyBorrowUSD = financials.dailyBorrowUSD.plus(deltaDebtUSD);
  } else if (deltaDebtUSD.lt(BIGDECIMAL_ZERO)) {
    // minus a negative number
    financials.dailyRepayUSD = financials.dailyRepayUSD.minus(deltaDebtUSD);
  }

  if (liquidateUSD.gt(BIGDECIMAL_ZERO)) {
    financials.dailyLiquidateUSD =
      financials.dailyLiquidateUSD.plus(liquidateUSD);
  }

  if (newTotalRevenueUSD.gt(BIGDECIMAL_ZERO)) {
    financials.dailyTotalRevenueUSD =
      financials.dailyTotalRevenueUSD.plus(newTotalRevenueUSD);
  }

  if (newSupplySideRevenueUSD.gt(BIGDECIMAL_ZERO)) {
    financials.dailySupplySideRevenueUSD =
      financials.dailySupplySideRevenueUSD.plus(newSupplySideRevenueUSD);
  }

  const newProtocolSideRevenueUSD = newTotalRevenueUSD.minus(
    newSupplySideRevenueUSD
  );
  if (newProtocolSideRevenueUSD.gt(BIGDECIMAL_ZERO)) {
    financials.dailyProtocolSideRevenueUSD =
      financials.dailyTotalRevenueUSD.minus(
        financials.dailySupplySideRevenueUSD
      );
    switch (protocolSideRevenueType) {
      case ProtocolSideRevenueType.STABILITYFEE:
        financials._dailyProtocolSideStabilityFeeRevenue =
          financials._dailyProtocolSideStabilityFeeRevenue!.plus(
            newProtocolSideRevenueUSD
          );
        break;
      case ProtocolSideRevenueType.LIQUIDATION:
        financials._dailyProtocolSideLiquidationRevenue =
          financials._dailyProtocolSideLiquidationRevenue!.plus(
            newProtocolSideRevenueUSD
          );
        break;
    }
  }

  financials.blockNumber = event.block.number;
  financials.timestamp = event.block.timestamp;
  financials.save();
}

// updatePosition based on deposit/withdraw/borrow/repay
// Need to be called after createTransactions
export function updatePosition(
  event: ethereum.Event,
  urn: string,
  ilk: Bytes,
  market: Market,
  deltaCollateral: BigInt = BIGINT_ZERO,
  deltaDebt: BigInt = BIGINT_ZERO
): void {
  const accountAddress = getOwnerAddress(urn);

  const protocol = getOrCreateLendingProtocol();
  const accountManager = new AccountManager(accountAddress);
  const account = accountManager.getAccount();

  if (deltaCollateral.notEqual(BIGINT_ZERO)) {
    let lenderPosition = getOpenPosition(
      event,
      urn,
      ilk,
      PositionSide.COLLATERAL
    );

    if (lenderPosition == null) {
      // this is a new lender position, deltaCollateral > 0
      // because user cannot create a lender position with deltaCollateral <=0
      lenderPosition = getOrCreatePosition(
        event,
        urn,
        ilk,
        PositionSide.COLLATERAL,
        true
      );

      if (deltaCollateral.le(BIGINT_ZERO)) {
        log.error(
          "[updatePosition]Creating a new lender position {} with deltaCollateral ={} <= 0 at tx {}-{}",
          [
            lenderPosition.id,
            deltaCollateral.toString(),
            event.transaction.hash.toHexString(),
            event.transactionLogIndex.toString(),
          ]
        );
        log.critical("", []);
      }

      protocol.openPositionCount += INT_ONE;
      protocol.cumulativePositionCount += INT_ONE;

      market.positionCount += INT_ONE;
      market.openPositionCount += INT_ONE;
      market.lendingPositionCount += INT_ONE;

      account.positionCount += INT_ONE;
      account.openPositionCount += INT_ONE;
      //account.depositCount += INT_ONE;
    }

    lenderPosition.balance = lenderPosition.balance.plus(deltaCollateral);
    // this may be less than 0 (but > -100) from rounding & for tokens with decimals < 18
    // because we keep position balance in native unit, but maker always keep them in WAD (18 decimals)
    //
    // for example 1. at block 12507581, urn 0x03453d22095c0edd61cd40c3ccdc394a0e85dc1a
    // repaid -203334964101176257798573 dai, when the borrow balance
    // was 203334964101176257798572
    // 2. at block 14055178, urn 0x1c47bb6773db2a441264c1af2c943d8bdfaf19fe
    // repaid -30077488379451392498995529 dai, when the borrow balance
    // was 30077488379451392498995503
    if (lenderPosition.balance.lt(BIGINT_ZERO)) {
      if (lenderPosition.balance.ge(BIGINT_NEG_HUNDRED)) {
        // a small negative lender position, likely due to rounding
        lenderPosition.balance = BIGINT_ZERO;
      } else {
        log.error(
          "[updatePosition]A negative lender balance of {} for position {} with tx {}-{}",
          [
            lenderPosition.balance.toString(),
            lenderPosition.id,
            event.transaction.hash.toHexString(),
            event.transactionLogIndex.toString(),
          ]
        );
        log.critical("", []);
      }
    }

    if (deltaCollateral.gt(BIGINT_ZERO)) {
      // deposit
      lenderPosition.depositCount += INT_ONE;

      const DepositId = createEventID(event, Transaction.DEPOSIT);
      // link event to position (createTransactions needs to be called first)
      const deposit = Deposit.load(DepositId)!;
      deposit.position = lenderPosition.id;
      deposit.save();
    } else if (deltaCollateral.lt(BIGINT_ZERO)) {
      lenderPosition.withdrawCount += INT_ONE;

      if (lenderPosition.balance == BIGINT_ZERO) {
        // close lender position
        lenderPosition.blockNumberClosed = event.block.number;
        lenderPosition.timestampClosed = event.block.timestamp;
        lenderPosition.hashClosed = event.transaction.hash;

        protocol.openPositionCount -= INT_ONE;

        market.openPositionCount -= INT_ONE;
        market.closedPositionCount += INT_ONE;

        account.openPositionCount -= INT_ONE;
        account.closedPositionCount += INT_ONE;
      }

      const withdrawId = createEventID(event, Transaction.WITHDRAW);

      // link event to position (createTransactions needs to be called first)
      const withdraw = Withdraw.load(withdrawId)!;
      withdraw.position = lenderPosition.id;
      withdraw.save();
    }

    log.info("[updatePosition]position positionID={}, account={}, balance={}", [
      lenderPosition.id,
      lenderPosition.account,
      lenderPosition.balance.toString(),
    ]);

    lenderPosition.save();
    snapshotPosition(event, lenderPosition);
  }

  if (deltaDebt.notEqual(BIGINT_ZERO)) {
    let borrowerPosition = getOpenPosition(
      event,
      urn,
      ilk,
      PositionSide.BORROWER
    );
    if (borrowerPosition == null) {
      // new borrower position
      borrowerPosition = getOrCreatePosition(
        event,
        urn,
        ilk,
        PositionSide.BORROWER,
        true
      );

      if (deltaDebt.le(BIGINT_ZERO)) {
        log.error(
          "[updatePosition]Creating a new lender position {} with deltaDebt={} <= 0 at tx {}-{}",
          [
            borrowerPosition.id,
            deltaDebt.toString(),
            event.transaction.hash.toHexString(),
            event.transactionLogIndex.toString(),
          ]
        );
        log.critical("", []);
      }

      protocol.openPositionCount += INT_ONE;
      protocol.cumulativePositionCount += INT_ONE;

      market.positionCount += INT_ONE;
      market.openPositionCount += INT_ONE;
      market.borrowingPositionCount += INT_ONE;

      account.positionCount += INT_ONE;
      account.openPositionCount += INT_ONE;
    }

    borrowerPosition.balance = borrowerPosition.balance.plus(deltaDebt);
    // see comment above for lenderPosition.balance
    if (borrowerPosition.balance.lt(BIGINT_ZERO)) {
      if (borrowerPosition.balance.ge(BIGINT_NEG_HUNDRED)) {
        // a small negative lender position, likely due to rounding
        borrowerPosition.balance = BIGINT_ZERO;
      } else {
        log.error(
          "[updatePosition]A negative lender balance of {} for position {} with tx {}-{}",
          [
            borrowerPosition.balance.toString(),
            borrowerPosition.id,
            event.transaction.hash.toHexString(),
            event.transactionLogIndex.toString(),
          ]
        );
        log.critical("", []);
      }
      log.critical("", []);
    }

    if (deltaDebt.gt(BIGINT_ZERO)) {
      borrowerPosition.borrowCount += INT_ONE;
      //account.borrowCount += INT_ONE;

      const borrowId = createEventID(event, Transaction.BORROW);

      // link event to position (createTransactions needs to be called first)
      const borrow = Borrow.load(borrowId)!;
      borrow.position = borrowerPosition.id;
      borrow.save();
    } else if (deltaDebt.lt(BIGINT_ZERO)) {
      borrowerPosition.repayCount += INT_ONE;

      if (borrowerPosition.balance == BIGINT_ZERO) {
        // close borrowerPosition
        borrowerPosition.blockNumberClosed = event.block.number;
        borrowerPosition.timestampClosed = event.block.timestamp;
        borrowerPosition.hashClosed = event.transaction.hash;

        protocol.openPositionCount -= INT_ONE;

        market.openPositionCount -= INT_ONE;
        market.closedPositionCount += INT_ONE;

        account.openPositionCount -= INT_ONE;
        account.closedPositionCount += INT_ONE;
        //account.repayCount += INT_ONE;
      }
      const repayId = createEventID(event, Transaction.REPAY);

      const repay = Repay.load(repayId)!;
      repay.position = borrowerPosition.id;
      repay.save();
    }

    log.info("[updatePosition]position positionID={}, account={}, balance={}", [
      borrowerPosition.id,
      borrowerPosition.account,
      borrowerPosition.balance.toString(),
    ]);
    borrowerPosition.save();
    snapshotPosition(event, borrowerPosition);
  }

  protocol.save();
  account.save();

  if (account.openPositionCount < 0) {
    log.error(
      "[updatePosition]urn {} for account {} openPositionCount={} at tx {}-{}",
      [
        urn,
        account.id,
        account.openPositionCount.toString(),
        event.transaction.hash.toHexString(),
        event.transactionLogIndex.toString(),
      ]
    );
    log.critical("", []);
  }
}

// handle transfer of position from one user account (src) to another (dst),
// possibly to another urn address
export function transferPosition(
  event: ethereum.Event,
  ilk: Bytes,
  srcUrn: string, // src urn
  dstUrn: string, // dst urn
  side: string,
  srcAccountAddress: string | null = null,
  dstAccountAddress: string | null = null,
  transferAmount: BigInt | null = null // suport partial transfer of a position
): void {
  if (srcUrn == dstUrn && srcAccountAddress == dstAccountAddress) {
    log.info(
      "[transferPosition]srcUrn {}==dstUrn {} && srcAccountAddress {}==dstAccountAddress {}, no transfer",
      [
        srcUrn,
        dstUrn,
        srcAccountAddress ? srcAccountAddress : "null",
        dstAccountAddress ? dstAccountAddress : "null",
      ]
    );
    return;
  }

  const protocol = getOrCreateLendingProtocol();
  const market: Market = getMarketFromIlk(ilk)!;
  const usageHourlySnapshot = getOrCreateUsageMetricsHourlySnapshot(event);
  const usageDailySnapshot = getOrCreateUsageMetricsDailySnapshot(event);

  if (srcAccountAddress == null) {
    srcAccountAddress = getOwnerAddress(srcUrn).toLowerCase();
  }
  const accountManager = new AccountManager(srcAccountAddress!);
  const srcAccount = accountManager.getAccount();

  const srcPosition = getOpenPosition(event, srcUrn, ilk, side);
  if (srcPosition == null) {
    log.warning(
      "[transferPosition]No open position found for source: urn {}/ilk {}/side {}; no transfer",
      [srcUrn, ilk.toHexString(), side]
    );
    return;
  }

  const srcPositionBalance0 = srcPosition.balance;
  if (!transferAmount || transferAmount > srcPosition.balance) {
    const transferAmountStr = transferAmount
      ? transferAmount.toString()
      : "null";
    log.warning(
      "[transferPosition]transferAmount={} is null or > src position balance {} for {}",
      [transferAmountStr, srcPosition.balance.toString(), srcPosition.id]
    );
    transferAmount = srcPosition.balance;
  }
  assert(
    transferAmount <= srcPosition.balance,
    `[transferPosition]src ${srcUrn}/ilk ${ilk.toHexString()}/side ${side} transfer amount ${transferAmount.toString()} > balance ${
      srcPosition.balance
    }`
  );

  srcPosition.balance = srcPosition.balance.minus(transferAmount);
  if (srcPosition.balance == BIGINT_ZERO) {
    srcPosition.blockNumberClosed = event.block.number;
    srcPosition.timestampClosed = event.block.timestamp;
    srcPosition.hashClosed = event.transaction.hash;
    protocol.openPositionCount -= INT_ONE;
    market.openPositionCount -= INT_ONE;
    market.closedPositionCount += INT_ONE;
    srcAccount.openPositionCount -= INT_ONE;
    srcAccount.closedPositionCount += INT_ONE;
  }
  srcPosition.save();
  snapshotPosition(event, srcPosition);

  if (dstAccountAddress == null) {
    dstAccountAddress = getOwnerAddress(dstUrn).toLowerCase();
  }

  let dstAccount = Account.load(dstAccountAddress!);
  if (dstAccount == null) {
    const accountManager = new AccountManager(dstAccountAddress!);
    dstAccount = accountManager.getAccount();
    protocol.cumulativeUniqueUsers += 1;
    usageDailySnapshot.cumulativeUniqueUsers += 1;
    usageHourlySnapshot.cumulativeUniqueUsers += 1;
  }

  // transfer srcUrn to dstUrn
  // or partial transfer of a position (amount < position.balance)
  let dstPosition = getOpenPosition(event, dstUrn, ilk, side);
  if (!dstPosition) {
    dstPosition = getOrCreatePosition(event, dstUrn, ilk, side, true);
  }

  dstPosition.balance = dstPosition.balance.plus(transferAmount);
  dstPosition.save();
  snapshotPosition(event, dstPosition);

  protocol.openPositionCount += INT_ONE;
  protocol.cumulativePositionCount += INT_ONE;
  market.openPositionCount += INT_ONE;
  market.positionCount += INT_ONE;
  if (side == PositionSide.BORROWER) {
    market.borrowingPositionCount += INT_ONE;
  } else if (side == PositionSide.COLLATERAL) {
    market.lendingPositionCount += INT_ONE;
  }
  dstAccount.openPositionCount += INT_ONE;
  dstAccount.positionCount += INT_ONE;

  log.info(
    "[transferPosition]transfer {} from {} (balance={}) to {} (balance={})",
    [
      transferAmount.toString(),
      srcPosition.id,
      srcPositionBalance0.toString(),
      dstPosition.id,
      dstPosition.balance.toString(),
    ]
  );

  protocol.save();
  market.save();
  usageDailySnapshot.save();
  usageHourlySnapshot.save();
  srcAccount.save();
  dstAccount.save();

  assert(
    srcAccount.openPositionCount >= 0,
    `Account ${srcAccount.id} openPositionCount=${srcAccount.openPositionCount}`
  );
  assert(
    dstAccount.openPositionCount >= 0,
    `Account ${dstAccount.id} openPositionCount=${dstAccount.openPositionCount}`
  );
}

// handle liquidations for Position entity
export function liquidatePosition(
  event: ethereum.Event,
  urn: string,
  ilk: Bytes,
  collateral: BigInt, // net collateral liquidated
  debt: BigInt // debt repaid
): string[] {
  const protocol = getOrCreateLendingProtocol();
  const market: Market = getMarketFromIlk(ilk)!;
  const accountAddress = getOwnerAddress(urn);
  const accountManager = new AccountManager(accountAddress);
  const account = accountManager.getAccount();

  log.info("[liquidatePosition]urn={}, ilk={}, collateral={}, debt={}", [
    urn,
    ilk.toHexString(),
    collateral.toString(),
    debt.toString(),
  ]);
  const borrowerPosition = getOpenPosition(
    event,
    urn,
    ilk,
    PositionSide.BORROWER
  )!;
  const lenderPosition = getOpenPosition(
    event,
    urn,
    ilk,
    PositionSide.COLLATERAL
  )!;
  if (debt > borrowerPosition.balance) {
    //this can happen because of rounding
    log.warning("[liquidatePosition]debt repaid {} > borrowing balance {}", [
      debt.toString(),
      borrowerPosition.balance.toString(),
    ]);
    debt = borrowerPosition.balance;
  }
  borrowerPosition.balance = borrowerPosition.balance.minus(debt);
  borrowerPosition.liquidationCount += INT_ONE;

  assert(
    borrowerPosition.balance.ge(BIGINT_ZERO),
    `[liquidatePosition]balance of position ${borrowerPosition.id} ${borrowerPosition.balance} < 0`
  );
  // liquidation closes the borrowing side position
  if (borrowerPosition.balance == BIGINT_ZERO) {
    borrowerPosition.blockNumberClosed = event.block.number;
    borrowerPosition.timestampClosed = event.block.timestamp;
    borrowerPosition.hashClosed = event.transaction.hash;
    snapshotPosition(event, borrowerPosition);

    protocol.openPositionCount -= INT_ONE;
    market.openPositionCount -= INT_ONE;
    market.closedPositionCount += INT_ONE;
    market.borrowingPositionCount -= INT_ONE;
    account.openPositionCount -= INT_ONE;
    account.closedPositionCount += INT_ONE;
  }
  borrowerPosition.save();
  snapshotPosition(event, borrowerPosition);

  lenderPosition.balance = lenderPosition.balance.minus(collateral);
  lenderPosition.liquidationCount += INT_ONE;

  if (lenderPosition.balance == BIGINT_ZERO) {
    // lender side is closed
    lenderPosition.blockNumberClosed = event.block.number;
    lenderPosition.timestampClosed = event.block.timestamp;
    lenderPosition.hashClosed = event.transaction.hash;

    protocol.openPositionCount -= INT_ONE;
    market.openPositionCount -= INT_ONE;
    market.closedPositionCount += INT_ONE;
    market.lendingPositionCount -= INT_ONE;
    account.openPositionCount -= INT_ONE;
    account.closedPositionCount += INT_ONE;
  }
  lenderPosition.save();
  snapshotPosition(event, lenderPosition);

  protocol.save();
  market.save();
  account.save();

  assert(
    account.openPositionCount >= 0,
    `Account ${account.id} openPositionCount=${account.openPositionCount}`
  );
  return ["lenderPosition.id", "borrowerPosition.id"];
}

export function snapshotPosition(
  event: ethereum.Event,
  position: Position
): void {
  const txHash = event.transaction.hash;
  const snapshotID = `${position.id}-${txHash}-${event.logIndex.toString()}`;
  let snapshot = PositionSnapshot.load(snapshotID);
  const market = getOrCreateMarket(position.market);
  const token = new TokenManager(market.inputToken, event);
  const mantissaFactorBD = exponentToBigDecimal(token.getDecimals());
  if (snapshot == null) {
    // this should always be the case with schema v2.0.1
    snapshot = new PositionSnapshot(snapshotID);
    snapshot.account = position.account;
    snapshot.hash = txHash;
    snapshot.logIndex = event.logIndex.toI32();
    snapshot.nonce = event.transaction.nonce;
    snapshot.position = position.id;
    snapshot.balance = position.balance;
    snapshot.balanceUSD = position.balance
      .toBigDecimal()
      .div(mantissaFactorBD)
      .times(market.inputTokenPriceUSD);
    snapshot.blockNumber = event.block.number;
    snapshot.timestamp = event.block.timestamp;
    snapshot.save();
  } else {
    log.error(
      "[snapshotPosition]Position snapshot {} already exists for position {} at tx hash {}",
      [snapshotID, position.id, txHash.toHexString()]
    );
  }
}

// update usage for deposit/withdraw/borrow/repay
export function updateUsageMetrics(
  event: ethereum.Event,
  market: Market,
  users: string[] = [], //user u, v, w
  deltaCollateralUSD: BigDecimal = BIGDECIMAL_ZERO,
  deltaDebtUSD: BigDecimal = BIGDECIMAL_ZERO,
  liquidateUSD: BigDecimal = BIGDECIMAL_ZERO,
  liquidator: string | null = null,
  liquidatee: string | null = null
): void {
  const protocol = getOrCreateLendingProtocol();
  const usageHourlySnapshot = getOrCreateUsageMetricsHourlySnapshot(event);
  const usageDailySnapshot = getOrCreateUsageMetricsDailySnapshot(event);
  const marketDailySnapshot = getOrCreateMarketDailySnapshot(event, market.id);

  const hours: string = (
    event.block.timestamp.toI64() / SECONDS_PER_HOUR
  ).toString();
  const days: string = (
    event.block.timestamp.toI64() / SECONDS_PER_DAY
  ).toString();

  // userU, userV, userW may be the same, they may not
  for (let i: i32 = 0; i < users.length; i++) {
    const accountID = users[i];
    let account = Account.load(accountID);
    if (account == null) {
      const accountManager = new AccountManager(accountID);
      account = accountManager.getAccount();

      protocol.cumulativeUniqueUsers += 1;
      usageHourlySnapshot.cumulativeUniqueUsers += 1;
      usageDailySnapshot.cumulativeUniqueUsers += 1;
      market.cumulativeUniqueUsers += 1;
    }

    const hourlyActiveAcctountID = "hourly-"
      .concat(accountID)
      .concat("-")
      .concat(hours);
    let hourlyActiveAccount = _ActiveAccount.load(hourlyActiveAcctountID);
    if (hourlyActiveAccount == null) {
      hourlyActiveAccount = new _ActiveAccount(hourlyActiveAcctountID);
      hourlyActiveAccount.save();

      usageHourlySnapshot.hourlyActiveUsers += 1;
    }

    const dailyActiveAcctountID = "daily-"
      .concat(accountID)
      .concat("-")
      .concat(days);
    let dailyActiveAccount = _ActiveAccount.load(dailyActiveAcctountID);
    if (dailyActiveAccount == null) {
      dailyActiveAccount = new _ActiveAccount(dailyActiveAcctountID);
      dailyActiveAccount.save();
      marketDailySnapshot.dailyActiveUsers += 1;
      usageDailySnapshot.dailyActiveUsers += 1;
      if (deltaCollateralUSD.gt(BIGDECIMAL_ZERO)) {
        marketDailySnapshot.dailyActiveDepositors += 1;
        marketDailySnapshot.dailyDepositCount += 1;
      }
      if (deltaDebtUSD.gt(BIGDECIMAL_ZERO)) {
        marketDailySnapshot.dailyActiveBorrowers += 1;
        marketDailySnapshot.dailyBorrowCount += 1;
      }
    }
  }

  if (deltaCollateralUSD.gt(BIGDECIMAL_ZERO)) {
    market.transactionCount += INT_ONE;
    protocol.transactionCount += INT_ONE;
    protocol.depositCount += 1;
    usageHourlySnapshot.hourlyDepositCount += 1;
    usageDailySnapshot.dailyDepositCount += 1;
    market.depositCount += 1;
    const depositAccount = Account.load(users[1]); // user v
    if (depositAccount!.depositCount == 0) {
      // a new depositor
      protocol.cumulativeUniqueDepositors += 1;
      usageDailySnapshot.cumulativeUniqueDepositors += 1;
      market.cumulativeUniqueDepositors += 1;
    }
    depositAccount!.depositCount += INT_ONE;
    depositAccount!.save();

    const dailyDepositorAcctountID = "daily-depositor-"
      .concat(users[1])
      .concat("-")
      .concat(days);
    let dailyDepositorAccount = _ActiveAccount.load(dailyDepositorAcctountID);
    if (dailyDepositorAccount == null) {
      dailyDepositorAccount = new _ActiveAccount(dailyDepositorAcctountID);
      dailyDepositorAccount.save();

      usageDailySnapshot.dailyActiveDepositors += 1;
    }
  } else if (deltaCollateralUSD.lt(BIGDECIMAL_ZERO)) {
    market.transactionCount += INT_ONE;
    protocol.transactionCount += INT_ONE;
    protocol.withdrawCount += 1;
    usageHourlySnapshot.hourlyWithdrawCount += 1;
    usageDailySnapshot.dailyWithdrawCount += 1;
    market.withdrawCount += 1;
    marketDailySnapshot.dailyWithdrawCount += 1;

    const withdrawAccount = Account.load(users[1]);
    withdrawAccount!.withdrawCount += INT_ONE;
    withdrawAccount!.save();
  }

  if (deltaDebtUSD.gt(BIGDECIMAL_ZERO)) {
    market.transactionCount += INT_ONE;
    protocol.transactionCount += INT_ONE;
    protocol.borrowCount += 1;
    usageHourlySnapshot.hourlyBorrowCount += 1;
    usageDailySnapshot.dailyBorrowCount += 1;
    market.borrowCount += 1;

    const borrowAccount = Account.load(users[2]); // user w
    if (borrowAccount!.borrowCount == 0) {
      // a new borrower
      protocol.cumulativeUniqueBorrowers += 1;
      usageDailySnapshot.cumulativeUniqueBorrowers += 1;
      market.cumulativeUniqueBorrowers += 1;
    }
    borrowAccount!.borrowCount += INT_ONE;
    borrowAccount!.save();

    const dailyBorrowerAcctountID = "daily-borrow-"
      .concat(users[2])
      .concat("-")
      .concat(days);
    let dailyBorrowerAccount = _ActiveAccount.load(dailyBorrowerAcctountID);
    if (dailyBorrowerAccount == null) {
      dailyBorrowerAccount = new _ActiveAccount(dailyBorrowerAcctountID);
      dailyBorrowerAccount.save();

      usageDailySnapshot.dailyActiveBorrowers += 1;
    }
  } else if (deltaDebtUSD.lt(BIGDECIMAL_ZERO)) {
    market.transactionCount += INT_ONE;
    protocol.transactionCount += INT_ONE;
    protocol.repayCount += 1;
    usageHourlySnapshot.hourlyRepayCount += 1;
    usageDailySnapshot.dailyRepayCount += 1;
    market.repayCount += 1;
    marketDailySnapshot.dailyRepayCount += 1;

    const repayAccount = Account.load(users[1]);
    repayAccount!.repayCount += INT_ONE;
    repayAccount!.save();
  }

  if (liquidateUSD.gt(BIGDECIMAL_ZERO)) {
    market.transactionCount += INT_ONE;
    protocol.transactionCount += INT_ONE;
    usageHourlySnapshot.hourlyLiquidateCount += 1;
    usageDailySnapshot.dailyLiquidateCount += 1;
    marketDailySnapshot.dailyLiquidateCount += 1;
    protocol.liquidationCount += INT_ONE;
    market.liquidationCount += INT_ONE;

    if (liquidator) {
      let liquidatorAccount = Account.load(liquidator);
      // a new liquidator
      if (liquidatorAccount == null || liquidatorAccount.liquidateCount == 0) {
        if (liquidatorAccount == null) {
          // liquidators will repay debt & withdraw collateral,
          // they are unique users if not yet in Account
          protocol.cumulativeUniqueUsers += 1;
          usageDailySnapshot.cumulativeUniqueUsers += 1;
          usageHourlySnapshot.cumulativeUniqueUsers += 1;
          market.cumulativeUniqueUsers += 1;
        }
        const accountManager = new AccountManager(liquidator);
        liquidatorAccount = accountManager.getAccount();

        protocol.cumulativeUniqueLiquidators += 1;
        usageDailySnapshot.cumulativeUniqueLiquidators += 1;
        market.cumulativeUniqueLiquidators += 1;
      }
      liquidatorAccount.liquidateCount += INT_ONE;
      liquidatorAccount.save();

      const dailyLiquidatorAcctountID = "daily-liquidate"
        .concat(liquidator)
        .concat("-")
        .concat(days);
      let dailyLiquidatorAccount = _ActiveAccount.load(
        dailyLiquidatorAcctountID
      );
      if (dailyLiquidatorAccount == null) {
        dailyLiquidatorAccount = new _ActiveAccount(dailyLiquidatorAcctountID);
        dailyLiquidatorAccount.save();

        usageDailySnapshot.dailyActiveLiquidators += 1;
        marketDailySnapshot.dailyActiveLiquidators += 1;
      }
    }
    if (liquidatee) {
      let liquidateeAccount = Account.load(liquidatee);
      // a new liquidatee
      if (
        liquidateeAccount == null ||
        liquidateeAccount.liquidationCount == 0
      ) {
        // liquidatee should already have positions & should not be new users
        const accountManager = new AccountManager(liquidatee);
        liquidateeAccount = accountManager.getAccount();

        protocol.cumulativeUniqueLiquidatees += 1;
        usageDailySnapshot.cumulativeUniqueLiquidatees += 1;
        market.cumulativeUniqueLiquidatees += 1;
      }

      liquidateeAccount.liquidationCount += INT_ONE;
      liquidateeAccount.save();

      const dailyLiquidateeAcctountID = "daily-liquidatee-"
        .concat(liquidatee)
        .concat("-")
        .concat(days);
      let dailyLiquidateeAccount = _ActiveAccount.load(
        dailyLiquidateeAcctountID
      );
      if (dailyLiquidateeAccount == null) {
        dailyLiquidateeAccount = new _ActiveAccount(dailyLiquidateeAcctountID);
        dailyLiquidateeAccount.save();

        usageDailySnapshot.dailyActiveLiquidatees += 1;
        marketDailySnapshot.dailyActiveLiquidatees += 1;
      }
    }
  }

  usageHourlySnapshot.hourlyTransactionCount += 1;
  usageDailySnapshot.dailyTransactionCount += 1;
  usageHourlySnapshot.blockNumber = event.block.number;
  usageDailySnapshot.blockNumber = event.block.number;
  usageHourlySnapshot.timestamp = event.block.timestamp;
  usageDailySnapshot.timestamp = event.block.timestamp;

  marketDailySnapshot.save();
  protocol.save();
  usageHourlySnapshot.save();
  usageDailySnapshot.save();
}

export function createTransactions(
  event: ethereum.Event,
  market: Market,
  lender: string | null,
  borrower: string | null,
  deltaCollateral: BigInt = BIGINT_ZERO,
  deltaCollateralUSD: BigDecimal = BIGDECIMAL_ZERO,
  deltaDebt: BigInt = BIGINT_ZERO,
  deltaDebtUSD: BigDecimal = BIGDECIMAL_ZERO
): void {
  if (deltaCollateral.gt(BIGINT_ZERO)) {
    // deposit
    const deposit = new Deposit(createEventID(event, Transaction.DEPOSIT));
    deposit.hash = event.transaction.hash;
    deposit.logIndex = event.logIndex.toI32();
    deposit.gasPrice = event.transaction.gasPrice;
    deposit.gasUsed = event.receipt ? event.receipt!.gasUsed : null;
    deposit.gasLimit = event.transaction.gasLimit;
    deposit.nonce = event.transaction.nonce;
    deposit.account = lender!;
    deposit.blockNumber = event.block.number;
    deposit.timestamp = event.block.timestamp;
    deposit.market = market.id;
    deposit.asset = market.inputToken;
    deposit.amount = deltaCollateral;
    deposit.amountUSD = deltaCollateralUSD;
    deposit.position = "";
    deposit.save();
  } else if (deltaCollateral.lt(BIGINT_ZERO)) {
    //withdraw
    const withdraw = new Withdraw(createEventID(event, Transaction.WITHDRAW));
    withdraw.hash = event.transaction.hash;
    withdraw.logIndex = event.logIndex.toI32();
    withdraw.gasPrice = event.transaction.gasPrice;
    withdraw.gasUsed = event.receipt ? event.receipt!.gasUsed : null;
    withdraw.gasLimit = event.transaction.gasLimit;
    withdraw.nonce = event.transaction.nonce;
    withdraw.account = lender!;
    withdraw.blockNumber = event.block.number;
    withdraw.timestamp = event.block.timestamp;
    withdraw.market = market.id;
    withdraw.asset = market.inputToken;
    withdraw.amount = deltaCollateral.times(BIGINT_NEGATIVE_ONE);
    withdraw.amountUSD = deltaCollateralUSD.times(BIGDECIMAL_NEG_ONE);
    withdraw.position = "";
    withdraw.save();
  }

  if (deltaDebt.gt(BIGINT_ZERO)) {
    // borrow
    const borrow = new Borrow(createEventID(event, Transaction.BORROW));
    borrow.hash = event.transaction.hash;
    borrow.logIndex = event.logIndex.toI32();
    borrow.gasPrice = event.transaction.gasPrice;
    borrow.gasUsed = event.receipt ? event.receipt!.gasUsed : null;
    borrow.gasLimit = event.transaction.gasLimit;
    borrow.nonce = event.transaction.nonce;
    borrow.account = borrower!;
    borrow.blockNumber = event.block.number;
    borrow.timestamp = event.block.timestamp;
    borrow.market = market.id;
    borrow.asset = ZAR_ADDRESS;
    borrow.amount = deltaDebt;
    borrow.amountUSD = deltaDebtUSD;
    borrow.position = "";
    borrow.save();
  } else if (deltaDebt.lt(BIGINT_ZERO)) {
    // repay
    const repay = new Repay(createEventID(event, Transaction.REPAY));
    repay.hash = event.transaction.hash;
    repay.logIndex = event.logIndex.toI32();
    repay.gasPrice = event.transaction.gasPrice;
    repay.gasUsed = event.receipt ? event.receipt!.gasUsed : null;
    repay.gasLimit = event.transaction.gasLimit;
    repay.nonce = event.transaction.nonce;
    repay.account = borrower!;
    repay.blockNumber = event.block.number;
    repay.timestamp = event.block.timestamp;
    repay.market = market.id;
    repay.asset = ZAR_ADDRESS;
    repay.amount = deltaDebt;
    repay.amountUSD = deltaDebtUSD;
    repay.position = "";
    repay.save();
  }
  // liquidate is handled by getOrCreateLiquidate() in getters
}

export function updatePriceForMarket(
  marketID: string,
  event: ethereum.Event
): void {
  // Price is updated for market marketID
  const market = getOrCreateMarket(marketID);
  const token = new TokenManager(market.inputToken.toLowerCase(), event);
  const tokenPrice = token.getPriceUSD();

  market.inputTokenPriceUSD = tokenPrice;
  market.totalDepositBalanceUSD = bigIntToBDUseDecimals(
    market.inputTokenBalance,
    token.getDecimals()
  ).times(market.inputTokenPriceUSD);
  market.totalValueLockedUSD = market.totalDepositBalanceUSD;
  market.save();

  // iterate to update protocol level totalDepositBalanceUSD
  const protocol = getOrCreateLendingProtocol();
  const marketIDList = protocol.marketIDList;
  let protocolTotalDepositBalanceUSD = BIGDECIMAL_ZERO;
  for (let i: i32 = 0; i < marketIDList.length; i++) {
    const marketAddress = marketIDList[i];
    const market = getOrCreateMarket(marketAddress);
    if (market == null) {
      log.warning("[updatePriceForMarket]market {} doesn't exist", [
        marketAddress,
      ]);
      continue;
    }
    protocolTotalDepositBalanceUSD = protocolTotalDepositBalanceUSD.plus(
      market.totalDepositBalanceUSD
    );
  }

  protocol.totalDepositBalanceUSD = protocolTotalDepositBalanceUSD;
  protocol.totalValueLockedUSD = protocol.totalDepositBalanceUSD;
  protocol.save();

  updateFinancialsSnapshot(event);
}

export function updateRevenue(
  event: ethereum.Event,
  marketID: string,
  newTotalRevenueUSD: BigDecimal = BIGDECIMAL_ZERO,
  newSupplySideRevenueUSD: BigDecimal = BIGDECIMAL_ZERO,
  protocolSideRevenueType: u32 = 0
): void {
  const market = getOrCreateMarket(marketID);
  if (market) {
    updateMarket(
      event,
      market,
      BIGINT_ZERO,
      BIGDECIMAL_ZERO,
      BIGINT_ZERO,
      BIGDECIMAL_ZERO,
      BIGINT_ZERO,
      BIGDECIMAL_ZERO,
      newTotalRevenueUSD,
      newSupplySideRevenueUSD
    );
  }

  updateProtocol(
    BIGDECIMAL_ZERO,
    BIGDECIMAL_ZERO,
    BIGDECIMAL_ZERO,
    newTotalRevenueUSD,
    newSupplySideRevenueUSD,
    protocolSideRevenueType
  );

  updateFinancialsSnapshot(
    event,
    BIGDECIMAL_ZERO,
    BIGDECIMAL_ZERO,
    BIGDECIMAL_ZERO,
    newTotalRevenueUSD,
    newSupplySideRevenueUSD,
    protocolSideRevenueType
  );
}

export function updateOrCreateUrn(
  owner: string,
  ilk: Bytes,
  event: ethereum.Event,
  dink: BigInt,
  dart: BigInt
): _Urn {
  const UrnID = owner.concat("-").concat(ilk.toHexString());
  const _ilk = getOrCreateIlk(ilk)!;
  if (!_ilk) {
    log.error("[updateOrCreateUrn] ilk Doesn't exist for urn. ilk: {}", [
      ilk.toHexString(),
    ]);
  }
  const market = getMarketFromIlk(ilk)!;
  if (!market) {
    log.error("[updateOrCreateUrn] market Doesn't exist for urn. ilk: {}", [
      ilk.toHexString(),
    ]);
  }

  let _urn = _Urn.load(UrnID);

  if (_urn == null) {
    _urn = new _Urn(UrnID);
    _urn.ownerAddress = owner;
    _urn.ilk = ilk.toHexString();
    _urn.collateralLocked = BIGDECIMAL_ZERO;
    _urn.normalizedDebt = BIGINT_ZERO;
    _urn.debt = BIGDECIMAL_ZERO;

    let _urns = _ilk._urns;
    if (_urns == null) {
      _urns = [];
    }
    _urns.push(UrnID);
    _ilk._urns = _urns;
    _ilk.save();
  }

  let collateralLocked = BIGDECIMAL_ZERO;
  let normalizedDebt = BIGINT_ZERO;
  let debt = BIGDECIMAL_ZERO;

  collateralLocked = _urn.collateralLocked.plus(
    dink.toBigDecimal().div(BIGDECIMAL_ONE_WAD)
  );
  normalizedDebt = _urn.normalizedDebt.plus(dart);
  debt = normalizedDebt
    .times(_ilk._rate)
    .toBigDecimal()
    .div(BIGDECIMAL_ONE_RAD);

  const liqudationRatio = _ilk.minimumCollateralizationRatio;

  const token = new TokenManager(_ilk.collateralToken, event);
  const collateralPriceTOMAN = token.getPriceTOMAN();
  const collateralPrice = collateralPriceTOMAN.div(BigDecimal.fromString("1000"));// in ZAR

  _urn.collateralLocked = collateralLocked;
  _urn.normalizedDebt = normalizedDebt;
  _urn.debt = debt;

  _urn.liquidationPrice =
    collateralLocked != BIGDECIMAL_ZERO
      ? debt.times(liqudationRatio).div(collateralLocked)
      : BIGDECIMAL_ZERO;

  _urn.collateralizationRatio =
    debt != BIGDECIMAL_ZERO
      ? collateralLocked.times(collateralPrice).div(debt)
      : BIGDECIMAL_ZERO;

  _urn.loanToValue =
    collateralLocked.times(collateralPrice) != BIGDECIMAL_ZERO
      ? debt.div(collateralLocked.times(collateralPrice))
      : BIGDECIMAL_ZERO;

  _urn.availableToWithdraw =
    collateralPrice != BIGDECIMAL_ZERO
      ? collateralLocked.minus(debt.times(liqudationRatio).div(collateralPrice))
      : BIGDECIMAL_ZERO;

  _urn.availableToMint =
    liqudationRatio != BIGDECIMAL_ZERO
      ? collateralLocked.times(collateralPrice).div(liqudationRatio).minus(debt)
      : BIGDECIMAL_ZERO;

  _urn.save();

  return _urn;
}
