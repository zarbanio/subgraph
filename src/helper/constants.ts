import { Address, BigDecimal, dataSource, log, BigInt, Bytes } from "@graphprotocol/graph-ts";
import { _ActiveAccount } from "../../generated/schema";


/////////////////////////////
///// Protocol Specific /////
/////////////////////////////

export namespace Protocol {
  export const PROTOCOL = "Zarban";
  export const NAME = "Zarban-Subgraph";
  export const SLUG = "Zarban-Subgraph";
}


////////////////////////
///// Type Helpers /////
////////////////////////

export const ZERO_ADDRESS = "0x0000000000000000000000000000000000000000";

export const DEFAULT_DECIMALS = 18;

export const USDC_DECIMALS = 6;
export const USDC_DENOMINATOR = BigDecimal.fromString("1000000");

export const BIGINT_ZERO = BigInt.fromI32(0);
export const BIGINT_ONE = BigInt.fromI32(1);
export const BIGINT_TWO = BigInt.fromI32(2);
export const BIGINT_THREE = BigInt.fromI32(3);
export const BIGINT_SIX = BigInt.fromI32(6);
export const BIGINT_TWELVE = BigInt.fromI32(12);
export const BIGINT_THOUSAND = BigInt.fromI32(1000);
export const BIGINT_MAX = BigInt.fromString(
  "115792089237316195423570985008687907853269984665640564039457584007913129639935"
);
export const BIGINT_NEG_HUNDRED = BigInt.fromI32(-100);
export const BIGINT_NEGATIVE_ONE = BigInt.fromI32(-1);
export const BIGINT_HUNDRED = BigInt.fromI32(100);
export const BIGINT_THREE_HUNDRED = BigInt.fromI32(300);
export const BIGINT_TEN_TO_EIGHTEENTH = BigInt.fromString("10").pow(18);

//10^18
export const BIGINT_ONE_WAD = BigInt.fromString("10").pow(18);
// 10^27
export const BIGINT_ONE_RAY = BigInt.fromString("10").pow(27);
// 10^45
export const BIGINT_ONE_RAD = BigInt.fromString("10").pow(45);

export const INT_NEGATIVE_ONE = -1 as i32;
export const INT_ZERO = 0 as i32;
export const INT_ONE = 1 as i32;
export const INT_TWO = 2 as i32;
export const INT_THREE = 3 as i32;
export const INT_FOUR = 4 as i32;
export const INT_FIVE = 5 as i32;
export const INT_SIX = 6 as i32;
export const INT_NINE = 9 as i32;
export const INT_TEN = 10 as i32;
export const INT_SIXTEEN = 16 as i32;
export const INT_EIGHTTEEN = 18 as i32;
export const INT_THIRTY_TWO = 32 as i32;
export const INT_SIXTY_FOUR = 64 as i32;
export const INT_152 = 152 as i32;

export const BIGDECIMAL_ZERO = new BigDecimal(BIGINT_ZERO);
export const BIGDECIMAL_ONE = new BigDecimal(BIGINT_ONE);
export const BIGDECIMAL_TWO = new BigDecimal(BIGINT_TWO);
export const BIGDECIMAL_THREE = new BigDecimal(BIGINT_THREE);
export const BIGDECIMAL_SIX = new BigDecimal(BIGINT_SIX);
export const BIGDECIMAL_TWELVE = new BigDecimal(BIGINT_TWELVE);
export const BIGDECIMAL_ONE_HUNDRED = new BigDecimal(BigInt.fromI32(100));
export const BIGDECIMAL_ONE_THOUSAND = new BigDecimal(BigInt.fromI32(1000));
export const BIGDECIMAL_NEG_ONE = BigDecimal.fromString("-1");
export const BIGDECIMAL_ONE_WAD = BIGINT_ONE_WAD.toBigDecimal();
export const BIGDECIMAL_ONE_RAY = BIGINT_ONE_RAY.toBigDecimal();
export const BIGDECIMAL_ONE_RAD = BIGINT_ONE_RAD.toBigDecimal();
export const BIGDECIMAL_HUNDRED = new BigDecimal(BigInt.fromI32(100));
export const MAX_UINT = BigInt.fromI32(2).times(BigInt.fromI32(255));



export const WAD = 18 as i32;
export const RAY = 27 as i32;
export const RAD = 45 as i32;

export const INIT_LIQUIDITY_INDEX = BigInt.fromString(
  "1000000000000000000000000000"
);

export const BIGDECIMAL_NEG_ONE_CENT = BigDecimal.fromString("-0.01");

export const RAY_OFFSET = 27;

export const FLASHLOAN_PREMIUM_TOTAL = BigDecimal.fromString("0.0009"); // = 9/10000

// set assets price to their value at dec 2023
export const ZAR_INIT_PRICE_USD = BigDecimal.fromString("0.019")
export const DAI_INIT_PRICE_USD = BigDecimal.fromString("1")
export const ETH_INIT_PRICE_USD = BigDecimal.fromString("1850")
export const WSTETH_INIT_PRICE_USD = BigDecimal.fromString("2022")

/////////////////////
///// Date/Time /////
/////////////////////

export const SECONDS_PER_HOUR = 60 * 60; // 360
export const SECONDS_PER_DAY = 60 * 60 * 24; // 86400
export const SECONDS_PER_YEAR = 60 * 60 * 24 * 365;
export const SECONDS_PER_YEAR_BIGINT = BigInt.fromI32(60 * 60 * 24 * 365);
export const SECONDS_PER_YEAR_BIGDECIMAL = new BigDecimal(
  BigInt.fromI32(60 * 60 * 24 * 365)
);
export const MS_PER_DAY = new BigDecimal(BigInt.fromI32(24 * 60 * 60 * 1000));

/////////////////////
///// Date/Time /////
/////////////////////


export const ARBITRUM_BLOCKS_PER_YEAR = SECONDS_PER_YEAR / 1; // 1 = seconds per block.

////////////////////////////
///// Network Specific /////
////////////////////////////

export namespace Network {
  export const ARBITRUM_ONE = "ARBITRUM_ONE";
}

export class NetworkSpecificConstant {
  constructor(
    public readonly protocolAddress: Address, // aka, LendingPoolAddressesProvider
    public readonly network: string
  ) { }
}

export function getNetworkSpecificConstant(): NetworkSpecificConstant {
  const network = dataSource.network();
  if (equalsIgnoreCase(network, Network.ARBITRUM_ONE)) {
    return new NetworkSpecificConstant(
      Address.fromString("0x11804AC1d57B8b703AEDDCcE3dDB54844d123632"),
      Network.ARBITRUM_ONE
    );
  } else {
    log.error("[getNetworkSpecificConstant] Unsupported network: {}", [
      network,
    ]);
    return new NetworkSpecificConstant(Address.fromString(ZERO_ADDRESS), "");
  }
}

export function equalsIgnoreCase(a: string, b: string): boolean {
  return a.replace("-", "_").toLowerCase() == b.replace("-", "_").toLowerCase();
}



////////////////////////
///// Schema Enums /////
////////////////////////

// The enum values are derived from Coingecko slugs (converted to uppercase
// and replaced hyphens with underscores for Postgres enum compatibility)

export namespace ProtocolType {
  export const EXCHANGE = "EXCHANGE";
  export const LENDING = "LENDING";
  export const YIELD = "YIELD";
  export const BRIDGE = "BRIDGE";
  export const GENERIC = "GENERIC";
}

export namespace VaultFeeType {
  export const MANAGEMENT_FEE = "MANAGEMENT_FEE";
  export const PERFORMANCE_FEE = "PERFORMANCE_FEE";
  export const DEPOSIT_FEE = "DEPOSIT_FEE";
  export const WITHDRAWAL_FEE = "WITHDRAWAL_FEE";
}

export namespace LiquidityPoolFeeType {
  export const FIXED_TRADING_FEE = "FIXED_TRADING_FEE";
  export const TIERED_TRADING_FEE = "TIERED_TRADING_FEE";
  export const DYNAMIC_TRADING_FEE = "DYNAMIC_TRADING_FEE";
  export const PROTOCOL_FEE = "PROTOCOL_FEE";
}

// They are defined as u32 for use with switch/case
export namespace ProtocolSideRevenueType {
  export const STABILITYFEE: u32 = 1;
  export const LIQUIDATION: u32 = 2;
}
export namespace PositionSide {
  // export const LENDER = "LENDER";
  export const BORROWER = "BORROWER";
  export const COLLATERAL = "COLLATERAL";
}

export namespace EventType {
  export const DEPOSIT = "DEPOSIT";
  export const WITHDRAW = "WITHDRAW";
  export const BORROW = "BORROW";
  export const REPAY = "REPAY";
  export const LIQUIDATOR = "LIQUIDAOTR";
  export const LIQUIDATEE = "LIQUIDATEE";
}

export namespace ActivityType {
  export const DAILY = "DAILY";
  export const HOURLY = "HOURLY";
}


export namespace InterestRateMode {
  export const NONE = 0 as i32;
  export const STABLE = 1 as i32;
  export const VARIABLE = 2 as i32;
}

export namespace IzarbanTokenType {
  export const ZTOKEN = "ZTOKEN";
  export const INPUTTOKEN = "INPUTTOKEN";
  export const VTOKEN = "VTOKEN";
  export const STOKEN = "STOKEN";
}


export namespace LendingType {
  export const CDP = "CDP";
  export const POOLED = "POOLED";
}

export namespace PermissionType {
  export const WHITELIST_ONLY = "WHITELIST_ONLY";
  export const PERMISSIONED = "PERMISSIONED";
  export const PERMISSIONLESS = "PERMISSIONLESS";
  export const ADMIN = "ADMIN";
}

export namespace RiskType {
  export const GLOBAL = "GLOBAL";
  export const ISOLATED = "ISOLATED";
}

export namespace CollateralizationType {
  export const OVER_COLLATERALIZED = "OVER_COLLATERALIZED";
  export const UNDER_COLLATERALIZED = "UNDER_COLLATERALIZED";
  export const UNCOLLATERALIZED = "UNCOLLATERALIZED";
}

export namespace TokenType {
  export const REBASING = "REBASING";
  export const NON_REBASING = "NON_REBASING";
}

export namespace InterestRateType {
  export const STABLE = "STABLE";
  export const VARIABLE = "VARIABLE";
  export const FIXED = "FIXED";
}
export type InterestRateType = string;

export namespace InterestRateSide {
  export const LENDER = "LENDER";
  export const BORROWER = "BORROWER";
}

export namespace FeeType {
  export const LIQUIDATION_FEE = "LIQUIDATION_FEE";
  export const ADMIN_FEE = "ADMIN_FEE";
  export const PROTOCOL_FEE = "PROTOCOL_FEE";
  export const MINT_FEE = "MINT_FEE";
  export const WITHDRAW_FEE = "WITHDRAW_FEE";
  export const FLASHLOAN_PROTOCOL_FEE = "FLASHLOAN_PROTOCOL_FEE";
  export const FLASHLOAN_LP_FEE = "FLASHLOAN_LP_FEE";
  export const OTHER = "OTHER";
}


export namespace OracleSource {
  export const UNISWAP = "UNISWAP";
  export const BALANCER = "BALANCER";
  export const CHAINLINK = "CHAINLINK";
  export const YEARN = "YEARN";
  export const SUSHISWAP = "SUSHISWAP";
  export const CURVE = "CURVE";
}

export namespace TransactionType {
  export const DEPOSIT = "DEPOSIT";
  export const WITHDRAW = "WITHDRAW";
  export const BORROW = "BORROW";
  export const REPAY = "REPAY";
  export const LIQUIDATE = "LIQUIDATE";
  export const TRANSFER = "TRANSFER";
  export const FLASHLOAN = "FLASHLOAN";

  export const LIQUIDATOR = "LIQUIDATOR";
  export const LIQUIDATEE = "LIQUIDATEE";

  export const SWAP = "SWAP"; // Swap between interest rate types
}

export namespace AccountActivity {
  export const DAILY = "DAILY";
  export const HOURLY = "HOURLY";
}

export namespace RewardTokenType {
  export const DEPOSIT = "DEPOSIT";
  export const BORROW = "BORROW";
  export const VARIABLE_BORROW = "VARIABLE_BORROW";
  export const STABLE_BORROW = "STABLE_BORROW";
  export const STAKE = "STAKE";
}

export enum Transaction {
  DEPOSIT = 0,
  WITHDRAW = 1,
  BORROW = 2,
  REPAY = 3,
  LIQUIDATE = 4,
  TRANSFER = 5,
  FLASHLOAN = 6,
}


/////////////////////////////
/////        Math       /////
/////////////////////////////

export const mantissaFactor = 18;
export const cTokenDecimals = 8;
export const mantissaFactorBD = exponentToBigDecimal(mantissaFactor);
export const cTokenDecimalsBD = exponentToBigDecimal(cTokenDecimals);

// n => 10^n
export function exponentToBigDecimal(decimals: i32): BigDecimal {
  let result = BIGINT_ONE;
  const ten = BigInt.fromI32(10);
  for (let i = 0; i < decimals; i++) {
    result = result.times(ten);
  }
  return result.toBigDecimal();
}

// BigInt to BigDecimal
export function bigIntToBigDecimal(x: BigInt, decimals: i32): BigDecimal {
  return x.toBigDecimal().div(exponentToBigDecimal(decimals));
}

// bigDecimal to BigInt
export function bigDecimalToBigInt(x: BigDecimal): BigInt {
  return BigInt.fromString(x.truncate(0).toString());
}

//change number of decimals for BigDecimal
export function BDChangeDecimals(
  x: BigDecimal,
  from: i32,
  to: i32
): BigDecimal {
  if (to > from) {
    // increase number of decimals
    const diffMagnitude = exponentToBigDecimal(to - from);
    return x.times(diffMagnitude);
  } else if (to < from) {
    // decrease number of decimals
    const diffMagnitude = exponentToBigDecimal(from - to);
    return x.div(diffMagnitude);
  } else {
    return x;
  }
}

// insert value into arr at index
export function insert<Type>(
  arr: Array<Type>,
  value: Type,
  index: i32 = -1
): Array<Type> {
  if (arr.length == 0) {
    return [value];
  }
  if (index == -1 || index > arr.length) {
    index = arr.length;
  }
  const result: Type[] = [];
  for (let i = 0; i < index; i++) {
    result.push(arr[i]);
  }
  result.push(value);
  for (let i = index; i < arr.length; i++) {
    result.push(arr[i]);
  }
  return result;
}

// returns the increment to update the usage activity by
// 1 for a new account in the specified period, otherwise 0
export function activityCounter(
  account: string,
  transactionType: string,
  useTransactionType: boolean,
  intervalID: i32, // 0 = no intervalID
  marketID: string | null = null
): i32 {
  let activityID = account
    .concat("-")
    .concat(intervalID.toString());
  if (marketID) {
    activityID = activityID.concat("-").concat(marketID);
  }
  if (useTransactionType) {
    activityID = activityID.concat("-").concat(transactionType);
  }
  let activeAccount = _ActiveAccount.load(activityID);
  if (!activeAccount) {
    // if account / market only + transactionType is LIQUIDATEE
    // then do not count that account as it did not spend gas to use the protocol
    if (!useTransactionType && transactionType == TransactionType.LIQUIDATEE) {
      return INT_ZERO;
    }

    activeAccount = new _ActiveAccount(activityID);
    activeAccount.save();
    return INT_ONE;
  }

  return INT_ZERO;
}

/////////////////////////////
///// Protocol Specific /////
/////////////////////////////


export const VAT_ADDRESS =
  "0x975Eb113D580c44aa5676370E2CdF8f56bf3F99F".toLowerCase();
export const VOW_ADDRESS =
  "0xC56bbE915bCc665e6b3A293700caFf8296526061".toLowerCase();
export const DOG_ADDRESS =
  "0x4eB5a223B2c797Dcc13297B3C002225b1770d837".toLowerCase();
export const SPOT_ADDRESS =
  "0xcAc6896D91A21c502e92D77c6d54455594D2eB22".toLowerCase();
export const JUG_ADDRESS =
  "0xE959bfe924A175Cd7348b7435D33620ceDcfD7cC".toLowerCase();
export const ZAR_MARKET =
  "0x1b0ab2827c4d25b3387c1d1bc9c076fe0c7edfb9".toLowerCase();
export const ZAR_ADDRESS =
  "0xd946188A614A0d9d0685a60F541bba1e8CC421ae".toLowerCase();
export const DAI_ADDRESS =
  "0xda10009cbd5d07dd0cecc66161fc93d7c9000da1".toLowerCase();
export const ETH_ADDRESS =
  "0x82af49447d8a07e3bd95bd0d56f35241523fbab1".toLowerCase();
export const WSTETH_ADDRESS =
  "0x5979d7b546e38e414f7e9822514be443a4800529".toLowerCase();
export const WETH_GATEWAY_ADDRESS = "0xab46a03d8B82F6f8551d05FfA5E071cAfB313e5D".toLowerCase();

export const MIGRATION_ADDRESS = "0xc73e0383f3aff3215e6f04b0331d58cecf0ab849";
export const CAT_V1_ADDRESS = "0x78f2c2af65126834c51822f56be0d7469d7a523e";

// the first market, used to detect cat/dog contract
export const ILK_ETH_A =
  "0x4554482d41000000000000000000000000000000000000000000000000000000";
