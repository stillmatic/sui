// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

import {
  getObjectFields,
  GetObjectDataResponse,
  SuiMoveObject,
  SuiObjectInfo,
  SuiObject,
  SuiData,
  getMoveObjectType,
} from './objects';
import { normalizeSuiObjectId, SuiAddress } from './common';

import BN from 'bn.js';
import { getOption, Option } from './option';
import { StructTag } from './sui-bcs';

export const COIN_PACKAGE_ID = '0x2';
export const COIN_MODULE_NAME = 'coin';
export const COIN_TYPE = `${COIN_PACKAGE_ID}::${COIN_MODULE_NAME}::Coin`;
export const COIN_SPLIT_VEC_FUNC_NAME = 'split_vec';
export const COIN_JOIN_FUNC_NAME = 'join';
export const COIN_DENOMINATIONS: Record<string, Record<string, number>> = {
  '0x2::sui::SUI': {
    'MIST': 1,
    'SUI': 1e9,
  },
} as const;
const COIN_TYPE_ARG_REGEX = /^0x2::coin::Coin<(.+)>$/;

type ObjectData = GetObjectDataResponse | SuiMoveObject | SuiObjectInfo;
type NumberFormatOptions = {
  notation?: 'standard' | 'compact',
  minimumFractionDigits?: number,
  maximumFractionDigits?: number,
};

type DenominationData = {symbol: string, value: number};
function getDenominationData(coinArgType: string, baseValue: bigint, denomination: 'auto' | string): DenominationData {
  if (denomination === 'auto' && COIN_DENOMINATIONS[coinArgType]) {
    const denominations = Object.entries(COIN_DENOMINATIONS[coinArgType]).sort(([_A, valA], [_B, valB]) => valA - valB);
    let selectedEntry = denominations[0];
    for (const anEntry of denominations) {
        if (baseValue * BigInt(100) >= anEntry[1]) {
          selectedEntry = anEntry;
          continue;
        }
        break;
    }
    return {
      symbol: selectedEntry[0],
      value: selectedEntry[1],
    };
  }
  if (denomination !== 'auto' && COIN_DENOMINATIONS[coinArgType]?.[denomination]) {
    return {
      symbol: denomination as string,
      value: COIN_DENOMINATIONS[coinArgType][denomination] as number,
    };
  }
  return {
    symbol: Coin.getCoinSymbol(coinArgType),
    value: 1,
  }
}

/**
 * Utility class for 0x2::coin
 * as defined in https://github.com/MystenLabs/sui/blob/ca9046fd8b1a9e8634a4b74b0e7dabdc7ea54475/sui_programmability/framework/sources/Coin.move#L4
 */
export class Coin {
  static isCoin(data: ObjectData): boolean {
    return Coin.getType(data)?.startsWith(COIN_TYPE) ?? false;
  }

  static getCoinTypeArg(obj: ObjectData) {
    const res = Coin.getType(obj)?.match(COIN_TYPE_ARG_REGEX);
    return res ? res[1] : null;
  }

  static isSUI(obj: ObjectData) {
    const arg = Coin.getCoinTypeArg(obj);
    return arg ? Coin.getCoinSymbol(arg) === 'SUI' : false;
  }

  static getCoinSymbol(coinTypeArg: string) {
    return coinTypeArg.substring(coinTypeArg.lastIndexOf(':') + 1);
  }

  static getCoinStructTag(coinTypeArg: string): StructTag {
    return {
      address: normalizeSuiObjectId(coinTypeArg.split('::')[0]),
      module: coinTypeArg.split('::')[1],
      name: coinTypeArg.split('::')[2],
      typeParams: [],
    };
  }

  static getBalance(
    data: GetObjectDataResponse | SuiMoveObject
  ): BN | undefined {
    if (!Coin.isCoin(data)) {
      return undefined;
    }
    const balance = getObjectFields(data)?.balance;
    return new BN.BN(balance, 10);
  }

  static getZero(): BN {
    return new BN.BN('0', 10);
  }

  // XXX: there are cases where we lose precision so don't use it for calculations
  // TODO: fix precision loss
  static getFormatData(
      baseValue: bigint,
      coinArgType: string,
      mode: 'accurate' | 'loose',
      denomination: 'auto' | string = 'auto'
    ): { value: number, symbol: string, formatOptions: NumberFormatOptions } {
      const denominationData = getDenominationData(coinArgType, baseValue, denomination);
      const adjValue = Number(baseValue) / denominationData.value;
      const formatOptions: NumberFormatOptions = {};
      if (mode === 'accurate') {
        formatOptions.maximumFractionDigits = 20;
      }
      if (mode === 'loose') {
        formatOptions.notation = 'compact';
        formatOptions.maximumFractionDigits = 2;
      }
    return {
      value: adjValue,
      symbol: denominationData.symbol,
      formatOptions,
    };
  }

  // XXX: there are cases where we lose precision so don't use it for calculations
  // TODO: fix precision loss
  static fromInput(inputValue: string, denomination: number): bigint {
    const value = parseFloat(inputValue);
    return BigInt(Math.round(value * denomination));
  }

  private static getType(data: ObjectData): string | undefined {
    if ('status' in data) {
      return getMoveObjectType(data);
    }
    return data.type;
  }
}

export type DelegationData = SuiMoveObject &
  Pick<SuiData, 'dataType'> & {
    type: '0x2::delegation::Delegation';
    fields: {
      active_delegation: Option<number>;
      delegate_amount: number;
      next_reward_unclaimed_epoch: number;
      validator_address: SuiAddress;
      info: {
        id: string;
        version: number;
      };
      coin_locked_until_epoch: Option<SuiMoveObject>;
      ending_epoch: Option<number>;
    };
  };

export type DelegationSuiObject = Omit<SuiObject, 'data'> & {
  data: DelegationData;
};

// Class for delegation.move
// see https://github.com/MystenLabs/fastnft/blob/161aa27fe7eb8ecf2866ec9eb192e768f25da768/crates/sui-framework/sources/governance/delegation.move
export class Delegation {
  public static readonly SUI_OBJECT_TYPE = '0x2::delegation::Delegation';
  private suiObject: DelegationSuiObject;

  public static isDelegationSuiObject(
    obj: SuiObject
  ): obj is DelegationSuiObject {
    return 'type' in obj.data && obj.data.type === Delegation.SUI_OBJECT_TYPE;
  }

  constructor(obj: DelegationSuiObject) {
    this.suiObject = obj;
  }

  public nextRewardUnclaimedEpoch() {
    return this.suiObject.data.fields.next_reward_unclaimed_epoch;
  }

  public activeDelegation() {
    return BigInt(getOption(this.suiObject.data.fields.active_delegation) || 0);
  }

  public delegateAmount() {
    return this.suiObject.data.fields.delegate_amount;
  }

  public endingEpoch() {
    return getOption(this.suiObject.data.fields.ending_epoch);
  }

  public validatorAddress() {
    return this.suiObject.data.fields.validator_address;
  }

  public isActive() {
    return this.activeDelegation() > 0 && !this.endingEpoch();
  }

  public hasUnclaimedRewards(epoch: number) {
    return (
      this.nextRewardUnclaimedEpoch() <= epoch &&
      (this.isActive() || (this.endingEpoch() || 0) > epoch)
    );
  }
}
