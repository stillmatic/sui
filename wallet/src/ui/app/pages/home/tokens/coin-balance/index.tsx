// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

import cl from 'classnames';
import { memo, useMemo } from 'react';
import { useIntl } from 'react-intl';
import { Coin } from '@mysten/sui.js';

import { useMiddleEllipsis } from '_hooks';

import st from './CoinBalance.module.scss';

export type CoinProps = {
    type: string;
    balance: bigint;
    hideStake?: boolean;
    mode?: 'row-item' | 'standalone';
};

function CoinBalance({ type, balance, mode = 'row-item' }: CoinProps) {
    const intl = useIntl();
    const { value, formatOptions, symbol } = useMemo(
        () => Coin.getFormatData(balance, type, 'accurate'),
        [balance, type]
    );
    const balanceFormatted = useMemo(
        () => intl.formatNumber(value, formatOptions),
        [intl, value, formatOptions]
    );
    console.log({ balance, value, formatOptions, symbol });
    const shortenType = useMiddleEllipsis(type, 30);
    return (
        <div className={cl(st.container, st[mode])}>
            <div className={cl(st.valuesContainer, st[mode])}>
                <span className={cl(st.value, st[mode])}>
                    {balanceFormatted}
                </span>
                <span className={cl(st.symbol, st[mode])}>{symbol}</span>
            </div>
            <div className={cl(st.typeActionsContainer, st[mode])}>
                {mode === 'row-item' ? (
                    <span className={st.type} title={type}>
                        {shortenType}
                    </span>
                ) : null}
            </div>
        </div>
    );
}

export default memo(CoinBalance);
