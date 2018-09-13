import os
import sys

import arrow
import pandas as pd

sys.path.insert(0, os.path.abspath('../'))

import abupy

from abupy.CrawlBu.ABuXqFile import map_stock_list_rom
from abupy.AlphaBu import ABuPickTimeExecute
from abupy.CoreBu import ABuEnv
from abupy import ABuSymbolPd, AbuSymbolCN, \
    EMarketDataFetchMode, ABuRegUtil, EMarketTargetType, \
    AbuFactorBuyBreak, AbuFactorSellBreak, AbuFactorAtrNStop, AbuFactorPreAtrNStop, AbuFactorCloseAtrNStop
from abupy import AbuBenchmark
from abupy import AbuCapital

# 从本地加载数据
ABuEnv.g_data_fetch_mode = EMarketDataFetchMode.E_DATA_FETCH_FORCE_LOCAL

# 目标市场为 A 股
ABuEnv.g_market_target = EMarketTargetType.E_MARKET_TARGET_CN

# 股票代码对应的公司信息
STOCK_INFOS = pd.read_csv(map_stock_list_rom('CN'), dtype=str)


def all_symbols():
    """
    获取 A 股所有股票
    """
    symbol = AbuSymbolCN()
    return symbol.all_symbol()


def get_symbol_info(symbol):
    return STOCK_INFOS[
        (STOCK_INFOS.symbol == symbol[2:]) &
        (STOCK_INFOS.market == symbol[:2].upper())]


def update_kl_data():
    """
    更新所有 A 股股票 K 线数据
    注意, 目前每次执行都会重新下载一遍
    """
    abupy.abu.run_kl_update(
        start='2011-08-08',
        end=arrow.now().strftime('%Y-%m-%d'),
        market=EMarketTargetType.E_MARKET_TARGET_CN,
        n_jobs=32)


def pick_stocks(symbols):
    """
    选股，最近 2 年上升, 且最近半年也上升
    """
    now = arrow.now('Asia/Shanghai')
    start = now.replace(days=-180).strftime('%Y-%m-%d')
    end = now.strftime('%Y-%m-%d')
    picks = []
    for i in symbols:
        kl_pd = ABuSymbolPd.make_kl_df(i, n_folds=2)
        if kl_pd is None:
            continue

        deg1 = ABuRegUtil.calc_regress_deg(kl_pd.close, show=False)
        if deg1 <= 0:
            continue

        kl_pd = ABuSymbolPd.make_kl_df(i, start=start, end=end)
        if kl_pd is None:
            continue

        deg2 = ABuRegUtil.calc_regress_deg(kl_pd.close, show=False)
        if deg1 * deg2 <= 0:
            continue

        picks.append(i)
    return picks


def buy_sell_signals(symbol):
    """
    买入和卖出信号
    """
    # buy_factors 60日向上突破，42日向上突破两个因子
    buy_factors = [
        {'xd': 60, 'class': AbuFactorBuyBreak},
        {'xd': 42, 'class': AbuFactorBuyBreak}]

    # 使用120天向下突破为卖出信号
    sell_factors = [
        {'xd': 120, 'class': AbuFactorSellBreak},
        {'stop_loss_n': 0.5, 'stop_win_n': 3.0, 'class': AbuFactorAtrNStop},
        {'class': AbuFactorPreAtrNStop, 'pre_atr_n': 1.0},
        {'class': AbuFactorCloseAtrNStop, 'close_atr_n': 1.5}]

    benchmark = AbuBenchmark()
    capital = AbuCapital(1000000, benchmark)
    orders_pd, action_pd, _ = ABuPickTimeExecute.do_symbols_with_same_factors(
        [symbol],
        benchmark,
        buy_factors,
        sell_factors,
        capital,
        show=True)


# if __name__ == '__main__':
#     symbols = all_symbols()
#     cs = pick_stocks(symbols)
