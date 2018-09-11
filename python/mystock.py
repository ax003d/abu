import os
import sys

import arrow

sys.path.insert(0, os.path.abspath('../'))

import abupy

from abupy.CoreBu import ABuEnv
from abupy import ABuSymbolPd, AbuPickRegressAngMinMax, AbuSymbolCN, \
    EMarketDataFetchMode, ABuRegUtil, AbuPickStockWorker, AbuKLManager, EMarketTargetType, AbuPickStockMaster
from abupy import AbuBenchmark
from abupy import AbuCapital

# 从本地加载数据
ABuEnv.g_data_fetch_mode = EMarketDataFetchMode.E_DATA_FETCH_FORCE_LOCAL

# 目标市场为 A 股
ABuEnv.g_market_target = EMarketTargetType.E_MARKET_TARGET_CN


def all_symbols():
    """
    获取 A 股所有股票
    """
    symbol = AbuSymbolCN()
    return symbol.all_symbol()


def update_kl_data():
    abupy.abu.run_kl_update(
        start='2011-08-08',
        end=arrow.now().strftime('%Y-%m-%d'),
        market=EMarketTargetType.E_MARKET_TARGET_CN,
        n_jobs=32)


def pick_stocks(symbols):
    """
    选股
    """
    stock_pickers = [
        {'class': AbuPickRegressAngMinMax,
         'threshold_ang_min': 0.0,
         'reversed': False,
         'xd': 504}]

    benchmark = AbuBenchmark(
        start='2016-09-10',
        end='2018-09-10')
    capital = AbuCapital(1000000, benchmark)
    return AbuPickStockMaster.do_pick_stock_with_process(
        capital, benchmark, stock_pickers, symbols)


def show_stock_reg(symbol, n_folds=2):
    kl_pd = ABuSymbolPd.make_kl_df(symbol, n_folds=n_folds)
    deg = ABuRegUtil.calc_regress_deg(kl_pd.close)
    print('选股周期内角度={}'.format(round(deg, 3)))


if __name__ == '__main__':
    pick_stocks(['sh600812'])
