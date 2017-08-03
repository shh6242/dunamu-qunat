# -*- coding: utf-8 -*-

import sys, os, datetime, time, urllib2, json, xlsxwriter, pickle
import pandas as pd
import numpy as np
import pandas.io.sql as psql
import MySQLdb as mysql
from bs4 import BeautifulSoup
from functools import wraps
import webbrowser
from tempfile import NamedTemporaryFile
from IPython.display import display

def get_calendar(conn, t0, t1):
    """"BETWEEN t0 - t1 inclusive"""
    sqltext = """
        select trd_dt
        from dunamu_calendar
        where trd_dt between '%s' and '%s'
        and open_gb_stock = '0';
    """ % (t0, t1)
    df = psql.read_sql(sqltext, conn)

    return df.trd_dt.tolist()


def get_trd_dt_list(conn, t0, t1, week_day):
    """"BETWEEN t0 - t1 inclusive"""
    sqltext = """
        select trd_dt
        from dunamu_calendar
        where trd_dt between '%s' and '%s'
        and open_gb_stock = '0'
        and WEEK_DAY = %s;
    """ % (t0, t1, week_day)
    df = psql.read_sql(sqltext, conn)

    return df.trd_dt.tolist()


def get_dunamu_sql_to_df(conn, t0, t1):
    """한방쿼리, r'%0'는 보통주만 고르려고 넣은 것"""
    sqltext = """
            SELECT factor.trd_dt, factor.gicode, factor.itemabbrnm, factor.market, factor.통계미래수익률1W as rtn_1W,
            factor.통계미래수익률1D as rtn_1D,
            lead.cap, lead.lead_investor, lead.demand_score, lead.score_foreign, lead.score_institution, lead.score_individual,
            hist.frg_amt, hist.ins_amt, hist.ind_amt, 
            jd.cls_prc_pday, jd.open_prc, jd.cls_prc
            from dunamu_factor factor
            join dunamu_lead_investor lead on (factor.trd_dt = lead.trd_dt and factor.gicode = lead.gicode)
            join dunamu_investor_hist hist on (factor.trd_dt = hist.trd_dt and factor.gicode = hist.gicode)
            join dunamu_stk_jd jd on (factor.trd_dt = jd.trd_dt and factor.gicode = jd.gicode)
            where (factor.trd_dt between '%s' and '%s')
            and factor.gicode like '%s'
            ORDER BY factor.gicode asc, factor.trd_dt asc;
            """ % (t0, t1, r'%0')
    df = psql.read_sql(sqltext, conn)
    return df


def RSI(x, rsi_window=14):
    dU, dD = x.copy(), x.copy()
    dU[dU < 0] = 0
    dD[dD > 0] = 0
    avgU = dU.rolling(rsi_window).mean()
    avgD = dD.rolling(rsi_window).mean().abs()
    rs = avgU / avgD
    rsi = 100 - (100/(1+rs))
    return rsi




if __name__ == '__main__':
    pd.set_option('display.expand_frame_repr', False)
    pd.set_option('display.unicode.east_asian_width', True)
    pd.set_option('display.unicode.ambiguous_as_wide', True)

    #############################SQL 접속용###############################
    host = 'confidential'
    id = "confidential"
    passwd = "passwd"
    database = "confidential"

    conn = mysql.connect(host, id, passwd, database, charset='utf8')
    #####################################################################
    DT_START = '20170301'
    DT_END = '20170705'
    WEEK_DAY = 4
    QUERY_LOOKBACK = 60

    PERIOD = 3
    NET_BUY_XD = 0.007
    NET_WIN = 20
    SLOW_WIN = 10
    FAST_WIN = 5
    SIG_WIN = 3

    # 포트폴리오 기록되는 곳 수정시 LAYOUT만 수정하면 됨.
    LAYOUT = ['trd_dt','gicode','itemabbrnm', 'market', 'cap',
              'lead_investor','demand_score','score_foreign', 'score_institution', 'score_individual',
              'RSI', 'signal', 'RSI_cross',
              'pcls_1D', 'open_1D', 'open_1W']
    SelectedPortfolio_df = pd.DataFrame(data=None,
                                        columns=['code', u'종목명', u'유형', u'시총', u'주도주체',
                                                 u'수급별점', u'외인', u'기관', u'개인',
                                                 'RSI','signal','RSI_cross',
                                                 u'종가1D', u'시가1D', u'시가1W'])

    # 캘린더 불러오기.
    calendar_list = sorted(get_calendar(conn, '20160101', DT_END))
    trd_dt_list = sorted(get_trd_dt_list(conn, DT_START, DT_END, WEEK_DAY))

    dt_lookback_start = calendar_list[calendar_list.index(trd_dt_list[0]) - QUERY_LOOKBACK]
    DT_LOOKBACK_START = dt_lookback_start.strftime('%Y%m%d')

    # SQL 데이터
    print 'MySql Query...'
    # SQLDF = (get_dunamu_sql_to_df(conn, DT_LOOKBACK_START, DT_END)
    #          .set_index('gicode')
    #          .sort_index())
    SQLDF = pd.read_pickle('testfile')
    print '불러오기 완료.'

    # ExcelWriter INPUTMODE
    PATH_SelectedPortfolio = u'SelectedPortfolio(%s%s).xlsx' % (WEEK_DAY,u'요일')
    WRITER_SelectedPortfolio = pd.ExcelWriter(path=PATH_SelectedPortfolio, engine='xlsxwriter')


    for dt_idx, trd_dt in enumerate(trd_dt_list):
        print '날짜 : %s' % trd_dt
        prev_dt = calendar_list[calendar_list.index(trd_dt) - 1]
        # 과거참조
        trailing_df = SQLDF[SQLDF.trd_dt < trd_dt]
        morning_df = SQLDF[SQLDF.trd_dt == trd_dt]
        # prev_dt_df: 전일(T-1) 장종료후 들어온 데이터
        prev_dt_df = trailing_df[trailing_df.trd_dt == prev_dt]


        # TODO 종목수 부족할 경우 여길 수정해보자...
        # 80점 이상  : 주도주체 순매수와 가격이 같이 가는 친구들
        over_80_mask = \
            (prev_dt_df.loc[:, ['score_foreign', 'score_institution', 'score_individual']]
             .apply(max, axis=1)) \
            >= 80
        prev_dt_df = prev_dt_df.loc[over_80_mask]
        over_80_gicodes = prev_dt_df.loc[over_80_mask].index.tolist()
        trailing_df = trailing_df.loc[over_80_gicodes]
        print '주도주체 스코어 80점 이상 종목수 : %d 종목' % (len(over_80_gicodes))

        # 시총 1500억 미만 제거
        cap_limit = prev_dt_df['cap'].ge(1.5e+11)
        prev_dt_df = prev_dt_df.loc[cap_limit]
        cap_gicodes = prev_dt_df.loc[cap_limit].index.tolist()
        trailing_df = trailing_df.loc[cap_gicodes].dropna()
        print '...중에서 전일종가기준 시가총액이 1500억 이상 종목수 : %d 종목' % (len(set(trailing_df.index.tolist())))

        # index 수정
        trailing_df = trailing_df.reset_index().set_index(['gicode','trd_dt'])
        # 5거래일 동안 주도주체 안변한 종목
        idx = pd.IndexSlice
        dt_5 = calendar_list[calendar_list.index(trd_dt) - 5].strftime('%Y%m%d')

        look = (trailing_df.loc[idx[:, dt_5:prev_dt.strftime('%Y%m%d')], :])
        consistent_mask = look.lead_investor.groupby('gicode').apply(lambda x: len(x.unique()) == 1)
        prev_dt_df = prev_dt_df.loc[consistent_mask]
        trailing_df = trailing_df.loc[prev_dt_df.index.tolist()].dropna()
        print '...5거래일 간 주도주체 불변 종목수 : %d 종목' % (len(set(prev_dt_df.index.tolist())))

        # 3거래일간...
        dt_3 = calendar_list[calendar_list.index(trd_dt) - 3].strftime('%Y%m%d')
        # # 3거래일간 수급별점 단조증가
        #
        # look = (trailing_df.loc[idx[:, dt_3:prev_dt.strftime('%Y%m%d')], :]).copy()
        # mono_demand = look.demand_score.groupby('gicode').apply(lambda x: x.is_monotonic_increasing)
        # prev_dt_df = prev_dt_df.loc[mono_demand]
        # trailing_df = trailing_df.loc[prev_dt_df.index.tolist()].dropna()
        #
        # # 전일 수급별점 4이상
        # star_demand = prev_dt_df.demand_score.gt(3.0)
        # prev_dt_df = prev_dt_df.loc[star_demand]
        # trailing_df = trailing_df.loc[prev_dt_df.index.tolist()].dropna()

        # 3거래일간 주도주체 스코어 단조 증가(감소한 적 없음)
        mp_view = (trailing_df.loc[idx[:, dt_3:prev_dt.strftime('%Y%m%d')], :]).copy()
        mp_view['mp_score'] = mp_view.score_foreign.copy()
        mp_view['mp_score'] = mp_view.score_institution.where(mp_view.lead_investor == 'INSTITUTION', mp_view.mp_score)
        mp_view['mp_score'] = mp_view.score_individual.where(mp_view.lead_investor == 'INDIVIDUAL', mp_view.mp_score)

        mono_mp = mp_view.mp_score.groupby('gicode').apply(lambda x: x.is_monotonic_increasing)
        prev_dt_df = prev_dt_df.loc[mono_mp]
        trailing_df = trailing_df.loc[prev_dt_df.index.tolist()].dropna()
        print '...3거래일 간 주도주체 스코어 꾸준히 증가한 종목수 : %d 종목' % (len(set(prev_dt_df.index.tolist())))

        # T-3 대비 20%이상 기상승 종목 제외
        # look = (trailing_df.loc[idx[:, dt_3:prev_dt.strftime('%Y%m%d')], 'cls_prc']).copy()
        # soar_mask = look.groupby('gicode').apply(lambda x: (x[2] / x[0]) >= 1.2)
        # prev_dt_df = prev_dt_df.loc[~soar_mask]
        # try:
        #     trailing_df = trailing_df.loc[prev_dt_df.index.tolist()].dropna()
        # except:
        #     pass

        # RSI analysis
        dt_30 = calendar_list[calendar_list.index(trd_dt) - 35].strftime('%Y%m%d')
        look = trailing_df.loc[idx[:, dt_30:prev_dt.strftime('%Y%m%d')], :].copy()
        # prev_dt_df 에서 전일 주도주체를 구해, 해당 주체의 RSI를 구함
        rsi_view = look.loc[:, ['lead_investor','frg_amt','ins_amt','ind_amt']]
        rsi_view['mp'] = prev_dt_df.lead_investor.reindex(index=rsi_view.index, level=0)
        rsi_view['mp_amt'] = rsi_view.frg_amt
        rsi_view['mp_amt'] = rsi_view.ins_amt.where(rsi_view.mp == 'INSTITUTION', rsi_view.mp_amt)
        rsi_view['mp_amt'] = rsi_view.ind_amt.where(rsi_view.mp == ' INDIVIDUAL', rsi_view.mp_amt)

        rsi_view['mp_score'] = look.score_foreign
        rsi_view['mp_score'] = look.score_institution.where(look.lead_investor == 'INSTITUTION', rsi_view.mp_score)
        rsi_view['mp_score'] = look.score_individual.where(look.lead_investor == 'INDIVIDUAL', rsi_view.mp_score)

        rsi_view['RSI'] = rsi_view.mp_amt.groupby('gicode').apply(RSI, rsi_window=20)
        rsi = pd.DataFrame({'mp_score': rsi_view.mp_score, 'RSI': rsi_view.RSI},columns=['mp_score', 'RSI'])
        rsi['signal'] = rsi.RSI.groupby('gicode').apply(lambda x: x.rolling(5).mean())

        rsi['RSI_cross'] = np.where(rsi.RSI > rsi.signal, 1, 0)
        rsi['RSI_cross'] = rsi['RSI_cross'] - rsi['RSI_cross'].shift(1)

        rsi_prev = rsi.loc[idx[:,prev_dt],:].reset_index(level=1,drop=True).copy()

        prev_dt_df = prev_dt_df.join(rsi_prev)

        # 전일 RSI 범위 30 이상
        rsi_cond = prev_dt_df.RSI.gt(30)
        prev_dt_df = prev_dt_df.loc[rsi_cond]
        trailing_df = trailing_df.loc[prev_dt_df.index.tolist()].dropna()
        print '...전일 RSI 30이상 종목수 : %d 종목' % (len(set(prev_dt_df.index.tolist())))

        # # 전일 RSI 범위 70 이하
        # rsi_cond = prev_dt_df.RSI.lt(70)
        # prev_dt_df = prev_dt_df.loc[rsi_cond]
        # trailing_df = trailing_df.loc[prev_dt_df.index.tolist()].dropna()

        # 전일 RSI 크로스
        rsix = prev_dt_df.RSI_cross == 1
        prev_dt_df = prev_dt_df.loc[rsix]
        trailing_df = trailing_df.loc[prev_dt_df.index.tolist()].dropna()
        print '...전일 RSI 시그널 상향돌파 발생 종목수 : %d 종목' % (len(set(prev_dt_df.index.tolist())))


        # 전후 5일
        try:
            flow_df = SQLDF[(SQLDF.trd_dt <= calendar_list[calendar_list.index(trd_dt) + 5])
                            & (SQLDF.trd_dt >= calendar_list[calendar_list.index(trd_dt) - 5])]
            flow_df = flow_df.loc[prev_dt_df.index.tolist()].dropna()
        except:
            pass

        candidate_df = prev_dt_df
        candidate_df.trd_dt = trd_dt

        # 수익률 조정
        today_rtn = morning_df.loc[prev_dt_df.index.tolist(), 'rtn_1D'].dropna()
        # 당일 가격정보 전일 종가, 당일시가, 당일종가
        prc = morning_df.loc[prev_dt_df.index.tolist(),
                             ['cls_prc_pday', 'open_prc', 'cls_prc']]
        # # 전일 종가 대비 당일 종가 수익률
        pcls_1D = prc['cls_prc'].divide(prc['cls_prc_pday']).sub(1)
        candidate_df.loc[:, 'pcls_1D'] = pcls_1D
        # 당일 시가 대비 당일 종가 수익률
        open_1D = prc['cls_prc'].divide(prc['open_prc']).sub(1)
        candidate_df.loc[:, 'open_1D'] = open_1D
        # 당일 시가 대비 1주일 후 종가 수익률(예: 금요일 시가 -> 다음주 목요일 종가)
        open_1W = candidate_df['rtn_1W'].add(1).multiply(prc['cls_prc_pday']).divide(prc['open_prc']).sub(1)
        candidate_df.loc[:, 'open_1W'] = open_1W

        # 디스플레이
        candidate_df = candidate_df.drop('rtn_1W', axis=1)
        candidate_df = candidate_df.drop('rtn_1D', axis=1)
        candidate_df.loc[:, ['pcls_1D', 'open_1D', 'open_1W']] \
            = candidate_df.loc[:, ['pcls_1D', 'open_1D', 'open_1W']].round(4)
        candidate_df.loc[:, 'cap'] = candidate_df.loc[:, 'cap'].round(-10)
        candidate_df.loc[:, ['demand_score', 'score_foreign', 'score_institution', 'score_individual']] = \
            candidate_df.loc[:, ['demand_score', 'score_foreign', 'score_institution', 'score_individual']].astype(int)

        candidate_df = candidate_df.reset_index().loc[:, LAYOUT].drop('trd_dt', axis=1)
        candidate_df.columns = ['code', u'종목명', u'유형', u'시총', u'주도주체',
                                u'수급별점', u'외인', u'기관', u'개인',
                                'RSI','signal','RSI_cross',
                                u'종가1D', u'시가1D', u'시가1W']

        display(candidate_df)
        print candidate_df.loc[:,u'시가1W'].describe()
        try:
            best_i = candidate_df.loc[:, 'RSI'].sub(candidate_df.loc[:, 'signal']).idxmax()
            SelectedPortfolio_df.loc[trd_dt] = candidate_df.loc[best_i, :]
            print SelectedPortfolio_df.iloc[len(SelectedPortfolio_df) -1]
        except:
            SelectedPortfolio_df.loc[trd_dt] = np.nan


        candidate_df.to_excel(WRITER_SelectedPortfolio, sheet_name=u'%s_후보종목' % trd_dt)
        try:
            flow_df.to_excel(WRITER_SelectedPortfolio, sheet_name=u'%s_추이' % trd_dt)
        except:
            pass
        del candidate_df


    SelectedPortfolio_df[u'누적수익률'] = SelectedPortfolio_df[u'시가1W'].add(1).cumprod(axis=0)
    print SelectedPortfolio_df
    SelectedPortfolio_df.to_excel(WRITER_SelectedPortfolio, sheet_name=u'포트폴리오결과')
    WRITER_SelectedPortfolio.save()