import pandas as pd
import numpy as np
# from datetime import datetime
# from datetime import timedelta

from .step import Step
from .step import StepException
import time

class Insufficient_Point(Step):
    def process(self, data, inputs, utils):
        start = time.time()

        bo = utils.import_point_movement('point earned redeem.xlsx')
        inv_raw = utils.import_dc('Inv')
        txn_raw = utils.import_dc('Txn')
        item_raw = utils.import_dc('Items')
        refund_raw = utils.import_dc('Refund')
        void_raw = utils.import_dc('Void')
        point_bal_raw = utils.import_point_bal('Daily point balance')
        L_point_bal_raw = utils.import_point_bal('LDaily point balance')

        bo.fillna({'Member Point Sub-type': 'missing'}, inplace=True)
        bo.dropna(how='all')
        print('Finished Reading files then Selecting Inv')

        inv_selected, redeemlist = self.inv_process(inv_raw)
        print('Finished inv_selected then Processing details and time count')

        transaction_log = self.trans_detail(txn_raw, refund_raw, void_raw, item_raw, inv_selected, redeemlist)

        inv_selected, e = self.times_calculate(inv_selected)
        print('Finished time counts then Processing point balance')

        merge = self.add_balance(inv_selected, point_bal_raw, L_point_bal_raw)
        print('Finished merge_BAL')
        merge = self.add_point_movement(bo, merge)
        print('Finished merge_movement')
        merge = self.adjustment(merge)
        print('ALL Finished then Exporting')
        self.export_files(merge, e, transaction_log)

        end = time.time()
        print('took', end - start, 'second')

    def inv_process(self, inv):
        """
        To select all redeem transactions and add point column by Tendered,
        Groupby
        :param inv_raw:
        :return: inv_selected
        """
        mask_redeem = inv['MediaType'] == 17
        redeemlist = inv[mask_redeem]['GlobalTxnID'].to_list()
        inv_selected = inv[inv['GlobalTxnID'].isin(redeemlist)]
        inv_selected['point'] = inv_selected['Tendered'] * 300

        inv_selected.fillna({'voucher_used': 0, 'credit_card': 0, 'Tendered': 0}, inplace=True)
        inv_selected.fillna('missing', inplace=True)
        inv_selected = inv_selected.groupby(
            ['store', 'sale_date', 'TillID', 'transaction_time', 'TransactionId', 'GlobalTxnID', 'OperatorID',
             'tran_tendered', 'MediaType', 'CardNo', 'voucher_used', 'credit_card'], sort=False)[
            'Tendered', 'point'].sum().reset_index()
        a = inv_selected.pop(inv_selected.columns[-2])
        b = inv_selected.pop(inv_selected.columns[-1])
        inv_selected.insert(9, a.name, a)
        inv_selected.insert(13, b.name, b)

        inv_selected['CardNo2'] = inv_selected['CardNo'].str.slice(7, 20).astype(str)  # [-13:]
        inv_selected['CardNo2'] = np.where(inv_selected['CardNo2'].str.startswith('027'),
                                           inv_selected['CardNo2'].str.slice(2, 14),
                                           inv_selected['CardNo2'].str.slice(1, 12))
        inv_selected['csum_redeem'] = inv_selected[inv_selected['MediaType'] == 17].groupby('CardNo2')[
            'point'].transform('sum')

        return inv_selected, redeemlist

    def times_calculate(self, x):
        """
        Parameters
        ----------
        x : TYPE DF and Groupby
            DESCRIPTION. Only for periodical trans counts

        Returns
        -------
        y : TYPE  DF
            DESCRIPTION. Save original inv_selected then return the counts of transctions during a sepecific period(30min)

        """
        y = x.copy()
        x = x.groupby(['transaction_time', 'GlobalTxnID', 'CardNo2'])['Tendered'].sum().reset_index()
        x['time'] = pd.to_datetime(x['transaction_time'].astype(str),
                                   format=('%Y-%m-%d %H:%M:%S'))  # F='Y-m-d' format=('%F %H:%M:%S')
        a = x.pop(x.columns[-1])
        x.insert(0, a.name, a)
        x.sort_values(by=['time', 'GlobalTxnID'])
        x = x.groupby([pd.Grouper(key='time', freq='30min'), 'CardNo2']).GlobalTxnID.count().reset_index()
        e = x.copy()
        x = x.loc[:, 'CardNo2':'GlobalTxnID'].groupby('CardNo2')['GlobalTxnID'].sum().reset_index().sort_values(
            by='GlobalTxnID', ascending=False).rename({'GlobalTxnID': '30minT'}, axis='columns')

        y = pd.merge(y, x, how='left', on='CardNo2')
        # x.sort_values(by='time')
        # x = x.resample('30min', on='time').count() #會變成每30分內有幾筆 沒辦法分卡號。 time變index
        # data.groupby([pd.Grouper(key='created_at', freq='M'), 'store_type']).price.sum().head(15)

        return y, e

    def add_balance(self, inv, bal_present, bal_last):
        """

        :param inv:
        :param bal:
        :return:
        """

        def point_bal_process(x):
            x['Card Number2'] = x['Card Number'].str.slice(1, 12)
            x = x.drop_duplicates()
            x = x.dropna(axis=0, how='any', subset=["Card Number"])
            return x

        bal_present = point_bal_process(bal_present)
        bal_last = point_bal_process(bal_last)

        merge = pd.merge(inv, bal_present, how='left', left_on='CardNo2', right_on='Card Number2')
        merge.drop(columns=['Member.Member Point Balance', 'Member ID', 'Card Number', 'Card Number2'],
                   inplace=True)
        merge.rename(columns={'Member.Member Point Balance': '點數餘額_本周'}, inplace=True)

        merge = pd.merge(merge, bal_last, how='left', left_on='CardNo2', right_on='Card Number2')
        merge.drop(columns=['Member.Member Point Balance', 'Member ID', 'Card Number', 'Card Number2'],
                   inplace=True)
        merge.rename(columns={'Member.Member Point Balance': '點數餘額_上周'}, inplace=True)
        return merge

    def add_point_movement(self, bo, merge):
        bo['Card Number2'] = bo['Card Number'].str.slice(1, 12)
        bo_grouped = bo.groupby('Card Number2')[
            '# of Member Point Accrual', '# of Member Point Redeemed'].sum().reset_index()
        merge = pd.merge(merge, bo_grouped, how='left', left_on='CardNo2', right_on='Card Number2')
        merge.drop(columns='Card Number2', inplace=True)
        return merge

    def trans_detail(self, t, r, v, item, inv_selected, redeemlist):
        t.drop(columns=['discount'], inplace=True)
        r.rename(columns={'RsGlobalTxnID': 'GlobalTxnID'}, inplace=True)
        transaction_log = t.append(r)

        inv_selected_c = inv_selected.drop(columns=['csum_redeem', 'CardNo2'])
        addvoid = pd.merge(inv_selected_c, v, how='inner',
                           on=['store', 'transaction_time', 'CardNo', 'sale_date', 'TillID', 'OperatorID',
                               'tran_tendered', 'TransactionId'])
        addvoid.drop(columns=['MediaType', 'Tendered', 'credit_card', 'point'], inplace=True)

        addvoid = addvoid[
            (addvoid['void_type'] == 'Transaction') | (addvoid['void_type'] == 'Item')].drop_duplicates()
        transaction_log = transaction_log.append(addvoid)
        """
        交易明細併入交易項目敘述，並以GID為key連結inv檔
        """
        transaction_log = pd.merge(transaction_log, item, how='left', on='item_code')
        # txn+refund+void

        gidmask = transaction_log['GlobalTxnID'].isin(redeemlist)
        transaction_log = transaction_log[gidmask]

        last_col = transaction_log.pop(transaction_log.columns[-9])
        transaction_log.insert(9, last_col.name, last_col)
        transaction_log.rename(columns={'TillID': '收銀機號', 'transaction_time': '交易時間',
                                        'TransactionId': '交易序號', 'tran_tendered': '整單金額',
                                        'OperatorID': '收銀員', 'discounted_price': '金額', 'Quantity': '數量'},
                               inplace=True)
        transaction_log.drop(columns=['stock_cost', 'soh_qty', 'price', 'cost', 'own_label', 'dept_code',
                                      'class_code', 'subclass_code', 'vendor_code', 'sup_code', 'sup_cname'],
                             inplace=True)

        fill = [(transaction_log['item_code'] == '099998'), (transaction_log['item_code'] == '099997'),
                (transaction_log['item_code'] == '099996'),
                (transaction_log['item_code'] == '099995'), (transaction_log['item_code'] == '099988'),
                (transaction_log['item_code'] == '099984'),
                (transaction_log['item_code'] == '999999')]
        result_fill = ['統一編號', '愛心碼', '手機條碼', '預購取貨', '紀錄預訂原交易', '紀錄作廢交易', '寵愛點數折抵']
        transaction_log['item_cdesc'] = np.select(fill, result_fill, default=transaction_log['item_cdesc'])
        return transaction_log

    def adjustment(self, merge):
        merge.fillna({'點數餘額_本周': 'missing', '點數餘額_上周': 'missing'}, inplace=True)
        merge['potential_neg'] = np.where(
            (merge['csum_redeem'] - merge['# of Member Point Redeemed'] > 0) & (merge['點數餘額_本周'] == "missing"),
            "potential_neg", "pass")
        return merge

    def export_files(self, df, e, transaction_log):
        writer = pd.ExcelWriter('R_redeem_neg_point.xlsx')
        df.to_excel(writer, sheet_name='inv_neg', index=False)
        e.to_excel(writer, sheet_name='30min', index=False)
        transaction_log.to_excel(writer, sheet_name='details', index=False)
        writer.save()
        writer.close()
