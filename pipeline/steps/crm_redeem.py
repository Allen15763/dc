import pandas as pd
import numpy as np
# from datetime import datetime
from datetime import timedelta
import time

from .step import Step
from .step import StepException

"""
Read files and combine
Retalix only
dtype is necessary in reading file at beginning
"""

class CRM_REDEEM(Step):

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

        bo['Member Point Sub-type'] = bo['Member Point Sub-type'].cat.add_categories('missing').fillna({'Member Point Sub-type': 'missing'})
        bo.dropna(how='all')
        print('Finished Reading files')

        print('Inv first process')
        i, b, c = self.inv_process(inv_raw)
        print('Trans detail process')
        details = self.transaction_details(i, txn_raw, refund_raw, void_raw, b, item_raw)

        print('Grouping inv and merge with point movement and balance')
        i2 = self.inv_summarizing(i)
        p, lp = self.point_balance_process(point_bal_raw, L_point_bal_raw)
        i2, p, lp = self.inv_process2(i2, p, lp)
        i2, c = self.final_process(bo, c, i2)
        c = self.pivot_flags(c, i2, details)

        print('Exporting files')
        self.export_file(c, i, i2, details, p, lp)

        end = time.time()
        print('took', end - start, 'second')

    def inv_process(self, inv_raw):
        """
        extract related tans log with type 17 from INV, then generate new columns for point and unique code for join.
        """
        mask_redeem = inv_raw['MediaType'] == 17
        redeemlist = inv_raw[mask_redeem]['GlobalTxnID'].to_list()
        inv_raw = inv_raw[inv_raw['GlobalTxnID'].isin(redeemlist)]
        inv_raw['point'] = inv_raw['Tendered'] * 300
        inv_raw['for_100K_mapping_code'] = inv_raw['CardNo'].astype(str) + inv_raw['sale_date'].astype(str)

        """
        For daily redeem over 100K point, extract all relevant data from INV. (based on cardno, sales_date and type:17 point_sum)
        # completed the filter for >100K, extract their unique code to a list in applying filter. 
        """
        pivot_raw = inv_raw.loc(axis=1)['CardNo', 'sale_date', 'Tendered', 'point', 'MediaType']
        pivot_raw = pivot_raw[pivot_raw['MediaType'] == 17]
        pivot = pivot_raw.groupby(['CardNo', 'sale_date']).point.agg(['sum'])
        pivot['CardNo'] = pivot.index.get_level_values("CardNo")
        pivot['sale_date'] = pivot.index.get_level_values("sale_date")
        pivot = pivot.reset_index(drop=True)
        pivot['for_100K_mapping_code'] = pivot['CardNo'].astype(str) + pivot['sale_date'].astype(str)

        pivot = pivot[pivot['sum'] >= 100000]
        ten_thousand_list = pivot['for_100K_mapping_code'].to_list()

        # 在這個list的卡號的交易都給上標記(>100K)
        def ten(row):
            if row.for_100K_mapping_code in ten_thousand_list:
                row['TenCard'] = 'CardRedeem>100K'
            else:
                row['TenCard'] = 'NA'
            return row

        inv_raw = inv_raw.apply(ten, axis="columns")

        """
        For 80% redeem rate.
        based on GID to calculate each transaction's redeem amount, then divided by tran_tendered(整單金額)
        # using groupby.transform() may be better.
        df['totla_redeem'] = df[df['type'] == 17].groupby('GID')['point'].transform('sum')

        Next, only take data which flagged CardRedeem>100K as major dataset
        """
        rate_raw = inv_raw[inv_raw['MediaType'] == 17].groupby(['GlobalTxnID']).point.agg(['sum'])
        rate_raw['GlobalTxnID'] = rate_raw.index.get_level_values("GlobalTxnID")
        rate_raw = rate_raw.reset_index(drop=True)
        inv_raw = pd.merge(inv_raw, rate_raw, how='left', on='GlobalTxnID')
        inv_raw.rename(columns={'sum': 'Total_redeem'}, inplace=True)
        inv_raw['redeem_rate'] = ((inv_raw['Total_redeem'] / 300) / inv_raw['tran_tendered']).round(decimals=2)
        inv_raw.rename(columns={'TillID': '收銀機號', 'transaction_time': '交易時間', 'TransactionId': '交易序號',
                                'tran_tendered': '整單金額', 'OperatorID': '收銀員', 'Tendered': '金額'}, inplace=True)
        inv_raw = inv_raw[inv_raw['TenCard'] == 'CardRedeem>100K']


        """
        # 擷取只有>100K被標記資料，計算一天GID>10筆，標記；單一欄位做groupby用transform使其一開始就為一個欄位而非index
        """
        daily_over_10trans = inv_raw[inv_raw['TenCard'] == 'CardRedeem>100K']
        daily_over_10trans['GID_count'] = daily_over_10trans.groupby('for_100K_mapping_code')['GlobalTxnID'].transform('nunique')

        daily_over_10trans = daily_over_10trans[daily_over_10trans['GID_count'] >= 10]

        ten_trans_list = daily_over_10trans['for_100K_mapping_code'].to_list()
        # print('最終超過100K的獨特碼數量')
        # print(len(ten_trans_list))

        # def daily10trans(row):
        #     if row.for_100K_mapping_code in ten_trans_list:
        #         row['daily_trans >= 10'] = 'Y'
        #     else:
        #         row['daily_trans >= 10'] = 'N'
        #     return row
        # inv_raw = inv_raw.apply(daily10trans, axis="columns")
        inv_raw['daily_trans >= 10'] = np.where(inv_raw['for_100K_mapping_code'].isin(ten_trans_list), 'Y', 'N')

        inv_raw.drop(columns={'for_100K_mapping_code'}, inplace=True)
        mask_redeem2 = inv_raw['MediaType'] == 17
        redeemlist2 = inv_raw[mask_redeem2]['GlobalTxnID'].to_list()

        return inv_raw, redeemlist2, pivot

    def transaction_details(self, inv_raw, txn_raw, refund_raw, void_raw, redeemlist2, item_raw):
        """
        交易明細合併
        First, Txn append Refund, add void
        """
        txn_raw.drop(columns=['discount'], inplace=True)
        refund_raw.rename(columns={'RsGlobalTxnID': 'GlobalTxnID'}, inplace=True)
        transaction_log = txn_raw.append(refund_raw)

        void_raw.rename(columns={'transaction_time': '交易時間', 'TillID': '收銀機號', 'OperatorID': '收銀員', 'tran_tendered': '整單金額',
                                 'TransactionId': '交易序號'}, inplace=True)
        addvoid = pd.merge(inv_raw, void_raw, how='inner',
                           on=['store', '交易時間', 'CardNo', 'sale_date', '收銀機號', '收銀員', '整單金額',
                               '交易序號'])  # 8/13 add 交易序號 remove drop 交易序號
        addvoid.drop(columns=['MediaType', '金額', 'credit_card', 'point', 'TenCard', 'Total_redeem', 'redeem_rate', 'daily_trans >= 10'], inplace=True)
        addvoid.rename(columns={'交易時間': 'transaction_time', '收銀機號': 'TillID', '收銀員': 'OperatorID', '整單金額': 'tran_tendered', '交易序號': 'TransactionId'}, inplace=True)
        # 因為inv有拆列情形，時間序號等相同但金額不同而已，可能造成多對多重複列，故移除重複

        addvoid = addvoid[(addvoid['void_type'] == 'Transaction') | (addvoid['void_type'] == 'Item')].drop_duplicates()

        transaction_log = transaction_log.append(addvoid)
        """
        交易明細併入交易項目敘述，並以GID為key連結inv檔
        """
        transaction_log = pd.merge(transaction_log, item_raw, how='left', on='item_code')

        gidmask = transaction_log['GlobalTxnID'].isin(redeemlist2)
        transaction_log = transaction_log[gidmask]

        last_col = transaction_log.pop(transaction_log.columns[-9])
        transaction_log.insert(9, last_col.name, last_col)
        transaction_log.rename(columns={'TillID': '收銀機號', 'transaction_time': '交易時間',
                                        'TransactionId': '交易序號', 'tran_tendered': '整單金額',
                                        'OperatorID': '收銀員', 'discounted_price': '金額', 'Quantity': '數量'}, inplace=True)
        transaction_log.drop(columns=['stock_cost', 'soh_qty', 'price', 'cost', 'own_label', 'dept_code',
                                      'class_code', 'subclass_code', 'vendor_code', 'sup_code', 'sup_cname'], inplace=True)


        fill = [(transaction_log['item_code'] == '099998'), (transaction_log['item_code'] == '099997'),
                (transaction_log['item_code'] == '099996'),
                (transaction_log['item_code'] == '099995'), (transaction_log['item_code'] == '099988'),
                (transaction_log['item_code'] == '099984'),
                (transaction_log['item_code'] == '999999')]
        result_fill = ['統一編號', '愛心碼', '手機條碼', '預購取貨', '紀錄預訂原交易', '紀錄作廢交易', '寵愛點數折抵']
        transaction_log['item_cdesc'] = np.select(fill, result_fill, default=transaction_log['item_cdesc'])

        """
        該銷售日之卡號商品集中度與一天大於=10筆交易
        """
        transaction_log['u_code'] = transaction_log['CardNo'].astype(str) + transaction_log['sale_date'].astype(str)
        transaction_log['item_count'] = transaction_log.groupby("u_code")['item_code'].transform('count')
        transaction_log['item_nunique'] = transaction_log.groupby("u_code")['item_code'].transform('nunique')
        transaction_log['Concentration'] = (transaction_log['item_nunique'] / transaction_log['item_count']).round(decimals=2)
        transaction_log['Remark'] = np.where(transaction_log['Concentration'] > 0.5, 'abnormal', 'pass')

        transaction_log['trans_nunique'] = transaction_log.groupby("u_code")['GlobalTxnID'].transform('nunique')
        transaction_log['daily_trans >= 10'] = np.where(transaction_log['trans_nunique'] >= 10, 'Y', 'N')
        # transaction_log.drop(columns=['u_code', 'item_count', 'item_nunique', 'trans_nunique'], inplace=True)

        return transaction_log

    def inv_summarizing(self, inv_raw):
        """
        把inv有拆列情形資料彙總 e.g. type:17總共扣1200但原資料拆成999+201，此類做合併
        """
        inv_raw2 = inv_raw.fillna({'voucher_used': 0, 'credit_card': 0, 'Tendered': 0})
        inv_raw2.fillna('missing', inplace=True)
        inv_raw2 = inv_raw2.groupby(['store', 'sale_date', '收銀機號', '交易時間', '交易序號', 'GlobalTxnID', '收銀員',
                                     '整單金額', 'MediaType', 'CardNo', 'voucher_used', 'credit_card', 'TenCard',
                                     'Total_redeem', 'redeem_rate', 'daily_trans >= 10'], sort=False)['金額', 'point'].sum().reset_index()
        a = inv_raw2.pop(inv_raw2.columns[-2])
        b = inv_raw2.pop(inv_raw2.columns[-1])
        inv_raw2.insert(9, a.name, a)
        inv_raw2.insert(13, b.name, b)
        # inv_raw2['redeem_rate'] = inv_raw2['redeem_rate'].round(2)

        inv_raw2['CardNo2'] = inv_raw2['CardNo'].str.slice(7, 20).astype(str)  # [-13:]
        inv_raw2['CardNo2'] = np.where(inv_raw2['CardNo2'].str.startswith('027'), inv_raw2['CardNo2'].str.slice(2, 13),
                                       inv_raw2['CardNo2'].str.slice(1, 12))
        return inv_raw2

    def point_balance_process(self, point_bal_raw, L_point_bal_raw):
        """
        Add point balance on a daily basis   tentative version
        """
        def point_bal_process(x):
            x['Card Number2'] = x['Card Number'].str.slice(1, 12)
            x = x.drop_duplicates()
            x = x.dropna(axis=0, how='any', subset=["Card Number"])
            return x

        point_bal_raw = point_bal_process(point_bal_raw)
        L_point_bal_raw = point_bal_process(L_point_bal_raw)

        return point_bal_raw, L_point_bal_raw

    def inv_process2(self, inv_raw2, point_bal_raw, L_point_bal_raw):
        """
        本周點數餘額merge及相關點數餘額檔
        """
        merge = pd.merge(inv_raw2, point_bal_raw, how='left', left_on='CardNo2', right_on='Card Number2')
        merge.drop(columns=['Member Point.Member Point Balance', 'Member ID', 'Card Number', 'Card Number2'], inplace=True)
        merge.rename(columns={'Member.Member Point Balance': '點數餘額_本周'}, inplace=True)

        card_list = merge['CardNo2'].to_list()
        point_matched = point_bal_raw[point_bal_raw['Card Number2'].isin(card_list)]

        """
        以上述DF執行上周點數餘額merge及相關點數餘額檔
        """
        merge = pd.merge(merge, L_point_bal_raw, how='left', left_on='CardNo2', right_on='Card Number2')
        merge.drop(columns=['Member Point.Member Point Balance', 'Member ID', 'Card Number', 'Card Number2'], inplace=True)
        merge.rename(columns={'Member.Member Point Balance': '點數餘額_上周'}, inplace=True)

        L_point_matched = L_point_bal_raw[L_point_bal_raw['Card Number2'].isin(card_list)]
        return merge, point_matched, L_point_matched

    def final_process(self, bo, pivot, merge):
        """
        2021/6/24 add BO movement test
        """
        bo['Card Number2'] = bo['Card Number'].str.slice(1, 12)
        bo_inv = bo
        bo = bo.groupby(['Card Number2', 'Member Point Process Date'], sort=False)[
            '# of Member Point Redeemed'].sum().reset_index()

        # add info into pivot
        pivot_forbo = pivot
        pivot_forbo['CardNo2'] = pivot_forbo['CardNo'].str.slice(7, 20).astype(str)  # [-13:]
        pivot_forbo['CardNo2'] = np.where(pivot_forbo['CardNo2'].str.startswith('027'),
                                          pivot_forbo['CardNo2'].str.slice(2, 13),
                                          pivot_forbo['CardNo2'].str.slice(1, 12))
        pivot_forbo['sale_date D+1'] = pd.to_datetime(pivot_forbo['sale_date'].astype(str)) + timedelta(days=1)
        pivot_forbo['sale_date D+2'] = pd.to_datetime(pivot_forbo['sale_date'].astype(str)) + timedelta(days=2)
        pivot_forbo = pd.merge(pivot_forbo, bo, how='left', left_on=['CardNo2', 'sale_date D+1'],
                               right_on=['Card Number2', 'Member Point Process Date'])
        pivot_forbo = pd.merge(pivot_forbo, bo, how='left', left_on=['CardNo2', 'sale_date D+2'],
                               right_on=['Card Number2', 'Member Point Process Date'])


        # add info to Inv
        bo_inv = bo_inv.groupby('Card Number2')[
            '# of Member Point Accrual', '# of Member Point Redeemed'].sum().reset_index()
        merge = pd.merge(merge, bo_inv, how='left', left_on='CardNo2', right_on='Card Number2')
        merge.drop(columns='Card Number2', inplace=True)

        merge['異動率'] = ((merge.loc[:, '點數餘額_本周'] - merge['# of Member Point Accrual'] + merge['# of Member Point Redeemed'])
                        / merge.loc[:, '點數餘額_上周'])
        # merge['異動率'].fillna('NA', inplace=True)
        # "{:.3%}".format((merge['點數餘額_本周'] - merge['# of Member Point Accrual'] + merge['# of Member Point Redeemed']) / merge['點數餘額_上周'])
        return merge, pivot_forbo

    def pivot_flags(self, pivot_forbo, merge, transaction_log):
        print('pivot flag processing')
        pivot_forbo['weekday'] = pd.to_datetime(pivot_forbo.iloc[:, 2].astype(str)).dt.weekday
        pivot_forbo['check1'] = np.where((pivot_forbo['sum'] - pivot_forbo['# of Member Point Redeemed_x'] == 0)|(pivot_forbo['sum'] - pivot_forbo['# of Member Point Redeemed_y'] == 0)|
                                         (pivot_forbo['sum'] - pivot_forbo['# of Member Point Redeemed_x'] - pivot_forbo['# of Member Point Redeemed_y'] == 0),
                                         "OK", np.where(pivot_forbo['weekday'] == 6, "次期", "TBC"))

        merge = merge.drop_duplicates(subset='CardNo').groupby('CardNo')['點數餘額_本周', '# of Member Point Redeemed'].sum().reset_index()
        pivot_forbo = pd.merge(pivot_forbo, merge, how='left', on='CardNo')
        pivot_forbo['sum_redeem_bc'] = pivot_forbo.groupby('CardNo')['sum'].transform('sum')
        voidlist = transaction_log[transaction_log.loc[:, 'void_type'] == 'Transaction'].loc[:, 'CardNo'].to_list()
        pivot_forbo['voidtrans'] = np.where(pivot_forbo['CardNo'].isin(voidlist), 'Y', 'N')
        # col = series.apply(lambda x: (for x in list : 'Y')).fillna('N')
        pivot_forbo['Reason'] = np.where((pivot_forbo['check1'] == '次期')|(pivot_forbo['check1'] == 'OK'),pivot_forbo['check1'],
                                         np.where(pivot_forbo['# of Member Point Redeemed'] - pivot_forbo['sum_redeem_bc'] >= 0,
                                                  'DC扣點小於CRM', np.where(pivot_forbo['CardNo'].str.startswith('00000000277'),
                                                                        'DC錯誤卡號', np.where(pivot_forbo['voidtrans'] == 'Y', 'has void', 'TBC'))))

        pivot_forbo = pivot_forbo.drop(['sum_redeem_bc', 'for_100K_mapping_code', 'CardNo2', 'sale_date D+1', 'sale_date D+2',
                                        'Card Number2_x', 'Member Point Process Date_x', 'Card Number2_y', 'Member Point Process Date_y',
                                        'weekday'], axis=1)
        last_col = pivot_forbo.pop(pivot_forbo.columns[5])
        pivot_forbo.insert(pivot_forbo.shape[1]-1, last_col.name, last_col) # 必須是[0, n]之間的數，n是dataframe的欄位個數，不能負數倒數。shape[1]-1= -2
        return pivot_forbo

    def export_file(self, pivot_forbo, inv_raw, merge, transaction_log, point_matched, L_point_matched):
        """
        Export files in general
        """
        writer = pd.ExcelWriter('R_redeem_details.xlsx')
        pivot_forbo.to_excel(writer, sheet_name='pivot', index=False)
        inv_raw.to_excel(writer, sheet_name='inv_nonGroup', index=False)  # 沒有合併列版本
        merge.to_excel(writer, sheet_name='inv_Grouped', index=False)
        transaction_log.to_excel(writer, sheet_name='details', index=False)
        point_matched.to_excel(writer, sheet_name='related_point_bal', index=False)
        L_point_matched.to_excel(writer, sheet_name='L_related_point_bal', index=False)
        writer.save()
        writer.close()
