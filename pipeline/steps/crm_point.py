import pandas as pd
import numpy as np
# import matplotlib.pyplot as plt
# import seaborn as sns
# from matplotlib.ticker import FuncFormatter
# from datetime import datetime
from datetime import timedelta
import time

from .step import Step
from .step import StepException

class CRM_POINT(Step):

    def process(self, data, inputs, utils):
        start = time.time()
        self.inputs = inputs['end_date']

        point_raw = utils.import_dc('Point')
        point_movement = utils.import_point_movement('point earned redeem.xlsx')
        inv_raw = utils.import_dc('Inv')
        txn_raw = utils.import_dc('Txn')
        item_raw = utils.import_dc('Items')
        refund_raw = utils.import_dc('Refund')
        void_raw = utils.import_dc('Void')
        print('Finished files reading')
        # point_bal_raw = utils.import_point_bal('Daily point balance')

        imp, p_o, i, pbi = self.inv_point_process(inv_raw, point_raw)
        imp = self.flag_500k_10trans(imp)
        imp, imp_bc = self.point_move_process(point_movement, imp)
        imp, o3g, o3c = self.flag_30k(imp)
        trans_d = self.trans_detail(txn_raw, refund_raw, void_raw, i, item_raw, o3c)
        trans_d, point_d = self.point_details(trans_d, p_o, o3g)
        imp, trans_d = self.reformat(imp, trans_d)
        self.export_file(imp, pbi, trans_d, point_d, imp_bc)

        end = time.time()
        print('took', end - start, 'second')

    def inv_point_process(self, inv_raw, point_raw):
        print('Inv & Point processing')
        inv_raw.fillna({'voucher_used': 0, 'credit_card': 0, 'Tendered': 0, 'MediaType': 'missing',
                        'CardNo': 'missing'}, inplace=True)
        print('INV na fill_after:', inv_raw.isnull().sum())
        inv = inv_raw.groupby(['store', 'sale_date', 'TillID', 'transaction_time', 'TransactionId', 'GlobalTxnID', 'OperatorID', 'tran_tendered',
                               'MediaType', 'CardNo', 'voucher_used', 'credit_card'], sort=False, observed=True)['Tendered'].sum().reset_index()
        """
        7/19updated
        """
        inv_formap = inv[inv['MediaType'] == 17][['GlobalTxnID', 'Tendered']]

        # inv_card = inv.groupby('CardNo')['Tendered'].sum().reset_index()
        inv_remove_media = inv_raw.groupby(['store', 'sale_date', 'TillID', 'transaction_time', 'TransactionId', 'GlobalTxnID', 'OperatorID',
                                            'tran_tendered', 'CardNo', 'voucher_used', 'credit_card'], sort=False, observed=True)['Tendered'].sum().reset_index()
        inv_remove_media = pd.merge(inv_remove_media, inv_formap, on='GlobalTxnID', how='left', sort=False,
                                    suffixes=['', '_折抵金額'])

        inv_remove_media.rename(columns={'Tendered_折抵金額': '折抵金額'}, inplace=True)
        move_col = inv_remove_media.pop(inv_remove_media.columns[-2])
        inv_remove_media.insert(8, move_col.name, move_col)
        inv_remove_media['折抵金額'].fillna(0, inplace=True)
        print('Before fill na in point:', point_raw.isnull().sum())

        # p_fill_cardno = point_raw.pop(point_raw.columns[-4])
        # p_fill_cardno = p_fill_cardno.cat.add_categories('missing').fillna('missing')
        # point_raw.insert(7, 'CardNo', p_fill_cardno)
        point_raw['CardNo'] = point_raw['CardNo'].cat.add_categories('missing').fillna('missing')

        point_original = point_raw.copy()
        print('After fill na in point:', point_original.isnull().sum())
        point_by_id = point_raw.groupby(['store', 'sale_date', 'TillID', 'transaction_time', 'TransactionId', 'GlobalTxnID', 'OperatorID',
                                         'tran_tendered', 'CardNo'], sort=False, observed=True)['points_earned'].sum().reset_index()

        inv_merge_point = pd.merge(inv_remove_media, point_by_id, how='left', on=['store', 'sale_date', 'TillID', 'transaction_time',
                                                                                  'TransactionId', 'GlobalTxnID', 'OperatorID', 'tran_tendered'])
        inv_merge_point['DIFF'] = inv_merge_point['points_earned'] - inv_merge_point['Tendered'] + inv_merge_point['voucher_used']

        # inv_merge_point['Checking'] = np.where(inv_merge_point['DIFF'] < -2, 'abnormal', 'pass')
        # print(inv_merge_point['points_earned'].sum())  # 會有point無卡號的退貨項目造成差異 minor
        inv_merge_point.rename(columns={'store': '門市', 'sale_date': '銷售認列日', 'TillID': '收銀機', 'transaction_time': '交易時間',
                                        'TransactionId': '交易序號', 'GlobalTxnID': 'GID', 'OperatorID': '收銀員', 'tran_tendered': '整單金額',
                                        'Tendered': '金額', 'points_earned': '賺點'}, inplace=True)

        # mask_drop = (inv_merge_point['CardNo_x'] != 'missing') & (inv_merge_point['賺點'].notnull())
        # inv_merge_point.drop(inv_merge_point[mask_drop], inplace=True)
        # print("Non Member trans",inv_merge_point[mask_drop])

        return inv_merge_point, point_original, inv, point_by_id

    def flag_500k_10trans(self, inv_merge_point):
        """
        update log on 2021/6/16
        For New Flags   daily cardno's earned point total > 500K  and  Daily Cardno's trans > 10
        """
        inv_merge_point['CardNo2'] = inv_merge_point['CardNo_x'].str.slice(7, 20).astype(str)
        inv_merge_point['CardNo2'] = np.where(inv_merge_point['CardNo2'].str.startswith('027'),
                                              inv_merge_point['CardNo2'].str.slice(2, 13),
                                              inv_merge_point['CardNo2'].str.slice(1, 12))

        inv_merge_point['unique_code'] = inv_merge_point['CardNo2'].astype(str) + inv_merge_point['銷售認列日'].astype(str)
        inv_merge_point['Daily earned by Card'] = inv_merge_point.groupby('unique_code')['賺點'].transform('sum')
        over500k = inv_merge_point[inv_merge_point['Daily earned by Card'] >= 500000]
        over500klist = over500k['unique_code'].to_list()
        inv_merge_point['Daily earned >= 500K'] = np.where(inv_merge_point['unique_code'].isin(over500klist), "Y", "N")

        inv_merge_point['Daily trans by Card'] = inv_merge_point.groupby('unique_code')['GID'].transform('nunique')
        over10trans = inv_merge_point[inv_merge_point['Daily trans by Card'] >= 10]
        over10translist = over10trans['unique_code'].to_list()
        inv_merge_point['Daily trans >= 10'] = np.where(inv_merge_point['unique_code'].isin(over10translist), "Y", "N")

        inv_merge_point.drop(columns='Daily trans by Card', inplace=True)
        inv_merge_point.rename(columns={'CardNo_x': 'CardNo_Inv', 'CardNo_y': 'CardNo_Point'}, inplace=True)

        return inv_merge_point

    def point_move_process(self, point_movement, inv_merge_point):
        """
        2021/6/24 update for new BO file
        """
        print("Bo file and vbo processing")
        point_movement['Member Point Sub-type'].cat.add_categories('missing').fillna('missing', inplace=True)
        point_movement.dropna(how='all')
        # 給INV_vs_point用的CRM累計
        point_movement['Card Number2'] = point_movement['Card Number'].str.slice(1, 12)
        point_movement_by_cardno = point_movement.groupby('Card Number2')['# of Member Point Accrual',
                                                                          '# of Member Point Redeemed'].sum().reset_index()
        inv_merge_point = pd.merge(inv_merge_point,
                                   point_movement_by_cardno[['Card Number2', '# of Member Point Accrual']],
                                   left_on='CardNo2', right_on='Card Number2', how='left', sort=False)

        # for BO by days match
        inv_merge_point_bycard = inv_merge_point.groupby(['CardNo2', '銷售認列日'], observed=True).賺點.agg(['sum'])
        inv_merge_point_bycard['CardNo2'] = inv_merge_point_bycard.index.get_level_values("CardNo2")
        inv_merge_point_bycard['銷售認列日'] = inv_merge_point_bycard.index.get_level_values("銷售認列日")
        inv_merge_point_bycard = inv_merge_point_bycard.reset_index(drop=True)
        print(inv_merge_point_bycard.count())
        inv_merge_point_bycard['銷售認列日 D+1'] = pd.to_datetime(inv_merge_point_bycard['銷售認列日'].astype(str)) + timedelta(days=1)
        inv_merge_point_bycard['銷售認列日 D+2'] = pd.to_datetime(inv_merge_point_bycard['銷售認列日'].astype(str)) + timedelta(days=2)

        inv_merge_point_bycard['u_code1'] = inv_merge_point_bycard['CardNo2'] + inv_merge_point_bycard[
            '銷售認列日 D+1'].astype(str)
        inv_merge_point_bycard['u_code2'] = inv_merge_point_bycard['CardNo2'] + inv_merge_point_bycard[
            '銷售認列日 D+2'].astype(str)

        point_movement = point_movement.groupby(['Card Number2', 'Member Point Process Date'], sort=False)['# of Member Point Accrual'].sum().reset_index()
        point_movement['u_code'] = point_movement['Card Number2'] + point_movement['Member Point Process Date'].astype(str)

        inv_merge_point_bycard = pd.merge(inv_merge_point_bycard, point_movement, how='left', left_on='u_code1', right_on='u_code')
        inv_merge_point_bycard = pd.merge(inv_merge_point_bycard, point_movement, how='left', left_on='u_code2', right_on='u_code')

        inv_merge_point_bycard.rename(columns={'# of Member Point Accrual_x': 'Point Accrual D+1', '# of Member Point Accrual_y': 'Point Accrual D+2'}, inplace=True)
        inv_merge_point_bycard['check1'] = np.where(inv_merge_point_bycard['sum'] != 0,
                                                    np.where((inv_merge_point_bycard['sum'] - inv_merge_point_bycard['Point Accrual D+1'] == 0) | (inv_merge_point_bycard['sum'] - inv_merge_point_bycard['Point Accrual D+2'] == 0), 'OK', 'TBC'),
                                                    'pass')
        inv_merge_point_bycard['weekday'] = pd.to_datetime(inv_merge_point_bycard['銷售認列日'].astype(str)).dt.weekday
        inv_merge_point_bycard['check2'] = np.where((inv_merge_point_bycard['check1'] == 'OK') | (inv_merge_point_bycard['check1'] == 'pass'), inv_merge_point_bycard['check1'], np.where(
            inv_merge_point_bycard['weekday'] == 6, '次期', 'TBC'))
        inv_merge_point_bycard = inv_merge_point_bycard.drop(['銷售認列日 D+1', '銷售認列日 D+2', 'u_code1', 'u_code2',
                                                              'Card Number2_x', 'Member Point Process Date_x',
                                                              'u_code_x', 'Card Number2_y',
                                                              'Member Point Process Date_y', 'u_code_y', 'weekday'], axis=1)
        return inv_merge_point, inv_merge_point_bycard

    def flag_30k(self, inv_merge_point):
        """
        2021/6/29 add flag 當日累積得點超過三萬點的卡號標註 & 檔筆交易比到的earned point 除 整單金額取得倍數。9/7修改為一天有超過三萬點的卡號，抓該卡號全部名細
        """
        over30k = inv_merge_point[inv_merge_point['Daily earned by Card'] >= 30000]
        """
        先確定一天有超過30K點的只標記當天的卡號，之後再把list蓋過去抓卡號所有明細。
        """
        over30klist = over30k['unique_code'].to_list()
        # over30klist_gid = over30k['GID'].to_list() # Original for GID search method
        # over30klist_cardno = over30k['CardNo_Inv'].to_list()
        inv_merge_point['Daily earned >= 30K'] = np.where(inv_merge_point['unique_code'].isin(over30klist), "Y", "N")

        over30klist = over30k['CardNo2'].to_list()  # 蓋掉之前的清單
        over30klist_gid = over30k['GID'].to_list()  # for point catching
        over30klist_cardno = over30k['CardNo2'].to_list()  # for trans log catching, CardNo_Inv to CardNo2 by 1013
        # inv_merge_point['Daily earned >= 30K'] = np.where(inv_merge_point['CardNo2'].isin(over30klist), "Y", "N")
        inv_merge_point.drop(columns=['unique_code', 'Daily earned by Card'], inplace=True)

        inv_merge_point['點數倍數'] = (inv_merge_point['賺點'] / (
                inv_merge_point['整單金額'] - inv_merge_point['折抵金額'] - inv_merge_point['voucher_used'])).round(3)
        # inv_merge_point = inv_merge_point[inv_merge_point['整單金額'] != 0] # Error，有本來整單金額就是零交易，而且有給點
        return inv_merge_point, over30klist_gid, over30klist_cardno

    def trans_detail(self, txn_raw, refund_raw, void_raw, inv, item_raw, over30klist_cardno):
        """
        7/19 added details
        """
        print('Processing details')
        txn_raw.drop(columns=['discount'], inplace=True)
        refund_raw.rename(columns={'RsGlobalTxnID': 'GlobalTxnID'}, inplace=True)
        transaction_log = txn_raw.append(refund_raw)

        addvoid = pd.merge(inv, void_raw, how='inner', on=['store', 'transaction_time', 'CardNo', 'sale_date', 'TillID', 'OperatorID',
                                                           'tran_tendered', 'TransactionId'])
        addvoid.drop(columns=['MediaType', 'Tendered', 'credit_card'], inplace=True)
        addvoid = addvoid[(addvoid['void_type'] == 'Transaction') | (addvoid['void_type'] == 'Item')].drop_duplicates()
        transaction_log = transaction_log.append(addvoid)

        transaction_log = pd.merge(transaction_log, item_raw, how='left', on='item_code')
        print('明細資訊_txn+refund+void')

        transaction_log['CardNo2'] = transaction_log['CardNo'].str.slice(7, 20).astype(str)
        transaction_log['CardNo2'] = np.where(transaction_log['CardNo2'].str.startswith('027'),
                                              transaction_log['CardNo2'].str.slice(2, 13),
                                              transaction_log['CardNo2'].str.slice(1, 12))

        transaction_log = transaction_log[transaction_log['CardNo2'].isin(over30klist_cardno)]

        transaction_log.drop(columns=['stock_cost', 'soh_qty', 'price', 'cost', 'dept_code',
                                      'class_code', 'subclass_code', 'vendor_code', 'sup_code', 'sup_cname',
                                      'own_label', 'CardNo2'], inplace=True)

        fill = [(transaction_log['item_code'] == '099998'), (transaction_log['item_code'] == '099997'),
                (transaction_log['item_code'] == '099996'),
                (transaction_log['item_code'] == '099995'), (transaction_log['item_code'] == '099988'),
                (transaction_log['item_code'] == '099984'),
                (transaction_log['item_code'] == '999999')]
        result_fill = ['統一編號', '愛心碼', '手機條碼', '預購取貨', '紀錄預訂原交易', '紀錄作廢交易', '寵愛點數折抵']
        transaction_log['item_cdesc'] = np.select(fill, result_fill, default=transaction_log['item_cdesc'])

        last_col = transaction_log.pop(transaction_log.columns[-1])
        transaction_log.insert(9, last_col.name, last_col)

        return transaction_log

    def point_details(self, transaction_log, point_original, over30klist_gid):
        """
        處理明細的PID備註
        先依據GID與PID兩層索引加總earned point(as_index=False)，再refer/merge到details sheet.
        """

        point_by_gid = point_original.loc[:, ['GlobalTxnID', 'promotion_id', 'prom_desc', 'points_earned']]
        point_by_gid_base = (point_by_gid[point_by_gid.loc[:, 'promotion_id'] == '10']).groupby('GlobalTxnID')['points_earned'].sum().reset_index()
        point_by_gid_promo = (point_by_gid[point_by_gid.loc[:, 'promotion_id'] != '10']).groupby('GlobalTxnID')['points_earned'].sum().reset_index()
        transaction_log = pd.merge(transaction_log, point_by_gid_base, on='GlobalTxnID', how="left", sort=False)

        transaction_log = pd.merge(transaction_log, point_by_gid_promo, on='GlobalTxnID', how="left", sort=False)


        # >30000 point details
        point_details_30k = point_original[point_original['GlobalTxnID'].isin(over30klist_gid)]
        return transaction_log, point_details_30k

    def reformat(self, inv_merge_point, transaction_log):
        """
        各表版面調整
        """
        inv_merge_point.drop(columns=['Card Number2'], inplace=True)
        inv_merge_point.rename(columns={'# of Member Point Accrual': 'CRM累計給點'}, inplace=True)
        # transaction_log.rename(columns={'points_earned_x': "based point", 'points_earned_y': 'promo point'}, inplace=True)
        transaction_log.rename(columns={'store': '門市', 'sale_date': '業績日', 'TillID': '收銀機', 'transaction_time': '交易時間',
                                        'TransactionId': '交易序號', 'GlobalTxnID': 'GID', 'tran_tendered': '整單金額',
                                        'OperatorID': '收銀員', 'item_code': '貨號', 'item_cdesc': '品名', 'Quantity': '數量',
                                        'discounted_price': '金額', 'CardNo': '卡號', 'voucher_used': '現金券',
                                        'void_type': '取消類型', 'points_earned_x': 'based point',
                                        'points_earned_y': 'promo point'}, inplace=True)
        return inv_merge_point, transaction_log

    def export_file(self, imp, pbi, trans_d, point_d, imp_bc):
        print("Exporting file")
        writer = pd.ExcelWriter(f'DC資料比對_inv_v_point_{self.inputs}_v4.xlsx')
        imp.to_excel(writer, sheet_name='inv_vs_point', index=False)
        pbi.to_excel(writer, sheet_name='point_per_trans', index=False)
        trans_d.to_excel(writer, sheet_name='details_o30k_bc', index=False)
        point_d.to_excel(writer, sheet_name='point_details_30K', index=False)
        imp_bc.to_excel(writer, sheet_name='vbo', index=False)
        # point_movement.to_excel(writer, sheet_name='point_movement', index=False)
        writer.save()
        writer.close()
