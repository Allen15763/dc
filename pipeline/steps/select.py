import pandas as pd
import numpy as np
import time

from .step import Step
from .step import StepException


class Select_Store(Step):
    def process(self, data, inputs, utils):
        start = time.time()
        self.inputs = inputs['storeid']
        self.inputs2 = inputs['date_period']

        txn_raw = utils.import_dc('Txn')
        item_raw = utils.import_dc('Items')
        refund_raw = utils.import_dc('Refund')
        void_raw = utils.import_dc('Void')
        inv_raw = utils.import_dc('Inv')

        v = self.fill_gid_tov(inv_raw, void_raw)
        t = self.merge_trvi(txn_raw, refund_raw, v, item_raw)
        self.export_file(self.reformat(self.tender_type(t, inv_raw)))

        end = time.time()
        print("took", end - start, "seconds")

    def fill_gid_tov(self, i, v):
        i.fillna({'TillID': 0, 'OperatorID': 0}, inplace=True)
        i['unique_code'] = i['store'].astype(str) + i['sale_date'].astype(str) + i['TillID'].astype(str) + i['transaction_time'].astype(str) + i['TransactionId'].astype(str) + i['OperatorID'].astype(str) + i['tran_tendered'].astype(str)
        i = i.loc[:, ['unique_code', 'GlobalTxnID']].drop_duplicates()
        v.loc[:, ['TillID', 'OperatorID']].fillna(0, inplace=True)
        v['unique_code'] = v['store'].astype(str) + v['sale_date'].astype(str) + v['TillID'].astype(int).astype(str) + v[
            'transaction_time'].astype(str) + v['TransactionId'].astype(str) + v['OperatorID'].astype(int).astype(str) + v[
                               'tran_tendered'].astype(str)
        v = pd.merge(v, i, how='left', on='unique_code')
        # f = lambda x['GlobalTxnID']: x['unique_code'] in i
        return v

    def merge_trvi(self, t, r, v, item):
        r.rename(columns={'RsGlobalTxnID': 'GlobalTxnID'}, inplace=True)
        t = t.append(r)

        # v['unique_code'] = v['store'].astype(str) + v['sale_date'].astype(str) + v['TillID'].astype(str) + v['transaction_time'].astype(str) + v['TransactionId'].astype(str) + v['OperatorID'].astype(str) + v['tran_tendered'].astype(str)
        # t_v_list = t.loc[:, 'unique_code'].value_counts(dropna=True).index.to_list()
        # v = v[v.loc[:, 'unique_code'].isin(t_v_list)]
        t = t.append(v)
        # INV收銀機跟收銀員讀成INT但TXN讀成FLOAT，這兩欄有空值不能直接轉int，要先填滿
        t.fillna({'TillID': 0, 'OperatorID': 0}, inplace=True)
        t['unique_code'] = t['store'].astype(str) + t['sale_date'].astype(str) + t['TillID'].astype(int).astype(str) + t[
            'transaction_time'].astype(str) + t['TransactionId'].astype(str) + t['OperatorID'].astype(int).astype(str) + t[
                               'tran_tendered'].astype(str)
        t = pd.merge(t, item, how='left', on='item_code')

        fill = [(t['item_code'] == '099998'), (t['item_code'] == '099997'), (t['item_code'] == '099996'), (t['item_code'] == '099995'),
                (t['item_code'] == '09988'), (t['item_code'] == '099984'), (t['item_code'] == '999999')]
        result_fill = ['統一編號', '愛心碼', '手機條碼', '預購取貨', '紀錄預訂原交易', '紀錄作廢交易', '寵愛點數折抵']
        t['item_cdesc'] = np.select(fill, result_fill, default=t['item_cdesc'])
        t = t[t['store'].astype(str).str.contains(self.inputs)]
        return t

    def tender_type(self, t, i):
        i.fillna({'TillID': 0, 'OperatorID': 0}, inplace=True)
        i['unique_code'] = i['store'].astype(str) + i['sale_date'].astype(str) + i['TillID'].astype(str) + i[
            'transaction_time'].astype(str) + i['TransactionId'].astype(str) + i['OperatorID'].astype(str) + i[
                               'tran_tendered'].astype(str)
        real_paid = {1: '現金', 2: '信用卡', 4: '悠遊卡', 7: 'LinePay', 51: 'WatsonsPay', 53: 'TaiwanPay', 59: '一卡通',
                     60: 'Pi錢包', 82: '振興券', 84: '支付寶', 89: '街口支付', 86: '玉山錢包行動支付'}
        paid_key = real_paid.keys()
        i = i[i.loc[:, 'MediaType'].isin(paid_key)].loc[:, ('unique_code', 'MediaType')].drop_duplicates(subset='unique_code') #單筆交易剛好使用2/12方式以上，笛卡兒積；設sublet
        t = pd.merge(t, i, how='left', on='unique_code')

        fill = [(t['MediaType'] == 1), (t['MediaType'] == 2),
                (t['MediaType'] == 4),
                (t['MediaType'] == 7), (t['MediaType'] == 51),
                (t['MediaType'] == 53),
                (t['MediaType'] == 59), (t['MediaType'] == 60),
                (t['MediaType'] == 82),
                (t['MediaType'] == 84), (t['MediaType'] == 89),
                (t['MediaType'] == 86)]
        result_fill = ['現金', '信用卡', '悠遊卡', 'LinePay', 'WatsonsPay', 'TaiwanPay', '一卡通', 'Pi錢包', '振興券', '支付寶', '街口支付',
                       '玉山錢包行動支付']
        t['付款方式'] = np.select(fill, result_fill)
        return t

    def reformat(self, t):
        t.drop(['stock_cost', 'soh_qty', 'cost', 'own_label', 'vendor_code', 'sup_code', 'sup_cname', 'dept_code', 'class_code',
                'subclass_code', 'unique_code'], axis=1, inplace=True)
        rename_col = {'store': '門市', 'sale_date': '認列日', 'TillID': '收銀機', 'transaction_time': '交易時間',
                      'TransactionId': '交易序號', 'tran_tendered': '整單金額', 'OperatorID': '收銀員',
                      'item_code': 'SKU', 'Quantity': '數量', 'price': '白標價', 'discounted_price': '價格',
                      'discount': '折扣', 'CardNo': '卡號', 'voucher_used': '折價券', 'item_cdesc': '品名'}
        t = t.rename(columns=rename_col)
        insert_col = t.pop(t.columns[-3])
        t.insert(9, insert_col.name, insert_col)
        t.sort_values(by=['交易時間', '交易序號'], ascending=True, inplace=True)
        return t

    def export_file(self, t):
        f = self.inputs.replace('|', '_')
        writer = pd.ExcelWriter(f'Store{f}明細_{self.inputs2}_v1.xlsx')
        t.to_excel(writer, sheet_name='txn', index=False)
        writer.save()
        writer.close()


class Select_CardNo(Step):
    def process(self, data, inputs, utils):
        start = time.time()
        self.inputs = inputs['card_no']