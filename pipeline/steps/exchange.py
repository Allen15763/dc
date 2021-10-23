import pandas as pd
import time

from .step import Step
from .step import StepException


class Exchange(Step):
    def process(self, data, inputs, utils):
        start = time.time()
        self.inputs = inputs['date_period']
        txn_raw = utils.import_dc('Txn')
        item_raw = utils.import_dc('Items')

        t = self.reformat(self.item_desc(self.zero_trans(txn_raw), item_raw))
        sta = self.descriptive_statistics(t)
        self.export_file(t, sta)

        end = time.time()
        print("took", end - start, "seconds")

    @staticmethod
    def zero_trans(t):
        t_mask = (t.loc[:, 'tran_tendered'] == 0) & (t.loc[:, 'Quantity'] == 0) #add No Void? S490後面接一筆網路商店取貨void 整單金額0.01
        # t_mask = t.loc[:, 'tran_tendered'] == 0
        t_list = t[t_mask].loc[:, 'GlobalTxnID'].to_list()
        t_exchange = t[t.loc[:, 'GlobalTxnID'].isin(t_list)]
        return t_exchange

    @staticmethod
    def item_desc(t_e, i):
        t_e = pd.merge(t_e, i, how="left", on="item_code")
        return t_e

    @staticmethod
    def reformat(t_e):
        drop_col = ['stock_cost', 'soh_qty', 'cost', 'own_label', 'vendor_code', 'sup_code', 'sup_cname',
                    'dept_code', 'class_code', 'subclass_code']
        rename_col = {'store': '門市', 'sale_date': '認列日', 'TillID': '收銀機', 'transaction_time': '交易時間',
                      'TransactionId': '交易序號', 'tran_tendered': '整單金額', 'OperatorID': '收銀員',
                      'item_code': 'SKU', 'Quantity': '數量', 'price': '白標價', 'discounted_price': '價格',
                      'discount': '折扣', 'CardNo': '卡號', 'voucher_used': '折價券', 'item_cdesc': '品名'}

        t_e.drop(columns=drop_col, inplace=True)
        insert_col = t_e.pop(t_e.columns[-1])
        t_e.insert(9, insert_col.name, insert_col)
        t_e.rename(columns=rename_col, inplace=True)
        t_e.sort_values(by=['交易時間', '交易序號'], ascending=True, inplace=True)
        return t_e

    @staticmethod
    def descriptive_statistics(t_e):
        t = t_e.iloc[:, 5].nunique()
        item_count = t_e.iloc[:, 8].value_counts(ascending=False).head(10).reset_index()
        store_count = t_e.groupby('門市').GlobalTxnID.agg('nunique').reset_index().sort_values(by='GlobalTxnID', ascending=False).head(10).reset_index(drop=True)
        total = {'Total_trans': t,}
        print(total)
        sta = pd.concat([item_count, store_count], axis=1)
        return sta

    def export_file(self, t_e, sta):
        writer = pd.ExcelWriter(f'換貨明細_{self.inputs}.xlsx')
        t_e.to_excel(writer, sheet_name='txn', index=False)
        sta.to_excel(writer, sheet_name='st', index=False)
        writer.save()
        writer.close()
