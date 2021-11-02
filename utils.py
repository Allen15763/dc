import os
import pandas as pd
import numpy as np


class Utils:
    def __init__(self):
        pass

    def import_dc(self, x):
        dfs = []
        if x == 'Inv':
            for root, dirs, files in os.walk('.'):
                for file in files:
                    if file.startswith(x):
                        print(file)
                        if file.startswith('~$'):
                            continue
                        else:
                            df = pd.read_csv(file, dtype=self.def_type_inv, encoding='utf-8', low_memory=False)
                            dfs.append(df)
            y = pd.concat(dfs, ignore_index=True)
        elif x == 'Txn':
            for root, dirs, files in os.walk('.'):
                for file in files:
                    if file.startswith(x):
                        print(file)
                        if file.startswith('~$'):
                            continue
                        else:
                            df = pd.read_csv(file, dtype=self.def_type_txn, encoding='utf-8', low_memory=False)
                            dfs.append(df)
            y = pd.concat(dfs, ignore_index=True)
        elif x == 'Point':
            for root, dirs, files in os.walk('.'):
                for file in files:
                    if file.startswith(x):
                        print(file)
                        if file.startswith('~$'):
                            continue
                        else:
                            df = pd.read_csv(file, dtype=self.def_type_point, encoding='utf-8', low_memory=False)
                            dfs.append(df)
            y = pd.concat(dfs, ignore_index=True)
        elif x == 'Refund':
            for root,dirs,files in os.walk('.'):
                for file in files:
                    if file.startswith(x):
                        print(file)
                        if file.startswith('~$'):
                            continue
                        else:
                            df = pd.read_csv(file, dtype=self.def_type_refund, encoding='utf-8', low_memory=False)
                            dfs.append(df)
            y = pd.concat(dfs,ignore_index=True)
        elif x == 'Void':
            for root,dirs,files in os.walk('.'):
                for file in files:
                    if file.startswith(x):
                        print(file)
                        if file.startswith('~$'):
                            continue
                        else:
                            df = pd.read_csv(file, dtype=self.def_type_void, encoding='utf-8', low_memory=False)
                            dfs.append(df)
            y = pd.concat(dfs,ignore_index=True)
        elif x == 'Items':
            for root, dirs, files in os.walk('.'):
                for file in files:
                    if file.startswith(x):
                        print(file)
                        if file.startswith('~$'):
                            continue
                        else:
                            df = pd.read_csv(file, dtype='category', encoding='utf-8')
                            dfs.append(df)
            y = pd.concat(dfs, ignore_index=True)
        else:
            return print("Function:import_dc Can't identify parameter")

        return y

    def import_point_bal(self, x):
        point = []
        for root, dirs, files in os.walk('.'):
            for file in files:
                if file.startswith(x):
                    print(file)
                    if file.startswith('~$'):
                        continue
                    elif file.startswith('sum'):  # 排除已有檔案
                        continue
                    else:
                        df = pd.read_excel(file, dtype=self.def_type_point_bal, engine='openpyxl', skiprows=3, usecols='B:E')
                        point.append(df)
        y = pd.concat(point, ignore_index=True)
        return y

    def import_point_movement(self, x):
        dfs = []
        for root, dirs, files in os.walk('.'):
            for file in files:
                if file.endswith(x):
                    print(file)
                    if file.startswith('~$'):
                        continue
                    else:
                        df = pd.read_excel(file, dtype=self.def_type_point_movement, engine='openpyxl', skiprows=3, usecols='B:H')
                        dfs.append(df)
        y = pd.concat(dfs, ignore_index=True)
        return y

    def_type_void = {
        'store': 'category',
        'sale_date': 'category',
        'TillID': 'category',
        'transaction_time': 'category',
        'TransactionId': 'category',
        'tran_tendered': np.float32,
        'OperatorID': 'category',
        'item_code': 'category',
        'stock_cost': np.float32,
        'soh_qty': 'category',
        'Quantity': np.int32,
        'price': np.float32,
        'discounted_price': np.float32,
        'void_type': 'category',
        'CardNo': 'category',
    }
    def_type_txn = {
        'store': 'category',
        'sale_date': 'category',
        'TillID': 'category',
        'transaction_time': 'category',
        'TransactionId': 'category',
        'GlobalTxnID': 'category',
        'tran_tendered': np.float32,
        'OperatorID': 'category',
        'item_code': 'category',
        'stock_cost': np.float32,
        'soh_qty': 'category',
        'Quantity': np.int32,
        'price': np.float32,
        'discounted_price': np.float32,
        'discount': np.float32,
        'CardNo': 'category',
        'voucher_used': np.float32, #16會被裁
    }
    def_type_inv = {
        'store': 'category', # no_null
        'sale_date': 'category', # no_null
        'TillID': 'category', # no_null
        'transaction_time': 'category', # no_null
        'TransactionId': 'category', # no_null
        'GlobalTxnID': 'category', # no_null
        'OperatorID': 'category', # no_null
        'tran_tendered': np.float32,
        'MediaType': np.float32,
        'Tendered': np.float32,
        'CardNo': str,
        'voucher_used': np.float32,
        'credit_card': str,
    }
    def_type_refund = {
        'store': 'category',
        'sale_date': 'category',
        'TillID': 'category',
        'transaction_time': 'category',
        'TransactionId': 'category',
        'RsGlobalTxnID': 'category',
        'tran_tendered': np.float32,
        'OperatorID': 'category',
        'item_code': 'category',
        'stock_cost': np.float32,
        'soh_qty': 'category',
        'Quantity': np.int32,
        'price': np.float32,
        'discounted_price': np.float32,
        'CardNo': 'category',
        'voucher_used': np.float32,
    }
    def_type_point = {
        'store': 'category',
        'sale_date': 'category',
        'TillID': 'category',
        'transaction_time': 'category',
        'TransactionId': 'category',
        'GlobalTxnID': 'category',
        'OperatorID': 'category',
        'tran_tendered': np.float32,
        'CardNo': 'category',
        'promotion_id': 'category',
        'prom_desc': 'category',
        'points_earned': np.float32,
    }
    def_type_point_movement = {
        'Card Number': 'category',
        'Member ID': 'category',
        'Member Point Sub-type': 'category',
        '# of Member Point Accrual': np.float64,
        '# of Member Point Redeemed': np.float64,
        '# of Member Point Negative': np.float64,
        'Member Point Process Date': np.datetime64,
    }
    def_type_point_bal = {
        'Member ID': 'category',
        'Member.Member Point Balance': np.int64,
        'Member Point.Member Point Balance': np.int64,
        'Card Number': 'category',
    }
