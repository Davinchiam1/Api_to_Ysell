import os
import re
import pandas as pd
import chardet

class Reports_loading:
    """Reader from files and merging data class"""
    def __init__(self):
        self.inventory_frame = None
        self.reserved_frame = None
        self.orders_frame = None
        self.orders_list=[]
        self.inventory_list = []
        self.reserved_list = []
        self.result_frame = None
        self.orders_stat=[]
        self.orders_stat_frame=None

    def _create_lists(self, directory):
        """
        Finding of all .csv and .txt files in directory and creating lists.

        Args:
            directory (string): filepath to target directory

        Returns:
            None
        """
        if directory is not None:
            for f in os.scandir(directory):
                if f.is_file() and f.path.split('.')[-1].lower() == 'csv':
                    if 'Inventory' in str(f.path):
                        self.inventory_list.append(f.path)
                    if 'Reserved' in str(f.path):
                        self.reserved_list.append(f.path)
                elif f.is_file() and f.path.split('.')[-1].lower() == 'txt':
                    if 'Orders1' in str(f.path):
                        self.orders_list.append(f.path)
                    if 'Orders' in str(f.path):
                        self.orders_stat.append(f.path)
        else:
            for f in os.scandir():
                if f.is_file() and f.path.split('.')[-1].lower() == 'csv':
                    if 'Inventory' in str(f.path):
                        self.inventory_list.append(f.path)
                    if 'Reserved' in str(f.path):
                        self.reserved_list.append(f.path)
                elif f.is_file() and f.path.split('.')[-1].lower() == 'txt':
                    if 'Orders1М' in str(f.path):
                        self.orders_list.append(f.path)
                    if 'Orders1' in str(f.path):
                        self.orders_stat.append(f.path)

    def _read_data(self):
        """ Reading data from files in target directory and transforming into temp frames"""
        for file in self.inventory_list:
            with open(file, 'rb') as f:
                result = chardet.detect(f.read())
                encoding = result['encoding']
            temp_frame = pd.read_csv(file, delimiter=',',encoding=encoding)
            self.inventory_frame = pd.concat([self.inventory_frame, temp_frame], ignore_index=True, axis=0)
        print('Inventory ready')
        for file in self.reserved_list:
            with open(file, 'rb') as f:
                result = chardet.detect(f.read())
                encoding = result['encoding']
            temp_frame = pd.read_csv(file, delimiter=',',encoding=encoding)
            self.reserved_frame = pd.concat([self.reserved_frame, temp_frame], ignore_index=True, axis=0)
        print('Reserved ready')
        for file in self.orders_list:
            with open(file, 'rb') as f:
                result = chardet.detect(f.read())
                encoding = result['encoding']
            temp_frame = pd.read_csv(file, delimiter='\t',encoding=encoding,low_memory=False)
            temp_frame['purchase-date']=pd.to_datetime(temp_frame['purchase-date']).dt.date
            temp_frame=temp_frame[['sku','asin','item-status','quantity','currency','item-price','purchase-date']]
            self.orders_frame=pd.concat([self.orders_frame, temp_frame], ignore_index=True, axis=0)
        print('Resent orders ready')
        for file in self.orders_stat:
            with open(file, 'rb') as f:
                result = chardet.detect(f.read())
                encoding = result['encoding']
            temp_frame = pd.read_csv(file, delimiter='\t',encoding=encoding, low_memory=False)
            temp_frame = temp_frame[['sku', 'asin', 'item-status', 'quantity', 'currency', 'item-price']]
            self.orders_stat_frame = pd.concat([self.orders_stat_frame, temp_frame], ignore_index=True, axis=0)
        print('Last 3M orders ready')

    def _concentrate_data(self, status='Shipped'):
        """Concentrating data into two dataframes"""
        # Inventory frame
        self.result_frame = pd.merge(self.inventory_frame, self.reserved_frame, on='fnsku', how='outer')

        # Orders frame
        end_date = self.orders_frame['purchase-date'].max()  # last date in purchase-date
        start_date = end_date - pd.DateOffset(days=9)
        last_orders = self.orders_frame[
            (self.orders_frame['purchase-date'] >= start_date.date()) &
            (self.orders_frame['purchase-date'] <= end_date)]
        # Preparing last orders (10 days)
        last_orders = last_orders[last_orders['item-status'] == 'Shipped']
        last_orders = last_orders.groupby('asin').agg({'quantity': 'sum', 'item-price': 'mean'})
        last_orders['avg_per_day_9_d'] = last_orders['quantity'] / 10
        last_orders.rename(inplace=True, columns={'quantity': 'quantity_10_d', 'item-price': 'item-price_10_d',
                                                             'avg_per_day_10_d': 'avg_per_day_10_d'})
        last_orders.reset_index(inplace=True, names='asin')
        # Preparing last orders (30 days)
        self.orders_frame = self.orders_frame[self.orders_frame['item-status'] == 'Shipped']
        self.orders_frame = self.orders_frame.groupby('asin').agg({'quantity': 'sum', 'item-price': 'mean'})
        self.orders_frame['avg_per_day']=self.orders_frame['quantity']/30
        self.orders_frame.reset_index(inplace=True, names='asin')

        # Preparing last orders (3 month)
        self.orders_stat_frame = self.orders_stat_frame[self.orders_stat_frame['item-status'] == 'Shipped']
        self.orders_stat_frame = self.orders_stat_frame.groupby('asin').agg({'quantity': 'sum', 'item-price': 'mean'})
        self.orders_stat_frame['quantity']=self.orders_stat_frame['quantity']/3
        self.orders_stat_frame['avg_per_day_3M']=self.orders_stat_frame['quantity']/30
        self.orders_stat_frame.reset_index(inplace=True,names='asin')
        self.orders_stat_frame.rename(inplace=True, columns={'quantity': 'quantity_3M', 'item-price': 'item-price_3M','avg_per_day_3M':'avg_per_day_3M'})
        self.orders_frame = pd.merge(self.orders_frame,self.orders_stat_frame,on='asin',how='outer')
        self.orders_frame = pd.merge(self.orders_frame, last_orders,on='asin',how='outer')
        self.orders_frame.fillna(0,inplace=True)
        self.result_frame=pd.merge(self.result_frame,self.orders_frame,left_on='asin_x',right_on='asin',how='outer')

    def get_data(self, directory=None):
        """
        General function for extracting and transforming data before uploading.

        Args:
            directory (string): filepath to target

        Returns:
            result_frame (Dataframe): frame of inventory data,
            orders_frame (Dataframe): frame of orders data
        """
        self._create_lists(directory=directory)
        self._read_data()
        self._concentrate_data()
        return self.result_frame, self.orders_frame


# test=Reports_loading()
# frame,frame2=test.get_data(directory='Z:\\Аналитика\\Amazon\\Update_api\\Reports 01062023')
# frame.to_excel('test_file.xlsx', sheet_name='list1')
# frame2.to_excel('test_file2.xlsx', sheet_name='list1')