import os.path
import time
from urllib.parse import urlencode
import requests
import json
import pandas as pd
from datetime import datetime
from tqdm import tqdm
import numpy as np


def json_to_columns(row):
    # convert json to dictionary
    if row is None:
        row = []
    if len(row) > 0:
        data = row[0]
        # creating new columns for each key in JSON
        temp_row = {}
        for key in data:
            temp_row['items.' + key] = data[key]
    else:

        temp_row = {}
    return pd.Series(temp_row)


class API_regu:
    """Base class for api requests"""

    def __init__(self, url='', token='token.txt'):
        # common class parameters
        self.url = url
        with open(token, "r", encoding='utf8') as f:
            token = f.readline()
            print(token)
        self.headers = {
            'accept': '*/*',
            'Authorization': token
        }
        self.pages = None
        self.temp_frame = None

    def orders_by_page(self, page=1):
        """
            Requesting orders from api by page sorted by purchase date in descending order.

            Args:
                page (int): number of page for api request
            Returns:
                temp_frame (dataframe): result frame of requested data
        """

        self.temp_frame = pd.DataFrame()

        # construct request url
        sort = '?sort=-purchase_date&'
        pages = 'page=' + str(page)
        url = self.url + 'order' + sort + pages

        # get data from service
        response = requests.get(url=url, headers=self.headers, timeout=30)
        json_data = response.json()
        df = pd.json_normalize(json_data)
        if response.status_code != 200:
            print(response.json())

        # Processing a dataframe
        df_temp = df['items'].apply(json_to_columns)
        df_temp['items.p_id'] = df_temp['items.p_id'].fillna(0)
        df['is_business_order'] = df['is_business_order'].fillna(0)
        temp_frame = pd.concat([df, df_temp], axis=1)
        temp_frame = temp_frame.drop(['items', 'items.product'], axis=1)
        df_temp = df['shipments'].apply(json_to_columns)
        df_temp = df_temp.replace({np.nan: None})
        temp_frame['shipment_date'] = df_temp['items.shipment_date']
        temp_frame['fulfillment_center'] = df_temp['items.shipmentOptions'].apply(
            lambda x: x.get('fulfillment_center') if x is not None else None)
        temp_frame.set_index('id', inplace=True)
        return temp_frame

    def finance_by_page(self, page=1):
        """
            Requesting financial info about an orders from api by page sorted by purchase date in descending order.

            Args:
                page (int): number of page for api request
            Returns:
                temp_frame (dataframe): result frame of requested data
        """

        self.temp_frame = pd.DataFrame()

        # construct request url
        sort = '?expand=order&sort=-event_time&'
        pages = 'page=' + str(page)
        url = self.url + 'order-settlement' + sort + pages

        # get data from service
        response = requests.get(url=url, headers=self.headers, timeout=30)
        json_data = response.json()
        df = pd.json_normalize(json_data)
        if response.status_code != 200:
            print(response.json())

        # Processing a dataframe
        temp_frame = df[['id', 'order_id', 'sku', 'event_type', 'event_time', 'value', 'currency', 'qty', 'company_id',
                         'marketplace_id', 'order.id', 'order.order_num', 'order.company_id', 'order.platform_order_id',
                         'order.purchase_date', 'order.payment_date', 'order.status.status_name',
                         'order.status.short_description']]
        # df_temp = df['items'].apply(json_to_columns)
        # df_temp['items.p_id'] = df_temp['items.p_id'].fillna(0)
        # df['is_business_order'] = df['is_business_order'].fillna(0)
        # temp_frame = pd.concat([df, df_temp], axis=1)
        # temp_frame = temp_frame.drop(['items', 'items.product'], axis=1)
        # df_temp = df['shipments'].apply(json_to_columns)
        # df_temp = df_temp.replace({np.nan: None})
        # temp_frame['shipment_date']=df_temp['items.shipment_date']
        # temp_frame['fulfillment_center']=df_temp['items.shipmentOptions'].apply(lambda x: x.get('fulfillment_center') if x is not None else None)
        # temp_frame.set_index('id', inplace=True)
        return temp_frame

    def products_by_page(self, page=1):
        """
            Requesting products from api by page.

            Args:
                page (int): number of page for api request
            Returns:
                temp_frame (dataframe): result frame of requested data
        """

        self.temp_frame = pd.DataFrame()

        # construct request url
        pages = '?page=' + str(page)
        url = self.url + 'product' + pages + '&per-page=50'

        # get data from service
        response = requests.get(url=url, headers=self.headers)
        print(response.status_code)
        json_data = response.json()
        df = pd.json_normalize(json_data)
        self.pages = response.headers.get('X-Pagination-Page-Count')

        # Processing a dataframe
        temp_frame = df
        temp_frame.set_index('id', inplace=True)
        return temp_frame

    def company_by_page(self, page=1):
        """
            Requesting companies from api by page.

            Args:
                page (int): number of page for api request
            Returns:
                temp_frame (dataframe): result frame of requested data
        """

        self.temp_frame = pd.DataFrame()

        # construct request url
        pages = '&page=' + str(page)
        url = self.url + 'company?expand=platforms' + pages + '&per-page=50'

        # get data from service
        response = requests.get(url=url, headers=self.headers)
        print(response.status_code)
        json_data = response.json()
        df = pd.json_normalize(json_data)
        self.pages = response.headers.get('X-Pagination-Page-Count')

        # Processing a dataframe
        temp_frame = df
        temp_frame.set_index('id', inplace=True)
        start_index = url.index('https://') + len('https://')
        end_index = url.index('.ysell')
        temp_frame['ysell_pack'] = self.url[start_index:end_index]
        df_temp = temp_frame['platforms'].apply(json_to_columns)
        df_temp.rename(columns={'items.id': 'platform.id', 'items.platform': 'platform', 'items.merchant': 'merchant',
                                'items.is_active': 'is_active', 'item.status': 'status'}, inplace=True)
        temp_frame.drop('platforms', axis=1, inplace=True)
        temp_frame = pd.concat([temp_frame, df_temp], axis=1)
        return temp_frame

    def orders_req(self, start=1, end=10):
        """
            Requesting set of pages containing information about orders.

            Args:
                start (int): number of page to start from
                end (int): number of page to end
            Returns:
                final_frame (dataframe): result frame of requested data from all pages
        """

        final_frame = pd.DataFrame()
        error_pages = []  # List for storing error pages
        total_pages = end - start + 1
        self.temp_frame = pd.DataFrame()

        with tqdm(total=total_pages) as pbar:
            for page in range(start, end + 1):
                try:
                    final_frame = pd.concat([final_frame, self.orders_by_page(page=page)])
                except Exception as e:
                    # Error Handling
                    print(f"Ошибка на странице {page}: {e}")
                    error_pages.append(page)

                pbar.update(1)  # Increase progress bar by 1

        # Additional request for error pages
        if len(error_pages) > 0:
            time.sleep(30)
            for page in error_pages:
                try:
                    final_frame = pd.concat([final_frame, self.orders_by_page(page=page)])
                except Exception as e:
                    print(f"Ошибка при повторном опросе страницы {page}: {e}")

        # final_frame['id']=final_frame.index
        final_frame['shipments'] = final_frame['shipments'].astype(str)
        final_frame['services'] = final_frame['services'].astype(str)

        return final_frame

    def finance_req(self, start=1, end=10):
        """
            Requesting set of pages containing financial info about an orders.

            Args:
                start (int): number of page to start from
                end (int): number of page to end
            Returns:
                final_frame (dataframe): result frame of requested data from all pages
        """

        final_frame = pd.DataFrame()
        error_pages = []  # List for storing error pages
        total_pages = end - start + 1
        self.temp_frame = pd.DataFrame()

        with tqdm(total=total_pages) as pbar:
            for page in range(start, end + 1):
                try:
                    final_frame = pd.concat([final_frame, self.finance_by_page(page=page)])
                except Exception as e:
                    # Error Handling
                    print(f"Ошибка на странице {page}: {e}")
                    error_pages.append(page)

                pbar.update(1)  # Increase progress bar by 1

        # Additional request for error pages
        if len(error_pages) > 0:
            time.sleep(30)
            for page in error_pages:
                try:
                    final_frame = pd.concat([final_frame, self.finance_by_page(page=page)])
                except Exception as e:
                    print(f"Ошибка при повторном опросе страницы {page}: {e}")

        # final_frame['id']=final_frame.index
        # final_frame['shipments'] = final_frame['shipments'].astype(str)
        # final_frame['services'] = final_frame['services'].astype(str)

        return final_frame

    def product_req(self, load_all=True, start=1, end=5):
        """
            Requesting set of pages containing information about products.

            Args:
                load_all (bool): flag for loading all pages (True)/ exact set of pages (False)
                start (int): number of page to start from
                end (int): number of page to end

            Returns:
                final_frame (dataframe): result frame of requested data from all pages
        """
        final_frame = pd.DataFrame()
        page = start
        while page <= end:
            final_frame = pd.concat([final_frame, self.products_by_page(page=page)])
            if load_all and page == start:
                end = int(self.pages)
            page += 1
        return final_frame

    def company_req(self, load_all=True, start=1, end=3):
        """
            Requesting set of pages containing information about companies.

            Args:
                load_all (bool): flag for loading all pages (True)/ exact set of pages (False)
                start (int): number of page to start from
                end (int): number of page to end

            Returns:
                final_frame (dataframe): result frame of requested data from all pages
        """
        final_frame = pd.DataFrame()
        page = start
        while page <= end:
            final_frame = pd.concat([final_frame, self.company_by_page(page=page)])
            if load_all and page == start:
                end = int(self.pages)
            page += 1
        return final_frame


# test = API_regu()
# test_data = test.orders_req(start=8300, end=8700)
# test_data.to_excel('test_data.xlsx')
# test.product_req()


class Keepa_req:
    """ Base class for connection to Keepa service"""

    def __init__(self):
        self.url = 'https://api.keepa.com/'
        with open('token_keepa.txt', "r", encoding='utf8') as f:
            token = f.readline()
            print(token)
        self.access_key = token
        self.pages = None
        self.temp_frame = None
        self.category_link = 'http://www.amazon.com/b/?node='
        self.asin_link = 'https://www.amazon.com/dp/'
        self.img_link = 'https://images-na.ssl-images-amazon.com/images/I/'

    def req(self):
        """
            Requesting information about remaining account
            tokens (https://keepa.com/#!discuss/t/retrieve-token-status/1305).
        """

        url = f"{self.url}token?key={self.access_key}"
        response = requests.get(url)
        print(response.text)

    def category_req(self, category=None, filepath=None):
        """
            Requesting information about root categories or specified categories list
            on amazon (https://keepa.com/#!discuss/t/category-lookup/113).
            Args:
                category (list): list of categories to request, if specified. Default None
                filepath (str): path to save result file if specified. Default None
            Returns:
                df (dataframe): result frame of requested data
        """

        second_requ_category = None
        if type(category) is list:
            if len(category) > 10:
                second_requ_category = category[10:]
                category = category[0:10]
            category = ','.join(category)
        elif category is None:
            category = 0
        url = f"{self.url}category?key={self.access_key}&domain={1}&category={category}&parents={0}"
        response = requests.get(url)
        json_data = response.json()['categories']
        df = pd.DataFrame.from_dict(json_data, orient='index')
        if filepath is not None:
            df.to_excel(filepath + '\\category_req.xlsx')
        else:
            if second_requ_category is not None:
                temp_df = self.category_req(category=second_requ_category)
                df = pd.concat([df, temp_df])
            df = df.fillna(0)
            return df

    def category_search(self, search="", filepath=None):
        """
            Requesting information about categories by specified search term
            (https://keepa.com/#!discuss/t/category-searches/114).
            Args:
                search (str): string to search in categories. Default empty
                filepath (str): path to save result file if specified. Default None
            Returns:
                df (dataframe): result frame of requested data
        """
        url = f"{self.url}search?key={self.access_key}&domain={1}&type=category&term={search}"
        response = requests.get(url)
        json_data = response.json()['categories']
        df = pd.DataFrame.from_dict(json_data, orient='index')
        if filepath is not None:
            df.to_excel(filepath + '\\category_req.xlsx')
        else:
            df.to_excel('category_req.xlsx')

    def product_req(self, asin, filepath=None, file_save=True, cat_names_req=True):
        """
            Requesting information about specified asins (https://keepa.com/#!discuss/t/request-products/110).
            Args:
                asin (str or list): asins to get data
                filepath (str): path to save result file if specified. Default None
                file_save (bool): mark to choose if file need to be saved. Default True
                cat_names_req (bool): mark to get categories names from requested data. Default True
            Returns:
                df (dataframe): result frame of requested data
        """
        self.req()
        second_requ_asin = None
        if type(asin) is list:
            if len(asin) > 100:
                second_requ_asin = asin[100:]
                asin = asin[0:100]

            asin = ','.join(asin)
        data = {
            'asin': asin,
            'days': 5,
            'rating': 1,
            'history': 1,
        }
        encoded_data = urlencode(data)
        url = f"{self.url}product?key={self.access_key}&domain={1}&{encoded_data}"
        response = requests.get(url)
        json_data = response.json()['products']
        # df = pd.DataFrame.from_dict(json_data, orient='index')
        df = pd.DataFrame(json_data)
        df['rating'] = df['csv'].apply(lambda x: (x[16][-1] / 10) if (x[16] is not None) else None)
        df['reviews'] = df['csv'].apply(lambda x: x[17][-1] if (x[17] is not None) else None)
        df['sales_rank'] = df['csv'].apply(lambda x: x[3][-1] if (x[3] is not None) else None)
        df['categories'] = df['categories'].apply(lambda x: str(int(x[-1])) if (x is not None) else 0)
        df['images'] = df['imagesCSV'].apply(lambda x: self.img_link + x.split(',')[0] if (x is not None) else '')
        if file_save:
            if filepath is not None:
                df.to_excel(filepath + '\\product_req.xlsx')
            else:
                df.to_excel('product_req.xlsx')
        else:
            df = df[['asin', 'categories', 'rating', 'reviews', 'sales_rank', 'packageHeight', 'packageLength',
                     'packageWidth', 'images', 'packageWeight']]
            if second_requ_asin is not None:
                temp_df = self.product_req(asin=second_requ_asin, file_save=False, cat_names_req=False)
                df = pd.concat([df, temp_df])
            if cat_names_req:
                cat_nums = df['categories'].dropna().apply(lambda x: str(int(x))).drop_duplicates().to_list()
                categoryes = self.category_req(category=cat_nums)['contextFreeName'].to_dict()
                df['category_name'] = df['categories'].map(categoryes)
                df['categories'] = df['categories'].apply(lambda x: self.category_link + str(x) if (x is not None) else None)
                df['asin_link'] = df['asin'].apply(lambda x: self.asin_link + x if (x is not None) else None)
                df = df[['asin', 'images', 'asin_link', 'categories', 'category_name', 'rating', 'reviews', 'sales_rank',
                         'packageHeight', 'packageLength', 'packageWidth', 'packageWeight']]
            df = df.fillna(0)
            return df

    def product_search(self, search={}, filepath=None):
        """
            Requesting information about products by specified search terms
            (https://keepa.com/#!discuss/t/product-searches/109).
            Args:
                search (json): The term you want to search for. Should be URL encoded
                filepath (str): path to save result file if specified. Default None
            Returns:
                df (dataframe): result frame of requested data
        """
        encoded_data = urlencode(search)
        url = f"{self.url}search?key={self.access_key}&domain={1}&type=product&term={encoded_data}&stats=180"
        response = requests.get(url)
        json_data = response.json()['categories']
        df = pd.DataFrame.from_dict(json_data, orient='index')
        if filepath is not None:
            df.to_excel(filepath + '\\category_req.xlsx')
        else:
            return df

    def product_finder(self, search={}, filepath=None):
        """
            Requesting information about products which match specified criteria
            (https://keepa.com/#!discuss/t/product-finder/5473).
            Args:
                search (json): The term you want to search for. Should be URL encoded
                filepath (str): path to save result file if specified. Default None
            Returns:
                df (dataframe): result frame of requested data
        """
        encoded_data = urlencode(search)
        url = f"{self.url}query?key={self.access_key}&domain={1}&selection={encoded_data}"
        response = requests.get(url)
        json_data = response.json()['categories']
        df = pd.DataFrame.from_dict(json_data, orient='index')
        if filepath is not None:
            df.to_excel(filepath + '\\category_req.xlsx')
        else:
            return df

    def bsr_req(self, category: int = 0, range: int = 30, filepath=None):
        """
            Requesting information about Best Sellers products in specified category criteria
            (https://keepa.com/#!discuss/t/request-best-sellers/1298).
            Args:
                category (int): id of a category
                filepath (str): path to save result file if specified. Default None
                range (int): range of days to count average bsr
            Returns:
                df (dataframe): result frame of requested data
        """
        url = f"{self.url}bestsellers?key={self.access_key}&domain={1}&category={category}&range={range}"
        response = requests.get(url)
        json_data = response.json()['categories']
        df = pd.DataFrame.from_dict(json_data, orient='index')
        if filepath is not None:
            df.to_excel(filepath + '\\category_req.xlsx')
        else:
            return df


# ['categories']
# test2 = Keepa_req()
# test2.req()
# print(test2.category_req(['2975396011','2975293011','2975381011','3024221011','3024166011','3024218011','2619534011',
#                           '2975393011','2975436011','2975397011','2975377011','3024222011','2975312011','2975389011',
#                           '2975384011','3024217011','3763561','15316491','11061971','6939041011','6990032011'])['contextFreeName'].to_dict())
# test2.product_req(asin=['B09P5KT3TT','B09P3ZJWLC','B0CFVVRGNF','B07W8LYX4S'])
# test2.category_search('health')
