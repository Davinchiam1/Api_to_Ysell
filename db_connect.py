import datetime
import sys
from Api_connect import API_regu, Keepa_req
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, insert, select, exists, inspect, delete
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Float, String, DATETIME, Date, MetaData, Table, text, DDL, BigInteger
import pandas as pd
import sqlalchemy
import numpy as np

from google_connector import Table_connest

# creating a database connection from a file
with open('database.txt', 'r') as file:
    conn_string = file.readline().strip()

engine = create_engine(conn_string)
Session = sessionmaker(bind=engine)
metadata = MetaData()


# session = Session()


def create_table(table_name, frame):
    """
    Function for creating a Table  from dataframe or establishment of dependency with existing table from db.

    Args:
        table_name (string): name of table.
        frame (pd.Dataframe): dataframe sample for table.

    Returns:
        Table: Table object with name table_name.
    """
    inspector = inspect(engine)
    if table_name in inspector.get_table_names():
        table = Table(table_name, metadata, autoload_with=engine)
        db_columns = table.columns.keys()
        frame_columns = frame.columns
        new_columns = set(frame_columns) - set(db_columns)
    else:
        table = Table(table_name, metadata)
        columns = create_colums(frame)
        for name, data_type in columns.items():
            if name == 'id':
                table.append_column(Column(name, data_type, primary_key=True))
            else:
                table.append_column(Column(name, data_type))
        metadata.create_all(engine)
    return table


def create_colums(frame=pd.DataFrame()):
    """
    Function for transformations dataframe columns into dict with column_names: data types structure.

    Args:
        frame (pd.Dataframe): dataframe sample for table.

    Returns:
        columns_dict (dict): dict of column_names and data types.
    """
    columns_dict = {'id': BigInteger}
    for col, dtype in frame.dtypes.items():
        if dtype == 'int64':
            columns_dict[col] = Integer
        elif dtype == 'float64':
            columns_dict[col] = Float
        elif str(dtype).startswith('object'):
            columns_dict[col] = String
        else:
            columns_dict[col] = String
    return columns_dict


def update_orders(table_name='orders', start=1, end=10, link='https://example/api/v1/',
                  token='token.txt', requ=None):
    """
    Function for updating a table of the "orders" type with data from the api service from start to end page.

    Args:
        table_name (string): name of table in db.
        link (string): link for api request.
        start (int): start page.
        end (int): end page.
        token (.txt file): file with token for API service
        requ (API_regu): base class for API service connection


    Returns:
        None

    """
    if requ is None:
        requ = API_regu(url=link, token=token)
    data = requ.orders_req(start=start, end=end)  # load dataframe
    orders = create_table(table_name, data)
    Base = sqlalchemy.orm.declarative_base()
    errors_list = []

    class Orders(Base):
        # base class for sqlalchemy to secure transactions
        __tablename__ = table_name
        __table__ = orders

    session = Session()

    with engine.connect() as connection:
        # prepare data for uploading
        data['id'] = data.index
        # data=data.drop('items.serial_num',axis=1)
        data1 = data.to_dict('records')

        # update or upload data in table
        for row in data1:
            try:
                orders = Orders(**row)
                session.merge(orders)
            except Exception as e:
                errors_list.append(row['id'])
                print(e)
                continue
    session.commit()
    session.close()
    # with engine.connect() as connection:
    #     result = connection.execute(text('SELECT * FROM ' + table_name))
    #     df_result = pd.DataFrame(result.fetchall(), columns=result.keys())
    #     df_result.to_excel('1234.xlsx')
    #     print(df_result.tail())
    #
    # # закрытие соединения
    # engine.dispose()


# update_orders(start=1, end=2)

def update_orders_batch(starter=1, ender=100, link='https://example/api/v1/', table_name='orders',
                        token='token.txt'):
    """
        Function for updating a table of the "orders" type with data from the api service with blocks of 100 pages.

        Args:
            table_name (string): name of table in db.
            link (string): link for api request.
            starter (int): start page.
            ender (int): end page.
            token (.txt file): file with token for API service

        Returns:
            None

        """
    requ = API_regu(url=link, token=token)  # сreating base API_requ class object
    for i in range(starter - 1, ender):
        Base = sqlalchemy.orm.declarative_base()
        update_orders(start=1 + 100 * i, end=100 + 100 * i, requ=requ, table_name=table_name)
    print('Ready!')


def update_finance(table_name='finance', start=1, end=10, link='https://example/api/v1/',
                   token='token.txt', requ=None):
    """
    Function for updating a table of the "finance" type with data from the api service from start to end page.

    Args:
        table_name (string): name of table in db.
        link (string): link for api request.
        start (int): start page.
        end (int): end page.
        token (.txt file): file with token for API service
        requ (API_regu): base class for API service connection


    Returns:
        None

    """
    if requ is None:
        requ = API_regu(url=link, token=token)
    data = requ.finance_req(start=start, end=end)  # load dataframe
    finance = create_table(table_name, data)
    Base = sqlalchemy.orm.declarative_base()
    errors_list = []

    class Finance(Base):
        # base class for sqlalchemy to secure transactions
        __tablename__ = table_name
        __table__ = finance

    session = Session()

    with engine.connect() as connection:
        # prepare data for uploading
        # data['id'] = data.index
        # data=data.set_index('id')
        # data=data.drop('items.serial_num',axis=1)
        data1 = data.to_dict('records')

        # update or upload data in table
        for row in data1:
            try:
                finance = Finance(**row)
                session.merge(finance)
            except Exception as e:
                errors_list.append(row['id'])
                print(e)
                continue
    session.commit()
    session.close()
    # with engine.connect() as connection:
    #     result = connection.execute(text('SELECT * FROM ' + table_name))
    #     df_result = pd.DataFrame(result.fetchall(), columns=result.keys())
    #     df_result.to_excel('1234.xlsx')
    #     print(df_result.tail())
    #
    # # закрытие соединения
    # engine.dispose()


# update_orders(start=1, end=2)

def update_financial_pages(starter=1, ender=100, link='https://example/api/v1/', table_name='finance',
                           token='token.txt'):
    """
        Function for updating a table of the "orders" type with data from the api service with blocks of 100 pages.

        Args:
            table_name (string): name of table in db.
            link (string): link for api request.
            starter (int): start page.
            ender (int): end page.
            token (.txt file): file with token for API service

        Returns:
            None

        """
    requ = API_regu(url=link, token=token)  # сreating base API_requ class object
    for i in range(starter - 1, ender):
        Base = sqlalchemy.orm.declarative_base()
        update_finance(start=1 + 100 * i, end=100 + 100 * i, requ=requ, table_name=table_name)
    print('Ready!')


def update_products(table_name='products', token='token.txt', link='https://example/api/v1/'):
    """
        Function for updating a table of the "products" type with data from the api service.

        Args:
            table_name (string): name of table in db.
            link (string): link for api request.

            token (.txt file): file with token for API service

        Returns:
            None

        """
    data = API_regu(url=link, token=token).product_req()  # load dataframe
    products = create_table(table_name, data)
    Base = sqlalchemy.orm.declarative_base()

    class Products(Base):
        # base class for sqlalchemy to secure transactions
        __tablename__ = table_name
        __table__ = products

    session = Session()

    with engine.connect() as connection:
        # prepare data for uploading
        data['id'] = data.index
        data1 = data.to_dict('records')

        # update or upload data in table
        for row in data1:
            products = Products(**row)
            session.merge(products)
    session.commit()
    session.close()
    print('Ready!')
    print('\n')


def update_companies(table_name='companies', token='token.txt', link='https://example/api/v1/'):
    """
        Function for updating a table of the "companies" type with data from the api service.

        Args:
            table_name (string): name of table in db.
            link (string): link for api request.
            token (.txt file): file with token for API service

        Returns:
            None

        """
    data = API_regu(url=link, token=token).company_req()
    companies = create_table(table_name, data)
    Base = sqlalchemy.orm.declarative_base()

    class Companies(Base):
        # base class for sqlalchemy to secure transactions
        __tablename__ = table_name
        __table__ = companies

    session = Session()

    with engine.connect() as connection:
        # prepare data for uploading
        data['id'] = data.index
        data1 = data.to_dict('records')

        # update or upload data in table
        for row in data1:
            companies = Companies(**row)
            session.merge(companies)
    session.commit()
    session.close()
    print('Ready!')
    print('\n')




def update_products_ranks(table_name='bitrix_prod'):
    """
        Function for getting products rating$review_count from Keepa and uploading it to Google Sheet.

        Args:
            table_name (string): name of table in db that contains data about company products.

        Returns:
            None

        """
    with open('query.txt', 'r') as file:
        # read query data from file
        query = file.readline()

    with engine.connect() as connection:
        # query = table.select()
        # result = connection.execute(query)
        query = text(query)
        result = connection.execute(query)
        df = pd.DataFrame(result.fetchall())
        df['asin']=df['asin'].str.replace(' ', '')
        asins = df['asin'].drop_duplicates().to_list()
        asins =[value for value in asins if len(value) == 10]
        data = Keepa_req().product_req(asin=asins, file_save=False)

    table_connect = Table_connest(table_name='Products_ranks')
    table_connect.load_api_frame(title='ranks', dataframe=data)

    print('Ready!')
    print('\n')


# update_products()
# update_bitrix_produsts()
# update_orders(start=1,end=10,token='token.txt',table_name='orders',link='https://1359.eu11.ysell.pro/api/v1/')
# update_pages(1, 5,token='token.txt',table_name='orders',link='https://1359.eu11.ysell.pro/api/v1/')
# update_pages(1, 5,token='token2.txt',table_name='orders_v2',link='https://nemo.eu2.ysell.pro/api/v1/')
# update_financial_pages(1, 2,token='token3.txt',table_name='finance_v3',link='https://2360.eu25.ysell.pro/api/v1/')
# update_products_ranks()
