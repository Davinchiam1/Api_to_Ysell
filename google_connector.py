import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from frame_reader import Reports_loading
import warnings
warnings.filterwarnings('ignore')

scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']


class Table_connest:
    """Google spreadsheets connect and update data class, using Google Service Account(must have permission to edit
    the target table"""

    def __init__(self, token='token.json', table_name='123'):
        self.credentials = ServiceAccountCredentials.from_json_keyfile_name(token, scope)
        self.table_name = table_name
        self.client = gspread.authorize(self.credentials)
        self.table = self.client.open(table_name)

    def _buckup_data(self, title, values, table_name='backup'):
        """
           Backuping data from target table to backup table.

            Args:
                title (string): sheet name in target table (and backup table also)
                values (Dataframe): frame of values from target table
                table_name(string): name of backup table

            Returns:
                None
        """
        table = self.client.open(table_name)
        worksheet = table.worksheet(title=title)
        worksheet.update('A1', values)

    def _load_data(self, title, values):
        """
           Loading data to sheet in target table.

            Args:
                title (string): sheet name in target table
                values (Dataframe): list of values for uploading

            Returns:
                None
        """
        try:
            worksheet = self.table.worksheet(title=title)
            values_back = worksheet.get_all_values()
            self._buckup_data(title=title, values= values_back)
        except gspread.WorksheetNotFound:
            self.table.add_worksheet(title=title, rows="200", cols="60")
            worksheet = self.table.worksheet(title)
        worksheet.clear()
        worksheet.update('A1', values)
        # worksheet.update(value=values, range_name='A1')
    def load_frame(self, title1, title2, data_directory):
        """
            General function for getting data from the files in the directory, connecting to Google spreadsheets,
            backuping data from target table to backup table and loading new data to target table.

            Args:
                title1 (string): sheet name in target table (and backup table also)
                title2 (string): sheet name in target table (and backup table also)
                data_directory (string): filepath to target directory

            Returns:
                None
        """
        # try:
        #     worksheet = self.table.worksheet(title=title1)
        #     values = worksheet.get_all_values()
        #     self._buckup_data(title=title1, values=values)
        # except gspread.WorksheetNotFound:
        #     self.table.add_worksheet(title=title1, rows="150", cols="60")
        #     worksheet = self.table.worksheet(title1)
        # worksheet.clear()
        loader = Reports_loading()
        data_invres, data_ord = loader.get_data(data_directory)
        data_invres.fillna('0', inplace=True)
        data = data_invres[data_invres.columns[1:].tolist() + [data_invres.columns[0]]]
        data = [data.columns.values.tolist()] + data.values.tolist()
        self._load_data(title=title1,values=data)
        # worksheet.update('A1', data)
        # worksheet.update(value=data, range_name='A1')

        # try:
        #     worksheet = self.table.worksheet(title=title2)
        #     values = worksheet.get_all_values()
        #     self._buckup_data(title=title2, values=values)
        # except gspread.WorksheetNotFound:
        #     self.table.add_worksheet(title=title2, rows="150", cols="60")
        #     worksheet = self.table.worksheet(title1)
        # worksheet.clear()
        columns = data_ord.columns.tolist()
        # columns = [columns[-1]] + columns[:-1]
        data_ord = data_ord[columns]
        data = [data_ord.columns.values.tolist()] + data_ord.values.tolist()
        # worksheet.update('A1', data)
        self._load_data(title=title2, values=data)
        # worksheet.update(value=data, range_name='A1')
        print('Данные успешно загружены в таблицу.')

    def load_api_frame(self, title, dataframe):
        """
            General function for getting data from the files in the directory, connecting to Google spreadsheets,
            backuping data from target table to backup table and loading new data to target table.

            Args:
                title (string): sheet name in target table (and backup table also)
                dataframe (pd.Dataframe): dataframe to load to the table

            Returns:
                None
        """
        columns = dataframe.columns.tolist()
        # columns = [columns[-1]] + columns[:-1]
        dataframe = dataframe[columns]
        data = [dataframe.columns.values.tolist()] + dataframe.values.tolist()
        self._load_data(title=title, values=data)
        # worksheet.update(value=data, range_name='A1')
        print('Данные успешно загружены в таблицу.')

# table_connect = Table_connest(table_name='Chews_stock_best deal')
# table_connect.load_frame(title1='Inv/Reserv', title2='Ord',
#                          data_directory='Z:\\Аналитика\\Amazon\\Update_api\\Reports 06072023')
# table_connect = Table_connest(table_name='Chews_stock_Chewia, Vetrica')
# table_connect.load_frame(title1='Inv/Reserv_1', title2='Ord_1',
#                          data_directory='Z:\\Аналитика\\Amazon\\Update_api\\Chews_stock_Chewia, Vetrica\\Chewia Reports 06072023')

