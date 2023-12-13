import tkinter as tk
import os
from tkinter import *
from tkinter.ttk import Combobox
from tkinter import filedialog
from tkinter import messagebox
import db_connect as dbc
from tkcalendar import DateEntry
import threading


class App(tk.Frame):
    def __init__(self, master):
        super().__init__(master)
        self.master = master
        self.grid()
        self.create_widgets()

    def create_widgets(self):
        """Creating a base window"""

        # combo box for selection of account for api request
        self.options = [line.strip() for line in open('accounts.txt', 'r')]
        self.combobox = Combobox(self, values=self.options)
        self.combobox_label = tk.Label(self, text="Account:")
        self.combobox_label.grid(row=0, column=0, sticky=tk.W)
        self.combobox.grid(row=0, column=1, sticky=tk.W)
        self.combobox.bind("<<ComboboxSelected>>", self.selected)

        # field for showing token after account is selected
        self.token_label = tk.Label(self, text='Token:')
        self.token_label.grid(row=1, column=0, sticky=tk.W, columnspan=3)

        # field for entering a start page
        self.start_label = tk.Label(self, text="Start:")
        self.start_label.grid(row=2, column=0, sticky=tk.W)
        self.start_entry = tk.Entry(self)
        self.start_entry.grid(row=2, column=1)

        # field for entering an end page
        self.end_label = tk.Label(self, text="End:")
        self.end_label.grid(row=2, column=2, sticky=tk.W)
        self.end_entry = tk.Entry(self)
        self.end_entry.grid(row=2, column=3, sticky=tk.W)

        # button for start updating orders (by 100 pages in 1 set)
        self.orders_update = tk.Button(self, text="Update orders", command=self.update_orders)
        self.orders_update.grid(row=3, column=0, sticky=tk.N)

        # button for start loading orders (by 1 page i set)
        self.orders = tk.Button(self, text="Load orders", command=self.load_orders)
        self.orders.grid(row=3, column=1, sticky=tk.N)

        # button for start loading products
        self.products = tk.Button(self, text="Load products", command=self.load_products)
        self.products.grid(row=3, column=2, sticky=tk.N)

        # button for start loading companies
        self.comp_button = tk.Button(self, text="Load companies", command=self.load_companies)
        self.comp_button.grid(row=3, column=3, sticky=tk.N)

        # button for start updating orders finance info (by 100 pages in 1 set)
        self.finance_update = tk.Button(self, text="Update finance", command=self.update_finance)
        self.finance_update.grid(row=4, column=0, sticky=tk.N)

        # button for start loading orders finance info (by 1 page in set)
        self.finance_load = tk.Button(self, text="Load finance", command=self.load_finance)
        self.finance_load.grid(row=4, column=1, sticky=tk.N)

        # button for start loading orders finance info (by 1 page in set)
        self.product_ranks = tk.Button(self, text="Load product ranks", command=self.load_product_ranks)
        self.product_ranks.grid(row=4, column=2, sticky=tk.N)

    def update_orders_thread(self):
        threading.Thread(target=self.update_orders).start()

    def load_orders_thread(self):
        threading.Thread(target=self.load_orders).start()

    def load_products_thread(self):
        threading.Thread(target=self.load_products).start()

    def load_finance_thread(self):
        threading.Thread(target=self.load_finance).start()

    def load_product_ranks_thread(self):
        threading.Thread(target=self.load_product_ranks).start()
    def selected(self, event):
        # load token after selection of an account in combox
        if self.combobox.get() == self.options[0]:
            with open('token.txt', "r", encoding='utf8') as f:
                token = f.readline()
                self.token_name = 'token.txt'
                self.orders_name = 'orders'
                self.prod_table = 'products'
                self.companies = 'companies'
                self.token_label["text"] = f"Token: {token}"
                self.finance = 'finance'
        elif self.combobox.get() == self.options[1]:
            with open('token2.txt', "r", encoding='utf8') as f:
                token = f.readline()
                self.token_name = 'token2.txt'
                self.orders_name = 'orders_v2'
                self.prod_table = 'products_v2'
                self.companies = 'companies.'
                self.token_label["text"] = f"Token: {token}"
                self.finance = 'finance_v2'
        elif self.combobox.get() == self.options[2]:
            with open('token3.txt', "r", encoding='utf8') as f:
                token = f.readline()
                self.token_name = 'token3.txt'
                self.orders_name = 'orders_v3'
                self.prod_table = 'products_v3'
                self.companies = 'companies'
                self.finance = 'finance_v3'
                self.token_label["text"] = f"Token: {token}"
                self.finance = 'finance_v3'

    # def read_token(self):
    #     if self.combobox.get() == self.options[0]:
    #         with open('token.txt', "r", encoding='utf8') as f:
    #             token = f.readline()
    #             self.token_name = 'token.txt'
    #             self.orders_name = 'orders'
    #             self.prod_table = 'products'
    #             self.companies = 'companies'
    #             return token
    #     elif self.combobox.get() == self.options[1]:
    #         with open('token2.txt', "r", encoding='utf8') as f:
    #             token = f.readline()
    #             self.token_name = 'token.txt'
    #             self.orders_name = 'orders_v2'
    #             self.prod_table = 'products_v2'
    #             self.companies = 'companies_v2'
    #             return token

    def update_orders(self):
        """Uploading orders to the base by 100 pages"""
        dbc.update_orders_batch(starter=int(self.start_entry.get()), ender=int(self.end_entry.get()),
                                link='https://' + self.combobox.get() + '.ysell.pro/api/v1/', table_name=self.orders_name,
                                token=self.token_name)

    def load_orders(self):
        """Uploading orders to the base by 1 page"""
        dbc.update_orders(table_name=self.orders_name, start=int(self.start_entry.get()), end=int(self.end_entry.get()),
                          link='https://' + self.combobox.get() + '.ysell.pro/api/v1/', token=self.token_name)

    def load_products(self):
        """Uploading products"""
        dbc.update_finance(table_name=self.prod_table, link='https://' + self.combobox.get() + '.ysell.pro/api/v1/',
                            token=self.token_name)

    def update_finance(self):
        """Uploading orders to the base by 100 pages"""
        dbc.update_financial_pages(starter=int(self.start_entry.get()), ender=int(self.end_entry.get()),
                         link='https://' + self.combobox.get() + '.ysell.pro/api/v1/', table_name=self.finance,
                         token=self.token_name)

    def load_finance(self):
        """Uploading orders to the base by 1 page"""
        dbc.update_finance(table_name=self.finance, start=int(self.start_entry.get()), end=int(self.end_entry.get()),
                          link='https://' + self.combobox.get() + '.ysell.pro/api/v1/', token=self.token_name)

    def load_companies(self):
        """Uploading companites"""
        dbc.update_companies(table_name=self.companies, link='https://' + self.combobox.get() + '.ysell.pro/api/v1/',
                             token=self.token_name)


    def load_product_ranks(self):
        """Refreshing products ranks data"""
        dbc.update_products_ranks()

root = tk.Tk()
root.title("Database update App")
root.geometry("450x125")
root.resizable(False, False)
# root.columnconfigure(3, minsize=50, weight=1)
# root.columnconfigure(1, minsize=50, weight=1)

app = App(master=root)
app.mainloop()
