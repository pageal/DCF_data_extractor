import time

import numpy as np
import requests
import pandas as pd
import ftplib
import io
import re
import json
import datetime
import yfinance as yf
import click
import threading
from pg_stocks import pg_stocks
import stocks

try:
    from requests_html import HTMLSession
except Exception:
    print("""Warning - Certain functionality 
             requires requests_html, which is not installed.

             Install using: 
             pip install requests_html

             After installation, you may have to restart your Python session.""")

class dividend_frequency:
    no_dividend = -1
    annually = 1
    quarterly = 4
    monthly = 12

class dividend_card:
    ticker = ""
    frequency = dividend_frequency.no_dividend
    dividend_yield = 0  # percentage–is the amount of money a company pays shareholders for owning a share of its stock divided by its current stock price
    payout = 0
    div_min = 0
    div_max = 0
    div_spread = 0      # div_max - div_min
    price_min = 0
    price_max = 0
    price_spread = 0    # price_max - price_min

    def __init__(self, ticker):
        self.ticker = ticker

    def to_array(self):
        arr = []
        arr.append(self.ticker)
        arr.append(self.frequency)
        arr.append(self.dividend_yield)
        arr.append(self.payout)
        arr.append(self.div_min)
        arr.append(self.div_max)
        arr.append(self.div_spread)
        arr.append(self.div_growth)
        arr.append(self.price_min)
        arr.append(self.price_max)
        arr.append(self.price_spread)
        arr.append(self.price_growth)
        return arr

class StockInfo:
    def __init__(self):
        self.base_url = "https://query1.finance.yahoo.com/v8/finance/chart/"

        self.cash_flow_columns = None
        self.cash_flow_statements = []

        # line per-ticker table: each line has ticker name and free-cash-flow values for 4 consecuent years
        self.cash_flow_columns_all = ['Ticker']  # the header
        self.cash_flow_columns_all_revenue = ['Ticker']  # the header
        self.cash_flow_columns_growth = ['Ticker', 'Growth']  # the header
        self.cash_flow_statements_for_all = []  # the table array
        self.revenue_statements_for_all = []  # the table array
        self.net_income_statements_for_all = []  # the table array
        self.required_growth_for_all = []

        self.stop_threads = False
        self.starting_from_ticker = ""

        self.risk_free_rate = self.get_risk_free_rate()
        self.market_perpetual_growth_rate = 0.025

    def build_url(self, ticker, start_date = None, end_date = None, interval = "1d"):

        if end_date is None:
            end_seconds = int(pd.Timestamp("now").timestamp())

        else:
            end_seconds = int(pd.Timestamp(end_date).timestamp())

        if start_date is None:
            start_seconds = 7223400

        else:
            start_seconds = int(pd.Timestamp(start_date).timestamp())

        site = self.base_url + ticker

        params = {"period1": start_seconds, "period2": end_seconds,
                  "interval": interval.lower(), "events": "div,splits"}


        return site, params


    def force_float(self, elt):
        try:
            return float(elt)
        except:
            return elt

    def _convert_to_numeric(self, s):
        if "M" in s:
            s = s.strip("M")
            return self.force_float(s) * 1_000_000

        if "B" in s:
            s = s.strip("B")
            return self.force_float(s) * 1_000_000_000
        return self.force_float(s)


    def get_data(self, ticker, start_date = None, end_date = None, index_as_date = True,
                 interval = "1d"):
        '''Downloads historical stock price data into a pandas data frame.  Interval
           must be "1d", "1wk", "1mo", or "1m" for daily, weekly, monthly, or minute data.
           Intraday minute data is limited to 7 days.

           @param: ticker
           @param: start_date = None
           @param: end_date = None
           @param: index_as_date = True
           @param: interval = "1d"
        '''

        if interval not in ("1d", "1wk", "1mo", "1m"):
            raise AssertionError("interval must be of of '1d', '1wk', '1mo', or '1m'")

        # build and connect to URL
        site, params = build_url(ticker, start_date, end_date, interval)
        resp = requests.get(site, params = params)

        if not resp.ok:
            raise AssertionError(resp.json())

        # get JSON response
        data = resp.json()

        # get open / high / low / close data
        frame = pd.DataFrame(data["chart"]["result"][0]["indicators"]["quote"][0])

        # get the date info
        temp_time = data["chart"]["result"][0]["timestamp"]

        if interval != "1m":
            # add in adjclose
            frame["adjclose"] = data["chart"]["result"][0]["indicators"]["adjclose"][0]["adjclose"]
            frame.index = pd.to_datetime(temp_time, unit = "s")
            frame.index = frame.index.map(lambda dt: dt.floor("d"))
            frame = frame[["open", "high", "low", "close", "adjclose", "volume"]]
        else:
            frame.index = pd.to_datetime(temp_time, unit = "s")
            frame = frame[["open", "high", "low", "close", "volume"]]


        frame['ticker'] = ticker.upper()
        if not index_as_date:
            frame = frame.reset_index()
            frame.rename(columns = {"index": "date"}, inplace = True)
        return frame



    def tickers_sp500(self, include_company_data = False):
        '''Downloads list of tickers currently listed in the S&P 500 '''
        # get list of all S&P 500 stocks
        sp500 = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]
        sp500["Symbol"] = sp500["Symbol"].str.replace(".", "-")

        if include_company_data:
            return sp500

        sp_tickers = sp500.Symbol.tolist()
        sp_tickers = sorted(sp_tickers)

        return sp_tickers


    def tickers_nasdaq(self, include_company_data = False):

        '''Downloads list of tickers currently listed in the NASDAQ'''

        ftp = ftplib.FTP("ftp.nasdaqtrader.com")
        ftp.login()
        ftp.cwd("SymbolDirectory")

        r = io.BytesIO()
        ftp.retrbinary('RETR nasdaqlisted.txt', r.write)

        if include_company_data:
            r.seek(0)
            data = pd.read_csv(r, sep = "|")
            return data

        info = r.getvalue().decode()
        splits = info.split("|")


        tickers = [x for x in splits if "\r\n" in x]
        tickers = [x.split("\r\n")[1] for x in tickers if "NASDAQ" not in x != "\r\n"]
        tickers = [ticker for ticker in tickers if "File" not in ticker]

        ftp.close()

        return tickers

    def tickers_other(self, include_company_data = False):
        '''Downloads list of tickers currently listed in the "otherlisted.txt"
           file on "ftp.nasdaqtrader.com" '''
        ftp = ftplib.FTP("ftp.nasdaqtrader.com")
        ftp.login()
        ftp.cwd("SymbolDirectory")

        r = io.BytesIO()
        ftp.retrbinary('RETR otherlisted.txt', r.write)

        if include_company_data:
            r.seek(0)
            data = pd.read_csv(r, sep = "|")
            return data

        info = r.getvalue().decode()
        splits = info.split("|")

        tickers = [x for x in splits if "\r\n" in x]
        tickers = [x.split("\r\n")[1] for x in tickers]
        tickers = [ticker for ticker in tickers if "File" not in ticker]

        ftp.close()

        return tickers


    def tickers_dow(self, include_company_data = False):
        '''Downloads list of currently traded tickers on the Dow'''

        site = "https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average"
        table = pd.read_html(site, attrs = {"id" :"constituents"})[0]

        if include_company_data:
            return table

        dow_tickers = sorted(table['Symbol'].tolist())
        return dow_tickers


    def tickers_ibovespa(self, include_company_data = False):
        '''Downloads list of currently traded tickers on the Ibovespa, Brazil'''

        table = pd.read_html("https://pt.wikipedia.org/wiki/Lista_de_companhias_citadas_no_Ibovespa")[0]
        table.columns = ["Symbol", "Share", "Sector", "Type", "Site"]

        if include_company_data:
            return table

        ibovespa_tickers = sorted(table.Symbol.tolist())
        return ibovespa_tickers

    def tickers_nifty50(self, include_company_data = False):
        '''Downloads list of currently traded tickers on the NIFTY 50, India'''
        site = "https://finance.yahoo.com/quote/%5ENSEI/components?p=%5ENSEI"
        table = pd.read_html(site)[0]

        if include_company_data:
            return table

        nifty50 = sorted(table['Symbol'].tolist())
        return nifty50

    def tickers_niftybank(self):
        ''' Currently traded tickers on the NIFTY BANK, India '''
        niftybank = ['AXISBANK', 'KOTAKBANK', 'HDFCBANK', 'SBIN', 'BANKBARODA', 'INDUSINDBK', 'PNB', 'IDFCFIRSTB', 'ICICIBANK', 'RBLBANK', 'FEDERALBNK', 'BANDHANBNK']
        return niftybank

    def tickers_ftse100(self, include_company_data = False):
        '''Downloads a list of the tickers traded on the FTSE 100 index'''

        table = pd.read_html("https://en.wikipedia.org/wiki/FTSE_100_Index", attrs = {"id": "constituents"})[0]
        if include_company_data:
            return table
        return sorted(table.EPIC.tolist())


    def tickers_ftse250(self, include_company_data = False):
        '''Downloads a list of the tickers traded on the FTSE 250 index'''

        table = pd.read_html("https://en.wikipedia.org/wiki/FTSE_250_Index", attrs = {"id": "constituents"})[0]
        table.columns = ["Company", "Ticker"]

        if include_company_data:
            return table
        return sorted(table.Ticker.tolist())

    def get_quote_table(self, ticker , dict_result = True):
        '''Scrapes data elements found on Yahoo Finance's quote page
           of input ticker

           @param: ticker
           @param: dict_result = True
        '''

        site = "https://finance.yahoo.com/quote/" + ticker + "?p=" + ticker
        tables = pd.read_html(site)
        data = tables[0].append(tables[1])
        data.columns = ["attribute" , "value"]

        quote_price = pd.DataFrame(["Quote Price", get_live_price(ticker)]).transpose()
        quote_price.columns = data.columns.copy()

        data = data.append(quote_price)
        data = data.sort_values("attribute")
        data = data.drop_duplicates().reset_index(drop = True)
        data["value"] = data.value.map(force_float)

        if dict_result:
            result = {key : val for key ,val in zip(data.attribute , data.value)}
            return result
        return data


    def get_stats(self, ticker):
        '''Scrapes information from the statistics tab on Yahoo Finance
           for an input ticker

           @param: ticker
        '''

        stats_site = "https://finance.yahoo.com/quote/" + ticker + \
                     "/key-statistics?p=" + ticker

        tables = pd.read_html(stats_site)
        tables = [table for table in tables[1:] if table.shape[1] == 2]
        table = tables[0]
        for elt in tables[1:]:
            table = table.append(elt)

        table.columns = ["Attribute" , "Value"]
        table = table.reset_index(drop = True)
        return table


    def get_stats_valuation(self, ticker):
        '''Scrapes Valuation Measures table from the statistics tab on Yahoo Finance
           for an input ticker

           @param: ticker
        '''

        stats_site = "https://finance.yahoo.com/quote/" + ticker + \
                     "/key-statistics?p=" + ticker

        tables = pd.read_html(stats_site)
        tables = [table for table in tables if "Trailing P/E" in table.iloc[: ,0].tolist()]

        table = tables[0].reset_index(drop = True)
        return table


    def _parse_json(self, url):
        html = requests.get(url=url, headers={'User-Agent': 'Custom'}).text

        json_str = html.split('root.App.main =')[1].split(
            '(this)')[0].split(';\n}')[0].strip()
        data = json.loads(json_str)[
            'context']['dispatcher']['stores']['QuoteSummaryStore']

        # return data
        new_data = json.dumps(data).replace('{}', 'null')
        new_data = re.sub(r'\{[\'|\"]raw[\'|\"]:(.*?),(.*?)\}', r'\1', new_data)

        json_info = json.loads(new_data)

        return json_info

    def _parse_json1(self, url):
        html = requests.get(url=url, headers={'User-Agent': 'Custom'}).text

        json_str = html.split('root.App.main =')[1].split(
            '(this)')[0].split(';\n}')[0].strip()
        data = json.loads(json_str)[
            'context']['dispatcher']['stores']

        # return data
        new_data = json.dumps(data).replace('{}', 'null')
        new_data = re.sub(r'\{[\'|\"]raw[\'|\"]:(.*?),(.*?)\}', r'\1', new_data)

        json_info = json.loads(new_data)

        return json_info


    def _parse_table(self, json_info):
        df = pd.DataFrame(json_info)
        del df["maxAge"]

        df.set_index("endDate", inplace=True)
        df.index = pd.to_datetime(df.index, unit="s")

        df = df.transpose()
        df.index.name = "Breakdown"

        return df



    def collect_statements(self, ticker, data_frame_cash_flow, data_frame_income_statement):

        if data_frame_cash_flow.empty or len(data_frame_cash_flow.columns) == 0 or data_frame_cash_flow.size == 0:
            print("collect_statements FAILED for {}: data frame is empty".format(ticker))
            return

        data_frame_cash_flow.set_index("endDate", inplace=True)
        data_frame_cash_flow.index = pd.to_datetime(data_frame_cash_flow.index, unit="s")
        #df.index.name = "Breakdown"

        data_frame_income_statement.set_index("endDate", inplace=True)
        data_frame_income_statement.index = pd.to_datetime(data_frame_income_statement.index, unit="s")

        the_total_row = [ticker]
        the_total_row_revenue = [ticker]
        the_total_row_income = [ticker]
        the_total_row_growth = [ticker]
        required_growth = self.calc_wacc(ticker)
        the_total_row_growth = the_total_row_growth + [required_growth]

        if(self.cash_flow_columns is None):
            cash_flow_columns = data_frame_cash_flow.columns

        prev_cash_flow = 0
        data_frame_income_statement = data_frame_income_statement[::-1]
        income_tatement_idx = 0
        for index, statement in data_frame_cash_flow[::-1].iterrows():
            the_row = statement.to_list()
            statement_date = str(index).split(" ")[0]
            statement_year = statement_date.split("-")[0]
            total_revenue = data_frame_income_statement.values[income_tatement_idx][15]
            income_tatement_idx = income_tatement_idx + 1

            print("collect_statements for {}; cash flow statement {}".format(ticker, statement_year))
            if(int(statement_year) < 2010):
                print("collect_statements FAILED for {}: too old cash flow statement {}".format(ticker, statement_year))
                return

            operations = None
            try:
                operations = statement['totalCashFromOperatingActivities']
            except Exception as e:
                pass
            if operations is None:
                try:
                    operations = statement['Total Cash From Operating Activities']
                except Exception as e:
                    pass

            investing = None
            try:
                investing = statement['totalCashflowsFromInvestingActivities'] # + statement['otherCashflowsFromInvestingActivities']
            except Exception as e:
                pass
            if investing is None:
                try:
                    investing = statement['Total Cashflows From Investing Activities'] # + statement['Other Cashflows From Investing Activities']
                except Exception as e:
                    pass

            finansing = None
            try:
                finansing = statement['totalCashFromFinancingActivities'] + statement['otherCashflowsFromFinancingActivities']
            except Exception as e:
                pass
            if finansing is None:
                try:
                    finansing = statement['Total Cash From Financing Activities'] + statement['Other Cashflows From Financing Activities']
                except Exception as e:
                    pass

            capital_expendatures = None
            try:
                capital_expendatures = statement['capitalExpenditures']
            except Exception as e:
                pass
            if capital_expendatures is None:
                try:
                    capital_expendatures = statement['Capital Expenditures']
                except Exception as e:
                    pass

            net_income = None
            try:
                net_income = statement['netIncome']
            except Exception as e:
                pass
            if net_income is None:
                try:
                    net_income = statement['Net Income']
                except Exception as e:
                    pass

            if(capital_expendatures is None) or (operations is None) or (investing is None) or (finansing is None) or (net_income is None):
                print("collect_statements FAILED for {}".format(ticker))
                return

            # EXPANDED formula includes net_borrowings
            # but for DCF it's usually difficult to predict when company will have borrowinds
            # free_csh_flow = operations + capital_expendatures  + net_borrowings
            free_csh_flow = operations + capital_expendatures
            if free_csh_flow <= 0:
                print("collect_statements: {} skipped due to negative cash flow".format(ticker))
                return

            if prev_cash_flow > 0:
                if free_csh_flow < prev_cash_flow:
                    print("collect_statements: {} skipped due to inconsistent cash flow growth".format(ticker))
                    return
            prev_cash_flow = free_csh_flow

            the_row = [ticker] + [statement_year] + [free_csh_flow] + the_row
            self.cash_flow_statements.append(the_row)
            # collect per-ticker yearly data into a single line
            if len(self.cash_flow_columns_all) < 5:
                if f"{statement_year}" not in self.cash_flow_columns_all:
                    self.cash_flow_columns_all.append(f"{statement_year}")
                    self.cash_flow_columns_all_revenue.append(f"{statement_year}")
            the_total_row = the_total_row + [free_csh_flow]
            the_total_row_revenue = the_total_row_revenue + [total_revenue]
            the_total_row_income = the_total_row_income + [net_income]

        #################################################
        #add revenue estimates from analysts
        try:
            cash_flow_site = "https://finance.yahoo.com/quote/" + \
                             ticker + "/analysis?p=" + ticker


            json_info = self._parse_json(cash_flow_site)
            data_frame = pd.DataFrame(json_info["earningsTrend"]["trend"])
            del data_frame["maxAge"]

            for index, statement in data_frame.iterrows():
                the_row = statement.to_list()
                if statement["period"] == "0y" or statement["period"] == "+1y":
                    statement_date = str(statement["endDate"]).strip()
                    statement_year = statement_date.split("-")[0]
                    if len(self.cash_flow_columns_all_revenue) < 7:
                        if f"{statement_year}" not in self.cash_flow_columns_all_revenue:
                            self.cash_flow_columns_all_revenue.append(f"{statement_year}")

                    print("collect_statements for {}; cash flow statement {}".format(ticker, statement_year))
                    average_revenue_estimate = statement["revenueEstimate"]["avg"]
                    the_total_row_revenue = the_total_row_revenue + [average_revenue_estimate]
        except Exception as e:
            pass

        self.cash_flow_statements_for_all.append(the_total_row)
        self.revenue_statements_for_all.append(the_total_row_revenue)
        self.net_income_statements_for_all.append(the_total_row_income)
        self.required_growth_for_all.append(the_total_row_growth)
        print("collect_statements done for {}".format(ticker))

    def get_income_statement(self, ticker, yearly = True):
        '''Scrape income statement from Yahoo Finance for a given ticker

           @param: ticker
        '''

        income_site = "https://finance.yahoo.com/quote/" + ticker + \
                      "/financials?p=" + ticker

        json_info = self._parse_json(income_site)

        if yearly:
            temp = json_info["incomeStatementHistory"]["incomeStatementHistory"]
        else:
            temp = json_info["incomeStatementHistoryQuarterly"]["incomeStatementHistory"]

        data_frame = pd.DataFrame(temp)
        return self._parse_table(temp)


    def get_balance_sheet(self, ticker, yearly = True):

        '''Scrapes balance sheet from Yahoo Finance for an input ticker

           @param: ticker
        '''

        balance_sheet_site = "https://finance.yahoo.com/quote/" + ticker + \
                             "/balance-sheet?p=" + ticker


        json_info = self._parse_json(balance_sheet_site)

        if yearly:
            temp = json_info["balanceSheetHistory"]["balanceSheetStatements"]
        else:
            temp = json_info["balanceSheetHistoryQuarterly"]["balanceSheetStatements"]

        return self._parse_table(temp)


    def get_cash_flow(self, ticker, yearly = True):
        '''Scrapes the cash flow statement from Yahoo Finance for an input ticker

           @param: ticker
        '''

        cash_flow_site = "https://finance.yahoo.com/quote/" + \
                         ticker + "/cash-flow?p=" + ticker

        json_info = self._parse_json(cash_flow_site)

        if yearly:
            temp = json_info["cashflowStatementHistory"]["cashflowStatements"]
        else:
            temp = json_info["cashflowStatementHistoryQuarterly"]["cashflowStatements"]

        return self._parse_table(temp)


    def get_earnings_estimates_pg(self, ticker, yearly=True):
        '''Scrapes the cash flow statement from Yahoo Finance for an input ticker

           @param: ticker
        '''

        data_frame = None
        i = 1
        try:
            cash_flow_site = "https://finance.yahoo.com/quote/" + \
                             ticker + "/analysis?p=" + ticker


            json_info = self._parse_json(cash_flow_site)
            data_frame = pd.DataFrame(json_info["earningsTrend"]["trend"])
            del data_frame["maxAge"]
        except Exception as e:
            pass

        for index, statement in data_frame.iterrows():
            the_row = statement.to_list()
            if statement["period"] == "0y" or statement["period"] == "+1y":
                statement_date = str(statement["endDate"]).strip()
                statement_year = statement_date.split("-")[0]

                average_revenue_estimate = statement["revenueEstimate"]["avg"]

        #self.collect_statements(ticker, data_frame)
        return

    def get_cash_flow_pg(self, ticker, yearly=True):
        '''Scrapes the cash flow statement from Yahoo Finance for an input ticker

           @param: ticker
        '''

        data_frame = None
        i = 1
        try:
            cash_flow_site = "https://finance.yahoo.com/quote/" + \
                             ticker + "/cash-flow?p=" + ticker

            json_info = self._parse_json(cash_flow_site)
            json_info_sel = json_info["cashflowStatementHistory"]["cashflowStatements"]
            data_frame = pd.DataFrame(json_info_sel)
            del data_frame["maxAge"]
        except Exception as e:
            pass

        if(data_frame is None):
            time.sleep(1)
            try:
                yf_ticker = yf.Ticker(ticker)
                cf = yf_ticker.cashflow
                if not cf.empty:
                    data_frame = cf.transpose()
            except Exception as e:
                pass

        if(data_frame is None):
            print(f"FAILED to extract data for {ticker}'s cash flow.")
            return
        data_frame_cash_flow = data_frame

        # extract Net Income from statements
        # netIncome
        # totalRevenue
        # endDate
        data_frame = None
        try:
            income_site = "https://finance.yahoo.com/quote/" + ticker + \
                          "/financials?p=" + ticker
            json_info = self._parse_json(income_site)
            json_info_sel = json_info["incomeStatementHistory"]["incomeStatementHistory"]
            data_frame = pd.DataFrame(json_info_sel)
            del data_frame["maxAge"]
        except Exception as e:
            pass

        if(data_frame is None):
            print(f"FAILED to extract data for {ticker}'s income statement.")
            return
        data_frame_income_statement = data_frame

        self.collect_statements(ticker, data_frame_cash_flow, data_frame_income_statement)
        return self.cash_flow_statements


    def get_financials(self, ticker, yearly = True, quarterly = True):

        '''Scrapes financials data from Yahoo Finance for an input ticker, including
           balance sheet, cash flow statement, and income statement.  Returns dictionary
           of results.

           @param: ticker
           @param: yearly = True
           @param: quarterly = True
        '''

        if not yearly and not quarterly:
            raise AssertionError("yearly or quarterly must be True")

        financials_site = "https://finance.yahoo.com/quote/" + ticker + \
                          "/financials?p=" + ticker

        json_info = self._parse_json(financials_site)

        result = {}

        if yearly:

            temp = json_info["incomeStatementHistory"]["incomeStatementHistory"]
            table = self._parse_table(temp)
            result["yearly_income_statement"] = table

            temp = json_info["balanceSheetHistory"]["balanceSheetStatements"]
            table = self._parse_table(temp)
            result["yearly_balance_sheet"] = table

            temp = json_info["cashflowStatementHistory"]["cashflowStatements"]
            table = self._parse_table(temp)
            result["yearly_cash_flow"] = table

        if quarterly:
            temp = json_info["incomeStatementHistoryQuarterly"]["incomeStatementHistory"]
            table = self._parse_table(temp)
            result["quarterly_income_statement"] = table

            temp = json_info["balanceSheetHistoryQuarterly"]["balanceSheetStatements"]
            table = self._parse_table(temp)
            result["quarterly_balance_sheet"] = table

            temp = json_info["cashflowStatementHistoryQuarterly"]["cashflowStatements"]
            table = self._parse_table(temp)
            result["quarterly_cash_flow"] = table

        return result


    def get_holders(self, ticker):
        '''Scrapes the Holders page from Yahoo Finance for an input ticker

           @param: ticker
        '''

        holders_site = "https://finance.yahoo.com/quote/" + \
                       ticker + "/holders?p=" + ticker

        tables = pd.read_html(holders_site , header = 0)
        table_names = ["Major Holders" , "Direct Holders (Forms 3 and 4)" ,
                       "Top Institutional Holders" , "Top Mutual Fund Holders"]

        table_mapper = {key : val for key ,val in zip(table_names , tables)}
        return table_mapper

    def get_analysts_info(self, ticker):
        '''Scrapes the Analysts page from Yahoo Finance for an input ticker
           @param: ticker
        '''

        analysts_site = "https://finance.yahoo.com/quote/" + ticker + \
                        "/analysts?p=" + ticker

        tables = pd.read_html(analysts_site , header = 0)
        table_names = [table.columns[0] for table in tables]
        table_mapper = {key : val for key , val in zip(table_names , tables)}

        return table_mapper


    def get_live_price(self, ticker):
        '''Gets the live price of input ticker
           @param: ticker
        '''

        df = self.get_data(ticker, end_date = pd.Timestamp.today() + pd.DateOffset(10))
        return df.close[-1]


    def _raw_get_daily_info(self, site):
        session = HTMLSession()
        resp = session.get(site)
        tables = pd.read_html(resp.html.raw_html)

        df = tables[0].copy()
        df.columns = tables[0].columns

        del df["52 Week Range"]
        df["% Change"] = df["% Change"].map(lambda x: float(x.strip("%+").replace(",", "")))

        fields_to_change = [x for x in df.columns.tolist() if "Vol" in x \
                            or x == "Market Cap"]

        for field in fields_to_change:
            if type(df[field][0]) == str:
                df[field] = df[field].map(self._convert_to_numeric)

        session.close()
        return df


    def get_day_most_active(self):
        return self._raw_get_daily_info("https://finance.yahoo.com/most-active?offset=0&count=100")

    def get_day_gainers(self):
        return self._raw_get_daily_info("https://finance.yahoo.com/gainers?offset=0&count=100")

    def get_day_losers(self):
        return self._raw_get_daily_info("https://finance.yahoo.com/losers?offset=0&count=100")


    def get_top_crypto(self):
        '''Gets the top 100 Cryptocurrencies by Market Cap'''
        session = HTMLSession()
        resp = session.get("https://finance.yahoo.com/cryptocurrencies?offset=0&count=100")

        tables = pd.read_html(resp.html.raw_html)

        df = tables[0].copy()
        df["% Change"] = df["% Change"].map(lambda x: float(x.strip("%"). \
                                                            strip("+"). \
                                                            replace(",", "")))
        del df["52 Week Range"]
        del df["1 Day Chart"]

        fields_to_change = [x for x in df.columns.tolist() if "Volume" in x \
                            or x == "Market Cap" or x == "Circulating Supply"]

        for field in fields_to_change:

            if type(df[field][0]) == str:
                df[field] = df[field].map(_convert_to_numeric)


        session.close()
        return df


    def get_dividends(self, ticker, start_date = None, end_date = None, index_as_date = True):
        '''Downloads historical dividend data into a pandas data frame.

           @param: ticker
           @param: start_date = None
           @param: end_date = None
           @param: index_as_date = True
        '''

        # build and connect to URL
        site, params = self.build_url(ticker, start_date, end_date, "1d")
        resp = requests.get(site, params = params)


        if not resp.ok:
            raise AssertionError(resp.json())


        # get JSON response
        data = resp.json()

        # check if there is data available for dividends
        if "dividends" not in data["chart"]["result"][0]['events']:
            raise AssertionError("There is no data available on dividends, or none have been granted")

        # get the dividend data
        frame = pd.DataFrame(data["chart"]["result"][0]['events']['dividends'])

        frame = frame.transpose()

        frame.index = pd.to_datetime(frame.index, unit = "s")
        frame.index = frame.index.map(lambda dt: dt.floor("d"))

        # sort in chronological order
        frame = frame.sort_index()

        frame['ticker'] = ticker.upper()

        # remove old date column
        frame = frame.drop(columns='date')

        frame = frame.rename({'amount': 'dividend'}, axis = 'columns')

        if not index_as_date:
            frame = frame.reset_index()
            frame.rename(columns = {"index": "date"}, inplace = True)

        return frame



    def get_splits(self, ticker, start_date = None, end_date = None, index_as_date = True):
        '''Downloads historical stock split data into a pandas data frame.

           @param: ticker
           @param: start_date = None
           @param: end_date = None
           @param: index_as_date = True
        '''

        # build and connect to URL
        site, params = build_url(ticker, start_date, end_date, "1d")
        resp = requests.get(site, params = params)


        if not resp.ok:
            raise AssertionError(resp.json())


        # get JSON response
        data = resp.json()

        # check if there is data available for splits
        if "splits" not in data["chart"]["result"][0]['events']:
            raise AssertionError("There is no data available on stock splits, or none have occured")

        # get the split data
        frame = pd.DataFrame(data["chart"]["result"][0]['events']['splits'])

        frame = frame.transpose()

        frame.index = pd.to_datetime(frame.index, unit = "s")
        frame.index = frame.index.map(lambda dt: dt.floor("d"))

        # sort in to chronological order
        frame = frame.sort_index()

        frame['ticker'] = ticker.upper()

        # remove unnecessary columns
        frame = frame.drop(columns=['date', 'denominator', 'numerator'])

        if not index_as_date:
            frame = frame.reset_index()
            frame.rename(columns = {"index": "date"}, inplace = True)

        return frame


    def get_earnings(self, ticker):
        '''Scrapes earnings data from Yahoo Finance for an input ticker

           @param: ticker
        '''

        financials_site = "https://finance.yahoo.com/quote/" + ticker + \
                          "/financials?p=" + ticker

        json_info = self._parse_json(financials_site)
        temp = json_info["earnings"]

        result = {}
        result["quarterly_results"] = pd.DataFrame.from_dict(temp["earningsChart"]["quarterly"])
        result["yearly_revenue_earnings"] = pd.DataFrame.from_dict(temp["financialsChart"]["yearly"])
        result["quarterly_revenue_earnings"] = pd.DataFrame.from_dict(temp["financialsChart"]["quarterly"])
        return result


    ### Earnings functions
    def _parse_earnings_json(self, url):
        resp = requests.get(url)
        content = resp.content.decode(encoding='utf-8', errors='strict')

        page_data = [row for row in content.split(
            '\n') if row.startswith('root.App.main = ')][0][:-1]

        page_data = page_data.split('root.App.main = ', 1)[1]
        return json.loads(page_data)

    def get_next_earnings_date(self, ticker):
        base_earnings_url = 'https://finance.yahoo.com/quote'
        new_url = base_earnings_url + "/" + ticker

        parsed_result = self._parse_earnings_json(new_url)
        temp = parsed_result['context']['dispatcher']['stores']['QuoteSummaryStore']['calendarEvents']['earnings']['earningsDate'][0]['raw']
        return datetime.datetime.fromtimestamp(temp)


    def get_earnings_history(self, ticker):
        '''Inputs: @ticker
           Returns the earnings calendar history of the input ticker with
           EPS actual vs. expected data.'''

        url = 'https://finance.yahoo.com/calendar/earnings?symbol=' + ticker

        result = self._parse_earnings_json(url)
        return result["context"]["dispatcher"]["stores"]["ScreenerResultsStore"]["results"]["rows"]



    def get_earnings_for_date(self, date, offset = 0, count = 1):
        '''Inputs: @date
           Returns a dictionary of stock tickers with earnings expected on the
           input date.  The dictionary contains the expected EPS values for each
           stock if available.'''

        base_earnings_url = 'https://finance.yahoo.com/calendar/earnings'

        if offset >= count:
            return []

        temp = pd.Timestamp(date)
        date = temp.strftime("%Y-%m-%d")

        dated_url = '{0}?day={1}&offset={2}&size={3}'.format(
            base_earnings_url, date, offset, 100)

        result = self._parse_earnings_json(dated_url)
        stores = result['context']['dispatcher']['stores']
        earnings_count = stores['ScreenerCriteriaStore']['meta']['total']
        new_offset = offset + 100
        more_earnings = self.get_earnings_for_date(date, new_offset, earnings_count)
        current_earnings = stores['ScreenerResultsStore']['results']['rows']
        total_earnings = current_earnings + more_earnings
        return total_earnings


    def get_earnings_in_date_range(self, start_date, end_date):
        '''Inputs: @start_date
                   @end_date

           Returns the stock tickers with expected EPS data for all dates in the
           input range (inclusive of the start_date and end_date.'''

        earnings_data = []

        days_diff = pd.Timestamp(end_date) - pd.Timestamp(start_date)
        days_diff = days_diff.days

        current_date = pd.Timestamp(start_date)

        dates = [current_date + datetime.timedelta(diff) for diff in range(days_diff + 1)]
        dates = [d.strftime("%Y-%m-%d") for d in dates]

        i = 0
        while i < len(dates):
            try:
                earnings_data += get_earnings_for_date(dates[i])
            except Exception:
                pass
            i += 1

        return earnings_data


    def get_currencies(self):
        '''Returns the currencies table from Yahoo Finance'''
        tables = pd.read_html("https://finance.yahoo.com/currencies")
        result = tables[0]
        return result


    def get_futures(self):
        '''Returns the futures table from Yahoo Finance'''

        tables = pd.read_html("https://finance.yahoo.com/commodities")
        result = tables[0]
        return result


    def get_undervalued_large_caps(self):
        '''Returns the undervalued large caps table from Yahoo Finance'''

        tables = pd.read_html("https://finance.yahoo.com/screener/predefined/undervalued_large_caps?offset=0&count=100")
        result = tables[0]
        return result


    def get_quote_data(self, ticker):
        '''Inputs: @ticker

           Returns a dictionary containing over 70 elements corresponding to the
           input ticker, including company name, book value, moving average data,
           pre-market / post-market price (when applicable), and more.'''

        site = "https://query1.finance.yahoo.com/v7/finance/quote?symbols=" + ticker
        resp = requests.get(site)
        if not resp.ok:
            raise AssertionError("""Invalid response from server.  Check if ticker is
                                  valid.""")

        json_result = resp.json()
        info = json_result["quoteResponse"]["result"]
        return info[0]


    def get_market_status(self):
        '''Returns the current state of the market - PRE, POST, OPEN, or CLOSED'''

        quote_data = self.get_quote_data("^dji")
        return quote_data["marketState"]

    def get_premarket_price(self, ticker):
        '''Inputs: @ticker
           Returns the current pre-market price of the input ticker
           (returns value if pre-market price is available.'''

        quote_data = self.get_quote_data(ticker)
        if "preMarketPrice" in quote_data:
            return quote_data["preMarketPrice"]
        raise AssertionError("Premarket price not currently available.")

    def get_postmarket_price(self, ticker):
        '''Inputs: @ticker

           Returns the current post-market price of the input ticker
           (returns value if pre-market price is available.'''

        quote_data = self.get_quote_data(ticker)
        if "postMarketPrice" in quote_data:
            return quote_data["postMarketPrice"]
        raise AssertionError("Postmarket price not currently available.")

    def get_cash_flow_for_all(self):
        tickers_nyse_amex = self.tickers_other()
        all_tickers_nasdaq = self.tickers_nasdaq()
        #all_tickers = all_tickers_nasdaq + tickers_nyse_amex
        all_tickers = stocks.all_stocks

        # undervalued_large_caps = get_undervalued_large_caps()

        # get_cash_flow: yearly = True
        # cash_flow_all = []
        # for ticker in pg_stocks:
        #    get_cash_flow_pg(ticker)

        # EXPERIMENT: getting free cash flow for 1 ticker
        # get_cash_flow_pg('INTC')
        # columns=["Ticker"]+["Date"] + ["FREE Cash Flow"] + cash_flow_columns.to_list()
        # cash_flow_df = pd.DataFrame(cash_flow_statements, columns=columns)

        dt_now = datetime.datetime.now()
        dt_string = dt_now.strftime("%Y%m%d_%H%M%S")

        started = False
        #for ticker in pg_stocks:
        for ticker in all_tickers:
            if self.stop_threads == True:
                break

            if (self.starting_from_ticker == ""):
                started = True

            if(started == False):
                if(ticker == self.starting_from_ticker):
                    started = True
                continue

            self.get_cash_flow_pg(ticker)
            time.sleep(1)

        fname = "C:/MyProjects/Indicators/DCF_screner/" + dt_string + "cash_flow.csv"
        print(f"saving into {fname}")
        cash_flow_for_all_df = pd.DataFrame(self.cash_flow_statements_for_all, columns=self.cash_flow_columns_all)
        cash_flow_for_all_df.to_csv(fname)

        fname = "C:/MyProjects/Indicators/DCF_screner/" + dt_string + "revenue.csv"
        print(f"saving into {fname}")
        cash_flow_for_all_df = pd.DataFrame(self.revenue_statements_for_all, columns=self.cash_flow_columns_all_revenue)
        cash_flow_for_all_df.to_csv(fname)

        fname = "C:/MyProjects/Indicators/DCF_screner/" + dt_string + "net_income.csv"
        print(f"saving into {fname}")
        cash_flow_for_all_df = pd.DataFrame(self.net_income_statements_for_all, columns=self.cash_flow_columns_all)
        cash_flow_for_all_df.to_csv(fname)


        fname = "C:/MyProjects/Indicators/DCF_screner/" + dt_string + "required_growth.csv"
        print(f"saving into {fname}")
        cash_flow_for_all_df = pd.DataFrame(self.required_growth_for_all, columns=self.cash_flow_columns_growth)
        cash_flow_for_all_df.to_csv(fname)

        print("DONE")



    def dividend_cards_to_csv(self, dividend_cards, file_tag):
        dt_now = datetime.datetime.now()
        dt_string = dt_now.strftime("%Y%m%d_%H%M%S")

        dividend_cards_arr = []
        for dividend_card in dividend_cards:
            dividend_cards_arr.append(dividend_card.to_array())
        csvDataFrame = pd.DataFrame(dividend_cards_arr, columns=["ticker" ,"frequency" ,"dividend_yield" ,"payout", \
                                                                 "div_min" ,"div_max" ,"div_spread" ,"div_growth", \
                                                                 "price_min" ,"price_max" ,"price_spread" ,"price_growth"])
        csvDataFrame.to_csv("C:/Users/USER/Downloads/yahoo_fin_news/" + dt_string + "_" + file_tag + "_dividend.csv")


    def get_stock_price(self, ticker):
        dt_now = datetime.datetime.now()
        dt_week = datetime.timedelta(days=7)

        data_frame = self.get_data(ticker, dt_now - dt_week, dt_now)
        return data_frame["close"][data_frame.__len__() - 1]

    def get_param_stability(self, param_name, data_frame):
        min = max = total = spread = growth = 0
        initialized = False
        for index, data_line in data_frame.iterrows():
            total = total + data_line[param_name]
            if(not initialized):
                min = data_line[param_name]
                max = data_line[param_name]
                initialized = True
                continue

            if(data_line[param_name] < min):
                min = data_line[param_name]
                continue

            if(data_line[param_name] > max):
                max = data_line[param_name]
                continue
        averaged_param_value = total /data_frame.__len__()

        if(averaged_param_value != 0):
            spread = (max - min ) /averaged_param_value
            growth = (data_frame[param_name][data_frame.__len__() - 1] - data_frame[param_name][0] ) /averaged_param_value
        return min, max, spread, growth


    def get_dividend_stability(self, ticker, year_span = 5):
        dt_now = datetime.datetime.now()
        dt_time_span = datetime.timedelta(days=year_span *365)
        min = max = spread = growth = 0
        try:
            data_frame = get_dividends(ticker, dt_now - dt_time_span, dt_now)
        except Exception as e:
            print(str(e))
            return min, max, spread, growth

        min, max, spread, growth = get_param_stability("dividend", data_frame)
        return min, max, spread, growth


    def get_price_stability(self, ticker, year_span = 5):
        dt_now = datetime.datetime.now()
        dt_time_span = datetime.timedelta(days=year_span *365)
        min = max = spread = growth = 0

        try:
            data_frame = get_data(ticker, dt_now - dt_time_span, dt_now)
        except Exception as e:
            print(str(e))
            return min, max, spread, growth
        min, max, spread, growth =  get_param_stability("close", data_frame)
        return min, max, spread, growth


    # extract dividend payment metrics: yield, monthly/yearly/daily payment type
    def get_dividends_for_all(self):
        tickers_nyse_amex = tickers_other()
        all_tickers_nasdaq = tickers_nasdaq()
        all_tickers = all_tickers_nasdaq + tickers_nyse_amex
        all_tickers = sorted(all_tickers)

        monthly = []
        quarterly = []
        annually = []

        dt_now = datetime.datetime.now()
        dt_year = datetime.timedelta(days= 1 *365)

        indx = 0;
        for ticker in all_tickers:
            indx = indx +1
            print("processing {} ({} of {})".format(ticker ,indx ,len(all_tickers)))
            ticker_div_card = dividend_card(ticker)
            try:
                dividend_frame = self.get_dividends(ticker, dt_now - dt_year, dt_now)
            except Exception as e:
                print(str(e))
                continue

            ticker_div_card.payout_annual = 0
            payout_count = 0
            for index, data_line in dividend_frame.iterrows():
                ticker_div_card.payout_annual = ticker_div_card.payout_annual + data_line["dividend"]
                payout_count = payout_count + 1

            ticker_div_card.frequency = payout_count
            try:
                stock_price = self.get_stock_price(ticker)
            except Exception as e:
                print(str(e))
                continue
            ticker_div_card.dividend_yield = ticker_div_card.payout_annual /stock_price
            ticker_div_card.payout = ticker_div_card.payout_annual / payout_count
            if(payout_count == 1):
                annually.append(ticker_div_card)
            if(payout_count == 4):
                quarterly.append(ticker_div_card)
            if(payout_count == 12):
                monthly.append(ticker_div_card)

            ticker_div_card.div_min, ticker_div_card.div_max, ticker_div_card.div_spread, ticker_div_card.div_growth = self.get_dividend_stability(ticker)
            ticker_div_card.price_min, ticker_div_card.price_max, ticker_div_card.price_spread, ticker_div_card.price_growth = self.get_price_stability(ticker)

        self.dividend_cards_to_csv(monthly, "monthly")
        self.dividend_cards_to_csv(quarterly, "quarterly")
        self.dividend_cards_to_csv(annually, "annually")

        return

    def cash_flow_thread(self):
        self.stop_threads = False
        self.get_cash_flow_for_all()

    def str_to_float(self, str_val):
        val = None
        try:
            if(str_val.find("M") > 0):
                val = float(str_val.split("M")[0])*10e6
            if(str_val.find("B") > 0):
                val = float(str_val.strip("()").split("B")[0])*10e9
        except Exception as e:
            pass
        return val

    def get_marketwatch_data(self, ticker):
        interest_expense_on_debth = 0
        pretax_income = 0
        income_taxes = 0

        income_site = f"https://www.marketwatch.com/investing/stock/{ticker}/financials"

        df_list = pd.read_html(income_site)
        data_frame = df_list[4]
        data_frame_transposed = data_frame.transpose()

        for key, statement in data_frame.iterrows():
            statement_values = statement.to_list()
            if('Interest Expense on Debt  Interest Expense on Debt' == statement_values[0]):
                interest_expense_on_debth = self.str_to_float(statement_values[5])
            if(interest_expense_on_debth == 0):
                if('Interest Expense  Interest Expense' == statement_values[0]):
                    interest_expense_on_debth = self.str_to_float(statement_values[5])

            if('Pretax Income  Pretax Income' == statement_values[0]):
                pretax_income = self.str_to_float(statement_values[5])

            if('Income Taxes  Income Taxes' == statement_values[0]):
                income_taxes = self.str_to_float(statement_values[5])
            if(income_taxes == 0):
                if ('Income Tax  Income Tax' == statement_values[0]):
                    income_taxes = self.str_to_float(statement_values[5])

        return interest_expense_on_debth, pretax_income, income_taxes

    def get_total_debt(self, ticker):
        income_site = f"https://finance.yahoo.com/quote/{ticker}/balance-sheet"
        json_info = self._parse_json(income_site)
        json_info_sel = json_info["balanceSheetHistory"]["balanceSheetStatements"]
        data_frame = pd.DataFrame(json_info_sel)
        del data_frame["maxAge"]

        data_frame.set_index("endDate", inplace=True)
        data_frame.index = pd.to_datetime(data_frame.index, unit="s")

        total_debt = 0
        for index, statement in data_frame.iterrows():
            statement_date = str(index).split(" ")[0]
            statement_year = int(statement_date.split("-")[0])
            this_year = int(datetime.datetime.now().year)
            if statement_year == this_year - 1:
                try:
                    total_debt = statement["longTermDebt"]
                except Exception as e:
                    pass
                break
        return total_debt

    def get_risk_free_rate(self):
        income_site = f"https://finance.yahoo.com/bonds"
        json_info = self._parse_json1(income_site)
        json_info_sel = json_info["StreamDataStore"]["quoteData"]
        data_frame = pd.DataFrame(json_info_sel)
        data_frame_transposed = data_frame.transpose()
        risk_free_rate = 1
        for index, statement in data_frame_transposed.iterrows():
            if index == "^TNX":
                risk_free_rate = statement["regularMarketPrice"]
        return risk_free_rate

    def get_key_statistics(self, ticker):
        income_site = f"https://finance.yahoo.com/quote/{ticker}"
        json_info = self._parse_json(income_site)
        json_info_sel = json_info["price"]
        market_cap = json_info_sel["marketCap"]

        json_info_sel = json_info["defaultKeyStatistics"]
        beta = json_info_sel["beta"]
        shares_outstanding = json_info_sel["sharesOutstanding"]
        return shares_outstanding, market_cap, beta

    def calc_wacc(self, ticker):
        '''
        calculate ticker's WWAC as 'Required Return' parameter in DCF calculation
        '''
        # WAAC = WdRd*(1-tax) + WeRe, where d - debth, e - equity
        try:
            print(f"calculating WAAC for {ticker}")
            interest_expense_on_debth, pretax_income, income_taxes = self.get_marketwatch_data(ticker)

            total_debt = self.get_total_debt(ticker)
            shares_outstanding, market_cap, beta = self.get_key_statistics(ticker)

            Wd = float(total_debt)/(total_debt + market_cap)
            We = float(market_cap)/(total_debt + market_cap)

            Rd = 1
            if(interest_expense_on_debth is None or interest_expense_on_debth == 0):
                interest_expense_on_debth = total_debt
            if(total_debt != 0):
                Rd = float(total_debt)/interest_expense_on_debth

            tax_rate = income_taxes/pretax_income

            #Re = Rf + beta*(Rm-Rf), where
            # - Rm (expected return of the market) = 10% historically on average
            # - Rf - risk free rate (10 year bond rate)
            Rf = self.risk_free_rate

            Re = Rf + beta*(10 -Rf)

            waac = Wd*Rd * tax_rate + We*Re
        except Exception as e:
            waac=1
            print(f"default WAAC ({waac}%) assigned for {ticker} ")
        print(f"WAAC for {ticker} is {waac}%")
        return float(waac)/100

    def test(self):
        ticker = "ABTX"
        print("done")


if __name__ == '__main__':
    si = StockInfo()
    si.starting_from_ticker = ""

    cash_flow_thread = threading.Thread(target=si.cash_flow_thread)
    cash_flow_thread.start()

    while True:
        time.sleep(0.1)
        if click.confirm('Do you want to exit', default=None):
            print('Exiting...')
            si.stop_threads = True
            print('Waiting for threads to join...')
            cash_flow_thread.join()
            print("Done")
            exit(1)

    # si.get_stock_price("INTC")
    # si.get_dividends_for_all()
    #si.get_cash_flow_pg('AAON')
    #si.get_earnings_estimates_pg('AAON')
    #si.get_income_statement('AAON')

    #si.calc_wacc("AAPL")
    #si.test()
    print("MAIN done")



