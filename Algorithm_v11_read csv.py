#from datetime import timedelta, time
import datetime
import pyodbc
import pandas
import csv




def check_time(ttime, etime, window, prod_market_opening, prod_market_closure):
    '''
        This function, received 2 trade datetimes and checks if these datatimes are within the allowed "Product washtrade window" considering the "Product Market opening" and "Product Market closure".
    '''

    #if ttime = datetime_ref:
    if ttime.date() == datetime_ref.date():
        #print (type(ttime)) #datetime.datetime
        if ttime.date() == etime.date(): #only look forward
            #print ((etime - ttime).total_seconds())
            #print ((etime - ttime).total_seconds() > 0 )
            if (((etime - ttime).total_seconds()) < int(window) *60) and (((etime - ttime).total_seconds()) >= 0):
                return (True)
            else:
                return (False)
        else:
            return(False)

    # if ttime != datetime_ref:
    else:
        #if Friday
        if ttime.isoweekday() == 5:
            # create prod market closing date and time
            prod_market_closure_time = (pandas.to_datetime((prod_market_closure), format='%H%M') ) #pandas required because strptime changes string to class datetime.datetime which will not work with combine below
            prod_market_closure_time = (prod_market_closure_time.time())
            prod_market_closure_date = datetime_ref.date() - datetime.timedelta(days=3)
            prod_market_closure =  datetime.datetime.combine(prod_market_closure_date , prod_market_closure_time)
            #print (prod_market_closure)

            # create prod market opening date and time
            prod_market_opening_time = (pandas.to_datetime((prod_market_opening), format='%H%M')) #pandas required because strptime changes string to class datetime.datetime which will not work with combine below
            prod_market_opening_time = (prod_market_opening_time.time())
            prod_market_opening_date = datetime_ref.date()
            prod_market_opening = datetime.datetime.combine(prod_market_opening_date, prod_market_opening_time)
            #print(prod_market_opening)

            a = ((prod_market_closure - ttime).total_seconds())
            b = ((etime - prod_market_opening).total_seconds()) #negative values means that element is on same day as trade, and if this is factored into next logic, will definately return true
            c = ((etime - ttime).total_seconds()) #make it forward looking

            #if t and element on same day
            if ttime.date() == etime.date():
                if (a + b < int(window) * 60 ) and (c >= 0):
                    return (True)
                else:
                    return (False)
            # if t and element not on same day
            else:
                if (a + b < int(window) * 60 ):
                    return (True)
                else:
                    return (False)

        #if not Friday
        else:
            prod_market_closure_time = (pandas.to_datetime((prod_market_closure), format='%H%M') ) #pandas required because strptime changes string to class datetime.datetime which will not work with combine below
            prod_market_closure_time = (prod_market_closure_time.time()) # this changes timestamp to datetime.time, which is a suitable format for combine below. if strptime is used, format is datetime.datetime, which will not work with combine
            #print(datetime_ref.date() - datetime.timedelta(days=1))
            prod_market_closure_date = datetime_ref.date() - datetime.timedelta(days=1)
            prod_market_closure =  datetime.datetime.combine(prod_market_closure_date , prod_market_closure_time)
            #print (prod_market_closure)

            # create prod market opening date and time
            prod_market_opening_time = (pandas.to_datetime((prod_market_opening), format='%H%M') ) #pandas required because strptime changes string to class datetime.datetime which will not work with combine below
            prod_market_opening_time = (prod_market_opening_time.time())
            prod_market_opening_date = datetime_ref.date()
            prod_market_opening = datetime.datetime.combine(prod_market_opening_date , prod_market_opening_time)
            #print (prod_market_opening)

            a = ((prod_market_closure - ttime).total_seconds())
            b = ((etime - prod_market_opening).total_seconds())  # negative values means that element is on same day as trade, and if this is factored into next logic, will definately return true
            c = ((etime - ttime).total_seconds())  # make it forward looking

            if ttime.date() == etime.date():
                if (a + b < int(window) * 60) and (c >= 0):
                    return (True)
                else:
                    return (False)
            else:
                if (a + b < int(window) * 60):
                    return (True)
                else:
                    return (False)

def get_candidates (t, list_of_t, window, prod_market_opening, prod_market_closure):
    '''
        This function, receives a trade (trade t) as a parameter, and a list of trades to check against.
        The funcion will validate trade t against every trade in list_of_t and evaluate if the checked trade is a "candidate" based on the following criteria:
        1. The 2 trades are withing the allowed wash trade window.
        2. The 2 trades have mirrored seller and buyer.
        3. The 2 trades have the same currency, quantity and price.
    '''
    analysis_list = []
    r = []
    #print (list_of_t)
    #print ("t before entry:", t.trade_id)


    for element in list_of_t:
        #print(element)
        #print("t =", t.trade_id)
        #print ("element:", element.trade_id)

        if t.trade_id != element.trade_id:
            time_flag = check_time(t.trader_datetime, element.trader_datetime, window, prod_market_opening, prod_market_closure)

            #SITUATION 1 - 1-1 ALL attributes match
            if (time_flag and t.trade_seller == element.trade_buyer \
                          and t.trade_buyer == element.trade_seller \
                          and t.trade_price == element.trade_price \
                          and t.trade_currency == element.trade_currency \
                          and t.trade_quantity == element.trade_quantity ):
                r.append(["Exact match",element])

            #SITUATION 2 = ALL BUT QUANTITY MATCH, PUT THEN IN ANALYSIS LIST
            elif (time_flag and t.trade_seller == element.trade_buyer \
                            and t.trade_buyer == element.trade_seller \
                            and t.trade_price == element.trade_price \
                            and t.trade_currency == element.trade_currency \
                            and t.trade_quantity != element.trade_quantity ):
                analysis_list.append(["Buyer", element])

            elif (time_flag and t.trade_seller == element.trade_seller \
                            and t.trade_buyer == element.trade_buyer \
                            and t.trade_price == element.trade_price \
                            and t.trade_currency == element.trade_currency \
                            and t.trade_quantity != element.trade_quantity ):
                analysis_list.append(["Seller",element])
    if r:
        r.append(["Exact match", t])
        return (r)
    if analysis_list:
        analysis_list.append (["Seller",t])
        return (analysis_list)



    # for l in r:
    #     for i in l:
    #         print (i.trade_id)
    # for l in analysis_list:
    #     for i in l:
    #         print(i.trade_id)

def algorithm_wash_trade(datetime_ref, data_sql, Trades_filename, Prodconf_filename):
    '''
    :param datetime_ref: date time to use as reference
    :return: number of wash trades found
    '''
    data_for_analysis = {}

    suspected_wash_trade = {}

    total_wash_trades = 0
    if data_sql:
        cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                              "Server=LON-WSOO\SQLEXPRESS;"
                              "Database=TMS;"
                              "Trusted_Connection=yes;")

        cursor = cnxn.cursor()

        # Get the distinct products traded in the day
        product_list = []
        query = "SELECT DISTINCT Product_ID FROM Trades where CONVERT(date, Trade_Time) = '" + str(datetime_ref.date()) + "'"

        list_relevant_products = cursor.execute(query)
        for distinct_product in list_relevant_products:
            product_list.append(distinct_product[0])
        print("Product list:",product_list)



        #For each product get the relevant trades --GO BY PRODUCT



        for product_id in product_list:

            prod_conf_details = []
            #Get the prod_conf for the product
            query = "SELECT * FROM Product_Conf WHERE product_id= '" + str(product_id) +"'"
            cursor.execute(query)
            for row in cursor:
                prod_conf_details.append(row[3])
            print("Prod conf details:", prod_conf_details)

            #Get relevant trades
            day = datetime_ref.isoweekday()
            #print (day)

            if day == 1:
                query = "SELECT * FROM Trades where product_id = '" + str(product_id) + "' and trade_time >= '" + str(datetime_ref.date() - datetime.timedelta(days=3)) + " " \
                        + str((pandas.to_datetime(prod_conf_details[1], format='%H%M%S') - datetime.timedelta(minutes=int(prod_conf_details[2]))).time()) + "'" \
                        + "and trade_time <= '" + str(datetime_ref.date()) + " " + str(pandas.to_datetime(prod_conf_details[1], format='%H%M%S').time()) + "'" + " order by Trade_Time"
            else:
                query = "SELECT * FROM Trades where product_id = '" + str(product_id) + "' and trade_time >= '" + str(datetime_ref.date() - datetime.timedelta(days=1)) + " " \
                        + str((pandas.to_datetime(prod_conf_details[1], format='%H%M%S') - datetime.timedelta(minutes=int(prod_conf_details[2]))).time()) + "'" \
                        + "and trade_time <= '" + str(datetime_ref.date()) + " " + str(pandas.to_datetime(prod_conf_details[1], format='%H%M%S').time()) + "'" + " order by Trade_Time"
            print (query)


            relevant_data = cursor.execute(query)
            # print(relevant_data)
            trades_dic = {}  # key is the trader_id, and value is a dictionary with product_id as key and a list of Trade objects as value

            l = []


            for e in relevant_data:
                #print(type(e[0]),type(e[1]),type(e[2]),type(e[3]),type(e[4]),type(e[5]),type(e[6]),type(e[7]),type(e[8]),type(e[9]))

                product_id = e[1]
                trader_id = e[8]
                t = Trade(e[0], e[9], e[2], e[3], e[4], e[5], e[6], e[8] )
                #print (type(e[9]))  #<class 'datetime.datetime'>
                #print(type(t.trader_datetime)) #<class 'datetime.datetime'>

                #1. check if trader_id already exists in dictionary
                if trader_id in trades_dic.keys():
                    if product_id in trades_dic[trader_id].keys(): #there will only be one and this will always be true
                        # if reaches here it means that the dic already contains a list of trades for this trader_id and product_id
                        l = trades_dic[trader_id][product_id] #retrieves the select list in trades_dic
                        l.append(t)
                        trades_dic[trader_id][product_id] = l
                    else:# trader exists, product doesnt -- this will not be functional but good knowledge
                        new_list_trades = [t]
                        new_product_dic = {}
                        new_product_dic[product_id] = new_list_trades
                        trades_dic[trader_id] = new_product_dic
                else: #creates a dictionay for non existent trader in e
                    l = [t]
                    new_product_dic = {}
                    new_product_dic[product_id] = l
                    trades_dic[trader_id] = new_product_dic

                #2. check if trader_id already exists in SUSPECTED_WASH TRADE dictionary
                if trader_id in suspected_wash_trade.keys():
                    if product_id in suspected_wash_trade[trader_id].keys():
                        pass

                    else:
                        suspected_wash_trade[trader_id][product_id] = []

                else:  # create a dictionay for non existent trader in e
                    new_prod_in_suspected_WT = {}
                    new_prod_in_suspected_WT[product_id] = []
                    suspected_wash_trade[trader_id] = new_prod_in_suspected_WT

            print ("suspected_wash_trade", suspected_wash_trade, "\n")

            #TRADES_DIC = DICTIONARY WHOSE KEYS = TRADER : (VALUES =>  DICTIONARY CONSISTING PRODUCT ID : VALUES CONSISTING LIST OF T FOR TRADES IN SPECIFIED DAY + CERTAIN TRADES OF PREVIOUS WORKING DAY)
            #ANALYSING 2 LEG

            for trader_id, product_dict in trades_dic.items():
                for product_id, list_of_t in product_dict.items():
                    for t in list_of_t:
                        candidates_l = get_candidates(t, list_of_t, prod_conf_details[2], prod_conf_details[0], prod_conf_details[1])
                        #print ("candidate_l", candidates_l,)

                        #if candidates_l is not empty

                        if candidates_l:

                            sum_seller = 0
                            sum_buyer = 0
                            match = []

                            firstlist_firstitem = candidates_l[0][0]
                            if firstlist_firstitem == "Exact match":
                                for trade in candidates_l:
                                    identity, t = trade
                                    match.append(t.trade_id)
                                suspected_wash_trade[trader_id][product_id].append(match)

                            elif firstlist_firstitem == "Seller" or firstlist_firstitem == "Buyer":
                                for trade in candidates_l:
                                    identity, t = trade
                                    match.append(t.trade_id)
                                    if identity == "Seller":
                                        sum_seller += t.trade_quantity

                                    elif identity == "Buyer":
                                        sum_buyer += t.trade_quantity
                                #print(sum_buyer)
                                #print(sum_seller)
                                if sum_buyer == sum_seller:
                                    suspected_wash_trade[trader_id][product_id].append(match)


            print (suspected_wash_trade)
    else:
        #create product list
        with open(Trades_filename) as csvfile:
            reader = csv.reader(csvfile, delimiter='|')
            row_num = 0
            product_list = []
            next(reader) # skip header
            for row in reader:
                if row[1] not in product_list:
                    product_list.append(row[1])
        #product_list = [int(element) if type(element) == str else element for element in product_list]
        #print ('product_list', product_list)



        #retrieve relevant trades

        for product_id in product_list:
            prod_conf_details = []
            day = datetime_ref.isoweekday()
            with open(Prodconf_filename) as PCfile:
                reader =  csv.reader(PCfile, delimiter='|')
                next(reader)  # skip header
                for row in reader:
                    if row[1] == product_id:
                        prod_conf_details.append(row[3])
            #print(prod_conf_details)


            prod_window_mins = int(prod_conf_details[2])
            prod_end_time = datetime.datetime.strptime(prod_conf_details[1], '%H%M')
            prod_end_time_adjusted_for_window = (prod_end_time - datetime.timedelta(minutes=prod_window_mins)).time()
            #print(prod_end_time_adjusted_for_window)

            relevant_data = [] # p
            with open(Trades_filename) as tradefile:
                reader = csv.reader(tradefile, delimiter='|')
                next(reader) #skip header

                for row in reader:
                    dati = datetime.datetime.strptime(row[9], '%d/%m/%Y %H:%M:%S')

                    if row[1] == product_id:
                        if day == 1:    #get all the relevant trades last friday plus monday
                            pass
                        else:   #get day and day-1
                            if dati.date() == datetime_ref.date() :
                                relevant_data.append(
                                    [int(row[0]), int(row[1]), int(row[2]), int(row[3]), float(row[4]), float(row[5]),
                                     str(row[6]), float(row[7]), int(row[8]), dati])
                            elif ( (dati.date() == datetime_ref.date() - datetime.timedelta(days=1)) and (dati.time() > prod_end_time_adjusted_for_window) ):
                              relevant_data.append([int(row[0]), int(row[1]), int(row[2]), int(row[3]), float(row[4]), float(row[5]), str(row[6]), float(row[7]), int(row[8]), dati ])

            # for x in relevant_data:
            #     print(x)
            relevant_data.sort(key=lambda x: x[9].time())
            relevant_data.sort(key=lambda x: x[9].date())
            # for x in relevant_data:
            #     print(x)


            trades_dic = {}  # key is the trader_id, and value is a dictionary with product_id as key and a list of Trade objects as value

            product_list = [int(element) if type(element) == str else element for element in product_list]
            #print('product_list', product_list)

            l = []

            for e in relevant_data:

                product_id = e[1]
                trader_id = e[8]
                t = Trade(e[0], e[9], e[2], e[3], e[4], e[5], e[6], e[8])
                #print(type(t.trader_datetime)) #<class 'datetime.datetime'>

                # 1. check if trader_id already exists in dictionary
                if trader_id in trades_dic.keys():
                    if product_id in trades_dic[
                        trader_id].keys():  # there will only be one and this will always be true
                        # if reaches here it means that the dic already contains a list of trades for this trader_id and product_id
                        l = trades_dic[trader_id][product_id]  # retrieves the select list in trades_dic
                        l.append(t)
                        trades_dic[trader_id][product_id] = l
                    else:  # trader exists, product doesnt -- this will not be functional but good knowledge
                        new_list_trades = [t]
                        new_product_dic = {}
                        new_product_dic[product_id] = new_list_trades
                        trades_dic[trader_id] = new_product_dic
                else:  # creates a dictionay for non existent trader in e
                    l = [t]
                    new_product_dic = {}
                    new_product_dic[product_id] = l
                    trades_dic[trader_id] = new_product_dic

                # 2. check if trader_id already exists in SUSPECTED_WASH TRADE dictionary
                if trader_id in suspected_wash_trade.keys():
                    if product_id in suspected_wash_trade[trader_id].keys():
                        pass

                    else:
                        suspected_wash_trade[trader_id][product_id] = []

                else:  # create a dictionay for non existent trader in e
                    new_prod_in_suspected_WT = {}
                    new_prod_in_suspected_WT[product_id] = []
                    suspected_wash_trade[trader_id] = new_prod_in_suspected_WT



            # TRADES_DIC = DICTIONARY WHOSE KEYS = TRADER : (VALUES =>  DICTIONARY CONSISTING PRODUCT ID : VALUES CONSISTING LIST OF T FOR TRADES IN SPECIFIED DAY + CERTAIN TRADES OF PREVIOUS WORKING DAY)
            # ANALYSING 2 LEG


            for trader_id, product_dict in trades_dic.items():
                for product_id, list_of_t in product_dict.items():
                    for t in list_of_t:
                        candidates_l = get_candidates(t, list_of_t, prod_conf_details[2], prod_conf_details[0],   #-------------------------------------------------
                                                      prod_conf_details[1])
                        #print("candidate_l", candidates_l, )

                        # if candidates_l is not empty

                        if candidates_l:

                            sum_seller = 0
                            sum_buyer = 0
                            match = []

                            firstlist_firstitem = candidates_l[0][0]
                            if firstlist_firstitem == "Exact match":
                                for trade in candidates_l:
                                    identity, t = trade
                                    match.append(t.trade_id)
                                suspected_wash_trade[trader_id][product_id].append(match)

                            elif firstlist_firstitem == "Seller" or firstlist_firstitem == "Buyer":
                                for trade in candidates_l:
                                    identity, t = trade
                                    match.append(t.trade_id)
                                    if identity == "Seller":
                                        sum_seller += t.trade_quantity

                                    elif identity == "Buyer":
                                        sum_buyer += t.trade_quantity
                                #print(sum_buyer)
                                #print(sum_seller)
                                if sum_buyer == sum_seller:
                                    suspected_wash_trade[trader_id][product_id].append(match)


        #print(suspected_wash_trade)
        print ("** SUSPECTED WASH TRADES for", datetime_ref.date(), "**")
        for product, dic in suspected_wash_trade.items():
            for trader_id, trades_id in dic.items():
                print ("Product", product, ",", "Trader", trader_id, "-->", "ID of washtrades identified :", trades_id)



    # 1. Query the Database for the required transactional data. Get all transactions since the last execution time (query Execution_Log table to assess this) + the "Wash Trades Max Time Period" time period

    # 2. For each "product" in the dataset perform the wash trade search match by pt price currency and trader_id



    # 3. Log results into a database table (to be defined) - write into a CSV file with the following collumns: datetime_ref, WashGroup_ID, Transaction ID


    #return total_wash_trades

class Trade(object):

    trade_id = -1
    trader_datetime = -1
    trade_seller = -1
    trade_buyer = -1
    trade_quantity = -1
    trade_price = -1
    trade_currency = -1
    trade_trader_id = -1

    def __init__(self, id, dt, s, b, qt, price, ccy, tid ):
        self.trade_id = id
        self.trader_datetime = dt
        self.trade_seller = s
        self.trade_buyer = b
        self.trade_quantity = qt
        self.trade_price = price
        self.trade_currency = ccy
        self.trader_id = tid

if __name__ == '__main__':

    datetime_ref = datetime.datetime(2018, 8, 1)

    #is data from SQL - True or False
    data_sql = False
    #filename for trades file if CSV
    Trades_filename = "FTITradingSolution_Data.csv"
    #filename for Product file if CSV
    Prodconf_filename = "Prodconf.csv"

    # Run Wash Trades algorithm
    algorithm_wash_trade(datetime_ref, data_sql, Trades_filename, Prodconf_filename)

#2. FOR 1 TO MANY MATCH, MAKE SURE THEY ARE CAUGHT
#3. EXPLORE OTHER POSSIBILITIES