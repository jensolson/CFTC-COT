import pandas as pd
import matplotlib.pyplot as plt
plt.style.use('ggplot')
import requests, io, zipfile, csv, lxml.etree

def get_CFTC_COT_FFOP_current(category):
    """
    Parameters:
        category: 'financial' or 'commodity'
    """
    output = []
    if category == 'financial': 
        feed_url = 'https://www.cftc.gov/dea/newcot/FinFutWk.txt'
        fields_url = 'https://www.cftc.gov/MarketReports/CommitmentsofTraders/HistoricalViewable/cotvariablestfm.html'
    else: 
        feed_url = 'https://www.cftc.gov/dea/newcot/f_disagg.txt'
        fields_url = 'https://www.cftc.gov/MarketReports/CommitmentsofTraders/HistoricalViewable/CFTC_023168.html'
    
    fields_response = requests.get(fields_url)
    doc = lxml.etree.HTML(fields_response.content.decode())
    header = [field.split(' ')[1] for field in doc.xpath("//td/p/text()")]
    response = requests.get(feed_url)
    f = io.StringIO(response.content.decode())
    csv_reader = csv.reader(f)
    for row in csv_reader:
        row_dict = {}
        for index, value in enumerate(row):
            row_dict[header[index]] = value.strip()
        output.append(row_dict)
    df = pd.DataFrame(output)
    
    if category == 'financial':
        df.set_index('Report_Date_as_MM_DD_YYYY', drop=True, inplace=True)
    else:
        df.set_index('As_of_Date_Form_MM/DD/YYYY', drop=True, inplace=True)
        
    df.index = pd.to_datetime(df.index, infer_datetime_format=True)
    del df.index.name
    return df

def get_CFTC_COT_FFOP_hist(category):
    """ Compiles CFTC COT reports for 2006-2016 and for 2017 into one pandas
        dataframe
        Paramters:
            category: 'financial' or 'commodity'
    """
    if category == 'financial':    
        try:    
            df = pd.read_pickle('06_17_CFTC_FFOP.pkl')
        except:    
            # see answer here: https://stackoverflow.com/questions/49575183/dtypewarning-columns-15-16-18-24-have-mixed-types-columns-get-removed-if-it
            df0616 = pd.read_csv('F_TFF_2006_2016.txt', dtype={3: str, 82: str}) 

            df17 = pd.read_csv('FinFut17.txt')
            df18 = pd.read_csv('FinFut18.txt')
            df1718 = df17.append(df18, sort=False)
            
            df = df0616.append(df1718, sort=False)
            df.set_index('Report_Date_as_YYYY-MM-DD', drop=True, inplace=True)
            df.index = pd.to_datetime(df.index, infer_datetime_format=True)
            del df.index.name
            df.to_pickle('06_17_CFTC_FFOP.pkl')
        
    else:
        try:    
            df = pd.read_pickle('06_17_CFTC_FFOP_commodity.pkl')
        except:    
            # see answer here: https://stackoverflow.com/questions/49575183/dtypewarning-columns-15-16-18-24-have-mixed-types-columns-get-removed-if-it
            df0616 = pd.read_csv('F_Disagg06_16.txt',
                                 dtype=dict.fromkeys([3, 120, 121, 125, 133, 134, 135, 145,
                                                      146, 147, 148, 149, 150, 151, 152, 153,
                                                      154, 155, 156, 157, 158, 159, 160, 186], str))

            df17 = pd.read_csv('f_17.txt',
                               dtype=dict.fromkeys([133, 145, 146, 147, 148, 149, 159, 160], str))
            
            df18 = pd.read_csv('f_18.txt',
                               dtype=dict.fromkeys([133, 145, 146, 147, 148, 149, 159, 160], str))

            df1718 = df17.append(df18, sort=False)
            
            df = df0616.append(df1718, sort=False)
            df.set_index('Report_Date_as_YYYY-MM-DD', drop=True, inplace=True)
            df.index = pd.to_datetime(df.index, infer_datetime_format=True)
            del df.index.name
            df.to_pickle('06_17_CFTC_FFOP_commodity.pkl')
        
    return df

def get_CFTC_COT_FFOP_CY(category):
    """ Downloads zipped CFTC COT report for current year and unzips into pandas dataframe
    """
    if category == 'financial':
        zip_2019_url = 'https://www.cftc.gov/files/dea/history/fut_fin_txt_2019.zip'
    else:
        zip_2019_url = 'https://www.cftc.gov/files/dea/history/fut_disagg_txt_2019.zip'
        
    r = requests.get(zip_2019_url, stream=True)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall()
    
    for zipinfo in z.infolist():
        with z.open(zipinfo) as f:
            if category == 'financial':
                df = pd.read_csv(zipinfo.filename)
            else:
                df = pd.read_csv(zipinfo.filename,
                                 dtype=dict.fromkeys([120, 121, 133, 134, 145, 146,
                                                      147, 148, 134,145, 149, 150,
                                                      154, 156, 157, 159, 160], str))
            
    df.set_index('Report_Date_as_YYYY-MM-DD', drop=True, inplace=True)
    df.index = pd.to_datetime(df.index, infer_datetime_format=True)
    del df.index.name
    return df

def get_CFTC_COT_cols(category, contracts):
    """ Concatenates historical and current year pandas dataframes and
        selects relevant futures contracts
    """
    df1 = get_CFTC_COT_FFOP_hist(category)
    df2 = get_CFTC_COT_FFOP_CY(category)
    df3 = get_CFTC_COT_FFOP_current(category)
    df = df1.append(df2, sort=False)
    if max(df3.index) != max(df2.index):
        df = df.append(df3, sort=False)
        
    df = df[df['Market_and_Exchange_Names'].isin(contracts)]
    return df
  
def get_CFTC_COT_LS(category):
    d = {
        'financial': {
            'instruments': {
                '10-YEAR U.S. TREASURY NOTES - CHICAGO BOARD OF TRADE': 'CBOE_US10Y_LS',
                '3-MONTH EURODOLLARS - CHICAGO MERCANTILE EXCHANGE': 'CBOE_GE_LS',
                'BRITISH POUND STERLING - CHICAGO MERCANTILE EXCHANGE': 'CBOE_GBP_LS',
                'E-MINI S&P 500 STOCK INDEX - CHICAGO MERCANTILE EXCHANGE': 'CBOE_ES_LS',
                'EURO FX - CHICAGO MERCANTILE EXCHANGE': 'CBOE_EUR_LS',
                'JAPANESE YEN - CHICAGO MERCANTILE EXCHANGE': 'CBOE_JPY_LS',
                'VIX FUTURES - CBOE FUTURES EXCHANGE': 'CBOE_VIX_LS',    
            },
            'fields': [
                'Lev_Money_Positions_Long_All',
                'Lev_Money_Positions_Short_All', 
            ],
        },
        'commodity': {
            'instruments': {
                'GOLD - COMMODITY EXCHANGE INC.': 'CBOE_GOLD_LS',
                'CRUDE OIL, LIGHT SWEET - NEW YORK MERCANTILE EXCHANGE': 'CBOE_OIL_LS',
            },
            'fields': [
                'M_Money_Positions_Long_All',
                'M_Money_Positions_Short_All',
            ],
        },
    }
            
    contracts = list(d[category]['instruments'].keys())
    fields = list(d[category]['fields'])
    
    df = get_CFTC_COT_cols(category=category, contracts=contracts)
    
    dfs = {}
    for item in fields:
        dfs[item] = df.pivot_table(index=df.index,
                                   columns='Market_and_Exchange_Names',
                                   values=item,
                                   aggfunc='sum').rename_axis(None, axis=1)

        dfs[item] = dfs[item].apply(lambda x: pd.to_numeric(x))
        
    df1 = dfs[d[category]['fields'][0]]-dfs[d[category]['fields'][1]]
    df1.rename(columns=d[category]['instruments'], inplace=True)
    return df1
