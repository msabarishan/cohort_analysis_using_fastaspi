from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

#Fetching required libraries
import pandas as pd
import numpy as np
import datetime as dt
from sqlalchemy import create_engine, select, MetaData, Table, and_
#Fetch all records from the table using the SQL. 

origins = [
    "http://localhost.tiangolo.com",
    "https://localhost.tiangolo.com",
    "http://localhost",
    "http://localhost:8080",
    "http://localhost:3000",
    "https://kryan-graph.vercel.app",
    "https://kryan-graph.vercel.app/",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# Password and host removed for security purpose
user = 'postgres'
password = ''
host = ''
port = 5432
database = 'postgres'

engine = create_engine(
        url="postgresql://{0}:{1}@{2}:{3}/{4}".format(
            user, password, host, port, database
        )
    )

sql='Select order_phonenumber,order_amount,created_at from all_orders;'
connection = engine.connect()
results = connection.execute(sql).fetchall()

transaction_df=pd.DataFrame(results)
transaction_df.columns = ['order_phonenumber','order_amount','created_at']
connection.close()

transaction_df['created_at'] = pd.to_datetime(transaction_df['created_at'])
transaction_df['date_new'] = transaction_df['created_at'].dt.date

# Replace the ' 's with NaN
transaction_df = transaction_df.replace(" ",np.NaN)
# Impute the missing values with mean imputation
transaction_df = transaction_df.fillna(transaction_df.mean())

transaction_df["date_new"] = transaction_df["date_new"].astype('datetime64[ns]')
transaction_df["order_amount"] = transaction_df["order_amount"].astype(int)

for col in transaction_df.columns:
    # Check if the column is of object type
    if transaction_df[col].dtypes == 'object':
        # Impute with the most frequent value
        transaction_df[col] = transaction_df[col].fillna(transaction_df[col].value_counts().index[0])

# A function that will parse the date Time based cohort:  1 day of month
def get_month(x): return dt.datetime(x.year, x.month, 1)
def get_year(y): return dt.datetime(y.year,1,1)
# Create transaction_date column based on month and store in TransactionMonth
transaction_df['TransactionMonth'] = transaction_df['date_new'].apply(get_month)
# Grouping by customer_id and select the InvoiceMonth value
grouping = transaction_df.groupby('order_phonenumber')['date_new'] 
# Assigning a minimum InvoiceMonth value to the dataset
transaction_df['CohortMonth'] = grouping.transform('min')
transaction_df['CohortMonth']= transaction_df['CohortMonth'].apply(get_month)
transaction_df['CohortMonth2'] =transaction_df['CohortMonth'].dt.strftime('%Y-%m')

transaction_df['CohortYear'] = grouping.transform('min')
transaction_df['CohortYear']= transaction_df['CohortYear'].apply(get_year)
transaction_df['CohortYear']= transaction_df['CohortYear'].dt.year

transaction_df['TransactionQuarter']=pd.PeriodIndex(transaction_df.TransactionMonth,freq='Q')
transaction_df['CohortQuarter']=pd.PeriodIndex(transaction_df.CohortMonth,freq='Q')
transaction_df['TransactionQuarter_no']=transaction_df['TransactionMonth'].dt.quarter
transaction_df['CohortQuarter_no']=transaction_df['CohortMonth'].dt.quarter

def get_date_int(df, column):
    year = df[column].dt.year
    month = df[column].dt.month
    day = df[column].dt.day
    return year, month, day
# Getting the integers for date parts from the `InvoiceDay` column
transaction_year, transaction_month, _ = get_date_int(transaction_df, 'TransactionMonth')
# Getting the integers for date parts from the `CohortDay` column
cohort_year, cohort_month, _ = get_date_int(transaction_df, 'CohortMonth')
TransactionQuarter_no=transaction_df['TransactionQuarter_no']
CohortQuarter_no=transaction_df['CohortQuarter_no']


#  Get the  difference in years
years_diff = transaction_year - cohort_year
month_diff = transaction_month - cohort_month
# Calculate difference in Quarter
Quarter_diff = TransactionQuarter_no - CohortQuarter_no

transaction_df['CohortIndex'] = years_diff * 4 + Quarter_diff + 1
transaction_df['CohortIndexYear'] = years_diff + 1
transaction_df['CohortIndexMonth'] = years_diff * 4 +month_diff*12 + 1 

def cohort_calculation(trasaction_df,interval,interval_index):
    # Counting daily active user from each chort
    grouping2 = transaction_df.groupby([interval, interval_index])
    # Counting number of unique customer Id's falling in each group of CohortMonth and CohortIndex
    cohort_data2 = grouping2['order_amount'].sum()
    cohort_data2 = cohort_data2.reset_index()
     # Assigning column names to the dataframe created above
    cohort_counts2 = cohort_data2.pivot(index=interval,
                                     columns = interval_index,
                                     values = 'order_amount')
    # Counting daily active user from each chort
    grouping = transaction_df.groupby([interval, interval_index])
    # Counting number of unique customer Id's falling in each group of CohortMonth and CohortIndex
    cohort_data = grouping['order_phonenumber'].apply(pd.Series.nunique)
    cohort_data = cohort_data.reset_index()
     # Assigning column names to the dataframe created above
    cohort_counts = cohort_data.pivot(index=interval,
                                     columns =interval_index,
                                     values = 'order_phonenumber')
    
    cohort_sizes = cohort_counts.iloc[:,0]
    retention = cohort_counts.divide(cohort_sizes, axis=0)
    # Coverting the retention rate into percentage and Rounding off.
    retention=retention.round(3)*100

    def to_string(dataframe):
        dataframe=dataframe.reset_index()
        dataframe[interval]=dataframe[interval].astype('str')
        dataframe=dataframe.reset_index(drop=True)
        #dataframe=dataframe.replace(np.nan, '-')
        return dataframe
    #retention=retention.replace(np.nan, '-')

    retention=to_string(retention)
    cohort_counts=to_string(cohort_counts)
    cohort_counts2=to_string(cohort_counts2)

    def create_list(dataframe,data_type,round_off):
        dataframe_transpose=dataframe.transpose()
        new_header=dataframe_transpose.iloc[0]
        dataframe_transpose=dataframe_transpose[1:]
        dataframe_transpose.columns=new_header
        dataframe_transpose= dataframe_transpose.fillna(0)
        dataframe_transpose=dataframe_transpose.astype(data_type)
        dataframe_transpose=dataframe_transpose.round(round_off)
        value=dataframe_transpose.to_dict('list')
        #value = {key : np.round(value[key], 2) for key in value}
        return value

    amount = create_list(cohort_counts2,'float',2)
    cohort_value = create_list(cohort_counts,'int',0)
    cohort_per = create_list(retention,'float',2)
    return amount,cohort_value,cohort_per

amount,cohort_value,cohort_per=cohort_calculation(transaction_df,'CohortQuarter', 'CohortIndex')
y_amount,y_cohort_value,y_cohort_per=cohort_calculation(transaction_df,'CohortYear', 'CohortIndexYear')
m_amount,m_cohort_value,m_cohort_per=cohort_calculation(transaction_df,'CohortMonth2', 'CohortIndexMonth')


@app.get("/")
def read_root():
    return {"Hello": "New_World"}

@app.get("/cohort_per")
def read_root():
    return cohort_per

@app.get("/cohort_amount")
def read_root():
    return amount

@app.get("/cohort_value")
def read_root():
    return cohort_value

@app.get("/y_cohort_value")
def read_root():
    return y_cohort_value

@app.get("/y_cohort_per")
def read_root():
    return y_cohort_per

@app.get("/y_cohort_amount")
def read_root():
    return y_amount

@app.get("/m_cohort_value")
def read_root():
    return m_cohort_value

@app.get("/m_cohort_per")
def read_root():
    return m_cohort_per

@app.get("/m_cohort_amount")
def read_root():
    return m_amount
