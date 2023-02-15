#Importing Necessary Modules
import os,string,mysql.connector as sql_db,pandas as pd,warnings as w
import math,os,schedule,time,string,flask
string.punctuation
from datetime import date
w.filterwarnings('ignore')

#DataFrame for Calling Count
query1 = '''
select date(t.CreatedDate) As Date,
count(distinct case when (t.ReceivedRoleName like '%HAO%' and t.IsOutbound is false) then t.BenCallID end) As 'Inbound Calls',
count(distinct case when t.IsOutbound is True then t.BenCallID end) As 'Outbound Calls',
(count(distinct case when (t.ReceivedRoleName like '%HAO%' and t.IsOutbound is false) then t.BenCallID end)+count(distinct case when t.IsOutbound is True then t.BenCallID end)) As 'Total Calls',
count(distinct f.BenCallID) As 'Grievance Received',
avg(case when (m.CallGroupType in ('Valid','Transfer') and t.ReceivedRoleName like '%HAO%') then (TIMESTAMPDIFF(SECOND,t.CallTime,t.CallEndTime)) end) As 'Average Call Handling Time'
from db_iemr.t_bencall t 
left join db_iemr.m_calltype m on m.CallTypeID=t.CallTypeID
left join db_iemr.t_feedback f on f.BenCallID=t.BenCallID
group by 1;
'''

query2='''
select date(t.CreatedDate) As Date,t.CreatedBy,avg(timestampdiff(second,t.CallTime,t.CallEndTime)) from t_bencall t
inner join m_calltype m on m.CallTypeID=t.CallTypeID
where  t.ReceivedRoleName like '%HAO%' 
and m.CallGroupType in ('Valid','Transfer')
group by 1,2;
'''

#Database connections
def mycon(db_name:str):
    while 1:
        try:
            mydb = sql_db.connect(host="192.168.5.10",user="Rajesh_s",password="Rajesh_s@2022$", database=db_name,auth_plugin="mysql_native_password")
            return mydb
        except Exception as e:
            print(e)
            print("in exception Block,mycon() been reverted to main loop 'continue'")
            continue
        break
        
def data_frame_1(query1):
    db_name="db_iemr"
    mydb = mycon(db_name)

    mycursor = mydb.cursor()
    mycursor.execute(query1)

    df=pd.DataFrame(mycursor, columns=[i[0] for i in mycursor.description])
    # self.df.drop_duplicates(inplace=True)
    df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')
    # self.df.dropna(inplace=True)
    # df.to_csv("internal_104NHM_main.csv")
    # print("first Excel been Created.")
    while 1:
        try:
            if os.listdir('data_folder')==[]:
                df.to_parquet("data_folder/dataframe_main.parquet")
            elif (os.listdir('data_folder')!=[]):
                if 'dataframe_main.parquet' in os.listdir('data_folder'):
                    os.remove("data_folder/dataframe_main.parquet")
                    # print("Old File Removed Now..!")
                    df.to_parquet("data_folder/dataframe_main.parquet")
                    # print("New File Been Created Now..")                
                elif 'dataframe_main.parquet' not in os.listdir('data_folder'):
                    df.to_parquet("data_folder/dataframe_main.parquet")
                else:
                    print("file_have been Corrupted,I am in Else Block.")
            else:
                print("Thread safe Been Corrupted.I am in Else block.")
        except:
            continue
        break
    # print("Parquet Main File been Updated.")
    
def data_frame_sec(query2):
    db_name_sec="db_iemr"
    mydb_sec = mycon(db_name_sec)

    mycursor_sec = mydb_sec.cursor()
    mycursor_sec.execute(query2)

    df_sec=pd.DataFrame(mycursor_sec, columns=[i[0] for i in mycursor_sec.description])
    # df_sec.drop_duplicates(inplace=True)
    df_sec['Date'] = pd.to_datetime(df_sec['Date'], format='%Y-%m-%d')
    # df_sec.dropna(inplace=True)
    # df_sec.to_csv("internal_104NHM_secondary.csv")
    # print("sec Excel been Created.")

    while 1:
        try:
            if os.listdir('data_folder')==[]:
                df_sec.to_parquet("data_folder/dataframe_secondary.parquet")
            elif (os.listdir('data_folder')!=[]):
                if 'dataframe_main.parquet' in os.listdir('data_folder'):
                    os.remove("data_folder/dataframe_secondary.parquet")
                    # print("Old File Removed Now..!")
                    df_sec.to_parquet("data_folder/dataframe_secondary.parquet")
                    # print("New File Been Created Now..")                
                elif 'dataframe_main.parquet' not in os.listdir('data_folder'):
                    df_sec.to_parquet("data_folder/dataframe_secondary.parquet")
                else:
                    print("file_have been Corrupted,I am in Else Block.")
            else:
                print("Thread safe Been Corrupted.I am in Else block.")
        except:
            continue
        break
    # print("Parquet Secondary File been Updated.")