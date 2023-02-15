#Importing Necessary Modules
import pandas as pd,json
import numpy as np
from datetime import date
from dash import ctx
# from sql_query_fetching_second import obj,obj_sec
from dash_extensions.enrich import callback_context
from pandas._libs.tslibs.timestamps import Timestamp
from updated_date import Current_things

def showing_proper_int(n:int)->str:
    if (len(str(n))==1) or (len(str(n))==2) or (len(str(n))==3):
        return str(n)
    elif (len(str(n))==4):
        mk1=[str(x) for x in str(n)]
        return (mk1[0]+","+mk1[1]+mk1[2]+mk1[3])
    elif (len(str(n))==5):
        mk1=[str(x) for x in str(n)]
        return (mk1[0]+mk1[1]+","+mk1[2]+mk1[3]+mk1[4])
    elif (len(str(n))==6):  #7,87,767
        mk1=[str(x) for x in str(n)]
        return (mk1[0]+","+mk1[1]+mk1[2]+","+mk1[3]+mk1[4]+mk1[5])
    elif (len(str(n))==7): #67,56,354
        mk1=[str(x) for x in str(n)]
        return (mk1[0]+mk1[1]+","+mk1[2]+mk1[3]+","+mk1[4]+mk1[5]+mk1[6])
    elif (len(str(n))==8): #6,75,63,549
        mk1=[str(x) for x in str(n)]
        return (mk1[0]+","+mk1[1]+mk1[2]+","+mk1[3]+mk1[4]+","+mk1[5]+mk1[6]+mk1[7])
    elif (len(str(n))==9): #67,56,35,499
        mk1=[str(x) for x in str(n)]
        return (mk1[0]+mk1[1]+","+mk1[2]+mk1[3]+","+mk1[4]+mk1[5]+","+mk1[6]+mk1[7]+mk1[8])
    elif (len(str(n))==10): #6,75,63,54,997
        mk1=[str(x) for x in str(n)]
        return (mk1[0]+","+mk1[1]+mk1[2]+","+mk1[3]+mk1[4]+","+mk1[5]+mk1[6]+","+mk1[7]+mk1[8]+mk1[9])
    elif (len(str(n))==11): #67,56,35,49,976
        mk1=[str(x) for x in str(n)]
        return (mk1[0]+mk1[1]+","+mk1[2]+mk1[3]+","+mk1[4]+mk1[5]+","+mk1[6]+mk1[7]+","+mk1[8]+mk1[9]+mk1[10])
    else:
        return int(n)

#Class For Data Reading From Parquet File
class Data_Reading:
    def __init__(self):
        updated_date_obj=Current_things()
        self.today_rep_data=updated_date_obj.current_date()
        self.today_class_data = pd.to_datetime(self.today_rep_data, format='%Y-%m-%d')
        self.yesterday_class_data = self.today_class_data-pd.Timedelta(days=1)
        self.df=pd.read_parquet('data_folder/dataframe_main.parquet', engine='pyarrow')
        self.df_secondary=pd.read_parquet('data_folder/dataframe_secondary.parquet', engine='pyarrow')
        
    #Answered Calls Cumulative ,Today & yesterday Count
    def answered_cumulative(self,st:str,en:str):
        answered_cum=self.df[(self.df['Date']>=st) & (self.df['Date']<=en)]
        return showing_proper_int(answered_cum['Inbound Calls'].sum())

    def answered_calls_yesteday(self):
        df_yesterday=self.df[(self.df['Date']==self.yesterday_class_data)]
        if df_yesterday.empty==True:
            return 0
        else:
            return showing_proper_int(df_yesterday['Inbound Calls'].sum())
        
    def answered_calls_today(self):
        df_today=self.df[(self.df['Date']==self.today_class_data)]
        if df_today.empty==True:
            return 0
        else:
            return showing_proper_int(df_today['Inbound Calls'].sum())
        
    #Outbound Calls 2nd Card
    def outbound_calls_cumulative(self,st,en):
        outbound_cumulative=self.df[(self.df['Date']>=st) & (self.df['Date']<=en)]
        return showing_proper_int(outbound_cumulative['Outbound Calls'].sum())

    def outbound_calls_yesterday(self):
        outbound_yesterday=self.df[(self.df['Date']==self.yesterday_class_data)]
        if outbound_yesterday.empty==True:
            return 0
        else:
            return showing_proper_int(outbound_yesterday['Outbound Calls'].sum())

    def outbound_calls_today(self):
        outbound_today=self.df[(self.df['Date']==self.today_class_data)]
        if outbound_today.empty==True:
            return 0
        else:
            return showing_proper_int(outbound_today['Outbound Calls'].sum(0))
    
    #Total Calls 3rd Card
    def total_calls_count(self,st,en):
        total_calls=self.df[(self.df['Date']>=st) & (self.df['Date']<=en)]
        return showing_proper_int(total_calls['Total Calls'].sum())

    def total_calls_yesterday(self):
        total_yesterday=self.df[(self.df['Date']==self.yesterday_class_data)]
        if total_yesterday.empty==True:
            return 0
        else:
            return showing_proper_int(total_yesterday['Total Calls'].sum())

    def total_calls_today(self):
        total_today=self.df[(self.df['Date']==self.today_class_data)]
        if total_today.empty==True:
            return 0
        else:
            return showing_proper_int(total_today['Total Calls'].sum())
    
    #4th Card Agents Count
    def agents_count(self,st,en):
        agents_count_total=self.df[(self.df['Date']>=st) & (self.df['Date']<=en)]
        agents_count_to=int(0)
        return agents_count_to
        
    #5th Card: Agents Active status Count
    def agents_active_status_count(self,st,en):
        agent_act_count=int(0)
        return agent_act_count

    def agents_idle_status_count(self,st,en):
        agent_idle_count=int(0)
        return agent_idle_count
    
    #6th card: AHT Calls (In sec.)==Yesterday Count
    def aht_calls_yest(self):
        aht_yest=self.df[(self.df['Average Call Handling Time']==self.yesterday_class_data)]
        if aht_yest.empty==True:
            m1=0
            return m1
        else:
            return showing_proper_int(aht_yest['Average Call Handling Time'].sum())
        
    def aht_calls_today(self):
        aht_today=self.df[(self.df['Average Call Handling Time']==self.today_class_data)]
        if aht_today.empty==True:
            n1=0
            return n1
        else:
            return showing_proper_int(aht_today['Average Call Handling Time'].sum())

    #7th card: Average Call Waiting Time
    def avg_call_waiting_time(self):
        avg_call_wait=int(0)
        return avg_call_wait
    
    #8Th Card: Agent Occupancy Rate
    def agent_occupancy_rate(self):
        agents_occupancy=0
        return agents_occupancy
    
    #9th Card
    def grievance_calls(self,st,en):
        grievance_calls=self.df[(self.df['Date']>=st) & (self.df['Date']<=en)]
        if grievance_calls.empty==True:
            return 0
        else:
            return showing_proper_int(grievance_calls['Grievance Received'].sum())

obj_1=Data_Reading()
