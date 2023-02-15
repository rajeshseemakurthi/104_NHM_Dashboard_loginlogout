import dash,datetime
from datetime import date,timedelta
from updated_date import Current_things
from dash import html, dcc,dash_table
import pandas as pd
from server import app
from data_reading import Data_Reading
from pandas._libs.tslibs.timestamps import Timestamp
from dash import html, dcc, callback, Input, Output

dash.register_page(__name__, path='/c_aht_calls'
                            ,title='AHT Calls(in Sec.)'
                            ,name='AHT Calls(in Sec.)'
                   )

#Let n1 be total number of Months

start=datetime.datetime.now()
end=datetime.datetime.now()

start_str=datetime.datetime.now()
end_str=datetime.datetime.now()


first_day_of_current_month=date.today().replace(day=1)
previous_month_end_date=first_day_of_current_month-timedelta(days=1)
#Prevuous Month Number
previous_month=previous_month_end_date.month

# let
present_month_year=datetime.datetime.now().strftime("Y")

obj_aht=Current_things()

#Declaring Empty dataFrame
# df_aht=pd.DataFrame()

date_picker_aht_main_class=html.Div(dcc.DatePickerRange(id='my_date_picker_range_aht',className="datepicker_aht",display_format='DD-MM-Y',min_date_allowed=date(2019, 7, 31),max_date_allowed=obj_aht.current_date(),initial_visible_month=obj_aht.current_date(),start_date=date(2022, previous_month, 1),end_date=obj_aht.current_date(),style={}),style={})

st = Timestamp('2022-11-01 00:00:00')
st = st.to_pydatetime()

df_column_aht = pd.DataFrame({'Sl.No.': pd.Series(dtype='int'),
                        'CreatedBy': pd.Series(dtype='str'),
                        'Avg Call Time Handled(in Sec.)': pd.Series(dtype='float'),
                        })

def layout():
    return html.Div([
    html.Div([html.P("Average Call Time Handled(in Sec.)",className="aht_heading_title_paragraph")],className='aht_title',style={}),
    html.Div([
        date_picker_aht_main_class,
        html.A([html.P("Home",style={})],href='/',className="aht_home",style={}),
        html.A([html.I(className="fa fa-refresh",style={})],href=dash.page_registry['pages.c_aht_calls']['path'],className="aht_refresh",style={}),
        html.Button("Download", id="btn_csv"),
        # html.Img(src="./assets/image/download_skyblue_removebg.png",alt="download_image",id="download_im",className="download_img_class",style={}),
        # html.A([html.Img(src="./assets/image/download_skyblue_removebg.png",alt="download_image",id="download_im",className="download_img_class",style={})],href='/',className="download_img_class_main_link",style={}),
    ],className="middle_layer",style={}),
    html.Div([html.Div(id="c_aht_error",className="aht_error",style={})],style={}),
    html.Div([
    dash_table.DataTable(columns=[{'id': c, 'name': c} for c in df_column_aht.columns],id="table_aht",
                        style_cell={'overflow': 'hidden','textOverflow': 'ellipsis','maxWidth': '50%','textAlign': 'center'},
                        style_cell_conditional=[{'if': {'column_id': c},'textAlign': 'left'} for c in ['Date', 'Region']],
                        style_data={'color': 'black','backgroundColor': 'white'},
                        style_table={"width":"606px","margin-left":"18%","margin-bottom":"4%"},
                        style_data_conditional=[{'if': {'row_index': 'odd'},'backgroundColor': 'rgb(220, 220, 220)'}],
                        style_header={'backgroundColor':'rgb(210, 210, 210)','color': 'black','fontWeight': 'bold'}),
    dcc.Download(id="download_dataframe_csv")
    ],className="datepicker_class_specific"),
        dcc.Interval(
            id='interval-component',
            interval=5*60*1000, # updates in every 5 Minutes
            n_intervals=20
        )
])

@app.callback([Output('my_date_picker_range_aht',"max_date_allowed"),
        Output('my_date_picker_range_aht',"initial_visible_month"),
        Output('my_date_picker_range_aht',"end_date")],
        [Input('interval-component', 'n_intervals')])
def aht_date_updation(n_date):
    obj_aht_date_callbacks=Data_Reading()
    if n_date:
        return [obj_aht_date_callbacks.today_rep_data,obj_aht_date_callbacks.today_rep_data,obj_aht_date_callbacks.today_rep_data]
    else:
        return [obj_aht_date_callbacks.today_rep_data,obj_aht_date_callbacks.today_rep_data,obj_aht_date_callbacks.today_rep_data]


@app.callback(Output('table_aht','data'),Output('c_aht_error','children'),
    #Input for Objects 
    inputs=dict(n_intervals=Input('interval-component', 'n_intervals'),start_date=Input('my_date_picker_range_aht','start_date'),end_date=Input('my_date_picker_range_aht','end_date')))
def update_aht_table(n_intervals:int,start_date,end_date):
    obj_aht_callbacks=Data_Reading()
    global start_str
    global end_str
    global start
    global end
    if n_intervals:
        start_str=start_date
        end_str=end_date
        start = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        end =  datetime.datetime.strptime(end_date, '%Y-%m-%d')
        
        #If start Date > end Date Values ,Then taking Default Values from 01/11/2022 to till date   
        if start>end:
            df_aht=pd.DataFrame()
            df_aht_init=obj_aht_callbacks.df_secondary[(obj_aht_callbacks.df_secondary['Date']>=st) & (obj_aht_callbacks.df_secondary['Date']<=obj_aht_callbacks.today_class_data)]
            df_aht_initial=pd.DataFrame(df_aht_init.groupby(['CreatedBy'])['avg(timestampdiff(second,t.CallTime,t.CallEndTime))'].sum()).reset_index()
            df_aht['Sl.No.']=df_aht_initial.index
            df_aht['CreatedBy']=df_aht_initial['CreatedBy']
            df_aht['Avg Call Time Handled(in Sec.)']=df_aht_initial['avg(timestampdiff(second,t.CallTime,t.CallEndTime))']
            return df_aht.to_dict('records'),"Start: {} date is Greater Than End: {} Date Values..!".format(start,end)
        elif start==end:
            df_aht=pd.DataFrame()
            df_aht_init=obj_aht_callbacks.df_secondary[obj_aht_callbacks.df_secondary['Date']==start]
            df_aht_initial=pd.DataFrame(df_aht_init.groupby(['CreatedBy'])['avg(timestampdiff(second,t.CallTime,t.CallEndTime))'].sum()).reset_index()
            df_aht['Sl.No.']=df_aht_initial.index
            df_aht['CreatedBy']=df_aht_initial['CreatedBy']
            df_aht['Avg Call Time Handled(in Sec.)']=df_aht_initial['avg(timestampdiff(second,t.CallTime,t.CallEndTime))']          
            return df_aht.to_dict('records')," "
        elif start<end:
            df_aht=pd.DataFrame()
            df_aht_init=obj_aht_callbacks.df_secondary[(obj_aht_callbacks.df_secondary['Date']>=start) & (obj_aht_callbacks.df_secondary['Date']<=end)]
            df_aht_initial=pd.DataFrame(df_aht_init.groupby(['CreatedBy'])['avg(timestampdiff(second,t.CallTime,t.CallEndTime))'].sum()).reset_index()
            df_aht['Sl.No.']=df_aht_initial.index
            df_aht['CreatedBy']=df_aht_initial['CreatedBy']
            df_aht['Avg Call Time Handled(in Sec.)']=df_aht_initial['avg(timestampdiff(second,t.CallTime,t.CallEndTime))']
            return df_aht.to_dict('records')," "
    else:
        return dash.no_update,""
    

@app.callback(
    Output("download_dataframe_csv", "data"),
    Input("btn_csv", "n_clicks"),
    prevent_initial_call=True)
def func(n_clicks):
    obj_aht_callbacks=Data_Reading()
    global start_str
    global end_str
    global start
    global end
    if start>end:
        df_downloads=pd.DataFrame()
        df_downloads_init=obj_aht_callbacks.df_secondary[(obj_aht_callbacks.df_secondary['Date']>=st) & (obj_aht_callbacks.df_secondary['Date']<=obj_aht_callbacks.today_class_data)]
        df_downloads_initial=pd.DataFrame(df_downloads_init.groupby(['CreatedBy'])['avg(timestampdiff(second,t.CallTime,t.CallEndTime))'].sum()).reset_index()
        df_downloads['Sl.No.']=df_downloads_initial.index
        df_downloads['CreatedBy']=df_downloads_initial['CreatedBy']
        df_downloads['Avg Call Time Handled(in Sec.)']=df_downloads_initial['avg(timestampdiff(second,t.CallTime,t.CallEndTime))']
        return dcc.send_data_frame(df_downloads.to_csv, "data_from_{}_to_{}.csv".format(start_str,end_str))
        # return df_downloads.to_dict('records'),"Start: {} date is Greater Than End: {} Date Values..!".format(start,end)
    elif start==end:
        df_downloads=pd.DataFrame()
        df_downloads_init=obj_aht_callbacks.df_secondary[obj_aht_callbacks.df_secondary['Date']==start]
        df_aht_initial=pd.DataFrame(df_downloads_init.groupby(['CreatedBy'])['avg(timestampdiff(second,t.CallTime,t.CallEndTime))'].sum()).reset_index()
        df_downloads['Sl.No.']=df_downloads_initial.index
        df_downloads['CreatedBy']=df_downloads_initial['CreatedBy']
        df_downloads['Avg Call Time Handled(in Sec.)']=df_downloads_initial['avg(timestampdiff(second,t.CallTime,t.CallEndTime))']
        return dcc.send_data_frame(df_downloads.to_csv, "data_from_{}_to_{}.csv".format(start_str,end_str))
    elif start<end:
        df_downloads=pd.DataFrame()
        df_downloads_init=obj_aht_callbacks.df_secondary[(obj_aht_callbacks.df_secondary['Date']>=start) & (obj_aht_callbacks.df_secondary['Date']<=end)]
        df_downloads_initial=pd.DataFrame(df_downloads_init.groupby(['CreatedBy'])['avg(timestampdiff(second,t.CallTime,t.CallEndTime))'].sum()).reset_index()
        df_downloads['Sl.No.']=df_downloads_initial.index
        df_downloads['CreatedBy']=df_downloads_initial['CreatedBy']
        df_downloads['Avg Call Time Handled(in Sec.)']=df_downloads_initial['avg(timestampdiff(second,t.CallTime,t.CallEndTime))']
        return dcc.send_data_frame(df_downloads.to_csv, "data_from_{}_to_{}.csv".format(start_str,end_str))
