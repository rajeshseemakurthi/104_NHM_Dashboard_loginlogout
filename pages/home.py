#Importing Necessary Modules
import dash,datetime,plotly.graph_objs as go
from datetime import date,timedelta
from dash import  dcc, html, Input, Output,ctx
from dash import html
import asyncio
from server import app
from sql_query_fetching_second import data_frame_1,data_frame_sec
import multiprocessing
from dash_extensions.enrich import Output, Input, callback_context
from data_reading import Data_Reading
from updated_date import Current_things
# from updated_date import Current_things,parquet_file_updation
from dash import html, dcc, callback, Input, Output


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


# dash.register_page(__name__, path='/'
#                             ,title='104_NHM Dashboard'
#                             ,name='104_nhm_dahsboard'
#                    )

first_day_of_current_month=date.today().replace(day=1)
previous_month_end_date=first_day_of_current_month-timedelta(days=1)
#Prevuous Month Number
previous_month=previous_month_end_date.month

# let
present_month_year=datetime.datetime.now().strftime("Y")

obj_home_datepicker=Current_things()

# data_picker=html.Div(dcc.DatePickerRange(id='my_date_picker_range_home',className="calender_class",display_format='DD-MM-Y',min_date_allowed=date(2019, 7, 31),max_date_allowed=obj_home_datepicker.current_date(),initial_visible_month=obj_home_datepicker.current_date(),start_date=date(2019, 7, 31),end_date=obj_home_datepicker.current_date(),style={"width":"110%","border-radius":"8px"}),style={})

def layout():
    return html.Div([
            html.Div([
                html.Img(src="./assets/image/assam_removebg.png",alt="assam_logo_main",className="assam_logo_main_class"),
                html.Img(src="./assets/image/piramal_logo.png",alt="piramal_logo_main",className="piramal_logo_main_class"),
                # html.Img(src="./assets/image/piramal_logo.png",alt="piramal_logo_main",className="piramal_logo_main_class"),
                html.P("104 NHM Dashboard",className="heading_title"),
                html.P("version:1.0",className="version_display")
                ],className="heading_bar"),
            html.Div([
                #For DatePicker
                html.Div([
                    html.A([html.Img(src="./assets/image/calendar_red.ico",alt="calender_image",className="calender_image_class")],href="/"),
                    dcc.DatePickerRange(id='my_date_picker_range_home',with_portal=True,className="calender_class",display_format='DD-MM-Y',min_date_allowed=date(2019, 7, 31),max_date_allowed=obj_home_datepicker.current_date(),initial_visible_month=obj_home_datepicker.current_date(),start_date=date(2019, 7, 31),end_date=obj_home_datepicker.current_date(),style={}),     
                    ],className="datepicker_main"),
                #For Error Displaying
                html.Div([],id="error_bar",className="error_class_bar"),
                #For Refresh Icon
                html.A([
                    html.Img(src="./assets/image/update_image_removebg.png",alt="refresh_initial",className="refresh_image")
                    ],href="/",className="refresh_home")
                ],className="middle_bar"),
            html.Div([
                #First Row
                html.Div([
                    #First Card
                    html.Div([
                        #Upper Part
                        html.P("Answered Call Volume",className="first_card_upper"),
                        #Lower part
                        html.Div([
                            html.Div([html.P("Cumulative: ",className="first_card_cumulative_count"),html.Div(id="first_card_cumulative_value")],className="first_card_lower_one"),
                            html.Div([html.P("Yesterday: ",className="first_card_yesterday_count"),html.Div(id="first_card_yesterday_value")],className="first_card_lower_two"),
                            html.Div([html.P("Today: ",className="first_card_today_count"),html.Div(id="first_card_today_value")],className="first_card_lower_three"),
                            ],className="first_card_lower")
                        ],className="first_card"),
                    #Second Card
                    html.Div([
                        #Upper Part
                        html.P("Outbound Call",className="second_card_upper"),
                        #Lower Part
                        html.Div([html.P("Cumulative: ",className="second_card_cumulative_count"),html.Div(id="second_card_lower_cumulative_value")],className="second_card_lower_one"),
                        html.Div([html.P("Yesterday: ",className="second_card_yesterday_count"),html.Div(id="second_card_yesterday_value")],className="second_card_lower_two"),
                        html.Div([html.P("Today: ",className="second_card_today_count"),html.Div(id="second_card_today_value")],className="second_card_lower_three"),
                    ],className="second_card"),
                    #Third Card Starting
                    html.Div([
                        html.P("Total Calls",className="third_card_upper_text"),
                        html.Div([html.P("Cumulative: ",className="third_card_cumulative"),html.Div(id="third_card_cumulative_value")],className="third_card_lower_one"),
                        html.Div([html.P("Yesterday: ",className="third_card_yesterday_count"),html.Div(id="third_card_yesterday_value")],className="third_card_lower_two"),
                        html.Div([html.P("Today: ",className="third_card_today_count"),html.Div(id="third_card_today_value")],className="third_card_lower_three"),
                        ],className="third_card"),
                        # html.Div([],className="fourth_card"),
                    ],className="first_row"),
                #Second Row
                html.Div([
                    #Fourth Card
                    html.Div([
                        #UpperPart
                        html.P("Agents Count",className="fourth_card_upper_part"),
                        #Lower Part
                        html.Div([html.P("Total: ",className="fourth_card_lower_text"),html.Div(id="fourth_card_lower_total_value")],className="fourth_card_lower_part")
                        ],className="fourth_card"),
                    #Fifth Card
                    html.Div([
                        html.P("Agents Active Status Count",className="fifth_card_upper"),
                        html.Div([
                            html.Div([html.P("Agents Active Status Count: ",className="fifth_card_lower_first_text"),html.Div(id="fifth_card_lower_first_value")],className="fifth_card_lower_one"),
                            html.Div([html.P("Agents Idle Status Count: ",className="fifth_card_lower_second_text"),html.Div(id="fifth_card_lower_second_value")],className="fifth_card_lower_two")
                            ],className="fifth_card_lower")
                        ],className="fifth_card"),
                    #Sixth card                        
                    html.Div([
                        #Upper Part
                        html.P("AHT Calls(in Sec.)",className="sixth_card_upper_part"),
                        #Lower Part
                        html.Div([
                            html.Div([html.P("Yesterday: ",className="sixth_card_lower_one_text"),html.Div(id="sixth_card_lower_one_value")],className="sixth_card_lower_one"),
                            html.Div([html.P("Today: ",className="sixth_card_lower_two_text"),html.Div(id="sixth_card_lower_two_value")],className="sixth_card_lower_two"),
                            html.A([html.Img(className="right_arrow_first",src="./assets/image/right_arrow_bg.png",alt="right_arrow_first_icon")],className="right_arrow_first_icon_class",href=dash.page_registry['pages.c_aht_calls']['path'],style={})
                            # html.A([html.I(className="fa fa-arrow-circle-right",style={})],className="second_card_left_arrow_icon",href=dash.page_registry['pages.c_mmu']['path'],style={"width":"1.5rem","height":"1.5rem","margin-left":"12.8rem","margin-top":"-0.6rem"})
                            ],className="sixth_card_lower_part")
                        ],className="sixth_card"),
                    ],className="second_row"),
                #Third Row
                html.Div([
                    #Seventh Card
                    html.Div([
                        #Upper Part
                        html.P("Average call Waiting Time",className="seventh_card_upper"),
                        #Lower Part
                        html.Div([html.P("Value(%): ",className="seventh_lower_first_text"),html.Div(id="seventh_lower_first_value")],className="seventh_card_lower")
                        ],className="seventh_card"),
                    #Eigth Card
                    html.Div([
                        #Upper Part
                        html.P("Agent Occupancy Rate",className="eigth_card_upperpart"),
                        #Lower Card
                        html.Div([html.P("Value(%): ",className="eigth_card_lower_one_text"),html.Div(id="eigth_card_lower_one_value")],className="eigth_card_lower"),
                        html.A([html.Img(className="right_arrow_second",src="./assets/image/right_arrow_bg.png",alt="right_arrow_second_icon")],className="right_arrow_second_icon_class",href="/",style={})
                        ],className="eigth_card"),
                    #Ninth Card
                    html.Div([
                        #Upper Card
                        html.P("Grievance Calls",className="ninth_upper_card"),
                        #Lower Card
                        html.Div([html.P("Received+Adressed: ",className="ninth_card_lower_one_text"),html.Div(id="ninth_card_lower_one_value")],className="ninth_card_lower"),
                        html.P("(Total Count) ",className="Ninth_card_lower_additional_text")
                        ],className="ninth_card"),
                    ],className="third_row")
                ],className="cards_div"),
            html.P(["@Powered by Piramal Swasthya."],className="last_tag"),
            dcc.Interval(id='interval-component',
                        interval=5*60*1000, # updates in every 5 Minutes
                        n_intervals=20),
            ],className="main_class")


@app.callback([Output('my_date_picker_range_home',"max_date_allowed"),
        Output('my_date_picker_range_home',"initial_visible_month"),
        Output('my_date_picker_range_home',"end_date")],
        [Input('interval-component', 'n_intervals')])
def date_updation(n_intervals):
    obj_home_date_callbacks=Data_Reading()
    if n_intervals:
        return [obj_home_date_callbacks.today_rep_data,obj_home_date_callbacks.today_rep_data,obj_home_date_callbacks.today_rep_data]
    else:
        return [obj_home_date_callbacks.today_rep_data,obj_home_date_callbacks.today_rep_data,obj_home_date_callbacks.today_rep_data]
    

@app.callback(
            #first card
            Output('first_card_cumulative_value','children'),
            Output('first_card_yesterday_value','children'),
            Output('first_card_today_value','children'),
            #2nd Card
            Output('second_card_lower_cumulative_value','children'),
            Output('second_card_yesterday_value','children'),            
            Output('second_card_today_value','children'),            
            #3rd Card
            Output('third_card_cumulative_value','children'),
            Output('third_card_yesterday_value','children'),
            Output('third_card_today_value','children'),
            #4th Card
            Output('fourth_card_lower_total_value','children'),
            #5th Card
            Output('fifth_card_lower_first_value','children'),
            Output('fifth_card_lower_second_value','children'),
            #6th Card
            Output('sixth_card_lower_one_value','children'),
            Output('sixth_card_lower_two_value','children'),
            #7th Card
            Output('seventh_lower_first_value','children'),
            #8th Card
            Output('eigth_card_lower_one_value','children'),
            #9th Card
            Output('ninth_card_lower_one_value','children'),
            #Output Error for Date
            Output('error_bar','children'),
            #Input for Taking's
            inputs=dict(n_intervals=Input('interval-component', 'n_intervals'),start_date=Input('my_date_picker_range_home','start_date'),end_date=Input('my_date_picker_range_home','end_date')),
        )
def marker_click_loc(n_intervals:int,start_date:str,end_date:str):
    p1=multiprocessing.Process(target=data_frame_1,args=[query1])
    p2=multiprocessing.Process(target=data_frame_sec,args=[query2])
    p1.start()
    p2.start()
    obj_home_callbacks=Data_Reading()
    if n_intervals:
        marker_id=dash.callback_context.triggered[0]
        button_clicked = ctx.triggered_id
        st = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        en =  datetime.datetime.strptime(end_date, '%Y-%m-%d')
                
        if (button_clicked==None):
            return obj_home_callbacks.answered_cumulative(st,en),obj_home_callbacks.answered_calls_yesteday(),obj_home_callbacks.answered_calls_today(),obj_home_callbacks.outbound_calls_cumulative(st,en),obj_home_callbacks.outbound_calls_yesterday(),obj_home_callbacks.outbound_calls_today(),obj_home_callbacks.total_calls_count(st,en),obj_home_callbacks.total_calls_yesterday(),obj_home_callbacks.total_calls_today(),obj_home_callbacks.agents_count(st,en),obj_home_callbacks.agents_active_status_count(st,en),obj_home_callbacks.agents_idle_status_count(st,en),obj_home_callbacks.aht_calls_yest(),obj_home_callbacks.aht_calls_today(),obj_home_callbacks.avg_call_waiting_time(),obj_home_callbacks.agent_occupancy_rate(),obj_home_callbacks.grievance_calls(st,en)," "
        elif (button_clicked=='start_date'):
            if (st>en):
                return " "," "," "," "," "," "," "," "," "," "," "," "," "," "," "," "," ","Start: {} Date Greater Than End: {} Date Value.".format(st,en)
            elif (st==en):
                return obj_home_callbacks.answered_cumulative(st,en),obj_home_callbacks.answered_calls_yesteday(),obj_home_callbacks.answered_calls_today(),obj_home_callbacks.outbound_calls_cumulative(st,en),obj_home_callbacks.outbound_calls_yesterday(),obj_home_callbacks.outbound_calls_today(),obj_home_callbacks.total_calls_count(st,en),obj_home_callbacks.total_calls_yesterday(),obj_home_callbacks.total_calls_today(),obj_home_callbacks.agents_count(st,en),obj_home_callbacks.agents_active_status_count(st,en),obj_home_callbacks.agents_idle_status_count(st,en),obj_home_callbacks.aht_calls_yest(),obj_home_callbacks.aht_calls_today(),obj_home_callbacks.avg_call_waiting_time(),obj_home_callbacks.agent_occupancy_rate(),obj_home_callbacks.grievance_calls(st,en)," "
            elif (st<en):
                return obj_home_callbacks.answered_cumulative(st,en),obj_home_callbacks.answered_calls_yesteday(),obj_home_callbacks.answered_calls_today(),obj_home_callbacks.outbound_calls_cumulative(st,en),obj_home_callbacks.outbound_calls_yesterday(),obj_home_callbacks.outbound_calls_today(),obj_home_callbacks.total_calls_count(st,en),obj_home_callbacks.total_calls_yesterday(),obj_home_callbacks.total_calls_today(),obj_home_callbacks.agents_count(st,en),obj_home_callbacks.agents_active_status_count(st,en),obj_home_callbacks.agents_idle_status_count(st,en),obj_home_callbacks.aht_calls_yest(),obj_home_callbacks.aht_calls_today(),obj_home_callbacks.avg_call_waiting_time(),obj_home_callbacks.agent_occupancy_rate(),obj_home_callbacks.grievance_calls(st,en)," "
            else:
                return obj_home_callbacks.answered_cumulative(st,en),obj_home_callbacks.answered_calls_yesteday(),obj_home_callbacks.answered_calls_today(),obj_home_callbacks.outbound_calls_cumulative(st,en),obj_home_callbacks.outbound_calls_yesterday(),obj_home_callbacks.outbound_calls_today(),obj_home_callbacks.total_calls_count(st,en),obj_home_callbacks.total_calls_yesterday(),obj_home_callbacks.total_calls_today(),obj_home_callbacks.agents_count(st,en),obj_home_callbacks.agents_active_status_count(st,en),obj_home_callbacks.agents_idle_status_count(st,en),obj_home_callbacks.aht_calls_yest(),obj_home_callbacks.aht_calls_today(),obj_home_callbacks.avg_call_waiting_time(),obj_home_callbacks.agent_occupancy_rate(),obj_home_callbacks.grievance_calls(st,en)," "
        elif (button_clicked=='end_date'):
            if (st>en):
                return " "," "," "," "," "," "," "," "," "," "," "," "," "," "," "," "," ","Start: {} Date Greater Than End: {} Date Value.".format(st,en)
            elif (st==en):
                return obj_home_callbacks.answered_cumulative(st,en),obj_home_callbacks.answered_calls_yesteday(),obj_home_callbacks.answered_calls_today(),obj_home_callbacks.outbound_calls_cumulative(st,en),obj_home_callbacks.outbound_calls_yesterday(),obj_home_callbacks.outbound_calls_today(),obj_home_callbacks.total_calls_count(st,en),obj_home_callbacks.total_calls_yesterday(),obj_home_callbacks.total_calls_today(),obj_home_callbacks.agents_count(st,en),obj_home_callbacks.agents_active_status_count(st,en),obj_home_callbacks.agents_idle_status_count(st,en),obj_home_callbacks.aht_calls_yest(),obj_home_callbacks.aht_calls_today(),obj_home_callbacks.avg_call_waiting_time(),obj_home_callbacks.agent_occupancy_rate(),obj_home_callbacks.grievance_calls(st,en)," "
            elif (st<en):
                return obj_home_callbacks.answered_cumulative(st,en),obj_home_callbacks.answered_calls_yesteday(),obj_home_callbacks.answered_calls_today(),obj_home_callbacks.outbound_calls_cumulative(st,en),obj_home_callbacks.outbound_calls_yesterday(),obj_home_callbacks.outbound_calls_today(),obj_home_callbacks.total_calls_count(st,en),obj_home_callbacks.total_calls_yesterday(),obj_home_callbacks.total_calls_today(),obj_home_callbacks.agents_count(st,en),obj_home_callbacks.agents_active_status_count(st,en),obj_home_callbacks.agents_idle_status_count(st,en),obj_home_callbacks.aht_calls_yest(),obj_home_callbacks.aht_calls_today(),obj_home_callbacks.avg_call_waiting_time(),obj_home_callbacks.agent_occupancy_rate(),obj_home_callbacks.grievance_calls(st,en)," "
            else:
                return obj_home_callbacks.answered_cumulative(st,en),obj_home_callbacks.answered_calls_yesteday(),obj_home_callbacks.answered_calls_today(),obj_home_callbacks.outbound_calls_cumulative(st,en),obj_home_callbacks.outbound_calls_yesterday(),obj_home_callbacks.outbound_calls_today(),obj_home_callbacks.total_calls_count(st,en),obj_home_callbacks.total_calls_yesterday(),obj_home_callbacks.total_calls_today(),obj_home_callbacks.agents_count(st,en),obj_home_callbacks.agents_active_status_count(st,en),obj_home_callbacks.agents_idle_status_count(st,en),obj_home_callbacks.aht_calls_yest(),obj_home_callbacks.aht_calls_today(),obj_home_callbacks.avg_call_waiting_time(),obj_home_callbacks.agent_occupancy_rate(),obj_home_callbacks.grievance_calls(st,en)," "
        else:
            return obj_home_callbacks.answered_cumulative(st,en),obj_home_callbacks.answered_calls_yesteday(),obj_home_callbacks.answered_calls_today(),obj_home_callbacks.outbound_calls_cumulative(st,en),obj_home_callbacks.outbound_calls_yesterday(),obj_home_callbacks.outbound_calls_today(),obj_home_callbacks.total_calls_count(st,en),obj_home_callbacks.total_calls_yesterday(),obj_home_callbacks.total_calls_today(),obj_home_callbacks.agents_count(st,en),obj_home_callbacks.agents_active_status_count(st,en),obj_home_callbacks.agents_idle_status_count(st,en),obj_home_callbacks.aht_calls_yest(),obj_home_callbacks.aht_calls_today(),obj_home_callbacks.avg_call_waiting_time(),obj_home_callbacks.agent_occupancy_rate(),obj_home_callbacks.grievance_calls(st,en)," "
    else:
        return dash.no_update,obj_home_callbacks.answered_calls_yesteday(),dash.no_update,obj_home_callbacks.outbound_calls_yesterday(),obj_home_callbacks.outbound_calls_today(),dash.no_update,obj_home_callbacks.total_calls_yesterday(),obj_home_callbacks.total_calls_today(),dash.no_update,dash.no_update,dash.no_update,obj_home_callbacks.aht_calls_yest(),obj_home_callbacks.aht_calls_today(),obj_home_callbacks.avg_call_waiting_time(),obj_home_callbacks.agent_occupancy_rate(),dash.no_update," "

