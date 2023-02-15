import dash_html_components as html
import dash_core_components as dcc
import dash_bootstrap_components as dbc
from dash.dependencies import Input,Output,State
from dash import no_update

from flask_login import login_user, current_user
from werkzeug.security import check_password_hash
import time

from server import app, User


success_alert = dbc.Alert(
    'Logged in successfully. Taking you home!',
    color='success',
    dismissable=True
)
failure_alert = dbc.Alert(
    'Login unsuccessful. Try again.',
    color='danger',
    dismissable=True
)
already_login_alert = dbc.Alert(
    'User already logged in. Taking you home!',
    color='warning',
    dismissable=True
)


def layout():
    return html.Div([
                    html.Div([
                            #First Part
                            html.Div([
                                html.Img(src="./assets/image/assam_removebg.png",alt="login_assam_logo_class",className="login_first_image"),
                                html.P("104 NHM Dashboard",className="login_name"),
                                html.Img(src="./assets/image/piramal_logo.png",alt="login_piramal_logo_class",className="login_second_image")
                                ],className="login_header"),
                            #Second Part
                            dbc.Row(
                            dbc.Col(
                                [
                                    dcc.Location(id='login-url',refresh=True,pathname='/login'),
                                    html.Div(id='login-trigger',style=dict(display='none')),
                                    html.Div(id='login-alert'),
                                    dbc.FormGroup(
                                        [
                                            # dbc.Alert('Try test@test.com / test', color='info',dismissable=True),
                                            # html.Br(),
                                            html.Div([
                                                dbc.FormText('Email'),
                                                dbc.Input(id='login-email',autoFocus=True)
                                            ],className="login_internal_first_div"),
                                            
                                            html.Br(),
                                            html.Div([
                                                dbc.FormText('Password'),
                                                dbc.Input(id='login-password',type='password',debounce=True),
                                            ],className="login_internal_second_div"),
                                            html.Br(),
                                            dbc.Button('Submit',color='primary',id='login-button', n_clicks=0),
                                            #dbc.FormText(id='output-state')
                                            
                                            html.Br(),
                                            html.Br(),
                                            dcc.Link('Register',href='/register'),
                                            html.Br(),
                                            dcc.Link('Forgot Password',href='/forgot')
                                        ],className="login_form_group_internal"
                                    )
                                ],
                                width=6
                            )
                        )],className="login_internal_class")
                    ],className="login_main_class")

@app.callback(
    [Output('login-url', 'pathname'),
     Output('login-alert', 'children')],
    [Input('login-button', 'n_clicks'),
     Input('login-password', 'value')],
    [State('login-email', 'value')]
)
def login_success(n_clicks, password, email):
    '''
    logs in the user
    '''
    if password is not None or n_clicks > 0:
        user = User.query.filter_by(email=email).first()
        if user:
            if check_password_hash(user.password, password):
                login_user(user)

                return '/home',success_alert
            else:
                return no_update,failure_alert
        else:
            return no_update,failure_alert
    else:
        return no_update,''
