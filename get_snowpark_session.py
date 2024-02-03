from snowflake.snowpark import Session
import sys
import logging

def get_snowpark_session():
    connection_param = {
    "ACCOUNT":"xxxx.eu-central-1",
    "USER":"xxxxx",
    "PASSWORD":"xxxxxxxx",
    "role":"ACCOUNTADMIN",
    "database":"sales_dwh",
    "warehouse":"compute_wh"
}

    session = Session.builder.configs(connection_param).create()
    return session
