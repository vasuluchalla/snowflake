from snowflake.snowpark.session import Session
import snowflake.connector as sf



connection_param = {
    "ACCOUNT":"sdfdsdfsds",
    "USER":"sdsdsds",
    "PASSWORD":"sdsdsdsds",
    "role":"sysadmin",
    "database":"cricket",
    "warehouse":"compute_wh"
}

session = Session.builder.configs(connection_param).create()








