from snowflake.snowpark.session import Session
import snowflake.connector as sf



connection_param = {
    "ACCOUNT":"NJ81018.eu-central-1",
    "USER":"vasuluchalla",
    "PASSWORD":"Saianj@123",
    "role":"sysadmin",
    "database":"cricket",
    "warehouse":"compute_wh"
}

session = Session.builder.configs(connection_param).create()








