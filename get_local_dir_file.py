import os
from get_snowpark_session import get_snowpark_session

conn=get_snowpark_session()
directory_path = '/Users/srinivasuluchalla/tmp/end2end-sample-data/'


def traverse_directory(directory,file_extension) -> list:
    local_file_path = []
    file_name = []  # List to store CSV file paths
    partition_dir = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(file_extension):
                file_path = os.path.join(root, file)
                file_name.append(file)
                partition_dir.append(root.replace(directory, ""))
                local_file_path.append(file_path)

    return file_name,partition_dir,local_file_path


def main():
    csv_file_name, csv_partition_dir , csv_local_file_path= traverse_directory(directory_path,'.csv')
    parquet_file_name, parquet_partition_dir , parquet_local_file_path= traverse_directory(directory_path,'.parquet')
    json_file_name, json_partition_dir , json_local_file_path= traverse_directory(directory_path,'.json')
    stage_location = '@sales_dwh.source.my_internal_stg'
     
    csv_index = 0
    for file_element in csv_file_name:
        put_result = ( 
                    conn.file.put( 
                        csv_local_file_path[csv_index], 
                        stage_location+"/"+csv_partition_dir[csv_index], 
                        auto_compress=False, overwrite=True, parallel=10)
                    )
        csv_index+=1

    parquet_index = 0
    for file_element in parquet_file_name:

        put_result = ( 
                    conn.file.put( 
                        parquet_local_file_path[parquet_index], 
                        stage_location+"/"+parquet_partition_dir[parquet_index], 
                        auto_compress=False, overwrite=True, parallel=10)
                    )
        parquet_index+=1
    
    json_index = 0
    for file_element in parquet_file_name:

        put_result = ( 
                    conn.file.put( 
                        json_local_file_path[json_index], 
                        stage_location+"/"+json_partition_dir[json_index], 
                        auto_compress=False, overwrite=True, parallel=10)
                    )
        json_index+=1  


        #2 
        conn.sql(" \
            copy into sales_dwh.source.in_sales_order from ( \
            select \
            in_sales_order_seq.nextval, \
            t.$1::text as order_id, \
            t.$2::text as customer_name, \
            t.$3::text as mobile_key,\
            t.$4::number as order_quantity, \
            t.$5::number as unit_price, \
            t.$6::number as order_valaue,  \
            t.$7::text as promotion_code , \
            t.$8::number(10,2)  as final_order_amount,\
            t.$9::number(10,2) as tax_amount,\
            t.$10::date as order_dt,\
            t.$11::text as payment_status,\
            t.$12::text as shipping_status,\
            t.$13::text as payment_method,\
            t.$14::text as payment_provider,\
            t.$15::text as mobile,\
            t.$16::text as shipping_address,\
            metadata$filename as stg_file_name,\
            metadata$file_row_number as stg_row_numer,\
            metadata$file_last_modified as stg_last_modified\
            from \
            @sales_dwh.source.my_internal_stg/sales/source=IN/format=csv/ \
            (                                                             \
                file_format => 'sales_dwh.common.my_csv_format'           \
            ) t  )  on_error = 'Continue'     \
            "
            ).collect()

        conn.sql(' copy into sales_dwh.source.us_sales_order                \
            from                                    \
            (                                       \
                select                              \
                us_sales_order_seq.nextval, \
                $1:"Order ID"::text as orde_id,   \
                $1:"Customer Name"::text as customer_name,\
                $1:"Mobile Model"::text as mobile_key,\
                to_number($1:"Quantity") as quantity,\
                to_number($1:"Price per Unit") as unit_price,\
                to_decimal($1:"Total Price") as total_price,\
                $1:"Promotion Code"::text as promotion_code,\
                $1:"Order Amount"::number(10,2) as order_amount,\
                to_decimal($1:"Tax") as tax,\
                $1:"Order Date"::date as order_dt,\
                $1:"Payment Status"::text as payment_status,\
                $1:"Shipping Status"::text as shipping_status,\
                $1:"Payment Method"::text as payment_method,\
                $1:"Payment Provider"::text as payment_provider,\
                $1:"Phone"::text as phone,\
                $1:"Delivery Address"::text as shipping_address,\
                metadata$filename as stg_file_name,\
                metadata$file_row_number as stg_row_numer,\
                metadata$file_last_modified as stg_last_modified\
                from                                \
                    @sales_dwh.source.my_internal_stg/sales/source=US/format=parquet/\
                    (file_format => sales_dwh.common.my_parquet_format)\
                    ) on_error = continue \
                 '
            ).collect()
        
        conn.sql(' \
        copy into sales_dwh.source.fr_sales_order                                \
        from                                                    \
        (                                                       \
            select                                              \
            sales_dwh.source.fr_sales_order_seq.nextval,         \
            $1:"Order ID"::text as orde_id,                   \
            $1:"Customer Name"::text as customer_name,          \
            $1:"Mobile Model"::text as mobile_key,              \
            to_number($1:"Quantity") as quantity,               \
            to_number($1:"Price per Unit") as unit_price,       \
            to_decimal($1:"Total Price") as total_price,        \
            $1:"Promotion Code"::text as promotion_code,        \
            $1:"Order Amount"::number(10,2) as order_amount,    \
            to_decimal($1:"Tax") as tax,                        \
            $1:"Order Date"::date as order_dt,                  \
            $1:"Payment Status"::text as payment_status,        \
            $1:"Shipping Status"::text as shipping_status,      \
            $1:"Payment Method"::text as payment_method,        \
            $1:"Payment Provider"::text as payment_provider,    \
            $1:"Phone"::text as phone,                          \
            $1:"Delivery Address"::text as shipping_address ,    \
            metadata$filename as stg_file_name,\
            metadata$file_row_number as stg_row_numer,\
            metadata$file_last_modified as stg_last_modified\
            from                                                \
            @sales_dwh.source.my_internal_stg/sales/source=FR/format=json/\
            (file_format => sales_dwh.common.my_json_format)\
            ) on_error=continue\
        '
        ).collect() 

    #stg to curated 
from snowflake.snowpark import Session, DataFrame


if __name__ == '__main__':
    main()


