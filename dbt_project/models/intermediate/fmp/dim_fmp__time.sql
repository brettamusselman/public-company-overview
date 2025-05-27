{{ generate_time_dimension(
    source_relation=ref('stg_fmp__hist_ticker'), 
    datetime_column_name='EventDateTime',        
    pk_name='TimeDimKey',                          
    time_column_name='EventTime'                  
) }}