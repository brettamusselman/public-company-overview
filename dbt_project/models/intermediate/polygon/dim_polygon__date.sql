{{ generate_date_dimension(
    source_relation=ref('stg_polygon__hist_ticker'), 
    datetime_column_name='EventDateTime',        
    pk_name='DateDimKey',                          
    date_column_name='EventDate'                  
) }}