create function get_nowcast_timeseries_station_data(_station_name character varying, _start_date character varying, _end_date character varying, _data_source character varying, _source_instance character varying) returns json
    language plpgsql
as
$$
   DECLARE
       _output json;
   DECLARE pivot_sql text =
       'SELECT JSON_AGG(ct)
        FROM (SELECT *
           FROM CROSSTAB
               (
                ''SELECT
                    CAST(d.time AS TEXT) AS id,
                    s.data_source AS category,
                    COALESCE(d.water_level) AS axis
                FROM
                   drf_model_data d
                JOIN
                   drf_model_source s ON s.source_id=d.source_id
                JOIN
                   drf_gauge_station g ON s.station_id=g.station_id
                WHERE
                   g.station_name = ''''' || _station_name || ''''' AND
                   time >= ''''' || _start_date || ''''' AND time <= ''''' || _end_date || ''''' AND 
                   data_source = ''''' || _data_source || '''''  AND
                   source_instance = ''''' || _source_instance || '''''
                ORDER BY d.time''
                ) AS (
                 time_stamp TEXT,
                 ' || SPLIT_PART(_data_source::TEXT, '.', 1) || SPLIT_PART(_data_source::TEXT, '.', 2) || ' double precision
                )) AS ct';
BEGIN
    -- gather the records and return them in json format
        EXECUTE (select pivot_sql) INTO _output;

    -- return the data to the caller
    return _output;
END
$$;

alter function get_nowcast_timeseries_station_data(varchar, varchar, varchar, varchar, varchar) owner to postgres;

grant execute on function get_nowcast_timeseries_station_data(varchar, varchar, varchar, varchar, varchar) to apsviz_ingester;

