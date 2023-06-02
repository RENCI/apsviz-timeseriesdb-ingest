CREATE OR REPLACE FUNCTION public.get_forecast_timeseries_station_data(_station_name character varying, _timemark character varying, _data_source character varying)
  RETURNS json 
  LANGUAGE plpgsql
AS $function$
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
                    coalesce(d.water_level) AS axis
                FROM
                   drf_gauge_data d
                JOIN
                   drf_gauge_source s ON s.source_id=d.source_id
                JOIN
                   drf_gauge_station g ON s.station_id=g.station_id
                WHERE
                   g.station_name = ''''' || _station_name || ''''' AND
                   timemark = ''''' || _timemark || ''''' AND data_source = ''''' || _data_source || '''''
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
$function$

