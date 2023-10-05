create function get_obs_timeseries_station_data(_station_name character varying, _start_date character varying, _end_date character varying, _nowcast_source character varying) returns json
    language plpgsql
as
$$
   DECLARE _output json;
   DECLARE pivot_sql text =
       'SELECT JSON_AGG(ct)
        FROM (SELECT *
           FROM CROSSTAB
               (
                ''SELECT
                    CAST(d.time AS TEXT) AS id,
                    s.data_source AS category,
                    COALESCE(d.water_level,
                    d.stream_elevation,
                    d.wave_height,
                    d.wind_speed,
                    d.air_pressure,
                    d.flow_volume) AS yaxis
                FROM
                   drf_gauge_data d
                JOIN
                   drf_gauge_source s ON s.source_id=d.source_id
                JOIN
                   drf_gauge_station g ON s.station_id=g.station_id
                WHERE
                   g.station_name= ''''' || _station_name || ''''' AND
                   time >= ''''' || _start_date || ''''' AND time <= ''''' || _end_date || '''''
                ORDER BY d.time'',
                ''SELECT data_source FROM (VALUES (''''air_barometer''''),
                                                  (''''' || _nowcast_source || '''''),
                                                  (''''ocean_buoy''''),
                                                  (''''tidal_gauge''''),
                                                  (''''tidal_predictions''''),
                                                  (''''coastal_gauge''''),
                                                  (''''river_gauge''''),
                                                  (''''stream_gauge''''), 
                                                  (''''wind_anemometer'''')) b(data_source)''
                ) AS (
                 time_stamp TEXT,
                 "air_barometer" double precision,
                 ' || SPLIT_PART(_nowcast_source::TEXT, '.', 1) || SPLIT_PART(_nowcast_source::TEXT, '.', 2) || ' double precision,
                 "ocean_buoy_wave_height" double precision,
                 "tidal_gauge_water_level" double precision,
                 "tidal_predictions" double precision,
                 "coastal_gauge_water_level" double precision,
                 "river_gauge_water_level" double precision,
                 "stream_gauge_stream_elevation" double precision,
                 "wind_anemometer" double precision
                )) AS ct';
BEGIN
    -- gather the records and return them in json format
        EXECUTE (select pivot_sql) INTO _output;

    -- return the data to the caller
    return _output;
END
$$;

alter function get_obs_timeseries_station_data(varchar, varchar, varchar, varchar) owner to postgres;

grant execute on function get_obs_timeseries_station_data(varchar, varchar, varchar, varchar) to apsviz_ingester;

