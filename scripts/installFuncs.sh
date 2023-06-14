# This script creates extension tablefunc and installs the two SQL functions below. 
psql -v ON_ERROR_STOP=1 -d "apsviz_gauges" --username "{{ .Values.adminUser }}" <<-EOSQL
        CREATE EXTENSION tablefunc;
psql -v ON_ERROR_STOP=1 -d "apsviz_gauges" --username "{{ .Values.adminUser }}" -f get_obs_timeseries_station_data.sql
psql -v ON_ERROR_STOP=1 -d "apsviz_gauges" --username "{{ .Values.adminUser }}" -f get_forecast_timeseries_station_data.sql
