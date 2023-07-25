CREATE OR REPLACE FUNCTION public.get_adcirc_filename_variables(_run_id character varying)
 RETURNS json
 LANGUAGE plpgsql
AS $function$
    DECLARE _output json;
    DECLARE pivot_sql text =
	'SELECT ROW_TO_JSON(ct)
	FROM (SELECT *
	    FROM CROSSTAB
		(
		    ''SELECT
			instance_id as id,
			key,
			value
		    FROM
			"ASGS_Mon_config_item"
		    WHERE
			key IN (''''ADCIRCgrid'''', ''''advisory'''', ''''downloadurl'''', ''''forcing.metclass'''', ''''stormnumber'''', ''''RunStartTime'''',''''workflow_type'''')
			and instance_id || ''''-'''' || uid = ''''' || _run_id || '''''
		    ORDER BY id ASC, key ASC'',
		    ''SELECT data_source
		     FROM (VALUES
			(''''ADCIRCgrid''''),
			(''''advisory''''),
			(''''downloadurl''''),
			(''''forcing.metclass''''),
                        (''''stormnumber''''),
                        (''''RunStartTime''''),
                        (''''workflow_type'''')) b(data_source)''
		) AS (
		    id INT,
		    "ADCIRCgrid" TEXT,
		    "advisory" TEXT,
		    "downloadurl" TEXT,
                    "forcing.metclass" TEXT,
                    "stormnumber" TEXT,
                    "RunStartTime" TEXT,
		    "workflow_type" TEXT)) AS ct';

BEGIN
    -- gather the records and return them in json format
	EXECUTE (select pivot_sql) INTO _output;

    -- return the data to the caller
    return _output;
END
$function$
