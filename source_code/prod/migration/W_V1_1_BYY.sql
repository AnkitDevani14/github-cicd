use database ASQN_DEV_BYY_DB;
use schema DBO;

Create or replace table ASQN_dev_BYY_DB.DBO.Demo
(
    PATH_ID               NUMBER(10)       NOT NULL,
    PATH_NAME             VARCHAR2(100)    NOT NULL,
    PATH_DESCRIPTION      VARCHAR2(255),
    CREATED_AT            TIMESTAMP        DEFAULT CURRENT_TIMESTAMP,
    UPDATED_AT            TIMESTAMP        DEFAULT CURRENT_TIMESTAMP 

);

CREATE OR REPLACE PROCEDURE "ARCHIVEBYYMEASUREPERFORMANCEBYYEARRANGE"("STARTYEAR" NUMBER(38,0), "ENDYEAR" NUMBER(38,0))
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
        YEAR INT := :STARTYEAR;
        ARCHIVETABLENAME VARCHAR(128);
        SQL VARCHAR;
        ERRORMESSAGE VARCHAR(4000);
BEGIN
   

    WHILE (:YEAR <= :ENDYEAR) LOOP
      BEGIN
        BEGIN TRANSACTION;

        -- Generate archive table name
        ARCHIVETABLENAME := ''ASQN_DEV_BYY_DB.''||''DBO.''||''BYY_Measure_Performance_Archive_'' || CAST(:YEAR AS VARCHAR);
        -- Create archive table if it does not exist
        IF (NOT ASQN_DEV_BYY_DB.DBO.TABLE_OBJECT_ID_UDF(:ARCHIVETABLENAME)) THEN
            BEGIN
                SQL := ''CREATE OR REPLACE TABLE '' || :ARCHIVETABLENAME || '' AS
                        SELECT TOP 0 * FROM ASQN_DEV_BYY_DB.DBO.BYY_Measure_Performance;'';

                EXECUTE IMMEDIATE :SQL;
            END;
            END IF;

     -- Insert data into archive table
            SQL := ''INSERT INTO '' || :ARCHIVETABLENAME || ''
                    SELECT * FROM ASQN_DEV_BYY_DB.DBO.BYY_Measure_Performance
                    WHERE YEAR(REPORT_MONTH :: TIMESTAMP) = @Y;'';
         
                EXECUTE IMMEDIATE ASQN_DEV_BYY_DB.DBO.TRANSFORM_SP_EXECUTE_SQL_STRING_UDF(:SQL, ''@Y INT'',           ARRAY_CONSTRUCT(''Y''), ARRAY_CONSTRUCT(:YEAR));

-- Delete data from source table
                DELETE FROM
                    ASQN_DEV__DB.DBO.BYY_Measure_Performance
                WHERE
                    YEAR(REPORT_MONTH :: TIMESTAMP) = :YEAR;

        

COMMIT;
EXCEPTION
        WHEN OTHER THEN
            errormessage :=SQLERRM;
            ROLLBACK ;
                     

                  SELECT 
                    ASQN_DEV_BYY_DB.DBO.RAISERROR_UDF(''Error archiving year %d: %s'', 16, 1, array_construct(:YEAR, :errormessage));
                    RETURN '''';
                                
        
END;
YEAR := YEAR + 1;        
END LOOP;

END;
';

CREATE OR REPLACE FUNCTION "QUOTENAME_UDF"("STR" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
IMMUTABLE
AS '
    SELECT CONCAT(''"'', STR, ''"'')
';

 
CREATE OR REPLACE PROCEDURE "ARCHIVEBYYMEASUREPERFORMANCEBYYEARRANGE"("STARTYEAR" NUMBER(38,0), "ENDYEAR" NUMBER(38,0))
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
        YEAR INT := :STARTYEAR;
        ARCHIVETABLENAME VARCHAR(128);
        SQL VARCHAR;
        ERRORMESSAGE VARCHAR(4000);
BEGIN
   
    WHILE (:YEAR <= :ENDYEAR) LOOP
      BEGIN
        BEGIN TRANSACTION;
        -- Generate archive table name
        ARCHIVETABLENAME := ''ASQN_DEV_BYY_DB.''||''DBO.''||''BYY_Measure_Performance_Archive_'' || CAST(:YEAR AS VARCHAR);
        -- Create archive table if it does not exist
        IF (NOT ASQN_DEV_BYY_DB.DBO.TABLE_OBJECT_ID_UDF(:ARCHIVETABLENAME)) THEN
            BEGIN
                SQL := ''CREATE OR REPLACE TABLE '' || :ARCHIVETABLENAME || '' AS
                        SELECT TOP 0 * FROM ASQN_DEV_BYY_DB.DBO.BYY_Measure_Performance;'';
                EXECUTE IMMEDIATE :SQL;
            END;
            END IF;
     -- Insert data into archive table
            SQL := ''INSERT INTO '' || :ARCHIVETABLENAME || ''
                    SELECT * FROM ASQN_DEV_BYY_DB.DBO.BYY_Measure_Performance
                    WHERE YEAR(REPORT_MONTH :: TIMESTAMP) = @Y;'';
         
                EXECUTE IMMEDIATE ASQN_DEV_BYY_DB.DBO.TRANSFORM_SP_EXECUTE_SQL_STRING_UDF(:SQL, ''@Y INT'',           ARRAY_CONSTRUCT(''Y''), ARRAY_CONSTRUCT(:YEAR));
-- Delete data from source table
                DELETE FROM
                    ASQN_DEV_BYY_DB.DBO.BYY_Measure_Performance
                WHERE
                    YEAR(REPORT_MONTH :: TIMESTAMP) = :YEAR;
        
COMMIT;
EXCEPTION
        WHEN OTHER THEN
            errormessage :=SQLERRM;
            ROLLBACK ;
                     
                  SELECT 
                    ASQN_DEV_BYY_DB.DBO.RAISERROR_UDF(''Error archiving year %d: %s'', 16, 1, array_construct(:YEAR, :errormessage));
                    RETURN '''';
                                
        
END;
YEAR := YEAR + 1;        
END LOOP;
END;
';
CREATE OR REPLACE FUNCTION "QUOTENAME_UDF"("STR" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
IMMUTABLE
AS '
    SELECT CONCAT(''"'', STR, ''"'')
';
 
 
CREATE OR REPLACE FUNCTION "QUOTENAME_UDF"("STR" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
IMMUTABLE
AS '
    SELECT CONCAT(''"'', STR, ''"'')
';
CREATE OR REPLACE FUNCTION "QUOTENAME_UDF"("STR" VARCHAR, "QUOTECHAR" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
IMMUTABLE
AS '
    SELECT
      CASE WHEN LEN(QUOTECHAR) = 1 THEN concat(QUOTECHAR, STR,QUOTECHAR)
      ELSE CONCAT(SUBSTR(QUOTECHAR,1,1),STR,SUBSTR(QUOTECHAR,2,1))
      END CASE
';
CREATE OR REPLACE FUNCTION "RAISERROR_UDF"("MSG_ID" FLOAT, "SEVERITY" FLOAT, "STATE" FLOAT, "PARAMS" ARRAY)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
IMMUTABLE
AS '
    var MSG = `MESSAGE: ${MSG_ID}, LEVEL: ${SEVERITY}, STATE: ${STATE}`;
    if(SEVERITY <= 10) {
        return MSG;
    } else {
        throw MSG;
    }
';
CREATE OR REPLACE FUNCTION "RAISERROR_UDF"("MSG_TEXT" VARCHAR, "SEVERITY" FLOAT, "STATE" FLOAT, "PARAMS" ARRAY)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
IMMUTABLE
AS '
    /**Regex for:
     * % [[flag] [width] [. precision] [{h | l}]] type
     * - flag: -, +, 0, #, '' ''
     * - width: number
     * - precision: number
     * - {h | l} type : d, i, o, s, u, x, X
    */
    const regex = /%(\\-|\\+|0|#|'''')*(\\*|[0-9])*(.(\\*|[0-9]))*(d|i|o|s|u|x|X)/g;
    const found = MSG_TEXT.match(regex);
    let paramIndex = 0
    for(let i=0; i < found.length; i++)
    {
        //Validate possible *.* expression
        if(found[i].match(/\\*/g) != null)
        {
            const asterisks = found[i].match(/\\*/g).length;
            const arguments = PARAMS.slice(paramIndex, paramIndex + asterisks);
            paramIndex = paramIndex + asterisks;
        }
   
        MSG_TEXT = MSG_TEXT.replace(found[i], PARAMS[paramIndex]);
        paramIndex++;
    }
    var MSG = `MESSAGE: ${MSG_TEXT}, LEVEL: ${SEVERITY}, STATE: ${STATE}`;
    if(SEVERITY <= 10) {
        return MSG;
    } else {
        throw MSG;
    }
';
 
CREATE OR REPLACE FUNCTION "TABLE_OBJECT_ID_UDF"("NAME" VARCHAR)
RETURNS BOOLEAN
LANGUAGE SQL
IMMUTABLE
AS '
    EXISTS (SELECT TABLE_SCHEMA || ''.'' || TABLE_NAME AS object_id FROM INFORMATION_SCHEMA.TABLES WHERE object_id = NAME)
';
 
CREATE OR REPLACE FUNCTION "TRANSFORM_SP_EXECUTE_SQL_STRING_UDF"("_SQL_STRING" VARCHAR, "_PARAMS_DEFINITION" VARCHAR, "_PARAMS_NAMES" ARRAY, "_PARAMS_VALUES" ARRAY)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
IMMUTABLE
AS '
    const param_regex = /@\\w+/i;
    const is_output_regex = /\\b(OUT|OUTPUT)\\b/i;
           
    function get_params_info(params_definition, params_names){
        const params_definition_list = params_definition.split('','');
        const lower_case_params_names = params_names.map(item => item.toLowerCase());
       
        let current_position = 0;
        return params_definition_list.map(param_definition => {
            const param_name = param_definition.match(param_regex)?.[0] || null;
            const is_output = is_output_regex.test(param_definition);
            let value_position = lower_case_params_names.indexOf(param_name.substring(1).toLowerCase());
 
            if (value_position === -1) {
                value_position = current_position;
            }
 
            current_position++;
            return [param_name, value_position, is_output];
        });
    }
 
    function replace_param_value(sql_string, param_name, value){
        const param_regex = new RegExp(`\\\\B${param_name}\\\\b`, ''gi'');
        const formatted_value = typeof value === ''string'' ? `''${value}''` : String(value);
        return sql_string.replace(param_regex, formatted_value);
    }
 
    function replace_output_param(sql_string, param_name){
        const assign_regex = new RegExp(`${param_name}\\\\s*=`, ''gi'');
        return sql_string.replace(assign_regex, '''');
    }
 
    function transform_sql_string(sql_string, params_definition, params_names, params_values){
        const params_info = get_params_info(params_definition, params_names);
        let new_sql_string = sql_string;
 
        params_info.forEach(([param_name, value_position, is_output]) => {
            new_sql_string = is_output
                ? replace_output_param(new_sql_string, param_name)
                : replace_param_value(new_sql_string, param_name, params_values[value_position]);
        });
 
        return new_sql_string;
    }    
 
    return transform_sql_string(_SQL_STRING, _PARAMS_DEFINITION, _PARAMS_NAMES, _PARAMS_VALUES);
';
 

 