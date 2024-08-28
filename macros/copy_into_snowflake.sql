{% macro macros_copy_json(table_nm, prefix, pattern) %} 
 
delete from {{var ('rawhist_db') }}.{{var ('wrk_schema')}}.{{ table_nm }};
 
COPY INTO {{var ('rawhist_db') }}.{{var ('wrk_schema')}}.{{ table_nm }} 
FROM 
(
SELECT
$1 AS SOURCE_DATA,
metadata$filename AS SOURCE_FILE_NAME,
metadata$file_row_number AS SOURCE_FILE_ROW_NUMBER,
CURRENT_TIMESTAMP() AS INSERT_DTS
FROM @{{ var('stage_name') }}{{ prefix }}
)
PATTERN = '{{ pattern }}'
FILE_FORMAT = {{var ('file_format_json') }}
PURGE={{ var('purge_status') }}
;
 
{% endmacro %}