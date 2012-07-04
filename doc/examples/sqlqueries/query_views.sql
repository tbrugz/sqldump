select table_catalog, table_schema, table_name, view_definition, check_option, is_updatable
from information_schema.views
where view_definition is not null
${q2.filter}
order by table_catalog, table_schema, table_name
