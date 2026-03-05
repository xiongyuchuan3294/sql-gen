-- Insert Values Query
INSERT INTO TABLE {{ params.table_name }} PARTITION ({{ params.partition }})
VALUES
{% for row in params.data_rows -%}
  ({{ row | join(', ') }}){{ ",\n" if not loop.last }}
{%- endfor %};
