-- Alter Table Operations
ALTER TABLE {{ params.table_name }}
{%- for op in params.operations %}
  {%- if op.action == 'add_columns' %}
ADD COLUMNS (
    {%- for col in op.columns %}
  {{ col.name }} {{ col.type }} COMMENT '{{ col.comment }}'{{ "," if not loop.last }}
    {%- endfor %}
)
  {%- elif op.action == 'change_column' %}
CHANGE COLUMN {{ op.old_name }} {{ op.new_name }} {{ op.type }}
  {%- endif %}
{{- ";" if not loop.last }}
{%- endfor %};
