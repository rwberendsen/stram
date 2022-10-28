SELECT
{%- for c in columns %}
  {{ ', ' if not loop.first else '  ' }}t.{{ c }}
{%- endfor %}
FROM {{ database }}.{{ schema }}.{{ table }} AS t
LEFT JOIN {{ referenced_database }}.{{ referenced_schema }}.{{ referenced_table }} AS ref
  ON
{%- for _ in columns %}
    {{ 'AND' if loop.index0 else '   ' }} t.{{ columns[loop.index0] }} = ref.{{ referenced_columns[loop.index0] }}
{%- endfor %}
WHERE ref.{{ referenced_columns[0] }} IS NULL
