{% macro config_core_streamline(schema="streamline") %}

- name: {{ schema }}.udf_rest_api
  signature:
    - [partition_key, STRING]
    - [method, STRING]
    - [url, STRING]
    - [headers, OBJECT]
    - [data, VARIANT]
    - [secret_name, STRING]
  return_type: ARRAY
  options: |
    NOT NULL
    RETURNS NULL ON NULL INPUT
    VOLATILE
  sql: |
    SELECT
      streamline.udf_rest_api(
          partition_key,
          method,
          url,
          headers,
          data,
          secret_name
      )
- name: {{ schema }}.udf_rest_api
  signature:
    - [partition_key, STRING]
    - [method, STRING]
    - [url, STRING]
    - [headers, OBJECT]
    - [data, VARIANT]
  return_type: ARRAY
  options: |
    NOT NULL
    RETURNS NULL ON NULL INPUT
    VOLATILE
  sql: |
    SELECT
      streamline.udf_rest_api(
          partition_key,
          method,
          url,
          headers,
          data,
          ''
      )
- name: {{ schema }}.udf_rest_api
  signature:
    - [partition_key, STRING]
    - [url, STRING]
    - [data, VARIANT]
  return_type: ARRAY
  options: |
    NOT NULL
    RETURNS NULL ON NULL INPUT
    VOLATILE
  sql: |
    SELECT
      streamline.udf_rest_api(
          partition_key,
          'POST',
          url,
          {'Content-Type': 'application/json'},
          data,
          ''
      )
- name: {{ schema }}.udf_rest_api
  signature:
    - [partition_key, STRING]
    - [url, STRING]
    - [data, VARIANT]
    - [secret_name, STRING]
  return_type: ARRAY
  options: |
    NOT NULL
    RETURNS NULL ON NULL INPUT
    VOLATILE
  sql: |
    SELECT
      streamline.udf_rest_api(
          partition_key,
          'POST',
          url,
          {'Content-Type': 'application/json'},
          data,
          secret_name
      )
- name: {{ schema }}.udf_rest_api
  signature:
    - [partition_key, STRING]
    - [url, STRING]
  return_type: ARRAY
  options: |
    NOT NULL
    RETURNS NULL ON NULL INPUT
    VOLATILE
  sql: |
    SELECT
      streamline.udf_rest_api(
          partition_key,
          'GET',
          url,
          {},
          {},
          ''
      )
- name: {{ schema }}.udf_rest_api
  signature:
    - [partition_key, STRING]
    - [url, STRING]
    - [secret_name, STRING]
  return_type: ARRAY
  options: |
    NOT NULL
    RETURNS NULL ON NULL INPUT
    VOLATILE
  sql: |
    SELECT
      streamline.udf_rest_api(
          partition_key,
          'GET',
          url,
          {},
          {},
          secret_name
      )

{% endmacro %}