from snowflake.connector import DictCursor, ProgrammingError
import logging
import os
from jinja2 import Environment, FileSystemLoader, select_autoescape


JINJA2_ENV = Environment(
        loader=FileSystemLoader(f'{os.path.abspath(os.path.dirname(__file__))}/jinja2'),
        autoescape=select_autoescape()
)


def run(conn, sql, params=None):
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
    except ProgrammingError:
        raise
    finally:
        cur.close()


def run_and_fetchall(conn, sql, params=None):
    cur = conn.cursor(DictCursor)
    try:
        cur.execute(sql, params)
    except ProgrammingError:
        raise
    else:
        results = cur.fetchall()
    finally:
        cur.close()
    # Lowercase all column names because we use lowercase identifiers everywhere in our SQL code
    return [{k.lower(): v for k, v in rec.items()} for rec in results]


def run_and_yield_page(conn, sql, page_size=1000, params=None):
    cur = conn.cursor(DictCursor)
    try:
        cur.execute(sql, params)
    except ProgrammingError:
        raise
    else:
        res = cur.fetchmany(size=page_size)
        while len(res) > 0:
            yield res
            res = cur.fetchmany(size=page_size)
    finally:
        cur.close()


def read_sql(relative_path, format_dict=None, jinja2_params=None):
    """
    relative_path is relative to invoked Python module, unless jinja2_params is specified,
    in that case it is relative to the jinja2 directory
    """
    if jinja2_params is not None:
        sql = JINJA2_ENV.get_template(relative_path).render(**jinja2_params)
    else:
        with open((f'{os.path.abspath(os.path.dirname(__file__))}/{relative_path}'), 'r') as f:
            sql = f.read()
    if format_dict is None:
        return sql
    return sql.format(**format_dict)


def test_unique_compound_key(conn, database, schema, table, columns):
    return run_and_fetchall(conn, read_sql('sql/test/ql/unique_compound_key.sql', format_dict={
            'columns_str': ', '.join(columns),
            'database': database,
            'schema': schema,
            'table': table
    }))


def test_foreign_key(conn, database, schema, table, columns,
                     referenced_database=None, referenced_schema=None, referenced_table=None, referenced_columns=None):
    """
    Will fail if the table that contains the foreign key constraint contains
    rows for which no primary key exists in the referenced table
    """
    referenced_database = database if referenced_database is None else referenced_database
    referenced_schema = schema if referenced_schema is None else referenced_schema
    referenced_table = table if referenced_table is None else referenced_table
    referenced_columns = columns if referenced_columns is None else referenced_columns
    if len(referenced_columns) != len(columns):
        raise ValueError('referenced_columns and columns must have equal length')
    return run_and_fetchall(conn, read_sql('sql/test/foreign_key.sql', jinja2_params={
            'database': database,
            'schema': schema,
            'table': table,
            'columns': columns,
            'referenced_database': referenced_database,
            'referenced_schema': referenced_schema,
            'referenced_table': referenced_table,
            'referenced_columns': referenced_columns
    }))


def raise_if_rows(res, test_name):
    if len(res):
        msg = f'Test \'{test_name}\' failed.'
        logging.error('=====================')
        logging.error(msg)
        logging.error('First offending row:')
        logging.error(res[0])
        logging.error('=====================')
        raise ValueError(msg)

