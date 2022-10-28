import os
import sys


from stram import read_sql, run_and_fetchall


def run_task(get_conn_callback):
    with get_conn_callback() as conn:

        print(run_and_fetchall(conn, "SELECT 'Hello, world!' AS msg")[0]['msg'])

        # Read a script from a location relative to where this file sits
        print(run_and_fetchall(conn, read_sql('sql/step_1.sql'))[0]['msg'])

        # Inject a value you have under source control (no user input) with format_dict
        print(run_and_fetchall(conn, read_sql('sql/format_dict_example.sql',
                                              format_dict={'col': 'msg'}))[0]['msg'])

        # Inject a value you want the Snowflake Python Connector to render
        print(run_and_fetchall(conn, read_sql('sql/params_example.sql'),
                               params={'msg': 'Params example done!'})[0]['msg'])

       
        # Inject a value you want to render with the Jinja 2 templating engine
        # Notice that if you specify jinja2_params, the relative path is evaluated against
        # the jinja2 directory
        print(run_and_fetchall(conn, read_sql('sql/jinja2_example.sql',
                                              jinja2_params={'col': 'msg'}))[0]['msg'])

        # You can find a more useful example of a jinja template in
        # 'jinja2/sql/test/foreign_key.sql' 

        # If you (really) want, you can combine all three ways of injecting values 
        print(run_and_fetchall(conn,
                               read_sql('sql/combined_example.sql',
                                        format_dict={'col1': 'msg1'},
                                        jinja2_params={'col2': 'msg2'}) ,
                               params={'msg1': 'Hello, ', 'msg2': 'world!'})[0]
             )


if 'LOCAL_DEVELOPMENT' in os.environ:
    def main(argv):
        """
        Intended for local use only
        """
        from getpass import getpass

        from get_conn import get_conn

        USER = os.environ['SNOWFLAKE_USER'].lower()
        ROLE = os.environ['SNOWFLAKE_ROLE'].lower()
        ACCOUNT = os.environ['SNOWFLAKE_ACCOUNT'].lower()
        PRIVATE_KEY_PATH = os.environ['SNOWFLAKE_PRIVATE_KEY_PATH']

        def _get_conn():
            return get_conn(
                    user=USER,
                    role=ROLE,
                    account=ACCOUNT,
                    private_key_path=PRIVATE_KEY_PATH,
                    passphrase=getpass().encode('utf-8')
            )

        run_task(_get_conn)


    if __name__ == "__main__":
        sys.exit(main(sys.argv))
