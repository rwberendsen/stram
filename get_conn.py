from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from snowflake import connector


def get_conn(user, account, private_key_path, passphrase, role=None):
    with open(private_key_path, 'rb') as f_key:
        p_key = serialization.load_pem_private_key(
                f_key.read(),
                password=passphrase,
                backend=default_backend()
        )

    pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
    )

    conn_config = {
        'user': user,
        'account': account,
        'private_key': pkb,
        'authenticator': 'snowflake'
    }

    if role is not None:
        conn_config['role'] = role

    return connector.connect(**conn_config)
