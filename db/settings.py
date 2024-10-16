import os

DATABASE = {
    'qqp': {
        'username': os.environ.get('DATABASE_USERNAME'),
        'password': os.environ.get('DATABASE_PASSWORD'),
        'host': os.environ.get('DATABASE_HOST'),
        'port': int(os.environ.get('DATABASE_PORT', '5432'))
    }
}


