# db/__init__.py
"""
Модуль для работы с базой данных
"""
from db.connection import (
    get_db_connection,
    get_db_connection_dict,
    test_connection,
    init_connection_pool,
    get_connection_from_pool,
    return_connection_to_pool,
    close_all_connections
)

__all__ = [
    'get_db_connection',
    'get_db_connection_dict',
    'test_connection',
    'init_connection_pool',
    'get_connection_from_pool',
    'return_connection_to_pool',
    'close_all_connections'
]