from typing import List
from sqlalchemy import create_engine, text
import psycopg2
import logging

logger = logging.getLogger(__name__)


def get_partition_tables(engine, parent_table_name: str) -> List[str]:
    """查询 PostgreSQL 中指定父表的所有子分区表名"""
    query = text("""
        SELECT c.relname AS partition_name
        FROM pg_inherits i
        JOIN pg_class c ON c.oid = i.inhrelid
        JOIN pg_class p ON p.oid = i.inhparent
        WHERE p.relname = :table_name
        ORDER BY c.relname;
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {"table_name": parent_table_name})
        partitions = [row[0] for row in result.fetchall()]
        logger.info(f"找到 {len(partitions)} 个分区: {partitions}")
        return partitions


def create_psycopg2_conn(config: dict):
    """创建 psycopg2 连接，显式设置 client_encoding"""
    dsn = (
        f"host={config['host']} "
        f"port={config['port']} "
        f"dbname={config['dbname']} "
        f"user={config['user']} "
        f"password={config['password']} "
    )
    conn = psycopg2.connect(dsn)
    conn.set_client_encoding(config.get('client_encoding', 'UTF8'))
    return conn