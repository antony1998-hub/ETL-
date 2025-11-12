import pandas as pd
import logging
from typing import List
from psycopg2 import extras
from .db_utils import get_partition_tables, create_psycopg2_conn
from .data_cleaner import clean_text

logger = logging.getLogger(__name__)


class PartitionMigrator:
    def __init__(self, source_config: dict, target_config: dict, migration_config: dict):
        self.source_config = source_config
        self.target_config = target_config
        self.parent_table = migration_config['parent_table']
        self.target_table = migration_config['target_table']

    def run(self):
        # 1. 创建 SQLAlchemy 引擎（仅用于元数据查询）
        from sqlalchemy import create_engine
        source_url = (
            f"postgresql://{self.source_config['user']}:{self.source_config['password']}"
            f"@{self.source_config['host']}:{self.source_config['port']}/{self.source_config['dbname']}"
        )
        source_engine = create_engine(source_url)

        # 2. 获取分区
        partitions = get_partition_tables(source_engine, self.parent_table)
        if not partitions:
            logger.warning("未找到任何分区，任务退出")
            return

        # 3. 用 psycopg2 读取数据（支持 Latin1）
        source_conn = create_psycopg2_conn(self.source_config)
        all_dfs = []

        for table in partitions:
            logger.info(f"读取分区: {table}")
            try:
                df = pd.read_sql_query(f'SELECT * FROM "{table}"', source_conn)
                if not df.empty:
                    all_dfs.append(df)
                    logger.info(f"加载 {len(df)} 行")
            except Exception as e:
                logger.error(f"读取 {table} 失败: {e}")

        source_conn.close()
        source_engine.dispose()

        if not all_dfs:
            raise RuntimeError("无有效数据可迁移")

        # 4. 合并与清洗
        combined = pd.concat(all_dfs, ignore_index=True)
        text_cols = combined.select_dtypes(include=['object']).columns
        for col in text_cols:
            combined[col] = combined[col].apply(clean_text)
        logger.info(f"清洗完成，总行数: {len(combined)}")

        # 5. 写入目标库
        self._write_to_target(combined)

    def _write_to_target(self, df: pd.DataFrame):
        target_conn = create_psycopg2_conn(self.target_config)
        cursor = target_conn.cursor()

        # 删除并重建表（全 TEXT 类型）
        cursor.execute(f'DROP TABLE IF EXISTS "{self.target_table}"')
        cols_def = ', '.join([f'"{col}" TEXT' for col in df.columns])
        cursor.execute(f'CREATE TABLE "{self.target_table}" ({cols_def})')
        target_conn.commit()

        # 批量插入
        data = [[clean_text(v) for v in row] for row in df.values]
        cols = df.columns.tolist()
        quoted_cols = ', '.join([f'"{c}"' for c in cols])
        insert_sql = f'INSERT INTO "{self.target_table}" ({quoted_cols}) VALUES %s'

        extras.execute_values(cursor, insert_sql, data, page_size=1000)
        target_conn.commit()
        logger.info(f"成功写入 {len(df)} 行到 {self.target_table}")

        cursor.close()
        target_conn.close()