import pandas as pd
import logging

logger = logging.getLogger(__name__)


def clean_text(val):
    """
    尝试修复 Latin1 编码误存为字符串的问题（常见于旧系统）。
    回退策略：Latin1 → GBK → UTF-8 → ASCII 替换
    """
    if pd.isna(val):
        return None
    if isinstance(val, str):
        try:
            # 常见场景：原始是 GBK 字节，被当 Latin1 解码成乱码字符串
            return val.encode('latin1').decode('gbk', errors='replace')
        except (UnicodeEncodeError, UnicodeDecodeError):
            try:
                return val.encode('latin1').decode('utf-8', errors='replace')
            except (UnicodeEncodeError, UnicodeDecodeError):
                # 最后手段：只保留 ASCII
                return ''.join(c if ord(c) < 128 else '?' for c in val)
    return str(val)