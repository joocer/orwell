"""
Null Writer - do nothing
"""
from typing import Optional
import datetime


def null_writer(
        source_file_name: str,
        target_path: str,
        date: Optional[datetime.date] = None,
        **kwargs):
    pass
