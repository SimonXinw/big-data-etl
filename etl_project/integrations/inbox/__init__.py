"""本地 data/inbox → data/raw CSV 物化。"""

from etl_project.integrations.inbox.materialize import (
    InboxSourceError,
    InboxSyncSummary,
    materialize_raw_from_inbox,
)

__all__ = [
    "InboxSourceError",
    "InboxSyncSummary",
    "materialize_raw_from_inbox",
]
