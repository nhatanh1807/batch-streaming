import json
import os
from datetime import datetime, timezone

from config.settings import BRONZE_DIR


class BronzeWriter:

    def __init__(self):
        self.folder = BRONZE_DIR
        os.makedirs(self.folder, exist_ok=True)

    def save(self, records):
        if not records:
            return

        timestamp = datetime.now(timezone.utc).strftime(
            "%Y%m%d_%H%M%S_%f"
        )

        tmp_file = os.path.join(
            self.folder,
            f"{timestamp}.tmp"
        )

        final_file = os.path.join(
            self.folder,
            f"{timestamp}.jsonl"
        )

        # JSON Lines:
        # mỗi dòng = một raw event
        with open(tmp_file, "w", encoding="utf-8") as f:
            for record in records:
                f.write(json.dumps(record) + "\n")

        # Atomic rename
        os.replace(tmp_file, final_file)

        print(
            f"Saved {len(records)} records to raw Bronze: "
            f"{final_file}"
        )