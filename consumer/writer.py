# import json
# import os
# from datetime import datetime


# class BronzeWriter:

#     def __init__(self, folder):

#         self.folder = folder

#         os.makedirs(folder, exist_ok=True)

#     def save(self, records):

#         if not records:
#             return

#         filename = datetime.now().strftime("%Y%m%d_%H%M%S")

#         tmp = os.path.join(
#             self.folder,
#             filename + ".tmp"
#         )

#         final = os.path.join(
#             self.folder,
#             filename + ".json"
#         )

#         with open(tmp, "w") as f:
#             json.dump(records, f)

#         os.replace(tmp, final)

#         print("Saved", final)
from db.postgres import get_connection


class BronzeWriter:

    def save(self, records):

        if not records:
            return

        with get_connection() as conn:

            with conn.cursor() as cur:

                for record in records:

                    cur.execute(
                        """
                        INSERT INTO bronze.trades (
                            trade_id,
                            event_time,
                            symbol,
                            price,
                            quantity,
                            buyer_maker
                        )
                        VALUES (
                            %s,
                            to_timestamp(%s / 1000.0),
                            %s,
                            %s,
                            %s,
                            %s
                        )
                        ON CONFLICT (trade_id) DO NOTHING
                        """,
                        (
                            record["trade_id"],
                            record["event_time"],
                            record["symbol"],
                            record["price"],
                            record["quantity"],
                            record["is_buyer_maker"],
                        ),
                    )

        print(f"Saved {len(records)} records to PostgreSQL")