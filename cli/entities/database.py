from __future__ import annotations

class Database:
    def __init__(self) -> None:
        self.name = ""
        self.create_time = ""

    @staticmethod
    def from_cli(cli_object) -> Database:
        db = Database()
        db.name = cli_object["Name"]
        db.create_time = cli_object["CreateTime"]

        return db

