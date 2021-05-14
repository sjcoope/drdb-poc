from __future__ import annotations

class Table:
    def __init__(self) -> None:
        self.name = ""
        self.create_time = ""
        self.columns = ""
        self.location = ""

    @staticmethod
    def from_cli(cli_object) -> Table:
        table = Table()
        table.name = cli_object["Name"]
        table.create_time = cli_object["CreateTime"]
        table.columns = cli_object["StorageDescriptor"]["Columns"]
        table.location = cli_object["StorageDescriptor"]["Location"]

        return table

