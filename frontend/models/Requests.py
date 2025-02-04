from dataclasses import dataclass

@dataclass
class ReqHistoried:
    index: int
    raw_text: str
    sql_query: str
    valid: bool
    result: dict | None
    elapsed_time: dict | None

@dataclass
class ReqPending:
    raw_text: str
    sql_query: str
    valid: bool
    result: dict | None
    elapsed_time: dict | None


