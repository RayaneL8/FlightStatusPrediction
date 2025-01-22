from services import natural_requests as s_nat_req
from models.Requests import ReqPending, ReqHistoried
from dataclasses import asdict

history_list : list[ReqHistoried] = []

def add_item(item: ReqPending):
    new_item = asdict(
            ReqHistoried(
                index=len(history_list),
                raw_text=item.raw_text,
                sql_query=item.sql_query,
                valid=item.valid,
                result=item.result,
                elapsed_time=item.elapsed_time
            )
        )
    history_list.append(new_item)    
    return new_item

def get_list(): 
    return history_list

def find_item(index:int = 0):
    return history_list[index]

def format_item_data(item:ReqHistoried):
    return f"{str(item["result"])}"

#------------------------------------------
def handle_submit(content: str):

    print("Handle submit")
    try:
        data, elapsed = s_nat_req.send_request(content= content.lower().strip())
        print("DATA and ELAPSED:", data, elapsed)
    except Exception:
        new_item = ReqPending(raw_text=content, sql_query=data["sql_query"], valid=False, result=None, elapsed_time={"server": data["elapsed_time"], "client": elapsed})
        return add_item(item= new_item)
    
    new_item = ReqPending(raw_text=content, sql_query=data["sql_query"], valid=True, result=data["content"], elapsed_time={"server": data["elapsed_time"], "client": elapsed})
    return add_item(item= new_item)
