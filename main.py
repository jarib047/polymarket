from py_clob_client.client import ClobClient
from py_clob_client.order_builder.constants import BUY
from tqdm import tqdm
import requests
import ast
import time

def get_order_book():
    host = "https://clob.polymarket.com"
    chain_id = 137  # Polygon mainnet
    token_ids = ['Bitcoin Up or Down - March 1, 11:00PM-11:05PM ET', [102452725605913348943039097161217309024081647632931309252077215077547264837295, 80665156423103587726522794473491361210829108104816890421779639498149029619560]]
    client = ClobClient(host, chain_id=chain_id,)


    book = client.get_order_book(84872199490335871740728949832520096734204168602345881181420031968208766256276)
    print(book.asks[-1])
    # print("Best bid:", book["bids"][0])
    # print("Best ask:", book["asks"][0])
    # print("Tick size:", book["tick_size"])


def get_token_ids():
    delta = 50
    token_list = []
    # req = "Bitcoin Up or Down - 5 Minutes"
    req = "btc-updown-5m"

    for i in tqdm(range(130,150)):
        response = requests.get(
            "https://gamma-api.polymarket.com/events",
            params={"active": "true", "closed": "false", "limit": 50, "offset": 0 + i * delta, "tags": "crypto"}
        )
        markets = response.json()
        # print(markets[0]["markets"][0]["clobTokenIds"])
        # exit()
        try:
            slug_dict = {markets[i]["slug"] : [markets[i]["title"], markets[i]["markets"][0]["clobTokenIds"]] for i in range(len(markets))}
        except Exception as E:
            print(f"Error: {str(E)}")
        if any([req in i for i in slug_dict.keys()]):
            for i in slug_dict.keys():
                if req in i:
                    token_list.append([slug_dict[i][0], [int(x) for x in ast.literal_eval(slug_dict[i][-1])]])
        else:
            print(f"not in batch {i+1}")
        
    print(token_list)
    return token_list


if __name__ == "__main__":
    # get_token_ids()
    while(True):
        time.sleep(0.01)
        get_order_book()