import json
from typing import Union

import requests

import main

ENDPOINT = "https://us-central1-digitaleyes-prod.cloudfunctions.net/offers-retriever"

def pp(dict_or_list: Union[list, dict]): # Pretty print
    print(json.dumps(dict_or_list, indent=2))

if __name__ == "__main__":
    # Test functions 
    offer_list = main._process_request_for_single_mint_with_owner("5FJeEJR8576YxXFdGRAu4NBBFcyfmtjsZrXHSsnzNPdS", "Fn1DmksaSansCcamYoEkdPqeJyCJdphCqxztU2KVv876")
    pp(offer_list)

    # Test if request by pk is working good
    offer = main._process_request_for_pk("HoeLjLxd97qcf6HkaQH8DHtV5GybutgmMt8Z3mY1ETT3")
    pp(offer)

    # API Testing
    # Test if single mint search is working good 
    response = requests.get(ENDPOINT, params=dict(mint="5FJeEJR8576YxXFdGRAu4NBBFcyfmtjsZrXHSsnzNPdS"))
    pp(response.json())

    # Test if single mint search with owner is working good 
    response = requests.get(ENDPOINT, params=dict(mint="5FJeEJR8576YxXFdGRAu4NBBFcyfmtjsZrXHSsnzNPdS", owner="Fn1DmksaSansCcamYoEkdPqeJyCJdphCqxztU2KVv876"))
    pp(response.json())

    # Test if request by pk is working good
    response = requests.get(ENDPOINT, params=dict(pk="HoeLjLxd97qcf6HkaQH8DHtV5GybutgmMt8Z3mY1ETT3", mint="5FJeEJR8576YxXFdGRAu4NBBFcyfmtjsZrXHSsnzNPdS"))
    pp(response.json())
    response = requests.get(ENDPOINT, params=dict(pk="HoeLjLxd97qcf6HkaQH8DHtV5GybutgmMt8Z3mY1ETT3"))
    pp(response.json())
