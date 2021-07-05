import os
import requests
import json
import mock, json
from dbt_builder import TransformGenerator
from dotenv import load_dotenv
load_dotenv(verbose=True)
hasura_url = os.getenv('hasura_url')
headers = {
    "Content-Type": "application/json",
    "x-hasura-admin-secret": os.getenv('hasura_key')
}


def call_target_for_rows(flowid):
    try:
        payload = {
            "query": """ query($id: Int!){
                data:data_flow_by_pk( id: $id){
                id
                name
                summary
                models {
                    id
                    model_type
                    model_config
                    output_cols
                    prev_model{
                    id
                    output_cols
                    }
                }
                }
            }""",
            "variables": {
                "id": flowid
            }
        }
        res = requests.post(hasura_url, data=json.dumps(payload), headers=headers)
        res_data = res.json()
        generator = TransformGenerator()
        # stmt = generator.generate_for_model(res_data['data']['data']['models'][0])
        for model_data in res_data['data']['data']['models']:
            stmt = generator.generate_for_model(model_data)
            print(stmt)
    except Exception as e:
        t_message = f"Error in call_target_for_rows, Error :  {e}   "
        print(t_message)
        raise e

call_target_for_rows(84)