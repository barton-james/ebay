import requests
import json
from urllib import parse
import base64
import configparser
import pandas as pd


def get_auth_token():
    # Get the authentication credentials from file, request the token and return it
    auth_config = configparser.ConfigParser()
    auth_config.read('private_auth.ini')
    authorization = base64.b64encode(bytes(auth_config['keys']['client_id'] + ":" +
                                           auth_config['keys']['client_secret'], "ISO-8859-1")).decode("ascii")
    auth_headers = {
        "Content-Type": auth_config['params']['content_type'],
        "Authorization": "Basic " + authorization
    }
    body = {
        "grant_type": auth_config['params']['grant_type'],
        "scope": auth_config['params']['scope']
    }
    data = parse.urlencode(body)
    token_url = auth_config['params']['token_url']

    auth_response = requests.post(token_url, headers=auth_headers, data=data)
    return auth_response.json()['access_token']


def get_data(auth_token, params, filters):
    # Construct the data to send in the request then get the data and return it
    header = construct_header(auth_token, params['market_place'])
    params.pop('market_place')
    search_str = 'https://api.ebay.com/buy/browse/v1/item_summary/search'

    search_str += '?'
    for key, value in params.items():
        #print(f'key:{key} , value:{value}')
        search_str+=f'{key}={value}&'
    #print(search_str)

    search_str += 'filter='
    for value in filters.values():
        #print(f'value:{value}')
        search_str += f'{value},'
    #print(search_str)

    returned_dict = loop_and_get_data(search_str, header)
    return returned_dict


def construct_header(auth_token, market_place):
    # Return the header for a request
    return {'X-EBAY-C-MARKETPLACE-ID': market_place,
            'X-EBAY-C-ENDUSERCTX': 'affiliateCampaignId=<ePNCampaignId>,affiliateReferenceId=<referenceId>',
            'Authorization': 'Bearer ' + auth_token
            }


def make_request(url_string, header_string):
    # Make a request for data and return a dictionary of the results
    print(f'Query string: {url_string}')
    r = requests.get(url_string, headers=header_string)
    print(f'Query response: {r.status_code}')
    r_dict = r.json() # This function parses the content as JSON and returns a dictionary
    #print(json.r_json(items, indent=4))
    return r_dict


def loop_and_get_data(url_value, header_value):
    # Loop and get data in chunks, build a full set of results in a dictionary and return it
    response_dict = make_request(url_value, header_value)
    items_dict = response_dict

    while "next" in response_dict:
        next_query = response_dict['next']
        items = make_request(next_query, header_value)
        items_dict['itemSummaries'].extend(response_dict['itemSummaries'])
    print(f'Count of items returned: {len(items_dict['itemSummaries'])}')
    #print(json.dumps(items_dict, indent=4))
    return items_dict


def prune_data(list_of_dict, refinements, columns):
    # Filter the returned list based on config requirements and output the final data to csv file
    filtered_list = [each_dict for each_dict in list_of_dict if 'shippingOptions' in each_dict]

    df = pd.json_normalize(filtered_list)
    filtered_df = df.loc[:,df.columns.isin(columns)]
    filtered_df = filtered_df[filtered_df['seller.feedbackScore'] < int(refinements['max_feedback_score'])]
    filtered_df = filtered_df[filtered_df['seller.feedbackScore'] > int(refinements['min_feedback_score'])]
    filtered_df = filtered_df[filtered_df['seller.feedbackPercentage'].astype(str).astype(float) > float(refinements['min_feedback_percentage'])]
    filtered_df.info()
    filtered_df.to_csv('data.csv', index=False)


if __name__ == '__main__':
    config = configparser.RawConfigParser(allow_no_value=True)
    config.optionxform = lambda option: option
    config.read('config.ini')

    token = get_auth_token()

    item_dictionary = get_data(token, config['default params'], config['default filters'])
    prune_data(item_dictionary['itemSummaries'], config['default refinements'], list(config['default columns'].keys()))
