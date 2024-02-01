import requests
import json
from urllib import parse
import base64
import configparser
import pandas as pd


def get_auth_token():
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
    print(search_str)

    items_as_json = loop_and_get_data(search_str, header)
    write_data(items_as_json)
    return items_as_json


def construct_header(auth_token, market_place):
    return {'X-EBAY-C-MARKETPLACE-ID': market_place,
            'X-EBAY-C-ENDUSERCTX': 'affiliateCampaignId=<ePNCampaignId>,affiliateReferenceId=<referenceId>',
            'Authorization': 'Bearer ' + auth_token
            }


def make_request(url_string, header_string):
    print(f'Query string: {url_string}')
    r = requests.get(url_string, headers=header_string)
    print(f'Query response: {r.status_code}')
    r_json = r.json()
    #print(json.r_json(items, indent=4))
    return r_json


def loop_and_get_data(url_value, header_value):
    items = make_request(url_value, header_value)
    all_items = items

    while "next" in items:
        next_query = items['next']
        items = make_request(next_query, header_value)
        all_items['itemSummaries'].extend(items['itemSummaries'])
    print(f'Count of items returned: {len(all_items['itemSummaries'])}')
    #print(json.dumps(all_items, indent=4))
    return all_items['itemSummaries']


def write_data(json_to_write):
    with open('data.json', 'w', encoding='utf-8') as f:
        json.dump(json_to_write, f, ensure_ascii=False, indent=4)


def convert_to_dataframe(json_data):
    df = pd.json_normalize(json_results)
    filtered_df = df.drop(['leafCategoryIds', 'categories', 'seller.username', 'conditionId', 'thumbnailImages',
                           'epid', 'itemAffiliateWebUrl', 'itemHref', 'itemLocation.country', 'additionalImages',
                           'adultOnly', 'legacyItemId', 'availableCoupons', 'topRatedBuyingExperience',
                           'priorityListing', 'listingMarketplaceId', 'itemId', 'image.imageUrl',
                           'seller.sellerAccountType'], axis=1)
    #filtered_df.info()


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('config.ini')

    token = get_auth_token()

    json_results = get_data(token, config['default params'], config['default filters'])
    convert_to_dataframe(json_results)



