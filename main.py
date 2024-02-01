import requests
import json
from urllib import parse
import base64
import configparser
import pandas as pd


def get_auth_token(auth_config):
    key_config = configparser.ConfigParser()
    key_config.read('private_auth.ini')
    app_settings = {
        'client_id': key_config['DEFAULT']['client_id'],
        'client_secret': key_config['DEFAULT']['client_secret']}
    authorization = base64.b64encode(bytes(app_settings['client_id'] + ":" + app_settings['client_secret'], "ISO-8859-1")).decode("ascii")

    auth_headers = {
        "Content-Type": auth_config['content_type'],
        "Authorization": "Basic " + authorization
    }
    body = {
        "grant_type": auth_config['grant_type'],
        "scope": auth_config['scope']
    }
    data = parse.urlencode(body)
    token_url = auth_config['token_url']

    auth_response = requests.post(token_url, headers=auth_headers, data=data)
    return auth_response.json()['access_token']


def construct_header(auth_token, header_config):
    return {'X-EBAY-C-MARKETPLACE-ID': header_config['market_place'],
            'X-EBAY-C-ENDUSERCTX': header_config['affiliate'],
            'Authorization': 'Bearer ' + auth_token
            }


def make_request(url_string, header_string):
    print(f'Query string: {url_string}')
    r = requests.get(url_string, headers=header_string)
    print(f'Query response: {r.status_code}')
    r_json = r.json()
    #print(json.r_json(items, indent=4))
    return r_json


def get_all_data(url_value, header_value):
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


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('config.ini')

    token = get_auth_token(config['auth'])
    headers = construct_header(token, config['header'])

    items_as_json = get_all_data(config['params']['query'], headers)

    write_data(items_as_json)

    df = pd.json_normalize(items_as_json)
    stripped_df = df.drop(['leafCategoryIds', 'categories', 'seller.username', 'conditionId', 'thumbnailImages',
                           'epid','itemAffiliateWebUrl', 'itemHref', 'itemLocation.country', 'additionalImages',
                           'adultOnly', 'legacyItemId', 'availableCoupons', 'topRatedBuyingExperience',
                           'priorityListing', 'listingMarketplaceId', 'itemId', 'image.imageUrl',
                           'seller.sellerAccountType'], axis=1)
    stripped_df.info()
