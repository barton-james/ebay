import requests
import json
from urllib import parse
import base64


def get_auth_token():
    app_settings = {
        'client_id': 'JamesBar-Finding-PRD-ee5603d2f-e4572c3c',
        'client_secret': 'PRD-e5603d2f399a-ca5c-4d10-bc74-bd9c'}
    authorization = base64.b64encode(bytes(app_settings['client_id'] + ":" + app_settings['client_secret'], "ISO-8859-1")).decode("ascii")

    auth_headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": "Basic " + authorization
    }

    body = {
        "grant_type": "client_credentials",
        "scope": "https://api.ebay.com/oauth/api_scope"
    }

    data = parse.urlencode(body)

    token_url = "https://api.ebay.com/identity/v1/oauth2/token"

    auth_response = requests.post(token_url, headers=auth_headers, data=data)
    return auth_response.json()


def construct_header(token):
    return {'X-EBAY-C-MARKETPLACE-ID': 'EBAY_GB',
            'X-EBAY-C-ENDUSERCTX': 'affiliateCampaignId=<ePNCampaignId>,affiliateReferenceId=<referenceId>',
            'Authorization': 'Bearer ' + token
            }


def construct_url():
    base_url = 'https://api.ebay.com/buy/browse/v1/item_summary/search'
    query = '?q=google pixel 7 128GB&category_ids=9355'
    max_items = 50
    limit = f'&limit={max_items}'
    sort = f'&sort=price'
    url = f'{base_url}{query}{limit}{sort}'
    filters = (
        #'bidCount:[10]',
        'buyingOptions:{FIXED_PRICE|BEST_OFFER}',
        #'buyingOptions:{FIXED_PRICE|BEST_OFFER|AUCTION}',
        'conditionIds:{3000|4000|5000|6000}',
        'deliveryCountry:GB',
        'deliveryPostalCode:CT202RQ',
        'itemLocationCountry:GB',
        #'priceCurrency:GBP',
        'searchInDescription:true',
        'sellerAccountTypes:{INDIVIDUAL}',
        'lastSoldDate:[2024-01-20T00:00:00Z..2024-01-30T00:00:00Z]'
    )
    filter_str = '&filter='
    for filter in filters:
        filter_str+=f'{filter},'

    return url+filter_str


def get_all_data(url_value, header_value):
    r = requests.get(url_value, headers=header_value)
    parsed = r.json()
    total_items = parsed['total']
    total_returned = len(parsed['itemSummaries'])
    print(total_items, total_returned)
    print(json.dumps(parsed, indent=4))
    new_parsed = parsed

    while total_returned < total_items:
        next_url = new_parsed['next']
        print(next_url)
        r = requests.get(next_url, headers=headers)
        print(f'{r.status_code}')
        new_parsed = r.json()
        print(json.dumps(new_parsed, indent=4))
        parsed['itemSummaries'].extend(new_parsed['itemSummaries'])
        total_returned += len(new_parsed['itemSummaries'])
        print(total_items, total_returned, next_url)
    #print(json.dumps(parsed, indent=4))
    return parsed


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    response = get_auth_token()
    #print(response)

    headers = construct_header(response['access_token'])
    url = construct_url()
    #print(url)
    json_data = get_all_data(url, headers)

    #r = requests.get(url,headers=headers)
    #print(f'{r.status_code}')
    #parsed = r.json()
    #print(f'{parsed['total']}')
    #print(json.dumps(parsed, indent=4))


