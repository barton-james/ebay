import os
import requests
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


def open_output_files():
    f_list = []
    output_dir = f'output'
    f_list.append(open(f'{output_dir}/all_status.csv', "w"))
    f_list.append(open(f'{output_dir}/all_completed.csv', "w"))
    f_list.append(open(f'{output_dir}/all_ongoing.csv', "w"))

    print('Output files opened')
    return f_list


def close_output_files(output_fs):
    for f in output_fs:
        f.close()
    print('\nOutput files closed')


def get_data(auth_token, params, filters, item):
    # Construct the data to send in the request then get the data and return it
    header = construct_header(auth_token, params['market_place'])
    search_str = 'https://api.ebay.com/buy/browse/v1/item_summary/search'

    search_str += f'?q={item}'
    for key, value in params.items():
        if key == 'market_place':
            continue
        # print(f'key:{key} , value:{value}')
        search_str += f'&{key}={value}'
    # print(search_str)

    search_str += '&filter='
    for value in filters.values():
        # print(f'value:{value}')
        search_str += f'{value},'
    # print(search_str)

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
    r_dict = r.json()  # This function parses the content as JSON and returns a dictionary
    # print(json.r_json(items, indent=4))
    return r_dict


def loop_and_get_data(url_value, header_value):
    # Loop and get data in chunks, build a full set of results in a dictionary and return it
    response_dict = make_request(url_value, header_value)
    items_dict = response_dict

    while "next" in response_dict:
        items_dict['itemSummaries'].extend(response_dict['itemSummaries'])
    print(f'Count of items returned: {len(items_dict['itemSummaries'])}')
    # print(json.dumps(items_dict, indent=4))
    return items_dict


def prune_data(list_of_dict, refinements, columns, item_type):
    # Filter the returned list based on config requirements and return it
    filtered_list = [each_dict for each_dict in list_of_dict if 'shippingOptions' in each_dict]

    df = pd.json_normalize(filtered_list)
    filtered_df = df.loc[:, df.columns.isin(columns)]
    filtered_df = filtered_df[filtered_df['seller.feedbackScore'] < int(refinements['max_feedback_score'])]
    filtered_df = filtered_df[filtered_df['seller.feedbackScore'] > int(refinements['min_feedback_score'])]
    filtered_df = filtered_df[filtered_df['seller.feedbackPercentage'].astype(str).astype(float) >
                              float(refinements['min_feedback_percentage'])]
    filtered_df['legacyItemId'] = filtered_df['legacyItemId'].astype(int)
    filtered_df['price.value'] = filtered_df['price.value'].astype(float)
    filtered_df['seller.feedbackPercentage'] = filtered_df['seller.feedbackPercentage'].astype(float)
    filtered_df['shippingPrice'] = filtered_df['shippingOptions'].astype(str).str.split('\'').str[9]
    filtered_df.drop(columns=['shippingOptions'], inplace=True)
    filtered_df['totalPrice'] = filtered_df['shippingPrice'].astype(float)+filtered_df['price.value'].astype(float)

    filtered_df['itemType'] = item_type

    print(f'Number of filtered items: {len(filtered_df)}')
    # filtered_df.info()
    return filtered_df


def output_results(latest_data_df, item_name, output_file_list):
    output_dir = f'output'
    filename_latest = f'{output_dir}/{item_name}.csv'
    filename_previous = f'{output_dir}/{item_name}_previous.csv'
    filename_completed = f'{output_dir}/{item_name}_completed.csv'
    filename_ongoing = f'{output_dir}/{item_name}_ongoing.csv'
    filename_status = f'{output_dir}/{item_name}_status.csv'

    if os.path.exists(filename_latest):
        os.rename(filename_latest, filename_previous)
        previous_data_df = pd.read_csv(filename_previous)
        # previous_data_df.info()

        comparison_df = pd.merge(latest_data_df, previous_data_df, indicator='Status', on=['legacyItemId',
                                                                                           'itemWebUrl'],
                                 how='outer', suffixes=('', '_y'))
        comparison_df.drop(comparison_df.filter(regex='_y$').columns, axis=1, inplace=True)
        comparison_df['Status'] = comparison_df['Status'].map({'both': 'ongoing', 'left_only': 'new',
                                                               'right_only': 'complete'})

        comparison_df.sort_values(by='Status', inplace=True)
        comparison_df.to_csv(filename_status, index=False)
        comparison_df.to_csv(output_file_list[0], index=False, mode='a', header=False)

        completed_df = comparison_df.loc[comparison_df['Status'] == 'complete']
        completed_df.to_csv(filename_completed, index=False)
        completed_df.to_csv(output_file_list[1], index=False, mode='a', header=False)
        ongoing_df = comparison_df.loc[comparison_df['Status'] != 'complete']
        ongoing_df.to_csv(filename_ongoing, index=False)
        ongoing_df.to_csv(output_file_list[2], index=False, mode='a', header=False)

    latest_data_df.to_csv(filename_latest, index=False)

    print(f'Output files written')
    return


if __name__ == '__main__':
    # Read the configuration
    config = configparser.RawConfigParser(allow_no_value=True)
    config.optionxform = lambda option: option
    config.read('config.ini')

    # Get the auth token to use for data retrieval
    token = get_auth_token()

    # Open the generic output files
    output_files = open_output_files()

    # Make successive calls for each query we want to run
    search_item = config['google pixel 8 pro 256gb']['q']
    print(f'\nRunning for {search_item}')
    item_dictionary = get_data(token, config['default params'], config['default filters'], search_item)
    pruned_data_df = prune_data(item_dictionary['itemSummaries'], config['default refinements'],
                                list(config['default columns'].keys()), search_item)
    output_results(pruned_data_df, search_item, output_files)

    search_item = config['google pixel 8 pro 512gb']['q']
    print(f'\nRunning for {search_item}')
    item_dictionary = get_data(token, config['default params'], config['default filters'], search_item)
    pruned_data_df = prune_data(item_dictionary['itemSummaries'], config['default refinements'],
                                list(config['default columns'].keys()), search_item)
    output_results(pruned_data_df, search_item, output_files)

    search_item = config['apple iphone 14 plus 256gb']['q']
    print(f'\nRunning for {search_item}')
    item_dictionary = get_data(token, config['default params'], config['default filters'], search_item)
    pruned_data_df = prune_data(item_dictionary['itemSummaries'], config['default refinements'],
                                list(config['default columns'].keys()), search_item)
    output_results(pruned_data_df, search_item, output_files)

    # Close output files
    close_output_files(output_files)