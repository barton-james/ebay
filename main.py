import os
import requests
from urllib import parse
import base64
import configparser
import pandas as pd
import json


def get_auth_token():
    # Get the authentication credentials from file, request the token and return it
    gat_auth_config = configparser.ConfigParser()
    gat_auth_config.read('private_auth.ini')
    gat_authorization = base64.b64encode(bytes(gat_auth_config['keys']['client_id'] + ":" +
                                           gat_auth_config['keys']['client_secret'], "ISO-8859-1")).decode("ascii")
    gat_auth_headers = {
        "Content-Type": gat_auth_config['params']['content_type'],
        "Authorization": "Basic " + gat_authorization
    }
    gat_body = {
        "grant_type": gat_auth_config['params']['grant_type'],
        "scope": gat_auth_config['params']['scope']
    }
    gat_data = parse.urlencode(gat_body)
    gat_token_url = gat_auth_config['params']['token_url']

    gat_auth_response = requests.post(gat_token_url, headers=gat_auth_headers, data=gat_data)
    return gat_auth_response.json()['access_token']


def open_output_files():
    oof_f_list = []
    oof_output_dir = f'output'

    if not os.path.exists(oof_output_dir):
        os.mkdir(oof_output_dir)
    oof_f_list.append(open(f'{oof_output_dir}/all_status.csv', "w"))
    oof_f_list.append(open(f'{oof_output_dir}/all_completed.csv', "w"))
    oof_f_list.append(open(f'{oof_output_dir}/all_ongoing.csv', "w"))

    print('Output files opened')
    return oof_f_list


def close_output_files(cof_output_fs):
    for cof_f in cof_output_fs:
        cof_f.close()
    print('\nOutput files closed')


def get_data(gd_auth_token, gd_params, gd_filters, gd_item):
    # Construct the data to send in the request then get the data and return it
    gd_header = construct_header(gd_auth_token, gd_params['market_place'])
    gd_search_str = 'https://api.ebay.com/buy/browse/v1/item_summary/search'

    gd_search_str += f'?q={gd_item}'
    for gd_key, gd_value in gd_params.items():
        if gd_key == 'market_place':
            continue
        # print(f'gd_key:{gd_key} , gd_value:{gd_value}')
        gd_search_str += f'&{gd_key}={gd_value}'
    # print(gd_search_str)

    gd_search_str += '&filter='
    for gd_value in gd_filters.values():
        # print(f'gd_value:{gd_value}')
        gd_search_str += f'{gd_value},'
    # print(gd_search_str)

    gd_returned_dict = loop_and_get_data(gd_search_str, gd_header)
    return gd_returned_dict


def construct_header(ch_auth_token, ch_market_place):
    # Return the header for a request
    return {'X-EBAY-C-MARKETPLACE-ID': ch_market_place,
            'X-EBAY-C-ENDUSERCTX': 'affiliateCampaignId=<ePNCampaignId>,affiliateReferenceId=<referenceId>',
            'Authorization': 'Bearer ' + ch_auth_token
            }


def make_request(mr_url_string, mr_header_string):
    # Make a request for data and return a dictionary of the results
    print(f'Query string: {mr_url_string}')
    r = requests.get(mr_url_string, headers=mr_header_string)
    print(f'Query response: {r.status_code}')
    mr_r_dict = r.json()  # This function parses the content as JSON and returns a dictionary
    # print(json.r_json(items, indent=4))
    return mr_r_dict


def loop_and_get_data(lagd_url_value, lagd_header_value):
    # Loop and get data in chunks, build a full set of results in a dictionary and return it
    lagd_response_dict = make_request(lagd_url_value, lagd_header_value)
    lagd_items_dict = lagd_response_dict

    while "next" in lagd_response_dict:
        lagd_items_dict['itemSummaries'].extend(lagd_response_dict['itemSummaries'])

    if 'itemSummaries' in lagd_items_dict:
        print(f'Count of items returned: {len(lagd_items_dict['itemSummaries'])}')
    else:
        print(f'Count of items returned: 0')
    # print(json.dumps(items_dict, indent=4))
    return lagd_items_dict


def prune_data(pd_list_of_dict, pd_refinements, pf_columns, pd_item_type):
    # Filter the returned list based on config requirements and return it
    pd_filtered_list = [pd_each_dict for pd_each_dict in pd_list_of_dict if 'shippingOptions' in pd_each_dict]

    pd_df = pd.json_normalize(pd_filtered_list)
    pd_filtered_df = pd_df.loc[:, pd_df.columns.isin(pf_columns)]
    pd_filtered_df = pd_filtered_df[pd_filtered_df['seller.feedbackScore'] < int(pd_refinements['max_feedback_score'])]
    pd_filtered_df = pd_filtered_df[pd_filtered_df['seller.feedbackScore'] > int(pd_refinements['min_feedback_score'])]
    pd_filtered_df = pd_filtered_df[pd_filtered_df['seller.feedbackPercentage'].astype(str).astype(float) >
                              float(pd_refinements['min_feedback_percentage'])]
    pd_filtered_df['legacyItemId'] = pd_filtered_df['legacyItemId'].astype(int)
    pd_filtered_df['price.value'] = pd_filtered_df['price.value'].astype(float)
    pd_filtered_df['seller.feedbackPercentage'] = pd_filtered_df['seller.feedbackPercentage'].astype(float)
    pd_filtered_df['shippingPrice'] = pd_filtered_df['shippingOptions'].astype(str).str.split('\'').str[9]
    pd_filtered_df.drop(columns=['shippingOptions'], inplace=True)
    pd_filtered_df['totalPrice'] = pd_filtered_df['shippingPrice'].astype(float)+pd_filtered_df['price.value'].astype(float)

    pd_filtered_df['itemType'] = pd_item_type
    pd_filtered_df = pd_filtered_df.reindex(sorted(pd_filtered_df.columns), axis=1)

    print(f'Number of filtered items: {len(pd_filtered_df)}')
    # pd_filtered_df.info()
    return pd_filtered_df


def output_results(or_latest_data_df, or_item_name, or_output_file_list):
    or_output_dir = f'output'
    or_filename_latest = f'{or_output_dir}/{or_item_name}.csv'
    or_filename_previous = f'{or_output_dir}/{or_item_name}_previous.csv'
    or_filename_completed = f'{or_output_dir}/{or_item_name}_completed.csv'
    or_filename_ongoing = f'{or_output_dir}/{or_item_name}_ongoing.csv'
    or_filename_status = f'{or_output_dir}/{or_item_name}_status.csv'

    if os.path.exists(or_filename_latest):
        os.rename(or_filename_latest, or_filename_previous)
        or_previous_data_df = pd.read_csv(or_filename_previous)
        # previous_data_df.info()

        or_comparison_df = pd.merge(or_latest_data_df, or_previous_data_df, indicator='Status', on=['legacyItemId',
                                                                                           'itemWebUrl'],
                                 how='outer', suffixes=('', '_y'))
        or_comparison_df.drop(or_comparison_df.filter(regex='_y$').columns, axis=1, inplace=True)
        or_comparison_df['Status'] = or_comparison_df['Status'].map({'both': 'ongoing', 'left_only': 'new',
                                                               'right_only': 'complete'})

        or_comparison_df.sort_values(by='Status', inplace=True)
        or_comparison_df.to_csv(or_filename_status, index=False)
        or_comparison_df.to_csv(or_output_file_list[0], index=False, mode='a', header=False)

        or_completed_df = or_comparison_df.loc[or_comparison_df['Status'] == 'complete']
        or_completed_df.to_csv(or_filename_completed, index=False)
        or_completed_df.to_csv(or_output_file_list[1], index=False, mode='a', header=False)
        or_ongoing_df = or_comparison_df.loc[or_comparison_df['Status'] != 'complete']
        or_ongoing_df.to_csv(or_filename_ongoing, index=False)
        or_ongoing_df.to_csv(or_output_file_list[2], index=False, mode='a', header=False)

    or_latest_data_df.to_csv(or_filename_latest, index=False)

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

    for query in config['queries'].keys():
        print(f'\nRunning for {query}')
        item_dictionary = get_data(token, config['default params'], config['default filters'], query)
        if 'itemSummaries' in item_dictionary:
            pruned_data_df = prune_data(item_dictionary['itemSummaries'], config['default refinements'],
                                        list(config['default columns'].keys()), query)
            output_results(pruned_data_df, query, output_files)

    # Close output files
    close_output_files(output_files)