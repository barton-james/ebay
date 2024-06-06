import os
import requests
from urllib import parse
import base64
import configparser
import pandas as pd

# Get the latest authorisation token to use for the calls
def get_auth_token():
    # Read the private keys and encode them
    gat_auth_config = configparser.ConfigParser()
    gat_auth_config.read('private_auth.ini')
    gat_authorization = base64.b64encode(bytes(gat_auth_config['keys']['client_id'] + ":" +
                                               gat_auth_config['keys']['client_secret'], "ISO-8859-1")).decode("ascii")
    # Set up the query header and body
    gat_auth_headers = {
        "Content-Type": gat_auth_config['params']['content_type'],
        "Authorization": "Basic " + gat_authorization
    }
    gat_body = {
        "grant_type": gat_auth_config['params']['grant_type'],
        "scope": gat_auth_config['params']['scope']
    }

    # Encode the query into a URL string and load the token request URL from the config
    gat_data = parse.urlencode(gat_body)
    gat_token_url = gat_auth_config['params']['token_url']

    # Make the call to request the token
    gat_auth_response = requests.post(gat_token_url, headers=gat_auth_headers, data=gat_data)
    return gat_auth_response.json()['access_token']


# Open the generic output files to which all results are appended
def open_output_files():
    oof_f_list = []
    oof_output_dir = f'output'

    # Create the output directory if it doesn't exist
    if not os.path.exists(oof_output_dir):
        os.mkdir(oof_output_dir)

    # Open the output files and store the file pointers in a list
    oof_f_list.append(open(f'{oof_output_dir}/all_status.csv', "w"))
    oof_f_list.append(open(f'{oof_output_dir}/all_completed.csv', "w"))
    oof_f_list.append(open(f'{oof_output_dir}/all_ongoing.csv', "w"))
    oof_f_list.append(open(f'{oof_output_dir}/all_new.csv', "w"))

    print('Output files opened')
    return oof_f_list


# Close the generic output files
def close_output_files(cof_output_fs):
    for cof_f in cof_output_fs:
        cof_f.close()
    print('\nOutput files closed')


# Construct the query URL and request the data from the API
def get_data(gd_auth_token, gd_params, gd_filters, gd_item):
    # Set the country market we want to query and the query URL
    gd_header = construct_header(gd_auth_token, gd_params['market_place'])
    gd_search_str = 'https://api.ebay.com/buy/browse/v1/item_summary/search'

    # Append the item search string as a URL parameter
    gd_search_str += f'?q={gd_item}'

    # Add all other URL parameters that we loaded from the config file
    for gd_key, gd_value in gd_params.items():
        if gd_key == 'market_place':
            continue
        # print(f'gd_key:{gd_key} , gd_value:{gd_value}')
        gd_search_str += f'&{gd_key}={gd_value}'
    # print(gd_search_str)

    # A single URL parameter called 'filter' is further used to filter the results
    # Set this parameter as a comma separated list of all the filter config parameters
    gd_search_str += '&filter='
    for gd_value in gd_filters.values():
        # print(f'gd_value:{gd_value}')
        gd_search_str += f'{gd_value},'
    # print(gd_search_str)

    # Call function to request results using the fully constructed URL
    gd_returned_dict = loop_and_get_data(gd_search_str, gd_header)
    return gd_returned_dict


# Construct the header to use for each query
def construct_header(ch_auth_token, ch_market_place):
    return {'X-EBAY-C-MARKETPLACE-ID': ch_market_place,
            'X-EBAY-C-ENDUSERCTX': 'affiliateCampaignId=<ePNCampaignId>,affiliateReferenceId=<referenceId>',
            'Authorization': 'Bearer ' + ch_auth_token
            }


# Make an API request
def make_request(mr_url_string, mr_header_string):
    print(f'Query string: {mr_url_string}')
    r = requests.get(mr_url_string, headers=mr_header_string)
    print(f'Query response: {r.status_code}')

    # Parse the content as JSON and return a dictionary
    mr_r_dict = r.json()
    # print(json.r_json(items, indent=4))
    return mr_r_dict


# Send API requests until all data has been returned
def loop_and_get_data(lagd_url_value, lagd_header_value):
    # Call a function to make the first request and return the items in a dictionary
    lagd_response_dict = make_request(lagd_url_value, lagd_header_value)
    lagd_items_dict = lagd_response_dict

    # If there is more data to receive then the API will return a field called 'next' that contains the next URL
    # So loop and get data until this field isn't returned any more
    while "next" in lagd_response_dict:
        lagd_next_query = lagd_response_dict['next']
        lagd_response_dict = make_request(lagd_next_query, lagd_header_value)
        # Build a dictionary of all returned responses
        lagd_items_dict['itemSummaries'].extend(lagd_response_dict['itemSummaries'])

    if 'itemSummaries' in lagd_items_dict:
        print(f'Count of items returned: {len(lagd_items_dict['itemSummaries'])}')
    else:
        print(f'Count of items returned: 0')
    # print(json.dumps(items_dict, indent=4))
    return lagd_items_dict


# Filter and prune the returned data
def prune_data(pd_list_of_dict, pd_refinements, pf_columns, pd_item_type):
    # Drop items with no shipping options as these are probably collection only
    pd_filtered_list = [pd_each_dict for pd_each_dict in pd_list_of_dict if 'shippingOptions' in pd_each_dict]

    # Flatten the dictionary of results into a dataframe table for easy manipulation
    pd_df = pd.json_normalize(pd_filtered_list)

    # Drop any columns not listed in the configuration
    pd_filtered_df = pd_df.loc[:, pd_df.columns.isin(pf_columns)]

    # Drop records with feedback scores and percentages out of range
    pd_filtered_df = pd_filtered_df[pd_filtered_df['seller.feedbackScore'] < int(pd_refinements['max_feedback_score'])]
    pd_filtered_df = pd_filtered_df[pd_filtered_df['seller.feedbackScore'] > int(pd_refinements['min_feedback_score'])]
    pd_filtered_df = pd_filtered_df[pd_filtered_df['seller.feedbackPercentage'].astype(str).astype(float) >
                                    float(pd_refinements['min_feedback_percentage'])]

    # Assign types to remaining untyped fields so that data is easier to merge later
    pd_filtered_df['legacyItemId'] = pd_filtered_df['legacyItemId'].astype(int)
    pd_filtered_df['seller.feedbackPercentage'] = pd_filtered_df['seller.feedbackPercentage'].astype(float)

    # Parse the shipping price string to extract the price into a new column
    if 'shippingPrice' in pd_filtered_df.columns:
        pd_filtered_df['shippingPrice'] = pd_filtered_df['shippingOptions'].astype(str).str.split('\'').str[9]
    else:
        pd_filtered_df['shippingPrice'] = 0
    # Drop the shipping options column
    pd_filtered_df.drop(columns=['shippingOptions'], inplace=True)

    # If a price exists then sum this with the shipping as a total price
    if 'price.value' in pd_filtered_df.columns:
        pd_filtered_df['price.value'] = pd_filtered_df['price.value'].astype(float)
        pd_filtered_df['totalPrice'] = (pd_filtered_df['shippingPrice'].astype(float) +
                                        pd_filtered_df['price.value'].astype(float))
    else:
        pd_filtered_df['shippingPrice'] = 0
        pd_filtered_df['totalPrice'] = 0

    # Add a new field which records the product searched for then sort the columns for consistency in merging later on
    pd_filtered_df['itemType'] = pd_item_type
    pd_filtered_df = pd_filtered_df.reindex(sorted(pd_filtered_df.columns), axis=1)

    print(f'Number of filtered items: {len(pd_filtered_df)}')
    # pd_filtered_df.info()
    return pd_filtered_df


# Merge results from all configured searches and output the results to file
def output_results(or_latest_data_df, or_item_name, or_output_file_list):
    or_output_dir = f'output'
    or_filename_latest = f'{or_output_dir}/{or_item_name}.csv'
    or_filename_previous = f'{or_output_dir}/{or_item_name}_previous.csv'
    or_filename_completed = f'{or_output_dir}/{or_item_name}_completed.csv'
    or_filename_ongoing = f'{or_output_dir}/{or_item_name}_ongoing.csv'
    or_filename_new = f'{or_output_dir}/{or_item_name}_new.csv'
    or_filename_status = f'{or_output_dir}/{or_item_name}_status.csv'

    # If we have a previous dataset then we can use this to extract any new or changed records
    if os.path.exists(or_filename_latest):
        # Rename the last data as previous data and read it in to a dataframe
        os.rename(or_filename_latest, or_filename_previous)
        or_previous_data_df = pd.read_csv(or_filename_previous)

        # Merge the previous and current datasets using the unique item number field as index
        # Add a suffix to any rows taken from the previous dataset and include a field to indicate which datasets
        # each row was found in
        or_comparison_df = pd.merge(or_latest_data_df, or_previous_data_df, indicator='Status', on=['legacyItemId',
                                                                                                    'itemWebUrl'],
                                    how='outer', suffixes=('', '_prev'))

        # Remove columns from the previous dataset
        or_comparison_df.drop(or_comparison_df.filter(regex='_prev$').columns, axis=1, inplace=True)

        # Use the indicator field and rename the values to show new, ongoing and complete rows
        # If a row appears only in the previous dataset then the sale must have completed since the last run
        # If a row appears only in the current dataset then it must be a new item for sale
        # If a row appears in both datasets then the sale is still ongoing
        or_comparison_df['Status'] = or_comparison_df['Status'].map({'both': 'ongoing', 'left_only': 'new',
                                                                     'right_only': 'complete'})

        # Sort the rows based on the sale status field and write it to the per product file and the top level file
        or_comparison_df.sort_values(by='Status', inplace=True)
        or_comparison_df.to_csv(or_filename_status, index=False)
        or_comparison_df.to_csv(or_output_file_list[0], index=False, mode='a', header=False)

        # Get only complete records and write a separate file for those
        or_completed_df = or_comparison_df.loc[or_comparison_df['Status'] == 'complete']
        or_completed_df.to_csv(or_filename_completed, index=False)
        or_completed_df.to_csv(or_output_file_list[1], index=False, mode='a', header=False)

        # Get only ongoing records and write a separate file for those
        or_ongoing_df = or_comparison_df.loc[or_comparison_df['Status'] == 'ongoing']
        or_ongoing_df.to_csv(or_filename_ongoing, index=False)
        or_ongoing_df.to_csv(or_output_file_list[2], index=False, mode='a', header=False)

        # Get only new records and write a separate file for those
        or_new_df = or_comparison_df.loc[or_comparison_df['Status'] == 'new']
        or_new_df.to_csv(or_filename_new, index=False)
        or_new_df.to_csv(or_output_file_list[3], index=False, mode='a', header=False)

    # Store the current dataset for use next time
    or_latest_data_df.to_csv(or_filename_latest, index=False)

    print(f'Output files written')
    return


# This program uses the ebay API to find certain products, parse and filter them and outputs some csv files
if __name__ == '__main__':
    # Read the configuration
    config = configparser.RawConfigParser(allow_no_value=True)
    config.optionxform = lambda option: option
    config.read('config.ini')

    # Get the auth token to use for data retrieval
    token = get_auth_token()

    # Open the generic output files
    output_files = open_output_files()

    # Make successive calls for each query configuration
    for query in config['queries'].keys():
        print(f'\nRunning for {query}')
        # Query for the data and return it in a dictionary
        item_dictionary = get_data(token, config['default params'], config['default filters'], query)
        # If any records were returned then filter them according to config rules
        if 'itemSummaries' in item_dictionary:
            pruned_data_df = prune_data(item_dictionary['itemSummaries'], config['default refinements'],
                                        list(config['default columns'].keys()), query)
            # Write the filtered results to csv files
            output_results(pruned_data_df, query, output_files)

    # Close the output files
    close_output_files(output_files)
