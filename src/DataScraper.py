#!/usr/bin/env python

import krakenex
import numpy as np
import pandas as pd
import time
import datetime
import sys
import argparse


# Command line parser
def parse_args():
    parser = argparse.ArgumentParser(usage="""{} """.
                                     format(sys.argv[0]),
                                     epilog="""Script to scrape data from Kraken about a currency pair.""")

    parser.add_argument("-p", "--pair", type=str, default='XETHZEUR',
                        help="""Currency pair""")
    parser.add_argument("-fo", "--output_file", type=str, default='',
                        help='Output file. Defaults to name of currency.')
    parser.add_argument("-fi", "--input_file", type=str, default='',
                        help='Input file to continue reading from where it was left of. Expects index to be timestamps')
    parser.add_argument("-v", "--verbose", type=bool, default=True,
                        help='Switch verbose on/off. Default is True')
    parser.add_argument("-f", "--file_format", type=str, default='csv',
                        help='File format to be returned')
    return parser.parse_args()


def main(args):
    # instantiate API object
    k = krakenex.API()

    if len(args.input_file) > 0:
        input_data = pd.read_csv(args.input_file, index_col=0)

        input_prices = input_data['{}_price'.format(args.pair)]
        input_volume = input_data['volume']
        input_buy_sell = input_data['buy/sell']
        input_market_limit = input_data['market/limit']

        try:
            if args.verbose:
                print('Loading data from {}\n'.format(args.input_file))

            # check if data is given as UNIX time (so seconds since 1970)
            # the final data file will be in the original time format
            if input_data.index.dtype == np.dtype('float'):
                input_timestamps = pd.to_datetime(input_data.index, unit='s').tolist()
            # if not assumes it's a string with time info
            else:
                input_timestamps = pd.to_datetime(input_data.index).tolist()
            #
            # if input_data.index.dtype == np.dtype('float'):
            #     input_timestamps = input_data.index.values.tolist()
            # # if not assumes it's a string with time info
            # else:
            #     input_timestamps = pd.to_datetime(input_data.index)

            last_id = '{:.0f}'.format(input_timestamps[-1].timestamp() * 1000000000)

        except:
            raise IOError("Expected index to contain datetime values")

    else:
        input_timestamps = []
        input_prices = []
        input_volume = []
        input_buy_sell = []
        input_market_limit = []

        last_id = 0

    # store data and trade volume in a list
    prices = []
    volumes = []
    buy_sell = []
    market_limit = []

    # keep track of iterations
    i = 0

    timestamps = []

    if args.verbose:
        print('Fetching your data...\n')

    while int(last_id) < time.time() * 1000000000:

        try:
            pair_data = k.query_public(method='Trades', req={'pair': args.pair, 'since': last_id})

            if 'result' not in pair_data.keys():
                time.sleep(10)
                continue
            else:
                last_id = pair_data['result']['last']

            prices += [x[0] for x in pair_data['result'][args.pair]]
            volumes += [x[1] for x in pair_data['result'][args.pair]]
            timestamps += [x[2] for x in pair_data['result'][args.pair]]
            buy_sell += [x[3] for x in pair_data['result'][args.pair]]
            market_limit += [x[4] for x in pair_data['result'][args.pair]]

            if i % 10 == 0 and args.verbose:
                print(datetime.datetime.fromtimestamp(timestamps[-1]).strftime('%d-%m-%Y %H:%M:%S'))
            i += 1

        except ValueError:
            if args.verbose:
                print('Reached request maximum. Sleeping for 10 seconds...')
            continue

        except:
            break

    if args.verbose:
        print("\nFinished retrieving all the data. Putting everything together in a {} file".format(args.file_format))

    # getting all the data together
    final_price = np.concatenate((input_prices, prices))
    final_volume = np.concatenate((input_volume, volumes))
    final_buy_sell = np.concatenate((input_buy_sell, buy_sell))
    final_market_limit = np.concatenate((input_market_limit, market_limit))
    final_timestamps = input_timestamps + pd.to_datetime(timestamps, unit='s').tolist()

    # print(len(final_timestamps))
    # print(len(final_volume))
    # print(len(final_price))

    final_dataset = pd.DataFrame(np.column_stack((final_price, final_volume, final_buy_sell, final_market_limit)),
                                 columns=['{}_price'.format(args.pair), 'volume', 'buy/sell', 'market/limit'],
                                 index=final_timestamps)

    if len(args.output_file) == 0:
        output_file = args.pair
    else:
        output_file = args.output_file

    final_dataset.to_csv(output_file)


if __name__ == "__main__":
    args = parse_args()
    main(args)
