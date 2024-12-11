import requests
import pandas as pd
from datetime import datetime, date

from pandas import Timedelta


def datetime_to_timestamp(datetime_obj) -> int:
    """Converts a datetime object to a Unix timestamp in milliseconds."""
    return int(datetime.timestamp(datetime_obj) * 1000)


def timestamp_to_datetime(timestamp) -> str:
    """Converts a Unix timestamp in milliseconds to a datetime object."""
    return datetime.fromtimestamp(timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

def filter_latest_tick_per_minute(trades, last_timestamp: int, shift: int = 0):
    """
    Filters the latest tick per minute from the list of trades.
    Args:
        trades (list): List of trade dictionaries containing 'timestamp'.
        last_timestamp (int): The latest timestamp of the previous batch to compare against.
        shift (int): additional increment of the timestamp (in minutes).
    Returns:
        list: Filtered list containing only the latest tick per minute.
    """
    trades_df = pd.DataFrame(trades)
    # Convert timestamp to datetime
    trades_df['datetime'] = pd.to_datetime(trades_df['timestamp'], unit='ms')

    # Truncate timestamp to the minute
    trades_df['minute'] = trades_df['datetime'].dt.floor('min')

    # Filter trades that are older than current_end_timestamp (without seconds and ms)
    truncated_end = pd.to_datetime(last_timestamp, unit='ms').replace(second=0, microsecond=0)
    # Only consider trades where the minute is less than current_end_timestamp's truncated time
    trades_df = trades_df[trades_df['minute'] < truncated_end + Timedelta(minutes=shift)]

    # Group by minute and get the latest tick in each group
    # latest_ticks = trades_df.groupby('minute', group_keys=False).apply(
    #     lambda group: group.loc[group['timestamp'].idxmax()]).reset_index(drop=True)
    # latest_ticks = latest_ticks.drop(columns='minute')
    latest_ticks = trades_df.groupby('minute').agg(
        latest_tick=('timestamp', 'idxmax')
    ).reset_index()

    # Merge to get the full row for the latest trade
    latest_ticks = pd.merge(latest_ticks, trades_df, left_on='latest_tick', right_index=True).drop(
        columns='latest_tick')
    return latest_ticks.to_dict(orient='records')


def parse_instrument(instrument_name, kind='future'):
    """
    Parse the instrument name to determine the contract type and expiration date.

    Args:
        instrument_name (str): The name of the instrument (e.g., 'BTC-29MAR24' or 'BTC-PERP').
        kind (str): The kind of instrument (e.g., 'future' or 'option').

    Returns:
        dict: A dictionary containing the contract type ('perpetual' or 'futures') and the expiration date (if applicable).
    """
    if "-PERP" in instrument_name:
        return {"contract_type": "perpetual", "expiration_date": None}

    try:
        if kind == 'future':
            # For futures contracts, extract expiration date
            # Expected format: SYMBOL-DAYMONYY (e.g., BTC-29MAR24)
            parts = instrument_name.split("-")
            if len(parts) != 2:
                raise ValueError("Invalid instrument format")

            date_str = parts[1]  # e.g., "29MAR24"
            expiration_date = datetime.strptime(date_str, "%d%b%y")  # Parse date
            formatted_date = expiration_date.strftime("%Y-%m-%d")
            return {"contract_type": kind, "expiration_date": formatted_date}
        elif kind == 'option':
            return None
    except Exception as e:
        raise ValueError(f"Failed to parse instrument name '{instrument_name}': {e}")


def process_trade_data(trades, kind_value='future'):
    result = []
    for trade in trades:
        # Determine the value of the 'kind' field
        instrument_name = trade['instrument_name']
        parsed_data = parse_instrument(instrument_name, kind_value)

        # Filter and format the fields
        processed_trade = {
            'timestamp': trade['timestamp'],
            'instrument_name': trade['instrument_name'],
            'price': trade['price'],
            'mark_price': trade['mark_price'],
            'index_price': trade['index_price'],
            'direction': trade['direction'],
            'amount': trade['amount'],
            'kind': parsed_data["contract_type"],
            'expiration_date': parsed_data["expiration_date"],
        }
        result.append(processed_trade)
    return result


def derivative_data(currency: str, kind: str, start_date: date, end_date: date, count: int = 1000) -> pd.DataFrame:
    """Returns derivative trade data for a specified currency and time range.

    Args:
        currency (str): The currency symbol, e.g. 'BTC'.
        kind (str): The type of derivative, either 'option' or 'future'.
        start_date (date): The start date of the time range (inclusive).
        end_date (date): The end date of the time range (inclusive).
        count (int, optional): The maximum number of trades to retrieve per request. Defaults to 10000.

    Returns:
        pandas.DataFrame: A dataframe of derivative trade data for the specified currency and time range.
    """

    # Validate input arguments
    assert isinstance(currency, str), "currency must be a string"
    assert isinstance(start_date, date), "start_date must be a date object"
    assert isinstance(end_date, date), "end_date must be a date object"
    assert start_date <= end_date, "start_date must be before or equal to end_date"

    derivative_list = []
    params = {
        "currency": currency,
        "kind": kind,
        "count": count,
        "include_old": True,
        "start_timestamp": datetime_to_timestamp(datetime.combine(start_date, datetime.min.time())),
        # "end_timestamp": datetime_to_timestamp(datetime.combine(end_date, datetime.max.time()))
    }

    url = 'https://history.deribit.com/api/v2/public/get_last_trades_by_currency_and_time'

    last_trade_id = None

    current_end_timestamp = datetime_to_timestamp(datetime.combine(end_date, datetime.max.time()))
    first_ts_shift = 1
    batch_number = 1

    print(datetime_to_timestamp(datetime.combine(start_date, datetime.min.time())),
          datetime_to_timestamp(datetime.combine(end_date, datetime.max.time())))

    with requests.Session() as session:
        while current_end_timestamp >= params["start_timestamp"]:
            params["end_timestamp"] = current_end_timestamp
            try:
                response = session.get(url, params=params)
                response.raise_for_status()
                response_data = response.json()
                result = response_data['result']['trades']

                if result is None or len(result) == 0:
                    break

                # result = sorted(result, key=lambda res: (res['timestamp'], res['trade_id']))
                if batch_number == 223:
                    print(result)
                    # break

                # exclude duplicates
                filtered_result = [
                    res for res in result
                    if not (res['timestamp'] == current_end_timestamp and res['trade_id'] >= last_trade_id)
                ]

                if filtered_result is None or len(filtered_result) == 0:
                    print("No new trades after filtering.")
                    break

                if filtered_result is None or len(filtered_result) == 0:
                    print("No new trades after filtering.")
                    break

                filtered_minutes_result = filter_latest_tick_per_minute(filtered_result, current_end_timestamp,
                                                                        first_ts_shift)
                if filtered_minutes_result is not None and len(filtered_minutes_result) > 0:
                    # We need to add 1 minute for 1st iteration only and reset then
                    first_ts_shift = 0
                    processed_trades = process_trade_data(filtered_minutes_result, kind_value=kind)
                    derivative_list.extend(processed_trades)
                else:
                    print("Skipping an iteration with already saved minute data.")
                    continue

                # The earliest timestamp in the batch
                last_trade = filtered_result[0]
                current_end_timestamp = last_trade["timestamp"]
                last_trade_id = last_trade["trade_id"]

                st_ts = timestamp_to_datetime(current_end_timestamp)
                end_ts = timestamp_to_datetime(params["end_timestamp"])
                print(f"Batch {batch_number}: Received {len(result)} -> {len(filtered_result)} ->"
                      f" {len(filtered_minutes_result)} EOM trades from {st_ts} to {end_ts}.")

                batch_number += 1

            except requests.exceptions.RequestException as e:
                print(f"Request failed: {e}")
                break
            except KeyError:
                print("Unexpected response structure")

    if derivative_list is None or len(derivative_list) == 0:
        print("No trades found for the specified range.")
        return pd.DataFrame()

    derivative_df = pd.DataFrame(derivative_list)
    derivative_df["date_time"] = pd.to_datetime(derivative_df["timestamp"], unit='ms')
    return derivative_df


if __name__ == "__main__":
    currency = 'BTC'
    kind = 'future'
    start_date_str = '2024-01-01'
    end_date_str = '2024-01-01'
    start_date: date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
    end_date: date = datetime.strptime(end_date_str, '%Y-%m-%d').date()

    df = derivative_data(currency, 'future', start_date, end_date, count=1000)
    print(df.head())
    print(df.tail())
