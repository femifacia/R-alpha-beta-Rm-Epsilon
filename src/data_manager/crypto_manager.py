import os
import pandas as pd

from binance.client import Client
client = Client(api_key='', api_secret='')


tag = "crypto"


def get_tag() -> str:
    return tag

def get_binance_ticker_sdk(symbol="BTCUSDT", interval="1h", start_str="2024-03-01"):
    # Récupère toutes les k-lines depuis la date spécifiée
    klines = client.get_historical_klines(
        symbol=symbol,
        interval=interval,
        start_str=start_str
    )

    # Colonnes de retour
    columns = [
        "timestamp", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "number_of_trades",
        "taker_buy_base_volume", "taker_buy_quote_volume", "ignore"
    ]

    # Mise en DataFrame
    df = pd.DataFrame(klines, columns=columns)

    # Conversions utiles
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")
    num_cols = ["open", "high", "low", "close", "volume"]
    df[num_cols] = df[num_cols].astype(float)
    df.index = df['timestamp']
    df = df.drop(columns='timestamp')

    return df

def update_securities(src_dir="../data", tag="crypto", tickers=None, conf_file="../../conf/list_crypto_securities.conf") -> bool:
    if not os.path.isdir(src_dir):
        raise Exception('src path does not exist')
    if not os.path.isdir(os.path.join(src_dir, tag)):
        raise Exception(f"the path {os.path.join(src_dir, tag)} does not exist")
    if not tickers:
        if not os.path.isfile(conf_file):
            raise Exception("Conf file path does not exist")
        try:
            fd = open(conf_file, 'r')
            lines = fd.read()
            tickers = list(filter(lambda x : x, lines.split('\n')))
        except Exception as e:
            print(f"Something happens during the {tag} loading securities: \n{e}")
            return False
    for ticker in tickers:
        start = "2017-01-01"    
        save_path = os.path.join(src_dir,tag, ticker + '.csv')
        previous_data = None
        if  os.path.exists(save_path) and os.path.isfile(save_path):
            previous_data = pd.read_csv(save_path,index_col=0)
            start = previous_data.index[-1]
            previous_data = previous_data.iloc[:-1]
        print(f'for security {ticker}, the updating start at {start}.\nThe file will be saved at {save_path}')
        print(ticker)
        new_df = get_binance_ticker_sdk(symbol=ticker, start_str=start)
        result_df = pd.concat([previous_data,new_df],axis=0,verify_integrity=True).drop(columns='ignore')
        result_df.to_csv(save_path)
    return True

def get_securities(src_dir="../../data", tag="crypto", tickers=None, conf_file="../../conf/list_crypto_securities.conf", start=None, end=None):
    if not os.path.isdir(src_dir):
        raise Exception('src path does not exist')
    if not os.path.isdir(os.path.join(src_dir, tag)):
        raise Exception(f"the path {os.path.join(src_dir, tag)} does not exist")
    if not tickers:
        if not os.path.isfile(conf_file):
            raise Exception("Conf file path does not exist")
        try:
            fd = open(conf_file, 'r')
            lines = fd.read()
            tickers = list(filter(lambda x : x, lines.split('\n')))
        except Exception as e:
            print(f"Something happens during the {tag} loading securities: \n{e}")
            return None
    ans = {}
    for ticker in tickers:
        try:
            df = pd.read_csv(f'{os.path.join(src_dir, tag, f"{ticker}.csv")}',index_col=0)
            if start and df.index[0] > start:
                raise Exception('Start not in bench')
            if end and df.index[-1] < end:
                raise Exception('End not in bench')
        except:
            df = update_securities(src_dir=src_dir, ticker=ticker)
        if start:
            df = df.loc[start:]
        if end:
            df = df.loc[:end]
        ans[ticker] = df
    return ans



if __name__ == '__main__':
    #update_securities(src_dir="../../data/")
    dfs  = get_securities()
    for i in dfs:
        print(i, dfs[i].head())