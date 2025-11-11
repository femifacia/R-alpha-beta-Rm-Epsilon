import crypto_manager


libs_arr = [crypto_manager]
tag_dict = {i.get_tag() :  i for i in libs_arr}


def get_securities(tag="crypto", tickers=None, start=None, end=None, src_dir="../../data", conf_file="../../conf/list_crypto_securities.conf"):
    lib = tag_dict[tag]
    ans = lib.get_securities(tickers=tickers, start=start, end=end, conf_file=conf_file)
    return ans


if __name__ == "__main__":
    print(tag_dict)
    sah = get_securities("crypto", tickers={"BTCUSDT", "SOLUSDT"}, start="2020-01-01")
    for i in sah:
        print(i, len(sah[i]), sah[i].head())
    print(len(sah))