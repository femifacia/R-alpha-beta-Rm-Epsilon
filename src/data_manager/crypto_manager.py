import os

def update_securities(src_dir="../data", tag="crypto", tickers=None, conf_file="../../conf/list_crypto_securities.conf") -> None:
    if not os.path.isdir(src_dir):
        raise Exception('src path does not exist')
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
        print(tickers)


if __name__ == '__main__':
    update_securities(src_dir="../../data/")