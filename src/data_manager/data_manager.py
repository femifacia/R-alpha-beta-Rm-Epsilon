import crypto_manager


libs_arr = [crypto_manager]
tag_dict = {i.get_tag() :  i for i in libs_arr}

print(tag_dict)