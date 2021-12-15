from itertools import combinations
from operator import itemgetter
import time

import pandas as pd

FILE_PATH = "Online Retail.xlsx"
sheet5k = "Retail5k"
sheet10k = "Retail10k"
sheet500k = "Online Retail"
min_sup = 20
preprocess_start = time.time()
data_set = pd.read_excel(FILE_PATH, sheet_name=sheet10k, usecols="A:H")
data_set = data_set[~data_set["InvoiceNo"].str.contains("С|C", na=False)][['StockCode', 'CustomerID']].dropna() \
    .astype(str)

grouped = data_set.groupby(['CustomerID'], as_index=False).agg({'StockCode': ', '.join})
print(grouped)

stock_count = grouped['StockCode'].str.get_dummies(', ').sum().reset_index()
stock_count = stock_count['index']
index_code = stock_count.to_dict()
codes_dictionary = {v: k for k, v in index_code.items()}

new_ds = data_set.replace({'StockCode': codes_dictionary}).astype(str)
grouped_with_numbers = new_ds.groupby(['CustomerID'], as_index=False).agg({'StockCode': ', '.join})
print(f'Preprocess Time: {time.time() - preprocess_start}')


def apriori_method(data, sup):
    single_items = (data['StockCode'].str.split(', ', expand=True)) \
        .apply(pd.value_counts).sum(axis=1) \
        .where(lambda val: val > sup).dropna()
    apriori_data = pd.DataFrame(
        {'items': single_items.index.astype(int),
         'sup': single_items.values,
         'set_size': 1}
    )

    data['set_size'] = data['StockCode'].str.count(", ") + 1
    data['StockCode'] = data['StockCode'].apply(lambda row: set(map(int, row.split(", "))))

    single_items_set = set(single_items.index.astype(int))

    for length in range(2, len(single_items_set) + 1):
        data = data[data['set_size'] >= length]
        d = data['StockCode'] \
            .apply(lambda st: pd.Series(s if set(s).issubset(st) else None
                                        for s in combinations(single_items_set, length))) \
            .apply(lambda col: [col.dropna().unique()[0], col.count()] if col.count() >= sup else None).dropna()

        if d.empty:
            break

        apriori_data = apriori_data.append(pd.DataFrame(
            {'items': list(map(itemgetter(0), d.values)),
             'sup': list(map(itemgetter(1), d.values)),
             'set_size': length}
        ), ignore_index=True)

    return apriori_data


apriori = grouped_with_numbers

start = time.time()
apriori_res = apriori_method(data=apriori, sup=min_sup).astype(str)
apriori_res = apriori_res[apriori_res["items"].str.contains(",")].dropna()


def replace_code(string):
    string = string.replace("(", "")
    string = string.replace(",", "")
    string = string.replace(")", "")
    index_code_strings = {str(key): str(val) for key, val in index_code.items()}
    res = ', '.join([index_code_strings.get(i, i) for i in string.split()])
    return res


apriori_res['items'] = apriori_res['items'].apply(lambda x: replace_code(x))
apriori_res = apriori_res.reset_index()
apriori_res = apriori_res.drop(columns=['index', 'set_size'])
print(apriori_res)
print(f'Apriori Time: {time.time() - start}')
