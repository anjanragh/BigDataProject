import pandas as pd
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import apriori

def print_frequent_itemsets(jsonfilename):
    with open('2003_Campaign_Contributions.tsv.json') as f:
        data = json.load(f)
    ls = []
    for col in data["columns"]:
        l=[]
        for tp in col["dataTypes"]:
            dt = tp["type"]
            l.append(dt)
        ls.append(l)
        
    te = TransactionEncoder()
    te_ary = te.fit(ls).transform(ls)
    df = pd.DataFrame(te_ary, columns=te.columns_)
    fi = apriori(df, min_support=0.1, use_colnames=True)
    fi['length'] = fi['itemsets'].apply(lambda x: len(x))
    for i in range(2,4):
        print(i,"frequent itemset")
        print("------------------------------------")
        tm = fi[ (fi['length'] == i) & (fi['support'] >= 0.1) ]
        if tm.size==0:
            print("No itemsets present")
        else:
            print(tm["itemsets"].to_string())
        print("------------------------------------\n")
