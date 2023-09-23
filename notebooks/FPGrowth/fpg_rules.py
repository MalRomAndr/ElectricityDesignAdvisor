import pickle
import pandas as pd
from mlxtend.frequent_patterns import fpgrowth, association_rules
from mlxtend.preprocessing import TransactionEncoder


te = TransactionEncoder()

df = pd.read_pickle('fpg_df.pickle')


# Создаем список транзакций
transactions = df.groupby(by='StructureId')['Id'].unique().to_frame().reset_index(drop=False).drop(0, axis=0)

# Кодируем
encoded_tr = pd.DataFrame(te.fit(transactions['Id']).transform(transactions['Id']), columns=te.columns_)

# Найдём наиболее частые наборы
frequent_itemsets = fpgrowth(encoded_tr, min_support=0.001, use_colnames=True, max_len=2)

rules = association_rules(frequent_itemsets, metric="support", min_threshold=0)
rules['antecedents'] = rules['antecedents'].apply(lambda x: list(x)[0])
rules['consequents'] = rules['consequents'].apply(lambda x: list(x)[0])

with open('fpg_rules.pickle', 'wb') as file:
    pickle.dump(rules, file)

with open('fpg_transactions.pickle', 'wb') as file:
    pickle.dump(transactions, file)
