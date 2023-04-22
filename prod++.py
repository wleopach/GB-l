from data_reader import load_dicts
import psutil
import pickle
from tqdm import tqdm
import gc
import ray
import random

load = {'Produccion'}

prod = load_dicts(load)['Produccion']

cod_ = ['ID_CUSTOMER', 'DATE', 'TELEPHONE', 'CALIFICACION', 'DESCRIPTION_COD1', 'COD1']
gestion = {key: value['GESTION'][cod_] for key, value in prod.items() if 'GESTION' in value}
cc_g = [set(value['ID_CUSTOMER']) for value in gestion.values()]
tuples1 = [(key, row, 'GESTION')
           for key, value in tqdm(gestion.items())
           for index, row in value.iterrows() if row['ID_CUSTOMER'] != 'nan']

act_ = ['ID_CUSTOMER', 'DATE', 'CUSTOMER_PHONE', 'CUSTOMER_CHARS', 'DESCRIPTION_COD_ACT']
gestion_wp = {key: value['GESTION_WHATSAPP'][act_] for key, value in prod.items() if 'GESTION_WHATSAPP' in
              value}
cc_gw = [set(value['ID_CUSTOMER']) for value in gestion_wp.values()]
ccw = set.union(*cc_gw)
tuples2 = [(key, row, 'GESTION_WHATSAPP')
           for key, value in tqdm(gestion_wp.items())
           for index, row in value.iterrows() if row['ID_CUSTOMER'] != 'nan']

new_dict = {}

tuples = tuples1 + tuples2

cols = {'GESTION': cod_[1:],
        'GESTION_WHATSAPP': act_[1:]}


@ray.remote
def process_customer(t: tuple):
    row = t[1]
    new_dict.setdefault(row['ID_CUSTOMER'], dict())
    key = t[0]
    new_dict[row['ID_CUSTOMER']].setdefault((key[0], key[1], t[2]), set())
    new_dict[row['ID_CUSTOMER']][(key[0], key[1], t[2])].add(tuple(row[cols[t[2]]]))
    return  new_dict


num_cpus = psutil.cpu_count(logical=True)
ray.init(num_cpus=int(num_cpus), ignore_reinit_error=True, include_dashboard=False)
for t in tqdm(tuples):
    process_customer.remote(t)
print('Wrapping results')
Diccionario_Com = new_dict.copy()
gc.collect()
while ray.is_initialized():
    ray.shutdown()
print("Ray_Seguro")


# Get a random key-value pair
random_key, random_value = random.choice(list(Diccionario_Com.items()))

print("Random key:", random_key)
print("Random value:", random_value)
print('writing the results')
with open("data/outputs/Productividad_Nuevo" + ".pickle", 'wb') as handle:
    pickle.dump(Diccionario_Com, handle, protocol=pickle.HIGHEST_PROTOCOL)
