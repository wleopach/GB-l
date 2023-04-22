from data_reader import load_dicts
import psutil
import pickle
import tqdm
import gc
import ray

load = {'Produccion'}

prod = load_dicts(load)['Produccion']

cod_ = ['ID_CUSTOMER', 'DATE', 'TELEPHONE', 'CALIFICACION', 'DESCRIPTION_COD1', 'COD1']
gestion = {key: value['GESTION'][cod_] for key, value in prod.items() if 'GESTION' in value}
cc_g = [set(value['ID_CUSTOMER']) for value in gestion.values()]
cc = set.union(*cc_g)

act_ = ['ID_CUSTOMER', 'DATE', 'CUSTOMER_PHONE', 'CUSTOMER_CHARS', 'DESCRIPTION_COD_ACT']
gestion_wp = {key: value['GESTION_WHATSAPP'][act_] for key, value in prod.items() if 'GESTION_WHATSAPP' in
              value}
cc_gw = [set(value['ID_CUSTOMER']) for value in gestion_wp.values()]
ccw = set.union(*cc_gw)

new_dict = {}

# gestion_ = ray.put(gestion)
# gestion_wp_ = ray.put(gestion_wp)

tuples1 = [(c, gestion, 'GESTION') for c in cc]

tuples2 = [(c, gestion_wp, 'GESTION_WHATSAPP') for c in ccw]

tuples = tuples1 + tuples2


@ray.remote
def process_customer(t: tuple):
    new_dict.setdefault(t[0], dict())
    for key in tqdm(t[1]):
        key_ = t[1][key]
        if t[0] in set(key_['ID_CUSTOMER']):
            new_dict[t[0]][(key[0], key[1], t[2])] = key_[key_['ID_CUSTOMER'] == t[0]]


num_cpus = psutil.cpu_count(logical=True)
ray.init(num_cpus=int(num_cpus), ignore_reinit_error=True, include_dashboard=False)
refs = [process_customer.remote(t) for t in tuples]
pis = ray.get(refs)
pis = [a for a in pis if a if a != "error"]
gc.collect()
while ray.is_initialized():
    ray.shutdown()
print("Ray_Seguro")
del pis
del refs

with open("data/outputs/Productividad_Nuevo" + ".pickle", 'wb') as handle:
    pickle.dump(new_dict, handle, protocol=pickle.HIGHEST_PROTOCOL)
