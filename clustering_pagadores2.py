import pandas as pd
import numpy as np
from datetime import datetime

#%% carga
# asignacion=pd.read_csv('Data_2/DF_ASIGNACION_LIMPIA_COMPLETA.csv')
evolucion=pd.read_csv('DF_EVOLUCION_LIMPIA_COMPLETA.csv')
# pagos=pd.read_csv('Data_2/DF_PAGOS_LIMPIA_COMPLETA.csv')

def exceldate(excel_date):
    try:
        dt = datetime.fromordinal(datetime(1900, 1, 1).toordinal() + excel_date - 2)
        return dt.strftime("%Y-%m-%d, %H:%M:%S")
    except:
        return '2018-05-20 00:00:00'
# pagos2=pd.read_csv('pagos_propios.csv')

# del asignacion['Unnamed: 0']
del evolucion['Unnamed: 0']
# del pagos['Unnamed: 0']

#%% limpieza adicional
# mask=asignacion['CUPO_APROBADO']=="INFO_INCOMPLETA"
# asignacion.loc[mask,'CUPO_APROBADO']=np.nan
# asignacion['CUPO_APROBADO']=pd.to_numeric(asignacion['CUPO_APROBADO'])
# evolucion['CLIENTES']=evolucion['CLIENTES'].str.strip()
# evolucion['CUPO_APROBADO']=evolucion['CUPO_APROBADO'].astype(str)
# evolucion['CUPO_APROBADO']=evolucion['CUPO_APROBADO'].str.strip()
# evolucion['CUPO_APROBADO']=evolucion['CUPO_APROBADO'].str.replace('$','')
# evolucion['CUPO_APROBADO']=evolucion['CUPO_APROBADO'].str.replace(',','.')
# evolucion['CUPO_APROBADO']=evolucion['CUPO_APROBADO'].str.replace('-','0')
# # evolucion['CUPO_APROBADO']=evolucion['CUPO_APROBADO'].str.replace('.00','')
# # evolucion['CUPO_APROBADO']=evolucion['CUPO_APROBADO'].str.replace('.','')
# evolucion['CUPO_APROBADO']=pd.to_numeric(evolucion['CUPO_APROBADO'],errors='ignore')
# evolucion['NRO_REESTRUCTUR']=evolucion['NRO_REESTRUCTUR'].str.replace('E','0')
# evolucion['NRO_REESTRUCTUR']=evolucion['NRO_REESTRUCTUR'].str.strip()
# evolucion['NRO_REESTRUCTUR']=evolucion['NRO_REESTRUCTUR'].str.replace('$','')
# evolucion['NRO_REESTRUCTUR']=evolucion['NRO_REESTRUCTUR'].str.replace(',00','')
# evolucion['NRO_REESTRUCTUR']=pd.to_numeric(evolucion['NRO_REESTRUCTUR'])

#crear columna clasificacion si ya realizo algun pago o no

cedulas = evolucion['IDENTIFICACION'].drop_duplicates()
evolucion['ya_pago']=0
for c in cedulas:
    # c=evolucion.loc[157291,'IDENTIFICACION']
    dfc=evolucion.loc[evolucion['IDENTIFICACION'] == c]
    if dfc['PAGOS_MES'].cumsum().iloc[0] == 0 and dfc['PAGOS_MES'].cumsum().iloc[-1]>0:
        evolucion.loc[dfc.index,'ya_pago']=(dfc['PAGOS_MES'].cumsum()>0).astype(int)
        # print(c)
    elif dfc['PAGOS_MES'].cumsum().iloc[0] > 0:
        evolucion.loc[dfc.index,'ya_pago']=1

dicmes = dict(zip(['ENERO','FEBRERO','MARZO','ABRIL','MAYO','JUNIO','JULIO','AGOSTO','SEPTIEMBRE','OCTUBRE',
                   'NOVIEMBRE','DICIEMBRE'],range(1,13)))    
dicmes = {k:'0'+str(v) if len(str(v))==1 else str(v) for k,v in dicmes.items() }    
evolucion['FECHA_ULTIMA_CORRECTED']=\
    [('-').join((t+('15',))) for t in list(zip(evolucion.ANNO.astype(str),[dicmes[m] for m in evolucion.MES]))]
#%% construccion del input
evolucion_pagos = evolucion[evolucion['ya_pago']!=0]
cols = ['FECHA_APERTURA','FECHA_DE_CASTIGO','FECHA_ULTIMA_GESTION','FECHA_ULTIMA_CORRECTED',
       'DIAS_DE_MORA_ACTUAL','IDENTIFICACION', 'NOMBRE', 
       'PAGOS_MES', 'SALDO_CAPITAL_CLIENTE', 'SALDO_CAPITAL_MES',
       'SALDO_TOTAL',
       'OBLIGACION', 'DESCRIPCION_PRODUCTO','CP', 'COMPRA_DE_CARTERA',
       'ORDEN_DE_GESTION', 'ASESOR',
       'NOMBRE_RECUPERADOR', 'PLAZO_INICIAL', 'VALOR_ORIGINAL',
       'ACCION', 'TIPO_CONTACTO', 'TIPO_DE_ACUERDO', 'MOTIVO_DE_NO_PAGO',
       'META_%', 'META_$',
       'RANGO_CANT_LLAMADAS',
       'ID_TABLA', 'MES', 'ANNO']
evolucion_pagos = evolucion_pagos[cols]

evolucion_pagos.dropna(subset=['FECHA_ULTIMA_GESTION'],inplace=True)
evolucion_pagos=evolucion_pagos[evolucion_pagos!='00:00:00']
evolucion_pagos=evolucion_pagos[evolucion_pagos!='SALARIO']
evolucion_pagos['FECHA_ULTIMA_CORRECTED'] = pd.to_datetime(evolucion_pagos['FECHA_ULTIMA_CORRECTED'])
evolucion_pagos.sort_values(by=['IDENTIFICACION','FECHA_ULTIMA_CORRECTED'],inplace=True)
#%% *** Completar fechas_castigo que aparecen 1970
#reemplazar Nan por string
evolucion_pagos.loc[evolucion_pagos['FECHA_DE_CASTIGO'].apply(lambda x: str(x)=='nan')]='nan'
milnuesetenta=evolucion_pagos.loc[evolucion_pagos['FECHA_DE_CASTIGO'].apply(lambda x: '1970' in str(x) or str(x)=='SIN_FECHA_DE_CASTIGO' or str(x)=='SIN_INFORMACION' or str(x)=='nan')]
cedulasajuste=milnuesetenta['IDENTIFICACION'].drop_duplicates()
for k in range(len(cedulasajuste)):
    # k=0
    # print(k)
    c=cedulasajuste.iloc[k]
    ic=cedulasajuste.index[k]
    dfc=evolucion_pagos.loc[evolucion_pagos['IDENTIFICACION'] == c]
    lc=[f for f in dfc['FECHA_DE_CASTIGO'] if '20' == f[0:2]]
    ics=dfc.loc[dfc['FECHA_DE_CASTIGO'].apply(lambda x: x[0:2]!='20')].index
    if lc!=[]:
        evolucion_pagos.loc[list(ics),'FECHA_DE_CASTIGO']=lc[0]
    else:
        evolucion_pagos.loc[ics,'FECHA_DE_CASTIGO']='2018-05-20 00:00:00'
        dfc=evolucion_pagos.loc[evolucion_pagos['IDENTIFICACION'] == c]
        # print([f for f in dfc['FECHA_DE_CASTIGO'] if f[0:2]!='20'])
maskd=evolucion_pagos['FECHA_DE_CASTIGO'].apply(lambda x: len(str(x))<6 or '.0' in str(x))
evolucion_pagos.loc[maskd,'FECHA_DE_CASTIGO']=evolucion_pagos.loc[maskd,'FECHA_DE_CASTIGO'].apply(lambda x: exceldate(int(float(x))))
evolucion_pagos['FECHA_DE_CASTIGO'] = pd.to_datetime(evolucion_pagos['FECHA_DE_CASTIGO'])
#%% correccion tipo evolucion_pagos['SALDO_CAPITAL_CLIENTE']
evolucion_pagos['SALDO_CAPITAL_CLIENTE']=evolucion_pagos['SALDO_CAPITAL_CLIENTE'].astype(float).round(0).astype(float)
#%% ***nueva columna: meses iniciales pago fecha_ultima_gestion - fecha_castigo
evolucion_pagos['MESES_INICIALES_NO_PAGO'] = ((evolucion_pagos['FECHA_ULTIMA_CORRECTED'] - evolucion_pagos['FECHA_DE_CASTIGO'])/np.timedelta64(1, 'M')).round()
#***aprecen fechas castigo > FECHA_ULTIMA_GESTION - eliminarlas
evolucion_pagos=evolucion_pagos[evolucion_pagos['MESES_INICIALES_NO_PAGO']>=0]
evolucion_pagos['MESES_INICIALES_NO_PAGO'] = evolucion_pagos['MESES_INICIALES_NO_PAGO'].astype(int)
#%% ***nueva columna: pagos_mes/saldo_cappital_cliente
evolucion_pagos=evolucion_pagos[evolucion_pagos['PAGOS_MES'].astype(str)!='nan']
evolucion_pagos=evolucion_pagos[evolucion_pagos['SALDO_CAPITAL_CLIENTE']>0]
evolucion_pagos['PORCION_PAGO'] = evolucion_pagos['PAGOS_MES'].astype(int)/evolucion_pagos['SALDO_CAPITAL_CLIENTE']
#%% **nueva columna: ACCION_CONTACTO
evolucion_pagos['ACCION_CONTACTO'] = evolucion_pagos['ACCION'] + ' - ' + evolucion_pagos['TIPO_CONTACTO']
#%% nueva columna: limpieza Columna MOTIVO_DE_NO_PAGO
evolucion_pagos['MOTIVO_DE_NO_PAGO']=evolucion_pagos['MOTIVO_DE_NO_PAGO'].astype(str)
evolucion_pagos['MOTIVO_DE_NO_PAGO']=evolucion_pagos['MOTIVO_DE_NO_PAGO'].str.strip()
coun=evolucion_pagos.groupby('MOTIVO_DE_NO_PAGO')['IDENTIFICACION'].count()
coun_dic=dict(zip(coun.index,coun))
dic_motivo={p:'SIN_INFORMACION' for p in ['0','NO CONTESTAN','MS BUZON',
                                         'SIN_INFORMACION','CHATBOT','APLICACION ENCUESTA',
                                         'OTRO (CUAL?)']}
dic_motivo.update({p:'DESEMPLEO' for p in ['DESEMPLEO','LIQUIDACION']})
dic_motivo.update({p:'OTRAS DEUDAS' for p in ['OTRAS DEUDAS','PAGO DE OTRAS DEUDAS','PRESTAMO PERSONAL']})
dic_motivo.update({p:'DISMINUCION DE INGRESOS' for p in ['DISMINUCION DE INGRESOS','DIMINUCION DE INGRESOS',
                                                  'DISMINUCION INGRESOS FAMILIA','REDUCCION DE COMISIONES',
                                                  'DISMINUCION DE VENTAS','DISMINUCION VENTAS','DISMINUCION DE COMISIONES',
                                                  'CAMBIO DE EMPLEO CON REDUCCION DE INGRESOS']}) 
dic_motivo.update({p:'LE_DEBEN' for p in ['NO PAGO DE TERCEROS','ATRASO PAGO NOMINA','INCUMPLIMIENTO DE TERCEROS',
                                   'ATRASO PAGO DE EMPRESA CONTRATISTA']})                                                   
dic_motivo.update({p:'QUIEBRA' for p in ['QUIEBRA','QUIEBRA DE NEGOCIO']}) 
for key in set(coun_dic.keys())-set(dic_motivo.keys()):
    dic_motivo[key]='OTROS'  
dic_motivo['nan']='OTROS'                                              
evolucion_pagos['MOTIVO']= evolucion_pagos['MOTIVO_DE_NO_PAGO'].apply(lambda x: dic_motivo[x])    
coun2=evolucion_pagos.groupby('MOTIVO')['IDENTIFICACION'].count()
#%% nueva columna: Portafolio
#%% ***nueva columna: suma(pagos_mes)/saldo_cappital_cliente inicial    
cedulas_todas=evolucion_pagos['IDENTIFICACION'].drop_duplicates()
# k=0
evolucion_pagos['PORTAFOLIO']=0
evolucion_pagos['PORCION_PAGOS']=0
for k in range(len(cedulas_todas)):
    try:
        c = cedulas_todas.iloc[k]                                    
        ic=cedulas_todas.index[k]
        dffc=evolucion_pagos.loc[evolucion_pagos['IDENTIFICACION'] == c]
        ics=dffc.index
        conjunto_prods = [p for p in set(dffc['DESCRIPCION_PRODUCTO']) if p != 'SIN_INFORMACION']
        conjunto_prods.sort()
        evolucion_pagos.loc[ics,'PORTAFOLIO']=(' - ').join(conjunto_prods)
        for r in range(1,len(dffc)+1):
            evolucion_pagos.loc[ics[r-1],'PORCION_PAGOS'] = dffc[0:r]['PAGOS_MES'].sum()/dffc.iloc[0]['SALDO_CAPITAL_CLIENTE']
    except:
        pass
#%% limpieza Plazo_inicial
ajusteplazo = evolucion_pagos[evolucion_pagos['PLAZO_INICIAL'].astype(str)=='nan'] 
cedulasajusteplazo=ajusteplazo[['IDENTIFICACION','OBLIGACION']].drop_duplicates()
cedulasajusteplazolist = list(zip(cedulasajusteplazo['IDENTIFICACION'],cedulasajusteplazo['OBLIGACION']))
evolucion_pagos['PLAZO_INICIAL_ADJ']=0
for h in range(len(cedulasajusteplazolist)):
    dfcc=evolucion_pagos[(evolucion_pagos['IDENTIFICACION']==cedulasajusteplazolist[h][0]) & (evolucion_pagos['OBLIGACION']==cedulasajusteplazolist[h][1])]
    iccs=dfcc.index
    nonuls = [p for p in dfcc['PLAZO_INICIAL'] if str(p)!='nan']
    dfcc2=evolucion_pagos[(evolucion_pagos['IDENTIFICACION']==cedulasajusteplazolist[h][0])]
    nonuls2 = [p for p in dfcc2['PLAZO_INICIAL'] if str(p)!='nan']
    if nonuls!=[]:
        evolucion_pagos.loc[iccs,'PLAZO_INICIAL_ADJ'] = max([float(n) for n in nonuls])
    elif nonuls2!=[]:
        evolucion_pagos.loc[iccs,'PLAZO_INICIAL_ADJ'] = max([float(n) for n in nonuls2])
    else:
        evolucion_pagos.loc[iccs,'PLAZO_INICIAL_ADJ'] = 90
    mask0=evolucion_pagos[evolucion_pagos['PLAZO_INICIAL_ADJ']==0]
    evolucion_pagos.loc[mask0.index,'PLAZO_INICIAL_ADJ'] = evolucion_pagos.loc[mask0.index,'PLAZO_INICIAL'] 
evolucion_pagos['PLAZO_INICIAL_ADJ']=evolucion_pagos['PLAZO_INICIAL_ADJ'].astype(float)
evolucion_pagos.loc[np.isnan(evolucion_pagos['PLAZO_INICIAL_ADJ'])]=90
evolucion_pagos.loc[evolucion_pagos['PLAZO_INICIAL_ADJ']==0]=90
evolucion_pagos['PLAZO_INICIAL_ADJ']=evolucion_pagos['PLAZO_INICIAL_ADJ'].astype(float).round(0).astype(int)


#%%        
evolucion_pagos.to_csv('outputs/evolucion_pagos2.csv',index=False)

a=0
#%% Input clusterin No supervisado
# cols_input = ['SALDO_CAPITAL_CLIENTE','CP','ASESOR','NOMBRE_Víctor Maldonado, cerebro de desfalco a Interbolsa ...https://www.eltiempo.com › justicia › delitos › victor-m...
#RECUPERADOR','PLAZO_INICIAL_ADJ',
#               'MESES_INICIALES_NO_PAGO', 'PORCION_PAGO','ACCION_CONTACTO', 'MOTIVO', 
#               'PORTAFOLIO', 'PORCION_PAGOS']

# c=evolucion.loc[157291,'IDENTIFICACION']
# dfc=evolucion.loc[evolucion['IDENTIFICACION'] == c]






# ides = list(pagos['CEDULA'].drop_duplicates())
# c=ides[1]

# casi = asignacion[asignacion['IDENTIFICACION']==c]
# cev = evolucion_pagos[evolucion_pagos['IDENTIFICACION']==c]
# cpag = pagos[pagos['CEDULA']==c]
# cpag2 = pagos2[pagos2['CEDULA']==c]
