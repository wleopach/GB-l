
# Colors

```mermaid
 %%{init: {'theme':'base'}}%%
  
  graph TB;
  %% Colors %%
  classDef str fill:#800000,stroke:#000,color:#000
  classDef int fill:#ADD8E6,stroke:#000,color:#000
  classDef dict fill:#FFA500,stroke:#000,color:#000
  classDef list fill:#00008B,stroke:#000,color:#000
  classDef float fill:#008000,stroke:#000,color:#000
  classDef df fill:#C0C0C0,stroke:#000,color:#000
  classDef date fill:#FFFF00,stroke:#000,color:#000
  %%depth0
  dict:::dict
  str:::str
  int:::int
  float:::float
  list:::list
  date:::date
  DataFrame:::df
```

#  FILE: Dicc_Datos_Propia_Pagos.pickle 14260681 MB

```mermaid
  %%{init: {'theme':'base'}}%%
  
  graph LR;
  %% Colors %%
  classDef str fill:#800000,stroke:#000,color:#000
  classDef int fill:#ADD8E6,stroke:#000,color:#000
  classDef dict fill:#FFA500,stroke:#000,color:#000
  classDef list fill:#00008B,stroke:#000,color:#000
  classDef float fill:#008000,stroke:#000,color:#000
  classDef df fill:#C0C0C0,stroke:#000,color:#000
  classDef date fill:#FFFF00,stroke:#000,color:#000
  %%depth0
      CC:::dict-->IDENTIFICACION:::str;
      CC-->CANTIDAD_OBLIGACIONES:::int;
      CC-->NOMBRE:::str;
      CC-->COD_OBLIGACION:::dict;
      COD_OBLIGACION-->CARTERA:::str;
      COD_OBLIGACION-->HISTORIA_PAGOS:::int;
      COD_OBLIGACION-->FECHAS_DE_PAGO:::list;
      COD_OBLIGACION-->PAGOS:::list;
      COD_OBLIGACION-->PAGO_ESPERADO:::float;
      COD_OBLIGACION-->FRECUENCIA_PAGO:::float;
      COD_OBLIGACION-->CUOTA_ESPERADA:::float;
      COD_OBLIGACION-->FECHAS_ESPERADAS_PAGOS:::list;
      COD_OBLIGACION-->SERIE_PAGOS:::df;
      SERIE_PAGOS-->FECHA_DE_PAGO:::date
      SERIE_PAGOS-->VALOR_PAGO:::str
      SERIE_PAGOS-->CUOTA_ESPERADA2:::str
      SERIE_PAGOS-->NO_OBLIGACION_BETA:::str
      SERIE_PAGOS-->NO_OBLIGACION_ACTUAL:::str
      SERIE_PAGOS-->Guia_Control:::str
```


# FILE : Dicc_Datos_Propia_Evolucion.pickle 481264542 MB

```mermaid
  %%{init: {'theme':'base'}}%%
  
  graph LR;
  %% Colors %%
  classDef str fill:#800000,stroke:#000,color:#000
  classDef int fill:#ADD8E6,stroke:#000,color:#000
  classDef dict fill:#FFA500,stroke:#000,color:#000
  classDef list fill:#00008B,stroke:#000,color:#000
  classDef float fill:#008000,stroke:#000,color:#000
  classDef df fill:#C0C0C0,stroke:#000,color:#000
  classDef date fill:#FFFF00,stroke:#000,color:#000
  %%depth0
    CC:::dict-->IDENTIFICACION:::str;
    CC-->CANTIDAD_OBLIGACIONES:::int;
    CC-->NOMBRE:::str;
    CC-->COD_OBLIGACION:::dict;
    COD_OBLIGACION-->DESCRIPCION_PRODUCTO:::str
    COD_OBLIGACION-->PORTAFOLIO:::str
    COD_OBLIGACION-->CP:::str
    COD_OBLIGACION-->COMPRA_DE_CARTERA:::str
    COD_OBLIGACION-->CANAL:::str
    COD_OBLIGACION-->FECHA_APERTURA:::date
    COD_OBLIGACION-->FECHA_CASTIGO:::date
    COD_OBLIGACION-->HISTORA_PAGOS:::int
    COD_OBLIGACION-->REGISTRO_PAGOS:::str
    COD_OBLIGACION-->TOTAL_PAGOS:::int
    COD_OBLIGACION-->DATOS:::df
    DATOS-->ORDEN_DE_GESTION:::str
    DATOS-->ASESOR:::str
    DATOS-->TASA_INTERES_CTE:::str
    DATOS-->SALDO_CAPITAL_CLIENTE:::float
    DATOS-->SALDO_CAPITAL_MES:::float
    DATOS-->SALDO_TOTAL:::float
    DATOS-->ESTADO:::str
    DATOS-->INTERES_ACTUAL_CAUSADO:::str
    DATOS-->PAGOS_AYER:::str
    DATOS-->SALDO_CAPITAL_ACTUAL:::float
    DATOS-->SALDO_TOTAL_ACTUAL:::float
    DATOS-->DIAS_DE_MORA_ACTUAL:::float
    DATOS-->ACCION:::str
    DATOS-->TIPO_CONTACTO:::str
    DATOS-->TIPO_DE_ACUERDO:::str
    DATOS-->MOTIVO_DE_NO_PAGO:::str
    DATOS-->MEJOR_GESTION_CLIENTE:::str
    DATOS-->META1[META_$]:::float
    DATOS-->META2[META_%]:::float
    DATOS-->FECHA_ULTIMA_GESTION:::date
    DATOS-->Guia_Control:::str
    DATOS-->PLAZO_INICIAL:::float
    DATOS-->NOMBRE_RECUPERADOR:::str
```

# FILE : Dicc_Datos_Propia_Asignacion.pickle 481264542 MB

```mermaid
  %%{init: {'theme':'base'}}%%
  
  graph LR;
  %% Colors %%
  classDef str fill:#800000,stroke:#000,color:#000
  classDef int fill:#ADD8E6,stroke:#000,color:#000
  classDef dict fill:#FFA500,stroke:#000,color:#000
  classDef list fill:#00008B,stroke:#000,color:#000
  classDef float fill:#008000,stroke:#000,color:#000
  classDef df fill:#C0C0C0,stroke:#000,color:#000
  classDef date fill:#FFFF00,stroke:#000,color:#000
  %%depth0
    CC:::dict-->IDENTIFICACION:::str;
    CC-->MESES_ASIGNACIONES:::int;
    CC-->MESES_SIN_ASIGNACIONES:::int;
    CC-->LISTA_MESES_ASIGNACIONES:::list;
    CC-->LISTA_MESES_SIN_ASIGNACIONES:::list;
    CC-->CANTIDAD_OBLIGACIONES:::int;
    CC-->NOMBRE:::str;
    CC-->LISTA_OBLIGACIONES_BETA:::list;
    CC-->LISTA_OBLIGACIONES_BANCO:::list;
    CC-->CANTIDAD_OBLIGACIONES_BETA:::int;
    CC-->CANTIDAD_OBLIGACIONES_BANCO:::int;
    CC-->OBLIGACIONES:::list;
    CC-->COD_OBLIGACION:::dict;
    COD_OBLIGACION-->ACPK:::str;
    COD_OBLIGACION-->FECHA_INICIAL:::date;
    COD_OBLIGACION-->FECHA_FINAL:::date;
    COD_OBLIGACION-->SALDO_TOTAL:::float;
    COD_OBLIGACION-->CA["CUPO_APROBADO|VALOR_CONGELADO"]:::float;
    COD_OBLIGACION-->HISTORIA_MESES:::int;
    COD_OBLIGACION-->FECHA_CASTIGO:::date;
    COD_OBLIGACION-->OBLIGACION:::str;
    COD_OBLIGACION-->SALDO_CAPITAL_MES:::float;
    COD_OBLIGACION-->PLAZO_INICIAL:::float;
    COD_OBLIGACION-->NRO_REESTRUCTUR:::float;
    COD_OBLIGACION-->V/R_CUOTA:::str;
    
   
```

# FILE : Diciccionario_Retornar.pickle 14160 MB

```mermaid
  %%{init: {'theme':'base'}}%%
  
  graph LR;
  %% Colors %%
  classDef str fill:#800000,stroke:#000,color:#000
  classDef int fill:#ADD8E6,stroke:#000,color:#000
  classDef dict fill:#FFA500,stroke:#000,color:#000
  classDef list fill:#00008B,stroke:#000,color:#000
  classDef float fill:#008000,stroke:#000,color:#000
  classDef df fill:#C0C0C0,stroke:#000,color:#000
  classDef date fill:#FFFF00,stroke:#000,color:#000
  %%depth0
    CC:::dict-->PATH/TO/FILE:::dict;
    PATH/TO/FILE-->CANTIDAD_OBLIGACIONES:::int;
    PATH/TO/FILE-->OBLIGACIONES:::list;
    PATH/TO/FILE-->NOMBRE:::str;
    PATH/TO/FILE-->COD_OBLIGACION:::dict;
    COD_OBLIGACION-->TASA_INTERES_CTE:::str
    COD_OBLIGACION-->FECHA_CASTIGO:::date
    COD_OBLIGACION-->SALDO_CAPITAL_VENDIDO:::float
    COD_OBLIGACION-->DESCRIPCION_CONVENIO_CLIENTE:::str
    COD_OBLIGACION-->SALDO_TOTAL:::float
    COD_OBLIGACION-->SALDO_CAPITAL_CLIENTE:::float
    COD_OBLIGACION-->CUPO_APROBADO:::float
    COD_OBLIGACION-->DESCRIPCION_PRODUCTO:::str
    COD_OBLIGACION-->FECHA_APERTURA:::date
    COD_OBLIGACION-->DIAS_MORA_ACTUAL:::float
    COD_OBLIGACION-->CUOTA:::float

```