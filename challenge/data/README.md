# data/

Área de trabalho da DAG, montada no container do Airflow. O conteúdo **não é versionado**.

```
data/
└── landing/   # criada pela DAG: links com nomes normalizados (yellow_tripdata_YYYY-MM.parquet)
```

Os **arquivos originais não ficam aqui**: eles são os 12 arquivos do desafio em
`../nyc-tlc-data/` (raiz do repositório), que o `docker-compose.yml` monta em
`/opt/airflow/nyc-tlc-data`. Se algum mês estiver faltando, a primeira tarefa da DAG
(`prepare_landing`) baixa o arquivo do repositório original do desafio.

Apesar da extensão `.gz`, são arquivos Parquet comuns (a compressão é interna), então
**não** precisam ser descompactados.
