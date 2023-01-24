# MSSQL Data Loader

## Wikipedia

All data dumps of Wikipedia can be downloaded from [here](https://dumps.wikimedia.org/backup-index.html).

### Wikipedia page data

Required files

  * <lang>wiki-<yyyymmdd>-pages-articles-multistream.xml.bz2 
  * <lang>wiki-<yyyymmdd>-pages-articles-multistream-index.txt.bz2 

```
./mssql-dataloader wikipedia \
  --dataset ./dataset/jawiki-20230101-pages-articles-multistream.xml.bz2 \
  --index ./dataset/jawiki-20230101-pages-articles-multistream-index.txt.bz2 \
  -s sqlserver.database.windows.net \
  -d sqldb \
  -u user \
  -p 'password'
```

### Wikipedia redirect data

Required files

  * <lang>wiki-<yyyymmdd>-redirect.sql.gz

```
./mssql-dataloader wikipedia redirect \
  --dataset ./dataset/jawiki-20230101-redirect.sql.gz \
  -s sqlserver.database.windows.net \
  -d sqldb \
  -u user \
  -p 'password'
```

