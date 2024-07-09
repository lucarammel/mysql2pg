# Migration MySQL -> PG

This tool is aimed to migrate MySQL to PG database system management. 

## Quick start ! 

- Clone the project and get into the project: 

```bash
git clone https://gitlab.adaje.oi.enedis.fr/be-prev/migration_sql_pg.git

cd migration_sql_pg
```

- Create virtual environment : 

```bash
python -m venv venv
pip install --proxy http://vip-users.proxy.edf.fr:3131 -r requirements.txt
```

- Edit the `config.exemple.yaml` file and rename it `config.yaml`

- Run main script : 

```bash
python main.py
```



