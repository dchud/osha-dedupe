# osha-dedupe

This is an experimental/learning project to explore using the Python
[dedupe](http://dedupe.readthedocs.org) library with PostgreSQL to
dedupe a large (>4M) number of records.  As such it is not directly
intended for general use.

The ```pgdedupe.py``` script borrows heavily from the
[pgsql_big_dedupe_example.py](http://datamade.github.io/dedupe-examples/docs/pgsql_big_dedupe_example.html)
example script with some modifications.  It has been debugged on a
sample set of OSHA inspection records, a fixture for which is
provided.  This data comes from the [OSHA Data
Enforcement](http://enforcedata.dol.gov/views/data_summary.php) set
from the U.S. Department of Labor.  It focuses on a subset of the
columns available in the ```osha_inspection``` file.


## installation and use

This script was developed on OS X 10.11.3 using Python 3.5 with the
sample data, then invoked on an EC2 Ubuntu 14.04 machine.  In both
environments, you might want to have Anaconda installed as well as
the gcc and g++ system packages to successfully install the dedupe
library without heavy lifting.

Following below is a transcript of steps taken to configure and 
execute ```pgdedupe.py``` on a clean EC2 instance.


### Prep the host
```
% sudo apt-get update && sudo apt-get upgrade
% sudo apt-get install gcc g++ postgresql git
```

#### Optional (suggested for remote terminals)
```
% sudo apt-get install screen
% screen
```

#### Optional (ec2 only) - make sure pgdata is on a healthy-sized partition
```
% sudo service postgresql stop
% sudo mv /var/lib/postgresql/9.3/main /mnt/pgdata
% sudo ln -s /mnt/pgdata /var/lib/postgresql/9.3/main
% sudo service postgresql restart
```

### Set up a db user with superuser rights
```
% sudo su - postgres
postgres% createuser -s ubuntu
postgres% exit
```

### Set up anaconda
```
% wget http://repo.continuum.io/archive/Anaconda3-2.5.0-Linux-x86_64.sh
% md5sum Anaconda3-2.5.0-Linux-x86_64.sh
% bash Anaconda3-2.5.0-Linux-x86_64.sh
  (respond to prompts)
% export PATH=/home/ubuntu/anaconda3/bin:$PATH
% conda update --all
  (respond to prompt)
```

### Grab this repo  
```
% git clone https://github.com/dchud/osha-dedupe.git
% cd osha-dedupe
```

### And the data...
```
% wget http://prd-enforce-xfr-02.dol.gov/data_catalog/OSHA/osha_inspection_20160322.csv.zip
```

FIXME: the db schema assumes fewer columns, the result of a
```csvcut``` operation.  Add those!


### Install python dependencies 
```
% conda install psycopg2
% pip install dedupe
```

### Prep the db
```
% createdb oshadedupe
% psql oshadedupe < create-table.sql
% psql oshadedupe -c "ALTER ROLE ubuntu WITH PASSWORD 'ubuntu'" 
% zcat osha_inspection.csv.gz | psql oshadedupe -c 'COPY records FROM STDIN CSV HEADER'
```

### There are a few records with bad data; remove those
```
% psql oshadedupe
oshadedupe=# DELETE FROM records WHERE CHAR_LENGTH(owner_type) > 1;
oshadedupe=# \q
``` 

### Config the db connection (FIXME: parameterize)
```
% vim pgdedupe.py
```

Edit to add "user='ubuntu', password='ubuntu'" to db connect at line 83.


### Get it started; time it, too

Note: ```--cores 30``` on a big EC2 instance type, change appropriately.

```
% time ./pgdedupe.py -v -s 0.10 --dbname oshadedupe -t training.json --cores 30
```

Provide some active learning responses.

(wait :)

Results will be in the ```entity_map``` table, where individual record
identifiers are mapped to target ```canon_id``` canonical identifiers.
