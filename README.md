# Mini SQL Engine

* A Mini SQL engine using python, which can parse and run a subset of SQL queries using command line interface.
* Supports Select, aggregate functions, where conditions, group by, order by, distinct, limit with appropriate error handling.
* Refer `files/metadata.txt` for table/column information of sample data.

### Run Instructions
* Install requirements using
```
pip install moz-sql-parser
pip install prettyprint
```

* Go to `src` directory and run:
```
bash run.sh "Your SQL Query"
```