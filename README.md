# tslite
A Simple Time Series Database Implemented By Python.

Install
===============
```
 pip install tslite
```

Usage
===============

```python
# -*- coding: UTF-8 -*
'''
Created on 2020-02-19
'''
from __future__ import print_function
import random, time

import tslite

path = '/tmp/tsdb/test'  # the database path
db = tslite.Database(path)
tabname = 't1'
db.drop_table(tabname)  # clern table

# get table
tab = db.get_table(tabname)

# define the table struct
tab.define({
    'field_lock': True,  # lock the table struct
    'fields': [
        {'name': 'name', 'type': 'string', 'index': True},
        {'name': 'group', 'type': 'int', 'index': True},
        {'name': 'x', 'type': 'int', 'index': True},

        {'name': 'v', 'type': 'int'},
        {'name': 'y', 'type': 'float'},
        {'name': 'z', 'type': 'float'},
        {'name': 't', 'type': 'float'},
    ]
})

# count for testing
count = 10
tm = time.time()
for i in range(count):
    # write data
    tab.write_data({'x': i, 'time': tm + i, 'name': random.choice(['a', 'aa', 'bb']), 'y': random.random()})

# query data
q = tab.query()
for r in q:
    print(r)

print('name is "a":')
# query field 'name' == "a"
q = tab.query(eqs={'name': 'a'})
for r in q:
    print(r)

```


[Click to view more information!](https://github.com/sintrb/tslite)