# marvel-task-py
Collecting tweets stats about marvel heroes.

# Running
Needs to be run in a Python3 virtual environment where `twitter` and `mysql-connector-python` packages are installed.

The script connects to MySQL running on localhost, authenticates as `marietamarvel:4516`, and connects to `marvelDB` database. All necessary tables will be recreated on each run.

Execute as:
```
(venv) $ python3 marvel.py
```
