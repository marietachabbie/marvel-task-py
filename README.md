# marvel-task-py
The script connects to Twitter's API and collects tweets from the last 7 days, which mention Avengers, their alter egos, as well as their actors. It also makes statistics of the most popular ones.

# Running
Needs to be run in a Python3 virtual environment where `twitter` and `mysql-connector-python` packages are installed.

The script connects to MySQL running on localhost, authenticates as `marietamarvel:4516`, and connects to `marvelDB` database. All necessary tables will be recreated on each run.

Execute as:
```
(venv) $ python3 marvel.py
```
