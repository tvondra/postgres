#!/usr/bin/env python3

import sys
import random
import time

import os
import psycopg2
import re
import subprocess

PGUSER = os.environ['USER']

def generate_tables(f, s, conn, ntables):
    tables = {}

    for t in range(0, ntables):
        cols = [f'c_{t}_{c}' for c in range(0, 32)]
        tables.update({f't_{t}' : cols})

    f.write(f'---------- {s} ----------\n')

    create_tables(f, conn, tables)
    generate_data(f, conn, tables)
    create_indexes(f, conn, tables)
    vacuum_analyze(f, conn)

    return tables


def create_tables(f, conn, tables):

    cur = conn.cursor()

    seed = random.random()

    cur.execute(f'select setseed({seed})')
    f.write(f'select setseed({seed});\n')

    for t in tables.keys():
        tmp = tables[t]
        cols = ', '.join([f'{c} int' for c in tmp])

        cur.execute(f'DROP TABLE IF EXISTS {t}')
        f.write(f'DROP TABLE IF EXISTS {t};\n')

        cur.execute(f'CREATE TABLE {t} ({cols})')
        f.write(f'CREATE TABLE {t} ({cols});\n')

    conn.commit()


def generate_data(f, conn, tables):

    cur = conn.cursor()

    for t in tables.keys():
        tmp = tables[t]

        # 100 - 1M rows, skew towards smaller values
        rows = 100 + pow(random.random(), 4.0) * 1000000
        rands = [random.randint(100, 1000000) for c in tmp]
        cols = ', '.join([f'mod(i,{r})' for r in rands])

        cur.execute(f'INSERT INTO {t} SELECT {cols} FROM generate_series(1, {rows}) s(i)')
        f.write(f'INSERT INTO {t} SELECT {cols} FROM generate_series(1, {rows}) s(i);\n')


def create_indexes(f, conn, tables):

    cur = conn.cursor()

    for t in tables.keys():
        tmp = tables[t]

        # indexes on random 1/2 of columns
        indexes = random.sample(tmp, int(len(tmp) / 2))
        for i in indexes:
            cur.execute(f'CREATE INDEX ON {t} ({i})')
            f.write(f'CREATE INDEX ON {t} ({i});\n')


def vacuum_analyze(f, conn):

    cur = conn.cursor()

    cur.execute('VACUUM ANALYZE')
    f.write('VACUUM ANALYZE;\n\n')


def generate_tree(tables):
    # how many relations to join
    k = random.randint(1, len(tables))
    l = []

    for i in range(0, k):
        l.append([tables[i]])

    for i in range(k, len(tables)):
        idx = random.randint(0, k-1)
        l[idx].append(tables[i])

    for i in range(0, k):
        if len(l[i]) > 2:
            l[i] = generate_tree(l[i])

    return l

def available_columns(tables, tree):

    if type(tree) == str:
        return tables[tree]

    cols = []
    for t in tree:
        cols.extend(available_columns(tables, t))

    return cols


def generate_join(tables, tree, level = 0, max_clauses = None):

    # pick complexity of the join - number of join clauses to other relations
    if max_clauses is None:
        max_clauses = random.randint(1, 3)

    if type(tree) == str:
        return tree

    if len(tree) == 1:
        return generate_join(tables, tree[0], level, max_clauses)

    sql = ''
    prev = []

    for t in range(0, len(tree)):
        r = generate_join(tables, tree[t], level + 1)
        c = available_columns(tables, tree[t])

        if t > 0:

            # pick number of clauses for this join
            nclauses = random.randint(1, max_clauses)

            clauses = []
            for i in range(0, nclauses):
                clauses.append('(' + random.choice(prev) + ' = ' + random.choice(c) + ')')

            sql += '\n' + (' ' * 2 * (level + 1)) + random.choice(['JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'FULL JOIN']) + ' ' + r + ' ON (' + ' AND '.join(clauses) + ')'
        else:
            sql = (' ' * 2 * (level + 1)) + r

        prev.extend(c)

    if (level > 0):
        sql = '(\n' + sql + '\n' + (' ' * 2 * level) + ')'

    return sql


def extract_cost(res):
    r = re.search(r'cost=[^\s]+\.\.([^\s]+) ', res[0][0])
    return r.group(1)


def plan_hyper(sql, seeds):

    conn = psycopg2.connect(f'host=localhost user={PGUSER} dbname=test')
    cur = conn.cursor()

    cur.execute("reset all")
    cur.execute("set join_collapse_limit = 100")
    cur.execute("set from_collapse_limit = 100")
    cur.execute("set geqo = off")

    cur.execute("LOAD 'lindp'")
    cur.execute('set lindp.enabled = on')
    cur.execute('set lindp.fallback = off')
    cur.execute('set lindp.min_relations = 2')
    cur.execute('set lindp.max_relations = 100')
    cur.execute(f'set lindp.seeds = {seeds}')

    s = time.time()
    cur.execute(f'explain {sql}')
    e = time.time()

    duration = (e - s) * 1000

    res = cur.fetchall()

    cost = extract_cost(res)

    return ('\n'.join([r[0] for r in res]), duration, cost)


def plan_geqo(sql):

    conn = psycopg2.connect(f'host=localhost user={PGUSER} dbname=test')
    cur = conn.cursor()

    cur.execute("reset all")
    cur.execute("set join_collapse_limit = 100")
    cur.execute("set from_collapse_limit = 100")
    cur.execute("set geqo_threshold = 2")

    s = time.time()
    cur.execute(f'explain {sql}')
    e = time.time()

    duration = (e - s) * 1000

    res = cur.fetchall()

    cost = extract_cost(res)

    return ('\n'.join([r[0] for r in res]), duration, cost)


def plan_standard(sql):

    conn = psycopg2.connect(f'host=localhost user={PGUSER} dbname=test')
    cur = conn.cursor()

    cur.execute("reset all")
    cur.execute("set join_collapse_limit = 100")
    cur.execute("set from_collapse_limit = 100")
    cur.execute("set geqo = off")

    s = time.time()
    cur.execute(f'explain {sql}')
    e = time.time()

    duration = (e - s) * 1000

    res = cur.fetchall()

    cost = extract_cost(res)

    return ('\n'.join([r[0] for r in res]), duration, cost)


def get_hardness():
    result = subprocess.run(['./get-hardness.sh'], stdout=subprocess.PIPE)
    return result.stdout.decode('utf-8').strip()


def plan_hardness(sql):

    conn = psycopg2.connect(f'host=localhost user={PGUSER} dbname=test')
    cur = conn.cursor()

    cur.execute("reset all")
    cur.execute("set join_collapse_limit = 100")
    cur.execute("set from_collapse_limit = 100")
    cur.execute("set geqo = off")

    cur.execute("LOAD 'join_hardness'")
    cur.execute('set join_hardness.enable = on')

    # set a limit, so that we abort explain if the join is too hard
    # we'll still get the estimate, though (the join_hardness needs a tweak
    # to make that happen, though)
    cur.execute('set join_hardness.threshold = 1000000')

    try:
        cur.execute(f'explain {sql}')
    except:
        # ignore elog(ERROR) if join too hard
        pass

    return get_hardness()


if __name__ == '__main__':

    ts = int(time.time())

    schema_file = open(f'schema-{ts}.log', 'w')
    query_file = open(f'query-{ts}.log', 'w')
    plans_file = open(f'plans-{ts}.log', 'w')
    results_file = open(f'results-{ts}.csv', 'w')

    conn = psycopg2.connect(f'host=localhost user={PGUSER} dbname=test')
    conn.autocommit = True

    ntables_from = int(sys.argv[1])
    ntables_to = int(sys.argv[2])

    # number of data sets to test (per table count)
    nruns = 10
    if len(sys.argv) > 3:
        nruns = int(sys.argv[3])

    # number of queries to test (per dataset)
    nqueries = 1000
    if len(sys.argv) > 4:
        nqueries = int(sys.argv[4])

    sid = 0
    qid = 0
    s = time.time()

    # repeated runs
    for r in range(1, nruns + 1):

        t = round(time.time() - s, 2)
        print(f'{t}\t\trun {r}')

        # test all join sizes between 2 and ntables (inclusive)
        for n in range(ntables_from, ntables_to + 1):

            sid += 1

            t = round(time.time() - s, 2)
            print(f'{t}\t\t\t{n} tables (schema {sid})')

            tables = generate_tables(schema_file, sid, conn, n)

            # 1000 random joins for each join size / schema / data
            for q in range(0, nqueries):

                qid += 1

                t = round(time.time() - s, 2)
                print(f'{t}\t\t\t\tquery {qid}')

                tree = generate_tree(list(tables.keys()))
                join = 'SELECT * FROM ' + generate_join(tables, tree)

                query_file.write(f'---------- {qid} ----------\n')
                query_file.write(f'{join};\n')

                hyper = plan_hyper(join, 1)
                plans_file.write(f'------------- {qid} hyper 1 -------------\n')
                plans_file.write(hyper[0])
                plans_file.write('\n\n')

                hyper2 = plan_hyper(join, n)
                plans_file.write(f'------------- {qid} hyper {n} -------------\n')
                plans_file.write(hyper2[0])
                plans_file.write('\n\n')

                geqo = plan_geqo(join)
                plans_file.write(f'------------- {qid} geqo -------------\n')
                plans_file.write(geqo[0])
                plans_file.write('\n\n')

                # don't do standard_join_search for (n > 14), it may not complete
                # set based on experiments
                if n <= 14:
                    standard = plan_standard(join)
                    plans_file.write(f'------------- {qid} standard -------------\n')
                    plans_file.write(standard[0])
                    plans_file.write('\n\n')
                else:
                    standard = (-1, -1, -1)

                hardness = plan_hardness(join)

                results_file.write(f'{sid} {qid} {n} {hyper[1]} {hyper[2]} {hyper2[1]} {hyper2[2]} {geqo[1]} {geqo[2]} {standard[1]} {standard[2]} {hardness}\n')
                results_file.flush()
