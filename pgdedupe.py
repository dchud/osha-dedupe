#!/usr/bin/env python

"""
pgdedupe.py - dedupe a file of OSHA inspection records using the
dedupe library.  Based heavily on example in dedupe-examples repo
available from:

http://datamade.github.io/dedupe-examples/docs/pgsql_big_dedupe_example.html

Note: this script will generate the requisite tables in PostgreSQL
but does not do any further analysis.

A hand-waving attempt to pull out some hard-coded values gets one
step away from having specific field and table names in the raw
SQL, but it's not very pretty.
"""

import argparse
import logging
import os

import dedupe
import psycopg2
from psycopg2.extras import RealDictCursor


KEY_FIELD = 'activity_nr'
SOURCE_TABLE = 'records'
BULK_INSERT_SIZE = 100

FIELDS = [
    {'field': 'estab_name', 'type': 'String'},
    {'field': 'site_address', 'type': 'String'},
    {'field': 'site_city', 'type': 'ShortString', 'Has Missing': True},
    {'field': 'site_state', 'type': 'ShortString', 'Has Missing': True},
    {'field': 'site_zip', 'type': 'ShortString', 'Has Missing': True},
    {'field': 'owner_type', 'type': 'Categorical',
     'categories': ['A', 'B', 'C', 'D', '\"\"']},
    {'field': 'sic_code', 'type': 'ShortString', 'Has Missing': True},
    {'field': 'naics_code', 'type': 'ShortString', 'Has Missing': True},
    {'field': 'union_status', 'type': 'Categorical', 'Has Missing': True,
     'categories': ['Y', 'U', 'A', 'N', 'B', '\"\"']},
    {'field': 'nr_in_estab', 'type': 'Price', 'Has Missing': True},
    {'field': 'open_date', 'type': 'ShortString', 'Has Missing': True}
    ]

logger = logging.getLogger(__file__)


def candidates_gen(result_set):
    lset = set
    block_id = None
    records = []
    i = 0
    for row in result_set:
        if row['block_id'] != block_id:
            if records:
                yield records

            block_id = row['block_id']
            records = []
            i += 1

            if i % 10000 == 0:
                logging.debug('%s blocks' % i)

        smaller_ids = row['smaller_ids']
        if smaller_ids:
            smaller_ids = lset(smaller_ids.split(','))
        else:
            smaller_ids = lset([])

        records.append((row[KEY_FIELD], row, smaller_ids))

    if records:
        yield records


# @profile
def main(args):
    deduper = dedupe.Dedupe(FIELDS, num_cores=args.cores)

    with psycopg2.connect(database=args.dbname,
                          host='localhost',
                          cursor_factory=RealDictCursor) as con:
        with con.cursor() as c:
            # Generate a sample size
            c.execute('SELECT COUNT(*) AS count FROM %s' % SOURCE_TABLE)
            row = c.fetchone()
            count = row['count']
            sample_size = int(count * args.sample)

            # Create the sample (warning: very memory intensive)
            logger.debug('Generating sample of %s records' % sample_size)
            with con.cursor('deduper') as c_deduper:
                c_deduper.execute('SELECT * FROM %s' % SOURCE_TABLE)
                temp_d = dict((i, row) for i, row in enumerate(c_deduper))
                deduper.sample(temp_d, sample_size)
                del(temp_d)

            # Load training data (no problem if it doesn't exist yet)
            if os.path.exists(args.training):
                logger.debug('Loading training file from %s' % args.training)
                with open(args.training) as tf:
                    deduper.readTraining(tf)

            # Active learning time
            logger.debug('Starting active learning')
            dedupe.convenience.consoleLabel(deduper)

            logger.debug('Starting training')
            deduper.train(ppc=0.001, uncovered_dupes=5)

            logger.debug('Saving new training file to %s' % args.training)
            with open(args.training, 'w') as training_file:
                deduper.writeTraining(training_file)

            deduper.cleanupTraining()

            # Blocking
            logger.debug('Creating blocking_map table')
            c.execute("""
                DROP TABLE IF EXISTS blocking_map
                """)
            c.execute("""
                CREATE TABLE blocking_map
                (block_key VARCHAR(200), %s INTEGER)
                """ % KEY_FIELD)

            # Generate inverted index for each field
            for field in deduper.blocker.index_fields:
                logger.debug('Selecting distinct values for "%s"' % field)
                c_index = con.cursor('index')
                c_index.execute("""
                    SELECT DISTINCT %s FROM %s
                    """ % (field, SOURCE_TABLE))
                field_data = (row[field] for row in c_index)
                deduper.blocker.index(field_data, field)
                c_index.close()

            # Generating blocking map
            logger.debug('Generating blocking map')
            c_block = con.cursor('block')
            c_block.execute("""
                SELECT * FROM %s
                """ % SOURCE_TABLE)
            full_data = ((row[KEY_FIELD], row) for row in c_block)
            b_data = deduper.blocker(full_data)

            logger.debug('Inserting blocks into blocking_map')
            for block in b_data:
                c.execute("""
                    INSERT INTO blocking_map (block_key, activity_nr)
                    VALUES {0}
                    """.format(block))

            con.commit()

            logger.debug('Indexing blocks')
            c.execute("""
                CREATE INDEX blocking_map_key_idx ON blocking_map (block_key)
                """)
            c.execute("DROP TABLE IF EXISTS plural_key")
            c.execute("DROP TABLE IF EXISTS plural_block")
            c.execute("DROP TABLE IF EXISTS covered_blocks")
            c.execute("DROP TABLE IF EXISTS smaller_coverage")

            logging.debug('Calculating plural_key')
            c.execute("""
                CREATE TABLE plural_key
                (block_key VARCHAR(200),
                block_id SERIAL PRIMARY KEY)
                """)
            c.execute("""
                INSERT INTO plural_key (block_key)
                SELECT block_key FROM blocking_map
                GROUP BY block_key HAVING COUNT(*) > 1
                """)

            logging.debug('Indexing block_key')
            c.execute("""
                CREATE UNIQUE INDEX block_key_idx ON plural_key (block_key)
                """)

            logging.debug('Calculating plural_block')
            c.execute("""
                CREATE TABLE plural_block
                AS (SELECT block_id, %s
                FROM blocking_map INNER JOIN plural_key
                USING (block_key))
                """ % KEY_FIELD)

            logging.debug('Adding %s index' % KEY_FIELD)
            c.execute("""
                CREATE INDEX plural_block_%s_idx
                    ON plural_block (%s)
                """ % (KEY_FIELD, KEY_FIELD))
            c.execute("""
                CREATE UNIQUE INDEX plural_block_block_id_%s_uniq
                ON plural_block (block_id, %s)
                """ % (KEY_FIELD, KEY_FIELD))

            logging.debug('Creating covered_blocks')
            c.execute("""
                CREATE TABLE covered_blocks AS
                    (SELECT %s,
                            string_agg(CAST(block_id AS TEXT), ','
                            ORDER BY block_id) AS sorted_ids
                     FROM plural_block
                     GROUP BY %s)
                 """ % (KEY_FIELD, KEY_FIELD))

            logging.debug('Indexing covered_blocks')
            c.execute("""
                CREATE UNIQUE INDEX covered_blocks_%s_idx
                    ON covered_blocks (%s)
                """ % (KEY_FIELD, KEY_FIELD))
            logging.debug('Committing')

            logging.debug('Creating smaller_coverage')
            c.execute("""
                CREATE TABLE smaller_coverage AS
                    (SELECT %s, block_id,
                        TRIM(',' FROM split_part(sorted_ids,
                                                 CAST(block_id AS TEXT), 1))
                         AS smaller_ids
                     FROM plural_block
                     INNER JOIN covered_blocks
                     USING (%s))
                """ % (KEY_FIELD, KEY_FIELD))
            con.commit()

            logging.debug('Clustering...')
            c_cluster = con.cursor('cluster')
            c_cluster.execute("""
                SELECT *
                FROM smaller_coverage
                INNER JOIN %s
                    USING (%s)
                ORDER BY (block_id)
                """ % (SOURCE_TABLE, KEY_FIELD))
            clustered_dupes = deduper.matchBlocks(
                    candidates_gen(c_cluster), threshold=0.5)

            logging.debug('Creating entity_map table')
            c.execute("DROP TABLE IF EXISTS entity_map")
            c.execute("""
                CREATE TABLE entity_map (
                    %s INTEGER,
                    canon_id INTEGER,
                    cluster_score FLOAT,
                    PRIMARY KEY(%s)
                )""" % (KEY_FIELD, KEY_FIELD))

            logger.debug('Inserting entities into entity_map')
            for cluster, scores in clustered_dupes:
                cluster_id = cluster[0]
                for key_field, score in zip(cluster, scores):
                    c.execute("""
                        INSERT INTO entity_map
                            (%s, canon_id, cluster_score)
                        VALUES (%s, %s, %s)
                        """ % (KEY_FIELD, key_field, cluster_id, score))

            logging.debug('Indexing head_index')
            c_cluster.close()
            c.execute("CREATE INDEX head_index ON entity_map (canon_id)")
            con.commit()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--cores', dest='cores', default=1, type=int,
                        help='number of CPU cores to use')
    parser.add_argument('--dbname', dest='dbname', default='dedupe',
                        help='database name')
    parser.add_argument('-s', '--sample', default=0.10, type=float,
                        help='sample size (percentage, default 0.10)')
    parser.add_argument('-t', '--training', default='training.json',
                        help='name of training file')
    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true',
                        default=False, help='detailed out')
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    main(args)
