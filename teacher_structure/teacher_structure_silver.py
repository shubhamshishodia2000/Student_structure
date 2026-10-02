#!/usr/bin/env python3
"""Normalize only the four requested tch_summary columns; reuse student dimensions."""
import argparse
from functools import reduce
from pyspark.sql import functions as F
from . import teacher_structure_bronze as bronze
from . import teacher_structure_runtime as shared

COLUMNS = (
    ('academic_year', 'VARCHAR(7)', False),
    ('udise_sch_code', 'VARCHAR(32)', False),
    ('male_tch', 'BIGINT', False),
    ('female_tch', 'BIGINT', False),
    ('transgen_tch', 'BIGINT', False),
)

def normalize(spark, year):
    raw = spark.read.parquet(str(bronze.bronze_year_root(year) / 'tch_summary'))
    names = {x.lower(): x for x in raw.columns}
    result = raw.select(
        F.lit(year).alias('academic_year'),
        F.trim(F.col(names['udise_sch_code']).cast('string')).alias('udise_sch_code'),
        *[F.col(names[n]).cast('long').alias(n) for n in ('male_tch','female_tch','transgen_tch')],
    ).cache()
    bad = F.col('udise_sch_code').isNull() | (F.length('udise_sch_code') == 0)
    for n in ('male_tch','female_tch','transgen_tch'):
        bad = bad | F.col(n).isNull() | (F.col(n) < 0)
    if result.filter(bad).limit(1).count():
        raise RuntimeError(f'{year}: missing/invalid teacher counts or school code; resolve source first')
    # Never sum duplicate school totals; even identical duplicates require review.
    if result.groupBy('academic_year','udise_sch_code').count().filter('count > 1').limit(1).count():
        raise RuntimeError(f'{year}: duplicate tch_summary school keys')
    return result

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--stage',choices=('all','normalize','fact'),default='all')
    args = parser.parse_args()
    if args.stage == 'fact':
        from . import teacher_structure_model as model
        model.build_fact()
        return
    bronze.build_manifest()
    spark = shared.make_spark('UDISE_Teacher_Structure_Silver')
    try:
        frames = [normalize(spark,y) for y in bronze.YEARS]
        result = reduce(lambda a,b: a.unionByName(b), frames)
        spec = shared.TableSpec('teacher_structure_snapshot', COLUMNS,
                                ('academic_year','udise_sch_code'),
                                ('academic_year','udise_sch_code'), 8)
        rows = shared.write_stage(result,spec)
        if rows <= 0:
            raise RuntimeError('No teacher rows')
        shared.publish(spec,rows)
    finally:
        spark.stop()
    if args.stage == 'all':
        from . import teacher_structure_model as model
        model.build_fact()

if __name__ == '__main__':
    main()
