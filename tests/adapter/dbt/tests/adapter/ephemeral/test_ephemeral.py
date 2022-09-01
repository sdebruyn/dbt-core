import pytest
import re
import os
from pathlib import Path
from dbt.tests.util import run_dbt, check_relations_equal


models__dependent_sql = """

-- multiple ephemeral refs should share a cte
select * from {{ref('base')}} where gender = 'Male'
union all
select * from {{ref('base')}} where gender = 'Female'

"""

models__double_dependent_sql = """

-- base_copy just pulls from base. Make sure the listed
-- graph of CTEs all share the same dbt_cte__base cte
select * from {{ref('base')}} where gender = 'Male'
union all
select * from {{ref('base_copy')}} where gender = 'Female'

"""

models__super_dependent_sql = """
select * from {{ref('female_only')}}
union all
select * from {{ref('double_dependent')}} where gender = 'Male'

"""

models__base__female_only_sql = """
{{ config(materialized='ephemeral') }}

select * from {{ ref('base_copy') }} where gender = 'Female'

"""

models__base__base_sql = """
{{ config(materialized='ephemeral') }}

select * from {{ this.schema }}.seed

"""

models__base__base_copy_sql = """
{{ config(materialized='ephemeral') }}

select * from {{ ref('base') }}

"""

ephemeral_errors__dependent_sql = """
-- base copy is an error
select * from {{ref('base_copy')}} where gender = 'Male'

"""

ephemeral_errors__base__base_sql = """
{{ config(materialized='ephemeral') }}

select * from {{ this.schema }}.seed

"""

ephemeral_errors__base__base_copy_sql = """
{{ config(materialized='ephemeral') }}

{{ adapter.invalid_method() }}

select * from {{ ref('base') }}

"""

models_n__ephemeral_level_two_sql = """
{{
  config(
    materialized = "ephemeral",
  )
}}
select * from {{ ref('source_table') }}

"""

models_n__root_view_sql = """
select * from {{ref("ephemeral")}}

"""

models_n__ephemeral_sql = """
{{
  config(
    materialized = "ephemeral",
  )
}}
select * from {{ref("ephemeral_level_two")}}

"""

models_n__source_table_sql = """
{{ config(materialized='table') }}

with source_data as (

    select 1 as id
    union all
    select null as id

)

select *
from source_data

"""


@pytest.fixture(scope="class", autouse=True)
def setUp(project):
    project.run_sql_file(project.test_data_dir / Path("seed.sql"))


class TestEphemeralMulti:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "dependent.sql": models__dependent_sql,
            "double_dependent.sql": models__double_dependent_sql,
            "super_dependent.sql": models__super_dependent_sql,
            "base": {
                "female_only.sql": models__base__female_only_sql,
                "base.sql": models__base__base_sql,
                "base_copy.sql": models__base__base_copy_sql,
            },
        }

    def test_ephemeral_multi(self, project):
        results = run_dbt(["run"])
        assert len(results) == 3

        check_relations_equal(project.adapter, ["seed", "dependent"])
        check_relations_equal(project.adapter, ["seed", "double_dependent"])
        check_relations_equal(project.adapter, ["seed", "super_dependent"])
        assert os.path.exists("./target/run/test/models/double_dependent.sql")
        with open("./target/run/test/models/double_dependent.sql", "r") as fp:
            sql_file = fp.read()

        sql_file = re.sub(r"\d+", "", sql_file)
        expected_sql = (
            'create view "dbt"."test_test_ephemeral"."double_dependent__dbt_tmp" as ('
            "with __dbt__cte__base as ("
            "select * from test_test_ephemeral.seed"
            "),  __dbt__cte__base_copy as ("
            "select * from __dbt__cte__base"
            ")-- base_copy just pulls from base. Make sure the listed"
            "-- graph of CTEs all share the same dbt_cte__base cte"
            "select * from __dbt__cte__base where gender = 'Male'"
            "union all"
            "select * from __dbt__cte__base_copy where gender = 'Female'"
            ");"
        )
        sql_file = "".join(sql_file.split())
        expected_sql = "".join(expected_sql.split())
        assert sql_file == expected_sql


class TestEphemeralNested:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "ephemeral_level_two.sql": models_n__ephemeral_level_two_sql,
            "root_view.sql": models_n__root_view_sql,
            "ephemeral.sql": models_n__ephemeral_sql,
            "source_table.sql": models_n__source_table_sql,
        }

    def test_ephemeral_nested(self, project):
        results = run_dbt(["run"])
        assert len(results) == 2
        assert os.path.exists("./target/run/test/models/root_view.sql")
        with open("./target/run/test/models/root_view.sql", "r") as fp:
            sql_file = fp.read()

        sql_file = re.sub(r"\d+", "", sql_file)
        expected_sql = (
            'create view "dbt"."test_test_ephemeral"."root_view__dbt_tmp" as ('
            "with __dbt__cte__ephemeral_level_two as ("
            'select * from "dbt"."test_test_ephemeral"."source_table"'
            "),  __dbt__cte__ephemeral as ("
            "select * from __dbt__cte__ephemeral_level_two"
            ")select * from __dbt__cte__ephemeral"
            ");"
        )

        sql_file = "".join(sql_file.split())
        expected_sql = "".join(expected_sql.split())
        assert sql_file == expected_sql


class TestEphemeralErrorHandling:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "dependent.sql": ephemeral_errors__dependent_sql,
            "base": {
                "base.sql": ephemeral_errors__base__base_sql,
                "base_copy.sql": ephemeral_errors__base__base_copy_sql,
            },
        }

    def test_ephemeral_error_handling(self, project):
        results = run_dbt(["run"], expect_pass=False)
        assert len(results) == 1
        assert results[0].status == "skipped"
        assert "Compilation Error" in results[0].message
