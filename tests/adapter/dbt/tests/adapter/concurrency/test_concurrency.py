import pytest
from pathlib import Path
from dbt.tests.util import (
    run_dbt,
    check_relations_equal,
    check_table_does_not_exist,
    run_dbt_and_capture,
)


models__invalid_sql = """
{{
  config(
    materialized = "table"
  )
}}

select a_field_that_does_not_exist from {{ this.schema }}.seed

"""

models__table_a_sql = """
{{
  config(
    materialized = "table"
  )
}}

select * from {{ this.schema }}.seed

"""

models__table_b_sql = """
{{
  config(
    materialized = "table"
  )
}}

select * from {{ this.schema }}.seed

"""

models__view_model_sql = """
{{
  config(
    materialized = "view"
  )
}}

select * from {{ this.schema }}.seed

"""

models__dep_sql = """
{{
  config(
    materialized = "table"
  )
}}

select * from {{ref('view_model')}}

"""

models__view_with_conflicting_cascade_sql = """
select * from {{ref('table_a')}}

union all

select * from {{ref('table_b')}}

"""

models__skip_sql = """
select * from {{ref('invalid')}}

"""


class BaseConcurrency:
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        project.run_sql_file(project.test_data_dir / Path("seed.sql"))

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "invalid.sql": models__invalid_sql,
            "table_a.sql": models__table_a_sql,
            "table_b.sql": models__table_b_sql,
            "view_model.sql": models__view_model_sql,
            "dep.sql": models__dep_sql,
            "view_with_conflicting_cascade.sql": models__view_with_conflicting_cascade_sql,
            "skip.sql": models__skip_sql,
        }


class TestConcurenncy(BaseConcurrency):
    def test_concurrency(self, project):
        results = run_dbt(["run"], expect_pass=False)
        assert len(results) == 7

        check_relations_equal(project.adapter, ["seed", "view_model"])
        check_relations_equal(project.adapter, ["seed", "dep"])
        check_relations_equal(project.adapter, ["seed", "table_a"])
        check_relations_equal(project.adapter, ["seed", "table_b"])
        check_table_does_not_exist(project.adapter, "invalid")
        check_table_does_not_exist(project.adapter, "skip")

        project.run_sql_file(project.test_data_dir / Path("update.sql"))

        results, output = run_dbt_and_capture(["run"], expect_pass=False)
        assert len(results) == 7

        check_relations_equal(project.adapter, ["seed", "view_model"])
        check_relations_equal(project.adapter, ["seed", "dep"])
        check_relations_equal(project.adapter, ["seed", "table_a"])
        check_relations_equal(project.adapter, ["seed", "table_b"])
        check_table_does_not_exist(project.adapter, "invalid")
        check_table_does_not_exist(project.adapter, "skip")

        assert "PASS=5 WARN=0 ERROR=1 SKIP=1 TOTAL=7" in output
