import pytest

from dbt.tests.adapter.utils.base_utils import BaseUtils
from dbt.tests.adapter.utils.fixture_null_compare import (
    models__test_null_compare_sql,
    models__test_null_compare_yml,
)


class BaseNullCompare(BaseUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_null_compare.yml": models__test_null_compare_yml,
            "test_null_compare.sql": models__test_null_compare_sql,
        }


class TestNullCompare(BaseNullCompare):
    pass
