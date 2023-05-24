models__test_null_compare_sql = """
select
    null as actual,
    null as expected
"""


models__test_null_compare_yml = """
version: 2
models:
  - name: test_null_compare
    tests:
      - assert_equal:
          actual: actual
          expected: expected
"""
