"""
Unit Tests — Data Quality Validators
======================================
Tests for the validation rules engine.

Run: pytest tests/test_data_quality.py -v
"""

import pytest
from pyspark.sql import functions as F

from src.data_quality.validators import DataQualityValidator, DataQualityError

# spark fixture provided by conftest.py


@pytest.fixture
def validator(spark):
    return DataQualityValidator(spark)


class TestNotNullCheck:
    def test_passes_when_no_nulls(self, spark, validator):
        df = spark.createDataFrame([("A",), ("B",)], ["col1"])
        rule = {"name": "test", "column": "col1", "check": "not_null", "severity": "critical"}
        results = validator.validate_table(df, "test", [rule])
        assert results[0].passed is True

    def test_fails_when_nulls_present(self, spark, validator):
        df = spark.createDataFrame([("A",), (None,)], ["col1"])
        rule = {"name": "test", "column": "col1", "check": "not_null", "severity": "critical"}
        results = validator.validate_table(df, "test", [rule])
        assert results[0].passed is False
        assert results[0].failed_rows == 1

    def test_fails_on_empty_strings(self, spark, validator):
        df = spark.createDataFrame([("A",), ("",), ("  ",)], ["col1"])
        rule = {"name": "test", "column": "col1", "check": "not_null", "severity": "critical"}
        results = validator.validate_table(df, "test", [rule])
        assert results[0].failed_rows == 2  # empty and whitespace-only


class TestUniqueCheck:
    def test_passes_when_unique(self, spark, validator):
        df = spark.createDataFrame([("A",), ("B",), ("C",)], ["col1"])
        rule = {"name": "test", "column": "col1", "check": "unique", "severity": "critical"}
        results = validator.validate_table(df, "test", [rule])
        assert results[0].passed is True

    def test_fails_with_duplicates(self, spark, validator):
        df = spark.createDataFrame([("A",), ("B",), ("A",)], ["col1"])
        rule = {"name": "test", "column": "col1", "check": "unique", "severity": "critical"}
        results = validator.validate_table(df, "test", [rule])
        assert results[0].passed is False
        assert results[0].failed_rows == 1  # 3 total - 2 distinct = 1


class TestInSetCheck:
    def test_passes_with_valid_values(self, spark, validator):
        df = spark.createDataFrame([("A",), ("B",)], ["col1"])
        rule = {"name": "test", "column": "col1", "check": "in_set",
                "valid_values": ["A", "B", "C"], "severity": "warning"}
        results = validator.validate_table(df, "test", [rule])
        assert results[0].passed is True

    def test_fails_with_invalid_values(self, spark, validator):
        df = spark.createDataFrame([("A",), ("X",), ("B",)], ["col1"])
        rule = {"name": "test", "column": "col1", "check": "in_set",
                "valid_values": ["A", "B"], "severity": "warning"}
        results = validator.validate_table(df, "test", [rule])
        assert results[0].failed_rows == 1


class TestBetweenCheck:
    def test_passes_in_range(self, spark, validator):
        df = spark.createDataFrame([(0.1,), (0.5,), (0.9,)], ["col1"])
        rule = {"name": "test", "column": "col1", "check": "between",
                "min_value": 0.0, "max_value": 1.0, "severity": "critical"}
        results = validator.validate_table(df, "test", [rule])
        assert results[0].passed is True

    def test_fails_out_of_range(self, spark, validator):
        df = spark.createDataFrame([(0.1,), (1.5,), (-0.1,)], ["col1"])
        rule = {"name": "test", "column": "col1", "check": "between",
                "min_value": 0.0, "max_value": 1.0, "severity": "critical"}
        results = validator.validate_table(df, "test", [rule])
        assert results[0].failed_rows == 2


class TestReferentialIntegrity:
    def test_passes_when_all_keys_exist(self, spark, validator):
        source = spark.createDataFrame([("C001",), ("C002",)], ["customer_id"])
        reference = spark.createDataFrame([("C001",), ("C002",), ("C003",)], ["customer_id"])

        result = validator.check_referential_integrity(
            source, reference, "customer_id", "customer_id"
        )
        assert result.passed is True

    def test_fails_with_orphan_keys(self, spark, validator):
        source = spark.createDataFrame([("C001",), ("C999",)], ["customer_id"])
        reference = spark.createDataFrame([("C001",), ("C002",)], ["customer_id"])

        result = validator.check_referential_integrity(
            source, reference, "customer_id", "customer_id"
        )
        assert result.passed is False
        assert result.failed_rows == 1


class TestPipelineGate:
    def test_passes_when_all_critical_pass(self, spark, validator):
        df = spark.createDataFrame([("A",), ("B",)], ["col1"])
        rules = [
            {"name": "test1", "column": "col1", "check": "not_null", "severity": "critical"},
            {"name": "test2", "column": "col1", "check": "unique", "severity": "critical"},
        ]
        results = validator.validate_table(df, "test", rules)
        # Should not raise
        validator.assert_critical_passed(results)

    def test_blocks_on_critical_failure(self, spark, validator):
        df = spark.createDataFrame([(None,), ("B",)], ["col1"])
        rules = [
            {"name": "test1", "column": "col1", "check": "not_null", "severity": "critical"},
        ]
        results = validator.validate_table(df, "test", rules)

        with pytest.raises(DataQualityError):
            validator.assert_critical_passed(results)

    def test_allows_warnings_to_pass(self, spark, validator):
        df = spark.createDataFrame([("X",)], ["col1"])
        rules = [
            {"name": "test1", "column": "col1", "check": "in_set",
             "valid_values": ["A", "B"], "severity": "warning"},
        ]
        results = validator.validate_table(df, "test", rules)
        # Warnings should NOT block the pipeline
        validator.assert_critical_passed(results)


class TestReport:
    def test_report_structure(self, spark, validator):
        df = spark.createDataFrame([("A",), (None,)], ["col1"])
        rules = [
            {"name": "null_check", "column": "col1", "check": "not_null", "severity": "critical"},
        ]
        results = validator.validate_table(df, "test", rules)
        report = validator.generate_report(results)

        assert report["total_checks"] == 1
        assert report["failed"] == 1
        assert report["critical_failures"] == 1
        assert len(report["details"]) == 1
        assert report["details"][0]["rule"] == "null_check"
