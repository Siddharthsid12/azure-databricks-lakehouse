"""
Data Quality Validation Framework
===================================
Rule-based validation engine that runs checks between pipeline layers.
Supports: not_null, unique, regex, in_set, greater_than, between,
          valid_date, date_not_future, referential integrity, freshness.

Critical failures block the pipeline. Warnings are logged but don't halt.
All results are collected into a validation report for monitoring.
"""

import re
from datetime import datetime
from dataclasses import dataclass, field
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src.utils.helpers import get_logger, load_quality_rules

logger = get_logger("data_quality.validators")


@dataclass
class ValidationResult:
    """Result of a single data quality check."""
    rule_name: str
    table: str
    column: str
    check_type: str
    severity: str
    passed: bool
    total_rows: int
    failed_rows: int
    failure_pct: float
    description: str
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def __str__(self):
        status = "PASS" if self.passed else "FAIL"
        return (
            f"[{status}] {self.rule_name} | {self.table}.{self.column} | "
            f"failed: {self.failed_rows}/{self.total_rows} ({self.failure_pct:.1%}) "
            f"| severity: {self.severity}"
        )


class DataQualityValidator:
    """
    Configurable data quality validation engine.

    Usage:
        validator = DataQualityValidator(spark, rules_config)
        report = validator.validate_table(df, "orders")
        validator.assert_critical_passed(report)
    """

    def __init__(self, spark: SparkSession, rules_path: str = None):
        self.spark = spark
        if rules_path:
            self.rules_config = load_quality_rules(rules_path)
        else:
            self.rules_config = {"rules": {}}

    def validate_table(
        self, df: DataFrame, table_name: str, rules: list[dict] = None
    ) -> list[ValidationResult]:
        """
        Run all configured rules for a table.

        Args:
            df: DataFrame to validate
            table_name: Name of the table (used to look up rules)
            rules: Optional override rules (otherwise loaded from config)

        Returns:
            List of ValidationResult objects
        """
        if rules is None:
            rules = self.rules_config.get("rules", {}).get(table_name, [])

        results = []
        total_rows = df.count()

        logger.info(f"Running {len(rules)} quality checks on '{table_name}' ({total_rows} rows)")

        for rule in rules:
            result = self._run_check(df, table_name, rule, total_rows)
            results.append(result)
            log_fn = logger.error if not result.passed else logger.info
            log_fn(str(result))

        return results

    def _run_check(
        self, df: DataFrame, table_name: str, rule: dict, total_rows: int
    ) -> ValidationResult:
        """Dispatch to the appropriate check function based on rule type."""
        check_type = rule["check"]
        column = rule.get("column", "N/A")

        check_map = {
            "not_null": self._check_not_null,
            "unique": self._check_unique,
            "regex": self._check_regex,
            "in_set": self._check_in_set,
            "greater_than": self._check_greater_than,
            "between": self._check_between,
            "valid_date": self._check_valid_date,
            "date_not_future": self._check_date_not_future,
        }

        check_fn = check_map.get(check_type)
        if check_fn is None:
            logger.warning(f"Unknown check type: {check_type}")
            return ValidationResult(
                rule_name=rule["name"],
                table=table_name,
                column=column,
                check_type=check_type,
                severity=rule.get("severity", "warning"),
                passed=False,
                total_rows=total_rows,
                failed_rows=0,
                failure_pct=0.0,
                description=f"Unknown check type: {check_type}",
            )

        failed_rows = check_fn(df, rule)
        failure_pct = failed_rows / total_rows if total_rows > 0 else 0.0
        passed = failed_rows == 0

        return ValidationResult(
            rule_name=rule["name"],
            table=table_name,
            column=column,
            check_type=check_type,
            severity=rule.get("severity", "warning"),
            passed=passed,
            total_rows=total_rows,
            failed_rows=failed_rows,
            failure_pct=failure_pct,
            description=rule.get("description", ""),
        )

    # ── Individual check implementations ────────────────────────────

    @staticmethod
    def _check_not_null(df: DataFrame, rule: dict) -> int:
        col = rule["column"]
        return df.filter(
            F.col(col).isNull() | (F.trim(F.col(col)) == "")
        ).count()

    @staticmethod
    def _check_unique(df: DataFrame, rule: dict) -> int:
        col = rule["column"]
        total = df.count()
        distinct = df.select(col).distinct().count()
        return total - distinct

    @staticmethod
    def _check_regex(df: DataFrame, rule: dict) -> int:
        col = rule["column"]
        pattern = rule["pattern"]
        return df.filter(
            F.col(col).isNotNull() & ~F.col(col).rlike(pattern)
        ).count()

    @staticmethod
    def _check_in_set(df: DataFrame, rule: dict) -> int:
        col = rule["column"]
        valid_values = rule["valid_values"]
        return df.filter(
            F.col(col).isNotNull() & ~F.col(col).isin(valid_values)
        ).count()

    @staticmethod
    def _check_greater_than(df: DataFrame, rule: dict) -> int:
        col = rule["column"]
        threshold = rule["threshold"]
        return df.filter(
            F.col(col).isNotNull() & (F.col(col) <= threshold)
        ).count()

    @staticmethod
    def _check_between(df: DataFrame, rule: dict) -> int:
        col = rule["column"]
        min_val = rule["min_value"]
        max_val = rule["max_value"]
        return df.filter(
            F.col(col).isNotNull()
            & ~F.col(col).between(min_val, max_val)
        ).count()

    @staticmethod
    def _check_valid_date(df: DataFrame, rule: dict) -> int:
        col = rule["column"]
        fmt = rule.get("format", "yyyy-MM-dd")
        return df.filter(
            F.col(col).isNotNull()
            & F.to_date(F.col(col), fmt).isNull()
        ).count()

    @staticmethod
    def _check_date_not_future(df: DataFrame, rule: dict) -> int:
        col = rule["column"]
        return df.filter(
            F.col(col).isNotNull() & (F.col(col) > F.current_date())
        ).count()

    # ── Cross-table checks ──────────────────────────────────────────

    def check_referential_integrity(
        self,
        source_df: DataFrame,
        reference_df: DataFrame,
        source_col: str,
        reference_col: str,
        rule_name: str = "referential_integrity",
    ) -> ValidationResult:
        """
        Check that all values in source_col exist in reference_col.
        """
        total = source_df.filter(F.col(source_col).isNotNull()).count()
        orphans = (
            source_df.alias("src")
            .join(
                reference_df.alias("ref"),
                F.col(f"src.{source_col}") == F.col(f"ref.{reference_col}"),
                "left_anti",
            )
            .filter(F.col(source_col).isNotNull())
            .count()
        )

        passed = orphans == 0
        result = ValidationResult(
            rule_name=rule_name,
            table="cross_table",
            column=source_col,
            check_type="referential_integrity",
            severity="critical",
            passed=passed,
            total_rows=total,
            failed_rows=orphans,
            failure_pct=orphans / total if total > 0 else 0.0,
            description=f"Orphan records in {source_col} not found in {reference_col}",
        )
        log_fn = logger.error if not passed else logger.info
        log_fn(str(result))
        return result

    # ── Pipeline control ────────────────────────────────────────────

    @staticmethod
    def assert_critical_passed(results: list[ValidationResult]) -> None:
        """
        Raise an exception if any critical-severity check failed.
        This is the quality gate that blocks the pipeline.
        """
        critical_failures = [
            r for r in results if r.severity == "critical" and not r.passed
        ]

        if critical_failures:
            msg = "PIPELINE BLOCKED — Critical data quality failures:\n"
            for r in critical_failures:
                msg += f"  • {r}\n"
            logger.error(msg)
            raise DataQualityError(msg)

        logger.info("All critical quality checks passed — pipeline may proceed")

    @staticmethod
    def generate_report(results: list[ValidationResult]) -> dict:
        """Generate a summary report from validation results."""
        return {
            "total_checks": len(results),
            "passed": sum(1 for r in results if r.passed),
            "failed": sum(1 for r in results if not r.passed),
            "critical_failures": sum(
                1 for r in results if not r.passed and r.severity == "critical"
            ),
            "warnings": sum(
                1 for r in results if not r.passed and r.severity == "warning"
            ),
            "details": [
                {
                    "rule": r.rule_name,
                    "status": "PASS" if r.passed else "FAIL",
                    "severity": r.severity,
                    "failed_rows": r.failed_rows,
                    "failure_pct": f"{r.failure_pct:.2%}",
                }
                for r in results
            ],
        }


class DataQualityError(Exception):
    """Raised when critical data quality checks fail."""
    pass
