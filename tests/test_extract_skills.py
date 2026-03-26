import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'dags'))

from extract_skills import extract_skills_from_text


class TestExtractSkillsFromText:
    def test_finds_python(self):
        result = extract_skills_from_text("We need a Python developer")
        assert "python" in result

    def test_finds_sql(self):
        result = extract_skills_from_text("Must have strong SQL skills")
        assert "sql" in result

    def test_finds_aws(self):
        result = extract_skills_from_text("Experience with AWS required")
        assert "aws" in result

    def test_finds_multiple_skills(self):
        result = extract_skills_from_text("Python, SQL, and Docker experience needed")
        assert "python" in result
        assert "sql" in result
        assert "docker" in result

    def test_case_insensitive(self):
        result = extract_skills_from_text("PYTHON and SQL experience")
        assert "python" in result
        assert "sql" in result

    def test_finds_plural(self):
        result = extract_skills_from_text("Build data pipelines using Airflow")
        assert "data pipeline" in result

    def test_no_false_positives(self):
        result = extract_skills_from_text("We are looking for a great communicator")
        assert len(result) == 0

    def test_handles_empty_string(self):
        result = extract_skills_from_text("")
        assert result == []

    def test_handles_none(self):
        result = extract_skills_from_text(None)
        assert result == []

    def test_finds_machine_learning(self):
        result = extract_skills_from_text("Experience in machine learning preferred")
        assert "machine learning" in result
