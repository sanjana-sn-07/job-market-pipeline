"""Offline tests for the all-sources LLM extraction replacement.

Stubs avoid live PostgreSQL, AWS, and OpenAI calls; add integration tests with a
real test Postgres and mocked API before production deployment.
"""
import importlib.util
import sys
import types
from pathlib import Path

import pytest

MODULE_FILE = Path(__file__).resolve().parents[1] / "dags" / "extract_skills_llm.py"


class FakeStore:
    def __init__(self, rows):
        self.rows = rows  # config host -> [(job_id, description, source), ...]
        self.saved = {host: set() for host in rows}
        self.queries = []
        self.fail_write_on = None

    def connect(self, **config):
        return FakeConnection(self, config["host"])


class FakeConnection:
    def __init__(self, store, host):
        self.store = store
        self.host = host
        self.closed = False

    def cursor(self):
        return FakeCursor(self.store, self.host)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def close(self):
        self.closed = True


class FakeCursor:
    def __init__(self, store, host):
        self.store = store
        self.host = host
        self.selected = []
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params):
        if "SELECT p.job_id" in query:
            self.store.queries.append(query)
            assert "NOT EXISTS" in query
            assert "'usajobs', 'adzuna'" in query
            assert "AND source = 'adzuna'" not in query
            assert params[0] > 0
            seen_ids = {job_id for job_id, skill, source in self.store.saved[self.host]}
            pending = [row for row in self.store.rows[self.host]
                       if row[0] not in seen_ids and row[2] in ("usajobs", "adzuna")]
            self.selected = pending[:params[0]]
        elif "INSERT INTO llm_extracted_skills" in query:
            if self.store.fail_write_on == self.host:
                raise RuntimeError("simulated database failure")
            record = (params[0], params[1], params[2])
            self.rowcount = int(record not in self.store.saved[self.host])
            self.store.saved[self.host].add(record)
        else:
            raise AssertionError("Unexpected SQL: " + query)

    def fetchall(self):
        return list(self.selected)


@pytest.fixture
def module_and_store(monkeypatch):
    store = FakeStore({
        "local": [
            ("us-1", "Python and SQL", "usajobs"),
            ("ad-1", "Spark and Docker", "adzuna"),
            ("us-2", "Communication and teamwork", "usajobs"),
            ("ad-2", "Python", "adzuna"),
        ],
        "rds": [
            ("us-1", "Python and SQL", "usajobs"),
            ("ad-1", "Spark and Docker", "adzuna"),
            ("us-2", "Communication and teamwork", "usajobs"),
            ("ad-2", "Python", "adzuna"),
        ],
    })
    monkeypatch.setitem(sys.modules, "psycopg2", types.SimpleNamespace(connect=store.connect))
    monkeypatch.setitem(sys.modules, "rds_config", types.SimpleNamespace(RDS_CONFIG={"host": "rds"}))
    monkeypatch.setitem(sys.modules, "boto3", types.SimpleNamespace(client=lambda *a, **kw: None))
    monkeypatch.setitem(sys.modules, "openai", types.SimpleNamespace(OpenAI=lambda *a, **kw: None))
    spec = importlib.util.spec_from_file_location("test_llm_replacement", MODULE_FILE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "CONFIGS", [{"host": "local"}, {"host": "rds"}])
    return module, store


def test_both_sources_all_pending_not_just_first_batch(module_and_store, monkeypatch):
    module, store = module_and_store
    calls = []

    def fake_extract(description, job_id):
        calls.append(job_id)
        return [] if job_id == "us-2" else ["python"]

    monkeypatch.setattr(module, "extract_skills_with_llm", fake_extract)
    stats = module.extract_llm_skills_and_store(batch_size=2)
    assert stats == {"jobs_processed": 8, "skills_inserted": 6}
    assert len(calls) == 4, "one LLM call per identical job across local and RDS"
    for host in ("local", "rds"):
        assert {row[0] for row in store.saved[host]} == {"us-1", "us-2", "ad-1", "ad-2"}
        assert ("us-2", "__processed__", "usajobs") in store.saved[host]
    assert store.queries, "the SQL query was used"
    assert module.extract_llm_skills_and_store(batch_size=2)["jobs_processed"] == 0


def test_rds_only_gap_is_recovered(module_and_store, monkeypatch):
    module, store = module_and_store
    for job_id, _, source in store.rows["local"]:
        store.saved["local"].add((job_id, "python", source))
    monkeypatch.setattr(module, "extract_skills_with_llm", lambda description, job_id: ["sql"])
    stats = module.extract_llm_skills_and_store(batch_size=2)
    assert stats["jobs_processed"] == 4
    assert len(store.saved["rds"]) == 4
    assert all(row[1] == "sql" for row in store.saved["rds"])


def test_model_failure_does_not_mark_as_processed(module_and_store, monkeypatch):
    module, store = module_and_store

    def failed_extract(description, job_id):
        raise RuntimeError("simulated model failure")

    monkeypatch.setattr(module, "extract_skills_with_llm", failed_extract)
    with pytest.raises(RuntimeError, match="simulated model failure"):
        module.extract_llm_skills_and_store(batch_size=2)
    assert store.saved["local"] == set()
    assert store.saved["rds"] == set()


def test_database_failure_stays_visible_and_rds_stays_pending(module_and_store, monkeypatch):
    module, store = module_and_store
    store.fail_write_on = "rds"
    monkeypatch.setattr(module, "extract_skills_with_llm", lambda description, job_id: ["sql"])
    with pytest.raises(RuntimeError, match="simulated database failure"):
        module.extract_llm_skills_and_store(batch_size=100)
    assert len(store.saved["local"]) == 4
    assert store.saved["rds"] == set()


def test_parse_normalize_deduplicate_and_no_short_text_skip(module_and_store):
    module, _ = module_and_store
    calls = []

    def fake_create(**kwargs):
        calls.append(kwargs)
        return types.SimpleNamespace(choices=[types.SimpleNamespace(
            finish_reason="stop",
            message=types.SimpleNamespace(content='[" Python ", "python", " AWS ", ""]')
        )])

    fake_client = types.SimpleNamespace(chat=types.SimpleNamespace(
        completions=types.SimpleNamespace(create=fake_create)
    ))
    assert module.extract_skills_with_llm("Python", "job-1", fake_client) == ["python", "aws"]
    assert calls[0]["messages"][1]["content"].endswith("Python")


def test_bad_llm_output_raises_not_empty_result(module_and_store):
    module, _ = module_and_store

    def fake_client_with(content, finish_reason="stop"):
        return types.SimpleNamespace(chat=types.SimpleNamespace(completions=types.SimpleNamespace(
            create=lambda **kw: types.SimpleNamespace(choices=[types.SimpleNamespace(
                finish_reason=finish_reason,
                message=types.SimpleNamespace(content=content),
            )])
        )))

    with pytest.raises(ValueError, match="Invalid JSON"):
        module.extract_skills_with_llm("Python", "job-1", fake_client_with("not json"))
    with pytest.raises(ValueError, match="truncated"):
        module.extract_skills_with_llm("Python", "job-1", fake_client_with("[]", "length"))
    with pytest.raises(ValueError, match="array of strings"):
        module.extract_skills_with_llm("Python", "job-1", fake_client_with('{"skills": ["python"]}'))


def test_reject_nonpositive_batch(module_and_store):
    module, _ = module_and_store
    with pytest.raises(ValueError, match="positive integer"):
        module.extract_llm_skills_and_store(batch_size=0)
