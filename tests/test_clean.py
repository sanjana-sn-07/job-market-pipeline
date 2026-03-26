import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'dags'))

from clean import strip_html, normalize_title


class TestStripHtml:
    def test_removes_basic_tags(self):
        result = strip_html("<p>Hello world</p>")
        assert result == "Hello world"

    def test_removes_nested_tags(self):
        result = strip_html("<ul><li>Python</li><li>SQL</li></ul>")
        assert "Python" in result
        assert "SQL" in result
        assert "<" not in result

    def test_decodes_nbsp(self):
        result = strip_html("Hello&nbsp;World")
        assert result == "Hello World"

    def test_decodes_amp(self):
        result = strip_html("Data &amp; Analytics")
        assert result == "Data & Analytics"

    def test_handles_empty_string(self):
        assert strip_html("") == ""

    def test_handles_none(self):
        assert strip_html(None) == ""

    def test_collapses_whitespace(self):
        result = strip_html("Hello   World")
        assert result == "Hello World"

    def test_plain_text_unchanged(self):
        result = strip_html("No HTML here")
        assert result == "No HTML here"


class TestNormalizeTitle:
    def test_expands_sr(self):
        result = normalize_title("Sr. Data Engineer")
        assert "Senior" in result
        assert "Sr." not in result

    def test_expands_jr(self):
        result = normalize_title("Jr. Developer")
        assert "Junior" in result

    def test_expands_engr(self):
        result = normalize_title("Data Engr.")
        assert "Engineer" in result

    def test_expands_mgr(self):
        result = normalize_title("Engineering Mgr.")
        assert "Manager" in result

    def test_plain_title_unchanged(self):
        result = normalize_title("Data Engineer")
        assert result == "Data Engineer"

    def test_handles_empty_string(self):
        assert normalize_title("") == ""

    def test_handles_none(self):
        assert normalize_title(None) == ""
