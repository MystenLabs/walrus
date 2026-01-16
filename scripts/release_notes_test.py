# Copyright (c) Walrus Foundation
# SPDX-License-Identifier: Apache-2.0

import unittest
from unittest.mock import patch, MagicMock
import json

from release_notes import (
    parse_notes,
    pr_has_release_notes,
    extract_notes_for_pr,
    Note,
    RE_HEADING,
    RE_CHECKED,
)


class TestParseNotes(unittest.TestCase):
    """Tests for the parse_notes function."""

    def test_empty_notes(self):
        """Test parsing empty notes."""
        pr, result = parse_notes(123, "")
        self.assertEqual(pr, 123)
        self.assertEqual(result, {})

    def test_none_notes(self):
        """Test parsing None notes."""
        pr, result = parse_notes(123, None)
        self.assertEqual(pr, 123)
        self.assertEqual(result, {})

    def test_no_release_notes_section(self):
        """Test parsing notes without release notes section."""
        pr, result = parse_notes(123, "Some PR description without release notes")
        self.assertEqual(pr, 123)
        self.assertEqual(result, {})

    def test_release_notes_with_checked_item(self):
        """Test parsing release notes with a checked item."""
        body = """## Release notes
- [x] Storage node: Updated storage allocation
- [ ] CLI: No changes
"""
        pr, result = parse_notes(123, body)
        self.assertEqual(pr, 123)
        self.assertIn("Storage node", result)
        self.assertTrue(result["Storage node"].checked)
        self.assertEqual(result["Storage node"].note, "Updated storage allocation")

    def test_release_notes_with_unchecked_item(self):
        """Test parsing release notes with an unchecked item."""
        body = """## Release notes
- [ ] Storage node: Some note
"""
        pr, result = parse_notes(123, body)
        self.assertIn("Storage node", result)
        self.assertFalse(result["Storage node"].checked)

    def test_release_notes_case_insensitive_heading(self):
        """Test that release notes heading is case insensitive."""
        body = """## RELEASE NOTES
- [x] CLI: Added new command
"""
        pr, result = parse_notes(123, body)
        self.assertIn("CLI", result)
        self.assertTrue(result["CLI"].checked)

    def test_release_notes_case_insensitive_checkbox(self):
        """Test that checkbox X is case insensitive."""
        body = """## Release notes
- [X] Storage node: Test uppercase X
"""
        pr, result = parse_notes(123, body)
        self.assertIn("Storage node", result)
        self.assertTrue(result["Storage node"].checked)

    def test_multiple_release_notes(self):
        """Test parsing multiple release notes."""
        body = """## Release notes
- [x] Storage node: Storage changes
- [x] CLI: CLI changes
- [ ] Aggregator: No changes
"""
        pr, result = parse_notes(123, body)
        self.assertEqual(len(result), 3)
        self.assertTrue(result["Storage node"].checked)
        self.assertTrue(result["CLI"].checked)
        self.assertFalse(result["Aggregator"].checked)

    def test_release_notes_with_multiline_note(self):
        """Test parsing release notes with multiline content."""
        body = (
            "## Release notes\n"
            "- [x] Storage node: This is a long note\n"
            "    that spans multiple lines\n"
            "    with additional details\n"
            "- [ ] CLI: No changes\n"
        )
        pr, result = parse_notes(123, body)
        self.assertIn("Storage node", result)
        note = result["Storage node"].note
        self.assertIn("long note", note)
        self.assertIn("multiple lines", note)


class TestPrHasReleaseNotes(unittest.TestCase):
    """Tests for the pr_has_release_notes function."""

    @patch('release_notes.gh_api')
    def test_pr_with_checked_release_notes(self, mock_gh_api):
        """Test that a PR with checked release notes returns True."""
        mock_gh_api.return_value = {
            "body": """## Release notes
- [x] Storage node: Some changes
"""
        }
        self.assertTrue(pr_has_release_notes(123))

    @patch('release_notes.gh_api')
    def test_pr_without_release_notes_section(self, mock_gh_api):
        """Test that a PR without release notes section returns False."""
        mock_gh_api.return_value = {
            "body": "Just a regular PR description"
        }
        self.assertFalse(pr_has_release_notes(123))

    @patch('release_notes.gh_api')
    def test_pr_with_unchecked_release_notes(self, mock_gh_api):
        """Test that a PR with only unchecked release notes returns False."""
        mock_gh_api.return_value = {
            "body": """## Release notes
- [ ] Storage node: No changes
"""
        }
        self.assertFalse(pr_has_release_notes(123))

    @patch('release_notes.gh_api')
    def test_pr_with_empty_body(self, mock_gh_api):
        """Test that a PR with empty body returns False."""
        mock_gh_api.return_value = {"body": ""}
        self.assertFalse(pr_has_release_notes(123))

    @patch('release_notes.gh_api')
    def test_pr_api_returns_none(self, mock_gh_api):
        """Test that API returning None is handled gracefully."""
        mock_gh_api.return_value = None
        self.assertFalse(pr_has_release_notes(123))


class TestExtractNotesForPr(unittest.TestCase):
    """Tests for the extract_notes_for_pr function."""

    @patch('release_notes.gh_api')
    def test_extract_notes_success(self, mock_gh_api):
        """Test successful extraction of notes from a PR."""
        mock_gh_api.return_value = {
            "body": """## Release notes
- [x] Storage node: Added new feature
- [x] CLI: Updated commands
"""
        }
        pr, notes = extract_notes_for_pr(456)
        self.assertEqual(pr, 456)
        self.assertEqual(len(notes), 2)
        self.assertTrue(notes["Storage node"].checked)
        self.assertTrue(notes["CLI"].checked)

    @patch('release_notes.gh_api')
    def test_extract_notes_api_failure(self, mock_gh_api):
        """Test extraction when API returns None."""
        mock_gh_api.return_value = None
        pr, notes = extract_notes_for_pr(789)
        self.assertEqual(pr, 789)
        self.assertEqual(notes, {})

    @patch('release_notes.gh_api')
    def test_extract_notes_no_body(self, mock_gh_api):
        """Test extraction when PR has no body."""
        mock_gh_api.return_value = {"body": None}
        pr, notes = extract_notes_for_pr(101)
        self.assertEqual(pr, 101)
        self.assertEqual(notes, {})


class TestRegexPatterns(unittest.TestCase):
    """Tests for the regex patterns."""

    def test_re_heading_matches_various_formats(self):
        """Test that RE_HEADING matches various release notes formats."""
        test_cases = [
            "## Release notes\nContent",
            "# Release Notes\nContent",
            "### RELEASE NOTES\nContent",
            "## release notes\nContent",
        ]
        for text in test_cases:
            with self.subTest(text=text):
                self.assertIsNotNone(RE_HEADING.search(text))

    def test_re_checked_matches_checked_boxes(self):
        """Test that RE_CHECKED matches checked checkboxes."""
        test_cases = [
            "- [x] Item",
            "- [X] Item",
            "  - [x] Indented",
            "- [x]No space after bracket",
        ]
        for text in test_cases:
            with self.subTest(text=text):
                self.assertIsNotNone(RE_CHECKED.search(text))

    def test_re_checked_does_not_match_unchecked(self):
        """Test that RE_CHECKED does not match unchecked checkboxes."""
        test_cases = [
            "- [ ] Unchecked",
            "- [] Empty",
        ]
        for text in test_cases:
            with self.subTest(text=text):
                self.assertIsNone(RE_CHECKED.search(text))


if __name__ == "__main__":
    unittest.main()
