# -*- coding: utf-8 -*-

import pytest

from eidaws.federator.utils.strict import ValidationError, AIOHTTPKeywordParser


class TestAIOHTTPKeywordParser:
    @staticmethod
    def create_parser(*args, **kwargs):
        return AIOHTTPKeywordParser(*args, **kwargs)

    def test_parse_arg_keys(self):
        arg_dict = {
            "key1": "val1",
            "key2": "val2",
            "key3": "val3",
        }

        parser = self.create_parser()

        assert parser._parse_arg_keys(arg_dict) == tuple(
            ["key1", "key2", "key3"]
        )

    def test_parse_postfile(self):
        postfile_data = "key1=val1\nkey2=val2\nkey3=val3"

        parser = self.create_parser()

        test_result = parser._parse_postfile(postfile_data)

        assert "key1" in test_result
        assert "key2" in test_result
        assert "key3" in test_result
        assert len(test_result) == 3

    def test_parse_equal(self):
        postfile_data = "="

        parser = self.create_parser()

        with pytest.raises(ValidationError):
            _ = parser._parse_postfile(postfile_data)

    def test_parse_empty_postfile(self):
        postfile_data = ""

        parser = self.create_parser()

        assert parser._parse_postfile(postfile_data) == tuple()

    def test_parse_postfile_with_sncl(self):
        postfile_data = "NL HGN * 2013-10-10 2013-10-11"

        parser = self.create_parser()

        assert parser._parse_postfile(postfile_data) == tuple()
