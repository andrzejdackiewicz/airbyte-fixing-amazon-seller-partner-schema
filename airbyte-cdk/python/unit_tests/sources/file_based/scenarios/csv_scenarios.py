#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from airbyte_cdk.sources.file_based.exceptions import ConfigValidationError, FileBasedSourceError, InvalidSchemaError, SchemaInferenceError
from unit_tests.sources.file_based.helpers import EmptySchemaParser, LowInferenceLimitDiscoveryPolicy
from unit_tests.sources.file_based.scenarios.scenario_builder import TestScenarioBuilder

single_csv_scenario = (
    TestScenarioBuilder()
    .set_name("single_csv_stream")
    .set_config(
        {
            "streams": [
                {
                    "name": "stream1",
                    "file_type": "csv",
                    "globs": ["*"],
                    "validation_policy": "emit_record",
                }
            ],
            "start_date": "2023-06-04T03:54:07Z"
        }
    )
    .set_files(
        {
            "a.csv": {
                "contents": [
                    ("col1", "col2"),
                    ("val11", "val12"),
                    ("val21", "val22"),
                ],
                "last_modified": "2023-06-05T03:54:07.000Z",
            }
        }
    )
    .set_file_type("csv")
    .set_expected_spec(
        {
            "documentationUrl": "https://docs.airbyte.com/integrations/sources/in_memory_files",
            "connectionSpecification": {
                "title": "InMemorySpec",
                "description": "Used during spec; allows the developer to configure the cloud provider specific options\nthat are needed "
                               "when users configure a file-based source.",
                "type": "object",
                "properties": {
                    "streams": {
                        "title": "The list of streams to sync",
                        "description": "Streams defines the behavior for grouping files together that will be synced to the downstream "
                                       "destination. Each stream has it own independent configuration to handle which files to sync, "
                                       "how files should be parsed, and the validation of records against the schema.",
                        "order": 10,
                        "type": "array",
                        "items": {
                            "title": "FileBasedStreamConfig",
                            "type": "object",
                            "properties": {
                                "name": {
                                    "title": "Name",
                                    "type": "string"
                                },
                                "file_type": {
                                    "title": "File Type",
                                    "type": "string"
                                },
                                "globs": {
                                    "title": "Globs",
                                    "type": "array",
                                    "items": {
                                        "type": "string"
                                    }
                                },
                                "schemaless": {
                                    "title": "Schemaless",
                                    "default": False,
                                    "type": "boolean"
                                },
                                "validation_policy": {
                                    "title": "Validation Policy",
                                    "type": "string"
                                },
                                "input_schema": {
                                    "title": "Input Schema",
                                    "type": "object"
                                },
                                "primary_key": {
                                    "title": "Primary Key",
                                    "anyOf": [
                                        {
                                            "type": "string"
                                        },
                                        {
                                            "type": "array",
                                            "items": {
                                                "type": "string"
                                            }
                                        },
                                        {
                                            "type": "array",
                                            "items": {
                                                "type": "array",
                                                "items": {
                                                    "type": "string"
                                                }
                                            }
                                        }
                                    ]
                                },
                                "days_to_sync_if_history_is_full": {
                                    "title": "Days To Sync If History Is Full",
                                    "default": 3,
                                    "type": "integer"
                                },
                                "format": {
                                    "anyOf": [
                                        {
                                            "title": "Format",
                                            "type": "object",
                                            "additionalProperties": {
                                                "title": "CsvFormat",
                                                "type": "object",
                                                "properties": {
                                                    "delimiter": {
                                                        "title": "Delimiter",
                                                        "default": ",",
                                                        "type": "string"
                                                    },
                                                    "quote_char": {
                                                        "title": "Quote Char",
                                                        "default": "\"",
                                                        "type": "string"
                                                    },
                                                    "escape_char": {
                                                        "title": "Escape Char",
                                                        "type": "string"
                                                    },
                                                    "encoding": {
                                                        "title": "Encoding",
                                                        "default": "utf8",
                                                        "type": "string"
                                                    },
                                                    "double_quote": {
                                                        "title": "Double Quote",
                                                        "type": "boolean"
                                                    },
                                                    "quoting_behavior": {
                                                        "default": "Quote Special Characters",
                                                        "enum": ["Quote All", "Quote Special Characters", "Quote Non-numeric", "Quote None"]
                                                    }
                                                },
                                                "required": ["double_quote"]
                                            }
                                        },
                                        {
                                            "type": "object"
                                        }
                                    ]
                                }
                            },
                            "required": ["name", "file_type", "validation_policy"]
                        }
                    },
                    "start_date": {
                        "title": "Start Date",
                        "description": "UTC date and time in the format 2017-01-25T00:00:00Z. Any file modified before this date will not "
                                       "be replicated.",
                        "examples": ["2021-01-01T00:00:00Z"],
                        "format": "date-time",
                        "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$",
                        "order": 1,
                        "type": "string"
                    }
                },
                "required": ["streams"]
            },
        }
    )
    .set_expected_catalog(
        {
            "streams": [
                {
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col1": {
                                "type": "string"
                            },
                            "col2": {
                                "type": "string"
                            },
                            "_ab_source_file_last_modified": {
                                "type": "string"
                            },
                            "_ab_source_file_url": {
                                "type": "string"
                            },
                        },
                    },
                    "name": "stream1",
                    "source_defined_cursor": True,
                    "supported_sync_modes": ["full_refresh", "incremental"],
                }
            ]
        }
    )
    .set_expected_records(
        [
            {"data": {"col1": "val11", "col2": "val12", "_ab_source_file_last_modified": "2023-06-05T03:54:07Z",
                      "_ab_source_file_url": "a.csv"}, "stream": "stream1"},
            {"data": {"col1": "val21", "col2": "val22", "_ab_source_file_last_modified": "2023-06-05T03:54:07Z",
                      "_ab_source_file_url": "a.csv"}, "stream": "stream1"},
        ]
    )
).build()

multi_csv_scenario = (
    TestScenarioBuilder()
    .set_name("multi_csv_stream")
    .set_config(
        {
            "streams": [
                {
                    "name": "stream1",
                    "file_type": "csv",
                    "globs": ["*"],
                    "validation_policy": "emit_record",
                }
            ]
        }
    )
    .set_files(
        {
            "a.csv": {
                "contents": [
                    ("col1", "col2"),
                    ("val11a", "val12a"),
                    ("val21a", "val22a"),
                ],
                "last_modified": "2023-06-05T03:54:07.000Z",
            },
            "b.csv": {
                "contents": [
                    ("col1", "col2", "col3"),
                    ("val11b", "val12b", "val13b"),
                    ("val21b", "val22b", "val23b"),
                ],
                "last_modified": "2023-06-05T03:54:07.000Z",
            },
        }
    )
    .set_file_type("csv")
    .set_expected_catalog(
        {
            "streams": [
                {
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col1": {
                                "type": "string"
                            },
                            "col2": {
                                "type": "string"
                            },
                            "col3": {
                                "type": "string"
                            },
                            "_ab_source_file_last_modified": {
                                "type": "string"
                            },
                            "_ab_source_file_url": {
                                "type": "string"
                            },
                        }
                    },
                    "name": "stream1",
                    "source_defined_cursor": True,
                    "supported_sync_modes": ["full_refresh", "incremental"],
                }
            ]
        }
    )
    .set_expected_records(
        [
            {"data": {"col1": "val11a", "col2": "val12a", "_ab_source_file_last_modified": "2023-06-05T03:54:07Z",
                      "_ab_source_file_url": "a.csv"}, "stream": "stream1"},
            {"data": {"col1": "val21a", "col2": "val22a", "_ab_source_file_last_modified": "2023-06-05T03:54:07Z",
                      "_ab_source_file_url": "a.csv"}, "stream": "stream1"},
            {"data": {"col1": "val11b", "col2": "val12b", "col3": "val13b", "_ab_source_file_last_modified": "2023-06-05T03:54:07Z",
                      "_ab_source_file_url": "b.csv"}, "stream": "stream1"},
            {"data": {"col1": "val21b", "col2": "val22b", "col3": "val23b", "_ab_source_file_last_modified": "2023-06-05T03:54:07Z",
                      "_ab_source_file_url": "b.csv"}, "stream": "stream1"},
        ]
    )
).build()

multi_csv_stream_n_file_exceeds_limit_for_inference = (
    TestScenarioBuilder()
    .set_name("multi_csv_stream_n_file_exceeds_limit")
    .set_config(
        {
            "streams": [
                {
                    "name": "stream1",
                    "file_type": "csv",
                    "globs": ["*"],
                    "validation_policy": "emit_record",
                }
            ]
        }
    )
    .set_files(
        {
            "a.csv": {
                "contents": [
                    ("col1", "col2"),
                    ("val11a", "val12a"),
                    ("val21a", "val22a"),
                ],
                "last_modified": "2023-06-05T03:54:07.000Z",
            },
            "b.csv": {
                "contents": [
                    ("col1", "col2", "col3"),
                    ("val11b", "val12b", "val13b"),
                    ("val21b", "val22b", "val23b"),
                ],
                "last_modified": "2023-06-05T03:54:07.000Z",
            },
        }
    )
    .set_file_type("csv")
    .set_expected_catalog(
        {
            "streams": [
                {
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col1": {
                                "type": "string"
                            }, "col2": {
                                "type": "string"
                            },
                            "_ab_source_file_last_modified": {
                                "type": "string"
                            },
                            "_ab_source_file_url": {
                                "type": "string"
                            },
                        }
                    },
                    "name": "stream1",
                    "source_defined_cursor": True,
                    "supported_sync_modes": ["full_refresh", "incremental"],
                }
            ]
        }
    )
    .set_expected_records(
        [
            {"data": {"col1": "val11a", "col2": "val12a", "_ab_source_file_last_modified": "2023-06-05T03:54:07Z",
                      "_ab_source_file_url": "a.csv"}, "stream": "stream1"},
            {"data": {"col1": "val21a", "col2": "val22a", "_ab_source_file_last_modified": "2023-06-05T03:54:07Z",
                      "_ab_source_file_url": "a.csv"}, "stream": "stream1"},
            {"data": {"col1": "val11b", "col2": "val12b", "col3": "val13b", "_ab_source_file_last_modified": "2023-06-05T03:54:07Z",
                      "_ab_source_file_url": "b.csv"}, "stream": "stream1"},
            {"data": {"col1": "val21b", "col2": "val22b", "col3": "val23b", "_ab_source_file_last_modified": "2023-06-05T03:54:07Z",
                      "_ab_source_file_url": "b.csv"}, "stream": "stream1"},
        ]
    )
    .set_discovery_policy(LowInferenceLimitDiscoveryPolicy())
).build()

invalid_csv_scenario = (
    TestScenarioBuilder()
    .set_name("invalid_csv_scenario")
    .set_config(
        {
            "streams": [
                {
                    "name": "stream1",
                    "file_type": "csv",
                    "globs": ["*"],
                    "validation_policy": "emit_record",
                }
            ]
        }
    )
    .set_files(
        {
            "a.csv": {
                "contents": [
                    ("col1",),
                    ("val11", "val12"),
                    ("val21", "val22"),
                ],
                "last_modified": "2023-06-05T03:54:07.000Z",
            }
        }
    )
    .set_file_type("csv")
    .set_expected_catalog(
        {
            "streams": [
                {
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col1": {
                                "type": "string"
                            }, "col2": {
                                "type": "string"
                            },
                            "_ab_source_file_last_modified": {
                                "type": "string"
                            },
                            "_ab_source_file_url": {
                                "type": "string"
                            },
                        }
                    },
                    "name": "stream1",
                    "source_defined_cursor": True,
                    "supported_sync_modes": ["full_refresh", "incremental"],
                }
            ]
        }
    )
    .set_expected_records([])
    .set_expected_discover_error(SchemaInferenceError, FileBasedSourceError.SCHEMA_INFERENCE_ERROR.value)
    .set_expected_logs(
        [
            {
                "level": "ERROR",
                "message": f"{FileBasedSourceError.ERROR_PARSING_RECORD.value} stream=stream1 file=a.csv line_no=1 n_skipped=0",
            },
        ]
    )
).build()

csv_single_stream_scenario = (
    TestScenarioBuilder()
    .set_name("csv_single_stream_scenario")
    .set_config(
        {
            "streams": [
                {
                    "name": "stream1",
                    "file_type": "csv",
                    "globs": ["*.csv"],
                    "validation_policy": "emit_record",
                }
            ]
        }
    )
    .set_files(
        {
            "a.csv": {
                "contents": [
                    ("col1", "col2"),
                    ("val11a", "val12a"),
                    ("val21a", "val22a"),
                ],
                "last_modified": "2023-06-05T03:54:07.000Z",
            },
            "b.jsonl": {
                "contents": [
                    {"col1": "val11b", "col2": "val12b", "col3": "val13b"},
                    {"col1": "val12b", "col2": "val22b", "col3": "val23b"},
                ],
                "last_modified": "2023-06-05T03:54:07.000Z",
            },
        }
    )
    .set_file_type("csv")
    .set_expected_catalog(
        {
            "streams": [
                {
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col1": {
                                "type": "string"
                            },
                            "col2": {
                                "type": "string"
                            },
                            "_ab_source_file_last_modified": {
                                "type": "string"
                            },
                            "_ab_source_file_url": {
                                "type": "string"
                            },
                        },
                    },
                    "name": "stream1",
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "source_defined_cursor": True,
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                }
            ]
        }
    )
    .set_expected_records(
        [
            {"data": {"col1": "val11a", "col2": "val12a", "_ab_source_file_last_modified": "2023-06-05T03:54:07Z",
                      "_ab_source_file_url": "a.csv"}, "stream": "stream1"},
            {"data": {"col1": "val21a", "col2": "val22a", "_ab_source_file_last_modified": "2023-06-05T03:54:07Z",
                      "_ab_source_file_url": "a.csv"}, "stream": "stream1"},
        ]
    )
).build()

csv_multi_stream_scenario = (
    TestScenarioBuilder()
    .set_name("csv_multi_stream")
    .set_config(
        {
            "streams": [
                {
                    "name": "stream1",
                    "file_type": "csv",
                    "globs": ["*.csv"],
                    "validation_policy": "emit_record",
                },
                {
                    "name": "stream2",
                    "file_type": "csv",
                    "globs": ["b.csv"],
                    "validation_policy": "emit_record",
                }
            ]
        }
    )
    .set_files(
        {
            "a.csv": {
                "contents": [
                    ("col1", "col2"),
                    ("val11a", "val12a"),
                    ("val21a", "val22a"),
                ],
                "last_modified": "2023-06-05T03:54:07.000Z",
            },
            "b.csv": {
                "contents": [
                    ("col3",),
                    ("val13b",),
                    ("val23b",),
                ],
                "last_modified": "2023-06-05T03:54:07.000Z",
            },
        }
    )
    .set_file_type("csv")
    .set_expected_catalog(
        {
            "streams": [
                {
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col1": {
                                "type": "string"
                            },
                            "col2": {
                                "type": "string"
                            },
                            "col3": {
                                "type": "string"
                            },
                            "_ab_source_file_last_modified": {
                                "type": "string"
                            },
                            "_ab_source_file_url": {
                                "type": "string"
                            },
                        },
                    },
                    "name": "stream1",
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "source_defined_cursor": True,
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                },
                {
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col3": {
                                "type": "string"
                            },
                            "_ab_source_file_last_modified": {
                                "type": "string"
                            },
                            "_ab_source_file_url": {
                                "type": "string"
                            },
                        },
                    },
                    "name": "stream2",
                    "source_defined_cursor": True,
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "supported_sync_modes": ["full_refresh", "incremental"],
                },
            ]
        }
    )
    .set_expected_records(
        [
            {"data": {"col1": "val11a", "col2": "val12a", "_ab_source_file_last_modified": "2023-06-05T03:54:07Z",
                      "_ab_source_file_url": "a.csv"}, "stream": "stream1"},
            {"data": {"col1": "val21a", "col2": "val22a", "_ab_source_file_last_modified": "2023-06-05T03:54:07Z",
                      "_ab_source_file_url": "a.csv"}, "stream": "stream1"},
            {"data": {"col3": "val13b", "_ab_source_file_last_modified": "2023-06-05T03:54:07Z", "_ab_source_file_url": "b.csv"},
             "stream": "stream1"},
            {"data": {"col3": "val23b", "_ab_source_file_last_modified": "2023-06-05T03:54:07Z", "_ab_source_file_url": "b.csv"},
             "stream": "stream1"},
            {"data": {"col3": "val13b", "_ab_source_file_last_modified": "2023-06-05T03:54:07Z", "_ab_source_file_url": "b.csv"},
             "stream": "stream2"},
            {"data": {"col3": "val23b", "_ab_source_file_last_modified": "2023-06-05T03:54:07Z", "_ab_source_file_url": "b.csv"},
             "stream": "stream2"},
        ]
    )
).build()


csv_custom_format_scenario = (
    TestScenarioBuilder()
    .set_name("csv_custom_format")
    .set_config(
        {
            "streams": [
                {
                    "name": "stream1",
                    "file_type": "csv",
                    "globs": ["*"],
                    "validation_policy": "emit_record",
                    "format": {
                        "csv": {
                            "filetype": "csv",
                            "delimiter": "#",
                            "quote_char": "|",
                            "escape_char": "!",
                            "double_quote": True,
                            "quoting_behavior": "Quote Special Characters"
                        }
                    }
                }
            ]
        }
    )
    .set_files(
        {
            "a.csv": {
                "contents": [
                    ("col1", "col2", "col3"),
                    ("val11", "val12", "val |13|"),
                    ("val21", "val22", "val23"),
                    ("val,31", "val |,32|", "val, !!!! 33"),
                ],
                "last_modified": "2023-06-05T03:54:07.000Z",
            }
        }
    )
    .set_file_type("csv")
    .set_expected_catalog(
        {
            "streams": [
                {
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col1": {
                                "type": "string",
                            },
                            "col2": {
                                "type": "string",
                            },
                            "col3": {
                                "type": "string",
                            },
                            "_ab_source_file_last_modified": {
                                "type": "string"
                            },
                            "_ab_source_file_url": {
                                "type": "string"
                            },
                        },
                    },
                    "name": "stream1",
                    "source_defined_cursor": True,
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "supported_sync_modes": ["full_refresh", "incremental"],
                }
            ]
        }
    )
    .set_expected_records(
        [
            {"data": {"col1": "val11", "col2": "val12", "col3": "val |13|", "_ab_source_file_last_modified": "2023-06-05T03:54:07Z",
                      "_ab_source_file_url": "a.csv"}, "stream": "stream1"},
            {"data": {"col1": "val21", "col2": "val22", "col3": "val23", "_ab_source_file_last_modified": "2023-06-05T03:54:07Z",
                      "_ab_source_file_url": "a.csv"}, "stream": "stream1"},
            {"data": {"col1": "val,31", "col2": "val |,32|", "col3": "val, !! 33", "_ab_source_file_last_modified": "2023-06-05T03:54:07Z",
                      "_ab_source_file_url": "a.csv"}, "stream": "stream1"},
        ]
    )
    .set_file_write_options(
        {
            "delimiter": "#",
            "quotechar": "|",
        }
    )
).build()


csv_legacy_format_scenario = (
    TestScenarioBuilder()
    .set_name("csv_legacy_format")
    .set_config(
        {
            "streams": [
                {
                    "name": "stream1",
                    "file_type": "csv",
                    "globs": ["*"],
                    "validation_policy": "emit_record",
                    "format": {
                        "filetype": "csv",
                        "delimiter": "#",
                        "quote_char": "|",
                        "escape_char": "!",
                        "double_quote": True,
                        "quoting_behavior": "Quote All"
                    }
                }
            ]
        }
    )
    .set_files(
        {
            "a.csv": {
                "contents": [
                    ("col1", "col2", "col3"),
                    ("val11", "val12", "val |13|"),
                    ("val21", "val22", "val23"),
                    ("val,31", "val |,32|", "val, !!!! 33"),
                ],
                "last_modified": "2023-06-05T03:54:07.000Z",
            }
        }
    )
    .set_file_type("csv")
    .set_expected_catalog(
        {
            "streams": [
                {
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col1": {
                                "type": "string",
                            },
                            "col2": {
                                "type": "string",
                            },
                            "col3": {
                                "type": "string",
                            },
                            "_ab_source_file_last_modified": {
                                "type": "string"
                            },
                            "_ab_source_file_url": {
                                "type": "string"
                            },
                        },
                    },
                    "name": "stream1",
                    "source_defined_cursor": True,
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "supported_sync_modes": ["full_refresh", "incremental"],
                }
            ]
        }
    )
    .set_expected_records(
        [
            {"data": {"col1": "val11", "col2": "val12", "col3": "val |13|", "_ab_source_file_last_modified": "2023-06-05T03:54:07Z",
                      "_ab_source_file_url": "a.csv"}, "stream": "stream1"},
            {"data": {"col1": "val21", "col2": "val22", "col3": "val23", "_ab_source_file_last_modified": "2023-06-05T03:54:07Z",
                      "_ab_source_file_url": "a.csv"}, "stream": "stream1"},
            {"data": {"col1": "val,31", "col2": "val |,32|", "col3": "val, !! 33", "_ab_source_file_last_modified": "2023-06-05T03:54:07Z",
                      "_ab_source_file_url": "a.csv"}, "stream": "stream1"},
        ]
    )
    .set_file_write_options(
        {
            "delimiter": "#",
            "quotechar": "|",
        }
    )
).build()


multi_stream_custom_format = (
    TestScenarioBuilder()
    .set_name("multi_stream_custom_format_scenario")
    .set_config(
        {
            "streams": [
                {
                    "name": "stream1",
                    "file_type": "csv",
                    "globs": ["*.csv"],
                    "validation_policy": "emit_record",
                    "format": {
                        "csv": {
                            "filetype": "csv",
                            "delimiter": "#",
                            "escape_char": "!",
                            "double_quote": True,
                            "newlines_in_values": False
                        }
                    }
                },
                {
                    "name": "stream2",
                    "file_type": "csv",
                    "globs": ["b.csv"],
                    "validation_policy": "emit_record",
                    "format": {
                        "csv": {
                            "filetype": "csv",
                            "delimiter": "#",
                            "escape_char": "@",
                            "double_quote": True,
                            "newlines_in_values": False,
                            "quoting_behavior": "Quote All"
                        }
                    }
                }
            ]
        }
    )
    .set_files(
        {
            "a.csv": {
                "contents": [
                    ("col1", "col2"),
                    ("val11a", "val !! 12a"),
                    ("val !! 21a", "val22a"),
                ],
                "last_modified": "2023-06-05T03:54:07.000Z",
            },
            "b.csv": {
                "contents": [
                    ("col3",),
                    ("val @@@@ 13b",),
                    ("val23b",),
                ],
                "last_modified": "2023-06-05T03:54:07.000Z",
            },
        }
    )
    .set_file_type("csv")
    .set_expected_catalog(
        {
            "streams": [
                {
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col1": {
                                "type": "string",
                            },
                            "col2": {
                                "type": "string",
                            },
                            "col3": {
                                "type": "string",
                            },
                            "_ab_source_file_last_modified": {
                                "type": "string"
                            },
                            "_ab_source_file_url": {
                                "type": "string"
                            },
                        },
                    },
                    "name": "stream1",
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "source_defined_cursor": True,
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                },
                {
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col3": {
                                "type": "string",
                            },
                            "_ab_source_file_last_modified": {
                                "type": "string"
                            },
                            "_ab_source_file_url": {
                                "type": "string"
                            },
                        },
                    },
                    "name": "stream2",
                    "source_defined_cursor": True,
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "supported_sync_modes": ["full_refresh", "incremental"],
                },
            ]
        }
    )
    .set_expected_records(
        [
            {"data": {"col1": "val11a", "col2": "val ! 12a", "_ab_source_file_last_modified": "2023-06-05T03:54:07Z",
                      "_ab_source_file_url": "a.csv"}, "stream": "stream1"},
            {"data": {"col1": "val ! 21a", "col2": "val22a", "_ab_source_file_last_modified": "2023-06-05T03:54:07Z",
                      "_ab_source_file_url": "a.csv"}, "stream": "stream1"},
            {"data": {"col3": "val @@@@ 13b", "_ab_source_file_last_modified": "2023-06-05T03:54:07Z", "_ab_source_file_url": "b.csv"},
             "stream": "stream1"},
            {"data": {"col3": "val23b", "_ab_source_file_last_modified": "2023-06-05T03:54:07Z", "_ab_source_file_url": "b.csv"},
             "stream": "stream1"},
            {"data": {"col3": "val @@ 13b", "_ab_source_file_last_modified": "2023-06-05T03:54:07Z", "_ab_source_file_url": "b.csv"},
             "stream": "stream2"},
            {"data": {"col3": "val23b", "_ab_source_file_last_modified": "2023-06-05T03:54:07Z", "_ab_source_file_url": "b.csv"},
             "stream": "stream2"},
        ]
    )
    .set_file_write_options(
        {
            "delimiter": "#",
        }
    )
).build()


empty_schema_inference_scenario = (
    TestScenarioBuilder()
    .set_name("empty_schema_inference_scenario")
    .set_config(
        {
            "streams": [
                {
                    "name": "stream1",
                    "file_type": "csv",
                    "globs": ["*"],
                    "validation_policy": "emit_record",
                }
            ]
        }
    )
    .set_files(
        {
            "a.csv": {
                "contents": [
                    ("col1", "col2"),
                    ("val11", "val12"),
                    ("val21", "val22"),
                ],
                "last_modified": "2023-06-05T03:54:07.000Z",
            }
        }
    )
    .set_file_type("csv")
    .set_expected_catalog(
        {
            "streams": [
                {
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col1": {
                                "type": "string"
                            },
                            "col2": {
                                "type": "string"
                            },
                            "_ab_source_file_last_modified": {
                                "type": "string"
                            },
                            "_ab_source_file_url": {
                                "type": "string"
                            },
                        },
                    },
                    "name": "stream1",
                    "source_defined_cursor": True,
                    "supported_sync_modes": ["full_refresh", "incremental"],
                }
            ]
        }
    )
    .set_parsers({'csv': EmptySchemaParser()})
    .set_expected_discover_error(InvalidSchemaError, FileBasedSourceError.INVALID_SCHEMA_ERROR.value)
    .set_expected_records(
        [
            {"data": {"col1": "val11", "col2": "val12", "_ab_source_file_last_modified": "2023-06-05T03:54:07Z",
                      "_ab_source_file_url": "a.csv"}, "stream": "stream1"},
            {"data": {"col1": "val21", "col2": "val22", "_ab_source_file_last_modified": "2023-06-05T03:54:07Z",
                      "_ab_source_file_url": "a.csv"}, "stream": "stream1"},
        ]
    )
).build()


schemaless_csv_scenario = (
    TestScenarioBuilder()
    .set_name("schemaless_csv_scenario")
    .set_config(
        {
            "streams": [
                {
                    "name": "stream1",
                    "file_type": "csv",
                    "globs": ["*"],
                    "validation_policy": "skip_record",
                    "schemaless": True,
                }
            ]
        }
    )
    .set_files(
        {
            "a.csv": {
                "contents": [
                    ("col1", "col2"),
                    ("val11a", "val12a"),
                    ("val21a", "val22a"),
                ],
                "last_modified": "2023-06-05T03:54:07.000Z",
            },
            "b.csv": {
                "contents": [
                    ("col1", "col2", "col3"),
                    ("val11b", "val12b", "val13b"),
                    ("val21b", "val22b", "val23b"),
                ],
                "last_modified": "2023-06-05T03:54:07.000Z",
            },
        }
    )
    .set_file_type("csv")
    .set_expected_catalog(
        {
            "streams": [
                {
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "data": {
                                "type": "object"
                            },
                            "_ab_source_file_last_modified": {
                                "type": "string"
                            },
                            "_ab_source_file_url": {
                                "type": "string"
                            },
                        }
                    },
                    "name": "stream1",
                    "source_defined_cursor": True,
                    "supported_sync_modes": ["full_refresh", "incremental"],
                }
            ]
        }
    )
    .set_expected_records(
        [
            {"data": {"data": {"col1": "val11a", "col2": "val12a"}, "_ab_source_file_last_modified": "2023-06-05T03:54:07Z",
                      "_ab_source_file_url": "a.csv"}, "stream": "stream1"},
            {"data": {"data": {"col1": "val21a", "col2": "val22a"}, "_ab_source_file_last_modified": "2023-06-05T03:54:07Z",
                      "_ab_source_file_url": "a.csv"}, "stream": "stream1"},
            {"data": {"data": {"col1": "val11b", "col2": "val12b", "col3": "val13b"}, "_ab_source_file_last_modified": "2023-06-05T03:54:07Z",
                      "_ab_source_file_url": "b.csv"}, "stream": "stream1"},
            {"data": {"data": {"col1": "val21b", "col2": "val22b", "col3": "val23b"}, "_ab_source_file_last_modified": "2023-06-05T03:54:07Z",
                      "_ab_source_file_url": "b.csv"}, "stream": "stream1"},
        ]
    )
).build()


schemaless_csv_multi_stream_scenario = (
    TestScenarioBuilder()
    .set_name("schemaless_csv_multi_stream_scenario")
    .set_config(
        {
            "streams": [
                {
                    "name": "stream1",
                    "file_type": "csv",
                    "globs": ["a.csv"],
                    "validation_policy": "skip_record",
                    "schemaless": True,
                },
                {
                    "name": "stream2",
                    "file_type": "csv",
                    "globs": ["b.csv"],
                    "validation_policy": "skip_record",
                }
            ]
        }
    )
    .set_files(
        {
            "a.csv": {
                "contents": [
                    ("col1", "col2"),
                    ("val11a", "val12a"),
                    ("val21a", "val22a"),
                ],
                "last_modified": "2023-06-05T03:54:07.000Z",
            },
            "b.csv": {
                "contents": [
                    ("col3",),
                    ("val13b",),
                    ("val23b",),
                ],
                "last_modified": "2023-06-05T03:54:07.000Z",
            },
        }
    )
    .set_file_type("csv")
    .set_expected_catalog(
        {
            "streams": [
                {
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "data": {
                                "type": "object"
                            },
                            "_ab_source_file_last_modified": {
                                "type": "string"
                            },
                            "_ab_source_file_url": {
                                "type": "string"
                            },
                        },
                    },
                    "name": "stream1",
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "source_defined_cursor": True,
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                },
                {
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col3": {
                                "type": "string"
                            },
                            "_ab_source_file_last_modified": {
                                "type": "string"
                            },
                            "_ab_source_file_url": {
                                "type": "string"
                            },
                        },
                    },
                    "name": "stream2",
                    "source_defined_cursor": True,
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "supported_sync_modes": ["full_refresh", "incremental"],
                },
            ]
        }
    )
    .set_expected_records(
        [
            {"data": {"data": {"col1": "val11a", "col2": "val12a"}, "_ab_source_file_last_modified": "2023-06-05T03:54:07Z",
                      "_ab_source_file_url": "a.csv"}, "stream": "stream1"},
            {"data": {"data": {"col1": "val21a", "col2": "val22a"}, "_ab_source_file_last_modified": "2023-06-05T03:54:07Z",
                      "_ab_source_file_url": "a.csv"}, "stream": "stream1"},
            {"data": {"col3": "val13b", "_ab_source_file_last_modified": "2023-06-05T03:54:07Z", "_ab_source_file_url": "b.csv"},
             "stream": "stream2"},
            {"data": {"col3": "val23b", "_ab_source_file_last_modified": "2023-06-05T03:54:07Z", "_ab_source_file_url": "b.csv"},
             "stream": "stream2"},
        ]
    )
).build()


schemaless_with_user_input_schema_fails_connection_check_scenario = (
    TestScenarioBuilder()
    .set_name("schemaless_with_user_input_schema_fails_connection_check_scenario")
    .set_config(
        {
            "streams": [
                {
                    "name": "stream1",
                    "file_type": "csv",
                    "globs": ["*"],
                    "validation_policy": "skip_record",
                    "input_schema": {"col1": "string", "col2": "string", "col3": "string"},
                    "schemaless": True,
                }
            ]
        }
    )
    .set_files(
        {
            "a.csv": {
                "contents": [
                    ("col1", "col2"),
                    ("val11a", "val12a"),
                    ("val21a", "val22a"),
                ],
                "last_modified": "2023-06-05T03:54:07.000Z",
            },
            "b.csv": {
                "contents": [
                    ("col1", "col2", "col3"),
                    ("val11b", "val12b", "val13b"),
                    ("val21b", "val22b", "val23b"),
                ],
                "last_modified": "2023-06-05T03:54:07.000Z",
            },
        }
    )
    .set_file_type("csv")
    .set_expected_catalog(
        {
            "streams": [
                {
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "data": {
                                "type": "object"
                            },
                            "_ab_source_file_last_modified": {
                                "type": "string"
                            },
                            "_ab_source_file_url": {
                                "type": "string"
                            },
                        }
                    },
                    "name": "stream1",
                    "source_defined_cursor": True,
                    "supported_sync_modes": ["full_refresh", "incremental"],
                }
            ]
        }
    )
    .set_expected_check_status("FAILED")
    .set_expected_check_error(ConfigValidationError, FileBasedSourceError.CONFIG_VALIDATION_ERROR.value)
    .set_expected_discover_error(ConfigValidationError, FileBasedSourceError.CONFIG_VALIDATION_ERROR.value)
    .set_expected_read_error(ConfigValidationError, FileBasedSourceError.CONFIG_VALIDATION_ERROR.value)
).build()


schemaless_with_user_input_schema_fails_connection_check_multi_stream_scenario = (
    TestScenarioBuilder()
    .set_name("schemaless_with_user_input_schema_fails_connection_check_multi_stream_scenario")
    .set_config(
        {
            "streams": [
                {
                    "name": "stream1",
                    "file_type": "csv",
                    "globs": ["a.csv"],
                    "validation_policy": "skip_record",
                    "schemaless": True,
                    "input_schema": {"col1": "string", "col2": "string", "col3": "string"},
                },
                {
                    "name": "stream2",
                    "file_type": "csv",
                    "globs": ["b.csv"],
                    "validation_policy": "skip_record",
                }
            ]
        }
    )
    .set_files(
        {
            "a.csv": {
                "contents": [
                    ("col1", "col2"),
                    ("val11a", "val12a"),
                    ("val21a", "val22a"),
                ],
                "last_modified": "2023-06-05T03:54:07.000Z",
            },
            "b.csv": {
                "contents": [
                    ("col3",),
                    ("val13b",),
                    ("val23b",),
                ],
                "last_modified": "2023-06-05T03:54:07.000Z",
            },
        }
    )
    .set_file_type("csv")
    .set_expected_catalog(
        {
            "streams": [
                {
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "data": {
                                "type": "object"
                            },
                            "_ab_source_file_last_modified": {
                                "type": "string"
                            },
                            "_ab_source_file_url": {
                                "type": "string"
                            },
                        },
                    },
                    "name": "stream1",
                    "supported_sync_modes": ["full_refresh", "incremental"],
                    "source_defined_cursor": True,
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                },
                {
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "col3": {
                                "type": "string"
                            },
                            "_ab_source_file_last_modified": {
                                "type": "string"
                            },
                            "_ab_source_file_url": {
                                "type": "string"
                            },
                        },
                    },
                    "name": "stream2",
                    "source_defined_cursor": True,
                    "default_cursor_field": ["_ab_source_file_last_modified"],
                    "supported_sync_modes": ["full_refresh", "incremental"],
                },
            ]
        }
    )
    .set_expected_check_status("FAILED")
    .set_expected_check_error(ConfigValidationError, FileBasedSourceError.CONFIG_VALIDATION_ERROR.value)
    .set_expected_discover_error(ConfigValidationError, FileBasedSourceError.CONFIG_VALIDATION_ERROR.value)
    .set_expected_read_error(ConfigValidationError, FileBasedSourceError.CONFIG_VALIDATION_ERROR.value)
).build()
