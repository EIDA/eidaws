# -*- coding: utf-8 -*-
"""
Federator API schema definition for ``eidaws-wfcatalog``.
"""

import functools

from marshmallow import validate, fields, pre_load

from eidaws.federator.utils.parser import ServiceSchema
from eidaws.utils.schema import (
    _merge_fields,
    FDSNWSBool,
    NotEmptyFloat,
    NotEmptyString,
    NotEmptyInt,
    Percentage,
)


Quality = functools.partial(
    fields.Str, validate=validate.OneOf(["D", "R", "Q", "M", "B"])
)


def _WFCatalogSchema():

    METRIC_FIELDS = (
        # sample options
        ("num_samples", NotEmptyInt),
        ("sample_max", NotEmptyInt),
        ("sample_min", NotEmptyInt),
        ("sample_mean", NotEmptyInt),
        ("sample_stdev", NotEmptyFloat),
        ("sample_rms", NotEmptyFloat),
        ("sample_median", NotEmptyFloat),
        ("sample_lower_quartil", NotEmptyFloat),
        ("sample_upper_quartile", NotEmptyFloat),
        ("num_gaps", NotEmptyInt),
        ("num_overlaps", NotEmptyInt),
        ("max_gap", NotEmptyFloat),
        ("max_overlap", NotEmptyFloat),
        ("sum_gaps", NotEmptyFloat),
        ("sum_overlaps", NotEmptyFloat),
        ("percent_availability", Percentage),
        # timing quality options
        ("timing_quality_mean", NotEmptyFloat),
        ("timing_quality_median", NotEmptyFloat),
        ("timing_quality_lower_quartile", NotEmptyFloat),
        ("timing_quality_upper_quartile", NotEmptyFloat),
        ("timing_quality_max", NotEmptyFloat),
        ("timing_quality_min", NotEmptyFloat),
        ("timing_correction", Percentage),
        # MSEED header options
        ("amplifier_saturation", Percentage),
        ("digitizer_clipping", Percentage),
        ("spikes", Percentage),
        ("glitches", Percentage),
        ("missing_padded_data", Percentage),
        ("telemetry_sync_error", Percentage),
        ("digital_filter_charging", Percentage),
        ("suspect_time_tag", Percentage),
        ("calibration_signal", Percentage),
        ("time_correction_applied", Percentage),
        ("event_begin", Percentage),
        ("event_end", Percentage),
        ("positive_leap", Percentage),
        ("negative_leap", Percentage),
        ("event_in_progress", Percentage),
        ("station_volume", Percentage),
        ("long_record_read", Percentage),
        ("short_record_read", Percentage),
        ("start_time_series", Percentage),
        ("end_time_series", Percentage),
        ("clock_locked", Percentage),
        # ("activity_flags", Percentage),
        # ("io_and_clock_flags", Percentage),
    )

    def _make_metric_field(field_name, field_type):
        METRIC_SUFFIXES = ["", "_eq", "_gt", "_ge", "_lt", "_le", "_ne"]

        for suffix in METRIC_SUFFIXES:
            setattr(API, field_name + suffix, field_type)

    class API(ServiceSchema):
        """
        WFCatalog webservice API definition

        The parameters defined correspond to the definition
        `<https://www.orfeus-eu.org/documents/WFCatalog_Specification-v0.22.pdf>`_.
        """

        # NOTE(damb): starttime and endtime are required for this schema; for GET
        # requests the extistance of these parameters must be verified, manually

        csegments = FDSNWSBool(missing="false")
        format = fields.Str(missing="json", validate=validate.OneOf(["json"]))
        granularity = fields.Str(missing="day")
        gran = fields.Str(load_only=True)
        include = fields.Str(
            missing="default",
            validate=validate.OneOf(["default", "sample", "header", "all"]),
        )
        longestonly = FDSNWSBool(missing="false")
        # TODO(damb): check with a current WFCatalog webservice
        # minimumlength = fields.Float(missing=0.)
        minimumlength = NotEmptyFloat()

        # record options
        encoding = NotEmptyString()
        num_records = NotEmptyInt()
        quality = Quality()
        record_length = NotEmptyInt()
        # sample_rate = NotEmptyFloat()
        sample_rate = fields.Float(as_string=True)

        @pre_load
        def merge_keys(self, data, **kwargs):
            """
            Merge alternative field parameter values.

            .. note::

                The default :py:mod:`webargs` parser does not provide this feature
                by default such that ``data_key`` field parameters are exclusively
                parsed.
            """
            _mappings = [
                ("gran", "granularity"),
            ]

            _merge_fields(data, _mappings)
            return data

        class Meta:
            service = "wfcatalog"
            strict = True
            ordered = True

    for _field, _type in METRIC_FIELDS:
        _make_metric_field(_field, _type)

    return API


WFCatalogSchema = _WFCatalogSchema()
