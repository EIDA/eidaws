# -*- coding: utf-8 -*-

import asyncio
import datetime
import errno
import io
import struct

from eidaws.federator.fdsnws_dataselect.miniseed.parser import DataselectSchema
from eidaws.federator.settings import (
    FED_BASE_ID,
    FED_DATASELECT_MINISEED_SERVICE_ID,
)
from eidaws.federator.utils.process import UnsortedResponse
from eidaws.federator.utils.worker import (
    BaseSplitAlignWorker,
    WorkerError,
)


FIXED_DATA_HEADER_SIZE = 48
MINIMUM_RECORD_LENGTH = 256
DATA_ONLY_BLOCKETTE_NUMBER = 1000


class MiniseedParsingError(WorkerError):
    """Error while parsing miniseed data: {}"""


def _get_mseed_record_size(fd):
    """
    Extract the *MiniSEED* record length from a file-like object.

    .. note::
        Taken from `fdsnws_fetch <https://github.com/andres-h/fdsnws_scripts>_`.
    """

    # read fixed header
    buf = fd.read(FIXED_DATA_HEADER_SIZE)
    if not buf:
        raise MiniseedParsingError("Missing data.")

    # get offset of data (value before last, 2 bytes, unsigned short)
    data_offset_idx = FIXED_DATA_HEADER_SIZE - 4
    try:
        (data_offset,) = struct.unpack(
            b"!H", buf[data_offset_idx : data_offset_idx + 2]
        )
    except struct.error as err:
        MiniseedParsingError(f"Error while reading data offset: {err}")

    if data_offset >= FIXED_DATA_HEADER_SIZE:
        remaining_header_size = data_offset - FIXED_DATA_HEADER_SIZE

    elif data_offset == 0:
        # This means that blockettes can follow, but no data samples. Use
        # minimum record size to read following blockettes. This can still fail
        # if blockette 1000 is after position 256
        remaining_header_size = MINIMUM_RECORD_LENGTH - FIXED_DATA_HEADER_SIZE

    else:
        # Full header size cannot be smaller than fixed header size. This is an
        # error.
        raise MiniseedParsingError(
            f"Data offset smaller than fixed header length: {data_offset}"
        )

    buf = fd.read(remaining_header_size)
    if not buf:
        raise MiniseedParsingError("Remaining header corrupt in record")

    # scan variable header for blockette 1000
    blockette_start = 0
    b1000_found = False
    try:
        while blockette_start < remaining_header_size:

            # 2 bytes, unsigned short
            (blockette_id,) = struct.unpack(
                b"!H", buf[blockette_start : blockette_start + 2]
            )

            # get start of next blockette (second value, 2 bytes, unsigned short)
            (next_blockette_start,) = struct.unpack(
                b"!H", buf[blockette_start + 2 : blockette_start + 4]
            )

            if blockette_id == DATA_ONLY_BLOCKETTE_NUMBER:

                b1000_found = True
                break

            elif next_blockette_start == 0:
                # no blockettes follow
                break

            else:
                blockette_start = next_blockette_start
    except struct.error:
        pass

    # blockette 1000 not found
    if not b1000_found:
        raise MiniseedParsingError("Blockette 1000 not found")

    # get record size (1 byte, unsigned char)
    record_size_exponent_idx = blockette_start + 6
    (record_size_exponent,) = struct.unpack(
        b"!B", buf[record_size_exponent_idx : record_size_exponent_idx + 1]
    )

    return 2 ** record_size_exponent


class _DataselectWorker(BaseSplitAlignWorker):
    """
    A worker task implementation for ``fdsnws-dataselect`` ``format=miniseed``.
    The worker implements splitting and aligning facilities.

    When splitting and aligning (i.e. merging potentially occurring overlaps)
    data is downloaded sequentially. Note, that a worker assumes the MiniSEED
    data to be shipped with a uniform record length (with respect to a stream
    epoch initially requested).

    .. note::

        The implementation strongly relies on the availability of ``blockette
        1000`` (i.e. data only miniseed blockette). For additional information
        see also the `SEED Reference Manual
        <http://www.fdsn.org/pdf/SEEDManual_V2.4.pdf>`_.
    """

    SERVICE_ID = FED_DATASELECT_MINISEED_SERVICE_ID
    QUERY_PARAM_SERIALIZER = DataselectSchema

    LOGGER = ".".join([FED_BASE_ID, SERVICE_ID, "worker"])

    # minimum chunk size; the chunk size must be aligned with the mseed
    # record_size
    _CHUNK_SIZE = MINIMUM_RECORD_LENGTH

    def __init__(
        self, request, session, drain, lock=None, **kwargs,
    ):
        super().__init__(
            request, session, drain, lock=lock, **kwargs,
        )

        self._mseed_record_size = None

    async def _write_response_to_buffer(self, resp, buf):
        last_record = None
        await buf.seek(0, 2)
        if await buf.tell():
            try:
                await buf.seek(-self._mseed_record_size, 2)
            except OSError as err:
                if err.errno == errno.EINVAL:
                    await buf.seek(0)
                else:
                    raise

            last_record = await buf.read()

        while True:
            try:
                chunk = await resp.content.read(self._chunk_size)
            except asyncio.TimeoutError as err:
                self.logger.warning(f"Socket read timeout: {type(err)}")
                break

            if not chunk:
                break

            if not self._mseed_record_size:
                try:
                    self._mseed_record_size = _get_mseed_record_size(
                        io.BytesIO(chunk)
                    )
                except MiniseedParsingError as err:

                    msg = f"{err}; stop reading."
                    fallback = self.config["fallback_mseed_record_size"]
                    if not fallback:
                        self.logger.warning(f"{msg}")
                        break

                    self.logger.info(f"{msg}")
                    self.logger.debug(
                        f"Using fallback miniseed record size: {fallback} "
                        "bytes"
                    )
                    self._mseed_record_size = fallback
                finally:
                    if self._mseed_record_size:
                        # align chunk_size with mseed record_size
                        self._chunk_size = self._mseed_record_size

            if last_record is not None:
                if last_record in chunk:
                    chunk = chunk[self._mseed_record_size :]
                last_record = None

            await buf.write(chunk)

    async def finalize(self):
        self._mseed_record_size = None
        self._chunk_size = self._CHUNK_SIZE


class DataselectRequestProcessor(UnsortedResponse):

    SERVICE_ID = FED_DATASELECT_MINISEED_SERVICE_ID
    ACCESS = "open"

    LOGGER = ".".join([FED_BASE_ID, SERVICE_ID, "process"])

    @property
    def content_type(self):
        return "application/vnd.fdsn.mseed"

    async def _prepare_response(self, response):
        response.content_type = self.content_type
        response.headers["Content-Disposition"] = (
            'attachment; filename="'
            + FED_BASE_ID.replace(".", "-")
            + "-"
            + datetime.datetime.utcnow().isoformat()
            + '.mseed"'
        )
        await response.prepare(self.request)

    def _create_worker(self, request, session, drain, lock=None, **kwargs):
        return _DataselectWorker(
            self.request,
            session,
            drain,
            lock=lock,
            endtime=self._default_endtime,
            **kwargs,
        )
