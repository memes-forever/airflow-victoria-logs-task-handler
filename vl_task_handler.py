from __future__ import annotations

import json
from datetime import timedelta
from operator import attrgetter
from typing import TYPE_CHECKING, Union, cast
from urllib.parse import quote

import pendulum
import requests
from airflow.providers.opensearch.log.os_response import Hit, OpensearchResponse
from airflow.providers.opensearch.log.os_task_handler import OpensearchTaskHandler
from airflow.providers.opensearch.version_compat import AIRFLOW_V_3_0_PLUS
from airflow.utils import timezone

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstance
    from airflow.utils.log.file_task_handler import LogMetadata

if AIRFLOW_V_3_0_PLUS:
    # pylint: disable=no-name-in-module
    from airflow.utils.log.file_task_handler import StructuredLogMessage

    OsLogMsgType = Union[list[StructuredLogMessage], str]
else:
    OsLogMsgType = list[tuple[str, str]]  # type: ignore[misc]


# for ES
# class LogModel:
#     def __init__(self, **kwargs):
#         for k, v in kwargs.items():
#             setattr(self, k, v)


# pylint: disable=too-many-positional-arguments, too-many-arguments, too-many-branches
class VictoriaLogsTaskHandler(OpensearchTaskHandler):
    """
    Task handler, который пишет логи в VictoriaLogs и читает их оттуда для UI.
    """

    PAGE = 0
    MAX_LINE_PER_PAGE = 500
    LOG_NAME = "VictoriaLogs"

    def __init__(
        self,
        vl_url: str,
        vl_timeout: int,
        frontend: str,
        query: str,
        query_start: str = '-2d',
        query_end: str = 'now',
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.vl_url = vl_url
        self.vl_timeout = vl_timeout
        self.frontend = frontend
        self.query = query
        self.query_start = query_start
        self.query_end = query_end

    @staticmethod
    def _fmt_dt(dt, add_hours=None):
        """datetime → '2026-09-15T14:20:22.147Z'"""
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(timezone.utc)
        if add_hours is not None:
            dt = dt + timedelta(hours=add_hours)
        # %f даёт микросекунды, обрезаем до миллисекунд
        return dt.strftime('%Y-%m-%dT%H:%M:%S.') + f'{dt.microsecond // 1000:03d}Z'

    def vl_read(self, log_id, offset, ti):
        start = self._fmt_dt(getattr(ti, 'start_date', None), add_hours=-24) or self.query_start
        end = self._fmt_dt(getattr(ti, 'end_date', None), add_hours=24) or self.query_end

        data = {
            "query": self.query.format(log_id=log_id, offset=offset),
            "limit": self.MAX_LINE_PER_PAGE,
            "start": start,
            "end": end,
        }
        self.log.warning(f'Data to victoria logs: {str(data)}')

        res = requests.post(
            self.vl_url + '/select/logsql/query',
            data=data,
            headers={
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:155.0) Gecko/20100101 Firefox/155.0',
            },
            timeout=self.vl_timeout,
        )
        res.raise_for_status()

        logs = []
        for line in res.text.strip().split("\n"):
            if line:
                log_data = json.loads(line)
                _msg = json.loads(log_data.get('_msg', '{}'))

                # for ES
                # logs.append(LogModel(**{**log_data, **_msg}))

                # for OS
                logs.append({"_source": {**log_data, **_msg}})

        # for ES
        # return logs

        # for OS
        return OpensearchResponse(
            self,
            {
                'hits': {
                    # 'total': len(logs),
                    'hits': logs,
                }
            },
        )

    def es_read(self, log_id, offset, ti):
        return self.vl_read(log_id, offset, ti)

    def _os_read(self, log_id, offset, ti):
        return self.vl_read(log_id, offset, ti)

    def _read(
        self, ti: TaskInstance, try_number: int, metadata: LogMetadata | None = None
    ) -> tuple[OsLogMsgType, LogMetadata]:
        """
        Endpoint for streaming log.

        :param ti: task instance object
        :param try_number: try_number of the task instance
        :param metadata: log metadata,
                         can be used for steaming log reading and auto-tailing.
        :return: a list of tuple with host and log documents, metadata.
        """
        # In Airflow 3 logs reach OpenSearch only after the task finishes, so a running task has
        # nothing to read there yet. Defer to the base handler for live worker/executor logs, as
        # S3/GCS do.
        # if (
        #     AIRFLOW_V_3_0_PLUS
        #     and ti.try_number == try_number
        #     and ti.state in (TaskInstanceState.RUNNING, TaskInstanceState.DEFERRED)
        # ):
        #     return super()._read(ti, try_number, metadata)  # type: ignore[return-value]

        if not metadata:
            # LogMetadata(TypedDict) is used as type annotation for log_reader; added ignore to suppress mypy error
            metadata = {"offset": 0}  # type: ignore[assignment]
        metadata = cast("LogMetadata", metadata)

        if "offset" not in metadata:
            metadata["offset"] = 0

        offset = metadata["offset"]
        log_id = self._render_log_id(ti, try_number)
        response = self.vl_read(log_id, offset, ti)
        if response is not None and response.hits:
            logs_by_host = self._group_logs_by_host(response)
            next_offset = attrgetter(self.offset_field)(response[-1])
        else:
            logs_by_host = None
            next_offset = offset

        # Ensure a string here. Large offset numbers will get JSON.parsed incorrectly
        # on the client. Sending as a string prevents this issue.
        # https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Number/MAX_SAFE_INTEGER
        metadata["offset"] = str(next_offset)

        # end_of_log_mark may contain characters like '\n' which is needed to
        # have the log uploaded but will not be stored in elasticsearch.
        metadata["end_of_log"] = False
        if logs_by_host:
            if any(self._get_log_message(x[-1]) == self.end_of_log_mark for x in logs_by_host.values()):
                metadata["end_of_log"] = True

        cur_ts = pendulum.now()
        if "last_log_timestamp" in metadata:
            last_log_ts = timezone.parse(metadata["last_log_timestamp"])

            # if we are not getting any logs at all after more than N seconds of trying,
            # assume logs do not exist
            if int(next_offset) == 0 and cur_ts.diff(last_log_ts).in_seconds() > 5:
                metadata["end_of_log"] = True
                missing_log_message = (
                    f"*** Log {log_id} not found in {self.LOG_NAME}. "
                    "If your task started recently, please wait a moment and reload this page. "
                    "Otherwise, the logs for this task instance may have been removed."
                )
                if AIRFLOW_V_3_0_PLUS:
                    # pylint: disable=no-name-in-module, import-outside-toplevel, redefined-outer-name
                    from airflow.utils.log.file_task_handler import StructuredLogMessage

                    # return list of StructuredLogMessage for Airflow 3.0+
                    return [StructuredLogMessage(event=missing_log_message)], metadata

                return [("", missing_log_message)], metadata  # type: ignore[list-item]
            if (
                # Assume end of log after not receiving new log for N min,
                cur_ts.diff(last_log_ts).in_minutes() >= 5
                # if max_offset specified, respect it
                or ("max_offset" in metadata and int(offset) >= int(metadata["max_offset"]))
            ):
                metadata["end_of_log"] = True

        if int(offset) != int(next_offset) or "last_log_timestamp" not in metadata:
            metadata["last_log_timestamp"] = str(cur_ts)

        # If we hit the end of the log, remove the actual end_of_log message
        # to prevent it from showing in the UI.
        if logs_by_host:
            if AIRFLOW_V_3_0_PLUS:
                # pylint: disable=no-name-in-module, import-outside-toplevel
                from airflow.utils.log.file_task_handler import StructuredLogMessage

                header = [
                    StructuredLogMessage(event="::group::Log message source details"),
                    *[StructuredLogMessage(event=host) for host in logs_by_host.keys()],
                    StructuredLogMessage(event="::endgroup::"),
                ]

                # Flatten all hits, filter to only desired fields, and construct StructuredLogMessage objects
                # message = header + [
                #     _safe_build_structured_log_message(hit.to_dict())
                #     for hits in logs_by_host.values()
                #     for hit in hits
                # ]
                message = header + [
                    StructuredLogMessage(event=self.concat_logs(hits)) for hits in logs_by_host.values()
                ]
            else:
                message = [(host, self.concat_logs(hits)) for host, hits in logs_by_host.items()]  # type: ignore[misc]
        else:
            message = []
            metadata["end_of_log"] = True
        return message, metadata

    def concat_logs(self, hits: list[Hit]) -> str:
        log_range = (len(hits) - 1) if self._get_log_message(hits[-1]) == self.end_of_log_mark else len(hits)
        return "\n".join(self._format_msg(hits[i]) for i in range(log_range))

    @staticmethod
    def _get_log_message(hit: Hit) -> str:
        if hasattr(hit, "event"):
            return hit.event
        if hasattr(hit, "message"):
            return hit.message
        return ""

    def get_external_log_url(self, task_instance: TaskInstance, try_number: int) -> str:
        """
        Creates an address for an external log collecting service.

        :param task_instance: task instance object
        :param try_number: task instance try_number to read logs from.
        :return: URL to the external log collection service
        """
        log_id = self._render_log_id(task_instance, try_number)
        return self.frontend.format(query=quote(self.query.format(log_id=log_id)))

    @property
    def supports_external_link(self) -> bool:
        """Whether we can support external links."""
        return bool(self.frontend)

    @property
    def log_name(self) -> str:
        """The log name."""
        return self.LOG_NAME

    def _read_remote_logs(self, ti, try_number, metadata=None) -> tuple[list[str], list[str]]:
        """
        Implement in subclasses to read from the remote service.

        This method should return two lists, messages and logs.

        * Each element in the messages list should be a single message,
          such as, "reading from x file".
        * Each element in the logs list should be the content of one file.
        """
        raise NotImplementedError
