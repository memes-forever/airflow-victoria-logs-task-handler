from copy import deepcopy

from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG

# --- СЕКЦИЯ КОНФИГУРАЦИИ ЛОГИРОВАНИЯ ---
LOGGING_CONFIG = deepcopy(DEFAULT_LOGGING_CONFIG)
LOGGING_CONFIG["handlers"]["task"].update(
    {
        # С учетом вашего PYTHONPATH импортируем класс напрямую из модуля vl_task_handler
        "class": "vl_task_handler.VictoriaLogsTaskHandler",
        #
        # Параметры victoria logs
        'vl_url': (vl_url := "https://victoria-logs-ui.company.com"),
        'vl_timeout': 60 * 5,
        'frontend': vl_url
        + '/select/vmui/#/?g0.range_input=30m&g0.relative_time=last_30_minutes&projectID=0&accountID=0&query={query}&tab=0&step=1m&limit=500&view=group',
        'query': 'cluster:="cluster_name" AND namespace:="airflow" AND container:="worker" AND _msg:"{log_id}" | unpack_json | filter offset:>{offset} | sort by (_time) asc',
        'query_start': '-1d',
        'query_end': 'now',
        #
        # Параметры-заглушки для базового класса
        "end_of_log_mark": "end_of_log",
        'write_stdout': True,
        'json_format': True,
        "json_fields": 'asctime, filename, lineno, levelname, message',
        #
        # Параметры-заглушки для Opensearch
        "host": "localhost",
        "port": 9200,
        'username': '',
        'password': '',
    }
)
