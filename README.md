# airflow-victoria-logs-task-handler
VictoriaLogsTaskHandler for airflow

## Example usage
* install apache-airflow-providers-opensearch = "1.7.0" or later
* copy vl_logging_config_template.py to vl_logging_config.py
* place files vl_logging_config.py and vl_task_handler.py in config folder (or plugins) (only in worker and webserver containers)
* in vl_logging_config.py
  * fix vl_url
  * fix query
* add env to compose/k8s
  * AIRFLOW__LOGGING__REMOTE_LOGGING: 'false'
    AIRFLOW__LOGGING__LOGGING_CONFIG_CLASS: vl_logging_config.LOGGING_CONFIG
