# 在官方镜像上安装本仓库 ETL 依赖（构建上下文为仓库根目录）
FROM apache/airflow:2.10.4

USER root
COPY requirements.txt /opt/airflow/requirements-etl.txt
RUN chown 50000:0 /opt/airflow/requirements-etl.txt

USER airflow
RUN pip install --no-cache-dir -r /opt/airflow/requirements-etl.txt
