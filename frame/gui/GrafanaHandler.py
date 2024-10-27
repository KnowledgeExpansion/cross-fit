from frame.base.CompBase import *
import requests
import json


class GrafanaHandler(CompBase):
    def __init__(self, grafana_url, grafana_token):
        CompBase.__init__(self, log_level=CompBase.LOG_DEBUG)
        """
        GrafanaHandler 클래스 초기화

        :param grafana_url: Grafana 서버 URL (예: "http://localhost:3000")
        :param grafana_token: Grafana API 토큰 (읽기/쓰기 권한이 필요)
        """
        self.grafana_url = grafana_url
        self.grafana_token = grafana_token
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.grafana_token}"
        }

    def add_influxdb_data_source(self, influx_url, influx_token, org, bucket):
        """
        Grafana에 InfluxDB 데이터 소스를 추가하는 메서드

        :param influx_url: InfluxDB 서버 URL (예: "http://localhost:8086")
        :param influx_token: InfluxDB API 토큰
        :param org: InfluxDB 조직 이름
        :param bucket: InfluxDB 버킷 이름
        :return: 요청 결과
        """
        data_source = {
            "name": "InfluxDB",
            "type": "influxdb",
            "url": influx_url,
            "access": "proxy",
            "basicAuth": False,
            "jsonData": {
                "organization": org,
                "defaultBucket": bucket,
                "token": influx_token
            }
        }

        response = requests.post(
            f"{self.grafana_url}/api/datasources",
            headers=self.headers,
            data=json.dumps(data_source)
        )

        if response.status_code == 200:
            print("InfluxDB 데이터 소스가 성공적으로 Grafana에 추가되었습니다.")
        else:
            print(f"데이터 소스 추가 실패: {response.content}")

        return response

    def create_dashboard(self, dashboard_title, measurement, field, bucket, time_range="-1h"):
        """
        Grafana에서 InfluxDB 데이터를 시각화할 수 있는 대시보드를 생성하는 메서드

        :param dashboard_title: 대시보드 제목
        :param measurement: InfluxDB에서 사용할 measurement 이름 (예: "sensor_data")
        :param field: 시각화할 필드 이름 (예: "temperature")
        :param bucket: 데이터가 저장된 InfluxDB 버킷 이름
        :param time_range: 데이터 조회 시간 범위 (기본값은 최근 1시간 "-1h")
        :return: 요청 결과
        """
        dashboard = {
            "dashboard": {
                "id": None,
                "uid": None,
                "title": dashboard_title,
                "tags": ["influxdb", "auto-generated"],
                "timezone": "browser",
                "panels": [
                    {
                        "title": f"{field.capitalize()} Over Time",
                        "type": "timeseries",
                        "datasource": "InfluxDB",
                        "targets": [
                            {
                                "query": f'from(bucket: "{bucket}") '
                                         f'|> range(start: {time_range}) '
                                         f'|> filter(fn: (r) => r["_measurement"] == "{measurement}") '
                                         f'|> filter(fn: (r) => r["_field"] == "{field}")',
                                "refId": "A",
                            }
                        ],
                        "fieldConfig": {
                            "overrides": [],
                            "defaults": {
                                "unit": "celsius",
                                "decimals": 2
                            }
                        },
                        "gridPos": {
                            "h": 10,
                            "w": 24,
                            "x": 0,
                            "y": 0
                        }
                    }
                ],
                "schemaVersion": 30,
                "version": 1
            },
            "overwrite": True
        }

        response = requests.post(
            f"{self.grafana_url}/api/dashboards/db",
            headers=self.headers,
            data=json.dumps(dashboard)
        )

        if response.status_code == 200:
            print("대시보드가 성공적으로 Grafana에 추가되었습니다.")
        else:
            print(f"대시보드 추가 실패: {response.content}")

        return response


if __name__ == '__main__':
    # Grafana 및 InfluxDB 설정
    grafana_url = "http://localhost:3000"
    grafana_token = "your_grafana_api_token"
    influx_url = "http://localhost:8086"
    influx_token = "your_influxdb_token"
    org = "your_org"
    bucket = "your_bucket_name"

    # GrafanaHandler 객체 생성
    grafana_handler = GrafanaHandler(grafana_url, grafana_token)

    # InfluxDB 데이터 소스 추가
    grafana_handler.add_influxdb_data_source(influx_url, influx_token, org, bucket)

    # 대시보드 생성
    dashboard_title = "InfluxDB Sensor Data Dashboard"
    measurement = "sensor_data"
    field = "temperature"

    grafana_handler.create_dashboard(dashboard_title, measurement, field, bucket)
