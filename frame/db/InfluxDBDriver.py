from frame.base.CompBase import *
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import datetime


class InfluxDBHandler(CompBase):
    def __init__(self, url, token, org, bucket):
        CompBase.__init__(self, log_level=CompBase.LOG_DEBUG)
        self.url = url
        self.token = token
        self.org = org
        self.bucket = bucket
        self.client = InfluxDBClient(url=self.url, token=self.token, org=self.org)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.query_api = self.client.query_api()

    def write_data(self, measurement, tags, fields, timestamp=None):
        """
        데이터 포인트를 InfluxDB에 기록하는 메서드

        :param measurement: InfluxDB의 measurement 이름
        :param tags: 태그 딕셔너리 (예: {"location": "office"})
        :param fields: 필드 딕셔너리 (예: {"temperature": 23.5, "humidity": 60})
        :param timestamp: 타임스탬프 (기본값은 현재 UTC 시간)
        """
        point = Point(measurement)

        # 태그 추가
        for tag, value in tags.items():
            point.tag(tag, value)

        # 필드 추가
        for field, value in fields.items():
            point.field(field, value)

        # 타임스탬프 설정
        if timestamp:
            point.time(timestamp)
        else:
            point.time(datetime.datetime.utcnow())

        # 데이터 쓰기
        self.write_api.write(bucket=self.bucket, record=point)
        self.i('Measure Data is normally loaded: {}'.format(measurement))

    def query_data(self, measurement, start="-1h", stop="now()", filters=None):
        """
        InfluxDB에서 데이터를 쿼리하는 메서드

        :param measurement: 검색할 measurement 이름
        :param start: 검색 시작 시간 (예: "-1h", "-1d")
        :param stop: 검색 종료 시간 (기본값은 "now()")
        :param filters: 필터 딕셔너리 (예: {"location": "office"})
        :return: 쿼리 결과 리스트
        """
        filter_conditions = [f'r["_measurement"] == "{measurement}"']

        if filters:
            for key, value in filters.items():
                filter_conditions.append(f'r["{key}"] == "{value}"')

        query = f'''
            from(bucket: "{self.bucket}")
            |> range(start: {start}, stop: {stop})
            |> filter(fn: (r) => {" and ".join(filter_conditions)})
        '''

        result = self.query_api.query(query)
        data = []
        for table in result:
            for record in table.records:
                data.append({
                    "time": record.get_time(),
                    "measurement": record.get_measurement(),
                    **record.values
                })

        return data

    def close(self):
        """클라이언트 종료 메서드"""
        self.client.close()


if __name__ == '__main__':
    # 설정 정보
    url = "http://localhost:8086"
    token = "your_influxdb_token"
    org = "your_org"
    bucket = "your_bucket_name"

    # InfluxDBHandler 객체 생성
    db_handler = InfluxDBHandler(url, token, org, bucket)

    # 데이터 쓰기
    measurement = "sensor_data"
    tags = {"location": "office"}
    fields = {"temperature": 22.5, "humidity": 55}

    db_handler.write_data(measurement, tags, fields)

    # DB Query Example
    # # 최근 1시간 동안의 데이터를 쿼리
    # measurement = "sensor_data"
    # start_time = "-1h"
    # filters = {"location": "office"}
    #
    # data = db_handler.query_data(measurement, start=start_time, filters=filters)
    #
    # # 결과 출력
    # for entry in data:
    #     print(entry)
    #
    # # 클라이언트 종료
    # db_handler.close()

