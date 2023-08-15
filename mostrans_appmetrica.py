import os
from datetime import datetime
import csv
from CronBase import CronBase

DT = datetime.now()
path_file = os.path.realpath(__file__)


class MostransAppmetrica(CronBase):
	def get_params_loader(self, request_type, phone_type_id, metric, row_ids):
		"""
		получение параметров из конфига для запросов 1ого и 2ого типов
		:param request_type:
		:param phone_type_id:
		:param metric:
		:param row_ids:
		:return:
		"""
		headers = self.config["headers"]
		url = self.config["url"]

		# Мне кажется это тоже костыль какой-то
		if request_type == "second_request_type":
			if [None] in row_ids:
				row_ids = str(row_ids).replace("[None]", "[null]")
			row_ids = str(row_ids).replace(" ", "").replace("\'", "\"")

		# Получаем общие параметры в зависимости от типа запроса
		params = {
			**self.config["common_params"],
			**self.config[request_type],
			"date1": self.date_from,
			"date2": self.date_to,
			"id": phone_type_id,
			"metrics": metric,
			"row_ids": row_ids
		}

		request_params = dict(url=url, headers=headers, params=params)
		main_params = dict(request_params=request_params)
		return main_params

	def read_json(self, request_type, load_data):
		"""
		парсим JSON для запросов 1ого и 2ого типов
		:param request_type:
		:param load_data:
		:return:
		"""
		data = []
		# Если JSON возвращен запросом 1ого типа
		if request_type == "first_request_type":
			data.append(load_data['query']['date1'])
			data.append(load_data['query']['date2'])
			data.append(load_data['time_intervals'][0][0])
			data.append(load_data['query']['ids'][0])
			data.append(load_data['query']['metrics'][0])
			data.append(load_data['totals'][0][0])

		# Если JSON возвращен запросом 2ого типа
		elif request_type == "second_request_type":
			for i in range(len(load_data['data']) - 1):
				for j in range(24):
					a = [
						load_data['query']['date1'],
						load_data['query']['date2'],
						load_data['time_intervals'][j][0],
						load_data['query']['ids'][0],
						load_data['data'][i]['dimensions'][0]['name'],
						load_data['data'][i]['metrics'][0][j]]
					data.append(a)
		return data

	def load_data(self, request_type, phone_type_id, metric, row_ids):
		"""
		Формируем параметры запроса и парсим полученный JSON
		:param request_type: тип запроса
		:param phone_type_id: тип мобильного устройства
		:param metric:
		:param row_ids:
		:return:
		"""
		main_params = self.get_params_loader(request_type, phone_type_id, metric, row_ids)
		params = self.load_request(main_params)
		response = params["response"]
		load_data = response.json()
		data = self.read_json(request_type, load_data)
		return data

	def write_csv(self, data, file_path):
		"""

		:param data:
		:param file_path:
		:return:
		"""
		columns = self.config["csv_headers"]
		with open(file_path, 'w', newline='') as f:
			wr = csv.writer(f)
			wr.writerow(columns)
			wr.writerows(data)

	def main_logic(self):
		data_list = []
		phone_type_ids = self.config["phone_type_ids"]
		metrics = self.config["metrics"][0]
		row_ids_list = self.config["row_ids"]

		for phone_type_id in phone_type_ids:
			# запросы 1-ого типа для каждого id и 2-ух метрик формируем по запросу (avgSessionDuration и sessionsPerUser)
			row_ids = []
			for metric in metrics:
				data = self.load_data("first_request_type", phone_type_id, metric, row_ids)
				data_list.append(data)

			# запросы 2-ого типа для каждого id и 2-ух наборов метрик формируем по запросу (42 метрики разбитые на два списка)
			metric = self.config["metrics"][1][0]
			for row_ids in row_ids_list:
				data = self.load_data("second_request_type", phone_type_id, metric, row_ids)
				data_list.extend(data)
		return data_list

	def run(self):
		try:
			files_dir = os.path.join(self.get_parent_path(), 'files', self.config['file_dir'])
			file_name_csv = self.config['file_name'] + '_{}.csv'.format(DT.strftime(self.config["date_format"]))
			file_csv = os.path.join(files_dir, file_name_csv)
			data = self.main_logic()
			self.write_csv(data, file_csv)
			self.zip_source(files_dir, file_name_csv)
		except Exception as err:
			# self.log.error_msg(f'{err}')
			pass
		finally:
			# self.log.end_execute()
			# self.log.stop()
			pass


if __name__ == '__main__':
	Main = MostransAppmetrica(path_file)
	Main.run()
