import os
from datetime import datetime, timedelta
import csv
from CronBase import CronBase




DT = datetime.now()
path_file = os.path.realpath(__file__)

class mostrans_appmetrica(CronBase):
	
	#получение параметров из конфига для запросов 1ого и 2ого типов
	def get_params_loader(self, request_type, id, parametr):
		params = {}
		headers = self.config["headers"]
		url = self.config["url"]
		self.timeout = self.config["timeout"]

		#Получаем общие параметры взависимости от типа запроса
		params = {**self.config["request_params"]["common"], **self.config["request_params"][request_type]}
		params["date1"] = self.date_from
		params["date2"] = self.date_to

		#Получаем изменяемые параметры
		#для запроса 1ого типа
		if request_type == "first_request_type":
			params["id"] = id
			params["metrics"] = parametr
			params["row_ids"] = self.config["row_ids"][0]
		#для запроса 2ого типа
		elif request_type == "second_request_type":
			params["id"] = id
			params["metrics"] = self.config["metrics"][1][0]
			self.count_metrics = len(parametr)
			if [None] in parametr:
				row_ids = str(parametr).replace("[None]", "[null]").replace(" ", "").replace("\'", "\"")
			else:
				row_ids = str(parametr).replace(" ", "").replace("\'", "\"") 
			params["row_ids"] = row_ids
		request_params = dict(url = url, headers = headers, params = params)
		main_params = dict(request_params = request_params)
		return main_params 
			
	#парсим JSON для запросов 1ого и 2ого типов
	def read_json(self, request_type, load_data):
		data = []
		#Если JSON возвращен запросом 1ого типа
		if request_type == "first_request_type":
			data.append(load_data['query']['date1'])
			data.append(load_data['query']['date2'])
			data.append(load_data['time_intervals'][0][0])
			data.append(load_data['query']['ids'][0])
			data.append(load_data['query']['metrics'][0])
			data.append(load_data['totals'][0][0])

		#Если JSON возвращен запросом 2ого типа	
		elif request_type == "second_request_type":
			for i in range(self.count_metrics - 1):
				for j in range(24):
					a = []
					a.append(load_data['query']['date1'])
					a.append(load_data['query']['date2'])
					a.append(load_data['time_intervals'][j][0])
					a.append(load_data['query']['ids'][0])
					a.append(load_data['data'][i]['dimensions'][0]['name'])
					a.append(load_data['data'][i]['metrics'][0][j])
					data.append(a)
		return data

	
	def write_csv(self, data, file_path):
		columns = self.config["csv_headers"]
		with open (file_path, 'w', newline='') as f:
			wr = csv.writer(f)
			wr.writerow(columns)
			wr.writerows(data)
		
	
	def main_logic(self):
		data_list = []
		ids = self.config["ids"]
		
		#запросы 1ого типа для каждого id и 2ух метрик формируем по запросу
		metrics = self.config["metrics"][0]
		for id in ids:
			for metric in metrics:
				main_params = self.get_params_loader("first_request_type", id, metric)
				params = self.load_request(main_params)
				self.response_dttm = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
				response = params["response"]
				load_data = response.json()
				data = self.read_json("first_request_type", load_data)
				data_list.append(data)

		#запросы 2ого типа для каждого id и 2ух наборов метрик формируем по запросу
		row_ids_list = self.config["row_ids"][1]
		for id in ids:
			for row_ids in row_ids_list:
				main_params = self.get_params_loader("second_request_type", id, row_ids)
				params = self.load_request(main_params)
				response = params["response"]
				load_data = response.json()
				data = self.read_json("second_request_type", load_data)
				data_list = data_list + data
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
            #self.log.error_msg(f'{err}')
			pass
		finally:
            #self.log.end_execute()
            #self.log.stop()
			pass
		


		

if __name__ == '__main__':
    Main = mostrans_appmetrica(path_file)
    Main.run()