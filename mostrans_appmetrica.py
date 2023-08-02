import sys
import os
import json
from datetime import datetime, timedelta
import csv
import requests
from CronBase import CronBase




DT = datetime.now()
path_file = os.path.realpath(__file__)

class mostrans_appmetrica(CronBase):
	
	#получение параметров из конфига
	def get_params_loader(self, req_type, i, j):
		params = {}
		headers = self.config["headers"]
		url = self.config["url"]
		self.timeout = self.config["timeout"]
		params["date1"] = self.date_from
		params["date2"] = self.date_to

		if req_type == 1:
			params = self.config["first_request_params"]["const_params"]
			params["id"] = self.config["first_request_params"]["variable_params"]["ids"][i]
			params["metrics"] = self.config["first_request_params"]["variable_params"]["metrics"][j]
		elif req_type == 2:
			params = self.config["second_request_params"]["const_params"]
			params["id"] = self.config["second_request_params"]["variable_params"]["ids"][i]
			row_ids1 = str(self.config["second_request_params"]["row_ids"][j])
			if j == 0:
				row_ids2 = row_ids1[:1] + "[null]," + row_ids1[1:]
			else:
				row_ids2 = row_ids1
			row_ids = row_ids2.replace(" ", "").replace("\'", "\"")
			params["row_ids"] = row_ids
			self.count_metrics = len(self.config["second_request_params"]["row_ids"][j])
		
		request_params = dict(url = url, headers = headers, params = params)
		main_params = dict(request_params = request_params)
		return main_params 
			
	#парсим JSON
	def read_json(self, req_type, load_data):
		data = []
		if req_type == 1:
			data.append(load_data['query']['date1'])
			data.append(load_data['query']['date2'])
			data.append(load_data['time_intervals'][0][0])
			data.append(load_data['query']['ids'][0])
			data.append(load_data['query']['metrics'][0])
			data.append(load_data['totals'][0][0])
		elif req_type == 2:
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
		date = datetime.now()
		date = datetime.strftime(date, "%Y%m%d")
		columns = self.config["csv_headers"]
		with open (file_path, 'w', newline='') as f:
			wr = csv.writer(f)
			wr.writerow(columns)
			wr.writerows(data)
		print("Запись завершена в ", file_path)
	
	def main_logic(self):
		data_list = []
		#запросы для каждого id и 2ух метрик
		req_type = 1
		for i in range(3):
			for j in range(2):
				main_params = self.get_params_loader(req_type, i, j)
				params = self.load_request(main_params)
				self.response_dttm = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
				response = params["response"]
				load_data = response.json()
				data = self.read_json(req_type, load_data)
				data_list.append(data)
	
		#запросы для каждого id и всяких разных метрик
		req_type = 2
		for i in range(3):
			for j in range(2):
				main_params = self.get_params_loader(req_type, i, j)
				params = self.load_request(main_params)
				response = params["response"]
				load_data = response.json()
				data = self.read_json(req_type, load_data)
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