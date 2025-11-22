import requests
import json, time, os
import pandas as pd
import traceback
import sqlalchemy
class AmoCrmApi(): 
    def __init__(self,client_id='',client_secret='',auth_code='',redirect_url='',sub_domain='',domen=''):
        self.client_id=client_id
        self.client_secret=client_secret
        self.auth_code=auth_code
        self.redirect_url=redirect_url
        self.sub_domen=sub_domain
        self.domen=domen

    def get_token(self):
        """Получаем refresh_token и сохраняем в amocrm_tokens.json"""
        url = f"https://{self.sub_domen}.{self.domen}/oauth2/access_token"
        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "authorization_code",
            "code": self.auth_code,
            "redirect_uri": self.redirect_url
        }
        try:
            r = requests.post(url, json=payload, timeout=30)
            if r.status_code == 200:
                tokens = r.json()  # содержит access_token, refresh_token, expires_in и т.д.
                with open('amocrm_tokens.json', "w", encoding="utf-8") as f:
                    json.dump(tokens, f, ensure_ascii=False, indent=2)
                return print('Токен успешно сохранен в amocrm_tokens.json')
            elif r.status_code >=400:
                return print(f"♻️ ошибка авторизаци. STATUS:{r.status_code} BODY:{r.text}")
        except:
            print("Произошла ошибка соединения с сервером API")
            print(f'{traceback.print_exc()}')
        
    def refresh_access_token(self):
        """Обновляем токен сохраняем в amocrm_tokens.json"""
        url = f"https://{self.sub_domen}.{self.domen}/oauth2/access_token"
        # Загружаем старые токены
        with open('amocrm_tokens.json', 'r', encoding='utf-8') as f:
            old_tokens = json.load(f)
            
        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "refresh_token",
            "refresh_token": old_tokens["refresh_token"],
            "redirect_uri": self.redirect_url
        }
    
        # Делаем запрос к API
        try:
            r = requests.post(url, json=payload, timeout=30)
            if r.status_code == 200:
                tokens = r.json()
                # Сохраняем новые токены в файл
                with open('amocrm_tokens.json', "w", encoding="utf-8") as f:
                    json.dump(tokens, f, ensure_ascii=False, indent=2)
                return print("♻️  Токен успешно обновлён и сохранён в amocrm_tokens.json")
              
            elif r.status_code >=400:
                return print(f"♻️ ошибка авторизаци. STATUS:{r.status_code} BODY:{r.text}")
        except:
            print("Произошла ошибка соединения с сервером API")
            print(f'{traceback.print_exc()}')

    def get_lead(self, params_lead={'limit': 250, 'with': 'contacts,loss_reason,source'}):
        """Постраничная выгрузка всех сделок из amoCRM"""
        self.report_lead = {'_embedded': {'leads': []}}
        offset = 1  # в amoCRM используется page
        limit = params_lead.get('limit', 250)
        with open('amocrm_tokens.json', "r", encoding="utf-8") as f:
            token = json.load(f)
        self.token = token['access_token']
        self.headers = {"Authorization": f"Bearer {self.token}"}
        try:
            while True:
                print(f'📄 Загрузка страницы {offset}')
                params_lead['page'] = offset
                url = f"https://{self.sub_domen}.{self.domen}/api/v4/leads"
                res = requests.get(url, headers=self.headers, params=params_lead, timeout=(5, 60))
                if res.status_code == 200:
                    data = res.json()
                    leads = data.get('_embedded', {}).get('leads', [])
                    self.report_lead['_embedded']['leads'].extend(leads)
                    if not leads or len(leads) < limit:
                        break
                    offset += 1  # следующая страница
                elif res.status_code == 401:
                    print("Пользователь не авторизован")
                    break
                elif res.status_code == 402:
                    print("Аккаунт не оплачен")
                    break
                else:
                    print(f'Ошибка:{r.status_code} BODY:{r.text}"')  
            return print(f'✅ Всего выгружено {len(self.report_lead["_embedded"]["leads"])} сделок')
        except:
            print("Произошла ошибка соединения с сервером API")
            print(f'{traceback.print_exc()}')


    def transform_report_lead(self):
        """Обработка сделок, сохраняем в обработаные данные в dicts_lead"""
        self.dicts_lead=[]
        try:
            for r in self.report_lead['_embedded']['leads']:
                data = {
                'id': r['id'],
                'responsible_user_id': r['responsible_user_id'],
                'Название':r['name'],
                'Бюджет':r['price'],
                'ID_статуса':r['status_id'],
                'ID_воронки':r['pipeline_id'],
                'Дата_создания':r['created_at'],
                'Дата_обновления':r['updated_at'],
                'Дата_закрытия':r['closed_at'],
                'Дата_ближайшей_задачи':r['closest_task_at'],
                'Ссылка в CRM': r['_links']['self']['href']}
                for custom in (r.get('custom_fields_values') or []):
                    for v in (custom.get('values') or []):
                        data[custom.get('field_name')] = v.get('value')
                self.dicts_lead.append(data)
            return print('Данные о сделках успешно обработанны и записаны в dicts_lead')
        except:
            print("Произошла ошибка")
            print(f'{traceback.print_exc()}')
        
    def get_contact(self,params_contact={'limit':250,'with':'catalog_elements,leads,customers'}):
        """Постраничная выгрузка всех контактов из amoCRM"""
        self.report_contacts = {'_embedded': {'contacts': []}}
        offset = 1  # в amoCRM используется page
        limit = params_contact.get('limit', 250)
        with open('amocrm_tokens.json', "r", encoding="utf-8") as f:
            token = json.load(f)
        self.token=token['access_token']
        self.headers={"Authorization": f"Bearer {self.token}"}
        while True:
            print(f'📄 Загрузка страницы {offset}')
            params_contact['page'] = offset
            url = f"https://{self.sub_domen}.{self.domen}/api/v4/contacts"
            res = requests.get(url,headers=self.headers,params=params_contact)
            if res.status_code == 200:
                data = res.json()
                contact = data.get('_embedded', {}).get('contacts', [])
                self.report_contacts['_embedded']['contacts'].extend(contact)
                if not contact or len(contact) < limit:
                    break
                offset += 1  # следующая страница
            elif res.status_code == 401:
                print("Пользователь не авторизован")
                break
            elif res.status_code == 402:
                print("Аккаунт не оплачен")
                break
            else:
                print(f'Ошибка:{r.status_code} BODY:{r.text}"')
                break
        print(f'✅ Всего выгружено {len(self.report_contacts["_embedded"]["contacts"])} контактов')
        return self.report_contacts
        
    def transform_contact(self):
        """Обработка контактов, сохраняем в обработаные данные в dicts_contact"""
        self.dicts_contact=[]
        try:
            for r in self.report_contacts['_embedded']['contacts']:
                data = {
                'id': r['id'],
                'responsible_user_id': r['responsible_user_id'],
                'first_name':r['first_name'],
                'last_name':r['last_name'],
                'Дата_создания':r['created_at'],
                'Дата_обновления':r['updated_at'],
                'Дата_ближайшей_задачи':r['closest_task_at']}
                for custom in (r.get('custom_fields_values') or []):
                    for v in (custom.get('values') or []):
                        data[custom.get('field_name')] = v.get('value')
                self.dicts_contact.append(data)
            return print(f"Данные о контактах обработанны и записаны в dicts_contact")
        except:
            print("Произошла ошибка при обработке")
            print(f'{traceback.print_exc()}')


    def get_tasks(self,params_tasks={"limit": 250}):
        """Постраничная выгрузка всех задач из amoCRM"""
        self.report_tasks={'_embedded': {'tasks': []}}
        offset = 1  # в amoCRM используется page
        limit = params_tasks.get('limit', 250)
        with open('amocrm_tokens.json', "r", encoding="utf-8") as f:
            token = json.load(f)
        self.token=token['access_token']
        self.headers={"Authorization": f"Bearer {self.token}"}
        try:
            while True:
                print(f'📄 Загрузка страницы {offset}')
                params_tasks['page'] = offset
                url = f"https://{self.sub_domen}.{self.domen}/api/v4/tasks"
                res = requests.get(url,params=params_tasks,headers=self.headers)
                if res.status_code == 200:
                    data =res.json()
                    tasks = data.get('_embedded', {}).get('tasks', [])
                    self.report_tasks['_embedded']['tasks'].extend(tasks)
                    if not tasks or len(tasks) < limit:
                        break
                    offset += 1  # следующая страница
                elif res.status_code == 401:
                    print("Пользователь не авторизован")
                    break
                else:
                    print(f'Ошибка:{r.status_code} BODY:{r.text}"')
                    break
            return print(f'✅ Всего выгружено {len(self.report_tasks["_embedded"]["tasks"])} задач')            
        except:
            print("Произошла ошибка соединения с сервером API")
            print(f'{traceback.print_exc()}')
        
    def transform_task(self):
        """Обработка задач, сохраняем в обработаные данные в dicts_tasks"""
        self.dicts_tasks=[]
        try:
            for r in self.report_tasks['_embedded']['tasks']:
                data = {
                'id': r['id'],
                'task_type_id':r['task_type_id'],
                'responsible_user_id': r['responsible_user_id'],
                'Сущность_к_котрой_привязана_задача':r['entity_type'],
                'Выполнена_ли_задача':r['is_completed'],
                'Кем_создана':r['created_by'],
                'Кем_измененна':r['updated_by'],
                'Дата_создания':r['created_at'],
                'Дата_обновления':r['updated_at'],
                'Описание':r['text']
                }
                self.dicts_tasks.append(data)
            return print("Данные о задачах успешно обработанны и записаны в dicts_tasks")
        except:
            print("Произошла ошибка при обработке")
            print(f'{traceback.print_exc()}')

    def get_pipeline_name(self,pipeline_id=''):
        """Выгрузка названий этопов воронок из amoCRM, где pipeline_id - номер воронки"""
        self.report_pipeline=''
        url = f"https://{self.sub_domen}.{self.domen}/api/v4/leads/pipelines/{pipeline_id}/statuses"
        with open('amocrm_tokens.json', "r", encoding="utf-8") as f:
            token = json.load(f)
        self.token=token['access_token']
        self.headers={"Authorization": f"Bearer {self.token}"}
        try:
            report_pipeline = requests.get(url,headers=self.headers)
            if report_pipeline.status_code == 200:
                self.report_pipeline=report_pipeline.json()
                return print(f"✅ Данные о этапах воронки {pipeline_id} успешно выгруженны") 
            elif report_pipeline.status_code == 400:
                print("Неверная структура массива передаваемых данных")
            elif report_pipeline.status_code == 422:
                print("Входящие данные не мог быть обработаны")
            elif report_pipeline.status_code == 405:
                print("Запрашиваемый HTTP-метод не поддерживается")
            elif report_pipeline.status_code == 429:
                print("Превышено допустимое количество запросов в секунду")
            elif report_pipeline.status_code == 2002:
                print("По вашему запросу ничего не найдено")
            else:
                print(f'Ошибка:{r.status_code} BODY:{r.text}"')
        except:
            print("Произошла ошибка соединения с сервером API")
            print(f'{traceback.print_exc()}')
    
    def transform_pipeline(self):
        """Обработка pipeline, сохраняем в обработаные данные в dicts_pipelin"""
        self.dicts_pipeline=[]
        try:
            for p in self.report_pipeline['_embedded']['statuses']:
                data = {
                'id_этапа':p['id'],
                'Название_этапа':p['name']
                }
                self.dicts_pipeline.append(data)
            return print("Данные о этапах воронки успешно обработанны и записаны в dicts_pipeline")
        except:
            print("Произошла ошибка при обработке")
            print(f'{traceback.print_exc()}')

    def convert_to_csv(self,file_name='',data = ''):
        try:
            pd.DataFrame(data).to_csv(f'{file_name}.csv', sep=',', index=False, encoding='utf-8')
            return print(f'{file_name}.csv успешно сохранен на локальном компьютере')
        except:
            print(f' Произошла ошибка при сохранении файла {file_name}.csv')
            print(f'{traceback.print_exc()}')
        
    def pssql_create_table(self,port ='',schema='',table_name='',user='',passvord='',host='',database='',data=''):
        """Создаем таблицу в postgresql"""
        self.port=port
        self.schema=schema
        self.table_name=table_name
        self.user=user
        self.passvord=passvord
        self.host=host
        self.database=database
        self.data=data
        self.engine = sqlalchemy.create_engine(f"postgresql+psycopg2://{self.user}:{self.passvord}@{self.host}:{self.port}/{self.database}",
                                            pool_pre_ping=True,
                                            pool_recycle=1800,
                                            connect_args={"connect_timeout": 30})
        self.unique_keys = list(dict.fromkeys(k for d in data for k in d)) #получаем название колонок
        self.column_name = (',').join(f'"{key}" text' for key in self.unique_keys) #добавляем формат данных text к названию колонки
        #создаем таблицу в бд
        try:
            with self.engine.connect() as conect:
                conect.execute(sqlalchemy.text(f'create table if not exists {self.schema}.{self.table_name}({self.column_name})'))
                conect.execute(sqlalchemy.text('commit'))
            print(f'Таблица {self.schema}.{self.table_name} успешно созданна')
        except:
           traceback.print_exc()

    def pssql_insert_table(self,schema='',port ='',table_name='',user='',passvord='',host='',database='',data=''):
            """Обнуляем таблицу и записываем данные"""
            self.port=port    
            self.schema=schema
            self.table_name=table_name
            self.user=user
            self.passvord=passvord
            self.host=host
            self.database=database
            self.data=data
            self.engine = sqlalchemy.create_engine(f"postgresql+psycopg2://{self.user}:{self.passvord}@{self.host}:{self.port}/{self.database}",
                                            pool_pre_ping=True,
                                            pool_recycle=1800,
                                            connect_args={"connect_timeout": 30})
         # 1) Фиксируем единый порядок колонок по всем строкам
            self.unique_keys = list(dict.fromkeys(k for d in data for k in d))
        # 2) Готовим SQL (колонки экранированы), плейсхолдеры позиционные p0..pN
            self.plaseholder = ','.join(f':p{i}' for i in range(len(self.unique_keys)))
            self.column_name = ','.join(f'"{key}"' for key in self.unique_keys)
        # 3) ВАЖНО: строим params по self.unique_keys, а не по row.keys() 
            self.insert_data = [
                {f'p{i}': row.get(col) for i, col in enumerate(self.unique_keys)}
                for row in self.data ]

            try:
                with self.engine.connect() as conect:
                    conect.execute(sqlalchemy.text(f'truncate table {self.schema}.{self.table_name}'))
                    print(f'Таблица обнулена {self.schema}.{self.table_name}')
                    conect.execute(sqlalchemy.text(f'insert into {self.schema}.{self.table_name}({self.column_name}) values({self.plaseholder})'),self.insert_data)
                    conect.execute(sqlalchemy.text('commit'))
                print(f'Данные записаны в таблицу {self.schema}.{self.table_name}')
            except:
                traceback.print_exc()

