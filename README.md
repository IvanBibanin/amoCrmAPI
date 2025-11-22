# Методы класса
## Основные методы
get_token() - Первичная авторизация \
refresh_access_token() - Обновление токена \
get_lead() - Выгрузка сделок \
get_contact() - Выгрузка контактов \
get_tasks() - Выгрузка задач \
get_pipeline_name() - Выгрузка воронки 
## Методы трансформации данных
transform_report_lead() - Обработка данных сделок, сохраняет данные в dicts_lead \
transform_contact() - Обработка контактов, сохраняет данные в dicts_contact \
transform_task() - Обработка задач, сохраняет данные в dicts_task \
transform_pipeline() - Обработка воронок, сохраняет данные в dicts_pipeline 
## Методы сохранения данных
convert_to_csv(file_name, data) - Сохранение в CSV. В кажестве параметра data указываем любой из <u>dicts_lead, dicts_contact, dicts_task, dicts_pipeline</u> \
pssql_create_table() - Создание таблицы в PostgreSQL. В data указываем любой из списков <u> dicts_lead,dicts_contact,dicts_task,dicts_pipeline </u> \
pssql_insert_table() - Вставка данных в PostgreSQL. В data указываем любой из списков <u> dicts_lead, dicts_contact dicts_task,dicts_pipeline </u>

# Config 
## Config для Postrgesql 
В Config.py уже прописаны данные БД их можно изменить в docker-compose.yaml
## Config для AmoCRM
Данные для Config нужно взять в интерфейсе "amocrm.ru" или "kommo.com" в настройках интеграции в разделе "Ключи и доступы" 
1. Введите поддомен вашей CRM в поле SUB_DOMAIN. Например для https://test.amocrm.ru SUB_DOMAIN='test' \
2. Введите в зависимости от вида CRM введите "amocrm.ru" или "kommo.com" в поле DOMEN \
3. Введите "ID интеграции" в поле CLIENT_ID \
4. Введите "Секретный ключ" в поле CLIENT_SECRET \
5. Введите "Ссылку перенаправлений" в REDIRECT_URI \
6. Введите "Код авторизации (действителен 20 минут) в поле AUTH_CODE \
