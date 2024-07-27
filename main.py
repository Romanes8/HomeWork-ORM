import sqlalchemy
from sqlalchemy.orm import sessionmaker
from models import create_tables, Publisher, Shop, Book, Stock, Sale
import json
from prettytable import PrettyTable

#ввод имени пользователя PostgreSQL, пароля, имени базы данных
USER_NAME_DB = input('Введите имя пользователя postgresql: ')
PASSWORD = input('Введите пароль пользователя postgresql: ')
NAME_DB = input('Введите имя базы данных: ')

DSN = f'postgresql://{USER_NAME_DB}:{PASSWORD}@localhost:5432/{NAME_DB}'
engine = sqlalchemy.create_engine(DSN)

Session = sessionmaker(bind=engine)
session = Session()

#функция загрузки тестовых данных в БД(задание №3)
def add_test_data():
    with open('fixtures/tests_data.json', 'r') as fd:
        data = json.load(fd)
    for record in data:
        model = {
            'publisher': Publisher,
            'shop': Shop,
            'book': Book,
            'stock': Stock,
            'sale': Sale,
        }[record.get('model')]
        session.add(model(id=record.get('pk'), **record.get('fields')))
    session.commit()

#функция запроса в БД по имени издателя ил id(задание №2)
def query():
    pub_name_or_id = input('Введите имя издателя или id: ')

    res = session.query(Book).with_entities(Book.title, Shop.name, Sale.price, Sale.date_sale) \
        .join(Publisher, Publisher.id == Book.id_publisher) \
        .join(Stock, Stock.id_book == Book.id) \
        .join(Shop, Shop.id == Stock.id_shop) \
        .join(Sale, Sale.id_stock == Stock.id)

    if pub_name_or_id.isdigit():
        result = res.filter(Publisher.id == pub_name_or_id).all()
    else:
        result = res.filter(Publisher.name == pub_name_or_id).all()

    t = PrettyTable(['Книга', 'Магазин', 'Цена', 'Дата покупки'])
    for c in result:
        t.add_row([c[0], c[1], c[2], c[3]])
    print(t)


print('1 - создать таблицы в БД')
print('2 - заполнить таблицы тестовыми данными (Задание №3)')
print('3 - выполнить запрос по имени или id издателя (Задание №2)')
action = input('Выберите действие: ')

if action == '1':
    create_tables(engine)
    print('Таблицы БД созданы')
    print('1 - заполнить таблицы тестовыми данными (Задание №3)')
    print('2 - закрыть сессию')
    action_1 = input('Выберите действие: ')
    if action_1 == '1':
        add_test_data()
        print('Тестовые данные загружены')
        print('1 - Выполнить запрос по имени или id издателя (Задание №2)')
        print('2 - закрыть сессию')
        action_2 = input('Выберите действие: ')
        if action_2 == '1':
            query()
            session.close()
        if action_2 == '2':
            session.close()
            print('Сессия закрыта')
    if action_1 == '2':
        session.close()
        print('Сессия закрыта')

if action == '2':
    add_test_data()
    print('Тестовые данные загружены')
    print('1 - Выполнить запрос по имени или id издателя (Задание №2)')
    print('2 - закрыть сессию')
    action_3 = input('Выберите действие: ')
    if action_3 == '1':
        query()
        session.close()
    if action_3 == '2':
        session.close()
        print('Сессия закрыта')

if action == '3':
    query()
    session.close()

session.close()


