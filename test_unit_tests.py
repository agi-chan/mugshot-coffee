import pytest
# import csv

def remove_sens_data(input_data_list : list,sens_data_keys:list):
    try:
        for dicts in input_data_list:
            for key in sens_data_keys:
                if key in dicts:
                    del dicts[key]
    except:
        raise TypeError("invalid inputs use lists")

def split_date_time(input_data_list : list):
    try:
        for dicts in input_data_list:
            dt_temp = dicts["Date and time"]
            date = dt_temp[0:10]
            time = dt_temp[11:16]
            dicts["date"] = date
            dicts["time"] = time
            del dicts["Date and time"]
    except:
        raise TypeError("invalid inputs use lists")

def split_order(input_data_list : list):
    for dicts in input_data_list:
        split_order_list = dicts["order"].split(", ")
        order_list = []
        for orders in split_order_list:
            order_list.append(orders.split(" - "))
        dicts["Order_list"] = order_list
        del dicts["order"]
<<<<<<< HEAD
    print(input_data_list)
    return input_data_list
=======
    return input_data_list
    
>>>>>>> a4ac856f3169a47f1795c4cf0992262b83b306ad

def test_remove_sens_data_raises_typeerror():
    with pytest.raises(Exception):
        remove_sens_data(1,1)

def test_remove_sens_data():
    data =[{'date_time' : "09/05/2023 09:00","location" :"Leeds",'name' : "Jerome Soper",'order' :"Regular Iced americano - 2.15, Large Hot Chocolate - 1.70, Regular Iced americano - 2.15, Large Filter coffee - 1.80",'total' : 7.8,'payment_type' :"CARD",'card_number' : 7925280230207247}]
    expected = [{'date_time' : "09/05/2023 09:00","location" :"Leeds",'order' :"Regular Iced americano - 2.15, Large Hot Chocolate - 1.70, Regular Iced americano - 2.15, Large Filter coffee - 1.80",'total' : 7.8,'payment_type' :"CARD"}]
    #act 
    actual = data
    remove_sens_data(actual,["card_number","name"])
    #assert pass
    print(actual)
    print(expected)
    assert actual == expected

def test_split_date_time_raises_typeerror():
    with pytest.raises(Exception):
        split_date_time(1)

def test_split_date_time():
    actual =[{'Date and time' : "09/05/2023 09:00","location" :"Leeds",'name' : "Jerome Soper",'order' :"Regular Iced americano - 2.15, Large Hot Chocolate - 1.70, Regular Iced americano - 2.15, Large Filter coffee - 1.80",'total' : 7.8,'payment_type' :"CARD",'card_number' : 7925280230207247}]
    expeted =[{"date": "09/05/2023","time": "09:00","location" :"Leeds",'name' : "Jerome Soper",'order' :"Regular Iced americano - 2.15, Large Hot Chocolate - 1.70, Regular Iced americano - 2.15, Large Filter coffee - 1.80",'total' : 7.8,'payment_type' :"CARD",'card_number' : 7925280230207247}]
    split_date_time(actual)

    assert actual == expeted

def test_split_order_raises_typeerror():
    with pytest.raises(Exception):
        split_order(1)

def test_split_order():
<<<<<<< HEAD
    actual =[{'Date and time' : "09/05/2023 09:00","location" :"Leeds",'name' : "Jerome Soper",'order' :"Regular Iced americano - 2.15, Large Hot Chocolate - 1.70, Regular Iced americano - 2.15, Large Filter coffee - 1.80",'total' : 7.8,'payment_type' :"CARD",'card_number' : 7925280230207247}]
    expected =[{"date": "09/05/2023","time": "09:00","location" :"Leeds",'name' : "Jerome Soper",'order' :"Regular Iced americano - 2.15, Large Hot Chocolate - 1.70, Regular Iced americano - 2.15, Large Filter coffee - 1.80",'total' : 7.8,'payment_type' :"CARD",'card_number' : 7925280230207247}]
    whatever = split_order(actual)
    print("1")
    print(whatever)
    print("2")
    print(expected)
    assert whatever == expected
=======
    actual =[{"date": "09/05/2023","time": "09:00","location" :"Leeds",'name' : "Jerome Soper",'order' :"Regular Iced americano - 2.15, Large Hot Chocolate - 1.70, Regular Iced americano - 2.15, Large Filter coffee - 1.80",'total' : 7.8,'payment_type' :"CARD",'card_number' : 7925280230207247}]
    expected =[{'date': '09/05/2023', 'time': '09:00', 'location': 'Leeds', 'name': 'Jerome Soper', 'total': 7.8, 'payment_type': 'CARD', 'card_number': 7925280230207247, 'Order_list': [['Regular Iced americano', '2.15'], ['Large Hot Chocolate', '1.70'], ['Regular Iced americano', '2.15'], ['Large Filter coffee', '1.80']]}]
    whatever = split_order(actual)
    print(actual)
    print(expected)
    assert whatever == expected

def test_build_transactions_df():
    actual = [{'Location': 'Leeds', 'Name': 'Jerome Soper', 'Total': '7.8', 'Payment Type': 'CARD', 'Date': '09/05/2023', 'Time': '09:00', 'Order_dict': [{'Name': 'Regular Iced americano', 'Price': '2.15', 'Quantity': 2}, {'Name': 'Large Hot Chocolate', 'Price': '1.70', 'Quantity': 1}, {'Name': 'Large Filter coffee', 'Price': '1.80', 'Quantity': 1}]}, {'Location': 'Leeds', 'Name': 'Ronald Moss', 'Total': '4.0', 'Payment Type': 'CASH', 'Date': '09/05/2023', 'Time': '09:01', 'Order_dict': [{'Name': 'Large Chai latte', 'Price': '2.60', 'Quantity': 1}, {'Name': 'Regular Hot Chocolate', 'Price': '1.40', 'Quantity': 1}]}]
    expected = [{'date': '09/05/2023', 'time': '09:00', 'city': 'Leeds', 'total_cost': '7.8', 'payment_method': 'CARD'}, {'date': '09/05/2023', 'time': '09:01', 'city': 'Leeds', 'total_cost': '4.0', 'payment_method': 'CASH'}] [('Regular Hot Chocolate', '1.40'), ('Large Hot Chocolate', '1.70'), ('Regular Iced americano', '2.15'), ('Large Chai latte', '2.60'), ('Large Filter coffee', '1.80')]
    build_transactions_df(actual)
    assert actual == expected
>>>>>>> a4ac856f3169a47f1795c4cf0992262b83b306ad

test_remove_sens_data_raises_typeerror()
test_remove_sens_data()
test_split_date_time()    
test_split_date_time_raises_typeerror()
test_split_order_raises_typeerror()
test_split_order()
test_build_transactions_df()

#Unit test for loading the data from csv
<<<<<<< HEAD
#filename1 = 'Data/test_data.csv'
#keys1 = ('Name', 'Colour', 'Age')
#with open (filename1, 'r') as data:
#    reader = csv.DictReader(data,keys1)
#    test = list()
#    for row in reader:
#        test.append(row)
#print(test)
=======
#needs to be in proper AAA format

# filename1 = 'Data/test_data.csv'
# keys1 = ('Name', 'Colour', 'Age')
# with open (filename1, 'r') as data:
#     reader = csv.DictReader(data,keys1)
#     test = list()
#     for row in reader:
#         test.append(row)
# print(test)
>>>>>>> a4ac856f3169a47f1795c4cf0992262b83b306ad
