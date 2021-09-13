import sys
import time
import tushare
import pymysql

pro = tushare.pro_api("37e292fa9e6d52a3ebf7f06604556959f13f6486dc18690b1b1d3a51")
db = pymysql.connect(host="127.0.0.1", user="root", password="123456", database="zijin")
db.autocommit(1)


## 增加基础数据
def stock_info():
    data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    count = data["ts_code"].count()
    cursor = db.cursor()

    while count > 0:
        tsCode = data["ts_code"][count - 1]
        symbol = (data["symbol"][count - 1])
        name = (data["name"][count - 1])
        area = (data["area"][count - 1])
        industry = (data["industry"][count - 1])
        listDate = (data["list_date"][count - 1])

        cursor.execute("select * from tb_stock_info where symbol = '"+symbol+"'")
        result = cursor.fetchone()
        get_day(tsCode)
        if result:
            return

        # SQL 插入语句
        sql = "INSERT INTO `tb_stock_info`(`code`, `symbol`, `name`, `area`, `industry`, `online_date`) VALUES ('"+tsCode+"','"+symbol+"','"+name+"','"+area+"','"+industry+"','"+listDate+"')"
        try:
            # 执行sql语句
            cursor.execute(sql)
        except:
            print("Error: unable to fetch data")
        count = count - 1
    return


## 每日K线
def get_day(str):
    today = time.strftime("%Y%m%d", time.localtime())
    cursor = db.cursor()
    sql = "select * from tb_stock_info_detail where `symbol` = '"+str+"' order by trade_date desc limit 1"
    print(sql)
    try:
        cursor.execute(sql)
        data = cursor.fetchone()
        if data:
            if data[1] != today:
                df = pro.daily(ts_code=str, start_date=data[1], end_date=today)
                insert_detail(df, str)
        else:
            df = pro.daily(ts_code=str, start_date='20180701', end_date=today)
            insert_detail(df, str)
    except:
        print("Unexpected error:", sys.exc_info())
    return


def insert_detail(df, code):
    cursor = db.cursor()
    s = df["ts_code"].count()
    while s > 0:
        trade_date = df["trade_date"][s - 1]
        open = df["open"][s - 1]
        high = df["high"][s - 1]
        low = df["low"][s - 1]
        close = df["close"][s - 1]
        change = df["change"][s - 1]
        vol = df["vol"][s - 1]
        amount = df["amount"][s - 1]

        sql = "INSERT INTO `tb_stock_info_detail`(`code`, `symbol`, `open`, `high`, `low`, `close`, `change`, `vol`, `amount`, `trade_date`) VALUES('"+trade_date+"','"+code+"', "+str(open)+", "+str(high)+", "+str(low)+", "+str(close)+", "+str(change)+", "+str(vol)+", "+str(amount)+", '"+trade_date+"')"
        cursor.execute(sql)
        s = s - 1
    return


stock_info()
print("基础数据更新成功")
