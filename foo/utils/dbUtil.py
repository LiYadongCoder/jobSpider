# coding:utf-8
import pymysql


# 定义数据库操作类
class MysqlObject(object):

    # 初始化
    def __init__(self, host, port, user, password, db):
        self.__conn_status = False
        try:
            self.__conn = pymysql.connect(host=host, port=port, user=user, password=password, db=db, charset='utf8')
            self.__conn_status = True
        except Exception as e:
            print("connect mysql Error %s" % e)
        if self.__conn_status:
            self.__cur = self.__conn.cursor()

    # 关闭数据库连接
    def close_mysql_conn(self):
        if self.__conn_status:
            if self.__cur:
                self.__cur.close()
            if self.__conn:
                self.__conn.close()
            return True
        else:
            print("there is no connect")
            return False

    # 插入一条数据
    def insert_record(self, sql, param):
        if self.__conn_status:
            try:
                result = self.__cur.execute(sql, param)
                self.__conn.commit()
                return result
            except Exception as e:
                self.__conn.rollback()
                print("insert Except：%" % e)

    # 删除记录
    def delete_record(self, sql, param):
        if self.__conn_status:
            print("delete data")

    # 更新数据
    def update_record(self, sql, param):
        if self.__conn_status:
            print("update data")

    # 查询数据
    def select_record(self, sql, param):
        if self.__conn_status:
            try:
                row_nums = self.__cur.execute(sql, param)
                result = self.__cur.fetchall()
                return row_nums, result
            except Exception as e:
                print("execute exception：%s" % e)
                return None
        else:
            print("connect to database fail")
