import pymysql


class Match(object):
    # 建立数据库链接
    def __init__(self):
        self.connect = pymysql.connect(host="127.0.0.1",
                                       port=3306,
                                       user="root",
                                       password="123456",
                                       database="spiderdatabase",
                                       charset="utf8")
        self.cursor = self.connect.cursor(cursor=pymysql.cursors.DictCursor)

    # 关闭链接
    def __del__(self):
        self.connect.close()
        self.cursor.close()

    # 薪资优先匹配
    def __salaryPriority(self, sql_0, sql_1, sql_2, sql_3, sql_4, sql_5,
                         city, area, experience, education, skill: list, salary):
        try:
            if len(skill) != 0:
                self.cursor.execute(sql_0,
                                    (city, area, experience, education,
                                     skill[0], skill[1], skill[2], skill[3], skill[4], salary))
                self.connect.commit()
                result_0 = self.cursor.fetchone()
                self.cursor.execute(sql_1, (city, experience, education,
                                            skill[0], skill[1], skill[2], skill[3], skill[4], salary))
                self.connect.commit()
                result_1 = self.cursor.fetchone()
            else:
                self.cursor.execute(sql_0, (city, area, experience, education, salary))
                self.connect.commit()
                result_0 = self.cursor.fetchone()
                self.cursor.execute(sql_1, (city, experience, education, salary))
                self.connect.commit()
                result_1 = self.cursor.fetchone()
            # 比较绝对值取最接近期望薪资的值
            if result_0 is not None and result_1 is not None:
                if abs(salary - result_0['avg_salary']) < abs(salary - result_1['avg_salary']):
                    return result_0
                else:
                    return result_1
            else:
                if len(skill) != 0:
                    self.cursor.execute(sql_2,
                                        (city, area, education,
                                         skill[0], skill[1], skill[2], skill[3], skill[4], salary))
                    self.connect.commit()
                    result_0 = self.cursor.fetchone()
                    self.cursor.execute(sql_3, (city, education,
                                                skill[0], skill[1], skill[2], skill[3], skill[4], salary))
                    self.connect.commit()
                    result_1 = self.cursor.fetchone()
                else:
                    self.cursor.execute(sql_2, (city, area, education, salary))
                    self.connect.commit()
                    result_0 = self.cursor.fetchone()
                    self.cursor.execute(sql_3, (city, education, salary))
                    self.connect.commit()
                    result_1 = self.cursor.fetchone()
                # 比较绝对值取最接近期望薪资的值
                if result_0 is not None and result_1 is not None:
                    if abs(salary - result_0['avg_salary']) < abs(salary - result_1['avg_salary']):
                        return result_0
                    else:
                        return result_1
                else:
                    if len(skill) != 0:
                        self.cursor.execute(sql_4,
                                            (city, area,
                                             skill[0], skill[1], skill[2], skill[3], skill[4], salary))
                        self.connect.commit()
                        result_0 = self.cursor.fetchone()
                        self.cursor.execute(sql_5, (city,
                                                    skill[0], skill[1], skill[2], skill[3], skill[4], salary))
                        self.connect.commit()
                        result_1 = self.cursor.fetchone()
                    else:
                        self.cursor.execute(sql_4, (city, area, salary))
                        self.connect.commit()
                        result_0 = self.cursor.fetchone()
                        self.cursor.execute(sql_5, (city, salary))
                        self.connect.commit()
                        result_1 = self.cursor.fetchone()
                    # 比较绝对值取最接近期望薪资的值
                    if result_0 is not None and result_1 is not None:
                        if abs(salary - result_0['avg_salary']) < abs(salary - result_1['avg_salary']):
                            return result_0
                        else:
                            return result_1
                    else:
                        # 无结果返回false
                        return False

        except Exception as e:
            self.connect.rollback()
            print(">>>读取数据失败 执行MySQL: {} 时出错：{}", e)

    # 地域优先匹配
    def __areaPriority(self, sql_0, sql_1, sql_2, sql_3, sql_4, sql_5,
                       city, area, experience, education, skill: list, salary):
        try:
            if len(skill) != 0:
                self.cursor.execute(sql_0,
                                    (city, area, experience, education,
                                     skill[0], skill[1], skill[2], skill[3], skill[4], salary))
                self.connect.commit()
                result_0 = self.cursor.fetchone()
                self.cursor.execute(sql_1, (
                    city, experience, education, skill[0], skill[1], skill[2], skill[3], skill[4], salary))
                self.connect.commit()
                result_1 = self.cursor.fetchone()
            else:
                self.cursor.execute(sql_0, (city, area, experience, education, salary))
                self.connect.commit()
                result_0 = self.cursor.fetchone()
                self.cursor.execute(sql_1, (city, experience, education, salary))
                self.connect.commit()
                result_1 = self.cursor.fetchone()
            if result_0 is not None:
                return result_0
            elif result_1 is not None:
                return result_1
            else:
                if len(skill) != 0:
                    self.cursor.execute(sql_2,
                                        (city, area, education,
                                         skill[0], skill[1], skill[2], skill[3], skill[4], salary))
                    self.connect.commit()
                    result_0 = self.cursor.fetchone()
                    self.cursor.execute(sql_3, (
                        city, education, skill[0], skill[1], skill[2], skill[3], skill[4], salary))
                    self.connect.commit()
                    result_1 = self.cursor.fetchone()
                else:
                    self.cursor.execute(sql_2, (city, area, education, salary))
                    self.connect.commit()
                    result_0 = self.cursor.fetchone()
                    self.cursor.execute(sql_3, (city, education, salary))
                    self.connect.commit()
                    result_1 = self.cursor.fetchone()
                if result_0 is not None:
                    return result_0
                elif result_1 is not None:
                    return result_1
                else:
                    if len(skill) != 0:
                        self.cursor.execute(sql_4,
                                            (city, area,
                                             skill[0], skill[1], skill[2], skill[3], skill[4], salary))
                        self.connect.commit()
                        result_0 = self.cursor.fetchone()
                        self.cursor.execute(sql_5, (
                            city, skill[0], skill[1], skill[2], skill[3], skill[4], salary))
                        self.connect.commit()
                        result_1 = self.cursor.fetchone()
                    else:
                        self.cursor.execute(sql_4, (city, area, salary))
                        self.connect.commit()
                        result_0 = self.cursor.fetchone()
                        self.cursor.execute(sql_5, (city, salary))
                        self.connect.commit()
                        result_1 = self.cursor.fetchone()
                    if result_0 is not None:
                        return result_0
                    elif result_1 is not None:
                        return result_1
                    else:
                        return False
        except Exception as e:
            self.connect.rollback()
            print(">>>读取数据失败 执行MySQL: {} 时出错：{}", e)
