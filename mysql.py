import pymysql      #将整个模块导入
from collections import Counter   #从某个模块导入某个函数
import json


# 数据库对象
class MySql(object):
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
    def __del_(self):
        self.connect.close()
        self.cursor.close()

    # 手动关闭数据库链接
    def close(self):
        self.connect.close()
        self.cursor.close()

    '''============================================数据处理======================================================='''

    # 原始数据存储
    def saveData(self, job_name, job_place, job_company, job_scale, job_salary, job_education, job_experience,
                 job_label, job_skill, job_welfare,type):

        sql = '''
                insert into row_data(name,place,company,scale,salary,education,experience,label,skill,welfare,type)
                values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            '''
        try:
            self.cursor.execute(sql, (job_name, job_place, job_company, job_scale, job_salary, job_education,
                                      job_experience, job_label, job_skill, job_welfare,type))
            self.connect.commit()
        except Exception as e:
            self.connect.rollback()
            print(">>>插入数据失败 执行MySQL: {} 时出错：{}".format(sql, e))

    # 原始数据存储
    def saveHandledData(self, name, city, area, detail, company, scale, salary_min, salary_max, salary_avg, education,
                        experience, label, skill, welfare):

        sql = '''
                insert into handled_data (name, city, area, detail, company, scale, min_salary, max_salary,
                                          avg_salary, education, experience, label, skill, welfare)
                values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            '''
        try:
            self.cursor.execute(sql, (name, city, area, detail, company, scale, salary_min, salary_max, salary_avg,
                                      education, experience, label, skill, welfare))
            self.connect.commit()
            print("【数据处理模块】===>SUCCESS！插入数据成功 执行MySQL打印 ")
            print((name, city, area, detail, company, scale, salary_min, salary_max, salary_avg,
                                      education, experience, label, skill, welfare))
        except Exception as e:
            self.connect.rollback()
            print(">>>插入数据失败 执行MySQL: {} 时出错：{}".format(sql, e))

    # 读取原始表单
    def readRowData(self):
        sql = '''
                select * from row_data
            '''

        try:
            self.cursor.execute(sql)
            self.connect.commit()
            # 获取所有字段
            fields = self.cursor.fetchall()
            return fields
        except Exception as e:
            self.connect.rollback()
            print(">>>读取数据失败 执行MySQL: {} 时出错：{}".format(sql, e))

    # 读取处理后表单
    def readHandledData(self):
        sql = '''
                select * from handled_data
            '''

        try:
            self.cursor.execute(sql)
            self.connect.commit()
            # 获取所有字段
            fields = self.cursor.fetchall()
            return fields
        except Exception as e:
            self.connect.rollback()
            print(">>>读取数据失败 执行MySQL: {} 时出错：{}".format(sql, e))

    # 读取原始表单
    def readAlgorithmData(self):
        sql = '''
                select * from algorithm_data
            '''

        try:
            self.cursor.execute(sql)
            self.connect.commit()
            # 获取所有字段
            fields = self.cursor.fetchall()
            return fields
        except Exception as e:
            self.connect.rollback()
            print(">>>读取数据失败 执行MySQL: {} 时出错：{}".format(sql, e))

    # 读取原始表单
    def readAndroidData(self):
        sql = '''
                select * from android_data
            '''

        try:
            self.cursor.execute(sql)
            self.connect.commit()
            # 获取所有字段
            fields = self.cursor.fetchall()
            return fields
        except Exception as e:
            self.connect.rollback()
            print(">>>读取数据失败 执行MySQL: {} 时出错：{}".format(sql, e))

    # 读取原始表单
    def readCData(self):
        sql = '''
                select * from c_data
            '''

        try:
            self.cursor.execute(sql)
            self.connect.commit()
            # 获取所有字段
            fields = self.cursor.fetchall()
            return fields
        except Exception as e:
            self.connect.rollback()
            print(">>>读取数据失败 执行MySQL: {} 时出错：{}".format(sql, e))

    # 读取原始表单
    def readC23Data(self):
        sql = '''
                select * from c23_data
            '''

        try:
            self.cursor.execute(sql)
            self.connect.commit()
            # 获取所有字段
            fields = self.cursor.fetchall()
            return fields
        except Exception as e:
            self.connect.rollback()
            print(">>>读取数据失败 执行MySQL: {} 时出错：{}".format(sql, e))

    # 读取原始表单
    def readIosData(self):
        sql = '''
                select * from ios_data
            '''

        try:
            self.cursor.execute(sql)
            self.connect.commit()
            # 获取所有字段
            fields = self.cursor.fetchall()
            return fields
        except Exception as e:
            self.connect.rollback()
            print(">>>读取数据失败 执行MySQL: {} 时出错：{}".format(sql, e))

    # 读取原始表单
    def readJavaData(self):
        sql = '''
                select * from java_data
            '''

        try:
            self.cursor.execute(sql)
            self.connect.commit()
            # 获取所有字段
            fields = self.cursor.fetchall()
            return fields
        except Exception as e:
            self.connect.rollback()
            print(">>>读取数据失败 执行MySQL: {} 时出错：{}".format(sql, e))

    # 读取原始表单
    def readOamData(self):
        sql = '''
                select * from oam_data
            '''

        try:
            self.cursor.execute(sql)
            self.connect.commit()
            # 获取所有字段
            fields = self.cursor.fetchall()
            return fields
        except Exception as e:
            self.connect.rollback()
            print(">>>读取数据失败 执行MySQL: {} 时出错：{}".format(sql, e))

    # 读取原始表单
    def readPhpData(self):
        sql = '''
                select * from php_data
            '''

        try:
            self.cursor.execute(sql)
            self.connect.commit()
            # 获取所有字段
            fields = self.cursor.fetchall()
            return fields
        except Exception as e:
            self.connect.rollback()
            print(">>>读取数据失败 执行MySQL: {} 时出错：{}".format(sql, e))

    # 读取原始表单
    def readPythonData(self):
        sql = '''
                select * from python_data
            '''

        try:
            self.cursor.execute(sql)
            self.connect.commit()
            # 获取所有字段
            fields = self.cursor.fetchall()
            return fields
        except Exception as e:
            self.connect.rollback()
            print(">>>读取数据失败 执行MySQL: {} 时出错：{}".format(sql, e))

    # 读取原始表单
    def readTestData(self):
        sql = '''
                select * from test_data
            '''

        try:
            self.cursor.execute(sql)
            self.connect.commit()
            # 获取所有字段
            fields = self.cursor.fetchall()
            return fields
        except Exception as e:
            self.connect.rollback()
            print(">>>读取数据失败 执行MySQL: {} 时出错：{}".format(sql, e))

    # 读取原始表单
    def readU3dueData(self):
        sql = '''
                select * from u3due_data
            '''

        try:
            self.cursor.execute(sql)
            self.connect.commit()
            # 获取所有字段
            fields = self.cursor.fetchall()
            return fields
        except Exception as e:
            self.connect.rollback()
            print(">>>读取数据失败 执行MySQL: {} 时出错：{}".format(sql, e))

    # 读取原始表单
    def readWebData(self):
        sql = '''
                select * from web_data
            '''

        try:
            self.cursor.execute(sql)
            self.connect.commit()
            # 获取所有字段
            fields = self.cursor.fetchall()
            return fields
        except Exception as e:
            self.connect.rollback()
            print(">>>读取数据失败 执行MySQL: {} 时出错：{}".format(sql, e))

    # 去除薪资为的实习岗位，便于计算

    '''================================================首页======================================================'''

    # 获取表数据量
    def getLastNum(self):
        sql = '''
                select num from handled_data
                where num = (select MAX(num) from handled_data)
            '''
        try:
            self.cursor.execute(sql)
            self.connect.commit()
            result = self.cursor.fetchone()
            last_num = result['num']
            return last_num
        except Exception as e:
            self.connect.rollback()
            print(">>>读取数据失败 执行MySQL: {} 时出错：{}".format(sql, e))

    # 查询最高薪资
    def getMaxSalary(self):
        sql = '''
                select max_salary from handled_data
                where max_salary = (select MAX(max_salary) from handled_data)
            '''

        try:
            self.cursor.execute(sql)
            self.connect.commit()
            result = self.cursor.fetchone()
            Max = result['max_salary']
            return Max
        except Exception as e:
            self.connect.rollback()
            print(">>>读取数据失败 执行MySQL: {} 时出错：{}".format(sql, e))

    # 查询最低薪资
    def getMinSalary(self):
        sql = '''
                select min_salary from handled_data
                where min_salary = (select MIN(min_salary) from handled_data)
            '''

        try:
            self.cursor.execute(sql)
            self.connect.commit()
            result = self.cursor.fetchone()
            Min = result['min_salary']
            return Min
        except Exception as e:
            self.connect.rollback()
            print(">>>读取数据失败 执行MySQL: {} 时出错：{}".format(sql, e))

    # 计算全部数据的平均薪资
    def getAllAvgSalary(self):
        sql = '''
                select avg_salary from handled_data
        '''
        try:
            self.cursor.execute(sql)
            self.connect.commit()
            result = self.cursor.fetchall()
            salary_list = []
            for var in result:
                salary_list.append(var['avg_salary'])
            avg = 0
            for var in salary_list:
                avg = avg + var
            return format(avg / len(salary_list), '.2f')
        except Exception as e:
            self.connect.rollback()
            print(">>>读取数据失败 执行MySQL: {} 时出错：{}".format(sql, e))

    # 查询所有学历的薪资平均情况
    def getSalaryForEducation(self):
        sql_0 = '''
                select  edu from educationforsalary
        '''
        sql_1 = '''
                select AVG(min) from educationforsalary
                where edu = %s
        '''
        sql_2 = '''
                select AVG(max) from educationforsalary
                where edu = %s
        '''
        sql_3 = '''
                select AVG(avg) from educationforsalary
                where edu = %s
        '''
        try:
            self.cursor.execute(sql_0)
            self.connect.commit()
            result = self.cursor.fetchall()
            education_list = []
            for var in result:
                education_list.append(var['edu'])
            edu = dict(Counter(education_list))
            for key in list(edu.keys()):
                if edu[key] < 26:
                    del edu[key]
                    continue
            edu_list = list(edu)
            # 最小值处理
            min_salary = []
            for var in edu_list:
                self.cursor.execute(sql_1, var)
                self.connect.commit()
                data = self.cursor.fetchall()
                min_salary.append(data)

            min_list = []
            for i in min_salary:
                for j in i:
                    min_list.append(round(float(j['AVG(min)']), 2))

            # 最大值
            max_salary = []
            for var in edu_list:
                self.cursor.execute(sql_2, var)
                self.connect.commit()
                data = self.cursor.fetchall()
                max_salary.append(data)

            max_list = []
            for i in max_salary:
                for j in i:
                    max_list.append(round(float(j['AVG(max)']), 2))

            # 平均值
            avg_salary = []
            for var in edu_list:
                self.cursor.execute(sql_3, var)
                self.connect.commit()
                data = self.cursor.fetchall()
                avg_salary.append(data)

            avg_list = []
            for i in avg_salary:
                for j in i:
                    avg_list.append(round(float(j['AVG(avg)']), 2))

            # 整合
            salary_list = []
            for i in range(len(edu)):
                data = [min_list[i], avg_list[i], max_list[i]]
                salary_list.append(data)

            return json.dumps({'name': edu_list, 'value': salary_list}, ensure_ascii=False)

        except Exception as e:
            self.connect.rollback()
            print(">>>读取数据失败 ：{}".format(e))

    # 查询总体学历
    def getEducation(self):
        sql = '''
                select education from handled_data
        '''
        try:
            self.cursor.execute(sql)
            self.connect.commit()
            result = self.cursor.fetchall()
            education_list = []
            for var in result:
                education_list.append(var['education'])
            edu = dict(Counter(education_list))
            for key in list(edu.keys()):
                if edu[key] < 26:
                    del edu[key]
                    continue
            edu_name = []
            edu_value = []

            for var in edu:
                edu_name.append(var)
                edu_value.append(edu[var])

            return json.dumps({'name': edu_name, 'value': edu_value}, ensure_ascii=False)

        except Exception as e:
            self.connect.rollback()
            print(">>>读取数据失败 执行MySQL: {} 时出错：{}".format(sql, e))

    # 查询总体经验与薪资关系
    def getSalaryForExperience(self):
        sql_0 = '''
                        select experience from experienceforsalary
                '''
        sql_1 = '''
                        select AVG(min_salary) from experienceforsalary
                        where experience = %s
                '''
        sql_2 = '''
                        select AVG(max_salary) from experienceforsalary
                        where experience = %s
                '''
        sql_3 = '''
                        select AVG(avg_salary) from experienceforsalary
                        where experience = %s
                '''
        try:
            self.cursor.execute(sql_0)
            self.connect.commit()
            result = self.cursor.fetchall()
            experience_list = []
            for var in result:
                experience_list.append(var['experience'])
            exp = dict(Counter(experience_list))
            for key in list(exp.keys()):
                if exp[key] < 188:
                    del exp[key]
                    continue
            exp_list = list(exp)

            # 最小值处理
            min_salary = []
            for var in exp_list:
                self.cursor.execute(sql_1, var)
                self.connect.commit()
                data = self.cursor.fetchall()
                min_salary.append(data)

            min_list = []
            for i in min_salary:
                for j in i:
                    min_list.append(round(float(j['AVG(min_salary)']), 2))

            # 最大值
            max_salary = []
            for var in exp_list:
                self.cursor.execute(sql_2, var)
                self.connect.commit()
                data = self.cursor.fetchall()
                max_salary.append(data)

            max_list = []
            for i in max_salary:
                for j in i:
                    max_list.append(round(float(j['AVG(max_salary)']), 2))

            # 平均值
            avg_salary = []
            for var in exp_list:
                self.cursor.execute(sql_3, var)
                self.connect.commit()
                data = self.cursor.fetchall()
                avg_salary.append(data)

            avg_list = []
            for i in avg_salary:
                for j in i:
                    avg_list.append(round(float(j['AVG(avg_salary)']), 2))

            # 整合
            salary_list = []
            for i in range(len(exp_list)):
                data = [min_list[i], avg_list[i], max_list[i]]
                salary_list.append(data)

            return json.dumps({'name': exp_list, 'value': salary_list}, ensure_ascii=False)

        except Exception as e:
            self.connect.rollback()
            print(">>>读取数据失败 ：{}".format(e))

    # 查询总体经验
    def getExperience(self):
        sql = '''
                select experience from handled_data
                       
        '''
        try:
            self.cursor.execute(sql)
            self.connect.commit()
            result = self.cursor.fetchall()
            experience_list = []
            for var in result:
                experience_list.append(var['experience'])
            exp = dict(Counter(experience_list))
            for key in list(exp.keys()):
                if exp[key] < 188:
                    del exp[key]
                    continue
            exp_name = []
            exp_value = []
            for var in exp:
                exp_name.append(var)
                exp_value.append(exp[var])

            return json.dumps({'name': exp_name, 'value': exp_value}, ensure_ascii=False)

        except Exception as e:
            self.connect.rollback()
            print(">>>读取数据失败 执行MySQL: {} 时出错：{}".format(sql, e))

    # 查询总体城市情况情况
    def getCity(self):
        sql = '''
                select city from handled_data 
        '''
        try:
            self.cursor.execute(sql)
            self.connect.commit()
            result = self.cursor.fetchall()
            city = []
            for var in result:
                city.append(var['city'])
            return list(set(city))
        except Exception as e:
            self.connect.rollback()
            print(">>>读取数据失败 执行MySQL: {} 时出错：{}".format(sql, e))

    # 查询所有岗位名称
    def getJobName(self):
        sql = '''
                select name from handled_data 
        '''
        try:
            self.cursor.execute(sql)
            self.connect.commit()
            result = self.cursor.fetchall()
            name = []
            for var in result:
                name.append(var['name'])
            return name
        except Exception as e:
            self.connect.rollback()
            print(">>>读取数据失败 执行MySQL: {} 时出错：{}".format(sql, e))

    # 城市平均待遇
    def getSalaryForCity(self):

        city = self.getCity()
        sql = '''
                select AVG(salary) from salaryforcity
                where city = %s
          '''
        try:
            salary = []
            for var in city:
                self.cursor.execute(sql, var)
                self.connect.commit()
                result = self.cursor.fetchall()
                salary.append(result)

            salary_list = []
            for var in salary:
                for i in var:
                    salary_list.append(round(int(i['AVG(salary)'])))

            return json.dumps({'name': city, 'value': salary_list}, ensure_ascii=False)



        except Exception as e:
            self.connect.rollback()
            print(">>>读取数据失败 执行MySQL: {} 时出错：{}".format(sql, e))

    # 福利查询
    def getWelfare(self):
        sql = '''
                select welfare from handled_data
        '''
        try:
            self.cursor.execute(sql)
            self.connect.commit()
            result = self.cursor.fetchall()
            welfare = []
            for var in result:
                welfare.append(var['welfare'])
            return welfare

        except Exception as e:
            self.connect.rollback()
            print(">>>读取数据失败 执行MySQL: {} 时出错：{}".format(sql, e))

    '''================================================Java======================================================'''

