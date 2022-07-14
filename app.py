from flask import Flask, render_template, request

import mysql
import json
import match

from flask_caching import Cache

app = Flask(__name__)

'''缓存设置'''
cache = Cache()
cache.init_app(app=app, config={'CACHE_TYPE': 'simple'})
timeout = 300

'''首页'''

# @app.route('/')
# @cache.cached(timeout=timeout)
# def frontPage():
#     wc = cloud.Cloud()
#     wc.popularJobs()
#     wc.welfareCloud()
#     sql = mysql.MySql()
#     num = sql.getLastNum()
#     avg = sql.getAllAvgSalary()
#     Max = sql.getMaxSalary()
#     Min = sql.getMinSalary()
#     return render_template('index.html', num=num, avg=avg, Max=Max, Min=Min)
#

@app.route('/radar')
@cache.cached(timeout=timeout)
def Radar():
    sql = mysql.MySql()
    radar_name = [json.loads(sql.cRadar())['name'].replace('c', 'C/C++'),
                  json.loads(sql.javaRadar())['name'].replace('java', 'Java'),
                  json.loads(sql.pythonRadar())['name'].replace('python', 'Python'),
                  json.loads(sql.phpRadar())['name'].replace('php', 'PHP'),
                  json.loads(sql.iosRadar())['name'].replace('ios', 'IOS'),
                  json.loads(sql.webRadar())['name'].replace('web', 'web前端'),
                  json.loads(sql.AndroidRadar())['name'],
                  json.loads(sql.u3dUeRadar())['name'].replace('u3dUe', 'Unity3D/UE'),
                  json.loads(sql.c23Radar())['name'].replace('c23', 'C#/.NET'),
                  json.loads(sql.algorithmRadar())['name'].replace('algorithm', '算法'),
                  json.loads(sql.testRadar())['name'].replace('test', '测试'),
                  json.loads(sql.oamRadar())['name'].replace('oam', '运维')]

    radar_value = [json.loads(sql.cRadar())['value'],
                   json.loads(sql.javaRadar())['value'],
                   json.loads(sql.pythonRadar())['value'],
                   json.loads(sql.phpRadar())['value'],
                   json.loads(sql.iosRadar())['value'],
                   json.loads(sql.webRadar())['value'],
                   json.loads(sql.AndroidRadar())['value'],
                   json.loads(sql.u3dUeRadar())['value'],
                   json.loads(sql.c23Radar())['value'],
                   json.loads(sql.algorithmRadar())['value'],
                   json.loads(sql.testRadar())['value'],
                   json.loads(sql.oamRadar())['value']]
    return json.dumps({'name': radar_name, 'value': radar_value}, ensure_ascii=False)


@app.route('/getEducation')
@cache.cached(timeout=timeout)
def getEducation():
    sql = mysql.MySql()
    edu = sql.getEducation()
    return edu


@app.route('/getSalaryForEducation')
@cache.cached(timeout=timeout)
def getSalaryForEducation():
    sql = mysql.MySql()
    salary = sql.getSalaryForEducation()
    return salary


@app.route('/getExperience')
@cache.cached(timeout=timeout)
def getExperience():
    sql = mysql.MySql()
    exp = sql.getExperience()
    return exp


@app.route('/getSalaryForExperience')
@cache.cached(timeout=timeout)
def getSalaryForExperience():
    sql = mysql.MySql()
    salary = sql.getSalaryForExperience()
    return salary


@app.route('/getSalaryForCity')
@cache.cached(timeout=timeout)
def getSalaryForCity():
    sql = mysql.MySql()
    salary = sql.getSalaryForCity()
    return salary


@app.route('/index.html')
@cache.cached(timeout=timeout)
def index():
    wc = cloud.Cloud()
    wc.popularJobs()
    wc.welfareCloud()
    sql = mysql.MySql()
    num = sql.getLastNum()
    avg = sql.getAllAvgSalary()
    Max = sql.getMaxSalary()
    Min = sql.getMinSalary()
    return render_template('index.html', num=num, avg=avg, Max=Max, Min=Min)







'''匹配'''


@app.route('/job-speculate.html', methods=['GET', 'POST'])
def job_speculate():
    return render_template('job-speculate.html')


@app.route('/job-submit', methods=['GET', 'POST'])
def job_submit():
    if request.method == 'POST':
        city = request.form.get('city')
        area = request.form.get('area')
        job = request.form.get('job')
        priority = request.form.get('priority')
        salary = float(request.form.get('salary'))
        education = request.form.get('education')
        experience = request.form.get('experience')
        skill = request.form.get('skill')
        skill_list = skill.split(' ')
        mc = match.Match()
        job_data = mc.jobMach(lan=job, city=city, area=area, priority=priority, education=education,
                              experience=experience, skill=skill_list, salary=salary)
        if job_data is not None and job_data is not False:
            city_data = job_data['city']
            area_data = job_data['area']
            name_data = job_data['name']
            detail_data = job_data['detail']
            scale_data = job_data['scale']
            company_data = job_data['company']
            label_data = job_data['label']
            edu_data = job_data['education']
            exp_data = job_data['experience']
            salary_data = str(int(job_data['avg_salary']))
            skill_data = job_data['skill']
            welfare_data = job_data['welfare']
            return render_template('match-result.html', name=name_data, job=job, company=company_data,
                                   label=label_data, scale=scale_data, city=city_data, area=area_data,
                                   detail=detail_data, edu=edu_data, exp=exp_data, salary=salary_data,
                                   skill=skill_data, welfare=welfare_data)
        else:
            return render_template('match-result.html')
    else:
        return render_template("404.html")


@app.route('/salary-speculate.html')
def salary_speculate():
    return render_template('salary-speculate.html')

@app.route('/salary-submit', methods=['GET', 'POST'])
def salary_submit():
    if request.method == 'POST':
        city = request.form.get('city')
        area = request.form.get('area')
        job = request.form.get('job')
        scale = request.form.get('scale')
        name = request.form.get('name')
        education = request.form.get('education')
        experience = request.form.get('experience')
        skill = request.form.get('skill')
        skill_list = skill.split(' ')

        sp = speculate.Speculate()
        result = sp.salarySpeculate(city=city, area=area, lan=job, scale=scale, name=name, education=education,
                                    experience=experience, skill=skill_list)
        if result is not None and result != 0:
            up = str(int((result + 1000) / 1000)) + "K"
            low = str(int((result - 1000) / 1000))
            value = low + "~" + up + "￥/月"

            return render_template("speculate-result.html", value=value)
        else:
            return render_template("speculate-result.html", value="此条件无法估测")
    else:
        return render_template("404.html")

@app.route('/charts.html')
@cache.cached(timeout=timeout)
def charts():
    return render_template("charts.html")


@app.route('/cards.html')
@cache.cached(timeout=timeout)
def cards():
    return render_template("demo/cards.html")


@app.route('/forgot-password.html')
def forgotPassword():
    return render_template("forgot-password.html")


@app.route('/')
def login():
    return render_template("login.html")


@app.route('/tables.html')
@cache.cached(timeout=timeout)
def tables():
    return render_template("tables.html")


@app.route('/register.html')
def register():
    return render_template("register.html")


@app.route('/404.html')
def error():
    return render_template("404.html")


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
