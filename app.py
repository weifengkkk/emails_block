from flask import Flask, render_template, request

import mysql
import json
import cloud
import match
import speculate

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


'''Java'''


@app.route('/javaGetEdu')
@cache.cached(timeout=timeout)
def javaGetEdu():
    sql = mysql.MySql()
    edu = sql.javaGetEdu()
    return edu


@app.route('/javaGetExp')
@cache.cached(timeout=timeout)
def javaGetExp():
    sql = mysql.MySql()
    exp = sql.javaGetExp()
    return exp


@app.route('/javaGetEduForSalary')
@cache.cached(timeout=timeout)
def javaGetEduForSalary():
    sql = mysql.MySql()
    salary = sql.javaGetEduForSalary()
    return salary


@app.route('/javaGetExpForSalary')
@cache.cached(timeout=timeout)
def javaGetExpForSalary():
    sql = mysql.MySql()
    salary = sql.javaGetExpForSalary()
    return salary


@app.route('/javaRadar')
@cache.cached(timeout=timeout)
def javaRadar():
    sql = mysql.MySql()
    radar = sql.javaRadar()
    return radar


@app.route('/java.html')
@cache.cached(timeout=timeout)
def java():
    wc = cloud.Cloud()
    wc.javaSkillCloud()
    sql = mysql.MySql()
    num = sql.javaGetNum()
    avg = sql.javaGetAvgSalary()
    Max = sql.javaGetMaxSalary()
    Min = sql.javaGetMinSalary()
    return render_template('java.html', num=num, avg=avg, Max=Max, Min=Min)


'''python'''


@app.route('/pythonGetEdu')
@cache.cached(timeout=timeout)
def pythonGetEdu():
    sql = mysql.MySql()
    edu = sql.pythonGetEdu()
    return edu


@app.route('/pythonGetExp')
@cache.cached(timeout=timeout)
def pythonGetExp():
    sql = mysql.MySql()
    exp = sql.pythonGetExp()
    return exp


@app.route('/pythonGetEduForSalary')
@cache.cached(timeout=timeout)
def pythonGetEduForSalary():
    sql = mysql.MySql()
    salary = sql.pythonGetEduForSalary()
    return salary


@app.route('/pythonGetExpForSalary')
@cache.cached(timeout=timeout)
def pythonGetExpForSalary():
    sql = mysql.MySql()
    salary = sql.pythonGetExpForSalary()
    return salary


@app.route('/pythonRadar')
@cache.cached(timeout=timeout)
def pythonRadar():
    sql = mysql.MySql()
    radar = sql.pythonRadar()
    return radar


@app.route('/python.html')
@cache.cached(timeout=timeout)
def python():
    wc = cloud.Cloud()
    wc.pythonSkillCloud()
    sql = mysql.MySql()
    num = sql.pythonGetNum()
    avg = sql.pythonGetAvgSalary()
    Max = sql.pythonGetMaxSalary()
    Min = sql.pythonGetMinSalary()
    return render_template('python.html', num=num, avg=avg, Max=Max, Min=Min)


'''c'''


@app.route('/cGetEdu')
@cache.cached(timeout=timeout)
def cGetEdu():
    sql = mysql.MySql()
    edu = sql.cGetEdu()
    return edu


@app.route('/cGetExp')
@cache.cached(timeout=timeout)
def cGetExp():
    sql = mysql.MySql()
    exp = sql.cGetExp()
    return exp


@app.route('/cGetEduForSalary')
@cache.cached(timeout=timeout)
def cGetEduForSalary():
    sql = mysql.MySql()
    salary = sql.cGetEduForSalary()
    return salary


@app.route('/cGetExpForSalary')
@cache.cached(timeout=timeout)
def cGetExpForSalary():
    sql = mysql.MySql()
    salary = sql.cGetExpForSalary()
    return salary


@app.route('/cRadar')
@cache.cached(timeout=timeout)
def cRadar():
    sql = mysql.MySql()
    radar = sql.cRadar()
    return radar


@app.route('/c.html')
@cache.cached(timeout=timeout)
def c():
    wc = cloud.Cloud()
    wc.cSkillCloud()
    sql = mysql.MySql()
    num = sql.cGetNum()
    avg = sql.cGetAvgSalary()
    Max = sql.cGetMaxSalary()
    Min = sql.cGetMinSalary()
    return render_template('c.html', num=num, avg=avg, Max=Max, Min=Min)


'''c23'''


@app.route('/c23GetEdu')
@cache.cached(timeout=timeout)
def c23GetEdu():
    sql = mysql.MySql()
    edu = sql.c23GetEdu()
    return edu


@app.route('/c23GetExp')
@cache.cached(timeout=timeout)
def c23GetExp():
    sql = mysql.MySql()
    exp = sql.c23GetExp()
    return exp


@app.route('/c23GetEduForSalary')
@cache.cached(timeout=timeout)
def c23GetEduForSalary():
    sql = mysql.MySql()
    salary = sql.c23GetEduForSalary()
    return salary


@app.route('/c23GetExpForSalary')
@cache.cached(timeout=timeout)
def c23GetExpForSalary():
    sql = mysql.MySql()
    salary = sql.c23GetExpForSalary()
    return salary


@app.route('/c23Radar')
@cache.cached(timeout=timeout)
def c23Radar():
    sql = mysql.MySql()
    radar = sql.c23Radar()
    return radar


@app.route('/c23.html')
@cache.cached(timeout=timeout)
def c23():
    wc = cloud.Cloud()
    wc.c23SkillCloud()
    sql = mysql.MySql()
    num = sql.c23GetNum()
    avg = sql.c23GetAvgSalary()
    Max = sql.c23GetMaxSalary()
    Min = sql.c23GetMinSalary()
    return render_template('c23.html', num=num, avg=avg, Max=Max, Min=Min)


'''u3dUe'''


@app.route('/u3dUeGetEdu')
@cache.cached(timeout=timeout)
def u3dUeGetEdu():
    sql = mysql.MySql()
    edu = sql.u3dUeGetEdu()
    return edu


@app.route('/u3dUeGetExp')
@cache.cached(timeout=timeout)
def u3dUeGetExp():
    sql = mysql.MySql()
    exp = sql.u3dUeGetExp()
    return exp


@app.route('/u3dUeGetEduForSalary')
@cache.cached(timeout=timeout)
def u3dUeGetEduForSalary():
    sql = mysql.MySql()
    salary = sql.u3dUeGetEduForSalary()
    return salary


@app.route('/u3dUeGetExpForSalary')
@cache.cached(timeout=timeout)
def u3dUeGetExpForSalary():
    sql = mysql.MySql()
    salary = sql.u3dUeGetExpForSalary()
    return salary


@app.route('/u3dUeRadar')
@cache.cached(timeout=timeout)
def u3dUeRadar():
    sql = mysql.MySql()
    radar = sql.u3dUeRadar()
    return radar


@app.route('/u3d-ue.html')
@cache.cached(timeout=timeout)
def u3dUe():
    wc = cloud.Cloud()
    wc.u3dUeSkillCloud()
    sql = mysql.MySql()
    num = sql.u3dUeGetNum()
    avg = sql.u3dUeGetAvgSalary()
    Max = sql.u3dUeGetMaxSalary()
    Min = sql.u3dUeGetMinSalary()
    return render_template('u3d-ue.html', num=num, avg=avg, Max=Max, Min=Min)


'''php'''


@app.route('/phpGetEdu')
@cache.cached(timeout=timeout)
def phpGetEdu():
    sql = mysql.MySql()
    edu = sql.phpGetEdu()
    return edu


@app.route('/phpGetExp')
@cache.cached(timeout=timeout)
def phpGetExp():
    sql = mysql.MySql()
    exp = sql.phpGetExp()
    return exp


@app.route('/phpGetEduForSalary')
@cache.cached(timeout=timeout)
def phpGetEduForSalary():
    sql = mysql.MySql()
    salary = sql.phpGetEduForSalary()
    return salary


@app.route('/phpGetExpForSalary')
@cache.cached(timeout=timeout)
def phpGetExpForSalary():
    sql = mysql.MySql()
    salary = sql.phpGetExpForSalary()
    return salary


@app.route('/phpRadar')
@cache.cached(timeout=timeout)
def phpRadar():
    sql = mysql.MySql()
    radar = sql.phpRadar()
    return radar


@app.route('/php.html')
@cache.cached(timeout=timeout)
def php():
    wc = cloud.Cloud()
    wc.phpSkillCloud()
    sql = mysql.MySql()
    num = sql.phpGetNum()
    avg = sql.phpGetAvgSalary()
    Max = sql.phpGetMaxSalary()
    Min = sql.phpGetMinSalary()
    return render_template('php.html', num=num, avg=avg, Max=Max, Min=Min)


'''web'''


@app.route('/webGetEdu')
@cache.cached(timeout=timeout)
def webGetEdu():
    sql = mysql.MySql()
    edu = sql.webGetEdu()
    return edu


@app.route('/webGetExp')
@cache.cached(timeout=timeout)
def webGetExp():
    sql = mysql.MySql()
    exp = sql.webGetExp()
    return exp


@app.route('/webGetEduForSalary')
@cache.cached(timeout=timeout)
def webGetEduForSalary():
    sql = mysql.MySql()
    salary = sql.webGetEduForSalary()
    return salary


@app.route('/webGetExpForSalary')
@cache.cached(timeout=timeout)
def webGetExpForSalary():
    sql = mysql.MySql()
    salary = sql.webGetExpForSalary()
    return salary


@app.route('/webRadar')
@cache.cached(timeout=timeout)
def webRadar():
    sql = mysql.MySql()
    radar = sql.webRadar()
    return radar


@app.route('/web.html')
@cache.cached(timeout=timeout)
def web():
    wc = cloud.Cloud()
    wc.webSkillCloud()
    sql = mysql.MySql()
    num = sql.webGetNum()
    avg = sql.webGetAvgSalary()
    Max = sql.webGetMaxSalary()
    Min = sql.webGetMinSalary()
    return render_template('web.html', num=num, avg=avg, Max=Max, Min=Min)


'''Android'''


@app.route('/AndroidGetEdu')
@cache.cached(timeout=timeout)
def AndroidGetEdu():
    sql = mysql.MySql()
    edu = sql.AndroidGetEdu()
    return edu


@app.route('/AndroidGetExp')
@cache.cached(timeout=timeout)
def AndroidGetExp():
    sql = mysql.MySql()
    exp = sql.AndroidGetExp()
    return exp


@app.route('/AndroidGetEduForSalary')
@cache.cached(timeout=timeout)
def AndroidGetEduForSalary():
    sql = mysql.MySql()
    salary = sql.AndroidGetEduForSalary()
    return salary


@app.route('/AndroidGetExpForSalary')
@cache.cached(timeout=timeout)
def AndroidGetExpForSalary():
    sql = mysql.MySql()
    salary = sql.AndroidGetExpForSalary()
    return salary


@app.route('/AndroidRadar')
@cache.cached(timeout=timeout)
def AndroidRadar():
    sql = mysql.MySql()
    radar = sql.AndroidRadar()
    return radar


@app.route('/android.html')
@cache.cached(timeout=timeout)
def Android():
    wc = cloud.Cloud()
    wc.AndroidSkillCloud()
    sql = mysql.MySql()
    num = sql.AndroidGetNum()
    avg = sql.AndroidGetAvgSalary()
    Max = sql.AndroidGetMaxSalary()
    Min = sql.AndroidGetMinSalary()
    return render_template('android.html', num=num, avg=avg, Max=Max, Min=Min)


'''ios'''


@app.route('/iosGetEdu')
@cache.cached(timeout=timeout)
def iosGetEdu():
    sql = mysql.MySql()
    edu = sql.iosGetEdu()
    return edu


@app.route('/iosGetExp')
@cache.cached(timeout=timeout)
def iosGetExp():
    sql = mysql.MySql()
    exp = sql.iosGetExp()
    return exp


@app.route('/iosGetEduForSalary')
@cache.cached(timeout=timeout)
def iosGetEduForSalary():
    sql = mysql.MySql()
    salary = sql.iosGetEduForSalary()
    return salary


@app.route('/iosGetExpForSalary')
@cache.cached(timeout=timeout)
def iosGetExpForSalary():
    sql = mysql.MySql()
    salary = sql.iosGetExpForSalary()
    return salary


@app.route('/iosRadar')
@cache.cached(timeout=timeout)
def iosRadar():
    sql = mysql.MySql()
    radar = sql.iosRadar()
    return radar


@app.route('/ios.html')
@cache.cached(timeout=timeout)
def ios():
    wc = cloud.Cloud()
    wc.iosSkillCloud()
    sql = mysql.MySql()
    num = sql.iosGetNum()
    avg = sql.iosGetAvgSalary()
    Max = sql.iosGetMaxSalary()
    Min = sql.iosGetMinSalary()
    return render_template('ios.html', num=num, avg=avg, Max=Max, Min=Min)


'''algorithm'''


@app.route('/algorithmGetEdu')
@cache.cached(timeout=timeout)
def algorithmGetEdu():
    sql = mysql.MySql()
    edu = sql.algorithmGetEdu()
    return edu


@app.route('/algorithmGetExp')
@cache.cached(timeout=timeout)
def algorithmGetExp():
    sql = mysql.MySql()
    exp = sql.algorithmGetExp()
    return exp


@app.route('/algorithmGetEduForSalary')
@cache.cached(timeout=timeout)
def algorithmGetEduForSalary():
    sql = mysql.MySql()
    salary = sql.algorithmGetEduForSalary()
    return salary


@app.route('/algorithmGetExpForSalary')
@cache.cached(timeout=timeout)
def algorithmGetExpForSalary():
    sql = mysql.MySql()
    salary = sql.algorithmGetExpForSalary()
    return salary


@app.route('/algorithmRadar')
@cache.cached(timeout=timeout)
def algorithmRadar():
    sql = mysql.MySql()
    radar = sql.algorithmRadar()
    return radar


@app.route('/algorithm.html')
@cache.cached(timeout=timeout)
def algorithm():
    wc = cloud.Cloud()
    wc.algorithmSkillCloud()
    sql = mysql.MySql()
    num = sql.algorithmGetNum()
    avg = sql.algorithmGetAvgSalary()
    Max = sql.algorithmGetMaxSalary()
    Min = sql.algorithmGetMinSalary()
    return render_template('algorithm.html', num=num, avg=avg, Max=Max, Min=Min)


'''test'''


@app.route('/testGetEdu')
@cache.cached(timeout=timeout)
def testGetEdu():
    sql = mysql.MySql()
    edu = sql.testGetEdu()
    return edu


@app.route('/testGetExp')
@cache.cached(timeout=timeout)
def testGetExp():
    sql = mysql.MySql()
    exp = sql.testGetExp()
    return exp


@app.route('/testGetEduForSalary')
@cache.cached(timeout=timeout)
def testGetEduForSalary():
    sql = mysql.MySql()
    salary = sql.testGetEduForSalary()
    return salary


@app.route('/testGetExpForSalary')
@cache.cached(timeout=timeout)
def testGetExpForSalary():
    sql = mysql.MySql()
    salary = sql.testGetExpForSalary()
    return salary


@app.route('/testRadar')
@cache.cached(timeout=timeout)
def testRadar():
    sql = mysql.MySql()
    radar = sql.testRadar()
    return radar


@app.route('/test.html')
@cache.cached(timeout=timeout)
def test():
    wc = cloud.Cloud()
    wc.testSkillCloud()
    sql = mysql.MySql()
    num = sql.testGetNum()
    avg = sql.testGetAvgSalary()
    Max = sql.testGetMaxSalary()
    Min = sql.testGetMinSalary()
    return render_template('test.html', num=num, avg=avg, Max=Max, Min=Min)


'''oam'''


@app.route('/oamGetEdu')
@cache.cached(timeout=timeout)
def oamGetEdu():
    sql = mysql.MySql()
    edu = sql.oamGetEdu()
    return edu


@app.route('/oamGetExp')
@cache.cached(timeout=timeout)
def oamGetExp():
    sql = mysql.MySql()
    exp = sql.oamGetExp()
    return exp


@app.route('/oamGetEduForSalary')
@cache.cached(timeout=timeout)
def oamGetEduForSalary():
    sql = mysql.MySql()
    salary = sql.oamGetEduForSalary()
    return salary


@app.route('/oamGetExpForSalary')
@cache.cached(timeout=timeout)
def oamGetExpForSalary():
    sql = mysql.MySql()
    salary = sql.oamGetExpForSalary()
    return salary


@app.route('/oamRadar')
@cache.cached(timeout=timeout)
def oamRadar():
    sql = mysql.MySql()
    radar = sql.oamRadar()
    return radar


@app.route('/oam.html')
@cache.cached(timeout=timeout)
def oam():
    wc = cloud.Cloud()
    wc.oamSkillCloud()
    sql = mysql.MySql()
    num = sql.oamGetNum()
    avg = sql.oamGetAvgSalary()
    Max = sql.oamGetMaxSalary()
    Min = sql.oamGetMinSalary()
    return render_template('oam.html', num=num, avg=avg, Max=Max, Min=Min)


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
