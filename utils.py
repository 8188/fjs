from config import *
from celery import Celery
import celeryconfig
import pandas as pd
import numpy as np
from statsmodels.tsa.api import adfuller, VAR
from statsmodels.tsa.vector_ar import vecm
from time import time, sleep
from datetime import datetime, timedelta
import orjson as json
from pythresh.thresholds.comb import COMB
from outliers import smirnov_grubbs as grubbs
from pyod.models import lof, cof, inne, ecod, lscp
import sranodec as anom
import redis
import paho.mqtt.client as mqtt
from queue import Queue
from functools import lru_cache


if not TEST:
    import warnings
    warnings.filterwarnings("ignore")
    import zeep
    wsdlClient = zeep.Client(wsdl=f"{ASMX_URL}?WSDL")

INTERVAL = 10
HISTORY_STEP = 25
FORECAST_STEP = 5
FORECAST_INTERVAL = 60
ANOMALY_COL_RATIO = 3
GRUBBS_SCORE = 10
LSCP_SCORE = 30
SRA_SCORE = 10
ANOMALY_SOCRE = 50
ALERT_SCORE = 50
PER_ALERT_SCORE = 5
DECIMALS = 3
ADFULLER_THRESHOLD_VAL = 0.05
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
MAX_QUEUE_SIZE = 10
TESTFILE = "test.feather"


@lru_cache(maxsize=None)
def codes_with_unit(codes, unit):
    if TEST:
        suffix = ""
        unit = '1'
    else:
        suffix = "_AVALUE"
    return [unit + item + suffix for item in codes]


# 调节阀开度
regulatorValveOpening = ( 
    "GRE017MM",
    "GRE027MM",
    "GRE037MM",
    "GRE047MM",
    "GRE117MM",
    "GRE127MM",
    "GRE137MM",
    "GRE147MM",
)

# 调节阀腔室油压
regulatorValveChamberOilPressure = ( 
    "GRE016MP",
    "GRE026MP",
    "GRE036MP",
    "GRE046MP",
    "GRE116MP",
    "GRE126MP",
    "GRE136MP",
    "GRE146MP",
)

# 油位
oilLevel = ( 
    "GFR001MN",
    "GFR002MN",
    "GFR003MN",
)

# 压力
oilPressure = ( 
    "GFR011MP",
    "GFR012MP",
    "GFR013MP",
)

# 温度
oilTemperature = ( 
    "GFR006MT",  # 油温
    "GFR008MT",  # 加热器
)


@lru_cache(maxsize=None)
def f_targets(unit):
    return (
        codes_with_unit(regulatorValveOpening, unit)
        + codes_with_unit(regulatorValveChamberOilPressure, unit)
        + codes_with_unit(oilLevel, unit)
        + codes_with_unit(oilPressure, unit)
        + codes_with_unit(oilTemperature, unit)
    )


# 主汽阀开度
mainValveOpening = ( 
    "GSE011MM",
    "GSE021MM",
    "GSE031MM",
    "GSE041MM",
    "GSE111MM",
    "GSE121MM",
    "GSE131MM",
    "GSE141MM",
)

# 主汽阀腔室油压
mainValveChamberOilPressure = ( 
    "GSE014MP",
    "GSE024MP",
    "GSE034MP",
    "GSE044MP",
    "GSE114MP",
    "GSE124MP",
    "GSE134MP",
    "GSE144MP",
)

# 过滤器差压
filterPressure = ( 
    "GFR039KS",
    "GFR046KS",
    "GFR043KS",
) if not TEST else ()

# 抗燃油泵电流
pumpCurrent = ( 
    "GFR011MI",
    "GFR111MI",
) if not TEST else ()

# 安全油压
safeOilPressure = (
    "GSE202MP",
    "GSE203MP",
)

# 汽轮机转速
turbineSpeed = (
    "GSE001MC_XF60_AVALUE" if TEST else "GSE001MC_XF60",
    "GSE002MC_XF60_AVALUE" if TEST else "GSE002MC_XF60",
    "GSE003MC_XF60",
)

# 盘车
jigger = (
    "GGR001MI",  # 盘车电流
    "GGR001SM" if TEST else "GGR001SM_LVALUE",  # 盘车投入
    "GGR002SM" if TEST else "GGR002SM_LVALUE",  # 盘车退出
)

others = (
    "GME701MP",  # 高压进汽压力
    "GME702MP",
    "GME703MP",
    "GME711MP",  # 中压进汽压力
    "GRE012MY",  # 有功功率
)


@lru_cache(maxsize=None)
def a_targets(unit):
    return (
        codes_with_unit(others, unit)
        + codes_with_unit(jigger, unit)
        + codes_with_unit(turbineSpeed, unit)
        + codes_with_unit(safeOilPressure, unit)
        + f_targets(unit)
        + codes_with_unit(mainValveOpening, unit)
        + codes_with_unit(mainValveChamberOilPressure, unit)
        + codes_with_unit(filterPressure, unit)
        + codes_with_unit(pumpCurrent, unit)
    )


@lru_cache(maxsize=None)
def target_list(unit):
    return (
        codes_with_unit(regulatorValveOpening, unit)[:4],  # 高压调节阀开度
        codes_with_unit(regulatorValveOpening, unit)[4:],  # 中压调节阀开度
        codes_with_unit(oilLevel, unit),  # 抗燃油液位
        codes_with_unit(oilPressure, unit),  # 压力
        codes_with_unit(oilTemperature, unit),  # 温度
        codes_with_unit(regulatorValveChamberOilPressure, unit),  # 调节阀腔室油压
    )


regulatorValveTags = (
    "GRE001VV",
    "GRE002VV",
    "GRE003VV",
    "GRE004VV",
    "GRE011VV",
    "GRE012VV",
    "GRE013VV",
    "GRE014VV",
)

mainValveTags = (
    "GSE001VV",
    "GSE002VV",
    "GSE003VV",
    "GSE004VV",
    "GSE011VV",
    "GSE012VV",
    "GSE013VV",
    "GSE014VV",
)

f_df = [pd.DataFrame() for _ in range(2)]
a_df = [pd.DataFrame() for _ in range(2)]
c_df = [pd.Series(dtype='object') for _ in range(2)]
status = [{} for _ in range(2)]
alerts = [{"alarms": [], "timestamp": ""} for _ in range(2)]
vrfData = [{} for _ in range(2)]
healthQ = [Queue(maxsize=MAX_QUEUE_SIZE) for _ in range(2)]

app = Celery()
app.config_from_object(celeryconfig)


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT broker")
    else:
        print("Failed to connect to MQTT broker")


def on_publish(client, userdata, mid):
    pass


def on_disconnect(client, userdata, rc):
    if rc != 0:
        try:
            print("Connection lost. Reconnecting...")
            client.reconnect()
        except:
            print("Failed to reconnect to MQTT broker")
            sleep(5)


class MQTTClientSingleton:
    _instance = None

    def __new__(cls, host=MQTT_IP, port=MQTT_PORT, user="", password=""):
        print("----build new MQTT connection----")
        if cls._instance is None:
            cls._instance = mqtt.Client()
            cls._instance.on_connect = on_connect
            cls._instance.on_publish = on_publish
            cls._instance.on_disconnect = on_disconnect
            if user and password:
                cls._instance.username_pw_set(user, password)
            cls._instance.connect(host, port)
        return cls._instance

    def __del__(self):
        self._instance.disconnect()


MQTTClient = MQTTClientSingleton()


class RedisSingleton:
    _instance = None

    def __new__(cls, host=REDIS_IP, port=REDIS_PORT, db=REDIS_DB):
        if cls._instance is None:
            print("----build new Redis connection----")
            cls._instance = object.__new__(cls)
            cls._instance.conn = redis.StrictRedis(host=host, port=port, db=db)
        return cls._instance.conn

    def __del__(self):
        self._instance.conn.close()


redis_conn = RedisSingleton()


@app.task
def sdk_get_forecast_df(unit):
    global f_df
    iUnit = int(unit) - 1
    f_df[iUnit] = pd.DataFrame()
    if TEST:
        f_df[iUnit] = pd.read_feather(TESTFILE)[f_targets(unit)].tail(24 * 60)
    else:
        now = datetime.now()
        for kks in f_targets(unit):
            r = wsdlClient.service.HistTimeSpan(
                kks=kks,
                begin=(now - timedelta(hours=24)).strftime(DATE_FORMAT),
                end=now.strftime(DATE_FORMAT),
                seconds=60,
            )
            r = r.replace('NaN', 'null')
            json_dict = json.loads(r)

            pointValues = []
            timestamp = []
            # r的最后一个数据是0，所以舍弃
            for item in json_dict["Result"][:-1]:
                pointValues.append(item["Value"])
                timestamp.append(item["Time"])
            # if pointValues == []:
            #     print(kks)
            f_df[iUnit][kks] = pd.Series(pointValues, index=timestamp)
        # print(f_df[iUnit].shape)
    f_df[iUnit] += np.random.uniform(0, 0.001, f_df[iUnit].shape)


@app.task
def sdk_get_current_df(unit):
    global c_df
    iUnit = int(unit) - 1
    c_df[iUnit] = pd.DataFrame()
    if TEST:
        c_df[iUnit] = pd.read_feather(TESTFILE)[a_targets(unit)].tail(1).iloc[-1]
    else:
        r = wsdlClient.service.SnapX(json.dumps(a_targets(unit)))
        r = r.replace('NaN', 'null')
        json_dict = json.loads(r)
  
        pointValues = []
        pointNames = []
        for item in json_dict["Result"]:
            pointValues.append(item["Value"])
            pointNames.append(item["PointName"])

        c_df[iUnit] = pd.Series(pointValues, pointNames).round(DECIMALS)
        # print(c_df[iUnit].shape)


@app.task
def sdk_get_anomaly_df(unit):
    global a_df
    iUnit = int(unit) - 1
    a_df[iUnit] = pd.DataFrame()
    if TEST:
        a_df[iUnit] = pd.read_feather(TESTFILE)[a_targets(unit)].tail(24 * 12)
        a_df[iUnit] += np.random.uniform(0, 0.001, a_df[iUnit].shape)
    else:
        now = datetime.now()
        for kks in a_targets(unit):
            r = wsdlClient.service.HistTimeSpan(
                kks=kks,
                begin=(now - timedelta(hours=24)).strftime(DATE_FORMAT),
                end=now.strftime(DATE_FORMAT),
                seconds=300,
            )
            r = r.replace('NaN', 'null')
            json_dict = json.loads(r)

            pointValues = []
            timestamp = []
            for item in json_dict["Result"]:
                pointValues.append(item["Value"])
                timestamp.append(item["Time"])
            # if pointValues == []:
            #     print(kks)
            a_df[iUnit][kks] = pd.Series(pointValues, index=timestamp)
        # print(a_df[iUnit].shape)


@app.task
def grubbs_t(unit):
    iUnit = int(unit) - 1
    if a_df[iUnit].shape[0] < 24:
        print("Not enough data for grubbs")
        return 0
    x = 0
    try:
        for i in range(a_df[iUnit].shape[1]):
            x += len(grubbs.two_sided_test_indices(a_df[iUnit].iloc[:, i].values, alpha=0.05)) > 0

        result = x / a_df[iUnit].shape[1]
        return result
    except Exception as e:
        print(e)
        return 0


@app.task
def lscp_t(unit):
    iUnit = int(unit) - 1
    if a_df[iUnit].shape[0] < 45:
        print("Not enough data for lscp")
        return 0

    detector_list = [
        lof.LOF(),
        cof.COF(),
        inne.INNE(),
        ecod.ECOD(),
    ]
    clf = lscp.LSCP(
        contamination=COMB(), detector_list=detector_list, n_bins=len(detector_list)
    )
    try:
        clf.fit(a_df[iUnit])
        result = clf.predict(a_df[iUnit])

        final = np.sum(result == 1) / a_df[iUnit].shape[1]
        return final
    except Exception as e:
        print(e)
        return 0


@app.task
def spectral_residual_saliency(unit):
    iUnit = int(unit) - 1
    if a_df[iUnit].shape[0] < 24:
        print("Not enough data for srs")
        return 0
    score_window_size = min(a_df[iUnit].shape[0], 100)
    spec = anom.Silency(
        amp_window_size=24, series_window_size=24, score_window_size=score_window_size
    )

    try:
        abnormal = 0
        for i in range(a_df[iUnit].shape[1]):
            score = spec.generate_anomaly_score(a_df[iUnit].values[:, i])
            abnormal += np.sum(score > np.percentile(score, 99)) > 0

        result = abnormal / a_df[iUnit].shape[1]
        return result
    except Exception as e:
        print(e)
        return 0


def VECM_forecast(data):
    # Johansen 检验方法确定协整关系阶数
    rank = vecm.select_coint_rank(data, det_order=0, k_ar_diff=1, signif=0.1)
    model = vecm.VECM(data, k_ar_diff=1, coint_rank=rank.rank)
    yhat = model.fit().predict(steps=FORECAST_STEP)
    # print(yhat)
    return yhat


def VAR_forecast(data):
    # data的索引必须是递增的 data = data.sort_index()
    model = VAR(data, exog=None)
    # 数据量太小maxlags选择10会报错，数据量不变维度多也会报错
    res = model.select_order(maxlags=5)
    best_lag = res.selected_orders["aic"]
    # print(f"best_lag={best_lag}")
    yhat = model.fit(maxlags=max(best_lag, 10)).forecast(
        data.values, steps=FORECAST_STEP
    )
    # print(yhat)
    return yhat


def trigger(key, field, tag, content, st, now, unit):
    alerts[unit]["alarms"].append(
        dict(
            code=tag,
            desc=content,
            advice="",
            startTime=st.decode('utf-8')
        )
    )
    if st == b'0':
        redis_conn.hset(key, field, now)


def revert(key, field, st):
    if st:
        redis_conn.hset(key, field, 0)


def regulatorValve(unit):
    iUnit = int(unit) - 1
    flag = 0
    now = datetime.now().strftime(DATE_FORMAT)
    regulator_valve_chamber_oil_pressure = codes_with_unit(regulatorValveChamberOilPressure, unit)
    safe_oil_pressure = codes_with_unit(safeOilPressure, unit)
    for chamber, tag in zip(regulator_valve_chamber_oil_pressure, regulatorValveTags):
        j_str = redis_conn.hget("regulatorValve", chamber)
        st = j_str if j_str else b'0'
        if np.all(c_df[iUnit][safe_oil_pressure] >= 5) and (
            c_df[iUnit][chamber] >= 9.6 or np.any(vrfData[iUnit][chamber][-FORECAST_STEP:] >= 9.6)
            if vrfData[iUnit]
            else False
        ):
            trigger("regulatorValve", chamber, tag, "开调节阀 阀门卡涩", st, now, iUnit)
            flag = 1
        else:
            revert("regulatorValve", chamber, st)
    return flag


def mainValve(unit):
    iUnit = int(unit) - 1
    flag = 0
    now = datetime.now().strftime(DATE_FORMAT)
    main_valve_chamber_oil_pressure = codes_with_unit(mainValveChamberOilPressure, unit)
    safe_oil_pressure = codes_with_unit(safeOilPressure, unit)
    if np.all(c_df[iUnit][safe_oil_pressure] >= 5):
        openCommand = redis_conn.hget("command", "open")
        if openCommand != '1':
            redis_conn.hset("command", "open", 1)
            redis_conn.hset("command", "openTime", now)
        else:
            start_time = datetime.strptime(redis_conn.hget("command", "openTime").decode('utf-8'), DATE_FORMAT)
            end_time = datetime.strptime(now, DATE_FORMAT)
            peroid = end_time - start_time
            print(f"peroid={peroid}")
            for chamber, tag in zip(main_valve_chamber_oil_pressure, mainValveTags):
                j_str = redis_conn.hget("mainValve", chamber + '_1')
                st1 = j_str if j_str else b'0'
                j_str = redis_conn.hget("mainValve", chamber + '_2')
                st2 = j_str if j_str else b'0'
                if peroid >= 180 and c_df[iUnit][unit + "GSE011MM"] < 95:
                    flag = 1
                    if c_df[iUnit][chamber] >= 9.6:
                        trigger("mainValve", chamber+'_1', tag, "开主汽阀 阀门卡涩", st1, now, iUnit)
                        revert("mainValve", chamber+'_2', st2)
                    else:
                        trigger("mainValve", chamber+'_2', tag, "试验电磁阀或关断阀卡涩，阀门无法开启（主汽阀）", st2, now, iUnit)
                        revert("mainValve", chamber+'_1', st1)
                else:
                    revert("mainValve", chamber+'_1', st1)
                    revert("mainValve", chamber+'_2', st2)
    else:
        for chamber, tag in zip(main_valve_chamber_oil_pressure, mainValveTags):
            j_str = redis_conn.hget("mainValve", chamber + '_1')
            st1 = j_str if j_str else b'0'
            j_str = redis_conn.hget("mainValve", chamber + '_2')
            st2 = j_str if j_str else b'0'
            revert("mainValve", chamber+'_1', st1)
            revert("mainValve", chamber+'_2', st2)
        redis_conn.hset("command", "open", 0)
    return flag


def liquidLevel(unit):
    iUnit = int(unit) - 1
    flag = 0
    now = datetime.now().strftime(DATE_FORMAT)
    oil_level = codes_with_unit(oilLevel, unit)
    for tag in oil_level:
        j_str = redis_conn.hget("liquidLevel", tag+'_1')
        st1 = j_str if j_str else b'0'
        j_str = redis_conn.hget("liquidLevel", tag+'_2')
        st2 = j_str if j_str else b'0'
        j_str = redis_conn.hget("liquidLevel", tag+'_3')
        st3 = j_str if j_str else b'0'
        if (
            c_df[iUnit][tag] > 580 or st2 == 0 and st3 == 0 and np.any(vrfData[iUnit][tag][-FORECAST_STEP:] > 580)
            if vrfData[iUnit] 
            else False
        ):
            flag = 1
            trigger("liquidLevel", tag+'_1', tag, "抗燃油液位高", st1, now, iUnit)
        else:
            revert("liquidLevel", tag+'_1', st1)
        if (
            c_df[iUnit][tag] < 200 or st1 == 0 and np.any(vrfData[iUnit][tag][-FORECAST_STEP:] < 200)
            if vrfData[iUnit]
            else False
        ):
            flag = 1
            trigger("liquidLevel", tag+'_2', tag, "抗燃油液位低低", st2, now, iUnit)
        else:
            revert("liquidLevel", tag+'_2', st2)
        if (
            c_df[iUnit][tag] < 250
            and c_df[iUnit][tag] >= 200
            or st1 == 0 and (
                np.any(vrfData[iUnit][tag][-FORECAST_STEP:] < 250)
                and np.any(vrfData[iUnit][tag][-FORECAST_STEP:] >= 200)
            )
            if vrfData[iUnit]
            else False
        ):
            flag = 1
            trigger("liquidLevel", tag+'_3', tag, "抗燃油液位低", st3, now, iUnit)
        else:
            revert("liquidLevel", tag+'_3', st3)
    return flag


def pressure(unit):
    iUnit = int(unit) - 1
    flag = 0
    now = datetime.now().strftime(DATE_FORMAT)
    oil_pressure = codes_with_unit(oilPressure, unit)
    for tag in oil_pressure:
        j_str = redis_conn.hget("pressure", tag+'_1')
        st1 = j_str if j_str else b'0'
        j_str = redis_conn.hget("pressure", tag+'_2')
        st2 = j_str if j_str else b'0'
        if (
            c_df[iUnit][tag] > 13.5 or st2 == 0 and np.any(vrfData[iUnit][tag][-FORECAST_STEP:] > 13.5)
            if vrfData[iUnit]
            else False
        ):
            flag = 1
            trigger("pressure", tag+'_1', tag, "抗燃油压力高", st1, now, iUnit)
        else:
            revert("pressure", tag+'_1', st1)
        if (
            c_df[iUnit][tag] < 10 or st1 == 0 and np.any(vrfData[iUnit][tag][-FORECAST_STEP:] < 10)
            if vrfData[iUnit]
            else False
        ):
            flag = 1
            trigger("pressure", tag+'_2', tag, "抗燃油压力低", st2, now, iUnit)
        else:
            revert("pressure", tag+'_2', st2)
    return flag


def temperature(unit):
    iUnit = int(unit) - 1
    flag = 0
    now = datetime.now().strftime(DATE_FORMAT)
    ot, ht = codes_with_unit(oilTemperature, unit)
    j_str = redis_conn.hget("temperature", ot + '_1')
    st1 = j_str if j_str else b'0'
    j_str = redis_conn.hget("temperature", ot + '_2')
    st2 = j_str if j_str else b'0'
    j_str = redis_conn.hget("temperature", ot + '_3')
    st3 = j_str if j_str else b'0'
    j_str = redis_conn.hget("temperature", ht)
    st4 = j_str if j_str else b'0'
    if (
        c_df[iUnit][ot] > 60 or st2 == 0 and st3 == 0 and np.any(vrfData[iUnit][ot][-FORECAST_STEP:] > 60)
        if vrfData[iUnit]
        else False
    ):
        flag = 1
        trigger("temperature", ot + '_1', ot, "抗燃油温度高", st1, now, iUnit)
    else:
        revert("temperature", ot + '_1', st1)
    if (
        c_df[iUnit][ot] < 12 or st1 == 0 and np.any(vrfData[iUnit][ot][-FORECAST_STEP:] < 12)
        if vrfData[iUnit]
        else False
    ):
        flag = 1
        trigger("temperature", ot + '_2', ot, "抗燃油温度低低", st2, now, iUnit)
    else:
        revert("temperature", ot + '_2', st2)
    if (
        c_df[iUnit][ot] < 36
        and c_df[iUnit][ot] >= 12
        or st1 == 0 and (
            np.any(vrfData[iUnit][ot][-FORECAST_STEP:] < 36)
            and np.any(vrfData[iUnit][ot][-FORECAST_STEP:] >= 12)
        )
        if vrfData[iUnit]
        else False
    ):
        flag = 1
        trigger("temperature", ot + '_3', ot, "抗燃油温度低", st3, now, iUnit)
    else:
        revert("temperature", ot + '_3', st3)
    if (
        c_df[iUnit][ht] > 110 or np.any(vrfData[iUnit][ht][-FORECAST_STEP:] > 110)
        if vrfData[iUnit]
        else False
    ):
        flag = 1
        trigger("temperature", ht, ht, "加热器温度高", st4, now, iUnit)
    else:
        revert("temperature", ht, st4)
    return flag


def filter(unit):
    iUnit = int(unit) - 1
    flag = 0
    now = datetime.now().strftime(DATE_FORMAT)
    filter_pressure = codes_with_unit(filterPressure, unit)
    for tag in filter_pressure:
        j_str = redis_conn.hget("filter", tag)
        st = j_str if j_str else b'0'
        if c_df[iUnit][tag] > 0.5:
            trigger("filter", tag, tag, "过滤器堵塞", st, now, iUnit)
            flag = 1
        else:
            revert("filter", tag, st)
    return flag


def foo(judgeFunc, unit):
    iUnit = int(unit) - 1
    tag = judgeFunc.__name__
    r = judgeFunc(unit)
    if r == 0:
        status[iUnit][tag] = "未见异常"
    else:
        status[iUnit][tag] = "异常"


@app.task
def vr_forecast(unit):
    iUnit = int(unit) - 1
    global vrfData
    vrfData[iUnit] = {}
    if f_df[iUnit].shape[0] < 40:
        print("Not enough data for forecast")
        return

    exogList = [[], [], [], [], [], []]

    targetList = target_list(unit)
    targetListLen = len(targetList)
    assert len(exogList) == targetListLen
    for i in range(targetListLen):
        vrdf = f_df[iUnit][targetList[i] + exogList[i]]

        # 随便取一列平稳性测试
        try:
            p = adfuller(vrdf.iloc[:, 0], regression="ct")[1]  # ct: 常数项和趋势项
        except:  # 全常数
            p = 0
        # print(f"p={p}")

        targetLen = len(targetList[i])
        try:
            if p <= ADFULLER_THRESHOLD_VAL:  # 平稳的
                yhat = VAR_forecast(vrdf)
            else:
                yhat = VECM_forecast(vrdf)
        except Exception as e:
            print("Exception: ", e)
        else:
            yhat = pd.DataFrame(yhat, columns=vrdf.columns)
            result = pd.concat(
                [vrdf.iloc[-HISTORY_STEP:, :], yhat], axis=0, ignore_index=True
            )
            vrfData[iUnit].update(
                {
                    col: result[col].values.round(DECIMALS)
                    for col in result.columns[:targetLen]
                }
            )
    # print(vrfData[iUnit].keys())


@app.task
def mechanism_analysis(unit):
    iUnit = int(unit) - 1
    if not c_df[iUnit].empty:
        global alerts
        current_time = datetime.now()
        alerts[iUnit] = {"alarms": [], "timestamp": current_time.strftime(DATE_FORMAT)}
        foo(liquidLevel, unit)
        foo(pressure, unit)
        foo(temperature, unit)
        foo(regulatorValve, unit)
        if not TEST:
            foo(filter, unit)
        else:
            status[iUnit]["filter"] = "未见异常"
        foo(mainValve, unit)

        # print(alerts)
        # print(status)
        MQTTClient.publish(f"forecast-status{unit}", json.dumps(status[iUnit]))
        MQTTClient.publish(f"forecast-alerts{unit}", json.dumps(alerts[iUnit]))

        data = {k: v.tolist() for k, v in vrfData[iUnit].items()}
        data["timestamp"] = [
            (current_time + timedelta(seconds=FORECAST_INTERVAL * i)).strftime(DATE_FORMAT)
            for i in range(-HISTORY_STEP + 1, FORECAST_STEP + 1)
        ]
        MQTTClient.publish(f"forecast-vr{unit}", json.dumps(data))


@app.task
def show_points(unit):
    iUnit = int(unit) - 1
    if not c_df[iUnit].empty:
        MQTTClient.publish(f"points{unit}", c_df[iUnit].to_json())


@app.task
def health_score(rGrubbs, rLscp, rSRA, unit):
    iUnit = int(unit) - 1
    score = ANOMALY_SOCRE + ALERT_SCORE
    score -= min(ALERT_SCORE, PER_ALERT_SCORE * len(alerts[iUnit]["alarms"]))
    remainder = score - ANOMALY_COL_RATIO
    if rLscp > ANOMALY_COL_RATIO:
        score -= LSCP_SCORE * (rLscp - ANOMALY_COL_RATIO) / remainder
    if rGrubbs > ANOMALY_COL_RATIO:
        score -= GRUBBS_SCORE * (rGrubbs - ANOMALY_COL_RATIO) / remainder
    if rSRA > ANOMALY_COL_RATIO:
        score -= SRA_SCORE * (rSRA - ANOMALY_COL_RATIO) / remainder

    data = {
        "timestamp": datetime.now().strftime(DATE_FORMAT),
        "healthscore": round(score, DECIMALS),
    }
    # print(data)
    if healthQ[iUnit].full():
        healthQ[iUnit].get()
    healthQ[iUnit].put(data)
   
    result_list = list(healthQ[iUnit].queue)
    healthscores = []
    timestamps = []

    for item in result_list:
        healthscores.append(item["healthscore"])
        timestamps.append(item["timestamp"])

    new_data = {
        "healthscore": healthscores,
        "timestamp": timestamps
    }

    MQTTClient.publish(f"forecast-health{unit}", json.dumps(new_data))


if __name__ == "__main__":
    unit = '1'
    iUnit = int(unit) - 1
    count = 0
    try:
        while 1:
            start = time()

            sdk_get_forecast_df(unit)
            sdk_get_anomaly_df(unit)
            sdk_get_current_df(unit)
            vr_forecast(unit)

            health_score(grubbs_t(unit), lscp_t(unit), spectral_residual_saliency(unit), unit)
            mechanism_analysis(unit)
            show_points(unit)
            
            end = time()
            elapsed_time = int((end - start) * 1000000)
            count += 1
            print(f"Loop {count} time used: {elapsed_time} microseconds")
            sleep(max(INTERVAL - elapsed_time / 1000000, 0))
    except KeyboardInterrupt:
        print("KeyboardInterrupt received. Shutting down the executor...")
        redis_conn.close()
