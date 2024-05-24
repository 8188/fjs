from config import *
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
import redis.asyncio as redis
from queue import Queue
from functools import lru_cache
import asyncio
import aiomqtt
import sys
from concurrent import futures

if sys.platform.lower() == "win32" or os.name.lower() == "nt":
    from asyncio import set_event_loop_policy, WindowsSelectorEventLoopPolicy
    set_event_loop_policy(WindowsSelectorEventLoopPolicy())

import warnings
warnings.filterwarnings("ignore")

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
PER_ALERT_SCORE = 2.5
DECIMALS = 3
ADFULLER_THRESHOLD_VAL = 0.05
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
MAX_QUEUE_SIZE = 10
TESTFILE = "../../test.feather"


@lru_cache(maxsize=None)
def codes_with_unit(codes, unit):
    suffix = ""
    unit = '1'
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
filterPressure = ()

# 抗燃油泵电流
pumpCurrent = ()

# 安全油压
safeOilPressure = (
    "GSE202MP",
    "GSE203MP",
)

# 汽轮机转速
turbineSpeed = (
    "GSE001MC_XF60_AVALUE",
    "GSE002MC_XF60_AVALUE",
    "GSE003MC_XF60",
)

# 盘车
jigger = (
    "GGR001MI",  # 盘车电流
    "GGR001SM",  # 盘车投入
    "GGR002SM",  # 盘车退出
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

f_df = [pd.DataFrame() for _ in range(2)]
a_df = [pd.DataFrame() for _ in range(2)]
status = [{} for _ in range(2)]
alerts = [{"alarms": [], "timestamp": ""} for _ in range(2)]
vrfData = [{} for _ in range(2)]
healthQ = [Queue(maxsize=MAX_QUEUE_SIZE) for _ in range(2)]

pool = redis.ConnectionPool.from_url(f"redis://{REDIS_IP}:{REDIS_PORT}/{REDIS_DB}")
redis_conn = redis.Redis.from_pool(pool)
print("----build new Redis connection----")

def get_forecast_df(unit):
    global f_df
    iUnit = int(unit) - 1
    f_df[iUnit] = pd.DataFrame()
    f_df[iUnit] = pd.read_feather(TESTFILE)[f_targets(unit)].tail(24 * 60)
    f_df[iUnit] += np.random.uniform(0, 0.001, f_df[iUnit].shape)


def get_anomaly_df(unit):
    global a_df
    iUnit = int(unit) - 1
    a_df[iUnit] = pd.DataFrame()
    a_df[iUnit] = pd.read_feather(TESTFILE)[a_targets(unit)].tail(24 * 12)
    a_df[iUnit] += np.random.uniform(0, 0.001, a_df[iUnit].shape)


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


async def trigger(key, field, tag, content, st, now, unit):
    alerts[unit]["alarms"].append(
        dict(
            code=tag,
            desc=content,
            advice="",
            startTime=st.decode('utf-8')
        )
    )
    if st == b'0':
        await redis_conn.hset(key, field, now)


async def revert(key, field, st):
    if st:
        await redis_conn.hset(key, field, 0)


async def regulatorValve(unit):
    iUnit = int(unit) - 1
    flag = 0
    key = "FJS:Forecast:regulatorValve"
    now = datetime.now().strftime(DATE_FORMAT)
    regulator_valve_chamber_oil_pressure = codes_with_unit(regulatorValveChamberOilPressure, unit)
    safe_oil_pressure = codes_with_unit(safeOilPressure, unit)
    for chamber, tag in zip(regulator_valve_chamber_oil_pressure, regulatorValveTags):
        j_str = await redis_conn.hget(key, chamber)
        st = j_str if j_str else b'0'
        if np.all(a_df[iUnit].iloc[-1][safe_oil_pressure] >= 5) and (
            np.any(vrfData[iUnit][chamber][-FORECAST_STEP:] >= 9.6)
            if vrfData[iUnit]
            else False
        ):
            await trigger(key, chamber, tag, "开调节阀 阀门卡涩", st, now, iUnit)
            flag = 1
        else:
            await revert(key, chamber, st)
    return flag


async def liquidLevel(unit):
    iUnit = int(unit) - 1
    flag = 0
    key = "FJS:Forecast:liquidLevel"
    now = datetime.now().strftime(DATE_FORMAT)
    oil_level = codes_with_unit(oilLevel, unit)
    for tag in oil_level:
        j_str = await redis_conn.hget(key, tag+'_1')
        st1 = j_str if j_str else b'0'
        j_str = await redis_conn.hget(key, tag+'_2')
        st2 = j_str if j_str else b'0'
        j_str = await redis_conn.hget(key, tag+'_3')
        st3 = j_str if j_str else b'0'
        if (
            st2 == 0 and st3 == 0 and np.any(vrfData[iUnit][tag][-FORECAST_STEP:] > 580)
            if vrfData[iUnit] 
            else False
        ):
            flag = 1
            await trigger(key, tag+'_1', tag, "抗燃油液位高", st1, now, iUnit)
        else:
            await revert(key, tag+'_1', st1)
        if (
            st1 == 0 and np.any(vrfData[iUnit][tag][-FORECAST_STEP:] < 200)
            if vrfData[iUnit]
            else False
        ):
            flag = 1
            await trigger(key, tag+'_2', tag, "抗燃油液位低低", st2, now, iUnit)
        else:
            await revert(key, tag+'_2', st2)
        if (
            st1 == 0 and (
                np.any(vrfData[iUnit][tag][-FORECAST_STEP:] < 250)
                and np.any(vrfData[iUnit][tag][-FORECAST_STEP:] >= 200)
            )
            if vrfData[iUnit]
            else False
        ):
            flag = 1
            await trigger(key, tag+'_3', tag, "抗燃油液位低", st3, now, iUnit)
        else:
            await revert(key, tag+'_3', st3)
    return flag


async def pressure(unit):
    iUnit = int(unit) - 1
    flag = 0
    key = "FJS:Forecast:pressure"
    now = datetime.now().strftime(DATE_FORMAT)
    oil_pressure = codes_with_unit(oilPressure, unit)
    for tag in oil_pressure:
        j_str = await redis_conn.hget(key, tag+'_1')
        st1 = j_str if j_str else b'0'
        j_str = await redis_conn.hget(key, tag+'_2')
        st2 = j_str if j_str else b'0'
        if (
            st2 == 0 and np.any(vrfData[iUnit][tag][-FORECAST_STEP:] > 13.5)
            if vrfData[iUnit]
            else False
        ):
            flag = 1
            await trigger(key, tag+'_1', tag, "抗燃油压力高", st1, now, iUnit)
        else:
            await revert(key, tag+'_1', st1)
        if (
            st1 == 0 and np.any(vrfData[iUnit][tag][-FORECAST_STEP:] < 10)
            if vrfData[iUnit]
            else False
        ):
            flag = 1
            await trigger(key, tag+'_2', tag, "抗燃油压力低", st2, now, iUnit)
        else:
            await revert(key, tag+'_2', st2)
    return flag


async def temperature(unit):
    iUnit = int(unit) - 1
    flag = 0
    key = "FJS:Forecast:temperature"
    now = datetime.now().strftime(DATE_FORMAT)
    ot, ht = codes_with_unit(oilTemperature, unit)
    j_str = await redis_conn.hget(key, ot + '_1')
    st1 = j_str if j_str else b'0'
    j_str = await redis_conn.hget(key, ot + '_2')
    st2 = j_str if j_str else b'0'
    j_str = await redis_conn.hget(key, ot + '_3')
    st3 = j_str if j_str else b'0'
    j_str = await redis_conn.hget(key, ht)
    st4 = j_str if j_str else b'0'
    if (
        st2 == 0 and st3 == 0 and np.any(vrfData[iUnit][ot][-FORECAST_STEP:] > 60)
        if vrfData[iUnit]
        else False
    ):
        flag = 1
        await trigger(key, ot + '_1', ot, "抗燃油温度高", st1, now, iUnit)
    else:
        await revert(key, ot + '_1', st1)
    if (
        st1 == 0 and np.any(vrfData[iUnit][ot][-FORECAST_STEP:] < 12)
        if vrfData[iUnit]
        else False
    ):
        flag = 1
        await trigger(key, ot + '_2', ot, "抗燃油温度低低", st2, now, iUnit)
    else:
        await revert(key, ot + '_2', st2)
    if (
        st1 == 0 and (
            np.any(vrfData[iUnit][ot][-FORECAST_STEP:] < 36)
            and np.any(vrfData[iUnit][ot][-FORECAST_STEP:] >= 12)
        )
        if vrfData[iUnit]
        else False
    ):
        flag = 1
        await trigger(key, ot + '_3', ot, "抗燃油温度低", st3, now, iUnit)
    else:
        await revert(key, ot + '_3', st3)
    if (
        np.any(vrfData[iUnit][ht][-FORECAST_STEP:] > 110)
        if vrfData[iUnit]
        else False
    ):
        flag = 1
        await trigger(key, ht, ht, "加热器温度高", st4, now, iUnit)
    else:
        await revert(key, ht, st4)
    return flag


async def foo(judgeFunc, unit):
    iUnit = int(unit) - 1
    tag = judgeFunc.__name__
    r = await judgeFunc(unit)
    if r == 0:
        status[iUnit][tag] = "未见异常"
    else:
        status[iUnit][tag] = "异常"


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


async def analysis(MQTTClient, unit):
    iUnit = int(unit) - 1
    global alerts
    current_time = datetime.now()
    alerts[iUnit] = {"alarms": [], "timestamp": current_time.strftime(DATE_FORMAT)}
    
    await foo(liquidLevel, unit)
    await foo(pressure, unit)
    await foo(temperature, unit)
    await foo(regulatorValve, unit)
    status[iUnit]["filter"] = "未见异常"

    # print(alerts)
    # print(status)
    await MQTTClient.publish(f"FJS{unit}/Forecast/Status", json.dumps(status[iUnit]))
    await MQTTClient.publish(f"FJS{unit}/Forecast/Alerts", json.dumps(alerts[iUnit]))

    data = {k: v.tolist() for k, v in vrfData[iUnit].items()}
    data["timestamp"] = [
        (current_time + timedelta(seconds=FORECAST_INTERVAL * i)).strftime(DATE_FORMAT)
        for i in range(-HISTORY_STEP + 1, FORECAST_STEP + 1)
    ]
    await MQTTClient.publish(f"FJS{unit}/Forecast/VR", json.dumps(data))


async def machanism_alarm_nums():
    keys = [
        "FJS:Mechanism:regulatorValve",
        "FJS:Mechanism:mainValve",
        "FJS:Mechanism:liquidLevel",
        "FJS:Mechanism:pressure",
        "FJS:Mechanism:temperature",
        "FJS:Mechanism:filter",
        "FJS:Mechanism:pressure",
    ]
    count = 0
    for key in keys:
        dic = await redis_conn.hgetall(key)
        for v in dic.values():
            if v != b'0':
                count += 1
    return count

async def health_score(MQTTClient, rGrubbs, rLscp, rSRA, unit):
    iUnit = int(unit) - 1
    nums = await machanism_alarm_nums()

    score = ANOMALY_SOCRE + ALERT_SCORE
    score -= min(ALERT_SCORE, PER_ALERT_SCORE * (len(alerts[iUnit]["alarms"]) + nums))
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

    await MQTTClient.publish(f"FJS{unit}/Forecast/Health", json.dumps(new_data))


async def test(unit):
    count = 0
    try:
        async with aiomqtt.Client(MQTT_IP, MQTT_PORT) as MQTTClient:
            print("----build new MQTT connection----")
            await MQTTClient.subscribe("FJS1/Mechanism/#")
            while 1:
                start = time()

                get_forecast_df(unit)
                get_anomaly_df(unit)
                vr_forecast(unit)

                await health_score(MQTTClient, grubbs_t(unit), lscp_t(unit), spectral_residual_saliency(unit), unit)
                await analysis(MQTTClient, unit)
                
                end = time()
                elapsed_time = int((end - start) * 1000000)
                count += 1
                print(f"Loop {count} time used: {elapsed_time} microseconds")
                sleep(max(INTERVAL - elapsed_time / 1000000, 0))
    except aiomqtt.MqttError:
        print(f"MQTT connection lost; Reconnecting in 5 seconds ...")
        await asyncio.sleep(5)
    except KeyboardInterrupt:
        print("KeyboardInterrupt received. Shutting down the executor...")
        await redis_conn.aclose()


async def tasks(unit):
    executor = futures.ThreadPoolExecutor()
    count = 0

    try:
        async with aiomqtt.Client(MQTT_IP, MQTT_PORT) as MQTTClient:
            print("----build new MQTT connection----")
            await MQTTClient.subscribe("FJS1/Mechanism/#")
            while 1:
                start = time()

                future_get_forecast_df = executor.submit(get_forecast_df, unit)
                future_get_anomaly_df = executor.submit(get_anomaly_df, unit)
                futures.wait([future_get_forecast_df, future_get_anomaly_df])
                
                future_grubbs = executor.submit(grubbs_t, unit)
                future_lscp = executor.submit(lscp_t, unit)
                future_spectral = executor.submit(spectral_residual_saliency, unit)
                future_vr_forecast = executor.submit(vr_forecast, unit)
                futures.wait([future_vr_forecast, future_grubbs, future_lscp, future_spectral])

                await health_score(MQTTClient, future_grubbs.result(), future_lscp.result(), future_spectral.result(), unit)
                await analysis(MQTTClient, unit)
                
                end = time()
                elapsed_time = int((end - start) * 1000000)
                count += 1
                print(f"Loop {count} time used: {elapsed_time} microseconds")
                sleep(max(INTERVAL - elapsed_time / 1000000, 0))
    except aiomqtt.MqttError:
        print(f"MQTT connection lost; Reconnecting in 5 seconds ...")
        await asyncio.sleep(5)
    except KeyboardInterrupt:
        print("KeyboardInterrupt received. Shutting down the executor...")
        await redis_conn.aclose()
        executor.shutdown(wait=False)


if __name__ == "__main__":
    asyncio.run(test('1'))
