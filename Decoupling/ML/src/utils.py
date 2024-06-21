from typing import Any, Literal, Callable, Tuple, Awaitable, Optional, cast, Iterable

from config import *

import sys
import uuid
from time import time, sleep
from datetime import datetime, timedelta
from queue import Queue
import asyncio
from concurrent import futures

import pandas as pd
import numpy as np
from statsmodels.tsa.api import adfuller, VAR
from statsmodels.tsa.vector_ar import vecm
from outliers import smirnov_grubbs as grubbs
from pyod.models import lof, cof, inne, ecod, lscp
import sranodec as anom

import redis.asyncio as redis
import aiomqtt

import orjson as json
from dependency_injector import containers, providers
from dependency_injector.wiring import Provide, inject

if sys.platform.lower() == "win32" or os.name.lower() == "nt":
    from asyncio import set_event_loop_policy, WindowsSelectorEventLoopPolicy
    set_event_loop_policy(WindowsSelectorEventLoopPolicy())

import warnings
warnings.filterwarnings("ignore")

INTERVAL = 10
HISTORY_STEP = 25
FORECAST_STEP = 5
FORECAST_INTERVAL = 60
DECIMALS = 3
ADFULLER_THRESHOLD_VAL = 0.05
MAX_QUEUE_SIZE = 10

ANOMALY_SCORE = 50
ALERT_SCORE = 50
PER_ALERT_SCORE = 2.5
ANOMALY_COL_RATIO = 3
GRUBBS_SCORE = 10
LSCP_SCORE = 30
SRA_SCORE = 10

DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
TESTFILE = "test.feather"

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
currentTime = [datetime.now() for _ in range(2)]
now = ["" for _ in range(2)]

def codes_with_unit(codes: Tuple[str, ...], unit: str) -> list[str]:
    suffix = ""
    unit = '1' # for test
    return [unit + item + suffix for item in codes]


class RedisService:

    def __init__(self, redis: redis.Redis) -> None:
        self._redis = redis

    async def hget(self, name: str, key: str) -> Awaitable[Optional[str]]:
        return await self._redis.hget(name, key) # type: ignore

    async def hgetall(self, name: str) -> Awaitable[dict]:
        return await self._redis.hgetall(name) # type: ignore

    async def hset(self, name: str, key: str, value: str) -> Awaitable[int]:
        return await self._redis.hset(name, key, value) # type: ignore


class RedisContainer(containers.DeclarativeContainer):
    # config = providers.Configuration()

    def _init_redis() -> redis.Redis: # type: ignore
        pool = redis.ConnectionPool.from_url(
            f"redis://{REDIS_USERNAME}:{REDIS_PASSWORD}@{REDIS_IP}:{REDIS_PORT}/{REDIS_DB}",
            encoding="utf-8",
            decode_responses=True,
        )
        conn = redis.Redis.from_pool(pool)
        print("----build new Redis connection----")
        return conn

    _redis_conn = providers.Resource(
        _init_redis,
    )

    service = providers.Factory(
        RedisService,
        redis=_redis_conn,
    )


class AppContainer(containers.DeclarativeContainer):
    redis_package = providers.Container(RedisContainer)


class GetData:

    def __init__(self, unit: str) -> None:
        self.unit = unit
        self.iUnit = int(unit) - 1
        self.fTargets = self._f_targets()
        self.aTargets = self._a_targets()

    def _f_targets(self) -> list[str]:
        return (
            codes_with_unit(regulatorValveOpening, self.unit)
            + codes_with_unit(regulatorValveChamberOilPressure, self.unit)
            + codes_with_unit(oilLevel, self.unit)
            + codes_with_unit(oilPressure, self.unit)
            + codes_with_unit(oilTemperature, self.unit)
        )

    def _a_targets(self) -> list[str]:
        return (
            codes_with_unit(others, self.unit)
            + codes_with_unit(jigger, self.unit)
            + codes_with_unit(turbineSpeed, self.unit)
            + codes_with_unit(safeOilPressure, self.unit)
            + self._f_targets()
            + codes_with_unit(mainValveOpening, self.unit)
            + codes_with_unit(mainValveChamberOilPressure, self.unit)
            + codes_with_unit(filterPressure, self.unit)
            + codes_with_unit(pumpCurrent, self.unit)
        )

    def get_forecast_df(self) -> None:
        f_df[self.iUnit] = pd.DataFrame()
        f_df[self.iUnit] = pd.read_feather(TESTFILE)[self.fTargets].tail(24 * 60)
        f_df[self.iUnit] += np.random.uniform(0, 0.001, f_df[self.iUnit].shape)

    def get_anomaly_df(self) -> None:
        a_df[self.iUnit] = pd.DataFrame()
        a_df[self.iUnit] = pd.read_feather(TESTFILE)[self.aTargets].tail(24 * 12)
        a_df[self.iUnit] += np.random.uniform(0, 0.001, a_df[self.iUnit].shape)


class AnomalyDetection:

    def __init__(self, unit: str) -> None:
        self.unit = unit
        self.iUnit = int(unit) - 1
        self.clf = lscp.LSCP(
            # contamination=COMB(),
            detector_list=self._create_detector_list(),
            n_bins=len(self._create_detector_list()),
        )

    def _create_detector_list(self) -> list[Any]:
        return [
            lof.LOF(),
            cof.COF(),
            inne.INNE(),
            ecod.ECOD(),
        ]

    def grubbs_t(self) -> float:
        if a_df[self.iUnit].shape[0] < 24:
            print("Not enough data for grubbs")
            return 0

        x = 0
        try:
            for i in range(a_df[self.iUnit].shape[1]):
                result = grubbs.two_sided_test_indices(a_df[self.iUnit].iloc[:, i].values, alpha=0.05)
                if result:
                    x += len(result) > 0

            final = x / a_df[self.iUnit].shape[1]
            return final
        except Exception as e:
            print(e)
            return 0

    def lscp_t(self) -> float:
        if a_df[self.iUnit].shape[0] < 45:
            print("Not enough data for lscp")
            return 0

        try:
            self.clf.fit(a_df[self.iUnit])
            result = self.clf.predict(a_df[self.iUnit])

            final = np.sum(result == 1) / a_df[self.iUnit].shape[1]
            return final
        except Exception as e:
            print(e)
            return 0

    def spectral_residual_saliency(self) -> float:
        if a_df[self.iUnit].shape[0] < 24:
            print("Not enough data for srs")
            return 0
        
        score_window_size = min(a_df[self.iUnit].shape[0], 100)
        spec = anom.Silency(
            amp_window_size=24, series_window_size=24, score_window_size=score_window_size
        )

        try:
            abnormal = 0
            for i in range(a_df[self.iUnit].shape[1]):
                score = spec.generate_anomaly_score(a_df[self.iUnit].values[:, i])
                abnormal += np.sum(score > np.percentile(score, 99)) > 0

            result = abnormal / a_df[self.iUnit].shape[1]
            return float(result)
        except Exception as e:
            print(e)
            return 0


class Forecast:

    def __init__(self, unit: str) -> None:
        self.unit = unit
        self.iUnit = int(unit) - 1
        self.targetList = self._target_list()

    def _target_list(self) -> Tuple[list[str], ...]:
        return (
            codes_with_unit(regulatorValveOpening, self.unit)[:4],  # 高压调节阀开度
            codes_with_unit(regulatorValveOpening, self.unit)[4:],  # 中压调节阀开度
            codes_with_unit(oilLevel, self.unit),  # 抗燃油液位
            codes_with_unit(oilPressure, self.unit),  # 压力
            codes_with_unit(oilTemperature, self.unit),  # 温度
            codes_with_unit(regulatorValveChamberOilPressure, self.unit),  # 调节阀腔室油压
        )

    def _VECM_forecast(self, data: pd.DataFrame) -> Any:
        # Johansen 检验方法确定协整关系阶数
        rank = vecm.select_coint_rank(data, det_order=0, k_ar_diff=1, signif=0.1)
        model = vecm.VECM(data, k_ar_diff=1, coint_rank=rank.rank)
        yhat = model.fit().predict(steps=FORECAST_STEP)
        # print(yhat)
        return yhat

    def _VAR_forecast(self, data: pd.DataFrame) -> Any:
        # data的索引必须是递增的 data = data.sort_index()
        model = VAR(data, exog=None)
        # 数据量太小maxlags选择10会报错，数据量不变维度多也会报错
        res = model.select_order(maxlags=5)
        best_lag = res.selected_orders["aic"]
        # print(f"best_lag={best_lag}")
        yhat = model.fit(maxlags=int(max(best_lag, 10))).forecast(
            data.values, steps=FORECAST_STEP
        )
        # print(yhat)
        return yhat

    def vr_forecast(self) -> None:
        vrfData[self.iUnit] = {}
        if f_df[self.iUnit].shape[0] < 40:
            print("Not enough data for forecast")
            return

        exogList = [[], [], [], [], [], []]

        targetListLen = len(self.targetList)
        assert len(exogList) == targetListLen
        for i in range(targetListLen):
            vrdf = f_df[self.iUnit][self.targetList[i] + exogList[i]]

            # 随便取一列平稳性测试
            try:
                p = adfuller(vrdf.iloc[:, 0], regression="ct")[1]  # ct: 常数项和趋势项
            except:  # 全常数
                p = 0
            # print(f"p={p}")

            targetLen = len(self.targetList[i])
            try:
                if p <= ADFULLER_THRESHOLD_VAL:  # 平稳的
                    yhat = self._VAR_forecast(vrdf)
                else:
                    yhat = self._VECM_forecast(vrdf)
            except Exception as e:
                print("Exception: ", e)
            else:
                yhat = pd.DataFrame(yhat, columns=vrdf.columns)
                result = pd.concat(
                    [vrdf.iloc[-HISTORY_STEP:, :], yhat], axis=0, ignore_index=True
                )
                vrfData[self.iUnit].update(
                    {
                        col: result[col].round(DECIMALS).values
                        for col in result.columns[:targetLen]
                    }
                )
        # print(vrfData[self.iUnit])


class Logic:

    @inject  # 可以不要
    def __init__(
        self,
        unit: str,
        conn: RedisContainer = Provide[AppContainer.redis_package.service],
    ) -> None:
        self.unit = unit
        self.iUnit = int(unit) - 1
        self.safe_oil_pressure = codes_with_unit(safeOilPressure, unit)
        self.regulator_valve_chamber_oil_pressure = codes_with_unit(regulatorValveChamberOilPressure, unit)
        self.oil_level = codes_with_unit(oilLevel, unit)
        self.oil_pressure = codes_with_unit(oilPressure, unit)
        self.ot, self.ht = codes_with_unit(oilTemperature, unit)
        self.redis_conn = conn
        self.RVName = f"FJS{unit}:Forecast:regulatorValve"
        self.LLName = f"FJS{unit}:Forecast:liquidLevel"
        self.PName = f"FJS{self.unit}:Forecast:pressure"
        self.TName = f"FJS{self.unit}:Forecast:temperature"

    def getNow(self) -> None:
        global currentTime, now
        currentTime[self.iUnit] = datetime.now()
        now[self.iUnit] = currentTime[self.iUnit].strftime(DATE_FORMAT)

    async def _trigger(
        self, name: str, key: str, tag: str, content: str, st: str
    ) -> None:
        alerts[self.iUnit]["alarms"].append(
            dict(
                code=tag,
                desc=content,
                advice="",
                startTime=st
            )
        )
        if not st:
            await self.redis_conn.hset(name, key, now[self.iUnit])

    async def _revert(self, name: str, key: str, st: str) -> None:
        if st:
            await self.redis_conn.hset(name, key, 0)

    async def _regulatorValve(self) -> Literal[0, 1]:
        flag = 0
        for chamber, tag in zip(self.regulator_valve_chamber_oil_pressure, regulatorValveTags):
            st = await self.redis_conn.hget(self.RVName, chamber)
            if np.all(a_df[self.iUnit].iloc[-1][self.safe_oil_pressure] >= 5) and (
                np.any(vrfData[self.iUnit][chamber][-FORECAST_STEP:] >= 9.6)
                if vrfData[self.iUnit]
                else False
            ):
                await self._trigger(self.RVName, chamber, tag, "开调节阀 阀门卡涩", st)
                flag = 1
            else:
                await self._revert(self.RVName, chamber, st)
        return flag

    async def _liquidLevel(self) -> Literal[0, 1]:
        flag = 0
        for tag in self.oil_level:
            st1 = await self.redis_conn.hget(self.LLName, tag+'_1')
            st2 = await self.redis_conn.hget(self.LLName, tag+'_2')
            st3 = await self.redis_conn.hget(self.LLName, tag+'_3')
            if (
                not st2 and not st3 and np.any(vrfData[self.iUnit][tag][-FORECAST_STEP:] > 580)
                if vrfData[self.iUnit] 
                else False
            ):
                flag = 1
                await self._trigger(self.LLName, tag+'_1', tag, "抗燃油液位高", st1)
            else:
                await self._revert(self.LLName, tag+'_1', st1)
            if (
                not st1 and np.any(vrfData[self.iUnit][tag][-FORECAST_STEP:] < 200)
                if vrfData[self.iUnit]
                else False
            ):
                flag = 1
                await self._trigger(self.LLName, tag+'_2', tag, "抗燃油液位低低", st2)
            else:
                await self._revert(self.LLName, tag+'_2', st2)
            if (
                not st1 and (
                    np.any(vrfData[self.iUnit][tag][-FORECAST_STEP:] < 250)
                    and np.any(vrfData[self.iUnit][tag][-FORECAST_STEP:] >= 200)
                )
                if vrfData[self.iUnit]
                else False
            ):
                flag = 1
                await self._trigger(self.LLName, tag+'_3', tag, "抗燃油液位低", st3)
            else:
                await self._revert(self.LLName, tag+'_3', st3)
        return flag

    async def _pressure(self) -> Literal[0, 1]:
        flag = 0
        for tag in self.oil_pressure:
            st1 = await self.redis_conn.hget(self.PName, tag+'_1')
            st2 = await self.redis_conn.hget(self.PName, tag+'_2')
            if (
                not st2 and np.any(vrfData[self.iUnit][tag][-FORECAST_STEP:] > 13.5)
                if vrfData[self.iUnit]
                else False
            ):
                flag = 1
                await self._trigger(self.PName, tag+'_1', tag, "抗燃油压力高", st1)
            else:
                await self._revert(self.PName, tag+'_1', st1)
            if (
                not st1 and np.any(vrfData[self.iUnit][tag][-FORECAST_STEP:] < 10)
                if vrfData[self.iUnit]
                else False
            ):
                flag = 1
                await self._trigger(self.PName, tag+'_2', tag, "抗燃油压力低", st2)
            else:
                await self._revert(self.PName, tag+'_2', st2)
        return flag

    async def _temperature(self) -> Literal[0, 1]:
        flag = 0
        st1 = await self.redis_conn.hget(self.TName, self.ot + '_1')
        st2 = await self.redis_conn.hget(self.TName, self.ot + '_2')
        st3 = await self.redis_conn.hget(self.TName, self.ot + '_3')
        st4 = await self.redis_conn.hget(self.TName, self.ht)
        if (
            not st2 and not st3 and np.any(vrfData[self.iUnit][self.ot][-FORECAST_STEP:] > 60)
            if vrfData[self.iUnit]
            else False
        ):
            flag = 1
            await self._trigger(self.TName, self.ot + '_1', self.ot, "抗燃油温度高", st1)
        else:
            await self._revert(self.TName, self.ot + '_1', st1)
        if (
            not st1 and np.any(vrfData[self.iUnit][self.ot][-FORECAST_STEP:] < 12)
            if vrfData[self.iUnit]
            else False
        ):
            flag = 1
            await self._trigger(self.TName, self.ot + '_2', self.ot, "抗燃油温度低低", st2)
        else:
            await self._revert(self.TName, self.ot + '_2', st2)
        if (
            not st1 and (
                np.any(vrfData[self.iUnit][self.ot][-FORECAST_STEP:] < 36)
                and np.any(vrfData[self.iUnit][self.ot][-FORECAST_STEP:] >= 12)
            )
            if vrfData[self.iUnit]
            else False
        ):
            flag = 1
            await self._trigger(self.TName, self.ot + '_3', self.ot, "抗燃油温度低", st3)
        else:
            await self._revert(self.TName, self.ot + '_3', st3)
        if (
            np.any(vrfData[self.iUnit][self.ht][-FORECAST_STEP:] > 110)
            if vrfData[self.iUnit]
            else False
        ):
            flag = 1
            await self._trigger(self.TName, self.ht, self.ht, "加热器温度高", st4)
        else:
            await self._revert(self.TName, self.ht, st4)
        return flag

    async def _foo(self, judgeFunc: Callable[[], Awaitable[Literal[0, 1]]]) -> None:
        tag = judgeFunc.__name__
        r = await judgeFunc()
        if r == 0:
            status[self.iUnit][tag] = "未见异常"
        else:
            status[self.iUnit][tag] = "异常"

    async def analysis(self, MQTTClient: aiomqtt.Client) -> None:
        alerts[self.iUnit] = {"alarms": [], "timestamp": now[self.iUnit]}

        await self._foo(self._liquidLevel)
        await self._foo(self._pressure)
        await self._foo(self._temperature)
        await self._foo(self._regulatorValve)
        status[self.iUnit]["filter"] = "未见异常"

        # print(alerts)
        # print(status)
        await MQTTClient.publish(f"FJS{self.unit}/Forecast/Status", json.dumps(status[self.iUnit]))
        await MQTTClient.publish(f"FJS{self.unit}/Forecast/Alerts", json.dumps(alerts[self.iUnit]))

        data = {k: v.tolist() for k, v in vrfData[self.iUnit].items()}
        data["timestamp"] = [
            (currentTime[self.iUnit] + timedelta(seconds=FORECAST_INTERVAL * i)).strftime(DATE_FORMAT)
            for i in range(-HISTORY_STEP + 1, FORECAST_STEP + 1)
        ]
        await MQTTClient.publish(f"FJS{self.unit}/Forecast/VR", json.dumps(data))


class Health:

    @inject
    def __init__(
        self,
        unit: str,
        conn: RedisContainer = Provide[AppContainer.redis_package.service],
    ) -> None:
        self.unit = unit
        self.iUnit = int(unit) - 1
        self.redis_conn = conn
        self.keys = [
            f"FJS{self.unit}:Mechanism:regulatorValve",
            f"FJS{self.unit}:Mechanism:mainValve",
            f"FJS{self.unit}:Mechanism:liquidLevel",
            f"FJS{self.unit}:Mechanism:pressure",
            f"FJS{self.unit}:Mechanism:temperature",
            f"FJS{self.unit}:Mechanism:filter",
        ]

    async def _machanism_alarm_nums(self) -> int:
        count = 0
        for key in self.keys:
            dic = await self.redis_conn.hgetall(key)
            for v in dic.values():
                # print(v)
                if v != '0':
                    count += 1
                    break
        return count

    async def health_score(
        self, MQTTClient: aiomqtt.Client, rGrubbs: float, rLscp: float, rSRA: float
    ) -> None:
        nums = await self._machanism_alarm_nums()
        score = ANOMALY_SCORE + ALERT_SCORE
        score -= min(ALERT_SCORE, PER_ALERT_SCORE * (len(alerts[self.iUnit]["alarms"]) + nums))
        remainder = score - ANOMALY_COL_RATIO
        if rLscp > ANOMALY_COL_RATIO:
            score -= LSCP_SCORE * (rLscp - ANOMALY_COL_RATIO) / remainder
        if rGrubbs > ANOMALY_COL_RATIO:
            score -= GRUBBS_SCORE * (rGrubbs - ANOMALY_COL_RATIO) / remainder
        if rSRA > ANOMALY_COL_RATIO:
            score -= SRA_SCORE * (rSRA - ANOMALY_COL_RATIO) / remainder

        data = {
            "timestamp": now[self.iUnit],
            "healthscore": round(score, DECIMALS),
        }
        # print(data)
        if healthQ[self.iUnit].full():
            healthQ[self.iUnit].get()
        healthQ[self.iUnit].put(data)

        result_list = list(healthQ[self.iUnit].queue)
        healthscores = []
        timestamps = []

        for item in result_list:
            healthscores.append(item["healthscore"])
            timestamps.append(item["timestamp"])

        new_data = {
            "healthscore": healthscores,
            "timestamp": timestamps
        }

        await MQTTClient.publish(f"FJS{self.unit}/Forecast/Health", json.dumps(new_data))


async def test(unit: str) -> None:
    count = 0
    identifier = str(uuid.uuid4())
    tls_params = (
        aiomqtt.TLSParameters(
            ca_certs=MQTT_CA_CERTS,
            certfile=MQTT_CERTFILE,
            keyfile=MQTT_KEYFILE,
            keyfile_password=MQTT_KEYFILE_PASSWORD,
        )
        if MQTT_CA_CERTS
        else None
    )
    try:
        async with aiomqtt.Client(
            hostname=MQTT_IP,
            port=MQTT_PORT,
            username=MQTT_USERNAME,
            password=MQTT_PASSWORD,
            identifier=identifier,
            tls_params=tls_params
        ) as MQTTClient:
            print("----build new MQTT connection----")
            gd = GetData(unit)
            ad = AnomalyDetection(unit)
            fore = Forecast(unit)
            lgc = Logic(unit)
            hlth = Health(unit)
            while 1:
                start = time()

                gd.get_forecast_df()
                gd.get_anomaly_df()
                fore.vr_forecast()
                lgc.getNow()

                await hlth.health_score(MQTTClient, ad.grubbs_t(), ad.lscp_t(), ad.spectral_residual_saliency())
                await lgc.analysis(MQTTClient)

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
        # await redis_conn.aclose()


class Tasks:

    def __init__(self, unit: str) -> None:
        app = AppContainer()
        app.wire(modules=[__name__])

        self.unit = unit
        self.exec = futures.ThreadPoolExecutor()
        self.count = 0

        self.gd = GetData(unit)
        self.ad = AnomalyDetection(unit)
        self.fore = Forecast(unit)
        self.lgc = Logic(unit)
        self.hlth = Health(unit)
        self.identifier = str(uuid.uuid4())
        self.tls_params = (
            aiomqtt.TLSParameters(
                ca_certs=MQTT_CA_CERTS,
                certfile=MQTT_CERTFILE,
                keyfile=MQTT_KEYFILE,
                keyfile_password=MQTT_KEYFILE_PASSWORD,
            )
            if MQTT_CA_CERTS
            else None
        )

    def _get_data(self) -> None:
        future_get_forecast_df = self.exec.submit(self.gd.get_forecast_df)
        future_get_anomaly_df = self.exec.submit(self.gd.get_anomaly_df)
        futures.wait([future_get_forecast_df, future_get_anomaly_df])

    def _anom_fore(self) -> Tuple[float, ...]:
        future_grubbs = self.exec.submit(self.ad.grubbs_t)
        future_lscp = self.exec.submit(self.ad.lscp_t)
        future_spectral = self.exec.submit(self.ad.spectral_residual_saliency)
        future_vr_forecast = self.exec.submit(self.fore.vr_forecast)
        future_get_now = self.exec.submit(self.lgc.getNow)
        futures.wait(
            cast(
                Iterable[futures.Future],
                [
                    future_vr_forecast,
                    future_grubbs,
                    future_lscp,
                    future_spectral,
                    future_get_now,
                ],
            )
        )
        return future_grubbs.result(), future_lscp.result(), future_spectral.result(),

    async def _heal_anal(self, anoms: Tuple[float, ...], MQTTClient: aiomqtt.Client) -> None:
        tasks = []
        tasks.append(await asyncio.to_thread(self.hlth.health_score, MQTTClient, *anoms))
        tasks.append(await asyncio.to_thread(self.lgc.analysis, MQTTClient))
        await asyncio.gather(*tasks)

    async def _task(self, MQTTClient: aiomqtt.Client) -> None:
        self._get_data()
        r = self._anom_fore()
        await asyncio.gather(self._heal_anal(r, MQTTClient))

    async def run(self) -> None:
        try:
            async with aiomqtt.Client(
                hostname=MQTT_IP,
                port=MQTT_PORT,
                username=MQTT_USERNAME,
                password=MQTT_PASSWORD,
                identifier=self.identifier,
                tls_params=self.tls_params
            ) as MQTTClient:
                print("----build new MQTT connection----")

                while 1:
                    start = time()

                    await self._task(MQTTClient)

                    end = time()
                    elapsed_time = int((end - start) * 1000000)
                    self.count += 1
                    print(f"Loop {self.count} (unit {self.unit}) time used: {elapsed_time} microseconds")
                    sleep(max(INTERVAL - elapsed_time / 1000000, 0))
        except aiomqtt.MqttError:
            print(f"MQTT connection lost; Reconnecting in 5 seconds ...")
            await asyncio.sleep(5)
        except KeyboardInterrupt:
            print("KeyboardInterrupt received. Shutting down the executor...")
            # await redis_conn.aclose()
            self.exec.shutdown(wait=False)


if __name__ == "__main__":
    app = AppContainer()
    app.wire(modules=[__name__])

    asyncio.run(test('2'))
