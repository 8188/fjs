#include <chrono>
#include <ctime>
// #include <execution>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string_view>

#include "csv.hpp"
#include "dotenv.h"
#include "nlohmann/json.hpp"
#include "taskflow/taskflow.hpp"
#include <mqtt/async_client.h>
#include <sw/redis++/redis++.h>

using json = nlohmann::json;

constexpr const long long INTERVAL { 5000000 };
constexpr const int QOS { 1 };
constexpr const auto TIMEOUT { std::chrono::seconds(10) };
constexpr const char* DATE_FORMAT { "%Y-%m-%d %H:%M:%S" };

const std::vector<std::string> codes_with_unit(const std::string& unit, const std::vector<std::string>& codes)
{
    std::vector<std::string> result;
    result.reserve(codes.size());
    for (const auto& item : codes) {
        result.emplace_back(unit + item);
    }
    // std::for_each(std::execution::par, codes.begin(), codes.end(), [&](const auto& item) {
    //     result.emplace_back(unit + item);
    // });
    return result;
}

bool fileExists(const std::string& filename)
{
    std::ifstream file(filename);
    return file.good();
}

std::string generate_random_string_with_hyphens()
{
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 15);

    std::stringstream ss;
    const char* hex_chars = "0123456789abcdef";
    for (int i = 0; i < 32; ++i) {
        if (i == 8 || i == 12 || i == 16 || i == 20) {
            ss << "-";
        }
        int index = dis(gen);
        ss << hex_chars[index];
    }

    return ss.str();
}

const std::string get_now()
{
    constexpr int BUFFER_SIZE { 20 };

    auto now { std::chrono::system_clock::now() };
    auto now_time { std::chrono::system_clock::to_time_t(now) };
    char buffer[BUFFER_SIZE];
    std::strftime(buffer, BUFFER_SIZE, DATE_FORMAT, std::localtime(&now_time));
    return std::string(buffer);
}

std::time_t string2time(const std::string& timeStr)
{
    std::tm tm = {};
    strptime(timeStr.c_str(), DATE_FORMAT, &tm);
    return std::mktime(&tm);
}

class MyRedis {
private:
    sw::redis::ConnectionOptions makeConnectionOptions(const std::string& ip, int port, int db, const std::string& user, const std::string& password)
    {
        sw::redis::ConnectionOptions opts;
        opts.host = ip;
        opts.port = port;
        opts.db = db;
        if (!user.empty()) {
            opts.user = user;
        }
        if (!password.empty()) {
            opts.password = password;
        }
        opts.socket_timeout = std::chrono::milliseconds(50);
        return opts;
    }

    sw::redis::ConnectionPoolOptions makePoolOptions()
    {
        sw::redis::ConnectionPoolOptions pool_opts;
        pool_opts.size = 3;
        pool_opts.wait_timeout = std::chrono::milliseconds(50);
        return pool_opts;
    }

public:
    sw::redis::Redis redis;

    MyRedis(const std::string& ip, int port, int db, const std::string& user, const std::string& password)
        : redis(makeConnectionOptions(ip, port, db, user, password), makePoolOptions())
    {
        std::cout << "Connected to Redis.\n";
    }
};

class MyMQTT {
private:
    mqtt::async_client client;
    mqtt::connect_options connOpts;

    mqtt::connect_options buildConnectOptions(const std::string& username, const std::string& password,
        const std::string& caCerts, const std::string& certfile,
        const std::string& keyFile, const std::string& keyFilePassword) const
    {
        // mqtt::connect_options_builder()对应mqtt:/ip:port, ::ws()对应ws:/ip:port
        auto connBuilder = mqtt::connect_options_builder()
                               .user_name(username)
                               .password(password)
                               .keep_alive_interval(std::chrono::seconds(45));

        if (!caCerts.empty()) {
            mqtt::ssl_options ssl;
            ssl.set_trust_store(caCerts);
            ssl.set_key_store(certfile);
            ssl.set_private_key(keyFile);
            ssl.set_private_key_password(keyFilePassword);

            connBuilder.ssl(ssl);
        }

        return connBuilder.finalize();
    }

    void disconnect()
    {
        if (client.is_connected()) {
            client.disconnect()->wait();
            std::cout << "Disconnected from MQTT broker.\n";
        }
    }

public:
    MyMQTT(const std::string& address, const std::string& clientId,
        const std::string& username, const std::string& password,
        const std::string& caCerts, const std::string& certfile,
        const std::string& keyFile, const std::string& keyFilePassword)
        : client(address, clientId)
        , connOpts { buildConnectOptions(username, password, caCerts, certfile, keyFile, keyFilePassword) }
    {
    }

    ~MyMQTT()
    {
        disconnect();
    }

    void connect()
    {
        client.connect(connOpts)->wait();
        std::cout << "Connected to MQTT broker.\n";
    }

    void publish(const std::string& topic, const std::string& payload, int qos, bool retained = false)
    {
        auto msg = mqtt::make_message(topic, payload, qos, retained);
        bool ok = client.publish(msg)->wait_for(TIMEOUT);
        if (!ok) {
            std::cerr << "Error: Publishing message timed out." << std::endl;
        }
    }
};

class MechanismBase {
private:
    struct Alarm {
        std::string_view code;
        std::string desc;
        std::string_view advice;
        std::string startTime;
    };

public:
    const std::string m_unit;
    std::shared_ptr<MyRedis> m_redis;
    std::shared_ptr<MyMQTT> m_MQTTCli;
    std::unordered_map<std::string_view, std::unordered_map<std::string_view, std::vector<Alarm>>> alerts {};
    csv::CSVRow& m_c_df;

    // 调节阀开度
    const std::vector<std::string> regulatorValveOpening;
    // 调节阀腔室油压
    const std::vector<std::string> regulatorValveChamberOilPressure;
    // 油位
    const std::vector<std::string> oilLevel;
    // 压力
    const std::vector<std::string> oilPressure;
    // 温度
    const std::vector<std::string> oilTemperature;
    // 主汽阀开度
    const std::vector<std::string> mainValveOpening;
    // 主汽阀腔室油压
    const std::vector<std::string> mainValveChamberOilPressure;
    // 过滤器差压
    const std::vector<std::string> filterPressure;
    // 抗燃油泵电流
    const std::vector<std::string> pumpCurrent;
    // 安全油压
    const std::vector<std::string> safeOilPressure;
    const std::vector<std::string> others;
    const std::vector<std::string_view> regulatorValveTags;
    const std::vector<std::string_view> mainValveTags;
    std::vector<std::string> m_all_targets {};

    MechanismBase(const std::string& unit, std::shared_ptr<MyRedis> redis, std::shared_ptr<MyMQTT> MQTTCli, csv::CSVReader::iterator& it)
        : m_unit { unit }
        , m_redis { redis }
        , m_MQTTCli { MQTTCli }
        , m_c_df { *it }
        , regulatorValveOpening { "GRE017MM", "GRE027MM", "GRE037MM", "GRE047MM", "GRE117MM", "GRE127MM", "GRE137MM", "GRE147MM" }
        , regulatorValveChamberOilPressure { "GRE016MP", "GRE026MP", "GRE036MP", "GRE046MP", "GRE116MP", "GRE126MP", "GRE136MP", "GRE146MP" }
        , oilLevel { "GFR001MN", "GFR002MN", "GFR003MN" }
        , oilPressure { "GFR011MP", "GFR012MP", "GFR013MP" }
        , oilTemperature { "GFR006MT" /* 油温 */, "GFR008MT" /* 加热器 */ }
        , mainValveOpening { "GSE011MM", "GSE021MM", "GSE031MM", "GSE041MM", "GSE111MM", "GSE121MM", "GSE131MM", "GSE141MM" }
        , mainValveChamberOilPressure { "GSE014MP", "GSE024MP", "GSE034MP", "GSE044MP", "GSE114MP", "GSE124MP", "GSE134MP", "GSE144MP" }
        , filterPressure { "GFR039KS", "GFR046KS", "GFR043KS" }
        , pumpCurrent { "GFR011MI", "GFR111MI" }
        , safeOilPressure { "GSE202MP", "GSE203MP" }
        , others {
            "GSE001MC_XF60_AVALUE", "GSE002MC_XF60_AVALUE", "GSE003MC_XF60", /* 汽轮机转速*/
            "GME701MP", "GME702MP", "GME703MP", /* 高压进汽压力*/
            "GME711MP" /* 中压进汽压力*/,
            "GRE012MY" /* 有功功率*/,
            "GGR001SM" /* 盘车投入*/, "GGR001MI" /* 盘车电流*/, "GGR002SM" /* 盘车退出*/
        }
        , regulatorValveTags { "GRE001VV", "GRE002VV", "GRE003VV", "GRE004VV", "GRE011VV", "GRE012VV", "GRE013VV", "GRE014VV" }
        , mainValveTags { "GSE001VV", "GSE002VV", "GSE003VV", "GSE004VV", "GSE011VV", "GSE012VV", "GSE013VV", "GSE014VV" }
    {
        if (unit < "1" || unit > "9") {
            throw std::invalid_argument("unit must be in the range from '1' to '9'");
        }

        const std::vector<std::vector<std::string>> allVectors {
            regulatorValveOpening,
            regulatorValveChamberOilPressure,
            oilLevel,
            oilPressure,
            oilTemperature,
            mainValveOpening,
            mainValveChamberOilPressure,
            filterPressure,
            pumpCurrent,
            safeOilPressure,
            others,
        };

        std::size_t estimatedTotalSize { 100 };
        std::size_t totalSize { 0 };
        m_all_targets.reserve(estimatedTotalSize);
        for (const auto& vec : allVectors) {
            totalSize += vec.size();
            if (totalSize > estimatedTotalSize) {
                m_all_targets.reserve(m_all_targets.capacity() * 2);
                estimatedTotalSize *= 2;

                std::cerr << "Warning: Total size exceeded estimated size. Doubling capacity.\n";
            }
            m_all_targets.insert(m_all_targets.end(), vec.begin(), vec.end());
        }

        m_all_targets.shrink_to_fit();
    }

    virtual int logic() = 0;

    void send_message(const std::string& topic)
    {
        json j;
        for (const auto& pair : alerts[m_unit]) {
            json alarms;
            for (const auto& alarm : pair.second) {
                json alarmJson;
                alarmJson["code"] = alarm.code;
                alarmJson["desc"] = alarm.desc;
                alarmJson["advice"] = alarm.advice;
                alarmJson["startTime"] = alarm.startTime;
                alarms.push_back(alarmJson);
                // std::cout << "Code: " << alarmJson["code"] << ", Desc: " << alarmJson["desc"] << ", Advice: " << alarmJson["advice"] << ", Start Time: " << alarmJson["startTime"] << '\n';
            }
            j[std::string(pair.first)] = alarms;
        }

        const std::string jsonString = j.dump();
        m_MQTTCli->publish(topic, jsonString, QOS);
        alerts.clear();
    }

    void trigger(const std::string_view& key, const std::string_view& field, const std::string_view& tag,
        const std::string_view& content, const std::string_view& st, const std::string_view& now)
    {
        Alarm newAlarm;
        newAlarm.code = tag;
        newAlarm.desc = content;
        newAlarm.advice = "";
        newAlarm.startTime = st;

        alerts[m_unit]["alarms"].emplace_back(newAlarm);

        if (st == "0") {
            m_redis->redis.hset(key, field, now);
        }
    }

    void revert(const std::string_view& key, const std::string_view& field, const std::string_view& st) const
    {
        if (!st.empty()) {
            m_redis->redis.hset(key, field, "0");
        }
    }

    template <typename T>
    std::optional<T> get_value_from_CSVRow(csv::CSVRow& row, const std::string& colName) const
    {
        T value {};
        try {
            return row[colName].get<T>();
        } catch (const std::exception& e) {
            std::cerr << e.what() << '\n';
            return std::nullopt;
        }
    }
};

class RegulatorValve : public MechanismBase {
private:
    const std::vector<std::string> m_regulatorValveChamberOilPressure;
    const std::vector<std::string> m_safeOilPressure;
    int iUnit;

public:
    RegulatorValve(const std::string& unit, std::shared_ptr<MyRedis> redis, std::shared_ptr<MyMQTT> MQTTCli, csv::CSVReader::iterator& it)
        : MechanismBase(unit, redis, MQTTCli, it)
        , m_regulatorValveChamberOilPressure { codes_with_unit(m_unit, regulatorValveChamberOilPressure) }
        , m_safeOilPressure { codes_with_unit(m_unit, safeOilPressure) }
        , iUnit { std::stoi(m_unit) - 1 }
    {
    }

    int logic() override
    {
        int flag { 0 };
        const std::string key { "FJS" + m_unit + ":Mechanism:regulatorValve" };
        const std::string content { "开调节阀 阀门卡涩" };
        const std::string now { get_now() }; // 用string_view写入redis会乱码

        for (std::size_t i { 0 }; i < m_regulatorValveChamberOilPressure.size(); ++i) {
            const std::string chamber = m_regulatorValveChamberOilPressure[i];
            const std::string_view tag = regulatorValveTags[i];

            const auto optional_str = m_redis->redis.hget(key, chamber);
            const std::string st = optional_str.value_or("0");

            bool condition = true;
            std::optional<double> chamber_opt {}; // 提前申明，否则goto引起重复初始化
            for (const std::string& pressure : m_safeOilPressure) {
                auto pressure_opt = get_value_from_CSVRow<double>(m_c_df, pressure);
                if (!pressure_opt.has_value()) {
                    goto end_of_loops_logic_of_RegulatorValve;
                }
                if (!(pressure_opt.value() > 5)) {
                    condition = false;
                    break;
                }
            }

            chamber_opt = get_value_from_CSVRow<double>(m_c_df, chamber);
            if (!chamber_opt.has_value()) {
                continue;
            }

            if (condition && chamber_opt.value() >= 9.6) {
                trigger(key, chamber, tag, content, st, now);
                flag = 1;
            } else {
                revert(key, chamber, st);
            }
        end_of_loops_logic_of_RegulatorValve: {
        }
        }
        return flag;
    }
};

class MainValve : public MechanismBase {
private:
    const std::vector<std::string> m_mainValveChamberOilPressure;
    const std::vector<std::string> m_safeOilPressure;
    int iUnit;

public:
    MainValve(const std::string& unit, std::shared_ptr<MyRedis> redis, std::shared_ptr<MyMQTT> MQTTCli, csv::CSVReader::iterator& it)
        : MechanismBase(unit, redis, MQTTCli, it)
        , m_mainValveChamberOilPressure { codes_with_unit(m_unit, mainValveChamberOilPressure) }
        , m_safeOilPressure { codes_with_unit(m_unit, safeOilPressure) }
        , iUnit { std::stoi(m_unit) - 1 }
    {
    }

    int logic() override
    {
        int flag { 0 };
        const std::string key { "FJS" + m_unit + ":Mechanism:mainValve" };
        const std::string keyCommand { "FJS" + m_unit + ":Mechanism:command" };
        const std::string content1 { "开主汽阀 阀门卡涩" };
        const std::string content2 { "试验电磁阀或关断阀卡涩，阀门无法开启（主汽阀）" };
        const std::string now { get_now() };

        bool condition = true;
        for (const std::string& pressure : m_safeOilPressure) {
            auto pressure_opt = get_value_from_CSVRow<double>(m_c_df, pressure);
            if (!pressure_opt.has_value()) {
                return flag;
            }
            if (!(pressure_opt.value() > 5)) {
                condition = false;
                break;
            }
        }

        std::optional<std::string> optional_str;
        if (condition) {
            optional_str = m_redis->redis.hget(keyCommand, "open");
            const std::string openCommand = optional_str.value_or("0");
            if (openCommand != "1") {
                m_redis->redis.hset(keyCommand, "open", "1");
                m_redis->redis.hset(keyCommand, "openTime", now);
            } else {
                optional_str = m_redis->redis.hget(keyCommand, "openTime");
                const std::string startTime = optional_str.value_or(now);
                std::time_t nowTimestamp = string2time(now);
                std::time_t startTimeTimestamp = string2time(startTime);
                std::chrono::seconds diff = std::chrono::seconds(nowTimestamp - startTimeTimestamp);

                for (std::size_t i { 0 }; i < m_mainValveChamberOilPressure.size(); ++i) {
                    const std::string chamber = m_mainValveChamberOilPressure[i];
                    const std::string_view tag = mainValveTags[i];

                    optional_str = m_redis->redis.hget(key, chamber + "_1");
                    const std::string st1 = optional_str.value_or("0");
                    optional_str = m_redis->redis.hget(key, chamber + "_2");
                    const std::string st2 = optional_str.value_or("0");

                    auto mainValveOpenning_opt = get_value_from_CSVRow<double>(m_c_df, m_unit + "GSE011MM");
                    if (!mainValveOpenning_opt.has_value()) {
                        continue;
                    }

                    if (diff > std::chrono::seconds(180) && mainValveOpenning_opt.value() < 95) {
                        flag = 1;
                        auto chamber_opt = get_value_from_CSVRow<double>(m_c_df, chamber);
                        if (!chamber_opt.has_value()) {
                            continue;
                        }

                        if (chamber_opt.value() >= 9.6) {
                            trigger(key, chamber + "_1", tag, content1, st1, now);
                            revert(key, chamber + "_2", st2);
                        } else {
                            trigger(key, chamber + "_2", tag, content2, st2, now);
                            revert(key, chamber + "_1", st1);
                        }
                    } else {
                        revert(key, chamber + "_1", st1);
                        revert(key, chamber + "_2", st2);
                    }
                }
            }
        } else {
            for (const std::string chamber : m_mainValveChamberOilPressure) {
                optional_str = m_redis->redis.hget(key, chamber + "_1");
                const std::string st1 = optional_str.value_or("0");
                optional_str = m_redis->redis.hget(key, chamber + "_2");
                const std::string st2 = optional_str.value_or("0");

                revert(key, chamber + "_1", st1);
                revert(key, chamber + "_2", st2);
            }
            m_redis->redis.hset(keyCommand, "open", "0");
        }
        return flag;
    }
};

class LiquidLevel : public MechanismBase {
private:
    const std::vector<std::string> m_OilLevel;
    int iUnit;

public:
    LiquidLevel(const std::string& unit, std::shared_ptr<MyRedis> redis, std::shared_ptr<MyMQTT> MQTTCli, csv::CSVReader::iterator& it)
        : MechanismBase(unit, redis, MQTTCli, it)
        , m_OilLevel { codes_with_unit(m_unit, oilLevel) }
        , iUnit { std::stoi(m_unit) - 1 }
    {
    }

    int logic() override
    {
        int flag { 0 };
        const std::string key { "FJS" + m_unit + ":Mechanism:liquidLevel" };
        const std::string content { "抗燃油液位" };
        const std::string now { get_now() };

        std::optional<std::string> optional_str;
        for (const std::string& tag : m_OilLevel) {
            optional_str = m_redis->redis.hget(key, tag + "_1");
            const std::string st1 = optional_str.value_or("0");
            optional_str = m_redis->redis.hget(key, tag + "_2");
            const std::string st2 = optional_str.value_or("0");
            optional_str = m_redis->redis.hget(key, tag + "_3");
            const std::string st3 = optional_str.value_or("0");

            auto tag_opt = get_value_from_CSVRow<double>(m_c_df, tag);
            if (!tag_opt.has_value()) {
                continue;
            }
            const double level { tag_opt.value() };

            if (level > 580) {
                trigger(key, tag + "_1", tag, content + "高", st1, now);
                flag = 1;
            } else {
                revert(key, tag + "_1", st1);
            }

            if (level < 200) {
                trigger(key, tag + "_2", tag, content + "低低", st2, now);
                flag = 1;
            } else {
                revert(key, tag + "_2", st2);
            }

            if (level < 250 && level >= 200) {
                trigger(key, tag + "_3", tag, content + "低", st3, now);
                flag = 1;
            } else {
                revert(key, tag + "_3", st3);
            }
        }
        return flag;
    }
};

class Pressure : public MechanismBase {
private:
    const std::vector<std::string> m_OilPressure;
    int iUnit;

public:
    Pressure(const std::string& unit, std::shared_ptr<MyRedis> redis, std::shared_ptr<MyMQTT> MQTTCli, csv::CSVReader::iterator& it)
        : MechanismBase(unit, redis, MQTTCli, it)
        , m_OilPressure { codes_with_unit(m_unit, oilPressure) }
        , iUnit { std::stoi(m_unit) - 1 }
    {
    }

    int logic() override
    {
        int flag { 0 };
        const std::string key { "FJS" + m_unit + ":Mechanism:pressure" };
        const std::string content { "抗燃油压力" };
        const std::string now { get_now() };

        std::optional<std::string> optional_str;
        for (const std::string& tag : m_OilPressure) {
            optional_str = m_redis->redis.hget(key, tag + "_1");
            const std::string st1 = optional_str.value_or("0");
            optional_str = m_redis->redis.hget(key, tag + "_2");
            const std::string st2 = optional_str.value_or("0");

            auto tag_opt = get_value_from_CSVRow<double>(m_c_df, tag);
            if (!tag_opt.has_value()) {
                continue;
            }
            const double pressure { tag_opt.value() };

            if (pressure > 13.5) {
                trigger(key, tag + "_1", tag, content + "高", st1, now);
                flag = 1;
            } else {
                revert(key, tag + "_1", st1);
            }

            if (pressure < 10) {
                trigger(key, tag + "_2", tag, content + "低", st2, now);
                flag = 1;
            } else {
                revert(key, tag + "_2", st2);
            }
        }
        return flag;
    }
};

class Temperature : public MechanismBase {
private:
    const std::vector<std::string> m_OilTemperature;
    int iUnit;

public:
    Temperature(const std::string& unit, std::shared_ptr<MyRedis> redis, std::shared_ptr<MyMQTT> MQTTCli, csv::CSVReader::iterator& it)
        : MechanismBase(unit, redis, MQTTCli, it)
        , m_OilTemperature { codes_with_unit(m_unit, oilTemperature) }
        , iUnit { std::stoi(m_unit) - 1 }
    {
    }

    int logic() override
    {
        int flag { 0 };
        const std::string key { "FJS" + m_unit + ":Mechanism:temperature" };
        const std::string content { "抗燃油温度" };
        const std::string now { get_now() };

        std::optional<std::string> optional_str;
        const std::string ot = m_OilTemperature[0];
        const std::string ht = m_OilTemperature[1];
        optional_str = m_redis->redis.hget(key, ot + "_1");
        const std::string st1 = optional_str.value_or("0");
        optional_str = m_redis->redis.hget(key, ot + "_2");
        const std::string st2 = optional_str.value_or("0");
        optional_str = m_redis->redis.hget(key, ot + "_3");
        const std::string st3 = optional_str.value_or("0");
        optional_str = m_redis->redis.hget(key, ht);
        const std::string st4 = optional_str.value_or("0");

        auto ot_opt = get_value_from_CSVRow<double>(m_c_df, ot);
        if (!ot_opt.has_value()) {
            return flag;
        }
        const double otValue { ot_opt.value() };

        if (otValue > 60) {
            trigger(key, ot + "_1", ot, content + "高", st1, now);
            flag = 1;
        } else {
            revert(key, ot + "_1", st1);
        }

        if (otValue < 12) {
            trigger(key, ot + "_2", ot, content + "低低", st2, now);
            flag = 1;
        } else {
            revert(key, ot + "_2", st2);
        }

        if (otValue < 36 && otValue >= 12) {
            trigger(key, ot + "_3", ot, content + "低", st3, now);
            flag = 1;
        } else {
            revert(key, ot + "_3", st3);
        }

        auto ht_opt = get_value_from_CSVRow<double>(m_c_df, ht);
        if (!ht_opt.has_value()) {
            return flag;
        }
        const double htValue { ht_opt.value() };

        if (htValue > 110) {
            trigger(key, ht, ht, "加热器温度高", st4, now);
            flag = 1;
        } else {
            revert(key, ht, st4);
        }
        return flag;
    }
};

class Filter : public MechanismBase {
private:
    const std::vector<std::string> m_FilterPressure;
    int iUnit;

public:
    Filter(const std::string& unit, std::shared_ptr<MyRedis> redis, std::shared_ptr<MyMQTT> MQTTCli, csv::CSVReader::iterator& it)
        : MechanismBase(unit, redis, MQTTCli, it)
        , m_FilterPressure { codes_with_unit(m_unit, filterPressure) }
        , iUnit { std::stoi(m_unit) - 1 }
    {
    }

    int logic() override
    {
        int flag { 0 };
        const std::string key { "FJS" + m_unit + ":Mechanism:filter" };
        const std::string content { "过滤器堵塞" };
        const std::string now { get_now() };

        std::optional<std::string> optional_str;
        for (const std::string& tag : m_FilterPressure) {
            optional_str = m_redis->redis.hget(key, tag);
            const std::string st = optional_str.value_or("0");

            auto block_opt = get_value_from_CSVRow<double>(m_c_df, tag);
            if (!block_opt.has_value()) {
                continue;
            }

            if (block_opt.value() > 0.5) {
                trigger(key, tag, tag, content, st, now);
                flag = 1;
            } else {
                revert(key, tag, st);
            }
        }
        return flag;
    }
};

class Task {
private:
    const std::string m_unit;
    std::shared_ptr<MyMQTT> m_MQTTCli;
    csv::CSVReader::iterator& m_it;

    const std::string regulator_valve_topic;
    const std::string main_valve_topic;
    const std::string liquid_level_topic;
    const std::string pressure_topic;
    const std::string temperature_topic;
    const std::string filter_topic;

    RegulatorValve regulator_valve;
    MainValve main_valve;
    LiquidLevel liquid_level;
    Pressure pressure;
    Temperature temperature;
    Filter filter;

    template <typename T>
    void test(T& mechanism, const std::string& topic) const
    {
        int flag = mechanism.logic();
        std::cout << flag << '\n';
        if (flag == 1) {
            mechanism.send_message(topic);
        }
    }

    void show_points() const
    {
        csv::CSVRow& c_df { *m_it };
        const std::string jsonString = c_df.to_json();
        m_MQTTCli->publish("FJS" + m_unit + "/Points", jsonString, QOS);
    }

public:
    Task(const std::string& unit, std::shared_ptr<MyRedis> redisCli, std::shared_ptr<MyMQTT> MQTTCli, csv::CSVReader::iterator& it)
        : m_unit { unit }
        , m_MQTTCli { MQTTCli }
        , m_it { it }
        , regulator_valve_topic { "FJS" + unit + "/Mechanism/RegulatorValve" }
        , main_valve_topic { "FJS" + unit + "/Mechanism/MainValve" }
        , liquid_level_topic { "FJS" + unit + "/Mechanism/LiquidLevel" }
        , pressure_topic { "FJS" + unit + "/Mechanism/Pressure" }
        , temperature_topic { "FJS" + unit + "/Mechanism/Temperature" }
        , filter_topic { "FJS" + unit + "/Mechanism/Filter" }
        , regulator_valve { unit, redisCli, MQTTCli, it }
        , main_valve { unit, redisCli, MQTTCli, it }
        , liquid_level { unit, redisCli, MQTTCli, it }
        , pressure { unit, redisCli, MQTTCli, it }
        , temperature { unit, redisCli, MQTTCli, it }
        , filter { unit, redisCli, MQTTCli, it }
    {
    }

    tf::Taskflow flow()
    {
        tf::Taskflow f1("F1");

        tf::Task f1A = f1.emplace([&]() {
                             test(regulator_valve, regulator_valve_topic);
                         }).name("test_regulator_valve");

        tf::Task f1B = f1.emplace([&]() {
                             test(main_valve, main_valve_topic);
                         }).name("test_main_valve");

        tf::Task f1C = f1.emplace([&]() {
                             test(liquid_level, liquid_level_topic);
                         }).name("test_liquid_level");

        tf::Task f1D = f1.emplace([&]() {
                             test(pressure, pressure_topic);
                         }).name("test_pressure");

        tf::Task f1E = f1.emplace([&]() {
                             test(temperature, temperature_topic);
                         }).name("test_temperature");

        tf::Task f1F = f1.emplace([&]() {
                             test(filter, filter_topic);
                         }).name("test_filter");

        tf::Task f1G = f1.emplace([&]() {
                             show_points();
                         }).name("show_points");
        return f1;
    }
};

int main()
{
    if (!fileExists(".env")) {
        throw std::runtime_error("File .env does not exist!");
    }

    dotenv::init();
    const std::string MQTT_ADDRESS { std::getenv("MQTT_ADDRESS") };
    const std::string MQTT_USERNAME { std::getenv("MQTT_USERNAME") };
    const std::string MQTT_PASSWORD { std::getenv("MQTT_PASSWORD") };
    const std::string MQTT_CA_CERTS { std::getenv("MQTT_CA_CERTS") };
    const std::string MQTT_CERTFILE { std::getenv("MQTT_CERTFILE") };
    const std::string MQTT_KEYFILE { std::getenv("MQTT_KEYFILE") };
    const std::string MQTT_KEYFILE_PASSWORD { std::getenv("MQTT_KEYFILE_PASSWORD") };
    const std::string CLIENT_ID { generate_random_string_with_hyphens() };

    const std::string REDIS_IP { std::getenv("REDIS_IP") };
    const int REDIS_PORT = std::atoi(std::getenv("REDIS_PORT"));
    const int REDIS_DB = std::atoi(std::getenv("REDIS_DB"));
    const std::string REDIS_USER { std::getenv("REDIS_USER") };
    const std::string REDIS_PASSWORD { std::getenv("REDIS_PASSWORD") };

    auto MQTTCli = std::make_shared<MyMQTT>(MQTT_ADDRESS, CLIENT_ID, MQTT_USERNAME, MQTT_PASSWORD,
        MQTT_CA_CERTS, MQTT_CERTFILE, MQTT_KEYFILE, MQTT_KEYFILE_PASSWORD);
    MQTTCli->connect();

    auto redisCli = std::make_shared<MyRedis>(REDIS_IP, REDIS_PORT, REDIS_DB, REDIS_USER, REDIS_PASSWORD);

    const std::string unit1 { "1" };
    const int iUnit1 { std::stoi(unit1) - 1 };
    csv::CSVReader reader("test.csv");
    std::array<csv::CSVReader::iterator, 2> it {};
    for (it[iUnit1] = reader.begin(); it[iUnit1] != reader.end(); ++it[iUnit1]) {
    }
    
    Task task1(unit1, redisCli, MQTTCli, it[iUnit1]);

    tf::Executor executor;
    int count { 0 };

    while (1) {
        auto start = std::chrono::steady_clock::now();

        executor.run(task1.flow()).wait();
        // executor.run(task2.flow()).wait();

        auto end = std::chrono::steady_clock::now();
        auto elapsed_time = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        printf("Loop %d time used: %ld microseconds\n", ++count, elapsed_time.count());
        std::this_thread::sleep_for(std::chrono::microseconds(INTERVAL - elapsed_time.count()));
    }

    return 0;
}