/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "conf/falcon_config.h"

#include <cstring>
#include <fstream>

#include "falcon_code.h"
#include "log/logging.h"

int32_t FalconConfig::InitConf(const std::string &file)
{
    int32_t ret = LoadConfig(file);
    if (ret != OK) {
        return ret;
    }

    FALCON_LOG(LOG_INFO) << "Init FALCON configuration successfully";
    return OK;
}

int32_t FalconConfig::LoadConfig(const std::string &file)
{
    int32_t ret = ReadJsonFile(file);
    if (ret != OK) {
        return ret;
    }

    auto properties = ParseJsonConfig(Scope::FALCON);
    if (!properties.has_value()) {
        FALCON_LOG(LOG_ERROR) << "Init Falcon config failed, caused by parse config failed.";
        return FALCON_IEC_INIT_CONF_FAILED;
    }

    propertiesMap.insert(properties->begin(), properties->end());

    return OK;
}

int32_t FalconConfig::ReadJsonFile(const std::string &file)
{
    if (file.empty()) {
        FALCON_LOG(LOG_ERROR) << "Init Falcon config failed, file is empty.";
        return FALCON_IEC_INIT_CONF_FAILED;
    }

    char realFilePath[PATH_MAX];
    (void)memset(realFilePath, 0, sizeof(realFilePath));

    auto formatRet = realpath(file.c_str(), realFilePath);
    if (formatRet == nullptr) {
        FALCON_LOG(LOG_ERROR) << "Normalize file failed with error: " << strerror(errno);
        FALCON_LOG(LOG_ERROR) << "config file path: " << file;
        return FALCON_IEC_INIT_CONF_FAILED;
    }

    std::ifstream conf(realFilePath);
    if (!conf.is_open()) {
        FALCON_LOG(LOG_ERROR) << "Init FALCON config failed, open file: " << file.c_str() << " failed.";
        return FALCON_IEC_INIT_CONF_FAILED;
    }

    Json::CharReaderBuilder builder;
    builder["collectComments"] = false;
    JSONCPP_STRING errs;

    bool parseRet = Json::parseFromStream(builder, conf, &root, &errs);
    if (!parseRet || !errs.empty()) {
        FALCON_LOG(LOG_ERROR) << "Init FALCON config failed, parse config from steam failed, file is " << file.c_str();
        return FALCON_IEC_INIT_CONF_FAILED;
    }

    return OK;
}

std::any FalconConfig::LookUpProperty(const std::shared_ptr<PropertyKey> &key)
{
    std::shared_lock<std::shared_mutex> readLock(mutex_);
    auto iter = propertiesMap.find(key);
    if (iter == propertiesMap.end()) {
        FALCON_LOG(LOG_ERROR) << "Property " << key->GetName() << " not exist.";
        return std::any();
    }
    return iter->second;
}

std::string FalconConfig::GetArray(const std::shared_ptr<PropertyKey> &key)
{
    auto value = LookUpProperty(key);
    if (!value.has_value() || value.type() != typeid(std::string)) {
        FALCON_LOG(LOG_ERROR) << "Get array property failed, " << key->GetName() << " if of type "
                              << key->GetDataType();
        return "";
    }
    return std::any_cast<std::string>(value);
}

uint32_t FalconConfig::GetUint32(const std::shared_ptr<PropertyKey> &key)
{
    auto value = LookUpProperty(key);
    if (!value.has_value() || value.type() != typeid(uint32_t)) {
        FALCON_LOG(LOG_ERROR) << "Get uint32 property failed, " << key->GetName() << " if of type "
                              << key->GetDataType();
        return 0U;
    }
    return std::any_cast<uint32_t>(value);
}

uint64_t FalconConfig::GetUint64(const std::shared_ptr<PropertyKey> &key)
{
    auto value = LookUpProperty(key);
    if (!value.has_value() || value.type() != typeid(uint64_t)) {
        FALCON_LOG(LOG_ERROR) << "Get uint64 property failed, " << key->GetName() << " if of type "
                              << key->GetDataType();
        return 0UL;
    }
    return std::any_cast<uint64_t>(value);
}

std::string FalconConfig::GetString(const std::shared_ptr<PropertyKey> &key)
{
    auto value = LookUpProperty(key);
    if (!value.has_value() || value.type() != typeid(std::string)) {
        FALCON_LOG(LOG_ERROR) << "Get string property failed, " << key->GetName() << " if of type "
                              << key->GetDataType();
        return "";
    }
    return std::any_cast<std::string>(value);
}

double FalconConfig::GetDouble(const std::shared_ptr<PropertyKey> &key)
{
    auto value = LookUpProperty(key);
    if (!value.has_value() || value.type() != typeid(double)) {
        FALCON_LOG(LOG_ERROR) << "Get double property failed, " << key->GetName() << " if of type "
                              << key->GetDataType();
        return 0.0;
    }
    return std::any_cast<double>(value);
}

bool FalconConfig::GetBool(const std::shared_ptr<PropertyKey> &key)
{
    auto value = LookUpProperty(key);
    if (!value.has_value() || value.type() != typeid(bool)) {
        FALCON_LOG(LOG_ERROR) << "Get bool property failed, " << key->GetName() << " if of type " << key->GetDataType();
        return false;
    }
    return std::any_cast<bool>(value);
}

void ParseJsonTree(const Json::Value &base, std::map<std::string, Json::Value> &jsonKvMap)
{
    auto members = base.getMemberNames();
    for (const auto &key : members) {
        const Json::Value &value = base[key];
        jsonKvMap[key] = value;
    }
}

std::optional<FalconConfig::PropertyMapType> FalconConfig::ParseJsonConfig(Scope scope)
{
    Json::Value mainBaseValue = root["main"];
    Json::Value runtimeBaseValue = root["runtime"];
    std::map<std::string, Json::Value> jsonKvMap;
    ParseJsonTree(mainBaseValue, jsonKvMap);
    ParseJsonTree(runtimeBaseValue, jsonKvMap);

    std::map<const std::shared_ptr<PropertyKey>, std::any> properties;

    for (const auto &kv : PropertyKey::keyMap) {
        std::string keyString = kv.first;
        auto propertyKey = kv.second;
        if (propertyKey == nullptr) {
            FALCON_LOG(LOG_ERROR) << "Get property key failed.";
            return std::nullopt;
        }
        if (propertyKey->GetScope() != scope) {
            continue;
        }
        auto jsonMapItr = jsonKvMap.find(keyString);
        if (jsonMapItr == jsonKvMap.end()) {
            FALCON_LOG(LOG_ERROR) << "Parse json config failed, property " << keyString.c_str() << " not exist.";
            return std::nullopt;
        }

        std::any value = FormatUtil::JsonToAny(jsonMapItr->second, propertyKey->GetDataType());
        if (!value.has_value()) {
            FALCON_LOG(LOG_ERROR) << "Parse json config failed, property " << keyString.c_str() << " should be of type "
                                  << propertyKey->GetDataType();
            return std::nullopt;
        }
        properties.insert(std::pair<std::shared_ptr<PropertyKey>, std::any>(propertyKey, value));
    }
    return properties;
}

std::any FormatUtil::ParseJsonArray(const Json::Value &value)
{
    std::string tmp;
    if (!value.empty()) {
        for (const auto &element : value) {
            tmp.append(element.asString() + ',');
        }
        tmp.pop_back();
    }
    return std::any(tmp);
}

std::any FormatUtil::JsonToAny(const Json::Value &value, DataType dataType)
{
    auto search = JsonToAnyConverter.find(dataType);
    return (search != JsonToAnyConverter.end()) ? JsonToAnyConverter[dataType](value) : std::any();
}

std::any FormatUtil::StringToAny(const std::string &value, DataType dataType)
{
    auto search = StringToAnyConverter.find(dataType);
    return (search != StringToAnyConverter.end()) ? StringToAnyConverter[dataType](value) : std::any();
}

std::string FormatUtil::AnyToString(const std::any &value, DataType dataType)
{
    auto search = AnyToStringConverter.find(dataType);
    return (search != AnyToStringConverter.end()) ? AnyToStringConverter[dataType](value) : std::string();
}

std::map<DataType, FormatUtil::JsonToAnyFunc> FormatUtil::JsonToAnyConverter = {
    {FALCON_STRING,
     [](const Json::Value &value) { return value.isString() ? std::any(value.asString()) : std::any(); }},
    {FALCON_UINT, [](const Json::Value &value) { return value.isUInt() ? std::any(value.asUInt()) : std::any(); }},
    {FALCON_BOOL, [](const Json::Value &value) { return value.isBool() ? std::any(value.asBool()) : std::any(); }},
    {FALCON_ARRAY, [](const Json::Value &value) { return value.isArray() ? ParseJsonArray(value) : std::any(); }},
    {FALCON_UINT64,
     [](const Json::Value &value) { return value.isUInt64() ? std::any(value.asUInt64()) : std::any(); }},
    {FALCON_DOUBLE,
     [](const Json::Value &value) { return value.isDouble() ? std::any(value.asDouble()) : std::any(); }}};

std::map<DataType, FormatUtil::StringToAnyFunc> FormatUtil::StringToAnyConverter = {
    {FALCON_STRING, [](const std::string &value) { return std::any(value); }},
    {FALCON_UINT, [](const std::string &value) { return std::any(std::stoul(value)); }},
    {FALCON_BOOL, [](const std::string &value) { return std::any(value != "0"); }},
    {FALCON_ARRAY, [](const std::string &value) { return std::any(value); }},
    {FALCON_UINT64, [](const std::string &value) { return std::any(std::stoull(value)); }},
    {FALCON_DOUBLE, [](const std::string &value) { return std::any(std::stod(value)); }}};

std::map<DataType, FormatUtil::AnyToStringFunc> FormatUtil::AnyToStringConverter = {
    {FALCON_STRING, [](const std::any &value) { return std::any_cast<std::string>(value); }},
    {FALCON_UINT, [](const std::any &value) { return std::to_string(std::any_cast<uint32_t>(value)); }},
    {FALCON_BOOL, [](const std::any &value) { return std::to_string(std::any_cast<bool>(value)); }},
    {FALCON_ARRAY, [](const std::any &value) { return std::any_cast<std::string>(value); }},
    {FALCON_UINT64, [](const std::any &value) { return std::to_string(std::any_cast<uint64_t>(value)); }},
    {FALCON_DOUBLE, [](const std::any &value) { return std::to_string(std::any_cast<double>(value)); }}};
