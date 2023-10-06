/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <iostream>
#include <map>

// 此处重载了<<操作符，在ColMeta中进行了调用
template <typename T, typename = typename std::enable_if<std::is_enum<T>::value, T>::type>
std::ostream &operator<<(std::ostream &os, const T &enum_val)
{
    os << static_cast<int>(enum_val);
    return os;
}

template <typename T, typename = typename std::enable_if<std::is_enum<T>::value, T>::type>
std::istream &operator>>(std::istream &is, T &enum_val)
{
    int int_val;
    is >> int_val;
    enum_val = static_cast<T>(int_val);
    return is;
}

struct Rid
{
    int page_no;
    int slot_no;

    friend bool operator==(const Rid &x, const Rid &y)
    {
        return x.page_no == y.page_no && x.slot_no == y.slot_no;
    }

    friend bool operator<(const Rid &r1, const Rid &r2)
    {
        if (r1.page_no != r2.page_no)
            return r1.page_no < r2.page_no;
        else
            return r1.slot_no < r2.slot_no;
    }

    friend bool operator!=(const Rid &x, const Rid &y) { return !(x == y); }

    void copy(const Rid &value)
    {
        page_no = value.page_no;
        slot_no = value.slot_no;
    }

    // friend std::ostream& operator<<(std::ostream& out, const Rid& r){
    //     out<< "{"<< r.page_no << ", "<< r.slot_no<<"}";
    //     return out;
    // }

    std::string to_string()
    {
        return "{" + std::to_string(page_no) + ", " + std::to_string(slot_no) + "}";
    }
};

enum ColType
{
    TYPE_INT,
    TYPE_FLOAT,
    TYPE_STRING,
    TYPE_DATETIME,
    TYPE_BIGINT,
    TYPE_UNUSE
};

enum AgreType
{
    AGRE_TYPE_MAX,
    AGRE_TYPE_MIN,
    AGRE_TYPE_SUM,
    AGRE_TYPE_COUNT,
    AGRE_TYPE_COUNT_ALL
};

inline std::string coltype2str(ColType type)
{
    std::map<ColType, std::string> m = {
        {TYPE_INT, "INT"},
        {TYPE_FLOAT, "FLOAT"},
        {TYPE_STRING, "STRING"},
        {TYPE_DATETIME, "DATETIME"},
        {TYPE_BIGINT, "BIGINT"}};
    return m.at(type);
}

class RecScan
{
public:
    virtual ~RecScan() = default;

    virtual void next() = 0;

    virtual bool is_end() const = 0;

    virtual Rid rid() const = 0;
};
