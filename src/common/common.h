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

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <vector>
#include "defs.h"
#include "record/rm_defs.h"
#include <map>


#define DEBUG_PERFORMANCE

struct TabCol {
    std::string tab_name;
    std::string col_name;

    friend bool operator<(const TabCol &x, const TabCol &y) {
        return std::make_pair(x.tab_name, x.col_name) < std::make_pair(y.tab_name, y.col_name);
    }
};

struct Value {
    ColType type;  // type of value
    union {
        int int_val;            // int value
        float float_val;        // float value
        long long bigint_val;   // bigint value
    };

    std::string str_val;  // string value

    std::string datetime_val; // datetime value

    std::shared_ptr<RmRecord> raw;  // raw record buffer

    Value() = default;
    
    void copy_from(const Value& v){
        type = v.type;
        if(this->type == TYPE_INT){
            int_val = v.int_val;
        }
        else if(this->type == TYPE_FLOAT){
            float_val = v.float_val;
        }
        else if(this->type == TYPE_BIGINT){
            bigint_val =  v.bigint_val;
        }
        else if(this->type == TYPE_DATETIME){
            datetime_val = v.datetime_val;
        }
        else if(this->type == TYPE_STRING){
            str_val = v.str_val;
        }
        else{
            assert(0);
        }
    }

    bool operator<(const Value& v) const {
        assert(this->type != v.type);
        if(this->type == TYPE_INT){
            return this->int_val < v.int_val;
        }
        else if(this->type == TYPE_FLOAT){
            return this->float_val < v.float_val;
        }
        else if(this->type == TYPE_BIGINT){
            return this->bigint_val < v.bigint_val;
        }
        else if(this->type == TYPE_DATETIME){
            return this->datetime_val < v.datetime_val;
        }
        else if(this->type == TYPE_STRING){
            return this->str_val < v.str_val;
        }
        else{
            assert(0);
        }
    }

    bool operator==(const Value& v) const {
        assert(this->type == v.type);
        if(this->type == TYPE_INT){
            return this->int_val == v.int_val;
        }
        else if(this->type == TYPE_FLOAT){
            return this->float_val == v.float_val;
        }
        else if(this->type == TYPE_BIGINT){
            return this->bigint_val == v.bigint_val;
        }
        else if(this->type == TYPE_DATETIME){
            return this->datetime_val == v.datetime_val;
        }
        else if(this->type == TYPE_STRING){
            return this->str_val == v.str_val;
        }
        else{
            assert(0);
        }
        return false;
    }

    void set_int(int int_val_) {
        type = TYPE_INT;
        int_val = int_val_;
    }

    void set_bigint(long long bigint_val_) {
        type = TYPE_BIGINT;
        bigint_val = bigint_val_;
    }

    void set_float(float float_val_) {
        type = TYPE_FLOAT;
        float_val = float_val_;
    }

    void set_str(std::string str_val_) {
        type = TYPE_STRING;
        str_val = std::move(str_val_);
    }

    void set_datetime(const std::string& str_val_){
        type = TYPE_DATETIME;
        datetime_val = str_val_;
    }

    void set_int_val(int int_val_) {
        int_val = int_val_;
    }

    void set_bigint_val(long long bigint_val_) {
        bigint_val = bigint_val_;
    }

    void set_float_val(float float_val_) {
        float_val = float_val_;
    }

    void set_str_val(std::string str_val_) {
        str_val = std::move(str_val_);
    }

    void set_datetime_val(const std::string& str_val_){
        datetime_val = str_val_;
    }

    void get_val_from_raw(const char* val, int len){
        if (type == TYPE_INT) {
            assert(len == sizeof(int));
            int_val = *(int *)(val);
        } else if (type == TYPE_BIGINT) {
            assert(len == sizeof(long long));
            bigint_val = *(long long *)(val);
        } else if (type == TYPE_FLOAT) {
            assert(len == sizeof(float));
            float_val = *(float *)(val);
        } else if (type == TYPE_STRING) {
            str_val = std::string(val, len);
        }
        else if (type == TYPE_DATETIME) {
            datetime_val = std::string(val, len);
        }
    }

    void init_raw(int len) {
        assert(raw == nullptr && "raw is not nullptr");
        raw = std::make_shared<RmRecord>(len);
        if (type == TYPE_INT) {
            assert(len == sizeof(int));
            *(int *)(raw->data) = int_val;
        } else if (type == TYPE_BIGINT) {
            assert(len == sizeof(long long));
            *(long long *)(raw->data) = bigint_val;
        } else if (type == TYPE_FLOAT) {
            assert(len == sizeof(float));
            *(float *)(raw->data) = float_val;
        } else if (type == TYPE_STRING) {
            if (len < (int)str_val.size()) {
                throw StringOverflowError();
            }
            memset(raw->data, 0, len);
            memcpy(raw->data, str_val.c_str(), str_val.size());
        }
        else if (type == TYPE_DATETIME) {
            if (len != (int)strlen(datetime_val.c_str())){
                assert(0 && "Error init_raw():DatetimeOverflowError.");
            }
            memset(raw->data, 0, len);
            memcpy(raw->data, datetime_val.c_str(), datetime_val.size());

        }
    }

    void cover_raw(int len) {
        assert(raw != nullptr);
        if (type == TYPE_INT) {
            assert(len == sizeof(int));
            *(int *)(raw->data) = int_val;
        } else if (type == TYPE_BIGINT) {
            assert(len == sizeof(long long));
            *(long long *)(raw->data) = bigint_val;
        } else if (type == TYPE_FLOAT) {
            assert(len == sizeof(float));
            *(float *)(raw->data) = float_val;
        } else if (type == TYPE_STRING) {
            if (len < (int)str_val.size()) {
                throw StringOverflowError();
            }
            memset(raw->data, 0, len);
            memcpy(raw->data, str_val.c_str(), str_val.size());
        }
        else if (type == TYPE_DATETIME) {
            if (len != (int)strlen(datetime_val.c_str())){
                assert(0 && "Error init_raw():DatetimeOverflowError.");
            }
            memset(raw->data, 0, len);
            memcpy(raw->data, datetime_val.c_str(), datetime_val.size());

        }
    }

};

struct ValueHash{
    size_t operator()(const Value& v) const{
        if(v.type == TYPE_INT){
            return std::hash<int>()(v.int_val);
        }
        else if(v.type == TYPE_FLOAT){
            return std::hash<float>()(v.float_val);
        }
        else if(v.type == TYPE_BIGINT){
            return std::hash<long long>()(v.bigint_val);
        }
        else if(v.type == TYPE_DATETIME){
            return std::hash<std::string>()(v.str_val);
        }
        else if(v.type == TYPE_STRING){
            return std::hash<std::string>()(v.datetime_val);
        }
        else{
            assert(0);
        }
        return 0;
    }
};

enum CompOp { OP_EQ, OP_NE, OP_LT, OP_GT, OP_LE, OP_GE };

enum class SetOp{ OP_ASSIGN, OP_PLUS, OP_MINUS, OP_INVALID};

struct AgreValue {
    AgreType agre_type;
    ColType type;
    union {
        int int_val;            // int value
        float float_val;        // float value
    };
    char* str_val = nullptr;  // string value
    int str_len;
    ~AgreValue() {
        if(str_val != nullptr) {
            delete[] str_val;
            str_val = nullptr;
        }
    }

    void init_agre_raw(ColType value_type, AgreType agre_type_, int len) {
        type = value_type;
        agre_type = agre_type_;
        if(type != TYPE_INT && type != TYPE_FLOAT && type != TYPE_STRING) {
            assert(0);
        }

        if(type == TYPE_STRING) {
            str_len = len;
        }

        if (agre_type == AGRE_TYPE_MAX) {
            if (type == TYPE_INT) {
                assert(len == sizeof(int));
                int_val = std::numeric_limits<int>::min();
            } else if (type == TYPE_FLOAT) {
                assert(len == sizeof(float));
                float_val = std::numeric_limits<float>::lowest();
            } else if (type == TYPE_STRING) {
                str_val = new char[len];
                memset(str_val, 0, len);
            }
        } else if (agre_type == AGRE_TYPE_MIN) {
            if (type == TYPE_INT) {
                assert(len == sizeof(int));
                int_val = std::numeric_limits<int>::max();
            } else if (type == TYPE_FLOAT) {
                assert(len == sizeof(float));
                float_val = std::numeric_limits<float>::max();
            } else if (type == TYPE_STRING) {
                str_val = new char[len];
                memset(str_val, 0xff, len);
            }
        } else if (agre_type == AGRE_TYPE_SUM) {
            if (type == TYPE_INT) {
                assert(len == sizeof(int));
                int_val = 0;
            } else if (type == TYPE_FLOAT) {
                assert(len == sizeof(float));
                float_val = 0;
            }
            else if(type == TYPE_STRING) {
                assert(0);
            }
        }
    }

    void add_value(const char* value, int len) {
        if (agre_type == AGRE_TYPE_MAX) {
            if (type == TYPE_INT) {
                assert(len == sizeof(int));
                int value_ = *(int *)(value);
                if(value_ > int_val) {
                    int_val = value_;
                }
            } else if (type == TYPE_FLOAT) {
                assert(len == sizeof(float));
                float value_ = *(float *)(value);
                if(value_ > float_val) {
                    float_val = value_;
                }
            } else if (type == TYPE_STRING) {
                if(memcmp(value, str_val, len) > 0) {
                    memcpy(str_val, value, len);
                }
            }
        } else if (agre_type == AGRE_TYPE_MIN) {
            if (type == TYPE_INT) {
                assert(len == sizeof(int));
                int value_ = *(int *)(value);
                if(value_ < int_val) {
                    int_val = value_;
                }
            } else if (type == TYPE_FLOAT) {
                assert(len == sizeof(float));
                float value_ = *(float *)(value);
                if(value_ < float_val) {
                    float_val = value_;
                }
            } else if (type == TYPE_STRING) {
                if(memcmp(value, str_val, len) < 0) {
                    memcpy(str_val, value, len);
                }
            }
        } else if (agre_type == AGRE_TYPE_SUM) {
            if (type == TYPE_INT) {
                assert(len == sizeof(int));
                int value_ = *(int *)(value);
                int_val += value_;
            } else if (type == TYPE_FLOAT) {
                assert(len == sizeof(float));
                float value_ = *(float *)(value);
                float_val += value_;
            }
            else if(type == TYPE_STRING) {
                assert(0);
            }
        }
    }
    
    std::unique_ptr<RmRecord> get_value() {
        if (agre_type == AGRE_TYPE_MAX || agre_type == AGRE_TYPE_MIN || agre_type == AGRE_TYPE_SUM) {
            if (type == TYPE_INT) {
                std::unique_ptr<RmRecord> ret = std::make_unique<RmRecord>((int)sizeof(int));
                *(int *)ret->data = int_val;
                return ret;
            } else if (type == TYPE_FLOAT) {
                std::unique_ptr<RmRecord> ret = std::make_unique<RmRecord>((int)sizeof(float));
                *(float *)ret->data = float_val;
                return ret;
            } else if (type == TYPE_STRING) {
                if(agre_type == AGRE_TYPE_SUM) {
                    assert(0);
                }
                else {
                    std::unique_ptr<RmRecord> ret = std::make_unique<RmRecord>(str_len);
                    memcpy(ret->data, str_val, str_len);
                    return ret;
                }
            }
        }
        return nullptr;
    }
};

struct Condition {
    TabCol lhs_col;   // left-hand side column
    CompOp op;        // comparison operator
    bool is_rhs_val;  // true if right-hand side is a value (not a column)
    TabCol rhs_col;   // right-hand side column
    Value rhs_val;    // right-hand side value
    bool operator<(const Condition& c) const {
        return std::make_tuple(lhs_col, op, is_rhs_val, rhs_col, rhs_val)
            < std::make_tuple(c.lhs_col, c.op, c.is_rhs_val, c.rhs_col, c.rhs_val);
    }
    bool is_join_eq() const{
        return (op == CompOp::OP_EQ) && (!is_rhs_val) && (lhs_col.tab_name != rhs_col.tab_name);
    }
};

struct SetClause {
    TabCol lhs;
    Value rhs;
    SetOp set_op;
};

//重载pair的hash
struct HashPair
{
    template<typename T, typename U>
    size_t operator() (const std::pair<T, U> &i) const
    {
        return std::hash<T>()(i.first) ^ std::hash<U>()(i.second);
    }
};