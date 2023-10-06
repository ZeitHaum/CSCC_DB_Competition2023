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

#include "execution_defs.h"
#include "common/common.h"
#include "index/ix.h"
#include "system/sm.h"
#include "analyze/analyze.h"


class AbstractExecutor {
   public:
    Rid _abstract_rid;
    std::vector<ColMeta> _cols;
    Context *context_;

    std::string get_hash_key(const TabCol& t){
        return t.tab_name + "$" + t.col_name;
    }

    std::string get_hash_key(const ColMeta& c){
        std::string ret;
        ret += c.tab_name;
        ret.push_back('$');
        ret += c.name;
        return ret;
    }

    std::unordered_map<std::string, int> hash_cols_;

    virtual ~AbstractExecutor() = default;

    virtual size_t tupleLen() const { return 0; };

    virtual const std::vector<ColMeta> &cols() const {
        return _cols;
    };

    virtual std::string getType() { return "AbstractExecutor"; };

    virtual void beginTuple(){};

    virtual void nextTuple(){};

    virtual bool is_end() const { return true; };

    virtual Rid &rid() = 0;

    virtual std::unique_ptr<RmRecord> Next() = 0;

    virtual ColMeta get_col_offset(const TabCol &target) { return ColMeta();};

    void init_hash_cols(){
        auto& cols_ = cols();
        for(auto iter = cols_.begin(); iter!= cols_.end(); iter++){
            hash_cols_[get_hash_key(*iter)] = iter - cols_.begin();
        }
    }

    // std::vector<ColMeta>::const_iterator get_col(const std::vector<ColMeta> &rec_cols, const TabCol &target) {
    //     auto pos = std::find_if(rec_cols.begin(), rec_cols.end(), [&](const ColMeta &col) {
    //         return col.tab_name == target.tab_name && col.name == target.col_name;
    //     });
    //     if (pos == rec_cols.end()) {
    //         throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
    //     }
    //     return pos;
    // }

    std::vector<ColMeta>::const_iterator get_col(const TabCol &target) {
        auto it = hash_cols_.find(get_hash_key(target));
        if(it == hash_cols_.end()){
            throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
        }
        return cols().begin() + it->second;
    }

    bool eval_cond(AbstractExecutor* prev_, const Condition &cond, std::unique_ptr<RmRecord>& rec) {
        auto lhs_col = prev_->get_col(cond.lhs_col);
        char *lhs = rec->data + lhs_col->offset;
        char *rhs;
        ColType rhs_type;
        if (cond.is_rhs_val) {
            rhs_type = cond.rhs_val.type;
            rhs = cond.rhs_val.raw->data;
        } else {
            auto rhs_col = prev_->get_col(cond.rhs_col);
            rhs_type = rhs_col->type;
            rhs = rec->data + rhs_col->offset;
        }
        assert(rhs_type == lhs_col->type);
        if(cond.is_rhs_val && rhs_type== TYPE_DATETIME){
            if(!SmManager::check_datetime_(cond.rhs_val.datetime_val)){
                throw InvalidValueError(cond.rhs_val.datetime_val);
            }
        }
        int cmp = ix_compare(lhs, rhs, rhs_type, lhs_col->len);
        if (cond.op == OP_EQ) {
            return cmp == 0;
        } else if (cond.op == OP_NE) {
            return cmp != 0;
        } else if (cond.op == OP_LT) {
            return cmp < 0;
        } else if (cond.op == OP_GT) {
            return cmp > 0;
        } else if (cond.op == OP_LE) {
            return cmp <= 0;
        } else if (cond.op == OP_GE) {
            return cmp >= 0;
        } else {
            throw InternalError("Unexpected op type");
        }
    }

    bool eval_conds(AbstractExecutor* prev_, const std::vector<Condition> &conds, std::unique_ptr<RmRecord>& rec) {
        for(auto &cond: conds) {
            if(!eval_cond(prev_, cond, rec)) {
                return false;
            }
        }
        return true;
    }
};

struct char_array_guard{
    char* c = nullptr;
    char_array_guard(char* c_){
        c = c_;
    }
    ~char_array_guard(){
        if(c!=nullptr){
            delete[] c;
            c = nullptr;
        }
    }
};