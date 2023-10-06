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
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class AggregationExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;        // 聚集函数的投影儿子节点
    std::vector<ColMeta> cols_;                     // 需要计算的字段
    std::vector<ColMeta> ans_;                      // 输出的字段
    std::vector<AgreValue> agre_values_;            // 计算的结果
    size_t len_;                                    // 聚集后结果的字段总长度
    int all_count = 0;
    
    bool end_flag = false;

   public:
    AggregationExecutor(std::unique_ptr<AbstractExecutor> &prev, const std::vector<AgreType> &agre_types, const std::vector<TabCol> &agre_cols, const std::vector<TabCol> &target_cols) 
    : prev_(std::move(prev))
    {
        size_t ans_offset = 0;
        auto &prev_cols = prev_->cols();
        agre_values_.resize(agre_cols.size());
        for (size_t i = 0; i < agre_cols.size(); ++i) {

            if(agre_types[i] != AGRE_TYPE_COUNT_ALL) {
                cols_.emplace_back(get_col_meta(prev_cols, target_cols[i]));
            }
            else {
                ColMeta tmp;
                cols_.emplace_back(tmp);
            }

            ColMeta ans_col_;
            if(agre_types[i] == AGRE_TYPE_COUNT || agre_types[i] == AGRE_TYPE_COUNT_ALL) {
                ans_col_.type = TYPE_INT;
                ans_col_.len = sizeof(int);
            }
            else {
                ans_col_.type = cols_[i].type;
                ans_col_.len = cols_[i].len;
                
            }
            ans_col_.offset = ans_offset;
            ans_offset += ans_col_.len;
            ans_.emplace_back(ans_col_);
        }
        len_ = ans_offset;
        for(size_t i = 0; i < agre_cols.size(); ++i) {
            if(agre_types[i] != AGRE_TYPE_COUNT_ALL) {
                agre_values_[i].init_agre_raw(cols_[i].type, agre_types[i], cols_[i].len);
            }
            else {
                agre_values_[i].init_agre_raw(TYPE_INT, agre_types[i], 4);
            }
        }
        init_hash_cols();
    }

    void beginTuple() override {
        for(prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple()) {
            auto Tuple = prev_->Next();
            std::string str(Tuple->data, Tuple->size);
            ++all_count;
            for(size_t i = 0; i < cols_.size(); ++i) {
                char *value_buf = Tuple->data + cols_[i].offset;
                if(agre_values_[i].agre_type != AgreType::AGRE_TYPE_COUNT_ALL && agre_values_[i].agre_type != AgreType::AGRE_TYPE_COUNT) {
                    agre_values_[i].add_value(value_buf, cols_[i].len);
                }
            }
        }
    }

    void nextTuple() override {
        if(!end_flag) {
            end_flag = true;
        }
    }

    bool is_end() const override { 
        return end_flag;
    }


    std::unique_ptr<RmRecord> Next() override {
        std::unique_ptr<RmRecord> ret = std::make_unique<RmRecord>(this->len_);
        int curr_offset = 0;
        for(size_t i = 0; i < ans_.size(); ++i) {
            if(agre_values_[i].agre_type == AgreType::AGRE_TYPE_COUNT_ALL || agre_values_[i].agre_type == AgreType::AGRE_TYPE_COUNT) {
                char* agre_rec = new char[ans_[i].len];
                *(int *)agre_rec = all_count;
                memcpy(ret->data + curr_offset, agre_rec, ans_[i].len);
                delete[] agre_rec;
            }
            else {
                auto agre_rec = agre_values_[i].get_value();
                memcpy(ret->data + curr_offset, agre_rec->data, ans_[i].len);
            }
            curr_offset += ans_[i].len;
        }
        return ret;
    }

    Rid &rid() override { return _abstract_rid; }

    const std::vector<ColMeta> &cols() const {
        return this->ans_;
    };

    size_t tupleLen() const { 
        return this->len_; 
    }

    std::string getType() { return "AggreagtionExecutor"; };

    ColMeta get_col_meta(const std::vector<ColMeta>& col_metas, const TabCol &target) {
        ColMeta ret;
        ret.tab_name = target.tab_name;
        ret.name = target.col_name;
        for(auto &col_meta: col_metas) {
            if(target.tab_name == col_meta.tab_name && target.col_name == col_meta.name) {
                ret.offset = col_meta.offset;
                ret.len = col_meta.len;
                ret.type = col_meta.type;
                break;
            }
        }
        return ret;
    }

    ColMeta get_col_offset(const TabCol &target) override{ 
        for(auto iter = this->ans_.begin(); iter!=this->ans_.end(); ++iter){
            if(iter->tab_name == target.tab_name && iter->name == target.col_name){
                return *iter;
            }
        }
        return ColMeta();
    };
};
