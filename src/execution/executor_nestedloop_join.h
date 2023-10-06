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

class NestedLoopJoinExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> outer_;    // 左儿子节点（需要join的表)）
    std::unique_ptr<AbstractExecutor> inner_;   // 右儿子节点（需要join的表）
    size_t len_;                                // join后获得的每条记录的长度
    std::vector<ColMeta> cols_;                 // join后获得的记录的字段
    std::unique_ptr<RmRecord> merged_rec; // 合并后的rec

    std::vector<Condition> fed_conds_;          // join条件
    // bool isend; // unused, use isend() function.

    std::map<TabCol, Value> prev_feed_dict_;

   public:
    NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right, std::vector<Condition> conds): 
    outer_(std::move(left)), inner_(std::move(right)) 
    {
        len_ = outer_->tupleLen() + inner_->tupleLen();
        cols_ = outer_->cols();
        auto inner_cols = inner_->cols();
        for (auto &col : inner_cols) {
            col.offset += outer_->tupleLen();
        }

        cols_.insert(cols_.end(), inner_cols.begin(), inner_cols.end());
        // isend = false;
        fed_conds_ = std::move(conds);
        init_hash_cols();
    }

    void beginTuple() override {
        outer_ -> beginTuple(); 
        while(true){
            bool break_enable = is_end();
            for(inner_->beginTuple(); !inner_->is_end() && !break_enable;inner_->nextTuple()) {
                std::unique_ptr<RmRecord> outer_rec = outer_->Next();
                std::unique_ptr<RmRecord> inner_rec = inner_->Next();
                this->merged_rec = std::move(merge_record(outer_rec, inner_rec));
                if(eval_conds(this, fed_conds_, merged_rec)){
                    break_enable = true;
                    break;
                }
            }
            if(break_enable){
                break;
            }
            outer_ -> nextTuple();
            inner_ -> beginTuple();
        }
    }

    void nextTuple() override {
        while(!is_end()) {
            //move
            inner_->nextTuple();
            if(inner_ -> is_end()){
                outer_ -> nextTuple();
                if(is_end()){
                    break;
                }
                inner_->beginTuple();
            }
            std::unique_ptr<RmRecord> outer_rec = outer_->Next();
            std::unique_ptr<RmRecord> inner_rec = inner_->Next();
            this->merged_rec = std::move(merge_record(outer_rec, inner_rec));
            if(eval_conds(this, fed_conds_, merged_rec)){
                break;
            }
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        return std::move(this->merged_rec);
    }

    Rid &rid() override { return _abstract_rid; }

    //合并rec
    std::unique_ptr<RmRecord> merge_record(std::unique_ptr<RmRecord>& outer_record, std::unique_ptr<RmRecord>& inner_record){
        std::unique_ptr<RmRecord> ret = std::make_unique<RmRecord>(this->len_);
        //赋值
        memcpy(ret->data, outer_record->data, outer_record->size);
        memcpy(ret->data + outer_record->size, inner_record->data, inner_record->size);
        return ret;
    }

    const std::vector<ColMeta> &cols() const {
        return this->cols_;
    };

    bool is_end() const { return outer_->is_end();};

    size_t tupleLen() const { 
        return this->len_; 
    }

    std::string getType() { return "NestedJoinExecutor"; }

    ColMeta get_col_offset(const TabCol &target) override{ 
        for(auto iter = this->cols_.begin(); iter!=this->cols_.end(); iter++){
            if(iter->tab_name == target.tab_name && iter->name == target.col_name){
                return *iter;
            }
        }
        return ColMeta();
    };
};