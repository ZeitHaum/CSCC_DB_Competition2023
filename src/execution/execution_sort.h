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
// #define DEBUG_SORT_EXCUTOR

class SortExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;
    std::vector<ColMeta> cols_;                            

    size_t tuple_num;
    std::vector<bool> is_descs_;
    size_t limit_cnt_;
    // std::vector<size_t> used_tuple;
    // std::unique_ptr<RmRecord> current_tuple;
    std::vector<std::unique_ptr<RmRecord>> sorted_tuples;
    size_t now_ptr;

   public:
    SortExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<TabCol>& order_cols, const std::vector<bool>& is_descs, int limit_cnt) 
        :prev_(std::move(prev)),
        tuple_num(0),
        is_descs_(is_descs),  
        limit_cnt_(limit_cnt), 
        now_ptr(0)
    {
        for(auto iter = order_cols.begin(); iter!= order_cols.end(); iter++){
            cols_.emplace_back(prev_->get_col_offset(*iter));
        }
        init_hash_cols();
    }

    void beginTuple() override { 
        //预先把所有Tuple取到, 然后排序
        #ifdef DEBUG_SORT_EXCUTOR
            std::string prev__name = prev_->getType();
        #endif
        for(prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple()){
            sorted_tuples.emplace_back(std::move(prev_->Next()));
        }
        //TODO: 排序
        now_ptr = 0;
        std::stable_sort(sorted_tuples.begin(), sorted_tuples.end(), [this](const std::unique_ptr<RmRecord>& r1, const std::unique_ptr<RmRecord>& r2){
            //lambda Expression
            bool is_r1_prev = false;// 稳定排序
            //遍历字段
            for(auto iter = this->cols_.begin(); iter!=this->cols_.end();++iter){
                char* r1_ptr = r1->data + iter->offset;
                char* r2_ptr = r2->data + iter->offset;
                int cmp_r1_r2 = ix_compare(r1_ptr, r2_ptr, iter->type, iter->len);
                //若is_desc为true， p1要放后面
                if(cmp_r1_r2<0){
                    is_r1_prev = (this-> is_descs_[iter - this->cols_.begin()]) ^ (true);
                    break;
                }
                else if(cmp_r1_r2>0){
                    is_r1_prev = (this-> is_descs_[iter - this->cols_.begin()]) ^ (false);
                    break;
                }
                else{
                    //相等什么也不做
                }
            }
            return is_r1_prev;
        });
    }

    void nextTuple() override {
        now_ptr++;
    }

    std::unique_ptr<RmRecord> Next() override {
        return std::move(sorted_tuples.at(now_ptr));
    }

    bool is_end() const { 
        return now_ptr >= sorted_tuples.size() || (limit_cnt_>=0 && now_ptr >= limit_cnt_);
    }

    //重载abstract的函数
    const std::vector<ColMeta> &cols() const {
        return prev_->cols();
    }

    size_t tupleLen() const { return prev_->tupleLen(); }

    std::string getType() { return "SortExecutor"; }

    Rid &rid() override { return _abstract_rid; }
};