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

// #define DEBUG_HASH_JOIN

class HashJoinExecutor : public AbstractExecutor {
    private:
    
    //重载abstractExecutor的函数
    std::unique_ptr<AbstractExecutor> hash_;    // 左儿子节点（需要join的表)）
    std::unique_ptr<AbstractExecutor> unhash_;   // 右儿子节点（需要join的表）
    size_t len_;                                // join后获得的每条记录的长度
    std::unique_ptr<RmRecord> merged_rec; // 合并后的rec
    std::vector<ColMeta> cols_;                 // join后获得的记录的字段
    const std::vector<Condition>& fed_conds_;          // join条件

    std::unordered_map<Value, std::vector<std::unique_ptr<RmRecord>>, ValueHash> hash_table;
    TabCol hash_col_;
    TabCol unhash_col_;
    int now_join_ptr = -1;
    Value now_value;
    ColMeta hash_col_meta;
    ColMeta unhash_col_meta;
    std::unique_ptr<RmRecord> now_unhash_rec;
    

    public:
    HashJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right, const std::vector<Condition>& conds, const TabCol& hash_col, const TabCol& unhash_col)
        :fed_conds_(conds),
        hash_col_(hash_col),
        unhash_col_(unhash_col)
    {
        //决定hash和unhash
        bool is_left_hash = false;
        for(auto iter = left->cols().begin(); iter!=left->cols().end(); ++iter){
            if(iter->tab_name == hash_col_.tab_name && iter->name == hash_col_.col_name){
                is_left_hash = true;
                break;
            }
        }
        if(is_left_hash){
            hash_ = std::move(left);
            unhash_ = std::move(right);
        }
        else{
            hash_ = std::move(right);
            unhash_ = std::move(left);
        }

        len_ = hash_->tupleLen() + unhash_->tupleLen();
        cols_ = hash_->cols();
        auto unhash_cols = unhash_->cols();
        #ifdef DEBUG_HASH_JOIN
            std::string hash_type = hash_ ->getType();
            std::string unhash_type = unhash_ -> getType();
        #endif 
        hash_col_meta = hash_->get_col_offset(hash_col_);
        unhash_col_meta = unhash_->get_col_offset(unhash_col_);
        for (auto &col : unhash_cols) {
            col.offset += hash_->tupleLen();
        }
        cols_.insert(cols_.end(), unhash_cols.begin(), unhash_cols.end());
        hash_table.clear();
        init_hash_table();
        init_hash_cols();
    }

    //合并rec
    std::unique_ptr<RmRecord> merge_record(std::unique_ptr<RmRecord>& hash_record, std::unique_ptr<RmRecord>& unhash_record){
        std::unique_ptr<RmRecord> ret = std::make_unique<RmRecord>(this->len_);
        //赋值
        memcpy(ret->data, hash_record->data, hash_record->size);
        memcpy(ret->data + hash_record->size, unhash_record->data, unhash_record->size);
        return ret;
    }

    bool is_end() const { 
        return unhash_->is_end();
    }

    void init_hash_table(){
        if(hash_col_meta.type == ColType::TYPE_UNUSE){
            //hash_table为空
            return;
        }
        for(hash_->beginTuple(); !hash_->is_end(); hash_->nextTuple()){
            std::unique_ptr<RmRecord> rec = hash_->Next();
            Value v;
            v.type = hash_col_meta.type;
            v.get_val_from_raw(rec->data + hash_col_meta.offset, hash_col_meta.len);
            hash_table[v].emplace_back(std::move(rec));
        }
    }

    void beginTuple() override {
        unhash_->beginTuple();
        while(!unhash_->is_end()){
            now_unhash_rec = unhash_->Next();
            Value v;
            v.type = unhash_col_meta.type;
            v.get_val_from_raw(now_unhash_rec->data + unhash_col_meta.offset, unhash_col_meta.len);
            if(hash_table.count(v)!=0){
                for(auto sub_it = hash_table[v].begin(); sub_it!= hash_table[v].end(); sub_it++){
                    merged_rec = merge_record(*sub_it, now_unhash_rec);
                    if(eval_conds(this, fed_conds_, merged_rec)){
                        now_join_ptr =  sub_it - hash_table[v].begin();
                        now_value = v;
                        return;
                    }
                }
            }
            unhash_->nextTuple();
        }
    }

    void nextTuple() override {
        ++now_join_ptr;
        for(auto now_it = hash_table[now_value].begin()+now_join_ptr;now_it!= hash_table[now_value].end(); now_it++){
            merged_rec = merge_record(*now_it, now_unhash_rec);
            if(eval_conds(this, fed_conds_, merged_rec)){
                now_join_ptr =  now_it - hash_table[now_value].begin();
                return;
            }
        }
        unhash_->nextTuple();
        while(!unhash_->is_end()){
            now_unhash_rec = unhash_->Next();
            Value v;
            v.type = unhash_col_meta.type;
            v.get_val_from_raw(now_unhash_rec->data + unhash_col_meta.offset, unhash_col_meta.len);
            if(hash_table.count(v)!=0){
                for(auto sub_it = hash_table[v].begin(); sub_it!= hash_table[v].end(); sub_it++){
                    merged_rec = merge_record(*sub_it, now_unhash_rec);
                    if(eval_conds(this, fed_conds_, merged_rec)){
                        now_join_ptr =  sub_it - hash_table[v].begin();
                        now_value = v;
                        return;
                    }
                }
            }
            unhash_->nextTuple();
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        return std::move(merged_rec);
    }


    Rid &rid() override { return _abstract_rid; }
    
    const std::vector<ColMeta> &cols() const {
        return this->cols_;
    };

    size_t tupleLen() const { 
        return this->len_; 
    }

    std::string getType() { return "HashJoinExecutor"; }

    ColMeta get_col_offset(const TabCol &target) override{ 
        for(auto iter = this->cols_.begin(); iter!=this->cols_.end(); ++iter){
            if(iter->tab_name == target.tab_name && iter->name == target.col_name){
                return *iter;
            }
        }
        return ColMeta();
    };



};
