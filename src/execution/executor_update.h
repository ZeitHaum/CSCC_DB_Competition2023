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
#include <vector>
#include <map>

class UpdateExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;
    std::vector<Condition> conds_;
    RmFileHandle *fh_;
    std::vector<Rid> rids_;
    std::string tab_name_;
    std::vector<SetClause> set_clauses_;
    SmManager *sm_manager_;

   public:
    UpdateExecutor(SmManager *sm_manager, const std::string &tab_name, const std::vector<SetClause>& set_clauses, const std::vector<Condition>& conds, const std::vector<Rid>& rids, Context *context)
        :tab_(sm_manager->db_.get_table(tab_name)),
        conds_(conds), 
        fh_(sm_manager->fhs_.at(tab_name).get()),
        rids_(rids), 
        tab_name_(tab_name), 
        set_clauses_(set_clauses),  
        sm_manager_(sm_manager)
    {
        context_ = context; 
    }


    std::unique_ptr<RmRecord> Next() override {

        bool add_lock = true;

        // 如果只update 1项, 只加意向互斥锁
        if(rids_.size() == 1) {
            context_->lock_mgr_->lock_IX_on_table(context_->txn_, fh_->GetFd());
        }
        else if(rids_.size() > 1) {
            //如果是范围更新，加互斥锁
            context_->lock_mgr_->lock_exclusive_on_table(context_->txn_, fh_->GetFd());
            add_lock = false;
        }
        //如果没有rid要更新，不加锁

        for (auto &set_clause : set_clauses_) {
            auto lhs_col = tab_.get_col(set_clause.lhs.col_name);
            if(lhs_col->type==TYPE_BIGINT && set_clause.rhs.type==TYPE_INT) {
                set_clause.rhs.type = TYPE_BIGINT;
                set_clause.rhs.set_bigint((long long)set_clause.rhs.int_val);
            }
            // set_clause.rhs.init_raw(lhs_col->len);
        }
        std::vector<RmRecord> update_records;

        //把更新后的records全部拿到
        for (auto &rid : rids_) {
            auto rec = fh_->get_record(rid, add_lock, context_);
            record_unpin_guard rug({fh_->GetFd(), rid.page_no}, false, sm_manager_->buffer_pool_manager_);
            // TODO: Remove old entry from index

            RmRecord update_record{rec->size};
            memcpy(update_record.data, rec->data, rec->size);

            //修改记录的值
            for (auto &set_clause : set_clauses_) {
                auto lhs_col = tab_.get_col(set_clause.lhs.col_name);
                if(lhs_col->type== TYPE_DATETIME){
                    set_clause.rhs.datetime_val = set_clause.rhs.str_val;
                    if(!SmManager::check_datetime_(set_clause.rhs.datetime_val)){
                        throw InvalidValueError(set_clause.rhs.datetime_val);
                    }
                    else{
                        set_clause.rhs.type = TYPE_DATETIME;
                    }
                }

                if(lhs_col->type != set_clause.rhs.type) {
                    throw IncompatibleTypeError(coltype2str(lhs_col->type), coltype2str(set_clause.rhs.type));
                }
                update_rhs_val(update_record, lhs_col, set_clause.rhs, set_clause.set_op);
            }
            update_records.emplace_back(update_record);

        }

        //找到冲突的Indexes
        std::vector<IndexMeta> ix_metas;
        for(auto& ix_meta: tab_.indexes) {
            bool index=false;
            for(auto &set_clause : set_clauses_) {
                for(auto& col_: ix_meta.cols) {
                    if(set_clause.lhs.col_name == col_.name && !index) {
                        ix_metas.emplace_back(ix_meta);
                        index = true;
                        break;
                    }
                }
                if(index) {
                    break;
                }
            }
        }


        //检查是否违背唯一索引
        std::map<std::string, bool> vis;
        for (int i = 0;i<(int)update_records.size(); i++) {
            bool is_failed = false;
            for(size_t ij = 0; ij<ix_metas.size();ij++){
                auto& index = ix_metas[ij];
                auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                #ifndef ENABLE_LOCK_CRABBING
                    std::lock_guard<std::mutex> lg(ih->root_latch_);
                #endif
                char* key = new char[index.col_tot_len];
                int offset = 0;
                for(size_t j = 0; j < (size_t)index.col_num; ++j) {
                    memcpy(key + offset, update_records[i].data + index.cols[j].offset, index.cols[j].len);
                    offset += index.cols[j].len;
                }
                std::string tmp = std::string(key, offset);
                is_failed |= (ih->is_key_exists(key,  context_->txn_) || vis.count(tmp)!=0);
                vis[tmp] = true;
                delete[] key;
            }
            if(is_failed) {
                throw InternalError("Error: update duplicated indexes.");
            }
        }


        //更新Record和Indexes
        for (int i = 0;i<(int)rids_.size();i++) {
            auto rec = fh_->get_record(rids_[i], add_lock, context_);
            record_unpin_guard rug({fh_->GetFd(), rids_[i].page_no}, true, sm_manager_->buffer_pool_manager_);
            // 更新到记录文件中
            fh_->update_record(rids_[i], update_records[i].data, add_lock, context_);

            // bool is_failed = false;
            for(size_t ij = 0; ij<ix_metas.size();ij++){
                auto& index = ix_metas[ij];
                auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                #ifndef ENABLE_LOCK_CRABBING
                    std::lock_guard<std::mutex> lg(ih->root_latch_);
                #endif
                char* key = new char[index.col_tot_len];
                char* old_key = new char[index.col_tot_len];
                int offset = 0;
                for(size_t j = 0; j < (size_t)index.col_num; ++j) {
                    memcpy(key + offset, update_records[i].data + index.cols[j].offset, index.cols[j].len);
                    memcpy(old_key + offset, rec->data + index.cols[j].offset, index.cols[j].len);
                    offset += index.cols[j].len;
                }
                
                ih->delete_entry(old_key, rids_[i], context_->txn_);
                ih->insert_entry(key, rids_[i], context_->txn_);
                delete[] key;
                delete[] old_key;
            }
        
        }
        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }

    //根据op更新rhs.val
    void update_rhs_val(RmRecord& update_record, std::vector<ColMeta>::iterator& lhs_col, Value rhs,const SetOp& set_op){
        if(set_op != SetOp::OP_ASSIGN){
            char* des = update_record.data + lhs_col->offset;
            assert(lhs_col->type == rhs.type);
            if(lhs_col->type == ColType::TYPE_INT){
                if(set_op == SetOp::OP_PLUS){
                    rhs.set_int(*(int*) des + rhs.int_val);
                }
                else{
                    rhs.set_int(*(int*) des - rhs.int_val);
                }
            }
            else if(lhs_col->type == ColType::TYPE_FLOAT){
                if(set_op == SetOp::OP_PLUS){
                    rhs.set_float(*(float*)des + rhs.float_val);
                }
                else{
                    rhs.set_float(*(float*)des - rhs.float_val);
                }                
            }
            else if(lhs_col->type == ColType::TYPE_BIGINT){
                if(set_op == SetOp::OP_PLUS){
                    rhs.set_bigint(*(long long*)des + rhs.bigint_val);
                }
                else{
                    rhs.set_bigint(*(long long*)des - rhs.bigint_val);
                }
            }
            else{
                //datetime 和string 不允许加减
                assert(0);
            }
        }
        if(rhs.raw==nullptr){//第一遍历初始化rhs.raw
            rhs.init_raw(lhs_col->len); //初始化set_clause的raw
        }
        memcpy(update_record.data + lhs_col->offset, rhs.raw->data, lhs_col->len);
    }
};