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

class DeleteExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;                   // 表的元数据
    std::vector<Condition> conds_;  // delete的条件
    RmFileHandle *fh_;              // 表的数据文件句柄
    std::vector<Rid> rids_;         // 需要删除的记录的位置
    std::string tab_name_;          // 表名称
    SmManager *sm_manager_;

   public:
    DeleteExecutor(SmManager *sm_manager, const std::string &tab_name, const std::vector<Condition>& conds,
                   const std::vector<Rid> &rids, Context *context) : tab_name_(tab_name){
        sm_manager_ = sm_manager;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }

    std::unique_ptr<RmRecord> Next() override {

        bool add_lock = true;

        // 如果只删除 1项, 只加意向互斥锁
        if(rids_.size() == 1) {
            context_->lock_mgr_->lock_IX_on_table(context_->txn_, fh_->GetFd());
        }
        else if(rids_.size() > 1) {
            //如果是范围删除，加互斥锁
            context_->lock_mgr_->lock_exclusive_on_table(context_->txn_, fh_->GetFd());
            add_lock = false;
        }
        //如果没有rid要删除，不加锁

        //删除所有rid
        for(Rid& rid :this->rids_){
            //获取Record
            std::unique_ptr<RmRecord> record = fh_->get_record(rid, add_lock, this->context_);
            record_unpin_guard rug({fh_->GetFd(), rid.page_no}, true, sm_manager_->buffer_pool_manager_);
            //删除record.
            fh_->delete_record(rid, add_lock, this->context_);

            for(size_t i = 0; i<tab_.indexes.size();i++){
                auto& index = tab_.indexes[i];
                //索引加锁
                auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                #ifndef ENABLE_LOCK_CRABBING
                    std::lock_guard<std::mutex> lg(ih->root_latch_);
                #endif
                char* key = new char[index.col_tot_len];
                int offset = 0;
                for(size_t i = 0; i < (size_t)index.col_num; ++i) {
                    memcpy(key + offset, record->data + index.cols[i].offset, index.cols[i].len);
                    offset += index.cols[i].len;
                }
                ih->delete_entry(key, rid, context_->txn_);
                delete[] key;
            }

        }

        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }
};