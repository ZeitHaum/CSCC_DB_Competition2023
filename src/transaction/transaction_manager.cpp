/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */
 
#include "transaction_manager.h"
#include "record/rm_file_handle.h"
#include "system/sm_manager.h"

std::unordered_map<txn_id_t, Transaction *> TransactionManager::txn_map = {};

/**
 * @description: 事务的开始方法
 * @return {Transaction*} 开始事务的指针
 * @param {Transaction*} txn 事务指针，空指针代表需要创建新事务，否则开始已有事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
Transaction * TransactionManager::begin(Transaction* txn, LogManager* log_manager) {
    // Todo:
    // 1. 判断传入事务参数是否为空指针
    // 2. 如果为空指针，创建新事务
    // 3. 把开始事务加入到全局事务表中
    // 4. 返回当前事务指针
    std::lock_guard<std::mutex>lg(this->latch_);

    Transaction* new_txn = nullptr;
    //1.
    if(txn == nullptr){
        //2.
        new_txn = new Transaction(this->get_next_txn_id());
        new_txn->set_state(TransactionState::GROWING);
        //3.
        assert(TransactionManager::txn_map.count(new_txn->get_transaction_id())==0);
        TransactionManager::txn_map[new_txn->get_transaction_id()] = new_txn;
    }
    else{
        //已有事务默认为txn
        new_txn = txn;
    }
    //4.
    log_manager->add_begin_log_record(new_txn->get_transaction_id());
    return new_txn;
}

/**
 * @description: 事务的提交方法
 * @param {Transaction*} txn 需要提交的事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
void TransactionManager::commit(Transaction* txn, LogManager* log_manager) {
    // Todo:
    // 1. 如果存在未提交的写操作，提交所有的写操作
    // 2. 释放所有锁
    // 3. 释放事务相关资源，eg.锁集
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态
    std::lock_guard<std::mutex>lg(this->latch_);
 
    //1. 
    //TODO
    //可串行化不需要.

    //2. 
    txn->set_state(TransactionState::SHRINKING);
    //释放所有txn加的锁
    std::shared_ptr<std::unordered_set<LockDataId>> lock_set_ = txn->get_lock_set();
    for(auto iter = lock_set_->begin(); iter!= lock_set_->end(); iter++){
        this->lock_manager_->unlock(txn, *iter);
    }

    //3.
    txn->clear_write_set();
    lock_set_->clear();

    //4.
    // 暂时不管，落盘只在:1. Buffer满了， 2. 超时了， 3. 不安全 时发生
    log_manager->add_commit_log_record(txn->get_transaction_id());
    log_manager->flush_log_to_disk();

    //5.
    txn->set_state(TransactionState::COMMITTED);

}

/**
 * @description: 事务的终止（回滚）方法
 * @param {Transaction *} txn 需要回滚的事务
 * @param {LogManager} *log_manager 日志管理器指针
 */
void TransactionManager::abort(Transaction * txn, LogManager *log_manager) {
    // Todo:
    // 1. 回滚所有写操作
    // 2. 释放所有锁
    // 3. 清空事务相关资源，eg.锁集
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态
    //1. 
    for(auto iter = txn->get_write_set()->rbegin(); iter!=txn->get_write_set()->rend(); iter++){
        this->rollback_writeRecord(*iter, txn, log_manager);
    }

    //2
    txn->set_state(TransactionState::SHRINKING);
    //释放所有txn加的锁
    std::shared_ptr<std::unordered_set<LockDataId>> lock_set_ = txn->get_lock_set();
    for(auto iter = lock_set_->begin(); iter!= lock_set_->end(); iter++){
        this->lock_manager_->unlock(txn, *iter);
    }
    
    //3
    txn->clear_write_set();
    lock_set_->clear();

    //4.
    //暂时不管，落盘只在:1. Buffer满了， 2. 超时了， 3. 不安全 时发生
    log_manager->add_abort_log_record(txn->get_transaction_id());
    log_manager->flush_log_to_disk();

    //5.
    txn->set_state(TransactionState::ABORTED);
}


/*
    回滚WriteRecord.
*/
void TransactionManager::rollback_writeRecord(WriteRecord* to_rol, Transaction * txn, LogManager *log_manager){
    //根据类型不同调用对应的反函数
    RmFileHandle* fh_  = sm_manager_ -> fhs_[to_rol->GetTableName()].get(); //能否使用get保持互斥性?
    TabMeta& tab_meta_ = sm_manager_ -> get_table_meta(to_rol->GetTableName());

    if(to_rol->GetWriteType() == WType::INSERT_TUPLE){
        //修改日志
        log_manager->add_delete_log_record(txn->get_transaction_id(), to_rol->GetRecord(), to_rol->GetRid(), to_rol->GetTableName());

        //调用delete
        fh_ -> delete_record(to_rol->GetRid(), nullptr);

        // set_page_lsn(to_rol->GetRid().page_no, now_lsn);

        for(auto iter = tab_meta_.indexes.begin(); iter!= tab_meta_.indexes.end(); iter++){
            std::string ix_file_name = sm_manager_ -> get_ix_manager() -> get_index_name(to_rol->GetTableName(), iter->cols);
            IxIndexHandle* ih_ = sm_manager_->ihs_.at(ix_file_name).get();
            #ifndef ENABLE_LOCK_CRABBING
                std::lock_guard<std::mutex> lg (ih_->root_latch_);
            #endif
            auto& index = *iter;
            char* key = new char[index.col_tot_len];
            int offset = 0;
            for(size_t i = 0; i < index.col_num; ++i) {
                memcpy(key + offset, to_rol->GetRecord().data + index.cols[i].offset, index.cols[i].len);
                offset += index.cols[i].len;
            }
            
            ih_ ->delete_entry(key, to_rol->GetRid(), txn);
            delete[] key;
        }
    }
    else if(to_rol->GetWriteType() == WType::DELETE_TUPLE){
        //修改日志
        log_manager->add_insert_log_record(txn->get_transaction_id(), to_rol->GetRecord(), to_rol->GetRid(), to_rol->GetTableName());
        //调用insert
        fh_ -> insert_record(to_rol->GetRid(),to_rol->GetRecord().data);
        // set_page_lsn(to_rol->GetRid().page_no, now_lsn);
        for(auto iter = tab_meta_.indexes.begin(); iter!= tab_meta_.indexes.end(); iter++){
            std::string ix_file_name = sm_manager_ -> get_ix_manager() -> get_index_name(to_rol->GetTableName(), iter->cols);
            IxIndexHandle* ih_ = sm_manager_->ihs_.at(ix_file_name).get();
            #ifndef ENABLE_LOCK_CRABBING
                std::lock_guard<std::mutex> lg (ih_->root_latch_);
            #endif
            auto& index = *iter;
            char* key = new char[index.col_tot_len];
            int offset = 0;
            for(size_t i = 0; i < index.col_num; ++i) {
                memcpy(key + offset, to_rol->GetRecord().data + index.cols[i].offset, index.cols[i].len);
                offset += index.cols[i].len;
            }
            try{
                ih_ ->insert_entry(key, to_rol->GetRid(),txn);
            }
            catch(IndexInsertDuplicatedError& e){
                delete[] key;
                throw e;
            }
            delete[] key;
        }
    }
    else if(to_rol->GetWriteType() == WType::UPDATE_TUPLE){
        auto old_rec = fh_->get_record(to_rol->GetRid(), nullptr);
        record_unpin_guard rug({fh_->GetFd(), to_rol->GetRid().page_no}, true, sm_manager_->buffer_pool_manager_);
        log_manager->add_update_log_record(txn->get_transaction_id(), to_rol->GetRecord(), *(old_rec.get()), to_rol->GetRid(), to_rol->GetTableName());
        //修改日志
        //调用update
        fh_ -> update_record(to_rol->GetRid(), to_rol->GetRecord().data,nullptr);
        // set_page_lsn(to_rol->GetRid().page_no, now_lsn);
        for(auto iter = tab_meta_.indexes.begin(); iter!= tab_meta_.indexes.end(); iter++){
            std::string ix_file_name = sm_manager_ -> get_ix_manager() -> get_index_name(to_rol->GetTableName(), iter->cols);
            IxIndexHandle* ih_ = sm_manager_->ihs_.at(ix_file_name).get();
            #ifndef ENABLE_LOCK_CRABBING
                std::lock_guard<std::mutex> lg (ih_->root_latch_);
            #endif
            auto& index = *iter;
            char* key = new char[index.col_tot_len];
            char* old_key = new char[index.col_tot_len];
            int offset = 0;
            for(size_t i = 0; i < index.col_num; ++i) {
                memcpy(key + offset, to_rol->GetRecord().data + index.cols[i].offset, index.cols[i].len);
                memcpy(old_key + offset, old_rec->data + index.cols[i].offset, index.cols[i].len);
                offset += index.cols[i].len;
            }
            ih_ ->delete_entry(old_key,to_rol->GetRid(), txn);
            try{
                ih_ ->insert_entry(key, to_rol->GetRid(), txn);
            }
            catch(IndexInsertDuplicatedError& e){
                delete[] key;
                delete[] old_key;
                throw e;
            }
            delete[] key;
            delete[] old_key;
        }
    }
    else{
        assert(0);
    }
}