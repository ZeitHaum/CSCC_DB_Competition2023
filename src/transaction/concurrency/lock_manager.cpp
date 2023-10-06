/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "lock_manager.h"
#include "time.h"

#define ENABLE_LOCK_MAP

/**
 * @description: 申请行级共享锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID 记录所在的表的fd
 * @param {int} tab_fd
 */
bool LockManager::lock_shared_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    // 管理锁的并发
    std::lock_guard<std::mutex>lg(latch_);
    
    if(txn->get_state() == TransactionState::SHRINKING) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }

    //获得对应的锁id
    LockDataId lock_id(tab_fd, rid, LockDataType::RECORD);
    //判断是否已经加过锁
    if(lock_table_.find(lock_id) == lock_table_.end()) {
        //新建数据项的锁申请队列
        LockRequest lock_req(txn->get_transaction_id(), LockMode::SHARED);
        #ifdef ENABLE_LOCK_MAP
            LockRequestQueue lock_rq;
            lock_table_.insert({lock_id, lock_rq});
            lock_table_.at(lock_id).request_queue_.emplace_back(lock_req);
        #endif
        //添加事务拥有的锁
        txn->append_lock_set(lock_id);
    }
    else {
        //no-wait策略
        bool append_flag = true;
        auto &lock_reqs = lock_table_[lock_id].request_queue_;

        for(auto iter = lock_reqs.begin(); iter!=lock_reqs.end(); iter++) {
            if(iter->txn_id_ != txn->get_transaction_id() && iter->lock_mode_ == LockMode::EXLUCSIVE) {
                throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
            }
        }

        for(auto iter = lock_reqs.begin(); iter!=lock_reqs.end(); iter++) {
            if(iter->txn_id_ == txn->get_transaction_id()) {
                append_flag = false;
                break;
            }
        }
        
        if(append_flag) {
            //添加数据项的锁申请队列
            LockRequest lock_req(txn->get_transaction_id(), LockMode::SHARED);
            lock_table_[lock_id].request_queue_.emplace_back(lock_req);
            //添加事务拥有的锁
            txn->append_lock_set(lock_id);
        }
    }
    return true;
}

/**
 * @description: 申请行级排他锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID
 * @param {int} tab_fd 记录所在的表的fd
 */
bool LockManager::lock_exclusive_on_record(Transaction* txn, const Rid& rid, int tab_fd) {

    std::lock_guard<std::mutex>lg(latch_);

    if(txn->get_state() == TransactionState::SHRINKING) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }

    LockDataId lock_id(tab_fd, rid, LockDataType::RECORD);

    if(lock_table_.find(lock_id) == lock_table_.end()) {

        LockRequest lock_req(txn->get_transaction_id(), LockMode::EXLUCSIVE);
        #ifdef ENABLE_LOCK_MAP
            LockRequestQueue lock_rq;
            lock_table_.insert({lock_id, lock_rq});
            lock_table_.at(lock_id).request_queue_.emplace_back(lock_req);
        #endif

        txn->append_lock_set(lock_id);
    }
    else {
        //no-wait策略
        bool append_flag = true;
        auto &lock_reqs = lock_table_[lock_id].request_queue_;

        for(auto iter = lock_reqs.begin(); iter!=lock_reqs.end(); iter++) {
            if(iter->txn_id_!=txn->get_transaction_id()) {
                throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
            }
        }

        for(auto iter = lock_reqs.begin(); iter!=lock_reqs.end(); iter++) {
            if(iter->txn_id_==txn->get_transaction_id()) {
                append_flag = false;
                iter->lock_mode_ = (iter->lock_mode_==LockMode::SHARED)? LockMode::EXLUCSIVE : iter->lock_mode_;
                break;
            }
        }

        if(append_flag) {
            LockRequest lock_req(txn->get_transaction_id(), LockMode::EXLUCSIVE);
            lock_table_[lock_id].request_queue_.emplace_back(lock_req);
            txn->append_lock_set(lock_id);
        }
    }
    return true;
}

/**
 * @description: 申请表级读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_shared_on_table(Transaction* txn, int tab_fd) {
    std::lock_guard<std::mutex>lg(latch_);

    if(txn->get_state() == TransactionState::SHRINKING) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }

    LockDataId lock_id(tab_fd, LockDataType::TABLE);

    if(lock_table_.find(lock_id) == lock_table_.end()) {

        LockRequest lock_req(txn->get_transaction_id(), LockMode::SHARED);
        #ifdef ENABLE_LOCK_MAP
            LockRequestQueue lock_rq;
            lock_table_.insert({lock_id, lock_rq});
            lock_table_.at(lock_id).request_queue_.emplace_back(lock_req);
        #endif

        txn->append_lock_set(lock_id);
    }
    else {
        //no-wait策略
        bool append_flag = true;
        auto &lock_reqs = lock_table_[lock_id].request_queue_;

        for(auto iter = lock_reqs.begin(); iter!=lock_reqs.end(); iter++) {
            if(iter->txn_id_!=txn->get_transaction_id() && (iter->lock_mode_ == LockMode::EXLUCSIVE || iter->lock_mode_ == LockMode::INTENTION_EXCLUSIVE || iter->lock_mode_ == LockMode::S_IX)) {
                throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
            }
        }

        for(auto iter = lock_reqs.begin(); iter!=lock_reqs.end(); iter++) {
            if(iter->txn_id_==txn->get_transaction_id()) {
                append_flag = false;
                iter->lock_mode_ = (iter->lock_mode_==LockMode::INTENTION_SHARED)? LockMode::SHARED : (iter->lock_mode_==LockMode::INTENTION_EXCLUSIVE)? LockMode::S_IX : iter->lock_mode_;
                break;
            }
        }

        if (append_flag) {
            LockRequest lock_req(txn->get_transaction_id(), LockMode::SHARED);
            lock_table_[lock_id].request_queue_.emplace_back(lock_req);
            txn->append_lock_set(lock_id);
        }
    }
    return true;
}

/**
 * @description: 申请表级写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_exclusive_on_table(Transaction* txn, int tab_fd) {

    std::lock_guard<std::mutex>lg(latch_);

    if(txn->get_state() == TransactionState::SHRINKING) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }

    LockDataId lock_id(tab_fd, LockDataType::TABLE);

    if(lock_table_.find(lock_id) == lock_table_.end()) {

        LockRequest lock_req(txn->get_transaction_id(), LockMode::EXLUCSIVE);
        #ifdef ENABLE_LOCK_MAP
            LockRequestQueue lock_rq;
            lock_table_.insert({lock_id, lock_rq});
            lock_table_.at(lock_id).request_queue_.emplace_back(lock_req);
        #endif

        txn->append_lock_set(lock_id);
    }
    else {
        bool append_flag = true;
        auto &lock_reqs = lock_table_[lock_id].request_queue_;

        for(auto iter = lock_reqs.begin(); iter!=lock_reqs.end(); iter++) {
            if(iter->txn_id_!=txn->get_transaction_id()) {
                throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
            }
        }

        for(auto iter = lock_reqs.begin(); iter!=lock_reqs.end(); iter++) {
            if(iter->txn_id_==txn->get_transaction_id()) {
                append_flag = false;
                iter->lock_mode_ = LockMode::EXLUCSIVE;
                break;
            }
        }

        if (append_flag) {
            LockRequest lock_req(txn->get_transaction_id(), LockMode::EXLUCSIVE);
            lock_table_[lock_id].request_queue_.emplace_back(lock_req);
            txn->append_lock_set(lock_id);
        }
    }
    return true;
}

/**
 * @description: 申请表级意向读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IS_on_table(Transaction* txn, int tab_fd) {

    std::lock_guard<std::mutex>lg(latch_);

    if(txn->get_state() == TransactionState::SHRINKING) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }

    LockDataId lock_id(tab_fd, LockDataType::TABLE);

    if(lock_table_.find(lock_id) == lock_table_.end()) {

        LockRequest lock_req(txn->get_transaction_id(), LockMode::INTENTION_SHARED);
        #ifdef ENABLE_LOCK_MAP
            LockRequestQueue lock_rq;
            lock_table_.insert({lock_id, lock_rq});
            lock_table_.at(lock_id).request_queue_.emplace_back(lock_req);
        #endif

        txn->append_lock_set(lock_id);
    }
    else {
        bool append_flag = true;
        auto &lock_reqs = lock_table_[lock_id].request_queue_;

        for(auto iter = lock_reqs.begin(); iter!=lock_reqs.end(); iter++) {
            if(iter->txn_id_!=txn->get_transaction_id() && iter->lock_mode_ == LockMode::EXLUCSIVE) {
                throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
            }
        }

        for(auto iter = lock_reqs.begin(); iter!=lock_reqs.end(); iter++) {
            if(iter->txn_id_==txn->get_transaction_id()) {
                append_flag = false;
                break;
            }
        }

        if (append_flag) {
            LockRequest lock_req(txn->get_transaction_id(), LockMode::INTENTION_SHARED);
            lock_table_[lock_id].request_queue_.emplace_back(lock_req);
            txn->append_lock_set(lock_id);
        }
    }
    return true;
}

/**
 * @description: 申请表级意向写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IX_on_table(Transaction* txn, int tab_fd) {

    std::lock_guard<std::mutex>lg(latch_);

    if(txn->get_state() == TransactionState::SHRINKING) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }

    LockDataId lock_id(tab_fd, LockDataType::TABLE);

    if(lock_table_.find(lock_id) == lock_table_.end()) {

        LockRequest lock_req(txn->get_transaction_id(), LockMode::INTENTION_EXCLUSIVE);
        #ifdef ENABLE_LOCK_MAP
            LockRequestQueue lock_rq;
            lock_table_.insert({lock_id, lock_rq});
            lock_table_.at(lock_id).request_queue_.emplace_back(lock_req);
        #endif

        txn->append_lock_set(lock_id);
    }
    else {
        bool append_flag = true;
        auto &lock_reqs = lock_table_[lock_id].request_queue_;

        for(auto iter = lock_reqs.begin(); iter!=lock_reqs.end(); iter++) {
            if(iter->txn_id_!=txn->get_transaction_id() && (iter->lock_mode_==LockMode::EXLUCSIVE || iter->lock_mode_ == LockMode::S_IX || iter->lock_mode_ == LockMode::SHARED)) {
                throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
            }
        }

        for(auto iter = lock_reqs.begin(); iter!=lock_reqs.end(); iter++) {
            if(iter->txn_id_==txn->get_transaction_id()) {
                append_flag = false;
                iter->lock_mode_ = (iter->lock_mode_==LockMode::INTENTION_SHARED)? LockMode::INTENTION_EXCLUSIVE:(iter->lock_mode_==LockMode::SHARED)? LockMode::S_IX:iter->lock_mode_;
                break;
            }
        }

        if (append_flag) {
            LockRequest lock_req(txn->get_transaction_id(), LockMode::INTENTION_EXCLUSIVE);
            lock_table_[lock_id].request_queue_.emplace_back(lock_req);
            txn->append_lock_set(lock_id);
        }
    }
    return true;
}

/**
 * @description: 释放锁
 * @return {bool} 返回解锁是否成功
 * @param {Transaction*} txn 要释放锁的事务对象指针
 * @param {LockDataId} lock_data_id 要释放的锁ID
 */
bool LockManager::unlock(Transaction* txn, LockDataId lock_data_id) {

    std::lock_guard<std::mutex>lg(latch_);

    if(lock_table_.find(lock_data_id) == lock_table_.end()) {
        return true;
    }
    else {
        auto &lock_reqs = lock_table_[lock_data_id].request_queue_;

        for(auto iter = lock_reqs.begin(); iter!=lock_reqs.end(); iter++) {
            if(iter->txn_id_ == txn->get_transaction_id()) {
                lock_reqs.erase(iter);
                break;
            }
        }
        if(lock_reqs.empty()) {
            lock_table_.erase(lock_data_id);
        }
        return true;
    }
    return false;
}
