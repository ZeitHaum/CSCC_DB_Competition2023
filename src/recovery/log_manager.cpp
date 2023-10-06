/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include <cstring>
#include "log_manager.h"
// #define DEBUG_LOG_MANAGER_CPP
/**
 * @description: 添加日志记录到日志缓冲区中，并返回日志记录号
 * @param {LogRecord*} log_record 要写入缓冲区的日志记录
 * @return {lsn_t} 返回该日志的日志记录号
 */
void LogManager::add_log_to_buffer(LogRecord* log_record) {
    assert(log_record->log_tot_len_!=0);
    if(log_buffer_.is_full(log_record->log_tot_len_)) {
        this->flush_log_to_disk(FlushReason::BUFFER_FULL);
    }
    if(log_record->log_type_ == LogType::ABORT){
        AbortLogRecord* casted_log_record = dynamic_cast<AbortLogRecord*> (log_record);
        casted_log_record->serialize(log_buffer_.buffer_ + log_buffer_.offset_);
        log_buffer_.offset_ += log_record->log_tot_len_;
    }
    else if(log_record->log_type_ == LogType::commit){
        CommitLogRecord* casted_log_record = dynamic_cast<CommitLogRecord*> (log_record);
        casted_log_record->serialize(log_buffer_.buffer_ + log_buffer_.offset_);
        log_buffer_.offset_ += log_record->log_tot_len_;
    }
    else if(log_record->log_type_ == LogType::begin){
        BeginLogRecord* casted_log_record = dynamic_cast<BeginLogRecord*> (log_record);
        casted_log_record->serialize(log_buffer_.buffer_ + log_buffer_.offset_);
        log_buffer_.offset_ += log_record->log_tot_len_;
    }
    else if(log_record->log_type_ == LogType::INSERT){
        InsertLogRecord* casted_log_record = dynamic_cast<InsertLogRecord*> (log_record);
        casted_log_record->serialize(log_buffer_.buffer_ + log_buffer_.offset_);
        log_buffer_.offset_ += log_record->log_tot_len_;
    }
    else if(log_record->log_type_ == LogType::DELETE){
        DeleteLogRecord* casted_log_record = dynamic_cast<DeleteLogRecord*> (log_record);
        casted_log_record->serialize(log_buffer_.buffer_ + log_buffer_.offset_);
        log_buffer_.offset_ += log_record->log_tot_len_;
    }
    else if(log_record->log_type_ == LogType::UPDATE){
        UpdateLogRecord* casted_log_record = dynamic_cast<UpdateLogRecord*> (log_record);
        casted_log_record->serialize(log_buffer_.buffer_ + log_buffer_.offset_);
        log_buffer_.offset_ += log_record->log_tot_len_;
    }
    else{
        assert(0);
    }
}

/**
 * @description: 把日志缓冲区的内容刷到磁盘中，由于目前只设置了一个缓冲区，因此需要阻塞其他日志操作
 */
void LogManager::flush_log_to_disk(){
    flush_log_to_disk(FlushReason::DEFAULT);
}

void LogManager::flush_log_to_disk(FlushReason flush_reason) {
    if(flush_reason != FlushReason::BUFFER_FULL){
        this->latch_.lock();
    }
    //写回到磁盘
    disk_manager_->write_log(this->get_log_buffer()->buffer_,this->get_log_buffer()->offset_);

    #ifdef DEBUG_RECOVERY
        char buffer_[this->get_log_buffer()->offset_];
        disk_manager_->read_log(this->buffer_, fsize, 0);
    #endif

    //维护元数据
    this->persist_lsn_ = this->global_lsn_ - 1;
    this->get_log_buffer()->resetBuffer();
    

    if(flush_reason != FlushReason::BUFFER_FULL){
        this->latch_.unlock();
    }
}

/**
 * @description: 用于分发lsn
 */
lsn_t LogManager::get_new_lsn() {
    return global_lsn_++;
}


/**
 * @description: 添加一条insert_log_record
 */
void LogManager::add_insert_log_record(txn_id_t txn_id, RmRecord& insert_value, 
                                            Rid& rid, const std::string& table_name) {
    
    std::lock_guard<std::mutex>lg(this->latch_);
    InsertLogRecord *insert_log = new InsertLogRecord(txn_id, insert_value, rid, table_name);
    insert_log->lsn_ = get_new_lsn();
    add_log_to_buffer(insert_log);
    delete insert_log;
}

void LogManager::add_delete_log_record(txn_id_t txn_id, RmRecord& delete_value, 
                                            Rid& rid, const std::string& table_name) {
    std::lock_guard<std::mutex>lg(this->latch_);
    DeleteLogRecord *delete_log = new DeleteLogRecord(txn_id, delete_value, rid, table_name);
    delete_log->lsn_ = get_new_lsn();
    add_log_to_buffer(delete_log);
    delete delete_log;
}

void LogManager::add_update_log_record(txn_id_t txn_id, RmRecord& update_value, RmRecord& old_value, 
                                            Rid& rid, const std::string& table_name) {
    std::lock_guard<std::mutex>lg(this->latch_);

    UpdateLogRecord *update_log = new UpdateLogRecord(txn_id, update_value, old_value, rid, table_name);
    update_log->lsn_ = get_new_lsn();
    add_log_to_buffer(update_log);
    delete update_log;
}


void LogManager::add_begin_log_record(txn_id_t txn_id) {
    std::lock_guard<std::mutex>lg(this->latch_);

    BeginLogRecord *begin_log = new BeginLogRecord(txn_id);
    begin_log->lsn_ = get_new_lsn();
    add_log_to_buffer(begin_log);
    delete begin_log;
}

void LogManager::add_commit_log_record(txn_id_t txn_id) {
    std::lock_guard<std::mutex>lg(this->latch_);

    CommitLogRecord *commit_log = new CommitLogRecord(txn_id);
    commit_log->lsn_ = get_new_lsn();
    add_log_to_buffer(commit_log);
    delete commit_log;
}

void LogManager::add_abort_log_record(txn_id_t txn_id) {
    std::lock_guard<std::mutex>lg(this->latch_);

    AbortLogRecord *abort_log = new AbortLogRecord(txn_id);
    abort_log->lsn_ = get_new_lsn();
    add_log_to_buffer(abort_log);
    delete abort_log;
}