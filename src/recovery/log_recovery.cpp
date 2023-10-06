/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "log_recovery.h"

/**
 * @description: analyze阶段，需要获得脏页表（DPT）和未完成的事务列表（ATT）
 */
void RecoveryManager::analyze() {
    //解析log_file
    // #ifdef DEBUG_RECOVERY
    // #endif
    int fsize = disk_manager_->get_file_size(LOG_FILE_NAME);
    // std::cerr << fsize << "\n";
    this->buffer_ = new char[std::max(fsize,1)];
    disk_manager_->read_log(this->buffer_, fsize, 0);
    this->parseLog();
}

/**
 * @description: 重做所有未落盘的操作
 */
void RecoveryManager::redo() {
    //正向redo每条log
    for(auto iter = this->read_log_records.begin(); iter != this->read_log_records.end(); iter++){
        #ifdef DEBUG_RECOVERY
            auto redo_ind = iter - this->read_log_records.begin();
        #endif
        this->RedoLog(*iter, iter - this->read_log_records.begin());
    }
}

/**
 * @description: 回滚未完成的事务
 */
void RecoveryManager::undo() {
    //逆向undo每条log
    for(auto iter = this->read_log_records.rbegin(); iter!= this->read_log_records.rend(); iter++){
        #ifdef DEBUG_RECOVERY
            auto undo_ind = iter - this->read_log_records.rbegin();
            if(undo_ind == 23){
                auto gdb_break = 1;
            }
        #endif
        if(this->undo_list.empty()){
            break;
        }
        this->UndoLog(*iter, iter - this->read_log_records.rend() - 1);
    }

    for(auto iter = this->read_log_records.rbegin(); iter!= this->read_log_records.rend(); iter++){
        //释放资源
        delete *iter;
    }

    //重建索引
    for(auto iter = this->sm_manager_->db_.tabs_.begin(); iter != this->sm_manager_->db_.tabs_.end(); iter++){
        std::vector<IndexMeta> tmp_indexes;
        for(auto sub_it = iter->second.indexes.begin(); sub_it != iter->second.indexes.end(); sub_it++){
            tmp_indexes.emplace_back(*sub_it);
        }
        for(auto sub_it = tmp_indexes.begin(); sub_it != tmp_indexes.end(); sub_it++){
            sm_manager_->drop_index(sub_it->tab_name, sub_it->cols, nullptr);
            std::vector<std::string> col_names;
            for(auto ss_it = sub_it->cols.begin(); ss_it!=sub_it->cols.end(); ss_it++){
                col_names.emplace_back(ss_it->name);
            }
            sm_manager_->create_index(sub_it->tab_name, col_names, nullptr);
        }
    }

    delete[] this->buffer_;
}

//判断落盘
bool RecoveryManager::is_record_stroed(const std::string& file_name, const int& page_no, lsn_t now_lsn){
    RmFileHandle* file_handle = this->sm_manager_->fhs_.at(file_name).get();
    if(page_no >= file_handle->get_file_hdr().num_pages ){
        return false;
    }
    RmPageHandle page_handle = file_handle->fetch_page_handle(page_no);
    #ifdef DEBUG_RECOVERY
        auto lsn_l =  page_handle.page->get_page_lsn();
    #endif
    unpin_page_guard upg({file_handle->GetFd(), page_no}, false, buffer_pool_manager_);
    return page_handle.page->get_page_lsn() >= now_lsn;
}

bool RecoveryManager::is_index_stored(const std::string& file_name, lsn_t now_lsn){
    IxIndexHandle* ih_ = this->sm_manager_->ihs_.at(file_name).get();
    return ih_->getFileHdr()->file_lsn_ >= now_lsn;
}

//获取目标页
Page* RecoveryManager::get_page(const std::string& tab_name,const int& page_no){
    RmFileHandle* file_handle = this->sm_manager_->fhs_.at(tab_name).get();
    RmPageHandle page_handle = file_handle->fetch_page_handle(page_no);
    return page_handle.page;
}

//Redo log
void RecoveryManager::RedoLog(LogRecord* log_record, lsn_t now_lsn){
    //根据不同类型执行操作
    if(log_record->log_type_ == LogType::begin){
        BeginLogRecord* begin_rec = (BeginLogRecord*) log_record;
        //加入undolist
        this->undo_list.insert(begin_rec->log_tid_);
    }
    else if(log_record->log_type_ == LogType::commit){
        CommitLogRecord* com_rec = (CommitLogRecord*) log_record;
        this->undo_list.erase(com_rec->log_tid_);
    }
    else if(log_record->log_type_ == LogType::ABORT){
        AbortLogRecord* abort_rec = (AbortLogRecord*) log_record;
        this->undo_list.erase(abort_rec->log_tid_);
    }
    else if(log_record->log_type_ == LogType::DELETE){
        DeleteLogRecord* del_rec = dynamic_cast<DeleteLogRecord*>(log_record);
        std::string del_name = del_rec->get_tab_name();
        if(!is_record_stroed(del_name, del_rec->rid_.page_no, now_lsn)){
            //需要redo
            #ifdef DEBUG_RECOVERY
                std::cout<<"REDO DELETE, RID:"<<del_rec->rid_.to_string()<<", val:"<<std::to_string(*(int*) del_rec->delete_value_.data)<<", table_name:"<<std::string(del_rec->table_name_, del_rec->table_name_size_) <<"\n";;
            #endif
            this->sm_manager_->fhs_.at(del_name)->delete_record(del_rec->rid_, nullptr);
        }
    }
    else if(log_record->log_type_ == LogType::IX_DELETE){     
    }
    else if(log_record->log_type_ == LogType::INSERT){
        InsertLogRecord* ins_rec = dynamic_cast<InsertLogRecord*>(log_record);
        std::string ins_name = ins_rec->get_tab_name();
        if(!is_record_stroed(ins_name, ins_rec->rid_.page_no, now_lsn)){
            //需要redo
            #ifdef DEBUG_RECOVERY
                std::cout<<"REDO INSERT, RID:"<<ins_rec->rid_.to_string()<<", val:"<<std::to_string(*(int*) ins_rec->insert_value_.data)<<", table_name:"<<std::string(ins_rec->table_name_, ins_rec->table_name_size_) <<"\n";
            #endif
            RmFileHandle* fh_ = this->sm_manager_->fhs_.at(ins_name).get();
            fh_->allocpage(ins_rec->rid_);
            fh_->insert_record(ins_rec->rid_, ins_rec->insert_value_.data);
        }
    }
    else if(log_record->log_type_ == LogType::IX_INSERT){
    }
    else if(log_record->log_type_ == LogType::UPDATE){
        UpdateLogRecord* up_rec = dynamic_cast<UpdateLogRecord*> (log_record);
        std::string up_name = up_rec->get_tab_name();
        if(!is_record_stroed(up_name, up_rec->rid_.page_no, now_lsn)){
            //需要redo
            #ifdef DEBUG_RECOVERY
                std::cout<<"REDO UPDATE, RID:"<<up_rec->rid_.to_string()<<", val:"<<std::to_string(*(int*) up_rec->update_value_.data)<<", old_val:"<<std::to_string(*(int*) up_rec->old_value_.data) << "table_name:"<<std::string(up_rec->table_name_, up_rec->table_name_size_)  <<"\n";
            #endif
            RmFileHandle* fh_ = this->sm_manager_->fhs_.at(up_name).get();
            fh_->allocpage(up_rec->rid_);
            fh_->update_record(up_rec->rid_, up_rec->update_value_.data, nullptr);
        }
    }
    else{
        assert(0);
    }
}



//Undo log
void RecoveryManager::UndoLog(LogRecord* log_record, lsn_t now_lsn){
    // auto set_page_lsn = [this](int page_no, lsn_t lsn, std::string& file_name){
    //     RmFileHandle* fh_ = sm_manager_ ->fhs_.at(file_name).get();
    //     #ifdef ENABLE_LSN
    //         RmPageHandle page = fh_ ->fetch_page_handle(page_no);
    //         page.page->set_page_lsn(lsn);
    //     #endif
    //     sm_manager_ ->buffer_pool_manager_->unpin_page({fh_->GetFd(), page_no}, true);
    // };
    //根据不同类型执行操作
    if(log_record->log_type_ == LogType::begin){
        BeginLogRecord* begin_rec = (BeginLogRecord*) log_record;
        if(this->undo_list.find(begin_rec->log_tid_)!= this->undo_list.end()){
            //写回到日志
            log_manager_->add_abort_log_record(begin_rec->log_tid_);
            undo_list.erase(begin_rec->log_tid_);
        }
    }
    else if(log_record->log_type_ == LogType::commit){
        //非法语句
        assert(this->undo_list.find(log_record->log_tid_)== this->undo_list.end());
    }
    else if(log_record->log_type_ == LogType::ABORT){
        //非法语句
        assert(this->undo_list.find(log_record->log_tid_)== this->undo_list.end());
    }
    else if(log_record->log_type_ == LogType::DELETE){
        DeleteLogRecord* del_rec = dynamic_cast<DeleteLogRecord*> (log_record);
        std::string del_name = del_rec->get_tab_name();
        #ifdef ENABLE_LSN 
            if(this->undo_list.find(del_rec->log_tid_)!= this->undo_list.end() && is_record_stroed(del_name, del_rec->rid_.page_no, now_lsn)){
        #endif
        #ifndef ENABLE_LSN
            if(this->undo_list.find(del_rec->log_tid_)!= this->undo_list.end()){
        #endif    
            //写回磁盘的才undo
            log_manager_->add_insert_log_record(del_rec->log_tid_, del_rec->delete_value_, del_rec->rid_, del_name);
            this->sm_manager_->fhs_.at(del_name)->insert_record(del_rec->rid_, del_rec->delete_value_.data);
            // set_page_lsn(del_rec->rid_.page_no, now_lsn ,del_name);
        }
    }
    else if(log_record->log_type_ == LogType::INSERT){
        InsertLogRecord* ins_rec = dynamic_cast<InsertLogRecord*>(log_record);
        std::string ins_name = ins_rec->get_tab_name();
        #ifdef ENABLE_LSN            
            if(this->undo_list.find(ins_rec->log_tid_)!= this->undo_list.end() && is_record_stroed(ins_name, ins_rec->rid_.page_no, now_lsn)){
        #endif
        #ifndef ENABLE_LSN
            if(this->undo_list.find(ins_rec->log_tid_)!= this->undo_list.end()){
        #endif
            //写回磁盘的才undo
            log_manager_->add_delete_log_record(ins_rec->log_tid_, ins_rec->insert_value_, ins_rec->rid_, ins_name);
            this->sm_manager_->fhs_.at(ins_name)->delete_record(ins_rec->rid_, nullptr);
            // set_page_lsn(ins_rec->rid_.page_no, now_lsn ,ins_name);
        }
    }
    else if(log_record->log_type_ == LogType::UPDATE){
        UpdateLogRecord* up_rec = dynamic_cast<UpdateLogRecord*> (log_record);
        std::string up_name = up_rec->get_tab_name();
        #ifdef ENABLE_LSN
            if(this->undo_list.find(up_rec->log_tid_)!= this->undo_list.end() && is_record_stroed(up_name, up_rec->rid_.page_no, now_lsn)){
        #endif
        #ifndef ENABLE_LSN
            if(this->undo_list.find(up_rec->log_tid_)!= this->undo_list.end()){
        #endif
            //写回磁盘的才undo
            log_manager_->add_update_log_record(up_rec->log_tid_, up_rec->old_value_, up_rec->update_value_,up_rec->rid_, up_name);
            this->sm_manager_->fhs_.at(up_name)->update_record(up_rec->rid_, up_rec->old_value_.data, nullptr);
            // set_page_lsn(up_rec->rid_.page_no, now_lsn ,up_name);
        }
    }
    else{
        assert(0);
    }
}

//解析LogBuffer.
void RecoveryManager::parseLog(){
    int offset = 0;
    //开始解析
    LogRecord* new_log = new LogRecord();
    while(disk_manager_->get_file_size(LOG_FILE_NAME) !=0){
        new_log->deserialize(this->buffer_+offset);
        // assert(new_log->log_tot_len_!=0);
        if(new_log->log_tot_len_<=0) {
            exit(1);
        }
        if(new_log->log_type_ == LogType::begin){
            BeginLogRecord* begin_log_rec = new BeginLogRecord();
            begin_log_rec->deserialize(this->buffer_+offset);
            this->read_log_records.emplace_back((LogRecord*)begin_log_rec);
            offset+= begin_log_rec->log_tot_len_;
            #ifdef DEBUG_RECOVERY
                begin_log_rec->format_print();
            #endif
        }
        else if(new_log->log_type_ == LogType::commit){
            CommitLogRecord* commit_log_rec = new CommitLogRecord();
            commit_log_rec->deserialize(this->buffer_+offset);
            this->read_log_records.emplace_back((LogRecord*)commit_log_rec);
            offset+= commit_log_rec->log_tot_len_;
            #ifdef DEBUG_RECOVERY
                commit_log_rec->format_print();
            #endif
        }
        else if(new_log->log_type_  == LogType::ABORT){
            AbortLogRecord* abort_log_rec = new AbortLogRecord();
            abort_log_rec->deserialize(this->buffer_+offset);
            this->read_log_records.emplace_back((LogRecord*)abort_log_rec);
            offset+= abort_log_rec->log_tot_len_;
            #ifdef DEBUG_RECOVERY
                abort_log_rec->format_print();
            #endif
        }
        else if(new_log->log_type_ == LogType::DELETE){
            DeleteLogRecord* del_log_rec = new DeleteLogRecord();
            del_log_rec->deserialize(this->buffer_+offset);
            this->read_log_records.emplace_back((LogRecord*) del_log_rec);
            offset+= del_log_rec->log_tot_len_;
            #ifdef DEBUG_RECOVERY
                del_log_rec->format_print();
            #endif
        }
        else if(new_log->log_type_ == LogType::INSERT){
             InsertLogRecord* ins_log_rec = new InsertLogRecord();
             ins_log_rec->deserialize(this->buffer_+offset);
             this->read_log_records.emplace_back((LogRecord*) ins_log_rec);
             offset+= ins_log_rec->log_tot_len_;
             #ifdef DEBUG_RECOVERY
                ins_log_rec->format_print();
            #endif
        }
        else if(new_log->log_type_ == LogType::UPDATE){
             UpdateLogRecord* up_log_rec = new UpdateLogRecord();
             up_log_rec->deserialize(this->buffer_ + offset);
             this->read_log_records.emplace_back(up_log_rec);
             offset+= up_log_rec->log_tot_len_;
             #ifdef DEBUG_RECOVERY
                up_log_rec->format_print();
            #endif
        }
        else if(new_log->log_type_ == LogType::IX_INSERT){
            assert(0);
        }
        else if(new_log->log_type_ == LogType::IX_DELETE){
            assert(0);
        }
        else{
            assert(0);
        }
        if(offset>= disk_manager_->get_file_size(LOG_FILE_NAME)){
            break;
        }
    }
    delete new_log;
    this->log_manager_->set_global_lsn_(this->read_log_records.size());
    this->tmp_lsn_cnt = this->log_manager_->get_global_lsn_();
    this->log_manager_->set_persist_lsn_(this->read_log_records.size() - 1);
}