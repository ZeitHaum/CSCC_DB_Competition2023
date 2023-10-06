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

#include <map>
#include <unordered_map>
#include "log_manager.h"
#include "storage/disk_manager.h"
#include "system/sm_manager.h"
// #define DEBUG_RECOVERY

class RedoLogsInPage {
public:
    RedoLogsInPage() { table_file_ = nullptr; }
    RmFileHandle* table_file_;
    std::vector<lsn_t> redo_logs_;   // 在该page上需要redo的操作的lsn
};

class RecoveryManager {
public:
    RecoveryManager(DiskManager* disk_manager, BufferPoolManager* buffer_pool_manager, SmManager* sm_manager, LogManager* log_manager) {
        disk_manager_ = disk_manager;
        buffer_pool_manager_ = buffer_pool_manager;
        sm_manager_ = sm_manager;
        log_manager_ = log_manager;
        tmp_lsn_cnt = 0;
        this->buffer_ = nullptr;
        // #ifdef DEBUG_RECOVERY
        //     const int file_size = disk_manager_->get_file_size(LOG_FILE_NAME);
        // #endif
        // buffer_ = new char[std::max(disk_manager_->get_file_size(LOG_FILE_NAME), 1)];
    }

    void analyze();
    void redo();
    void undo();
    void RedoLog(LogRecord* log_record, lsn_t now_lsn);
    void UndoLog(LogRecord* log_record, lsn_t now_lsn);
    void parseLog();
    Page* get_page(const std::string& tab_name,const int& page_no);
    bool is_record_stroed(const std::string& file_name, const int& page_no, lsn_t now_lsn);
    bool is_index_stored(const std::string& file_name, lsn_t now_lsn);
    void allocpage(Rid& rid, RmFileHandle* fh_);
private:
    // LogBuffer buffer_;                                              // 读入日志
    char* buffer_;
    DiskManager* disk_manager_;                                     // 用来读写文件
    BufferPoolManager* buffer_pool_manager_;                        // 对页面进行读写
    SmManager* sm_manager_;                                         // 访问数据库元数据
    LogManager* log_manager_;                                       //维护元数据
    int tmp_lsn_cnt;
    std::vector<LogRecord* > read_log_records;                       //读到的log_record
    std::set<txn_id_t> undo_list;                                //undo_list.
};