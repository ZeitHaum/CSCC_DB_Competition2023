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

#include "index/ix.h"
#include "record/rm_file_handle.h"
#include "sm_defs.h"
#include "sm_meta.h"
#include "common/context.h"

class Context;

struct ColDef {
    std::string name;  // Column name
    ColType type;      // Type of column
    int len;           // Length of column
};

class record_unpin_guard{
public:
    PageId p_id;
    bool is_dirty;
    BufferPoolManager* buffer_pool_manager;
    record_unpin_guard(PageId p_id_, bool is_dirty_, BufferPoolManager* buffer_pool_manager_)
    : p_id(p_id_), is_dirty(is_dirty_), buffer_pool_manager(buffer_pool_manager_)
    {

    }
    ~record_unpin_guard(){
        buffer_pool_manager->unpin_page(p_id, is_dirty);
    }
};

/* 系统管理器，负责元数据管理和DDL语句的执行 */
class SmManager {
   public:
    DbMeta db_;             // 当前打开的数据库的元数据
    std::unordered_map<std::string, std::unique_ptr<RmFileHandle>> fhs_;    // file name -> record file handle, 当前数据库中每张表的数据文件
    std::unordered_map<std::string, std::unique_ptr<IxIndexHandle>> ihs_;   // file name -> index file handle, 当前数据库中每个索引的文件
    BufferPoolManager* buffer_pool_manager_;
    std::atomic<bool> io_enabled_;
   private:
    DiskManager* disk_manager_;
    RmManager* rm_manager_;
    IxManager* ix_manager_;
   public:

    SmManager(DiskManager* disk_manager, BufferPoolManager* buffer_pool_manager, RmManager* rm_manager, IxManager* ix_manager)
        : buffer_pool_manager_(buffer_pool_manager),  io_enabled_(true), disk_manager_(disk_manager), rm_manager_(rm_manager), ix_manager_(ix_manager)
        {
        
        }

     ~SmManager() {
     }

    BufferPoolManager* get_bpm() { return buffer_pool_manager_; }

    RmManager* get_rm_manager() { return rm_manager_; }  

    IxManager* get_ix_manager() { return ix_manager_; }  

    bool is_dir(const std::string& db_name);

    void create_db(const std::string& db_name);

    void drop_db(const std::string& db_name);

    void open_db(const std::string& db_name);

    void close_db();

    void flush_meta();

    void show_tables(Context* context);

    void show_index(const std::string& tab_name, Context* context);

    void desc_table(const std::string& tab_name, Context* context);

    void create_table(const std::string& tab_name, const std::vector<ColDef>& col_defs, Context* context);

    void drop_table(const std::string& tab_name, Context* context);

    void create_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context);

    void drop_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context);
    
    void drop_index(const std::string& tab_name, const std::vector<ColMeta>& col_names, Context* context);

    TabMeta& get_table_meta(std::string& tab_name){
        return this->db_.tabs_.at(tab_name);
    }

    RmFileHandle* get_file_handle(const std::string& tab_name){
        return fhs_.at(tab_name).get();
    }

    void insert_rec(RmFileHandle* fh_, std::vector<Value>& values_, TabMeta& tab_, Context* context_, Rid& rid_);

    static bool check_datetime_(std::string s);

    void load_data(const std::string& tab_name, const std::string& file_name);

    void parse_csv(const std::string& file_name, const std::string& table_name, std::vector<std::vector<Value>>& ans, int record_size);

    void do_pre_insert(RmFileHandle* fh_, std::vector<Value>& values_, TabMeta& tab_, RmRecord& rec, Context* context_);

    void load_pre_insert(RmFileHandle* fh_, std::vector<Value>& values_, TabMeta& tab_, RmRecord& rec, Context* context_);

    void insert_into_index(TabMeta& tab_, RmRecord& rec, Context* context_, Rid& rid_);

    void load_csv_itermodel(const std::string& file_name, const std::string& table_name);

};