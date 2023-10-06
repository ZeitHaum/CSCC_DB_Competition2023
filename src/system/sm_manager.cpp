/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sm_manager.h"

#include <sys/stat.h>
#include <unistd.h>

#include <fstream>

#include "index/ix.h"
#include "record/rm.h"
#include "record_printer.h"
#include <iostream>

// #define DEBUG_SM_MANAGER

/**
 * @description: 判断是否为一个文件夹
 * @return {bool} 返回是否为一个文件夹
 * @param {string&} db_name 数据库文件名称，与文件夹同名
 */
bool SmManager::is_dir(const std::string& db_name) {
    struct stat st;
    return stat(db_name.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

/**
 * @description: 创建数据库，所有的数据库相关文件都放在数据库同名文件夹下
 * @param {string&} db_name 数据库名称
 */
void SmManager::create_db(const std::string& db_name) {
    if (is_dir(db_name)) {
        throw DatabaseExistsError(db_name);
    }
    //为数据库创建一个子目录
    std::string cmd = "mkdir " + db_name;
    if (system(cmd.c_str()) < 0) {  // 创建一个名为db_name的目录
        throw UnixError();
    }
    if (chdir(db_name.c_str()) < 0) {  // 进入名为db_name的目录
        throw UnixError();
    }
    //创建系统目录
    DbMeta *new_db = new DbMeta();
    new_db->name_ = db_name;

    // 注意，此处ofstream会在当前目录创建(如果没有此文件先创建)和打开一个名为DB_META_NAME的文件
    std::ofstream ofs(DB_META_NAME);

    // 将new_db中的信息，按照定义好的operator<<操作符，写入到ofs打开的DB_META_NAME文件中
    ofs << *new_db;  // 注意：此处重载了操作符<<

    delete new_db;

    // 创建日志文件
    disk_manager_->create_file(LOG_FILE_NAME);

    // 回到根目录
    if (chdir("..") < 0) {
        throw UnixError();
    }
}

/**
 * @description: 删除数据库，同时需要清空相关文件以及数据库同名文件夹
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::drop_db(const std::string& db_name) {
    if (!is_dir(db_name)) {
        throw DatabaseNotFoundError(db_name);
    }
    std::string cmd = "rm -r " + db_name;
    if (system(cmd.c_str()) < 0) {
        throw UnixError();
    }
}

/**
 * @description: 打开数据库，找到数据库对应的文件夹，并加载数据库元数据和相关文件
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::open_db(const std::string& db_name) {
    // 1. 进入db目录
    // 2. DbMeta db_; 
    // 3. std::unordered_map<std::string, std::unique_ptr<RmFileHandle>> fhs_; 
    // 4. std::unordered_map<std::string, std::unique_ptr<IxIndexHandle>> ihs_;
    // 没有处理日志文件
    if (chdir(db_name.c_str()) < 0) {  // 进入名为db_name的目录
        throw UnixError();
    }

    std::ifstream ifs(DB_META_NAME);

    //读入db_元数据
    ifs >> db_;
    
    //读入表handle
    for(auto iter = db_.tabs_.begin();iter!=db_.tabs_.end();++iter) {
        fhs_[iter->first] = rm_manager_->open_file(iter->first);
        // RmFileHandle* file_handle = fhs_[iter->first].get();
        // disk_manager_->set_fd2pageno(file_handle->GetFd(), file_handle->get_file_hdr().num_pages);
    }

    for(auto iter = db_.tabs_.begin();iter!=db_.tabs_.end();++iter) {
        TabMeta& tab_meta = iter->second;
        tab_meta.init_hash();
        for(auto sub_it =  tab_meta.indexes.begin(); sub_it!=tab_meta.indexes.end(); sub_it++){
            std::string name = ix_manager_->get_index_name(iter->first, sub_it->cols);
            ihs_[name] = ix_manager_->open_index(tab_meta.name, sub_it->cols);
            // IxIndexHandle* ix_handle = ihs_[name].get();
            // disk_manager_ ->set_fd2pageno(ix_handle->getFd(), ix_handle->getFileHdr()->num_pages_);
        }
    }

}

/**
 * @description: 把数据库相关的元数据刷入磁盘中
 */
void SmManager::flush_meta() {
    // 默认清空文件
    std::ofstream ofs(DB_META_NAME);
    ofs << db_;
}

/**
 * @description: 关闭数据库并把数据落盘
 */
void SmManager::close_db() {
    // 1. 遍历fhs_, flush所有文件的页, fhs_ 清空
    // 2. 处理 fhs_, ihs_ 清空
    // 3. db_ 调用flush_meta()数据库元数据文件(db.meta), 清零 db_
    // 4. 回到根目录

    //1.
    for(auto iter = fhs_.begin(); iter!= fhs_.end(); iter++){
        //取fd
        // int fd = (iter->second).get()->GetFd();
        //flush
        rm_manager_->close_file((iter->second).get());
    }

    //2. 
    fhs_.clear();
    ihs_.clear();

    //3. 
    flush_meta();

    //4. 
    // 回到根目录
    if (chdir("..") < 0) {
        throw UnixError();
    }
}

/**
 * @description: 显示所有的表,通过测试需要将其结果写入到output.txt,详情看题目文档
 * @param {Context*} context 
 */
void SmManager::show_tables(Context* context) {
    // disk_manager_->outfile << "| Tables |\n";
    #ifdef ENABLE_OUTPUTFILE_IO
        std::fstream outfile;
    #endif
    #ifndef ENABLE_OUTPUTFILE_IO
        std::fstream& outfile = disk_manager_->outfile;
    #endif
    if(io_enabled_){
        #ifdef ENABLE_OUTPUTFILE_IO
            outfile.open("output.txt", std::ios::out | std::ios::app);
        #endif
        outfile << "| Tables |\n";
    }
    RecordPrinter printer(1);
    printer.print_separator(context);
    printer.print_record({"Tables"}, context);
    printer.print_separator(context);
    for (auto &entry : db_.tabs_) {
        auto &tab = entry.second;
        printer.print_record({tab.name}, context);
        if(io_enabled_){
            outfile << "| " << tab.name << " |\n";
        }
    }
    printer.print_separator(context);
    if(io_enabled_){
        #ifdef ENABLE_OUTPUTFILE_IO
            outfile.close();
        #endif
        #ifndef ENABLE_OUTPUTFILE_IO
            outfile.flush();
        #endif
    }
}

/**
 * @description: 显示所有的索引,通过测试需要将其结果写入到output.txt,详情看题目文档
 * @param {string&} tab_name 表名称
 * @param {Context*} context 
 */
void SmManager::show_index(const std::string& tab_name, Context* context) {
    const std::vector<IndexMeta>& indexes =  db_.get_table(tab_name).indexes;
    RecordPrinter printer(3);// 3列
    printer.print_separator(context);
    auto get_index_str = [&](const IndexMeta& ixmeta){
        std::string ret = "(";
        for(auto iter = ixmeta.cols.begin(); iter!= ixmeta.cols.end(); iter++){
            if(iter!=ixmeta.cols.begin()){
                ret.push_back(',');
            }
            ret += iter->name;
        }
        ret += ")";
        return ret;
    };
    #ifdef ENABLE_OUTPUTFILE_IO
        std::fstream outfile;
    #endif
    #ifndef ENABLE_OUTPUTFILE_IO
        std::fstream& outfile = disk_manager_->outfile;
    #endif
    if(io_enabled_){
        #ifdef ENABLE_OUTPUTFILE_IO
            outfile.open("output.txt", std::ios::out | std::ios::app);
        #endif
    }
    for(auto iter = indexes.begin(); iter!= indexes.end(); iter++){
        printer.print_record({tab_name, "unique", get_index_str(*iter)}, context);
        if(io_enabled_){
            outfile<<"| "<<tab_name<<" | "<<"unique"<<" | "<<get_index_str(*iter)<<" |"<<"\n";
        }
    }
    if(io_enabled_){
        #ifdef ENABLE_OUTPUTFILE_IO
            outfile.close();
        #endif
        #ifndef ENABLE_OUTPUTFILE_IO
            outfile.flush();
        #endif
    }
    printer.print_separator(context);
}

/**
 * @description: 显示表的元数据
 * @param {string&} tab_name 表名称
 * @param {Context*} context 
 */
void SmManager::desc_table(const std::string& tab_name, Context* context) {
    TabMeta &tab = db_.get_table(tab_name);

    std::vector<std::string> captions = {"Field", "Type", "Index"};
    RecordPrinter printer(captions.size());
    // Print header
    printer.print_separator(context);
    printer.print_record(captions, context);
    printer.print_separator(context);
    // Print fields
    for (auto &col : tab.cols) {
        std::vector<std::string> field_info = {col.name, coltype2str(col.type), col.index ? "YES" : "NO"};
        printer.print_record(field_info, context);
    }
    // Print footer
    printer.print_separator(context);
}

/**
 * @description: 创建表
 * @param {string&} tab_name 表的名称
 * @param {vector<ColDef>&} col_defs 表的字段
 * @param {Context*} context 
 */
void SmManager::create_table(const std::string& tab_name, const std::vector<ColDef>& col_defs, Context* context) {
    if (db_.is_table(tab_name)) {
        throw TableExistsError(tab_name);
    }
    #ifdef DEBUG_SM_MANAGER
        std::map<PageId, int> id_pin_ = buffer_pool_manager_->get_pin_count();
    #endif
    // Create table meta
    int curr_offset = 0;
    TabMeta tab;
    tab.name = tab_name;
    for (auto &col_def : col_defs) {
        ColMeta col = {.tab_name = tab_name,
                       .name = col_def.name,
                       .type = col_def.type,
                       .len = col_def.len,
                       .offset = curr_offset,
                       .index = false};
        curr_offset += col_def.len;
        tab.cols_hash[col.name] = tab.cols.size();
        tab.cols.emplace_back(col);
    }
    // Create & open record file
    int record_size = curr_offset;  // record_size就是col meta所占的大小（表的元数据也是以记录的形式进行存储的）
    rm_manager_->create_file(tab_name, record_size);
    db_.tabs_[tab_name] = tab;
    // fhs_[tab_name] = rm_manager_->open_file(tab_name);
    fhs_.emplace(tab_name, rm_manager_->open_file(tab_name));

    flush_meta();
}

/**
 * @description: 删除表
 * @param {string&} tab_name 表的名称
 * @param {Context*} context
 */
void SmManager::drop_table(const std::string& tab_name, Context* context) {
    //wsh : 先不考虑外键依赖。(参考create_table的写法)
    // 1. 先检查tab_name是否存在
    // 2. 删除表文件， 删除db_元数据中的table_meta
    // 3. flush_meta
    // 4. fh_ ih_ 删除对应的Handle指针
    // 5. Contex事物处理(暂不管)

    //1.
    if (!db_.is_table(tab_name)) {
        throw TableNotFoundError(tab_name);
    }

    //2.
    RmFileHandle* filehandle = fhs_[tab_name].get();
    for(int i = 0;i<filehandle->get_file_hdr().num_pages;i++){
        assert(buffer_pool_manager_->unpin_page({filehandle->GetFd(), i}, false) == false);
        this->buffer_pool_manager_->delete_page({filehandle->GetFd(), i});
    }
    rm_manager_->close_file(filehandle);
    db_.tabs_.erase(tab_name);
    rm_manager_->destroy_file(tab_name);

    //3.
    flush_meta();

    //4.
    fhs_.erase(tab_name);

}

/**
 * @description: 创建索引
 * @param {string&} tab_name 表的名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::create_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {
    //重复建立索引直接return，do nothing,部分重复也算

    //拿到索引所有字段的元数据信息
    TabMeta &tab = db_.get_table(tab_name);

    //判断字段是否已经建立索引，已经建立则return，并记录索引长度
    if(tab.is_index(col_names)) {
        return;
    }

    IndexMeta inedx_meta;
    inedx_meta.tab_name = tab.name;
    inedx_meta.col_num = col_names.size();

    int tol_size = 0;
    for(auto col_name:col_names) {
        std::vector<ColMeta>::iterator col = tab.get_col(col_name);
        tol_size += col->len;
        inedx_meta.cols.emplace_back(*col);
    }
    inedx_meta.col_tot_len = tol_size;
    ix_manager_->create_index(tab_name, inedx_meta.cols);

    std::unique_ptr<IxIndexHandle> ih = ix_manager_->open_index(tab_name, col_names);
    #ifndef ENABLE_LOCK_CRABBING
        std::lock_guard<std::mutex> lg(ih->root_latch_);
    #endif
    auto file_handle = fhs_.at(tab_name).get();

    for (RmScan rm_scan(file_handle, context); !rm_scan.is_end(); rm_scan.next()) {
        auto rec = rm_scan.get_now_record();
        //将索引的值进行组合
        RmRecord tmp(tol_size);
        int now_offset = 0;
        for(auto col : inedx_meta.cols) {
            memcpy(tmp.data + now_offset, rec->data + col.offset, col.len);
            now_offset += col.len;
        }
        ih->insert_entry(tmp.data, rm_scan.rid(),context==nullptr? nullptr:context->txn_);
    }

    auto index_name = ix_manager_->get_index_name(tab_name, inedx_meta.cols);
    assert(ihs_.count(index_name) == 0);
    ihs_.emplace(index_name, std::move(ih));
    tab.indexes.emplace_back(inedx_meta);
    flush_meta();
}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {
    TabMeta &tab = db_.tabs_[tab_name];
    std::vector<ColMeta> colmetas;
    for(auto iter = col_names.begin(); iter!= col_names.end(); iter++){
        colmetas.emplace_back(*tab.get_col(*iter));
    }

    //没有建立直接return，do nothing, 部分未建立也算
    //检查部分索引是否未建立
    if (!tab.is_index(col_names)) {
        //Do Nothing
        return;
    }
    std::string index_name = ix_manager_->get_index_name(tab_name, colmetas);
    ix_manager_->close_index(ihs_.at(index_name).get());
    ix_manager_->destroy_index(tab_name, colmetas);
    IxFileHdr* ihs_file_hdr = ihs_.at(index_name).get()->getFileHdr();
    for(int i = 0;i<ihs_file_hdr->num_pages_;i++){
        PageId todel = {ihs_.at(index_name).get()->getFd(), i};
        //删之前必须pin_count = 0
        // while(buffer_pool_manager_->unpin_page(todel, false)){
            
        // }
        buffer_pool_manager_->unpin_page(todel, true);
        assert(buffer_pool_manager_->unpin_page(todel, true) == false);
        buffer_pool_manager_->delete_page({ihs_.at(index_name).get()->getFd(), i});
    }
    ihs_.erase(index_name);
    tab.indexes.erase(tab.find_index_pos(col_names) + tab.indexes.begin());
    flush_meta();
}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<ColMeta>&} 索引包含的字段元数据
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string& tab_name, const std::vector<ColMeta>& cols, Context* context) {
    std::vector<std::string> col_names;
    for(auto iter = cols.begin(); iter!=cols.end(); iter++){
        col_names.emplace_back(iter->name);
    }

    TabMeta &tab = db_.tabs_[tab_name];
    std::vector<ColMeta> colmetas;
    for(auto iter = col_names.begin(); iter!= col_names.end(); iter++){
        colmetas.emplace_back(*tab.get_col(*iter));
    }

    //没有建立直接return，do nothing, 部分未建立也算
    //检查部分索引是否未建立
    if (!tab.is_index(col_names)) {
        //Do Nothing
        return;
    }
    std::string index_name = ix_manager_->get_index_name(tab_name, colmetas);
    ix_manager_->close_index(ihs_.at(index_name).get());
    ix_manager_->destroy_index(tab_name, colmetas);
    IxFileHdr* ihs_file_hdr = ihs_.at(index_name).get()->getFileHdr();
    for(int i = 0;i<ihs_file_hdr->num_pages_;i++){
        PageId todel = {ihs_.at(index_name).get()->getFd(), i};
        buffer_pool_manager_->unpin_page(todel, true);
        //删之前必须pin_count = 0
        assert(buffer_pool_manager_->unpin_page(todel, true) == false);
        buffer_pool_manager_->delete_page({ihs_.at(index_name).get()->getFd(), i});
    }
    ihs_.erase(index_name);
    tab.indexes.erase(tab.find_index_pos(col_names) + tab.indexes.begin());
    flush_meta();
}

bool SmManager::check_datetime_(std::string s){
    if(strlen(s.c_str()) != 19){
        return false;
    }
    if(s<"1000-01-01 00:00:00" || s>"9999-12-31 23:59:59"){
        return false;
    }
    /**Parse datetime*/
    std::string year_str = "";
    std::string month_str = "";
    std::string day_str = "";
    std::string hour_str = "";
    std::string min_str = "";
    std::string sec_str = "";

    int now_parse_ptr = 0;
    for(int i = 0;i<4;i++){
        if(s[now_parse_ptr]<'0' || s[now_parse_ptr]>'9'){
            return false;
        }
        year_str.push_back(s[now_parse_ptr]);
        now_parse_ptr++;
    }
    if(s[now_parse_ptr++]!='-'){
        return false;
    }
    for(int i = 0;i<2;i++){
        if(s[now_parse_ptr]<'0' || s[now_parse_ptr]>'9'){
            return false;
        }
        month_str.push_back(s[now_parse_ptr]);
        now_parse_ptr++;
    }
    if(s[now_parse_ptr++]!='-'){
        return false;
    }
    for(int i = 0;i<2;i++){
        if(s[now_parse_ptr]<'0' || s[now_parse_ptr]>'9'){
            return false;
        }
        day_str.push_back(s[now_parse_ptr]);
        now_parse_ptr++;
    }
    if(s[now_parse_ptr++]!=' '){
        return false;
    }
    for(int i = 0;i<2;i++){
        if(s[now_parse_ptr]<'0' || s[now_parse_ptr]>'9'){
            return false;
        }
        hour_str.push_back(s[now_parse_ptr]);
        now_parse_ptr++;
    }
    if(s[now_parse_ptr++]!=':'){
        return false;
    }
    for(int i = 0;i<2;i++){
        if(s[now_parse_ptr]<'0' || s[now_parse_ptr]>'9'){
            return false;
        }
        min_str.push_back(s[now_parse_ptr]);
        now_parse_ptr++;
    }
    if(s[now_parse_ptr++]!=':'){
        return false;
    }
    for(int i = 0;i<2;i++){
        if(s[now_parse_ptr]<'0' || s[now_parse_ptr]>'9'){
            return false;
        }
        sec_str.push_back(s[now_parse_ptr]);
        now_parse_ptr++;
    }
    /*Check valid range*/
    if(std::stoi(month_str) < 1 || std::stoi(month_str)> 12){
        return false;
    }
    if(std::stoi(day_str) < 1 || std::stoi(day_str)> 31){
        return false;
    }
    //2月
    if(std::stoi(month_str)==2 && std::stoi(day_str)==30){
        return false;
    }
    if(std::stoi(hour_str)>23){
        return false;
    }
    if(std::stoi(min_str)>59){
        return false;
    }
    if(std::stoi(sec_str)>59){
        return false;
    }
    return true;
}

/*
插入record, 抽象为单独函数
*/
void SmManager::insert_rec(RmFileHandle* fh_, std::vector<Value>& values_, TabMeta& tab_, Context* context_, Rid& rid_){
    RmRecord rec(fh_->get_file_hdr().record_size);
    
    do_pre_insert(fh_, values_, tab_, rec, context_);

    // Insert into record file
    rid_ = fh_->insert_record(rec.data, context_);

    // Insert into index
    insert_into_index(tab_, rec, context_, rid_);
}

/**
 * insert 功能函数, 从values获得Record, 并检测合法性
 * 
*/
    //初始化record;
void SmManager::do_pre_insert(RmFileHandle* fh_, std::vector<Value>& values_, TabMeta& tab_, RmRecord& rec, Context* context_){
    for (size_t i = 0; i < values_.size(); ++i) {
        auto &col = tab_.cols[i];
        auto &val = values_[i];
        if (col.type != val.type) {
            if(col.type==TYPE_DATETIME){
                val.type = TYPE_DATETIME;
                val.datetime_val = val.str_val;
                if(!SmManager::check_datetime_(val.datetime_val)){
                    throw InvalidValueError(val.datetime_val);
                }
            }
            else if (col.type==TYPE_BIGINT) {
                if(val.type==TYPE_INT) {
                    val.type = TYPE_BIGINT;
                    val.set_bigint((long long)val.int_val);
                }
                else {
                    throw IncompatibleTypeError(coltype2str(col.type), coltype2str(val.type));
                }
            }
            else{
                throw IncompatibleTypeError(coltype2str(col.type), coltype2str(val.type));
            }
        }
        val.init_raw(col.len);
        memcpy(rec.data + col.offset, val.raw->data, col.len);
    }

    //给表加意向互斥锁
    if(context_ !=nullptr){
        context_->lock_mgr_->lock_IX_on_table(context_->txn_, fh_->GetFd());
    }

    for(size_t i = 0; i < tab_.indexes.size(); ++i) {
        auto& index = tab_.indexes[i];
        auto ih = ihs_.at(get_ix_manager()->get_index_name(tab_.name, index.cols)).get();
        #ifndef ENABLE_LOCK_CRABBING
            std::lock_guard<std::mutex> lg(ih->root_latch_);
         #endif
        char* key = new char[index.col_tot_len];
        int offset = 0;
        for(size_t i = 0; i < (size_t)index.col_num; ++i) {
            memcpy(key + offset, rec.data + index.cols[i].offset, index.cols[i].len);
            offset += index.cols[i].len;
        }
        bool is_failed = ih->binary_search(key, context_).page_no != -1;
        if(is_failed){
            delete[] key;
            throw IndexInsertDuplicatedError();
        }
        delete[] key;
    }
}


void SmManager::insert_into_index(TabMeta& tab_, RmRecord& rec, Context* context_, Rid& rid_){
    // Insert into index
    for(size_t i = 0; i < tab_.indexes.size(); ++i) {
        auto& index = tab_.indexes[i];
        auto ih = ihs_.at(get_ix_manager()->get_index_name(tab_.name, index.cols)).get();
        #ifndef ENABLE_LOCK_CRABBING
            std::lock_guard<std::mutex> lg(ih->root_latch_);
        #endif
        char* key = new char[index.col_tot_len];
        int offset = 0;
        for(size_t i = 0; i < (size_t)index.col_num; ++i) {
            memcpy(key + offset, rec.data + index.cols[i].offset, index.cols[i].len);
            offset += index.cols[i].len;
        }
        ih->insert_entry(key, rid_, context_==nullptr? nullptr:context_->txn_);
        delete[] key;
    }
}


void SmManager::parse_csv(const std::string& file_name, const std::string& table_name, std::vector<std::vector<Value>>& ans, int record_size){
    std::ifstream ifs;
    ifs.open(file_name);
    std::string s;
    TabMeta &tab_meta = this->db_.tabs_.at(table_name);

    int file_size = disk_manager_->get_file_size(file_name);
    ans.reserve(file_size/record_size);

    bool tab_name_enable = false;
    while(std::getline(ifs, s)){
        std::vector<Value> row;
        row.reserve(tab_meta.cols.size());

        std::stringstream ss(s);
        std::string field;
        if(tab_name_enable == false){
            tab_name_enable = true;
            continue;
        }
        int ind = 0;
        while (std::getline(ss, field, ',')) {
            //parse string to Value
            Value v;
            ColType c_type = tab_meta.cols.at(ind).type;
            if(c_type == ColType::TYPE_INT){
                v.set_int(std::stoi(field));
            }
            else if(c_type == ColType::TYPE_FLOAT){
                v.set_float(std::stof(field));
            }
            else if(c_type == ColType::TYPE_BIGINT){
                v.set_bigint(std::stoll(field));
            }
            else if(c_type == ColType::TYPE_DATETIME){
                v.set_datetime(field);
            }
            else if(c_type == ColType::TYPE_STRING){
                v.set_str(field);
            }
            else{
                assert(0);
            }
            row.emplace_back(v);
            ind++;
        }

        ans.emplace_back(row);
    }
    ifs.close();
}

void SmManager::load_data(const std::string& table_name, const std::string& file_name){
    std::vector<std::vector<Value>> insert_values;
    TabMeta& tab_meta = this->db_.get_table(table_name);
    RmFileHandle* fh_ = this->fhs_.at(table_name).get();
    parse_csv(file_name, table_name, insert_values, fh_->get_record_size());
    Rid rid_{-1,-1};
    RmRecord buffer_load_record(fh_->get_file_hdr().record_size);
    RmPageHandle page_buffer = fh_ -> create_page_handle();//缓冲page
    for(size_t i = 0;i<insert_values.size();i++){
        do_pre_insert(fh_, insert_values[i], tab_meta, buffer_load_record, nullptr);
        //先插然后判断是否需要切换buffer
        rid_ = fh_->insert_record_for_load_data(buffer_load_record.data, page_buffer);
        insert_into_index(tab_meta, buffer_load_record, nullptr, rid_);
    }
    fh_->buffer_pool_manager_->unpin_page(page_buffer.page->get_page_id(), true);
}

void  SmManager::load_csv_itermodel(const std::string& file_name, const std::string& table_name){
    //begin
    std::ifstream ifs;
    ifs.open(file_name);
    std::string s;
    TabMeta &tab_meta = this->db_.tabs_.at(table_name);

    bool tab_name_enable = false;
    Rid rid_{-1,-1};
    RmFileHandle* fh_ = this->fhs_.at(table_name).get();
    RmRecord buffer_load_record(fh_->get_file_hdr().record_size);
    RmPageHandle page_buffer = fh_ -> create_page_handle();//缓冲page
    //iter

    //初始化row
    std::vector<Value> row(tab_meta.cols.size());
    for(size_t i=0;i<tab_meta.cols.size();++i) {
        if(tab_meta.cols[i].type == ColType::TYPE_INT) {
            row[i].set_int(0);
        }
        else if(tab_meta.cols[i].type == ColType::TYPE_FLOAT) {
            row[i].set_float(0);
        }
        else if(tab_meta.cols[i].type == ColType::TYPE_BIGINT) {
            row[i].set_bigint(0);
        }
        else if(tab_meta.cols[i].type == ColType::TYPE_STRING) {
            row[i].set_str("");
        }
        else if(tab_meta.cols[i].type == ColType::TYPE_DATETIME) {
            row[i].set_datetime("1000-01-01 00:00:00");
        }
        else {
            assert(0);
        }
        row[i].init_raw(tab_meta.cols[i].len);
    }

    while(std::getline(ifs, s)){
        std::stringstream ss(s);
        std::string field;
        if(tab_name_enable == false){
            tab_name_enable = true;
            continue;
        }
        int ind = 0;
        while (std::getline(ss, field, ',')) {
            //parse string to Value
            if(row[ind].type == ColType::TYPE_INT){
                row[ind].set_int_val(std::stoi(field));
            }
            else if(row[ind].type == ColType::TYPE_FLOAT){
                row[ind].set_float_val(std::stof(field));
            }
            else if(row[ind].type == ColType::TYPE_BIGINT){
                row[ind].set_bigint_val(std::stoll(field));
            }
            else if(row[ind].type == ColType::TYPE_DATETIME){
                row[ind].set_datetime_val(field);
            }
            else if(row[ind].type == ColType::TYPE_STRING){
                row[ind].set_str_val(field);
            }
            else{
                assert(0);
            }
            row[ind].cover_raw(tab_meta.cols[ind].len);
            ind++;
        }
        load_pre_insert(fh_, row, tab_meta, buffer_load_record, nullptr);
        //先插然后判断是否需要切换buffer
        rid_ = fh_->insert_record_for_load_data(buffer_load_record.data, page_buffer);
        insert_into_index(tab_meta, buffer_load_record, nullptr, rid_);
    }
    fh_->buffer_pool_manager_->unpin_page(page_buffer.page->get_page_id(), true);
    ifs.close();
}

void SmManager::load_pre_insert(RmFileHandle* fh_, std::vector<Value>& values_, TabMeta& tab_, RmRecord& rec, Context* context_) {
    for (size_t i = 0; i < values_.size(); ++i) {
        auto &col = tab_.cols[i];
        auto &val = values_[i];
        if (col.type != val.type) {
            assert(0);
        }
        // if(col.type == TYPE_DATETIME) {
        //     if(!SmManager::check_datetime_(val.datetime_val)){
        //         throw InvalidValueError(val.datetime_val);
        //     }
        // }
        memcpy(rec.data + col.offset, val.raw->data, col.len);
    }
    // for(size_t i = 0; i < tab_.indexes.size(); ++i) {
    //     auto& index = tab_.indexes[i];
    //     auto ih = ihs_.at(get_ix_manager()->get_index_name(tab_.name, index.cols)).get();
    //     #ifndef ENABLE_LOCK_CRABBING
    //         std::lock_guard<std::mutex> lg(ih->root_latch_);
    //      #endif
    //     char* key = new char[index.col_tot_len];
    //     int offset = 0;
    //     for(size_t i = 0; i < (size_t)index.col_num; ++i) {
    //         memcpy(key + offset, rec.data + index.cols[i].offset, index.cols[i].len);
    //         offset += index.cols[i].len;
    //     }
    //     bool is_failed = ih->binary_search(key, context_).page_no != -1;
    //     if(is_failed){
    //         delete[] key;
    //         throw IndexInsertDuplicatedError();
    //     }
    //     delete[] key;
    // }
}