/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "rm_file_handle.h"
#include <assert.h>

/**
 * @description: 获取当前表中记录号为rid的记录
 * @param {Rid&} rid 记录号，指定记录的位置
 * @param {Context*} context
 * @return {unique_ptr<RmRecord>} rid对应的记录对象指针
 */
std::unique_ptr<RmRecord> RmFileHandle::get_record(const Rid& rid, Context* context) const {
    // Todo:
    // 1. 获取指定记录所在的page handle
    // 2. 初始化一个指向RmRecord的指针（赋值其内部的data和size）

    // 处理事务, 加共享锁
    if(context != nullptr) {
        context->lock_mgr_->lock_shared_on_record(context->txn_, rid, fd_);
    }

    //1
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    
    //2
    char* data = page_handle.get_slot(rid.slot_no);
    int record_size = page_handle.file_hdr->record_size;
    //参考rmdb.cpp 使用make_unique而不是new.
    std::unique_ptr<RmRecord> ret_ptr = std::make_unique<RmRecord>(record_size, data);
    return ret_ptr;
}

/**
 * @description: 获取当前表中记录号为rid的记录
 * @param {Rid&} rid 记录号，指定记录的位置
 * @param {Context*} context
 * @return {unique_ptr<RmRecord>} rid对应的记录对象指针
 */
std::unique_ptr<RmRecord> RmFileHandle::get_record(const Rid& rid, bool is_add_lock, Context* context) const {
    // Todo:
    // 1. 获取指定记录所在的page handle
    // 2. 初始化一个指向RmRecord的指针（赋值其内部的data和size）

    // 处理事务, 加共享锁
    if(is_add_lock && context != nullptr) {
        context->lock_mgr_->lock_shared_on_record(context->txn_, rid, fd_);
    }

    //1
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    
    //2
    char* data = page_handle.get_slot(rid.slot_no);
    int record_size = page_handle.file_hdr->record_size;
    //参考rmdb.cpp 使用make_unique而不是new.
    std::unique_ptr<RmRecord> ret_ptr = std::make_unique<RmRecord>(record_size, data);
    return ret_ptr;
}

/**
 * @description: 在当前表中插入一条记录，不指定插入位置
 * @param {char*} buf 要插入的记录的数据
 * @param {Context*} context
 * @return {Rid} 插入的记录的记录号（位置）
 */
Rid RmFileHandle::insert_record(char* buf, Context* context_) {
    // 1. 获取当前未满的page handle
    // 2. 在page handle中找到空闲slot位置
    // 3. 将buf复制到空闲slot位置
    // 4. 更新page_handle.page_hdr中的数据结构
    // 注意考虑插入一条记录后页面已满的情况，需要更新file_hdr_.first_free_page_no

    //1
    std::lock_guard<std::mutex> lg(this->latch_);
    RmPageHandle free_page_handle = create_page_handle();
    page_id_t ret_page_no = free_page_handle.page->get_page_id().page_no;
    int free_slot_no = Bitmap::first_bit(0, free_page_handle.bitmap, file_hdr_.num_records_per_page);

    // 插入前先加互斥锁
    if(context_ != nullptr) {
        context_->lock_mgr_->lock_exclusive_on_record(context_->txn_, Rid({ret_page_no, free_slot_no}), fd_);
    }
    
    char* des = free_page_handle.get_slot(free_slot_no);

    memcpy(des,  buf, file_hdr_.record_size);
    Bitmap::set(free_page_handle.bitmap, free_slot_no);
    free_page_handle.page_hdr->num_records++;
    if(free_page_handle.page_hdr->num_records == file_hdr_.num_records_per_page){
        free_pageno_set.erase(ret_page_no);
    }

    Rid ret =  Rid{ret_page_no, free_slot_no};

    if(context_ != nullptr) {
        //事务管理
        RmRecord ins_rec(file_hdr_.record_size, buf);
        WriteRecord *wr_rec = new WriteRecord(WType::INSERT_TUPLE, disk_manager_->get_file_name(fd_), ret, ins_rec);
        context_->txn_->append_write_record(wr_rec);

        //日志管理
        context_->log_mgr_->add_insert_log_record(context_->txn_->get_transaction_id(), ins_rec, ret, disk_manager_->get_file_name(fd_));
    }
    buffer_pool_manager_->unpin_page(free_page_handle.page->get_page_id(), true);
    return ret;
}

/**
 * @description: 在当前表中的指定位置插入一条记录
 * @param {Rid&} rid 要插入记录的位置
 * @param {char*} buf 要插入记录的数据
 */
void RmFileHandle::insert_record(Rid& rid, char* buf) {
    std::lock_guard<std::mutex> lg(this->latch_);
    RmPageHandle insert_page_handle = fetch_page_handle(rid.page_no);
    char* bitmap = insert_page_handle.bitmap;
    if(Bitmap::is_set(bitmap,rid.slot_no)){
        assert(0 && "ERROR RmFileHandle::insert_record this slot is already set.");
    }
    else{
        char* des = insert_page_handle.get_slot(rid.slot_no);
        memcpy(des, buf, file_hdr_.record_size);
        Bitmap::set(bitmap,rid.slot_no);
        insert_page_handle.page_hdr->num_records++;
    }
    if(insert_page_handle.page_hdr->num_records == file_hdr_.num_records_per_page){
        free_pageno_set.erase(rid.page_no);
    }
    buffer_pool_manager_->unpin_page(insert_page_handle.page->get_page_id(), true);
}

Rid RmFileHandle::insert_record_for_load_data(char* buf, RmPageHandle& page_buffer){
    //1
    page_id_t ret_page_no = page_buffer.page->get_page_id().page_no;
    int free_slot_no = Bitmap::first_bit(0, page_buffer.bitmap, file_hdr_.num_records_per_page);

    char* des = page_buffer.get_slot(free_slot_no);

    memcpy(des,  buf, file_hdr_.record_size);
    Bitmap::set(page_buffer.bitmap, free_slot_no);
    page_buffer.page_hdr->num_records++;

    if(page_buffer.page_hdr->num_records == get_file_hdr().num_records_per_page){
        //unpin并切换page
        free_pageno_set.erase(ret_page_no);
        buffer_pool_manager_->unpin_page(page_buffer.page->get_page_id(), true);
        page_buffer = create_page_handle();
    }

    return {ret_page_no, free_slot_no};
}

void RmFileHandle::allocpage(Rid& rid){
    while(rid.page_no >= get_file_hdr().num_pages){
        RmPageHandle rm_page_hd = create_new_page_handle();
        free_pageno_set.insert(rm_page_hd.page->get_page_id().page_no);
        buffer_pool_manager_->unpin_page(rm_page_hd.page->get_page_id(), false);
    }
}

/**
 * @description: 删除记录文件中记录号为rid的记录
 * @param {Rid&} rid 要删除的记录的记录号（位置）
 * @param {Context*} context
 */
void RmFileHandle::delete_record(Rid& rid, Context* context_) {
    // 1. 获取指定记录所在的page handle
    // 2. 更新page_handle.page_hdr中的数据结构
    // 注意考虑删除一条记录后页面未满的情况，需要调用release_page_handle()
    std::lock_guard<std::mutex> lg(this->latch_);
    RmPageHandle delete_page_handle = fetch_page_handle(rid.page_no);
    char* bitmap = delete_page_handle.bitmap;
    if(Bitmap::is_set(bitmap,rid.slot_no)){

        //找到了记录，事务才记录删除操作
        //向get_record传递context
        if(context_ != nullptr) {
            //加互斥锁
            auto rec = get_record(rid, context_);
            context_->lock_mgr_->lock_exclusive_on_record(context_->txn_, rid, fd_);
            WriteRecord *wr_rec = new WriteRecord(WType::DELETE_TUPLE, disk_manager_->get_file_name(fd_), rid, *rec);
            context_->txn_->append_write_record(wr_rec);

            //日志管理
            context_->log_mgr_->add_delete_log_record(context_->txn_->get_transaction_id(), *rec, rid, disk_manager_->get_file_name(fd_));
            buffer_pool_manager_->unpin_page(delete_page_handle.page->get_page_id(), true);
        }

        Bitmap::reset(bitmap,rid.slot_no);
        delete_page_handle.page_hdr->num_records--;
        free_pageno_set.insert(rid.page_no);
    }
    else{
        assert(0 && "ERROR RmFileHandle::delete_record this slot is unset.");
    }
    buffer_pool_manager_->unpin_page(delete_page_handle.page->get_page_id(), true);
}


/**
 * @description: 更新记录文件中记录号为rid的记录
 * @param {Rid&} rid 要更新的记录的记录号（位置）
 * @param {char*} buf 新记录的数据
 * @param {Context*} context
 */
void RmFileHandle::update_record(Rid& rid, char* buf, Context* context_) {
    // 1. 获取指定记录所在的page handle
    // 2. 更新记录

    RmPageHandle update_page_handle = fetch_page_handle(rid.page_no);
    char* bitmap = update_page_handle.bitmap;
    
    if(Bitmap::is_set(bitmap, rid.slot_no)){

        //找到了记录，事务才记录更新操作
        //向get_record传递context
        if(context_ != nullptr) {
            //加互斥锁
            auto old_rec = get_record(rid, context_);
            context_->lock_mgr_->lock_exclusive_on_record(context_->txn_, rid, fd_);
            WriteRecord *wr_rec = new WriteRecord(WType::UPDATE_TUPLE, disk_manager_->get_file_name(fd_), rid, *old_rec);
            context_->txn_->append_write_record(wr_rec);

            //日志处理
            RmRecord update_record(file_hdr_.record_size);
            memcpy(update_record.data, buf, update_record.size);
            context_->log_mgr_->add_update_log_record(context_->txn_->get_transaction_id(), update_record, *old_rec, rid, disk_manager_->get_file_name(fd_));
            #ifdef ENABLE_LSN
                update_page_handle.page->set_page_lsn(new_lsn);
            #endif
            buffer_pool_manager_->unpin_page(update_page_handle.page->get_page_id(), true);
        }

        char* des = update_page_handle.get_slot(rid.slot_no);
        memcpy(des, buf, file_hdr_.record_size);
    }
    else{
        //更新错误
        assert(0 && "ERROR RmFileHandle::update_record cannot update invalid slot!");
    }
    buffer_pool_manager_->unpin_page(update_page_handle.page->get_page_id(), true);
}

/**
 * 以下函数为辅助函数，仅提供参考，可以选择完成如下函数，也可以删除如下函数，在单元测试中不涉及如下函数接口的直接调用
*/
/**
 * @description: 获取指定页面的页面句柄
 * @param {int} page_no 页面号
 * @return {RmPageHandle} 指定页面的句柄
 */


RmPageHandle RmFileHandle::fetch_page_handle(int page_no) const {
    // 使用缓冲池获取指定页面，并生成page_handle返回给上层
    // if page_no is invalid, throw PageNotExistError exception
    if(page_no == INVALID_PAGE_ID){
        //Problem Table name
        std::string table_name = "";
        throw PageNotExistError(table_name, page_no);
    }
    Page* page_ = buffer_pool_manager_->fetch_page({fd_, page_no});
    return RmPageHandle(&file_hdr_, page_);
}



/**
 * @description: 创建一个新的page handle
 * @return {RmPageHandle} 新的PageHandle
 */
RmPageHandle RmFileHandle::create_new_page_handle() {
    // 1.使用缓冲池来创建一个新page
    // 2.更新page handle中的相关信息
    // 3.更新file_hdr_

    //1
    Page* new_page = buffer_pool_manager_->new_page(&tmp_new_page_id);

    //3
    file_hdr_.num_pages++;

    //2
    return RmPageHandle(&file_hdr_, new_page);
}

/**
 * @brief 创建或获取一个空闲的page handle
 *
 * @return RmPageHandle 返回生成的空闲page handle
 * @note pin the page, remember to unpin it outside!
 */
RmPageHandle RmFileHandle::create_page_handle() {
    // Todo:
    // 1. 判断file_hdr_中是否还有空闲页
    //     1.1 没有空闲页：使用缓冲池来创建一个新page；可直接调用create_new_page_handle()
    //     1.2 有空闲页：直接获取第一个空闲页
    // 2. 生成page handle并返回给上层

    //1 
    if(free_pageno_set.empty()){
        RmPageHandle new_page =  create_new_page_handle();
        free_pageno_set.insert(new_page.page->get_page_id().page_no);
        return new_page;
    }
    else{
        return fetch_page_handle(*free_pageno_set.begin());
    }
}

/**
 * @description: 当一个页面从没有空闲空间的状态变为有空闲空间状态时，更新文件头和页头中空闲页面相关的元数据
 */
void RmFileHandle::release_page_handle(RmPageHandle&page_handle) {
    // Todo: 
    // 当page从已满变成未满，考虑如何更新：
    // 1. page_handle.page_hdr->next_free_page_no
    // 2. file_hdr_.first_free_page_no
    
}

void RmFileHandle::update_record(Rid& rid, char* buf, bool is_add_lock, Context* context_) {
    // 1. 获取指定记录所在的page handle
    // 2. 更新记录

    RmPageHandle update_page_handle = fetch_page_handle(rid.page_no);
    char* bitmap = update_page_handle.bitmap;
    
    if(Bitmap::is_set(bitmap, rid.slot_no)){

        //找到了记录，事务才记录更新操作
        //向get_record传递context
        if(context_ != nullptr) {
            //加互斥锁
            auto old_rec = get_record(rid, context_);
            if(is_add_lock) {
                context_->lock_mgr_->lock_exclusive_on_record(context_->txn_, rid, fd_);
            }
            WriteRecord *wr_rec = new WriteRecord(WType::UPDATE_TUPLE, disk_manager_->get_file_name(fd_), rid, *old_rec);
            context_->txn_->append_write_record(wr_rec);

            //日志处理
            RmRecord update_record(file_hdr_.record_size);
            memcpy(update_record.data, buf, update_record.size);
            if(is_add_lock){
                context_->log_mgr_->add_update_log_record(context_->txn_->get_transaction_id(), update_record, *old_rec, rid, disk_manager_->get_file_name(fd_));
            }
            buffer_pool_manager_->unpin_page(update_page_handle.page->get_page_id(), true);
        }

        char* des = update_page_handle.get_slot(rid.slot_no);
        memcpy(des, buf, file_hdr_.record_size);
    }
    else{
        //更新错误
        assert(0 && "ERROR RmFileHandle::update_record cannot update invalid slot!");
    }
    buffer_pool_manager_->unpin_page(update_page_handle.page->get_page_id(), true);
}

void RmFileHandle::delete_record(Rid& rid, bool is_add_lock, Context* context_) {
    // 1. 获取指定记录所在的page handle
    // 2. 更新page_handle.page_hdr中的数据结构
    // 注意考虑删除一条记录后页面未满的情况，需要调用release_page_handle()
    std::lock_guard<std::mutex> lg(this->latch_);
    RmPageHandle delete_page_handle = fetch_page_handle(rid.page_no);
    char* bitmap = delete_page_handle.bitmap;
    if(Bitmap::is_set(bitmap,rid.slot_no)){

        //找到了记录，事务才记录删除操作
        //向get_record传递context
        if(context_ != nullptr) {
            //加互斥锁
            auto rec = get_record(rid, context_);
            if(is_add_lock) {
                context_->lock_mgr_->lock_exclusive_on_record(context_->txn_, rid, fd_);
            }
            WriteRecord *wr_rec = new WriteRecord(WType::DELETE_TUPLE, disk_manager_->get_file_name(fd_), rid, *rec);
            context_->txn_->append_write_record(wr_rec);

            //日志管理
            context_->log_mgr_->add_delete_log_record(context_->txn_->get_transaction_id(), *rec, rid, disk_manager_->get_file_name(fd_));
            buffer_pool_manager_->unpin_page(delete_page_handle.page->get_page_id(), true);
        }

        Bitmap::reset(bitmap,rid.slot_no);
        delete_page_handle.page_hdr->num_records--;
        free_pageno_set.insert(rid.page_no);
    }
    else{
        assert(0 && "ERROR RmFileHandle::delete_record this slot is unset.");
    }
    buffer_pool_manager_->unpin_page(delete_page_handle.page->get_page_id(), true);
}