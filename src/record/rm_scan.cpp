/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "rm_scan.h"

/**
 * @brief 初始化file_handle和rid
 * @param file_handle
 */
RmScan::RmScan(const RmFileHandle *file_handle, Context* context) : file_handle_(file_handle), page_buffer(&(file_handle_->file_hdr_),(Page*)nullptr), context_(context){
    // Todo:
    // 初始化file_handle和rid（指向第一个存放了记录的位置）
    const int MAX_RECORD_SIZE =  file_handle_->file_hdr_.num_records_per_page;
    bool is_all_empty = true;
    for(int page_no = 1; page_no<file_handle->file_hdr_.num_pages;page_no++){
        RmPageHandle tmp_page_hd = file_handle->fetch_page_handle(page_no);
        unpin_page_guard upg({file_handle_->fd_, page_no}, false, file_handle->buffer_pool_manager_);
        int ind = Bitmap::next_bit(1, tmp_page_hd.bitmap, MAX_RECORD_SIZE, -1);
        if(ind==MAX_RECORD_SIZE){
            //DoNothing
        }
        else{
            rid_.page_no = page_no;
            rid_.slot_no = ind;
            is_all_empty = false;
            break;
        }
    }
    if(is_all_empty){
        // std::cout<<"RmScan::RmScan all empty.\n";
        rid_.page_no = INVALID_PAGE_ID;
    }
    else{
        //初始化缓冲页
        this-> page_buffer = file_handle->fetch_page_handle(rid_.page_no);
    }
}

/**
 * @brief 找到文件中下一个存放了记录的位置
 */
void RmScan::next() {
    // Todo:
    // 找到文件中下一个存放了记录的非空闲位置，用rid_来指向这个位置

    const int MAX_RECORD_SIZE =  file_handle_->file_hdr_.num_records_per_page;
    bool is_all_empty = true;
    if(rid_.page_no == INVALID_PAGE_ID){
        assert(0 && "ERROR RmScan::next invalid page");
    }
    int tmp_slot_no = rid_.slot_no;
    for(int page_no = rid_.page_no; page_no<file_handle_->file_hdr_.num_pages;page_no++){
        update_page_buffer(page_no);
        int ind = Bitmap::next_bit(1, page_buffer.bitmap, MAX_RECORD_SIZE, tmp_slot_no);
        if(ind==MAX_RECORD_SIZE){
            tmp_slot_no = -1;
        }
        else{
            rid_.page_no = page_no;
            rid_.slot_no = ind;
            is_all_empty = false;
            break;
        }
    }

    if(is_all_empty){
        rid_.page_no = INVALID_PAGE_ID;
        update_page_buffer(rid_.page_no);
    }
}

/**
 * @brief ​ 判断是否到达文件末尾
 */
bool RmScan::is_end() const {
    return rid_.page_no == INVALID_PAGE_ID;
}

std::unique_ptr<RmRecord> RmScan::get_now_record(){
    assert(!is_end());
    assert(page_buffer.page != nullptr);
    if(context_ != nullptr) {
        context_->lock_mgr_->lock_shared_on_record(context_->txn_, rid_, file_handle_->fd_);
    }
    assert(file_handle_->is_record(rid_));
    return std::make_unique<RmRecord>(file_handle_->file_hdr_.record_size, page_buffer.get_slot(rid_.slot_no));
}

std::unique_ptr<RmRecord> RmScan::get_now_record(bool is_add_lock) {
    assert(!is_end());
    assert(page_buffer.page != nullptr);

    if(is_add_lock && context_ != nullptr) {
        context_->lock_mgr_->lock_shared_on_record(context_->txn_, rid_, file_handle_->fd_);
    }

    assert(file_handle_->is_record(rid_));
    return std::make_unique<RmRecord>(file_handle_->file_hdr_.record_size, page_buffer.get_slot(rid_.slot_no));
}

/**
 * @brief RmScan内部存放的rid
 */
Rid RmScan::rid() const {
    return rid_;
}

void RmScan::update_page_buffer(page_id_t page_no){
    if(page_buffer.page != nullptr && page_no != page_buffer.page->get_page_id().page_no){
        file_handle_->buffer_pool_manager_->unpin_page(page_buffer.page->get_page_id(), false);
    }
    if((page_buffer.page == nullptr || page_no != page_buffer.page->get_page_id().page_no) && page_no != INVALID_PAGE_ID){
        page_buffer = file_handle_->fetch_page_handle(page_no);
    }
}