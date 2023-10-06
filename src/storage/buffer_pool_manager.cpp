/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "buffer_pool_manager.h"
#include "recovery/log_manager.h"
#define error() assert(0 && "Error in file buffer_pool_manager.")
/**
 * @description: 从free_list或replacer中得到可淘汰帧页的 *frame_id
 * @return {bool} true: 可替换帧查找成功 , false: 可替换帧查找失败
 * @param {frame_id_t*} frame_id 帧页id指针,返回成功找到的可替换帧id
 */
bool BufferPoolManager::find_victim_page(frame_id_t* frame_id) {
    // 1 使用BufferPoolManager::free_list_判断缓冲池是否已满需要淘汰页面
    // 1.1 未满获得frame
    // 1.2 已满使用lru_replacer中的方法选择淘汰页面
    //CHECK OK

    if(!free_list_.empty()) {
        *frame_id = free_list_.front();
        free_list_.erase(free_list_.begin());
        return true;
    }
    else {
        return replacer_->victim(frame_id);
    }
}

/**
 * @description: 更新页面数据, 如果为脏页则需写入磁盘，再更新为新页面，更新page元数据(data, is_dirty, page_id)和page table
 * @param {Page*} page 写回页指针
 * @param {PageId} new_page_id 新的page_id
 * @param {frame_id_t} new_frame_id 新的帧frame_id
 */
void BufferPoolManager::update_page(Page *page, PageId new_page_id, frame_id_t new_frame_id) {
    // 1 如果是脏页，写回磁盘，并且把dirty置为false
    // 2 更新page table
    // 3 重置page的data，更新page id
    //CHECK OK

    //1. 如果是脏页，写回磁盘，并且把dirty置为false
    if(page->is_dirty()) {
        //判断日志是否要落盘
        if(page->get_page_lsn() > log_manager_->get_persist_lsn_()) {
            #ifndef ENABLE_LSN
                assert(0);
            #endif
            log_manager_->flush_log_to_disk();
        }

        //仅脏页写回
        disk_manager_->write_page(page->get_page_id().fd, page->get_page_id().page_no, page->get_data(), PAGE_SIZE);
    }

    //2 更新page table
    assert(new_page_id.page_no != INVALID_PAGE_ID);
    // if(new_page_id.page_no != INVALID_PAGE_ID){
    //注意互斥关系 free_list, page_table, LRUlist_
    page_table_[new_page_id] = new_frame_id;
    // }
    page_table_.erase(page->get_page_id());
    
    //3 更新更多的元数据
    page->id_ = new_page_id;
    page->reset_data();
    page->set_dirty(false);
    page->set_page_lsn(INVALID_LSN);
    //pin_count不变.
}

/**
 * @description: 从buffer pool获取需要的页。
 *              如果页表中存在page_id（说明该page在缓冲池中），并且pin_count++。
 *              如果页表不存在page_id（说明该page在磁盘中），则找缓冲池victim page，将其替换为磁盘中读取的page，pin_count置1。
 * @return {Page*} 若获得了需要的页则将其返回，否则返回nullptr
 * @param {PageId} page_id 需要获取的页的PageId
 */
Page* BufferPoolManager::fetch_page(PageId page_id) {
    // 1.     从page_table_中搜寻目标页
    // 1.1    若目标页有被page_table_记录，则将其所在frame固定(pin)，并返回目标页。
    // 1.2    否则，尝试调用find_victim_page获得一个可用的frame，若失败则返回nullptr
    // 2.     若获得的可用frame存储的为dirty page，则须调用updata_page将page写回到磁盘
    // 3.     调用disk_manager_的read_page读取目标页到frame
    // 4.     固定目标页，更新pin_count_
    // 5.     返回目标页
    //Check OK

    std::lock_guard<std::mutex>lg(latch_);

    //1
    if(page_table_.find(page_id) == page_table_.end()){
        //从磁盘读
        frame_id_t frame_id;
        //1.2
        if(!find_victim_page(&frame_id)){
            //找不到替换页,返回nullptr.
            return nullptr;
        }
        else{
            Page& fpage = pages_[frame_id];

            //2. 消除冗余,updata_page自动判断dirty
            update_page(pages_ + frame_id, page_id, frame_id);
        
            //3 调用disk_manager_的read_page读取目标页到frame
            disk_manager_->read_page(page_id.fd, page_id.page_no, fpage.get_data(), PAGE_SIZE);

            //4 
            //除了pin_count之外的元数据已经在update_page中更新
            fpage.set_pin_count(1);
            replacer_->pin(frame_id);//有可能冗余，但是加上一定不会错

            //5.
            return pages_ + frame_id;
        }
        error();
    }
    else{
        //缓存读
        //1.1
        //4 && 5
        frame_id_t frame_id = page_table_[page_id];
        pages_[frame_id].add_pin_count();
        replacer_->pin(frame_id);
        return pages_ + frame_id;
    }
    error();
}

/**
 * @description: 取消固定pin_count>0的在缓冲池中的page
 * @return {bool} 如果目标页的pin_count<=0则返回false，否则返回true
 * @param {PageId} page_id 目标page的page_id
 * @param {bool} is_dirty 若目标page应该被标记为dirty则为true，否则为false
 */
bool BufferPoolManager::unpin_page(PageId page_id, bool is_dirty) {
    // 0. lock latch
    // 1. 尝试在page_table_中搜寻page_id对应的页P
    // 1.1 P在页表中不存在 return false
    // 1.2 P在页表中存在，获取其pin_count_
    // 2.1 若pin_count_已经等于0，则返回false
    // 2.2 若pin_count_大于0，则pin_count_自减一
    // 2.2.1 若自减后等于0，则调用replacer_的Unpin
    // 3 根据参数is_dirty，更改P的is_dirty_
    //Check OK

    std::lock_guard<std::mutex>lg(latch_);

    if(page_table_.find(page_id)!=page_table_.end()) {
        //1.2 缓冲中有此页
        frame_id_t frame_id = page_table_[page_id];
        Page& up_page = pages_[frame_id];
        if(up_page.get_pin_count()==0) {
            //2.1 该页已经Unpin, 无需再unpin
            return false;
        }
        else {
            //2.2
            if(up_page.get_pin_count()>0){
                up_page.set_pin_count(up_page.get_pin_count()-1);
            }
            else{
                //非法pin_count.
                error();
            }
            //2.3 
            if(up_page.get_pin_count() == 0){
                // 页面无线程占用
                replacer_->unpin(frame_id);
            }
            //3 如果为脏，改变脏位，否则不更新
            if(is_dirty){
                up_page.set_dirty(is_dirty);
            }
            return true;
        }
        error();
    }
    else {
        //1.1 缓存中无该页
        return false;
    }
    error();
}

/**
 * @description: 将目标页写回磁盘，不考虑当前页面是否正在被使用
 * @return {bool} 成功则返回true，否则返回false(只有page_table_中没有目标页时)
 * @param {PageId} page_id 目标页的page_id，不能为INVALID_PAGE_ID
 */
bool BufferPoolManager::flush_page(PageId page_id) {
    // 0. lock latch
    // 1. 查找页表,尝试获取目标页P
    // 1.1 目标页P没有被page_table_记录 ，返回false
    // 2. 无论P是否为脏都将其写回磁盘。
    // 3. 更新P的is_dirty_
    //CHECK OK

    std::lock_guard<std::mutex>lg(latch_);
    //1
    if(page_table_.find(page_id) == page_table_.end()) {
        //找不到目标页, 返回false.
        return false;
    }

    assert(page_id.page_no != INVALID_PAGE_ID && "Error: INVALID_PAGE_ID is in page_table_");
    frame_id_t frame_id = page_table_[page_id];
    Page& page = pages_[frame_id];

    //2
    //判断日志是否要落盘
    if(page.get_page_lsn() > log_manager_->get_persist_lsn_()) {
        #ifndef ENABLE_LSN
            assert(0);
        #endif
        log_manager_->flush_log_to_disk();
        
    }
    disk_manager_->write_page(page_id.fd, page_id.page_no, page.get_data(), PAGE_SIZE);

    //3
    page.set_dirty(false);

    return true;
}

/**
 * @description: 创建一个新的page，即从磁盘中移动一个新建的空page到缓冲池某个位置。
 * @return {Page*} 返回新创建的page，若创建失败则返回nullptr
 * @param {PageId*} page_id 当成功创建一个新的page时存储其page_id
 */
Page* BufferPoolManager::new_page(PageId* page_id) {
    // 1.   获得一个可用的frame，若无法获得则返回nullptr
    // 2.   在fd对应的文件分配一个新的page_id
    // 3.   将frame的数据写回磁盘
    // 4.   固定frame，更新pin_count_
    // 5.   返回获得的page
    //CHECK OK

    std::lock_guard<std::mutex>lg(latch_);

    //1. 得到可用的frame
    frame_id_t* new_frame_id = new frame_id_t(-1);//初始化为非法值更好
    if(!find_victim_page(new_frame_id)) {
        return nullptr;
    }


    //2. 在fd对应的文件分配一个新的page_id
    page_id->page_no =  disk_manager_->allocate_page(page_id->fd);
    Page* new_page = pages_ + (*new_frame_id);
    
    //3. 调用update_page换出缓存
    update_page(new_page, *page_id, *new_frame_id);
    //new_page后立刻将新数据写回到磁盘
    // disk_manager_->write_page(new_page->get_page_id().fd, new_page->get_page_id().page_no, new_page->get_data(), PAGE_SIZE);

    //4. 固定frame，更新pin_count_
    //new_page 其他元数据更新和update_page一致,无须重复。
    new_page->set_dirty(true); //换出时需要将新页的page_hdr刷回磁盘
    new_page->set_pin_count(1);
    replacer_->pin(*new_frame_id);//有可能冗余，但是加上一定不会错
    
    // 5. 返回获得的page
    delete new_frame_id;
    return new_page;
}

/**
 * @description: 从buffer_pool删除目标页
 * @return {bool} 如果目标页不存在于buffer_pool或者成功被删除则返回true，若其存在于buffer_pool但无法删除则返回false
 * @param {PageId} page_id 目标页
 */
bool BufferPoolManager::delete_page(PageId page_id) {
    // 1.   在page_table_中查找目标页，若不存在返回true
    // 2.   若目标页的pin_count不为0，则返回false
    // 3.   将目标页数据写回磁盘，从页表中删除目标页，重置其元数据，将其加入free_list_，返回true
    //CHECK OK

    std::lock_guard<std::mutex>lg(latch_);

    //1
    if(page_table_.find(page_id) == page_table_.end()) {
        return true;
    }

    //2
    frame_id_t frame_id = page_table_[page_id];
    if(pages_[frame_id].get_pin_count() > 0) {
        return false;
    }
    assert(pages_[frame_id].get_pin_count()==0 && "Invalid pin_count(!=0)");
    
    //3
    disk_manager_->deallocate_page(page_id.page_no);
    //重置page元数据
    pages_[frame_id].set_page_lsn(INVALID_LSN);
    pages_[frame_id].reset_data();
    pages_[frame_id].reset_page_id();
    pages_[frame_id].set_dirty(false);
    //pin_cout已经为0.

    //维护互斥性,只能存在于free_list中
    page_table_.erase(page_id);
    replacer_->pin(frame_id);
    free_list_.emplace_back(frame_id);

    return true;
}

/**
 * @description: 将buffer_pool中的所有页写回到磁盘
 * @param {int} fd 文件句柄
 */
void BufferPoolManager::flush_all_pages(int fd) {
    //CHECK OK.
    std::lock_guard<std::mutex>lg(latch_);
    for (size_t i = 0; i < pool_size_; ++i) {
        Page *page = &pages_[i];
        //仅删除fd下的页
        if (page->get_page_id().fd == fd && page->get_page_id().page_no != INVALID_PAGE_ID) {

            //判断是否要落盘日志
            if(page->get_page_lsn() > log_manager_->get_persist_lsn_()) {
                #ifndef ENABLE_LSN
                    assert(0);
                #endif
                log_manager_->flush_log_to_disk();
            }

            disk_manager_->write_page(page->get_page_id().fd, page->get_page_id().page_no, page->get_data(), PAGE_SIZE);
            page->is_dirty_ = false;
        }
    }
}