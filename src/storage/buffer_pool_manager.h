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
#include <fcntl.h>
#include <unistd.h>

#include <cassert>
#include <list>
#include <unordered_map>
#include <vector>
#include <map>

#include "disk_manager.h"
#include "errors.h"
#include "page.h"
#include "replacer/lru_replacer.h"
#include "replacer/replacer.h"

// #define ENABLE_LSN
 
class LogManager;

class BufferPoolManager {
   private:
    size_t pool_size_;      // buffer_pool中可容纳页面的个数，即帧的个数
    Page *pages_;           // buffer_pool中的Page对象数组，在构造空间中申请内存空间，在析构函数中释放，大小为BUFFER_POOL_SIZE
    std::unordered_map<PageId, frame_id_t, PageIdHash> page_table_; // 帧号和页面号的映射哈希表，用于根据页面的PageId定位该页面的帧编号
    std::list<frame_id_t> free_list_;   // 空闲帧编号的链表
    DiskManager *disk_manager_;
    Replacer *replacer_;    // buffer_pool的置换策略，当前赛题中为LRU置换策略
    std::mutex latch_;      // 用于共享数据结构的并发控制

    LogManager *log_manager_;

   public:
    BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
        : pool_size_(pool_size), disk_manager_(disk_manager){
        // 为buffer pool分配一块连续的内存空间
        pages_ = new Page[pool_size_];
        // 可以被Replacer改变
        if (REPLACER_TYPE.compare("LRU"))
            replacer_ = new LRUReplacer(pool_size_);
        else if (REPLACER_TYPE.compare("CLOCK"))
            replacer_ = new LRUReplacer(pool_size_);
        else {
            replacer_ = new LRUReplacer(pool_size_);
        }
        // 初始化时，所有的page都在free_list_中
        for (size_t i = 0; i < pool_size_; ++i) {
            free_list_.emplace_back(static_cast<frame_id_t>(i));  // static_cast转换数据类型
        }
    }
    BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
        : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
        // 为buffer pool分配一块连续的内存空间
        pages_ = new Page[pool_size_];
        // 可以被Replacer改变
        if (REPLACER_TYPE.compare("LRU"))
            replacer_ = new LRUReplacer(pool_size_);
        else if (REPLACER_TYPE.compare("CLOCK"))
            replacer_ = new LRUReplacer(pool_size_);
        else {
            replacer_ = new LRUReplacer(pool_size_);
        }
        // 初始化时，所有的page都在free_list_中
        for (size_t i = 0; i < pool_size_; ++i) {
            free_list_.emplace_back(static_cast<frame_id_t>(i));  // static_cast转换数据类型
        }
    }

    ~BufferPoolManager() {
        delete[] pages_;
        delete replacer_;
    }

    LogManager * get_log_manager() {
        return log_manager_;
    }

    /**
     * @description: 将目标页面标记为脏页
     * @param {Page*} page 脏页
     */
    static void mark_dirty(Page* page) { page->is_dirty_ = true; }

   public: 
    Page* fetch_page(PageId page_id);

    bool unpin_page(PageId page_id, bool is_dirty);

    bool flush_page(PageId page_id);

    Page* new_page(PageId* page_id);

    bool delete_page(PageId page_id);

    void flush_all_pages(int fd);

    std::map<PageId, int> get_pin_count() {
        std::map<PageId, int> id_pin;
        for(size_t i = 0; i < (size_t)pool_size_; i++) {
            if(pages_[i].get_page_id().page_no != -1) {
                id_pin[pages_[i].get_page_id()] = pages_[i].get_pin_count();
            }
        }
        return id_pin;
    }

   private:
    bool find_victim_page(frame_id_t* frame_id);

    void update_page(Page* page, PageId new_page_id, frame_id_t new_frame_id);
};

//RALL 
class unpin_page_guard{
    private:
        PageId p_id;
        bool is_dirty;
        BufferPoolManager* buffer_pool_manager;
    public:
    unpin_page_guard(PageId p_id_, bool is_dirty_, BufferPoolManager* buffer_pool_manager_)
    : p_id(p_id_), is_dirty(is_dirty_), buffer_pool_manager(buffer_pool_manager_)
    {

    }
    ~unpin_page_guard(){
        buffer_pool_manager->unpin_page(p_id, is_dirty);// TODO: 区分true和false, 减少IO访问次数
    }
};