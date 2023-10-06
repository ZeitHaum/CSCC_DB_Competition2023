/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "ix_scan.h"

/**
 * @brief 
 * @todo 加上读锁（需要使用缓冲池得到page）
 */
void IxScan::next() {
    assert(!is_end());
    update_node_buffer(iid_.page_no);
    assert(node_buffer->is_leaf_page());
    assert(iid_.slot_no < node_buffer->get_num_vals());
    // increment slot no
    iid_.slot_no++;
    if (iid_.page_no != ih_->file_hdr_->last_leaf_ && iid_.slot_no == node_buffer->get_num_vals()) {
        // go to next leaf
        iid_.slot_no = 0;
        iid_.page_no = node_buffer->get_next_leaf();
        update_node_buffer(iid_.page_no);
        //给next_leaf上锁
        #ifdef ENABLE_LOCK_CRABBING
            ih_->lock(Operation::FIND, node_buffer->page);
            if(node_buffer->is_safe(Operation::FIND)){
                ih_ -> unlock_all_latch(context_->txn_);
            }
            context_->txn_->append_index_latch_page_set(node_buffer->page, static_cast<int>(Operation::FIND));
        #endif
    }
    if(is_end()){
        update_node_buffer(INVALID_PAGE_ID);
    }
}

Rid IxScan::rid() const {
    return *node_buffer->get_rid(iid_.slot_no);
}

void IxScan::update_node_buffer(page_id_t page_no){
    if(node_buffer != nullptr && page_no != node_buffer->get_page_no()){
        ih_ -> buffer_pool_manager_->unpin_page(node_buffer->get_page_id(), false);
        delete node_buffer;
        node_buffer = nullptr;
    }
    if((node_buffer == nullptr || page_no != node_buffer->get_page_no()) && page_no !=INVALID_PAGE_ID){
        node_buffer = ih_ -> fetch_node(page_no);
    }
}