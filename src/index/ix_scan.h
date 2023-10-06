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

#include "ix_defs.h"
#include "ix_index_handle.h"

// 用于遍历叶子结点
// 用于直接遍历叶子结点，而不用findleafpage来得到叶子结点
// TODO：对page遍历时，要加上读锁
class IxScan : public RecScan {
    IxIndexHandle *ih_;
    Iid iid_;  // 初始为lower（用于遍历的指针）
    Iid end_;  // 初始为upper
    Context* context_;
    IxNodeHandle* node_buffer;

   public:
    txn_id_t txn_id;
    IxScan(IxIndexHandle *ih, const Iid &lower, const Iid &upper, Context* context)
        : ih_(ih), iid_(lower), end_(upper), context_(context){
        if(iid_.page_no!=-1){
            node_buffer = ih_ -> fetch_node(iid_.page_no);
        }
        else{
            node_buffer = nullptr;
        }
    }

    void next() override;

    bool is_end() const override { 
        return iid_ == end_; 
    }

    Rid rid() const override;

    const Iid &iid() const { return iid_; }

    void update_node_buffer(page_id_t page_no);

    ~IxScan(){
        update_node_buffer(INVALID_PAGE_ID);
    }

    void reload(const Iid &lower, const Iid &upper){
        update_node_buffer(INVALID_PAGE_ID);
        iid_ = lower;
        end_ = upper;
        node_buffer = ih_ -> fetch_node(iid_.page_no);
    }
};