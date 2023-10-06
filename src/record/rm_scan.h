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

#include "rm_defs.h"
#include "rm_file_handle.h"

class RmScan : public RecScan {
    const RmFileHandle *file_handle_;
    Rid rid_;
    //缓冲pageHandle
    RmPageHandle page_buffer;
    Context* context_;
public:
    RmScan(const RmFileHandle *file_handle, Context* context);

    void next() override;

    bool is_end() const override;

    std::unique_ptr<RmRecord> get_now_record();

    std::unique_ptr<RmRecord> get_now_record(bool is_add_lock);

    Rid rid() const override;

    void update_page_buffer(page_id_t page_no);

    ~RmScan(){
        update_page_buffer(INVALID_PAGE_ID);
    }
};
