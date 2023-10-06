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

#include <mutex> 
#include <shared_mutex>


class RWLatch {
    public:

    void lock_write() { mutex_.lock(); }


    void unlock_write() { mutex_.unlock(); }


    void lock_read() { mutex_.lock_shared(); }


    void unlock_read() { mutex_.unlock_shared(); }

    private:
    std::shared_mutex mutex_;
};
