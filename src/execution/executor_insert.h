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
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class InsertExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;                   // 表的元数据
    std::vector<Value> values_;     // 需要插入的数据
    RmFileHandle *fh_;              // 表的数据文件句柄
    std::string tab_name_;          // 表名称
    Rid rid_;                       // 插入的位置，由于系统默认插入时不指定位置，因此当前rid_在插入后才赋值
    SmManager *sm_manager_;

   public:
    InsertExecutor(SmManager *sm_manager, const std::string &tab_name, const std::vector<Value>& values, Context *context) 
        :tab_(sm_manager->db_.get_table(tab_name)),
        values_(values),
        fh_(sm_manager->fhs_.at(tab_name).get()),
        tab_name_(tab_name),
        sm_manager_(sm_manager)
    {
        if (values.size() != tab_.cols.size()) {
            throw InvalidValueCountError();
        }
        context_ = context;
    };

    std::unique_ptr<RmRecord> Next() override {
        sm_manager_ -> insert_rec(this-> fh_, this->values_, this->tab_, this->context_, this->rid_);
        return nullptr;
    }
    Rid &rid() override { return rid_; }
};