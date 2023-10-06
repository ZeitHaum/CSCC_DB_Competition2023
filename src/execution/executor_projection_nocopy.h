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

class ProjectionNocopyExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;        // 投影节点的儿子节点
    std::vector<ColMeta> cols_;                     // 需要投影的字段
    size_t len_;                                    // 投影后的字段总长度

   public:
    ProjectionNocopyExecutor(std::unique_ptr<AbstractExecutor> &prev, const std::vector<TabCol> &sel_cols) 
    : prev_(std::move(prev)){
        // prev_ = std::move(prev);
        size_t curr_len = 0;
        for (auto &sel_col : sel_cols) {
            auto pos = prev_->get_col(sel_col);
            auto& col = *pos;
            curr_len += col.len;
            cols_.emplace_back(col);
        }
        len_ = curr_len;
        init_hash_cols();
    }

    void beginTuple() override {
        // 递归调用子结点对应函数，定位
        prev_->beginTuple();
    }

    void nextTuple() override {
        // 递归调用子结点对应函数，定位
        if(is_end()){
            assert(0 && "Error nextTuple(): already reached end(), no next.");
        }
        prev_->nextTuple();
    }

    bool is_end() const override { 
        // 取值和子结点对应函数相等
        return prev_->is_end(); 
    }


    std::unique_ptr<RmRecord> Next() override {
        // 投影操作 return Pi_{proj_rec}(tuple)
        // 1. 获取(创建) 投影算子需要的输入和输出
        // 2. 投影并返回(调用私有函数成员project(Newed by wsh))
        return std::move(prev_->Next());
    }

    Rid &rid() override { return _abstract_rid; }

    const std::vector<ColMeta> &cols() const {
        return this->cols_;
    };

    size_t tupleLen() const { 
        return this->len_; 
    }

    std::string getType() { return "ProjectionExecutor"; };

    ColMeta get_col_offset(const TabCol &target) override{ 
        for(auto iter = this->cols_.begin(); iter!=this->cols_.end(); iter++){
            if(iter->tab_name == target.tab_name && iter->name == target.col_name){
                return *iter;
            }
        }
        return ColMeta();
    };
};