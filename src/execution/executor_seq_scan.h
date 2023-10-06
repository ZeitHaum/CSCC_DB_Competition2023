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

class SeqScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;              // 表的名称
    std::vector<Condition> conds_;      // scan的条件
    RmFileHandle *fh_;                  // 表的数据文件句柄
    std::vector<ColMeta> cols_;         // scan后生成的记录的字段
    size_t len_;                        // scan后生成的每条记录的长度
    std::vector<Condition> fed_conds_;  // 同conds_，两个字段相同

    Rid rid_;
    std::unique_ptr<RmScan> scan_;     // table_iterator
    
    std::unique_ptr<RmRecord> record_buffer;

    SmManager *sm_manager_;

   public:
    SeqScanExecutor(SmManager *sm_manager, const std::string& tab_name, std::vector<Condition> conds, Context *context)
    : tab_name_(tab_name), conds_(std::move(conds)), fed_conds_(conds_), sm_manager_(sm_manager)
    {
        fh_ = sm_manager->fhs_[tab_name].get();
        cols_ = sm_manager->db_.get_table(tab_name).cols;
        len_ = cols_.back().offset + cols_.back().len;
        context_ = context;
        init_hash_cols();
    }

    void beginTuple() override {
        // 0.给表上锁
        // 1.检查条件中所有字段的表和算子的表是否匹配
        // 2.得到第一个满足fed_conds_条件的record,并把其rid赋给算子成员rid_，通过while
            //2.1 处理 RecordNotFoundError
        // 3.判断谓词是否满足，满足终止循环
        check_runtime_conds();
        context_->lock_mgr_->lock_shared_on_table(context_->txn_, fh_->GetFd());

        scan_ = std::make_unique<RmScan>(fh_, context_);

        //如果没有记录，不加锁
        

        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            record_buffer = scan_->get_now_record(false);
            // record_unpin_guard rug({fh_->GetFd(), rid_.page_no}, false, sm_manager_->buffer_pool_manager_);
            if(eval_conds(this, fed_conds_, record_buffer)) {
                break;
            }
            scan_->next();
        }
    }

    void nextTuple() override {
        // TODO
        // 1.检查条件中所有字段的表和算子的表是否匹配
        // 2.获取当前记录(参考beginTuple())赋给算子成员rid_
        // 3.判断是否当前记录(rec.get())满足谓词条件
        // 4.满足则终止循环

        check_runtime_conds();
        assert(!is_end());

        for (scan_->next(); !scan_->is_end(); scan_->next()) {
            rid_ = scan_->rid();
            record_buffer = scan_->get_now_record(false);
            // record_unpin_guard rug({fh_->GetFd(), rid_.page_no}, false, sm_manager_->buffer_pool_manager_);
            if(eval_conds(this, fed_conds_, record_buffer)) {
                break;
            }
        }
    }

    bool is_end() const override { return scan_->is_end(); }

    std::unique_ptr<RmRecord> Next() override {
        assert(!is_end());
        return std::move(record_buffer);
    }

    void check_runtime_conds() {
        for (auto &cond : fed_conds_) {
            assert(cond.lhs_col.tab_name == tab_name_);
            if (!cond.is_rhs_val) {
                assert(cond.rhs_col.tab_name == tab_name_);
            }
        }
    }

    Rid &rid() override { return rid_; }

    const std::vector<ColMeta> &cols() const {
        return this->cols_;
    };

    size_t tupleLen() const { 
        return fh_-> get_file_hdr().record_size; 
    }

    std::string getType() { return "SeqScanExecutor"; };

    ColMeta get_col_offset(const TabCol &target) override{ 
        for(auto iter = this->cols_.begin(); iter!=this->cols_.end(); iter++){
            if(iter->tab_name == target.tab_name && iter->name == target.col_name){
                return *iter;
            }
        }
        return ColMeta();
    };

};