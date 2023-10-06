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

#include <limits.h>
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

// #define DEBUG_IXSCAN

class IndexScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;                      // 表名称
    TabMeta tab_;                               // 表的元数据
    std::vector<Condition> conds_;              // 扫描条件
    RmFileHandle *fh_;                          // 表的数据文件句柄
    std::vector<ColMeta> cols_;                 // 需要读取的字段
    size_t len_;                                // 选取出来的一条记录的长度

    //自己添加的
    std::vector<Condition> fed_conds_;          // 等值扫描条件，右值不是字段
    std::vector<Condition> other_conds_;        // 除了等值条件之外的条件
    IxIndexHandle *ih_;

    std::vector<std::string> index_col_names_;  // index scan涉及到的索引包含的字段
    IndexMeta index_meta_;                      // index scan涉及到的索引元数据

    std::unique_ptr<IxScan> ix_scan_;

    Rid rid_;
    std::unique_ptr<RecScan> scan_;

    SmManager *sm_manager_;

    //是否启用范围查询
    bool is_range_query = false;
    std::vector<Condition> range_cond_lss;
    bool is_lss = false;
    std::vector<Condition> range_cond_gtr;
    bool is_gtr = false;

    //buffer,为性能用
    std::unique_ptr<RmRecord> record_buffer;

    bool is_single_read = false;

   public:
    IndexScanExecutor(SmManager *sm_manager, const std::string& tab_name, std::vector<Condition> conds, std::vector<std::string> index_col_names, Context *context) 
        :tab_name_(tab_name), 
        tab_(sm_manager->db_.get_table(tab_name_)),
        conds_(std::move(conds)),
        fh_(sm_manager->fhs_.at(tab_name_).get()), 
        cols_(tab_.cols),
        len_(cols_.back().offset + cols_.back().len), 
        index_col_names_(index_col_names), 
        index_meta_(*(tab_.get_index_meta(index_col_names_))), 
        sm_manager_(sm_manager)
    {
        context_ = context;                         
        //区分用于索引的条件和非索引条件
        for(int i = 0; i < (int)std::min(conds_.size(), index_col_names.size()); i++) {
            if(!is_range_query && conds_[i].is_rhs_val && conds_[i].lhs_col.col_name == index_col_names[i] && conds_[i].op != CompOp::OP_NE) {
                if(conds_[i].op == CompOp::OP_EQ) {
                    fed_conds_.emplace_back(conds_[i]);
                }
                else if(conds_[i].op != CompOp::OP_EQ) {
                    if(conds_[i].op == CompOp::OP_LE || conds_[i].op == CompOp::OP_LT) {
                        range_cond_lss.emplace_back(conds_[i]);
                        is_lss = true;
                    }
                    else {
                        range_cond_gtr.emplace_back(conds_[i]);
                        is_gtr = true;
                    }
                    is_range_query = true;
                }
                else {
                    assert(0);
                }
            }
            else if(is_range_query && !(is_lss && is_gtr) && conds_[i].is_rhs_val && conds_[i].op != CompOp::OP_NE) {
                if(is_lss && !is_gtr) {
                    if(conds_[i].lhs_col.col_name == range_cond_lss[0].lhs_col.col_name && (conds_[i].op == CompOp::OP_GE || conds_[i].op == CompOp::OP_GT)) {
                        range_cond_gtr.emplace_back(conds_[i]);
                        is_gtr = true;
                    }
                }
                else if(is_gtr && !is_lss) {
                    if(conds_[i].lhs_col.col_name == range_cond_gtr[0].lhs_col.col_name && (conds_[i].op == CompOp::OP_LE || conds_[i].op == CompOp::OP_LT)) {
                        range_cond_lss.emplace_back(conds_[i]);
                        is_lss = true;
                    }
                }
            }
            else {
                other_conds_.emplace_back(conds_[i]);
            }
        }

        is_single_read = (fed_conds_.size() == index_col_names.size());

        //得到对应的索引文件
        std::string ix_file_name = sm_manager_->get_ix_manager()->get_index_name(tab_name_, index_meta_.cols);
        ih_ = sm_manager_->ihs_.at(ix_file_name).get();
        init_hash_cols();
    }

    void beginTuple() override {

        context_->lock_mgr_->lock_shared_on_table(context_->txn_, fh_->GetFd());

        #ifndef ENABLE_LOCK_CRABBING
            std::lock_guard<std::mutex> lg(ih_ -> root_latch_);
        #endif

        int key_len_ = ih_->getFileHdr()->col_tot_len_;
        char* min_key = new char[key_len_];
        char* max_key = new char[key_len_];
        char_array_guard cag_mink(min_key);
        char_array_guard cag_maxk(max_key);

        Iid pos_min;
        Iid pos_max;

        int curr_offset = 0;

        //处理索引字段的等值条件
        for(int i = 0; i < (int)fed_conds_.size(); i++) {
            auto col_meta_ = tab_.get_col(fed_conds_[i].lhs_col.col_name);
            memcpy(min_key + curr_offset, fed_conds_[i].rhs_val.raw->data, col_meta_->len);
            memcpy(max_key + curr_offset, fed_conds_[i].rhs_val.raw->data, col_meta_->len);
            curr_offset += col_meta_->len;
        }
        
        //判断是否使用范围查询
        if(is_range_query) {
            if(is_lss && !is_gtr) {
                auto col_meta_ = tab_.get_col(range_cond_lss[0].lhs_col.col_name);
                char* type_min_value = get_type_min(*col_meta_);
                memcpy(min_key + curr_offset, type_min_value, col_meta_->len);
                memcpy(max_key + curr_offset, range_cond_lss[0].rhs_val.raw->data, col_meta_->len);
                curr_offset += col_meta_->len;
                delete[] type_min_value;
            }
            else if(!is_lss && is_gtr) {
                auto col_meta_ = tab_.get_col(range_cond_gtr[0].lhs_col.col_name);
                char* type_max_value = get_type_max(*col_meta_);
                memcpy(min_key + curr_offset, range_cond_gtr[0].rhs_val.raw->data, col_meta_->len);
                memcpy(max_key + curr_offset, type_max_value, col_meta_->len);
                curr_offset += col_meta_->len;
                delete[] type_max_value;
            }
            else if(is_lss && is_gtr) {
                auto col_meta_ = tab_.get_col(range_cond_gtr[0].lhs_col.col_name);
                memcpy(min_key + curr_offset, range_cond_gtr[0].rhs_val.raw->data, col_meta_->len);
                memcpy(max_key + curr_offset, range_cond_lss[0].rhs_val.raw->data, col_meta_->len);
                curr_offset += col_meta_->len;
            }
            else {
                assert(0);
            }
        }

        //处理索引剩下的字段
        for(int i = (int)fed_conds_.size() + (int)(is_range_query); i < (int)index_col_names_.size(); i++) {
            auto col_meta_ = tab_.get_col(index_meta_.cols[i].name);
            char* type_min_value = get_type_min(*col_meta_);
            char* type_max_value = get_type_max(*col_meta_);
            memcpy(min_key + curr_offset, type_min_value, col_meta_->len);
            memcpy(max_key + curr_offset, type_max_value, col_meta_->len);
            curr_offset += col_meta_->len;
            delete[] type_min_value;
            delete[] type_max_value;
        }

        #ifdef DEBUG_IXSCAN
            std::string min_0 = std::string(min_key, curr_offset);
            std::string max_0 = std::string(max_key, curr_offset);
        #endif

        pos_min = ih_->lower_bound(min_key, context_);
        pos_max = ih_->upper_bound(max_key, context_);

        ix_scan_ = std::make_unique<IxScan>(ih_, pos_min, pos_max, context_);
        ix_scan_ -> txn_id = context_->txn_->get_transaction_id();

        while (!ix_scan_->is_end()) {
            rid_ = ix_scan_->rid();
            record_buffer = fh_->get_record(rid_, false, context_);
            record_unpin_guard rug({fh_->GetFd(), rid_.page_no}, false, sm_manager_->buffer_pool_manager_);
            if(eval_conds(this, conds_, record_buffer)) {
                break;
            }
            ix_scan_->next();
        }
    }

    void nextTuple() override {
        #ifndef ENABLE_LOCK_CRABBING
            std::lock_guard<std::mutex> lg(ih_->root_latch_);
        #endif
        for(ix_scan_->next(); !ix_scan_->is_end(); ix_scan_->next()) {
            rid_ = ix_scan_->rid();
            record_buffer = fh_->get_record(rid_, false, context_);
            record_unpin_guard rug({fh_->GetFd(), rid_.page_no}, false, sm_manager_->buffer_pool_manager_);
            if(eval_conds(this, conds_, record_buffer)) {
                break;
            }
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        assert(!is_end());
        return std::move(record_buffer);
    }

    bool is_end() const override {
        bool ret =  ix_scan_->is_end();
        return ret;
    }

    Rid &rid() override { return rid_; }

    size_t tupleLen() const { 
        return this->len_; 
    }
    
    const std::vector<ColMeta> &cols() const {
        return this->cols_;
    };

    std::string getType() { 
        return "IndexScanExecutor"; 
    }

    ColMeta get_col_offset(const TabCol &target) { 
        for(auto iter = cols_.begin();iter!=cols_.end();iter++){
            if(iter->tab_name == target.tab_name && iter->name == target.col_name){
                return *iter;
            }
        }
        return ColMeta();
    }


    char* get_type_min(ColMeta col) {
        char* return_ptr = new char[col.len];
        if (col.type ==ColType::TYPE_INT) {
            int min_int = std::numeric_limits<int>::min();
            *(int*)return_ptr = min_int;
        }
        else if (col.type ==ColType::TYPE_FLOAT ) {
            float float_min = std::numeric_limits<float>::lowest();
            *(float*)return_ptr = float_min;
        }
        else if (col.type ==ColType::TYPE_BIGINT ) {
            long long long_min = std::numeric_limits<long long>::min();
            *(long long*)return_ptr = long_min;
        }
        else if (col.type ==ColType::TYPE_STRING) {
            memset(return_ptr, 0x00, col.len);
        }
        else if (col.type ==ColType::TYPE_DATETIME) {
            std::string date_min = "1000-01-01 00:00:00";
            memcpy(return_ptr, date_min.c_str(), date_min.size());
        }
        return return_ptr;
    }

    char* get_type_max(ColMeta col) {
        char* return_ptr = new char[col.len];
        if (col.type ==ColType::TYPE_INT) {
            int max_int = std::numeric_limits<int>::max();
            *(int*)return_ptr = max_int;
        }
        else if (col.type ==ColType::TYPE_FLOAT) {
            float max_float = std::numeric_limits<float>::max();
            *(float*)return_ptr = max_float;
        }
        else if (col.type ==ColType::TYPE_BIGINT ) {
             long long long_max = std::numeric_limits<long long>::max();
            *(long long*)return_ptr = long_max;
         }
        else if (col.type ==ColType::TYPE_STRING ) {
            memset(return_ptr, 0xff, col.len);
        }
        else if (col.type ==ColType::TYPE_DATETIME ) {
            std::string date_min = "9999-12-31 23:59:59";
            memcpy(return_ptr, date_min.c_str(), date_min.size());
        }
        return return_ptr;
    }
};