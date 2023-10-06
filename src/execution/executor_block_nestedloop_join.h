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

//内存限制1GB = 1024*1024字节
// #define BUFFER_JOIN_SIZE 128*1024*1024
#define BUFFER_JOIN_SIZE 64*1024*1024

class BlockNestedLoopJoinExecutor : public AbstractExecutor {
    private:
    
    //重载abstractExecutor的函数
    //建立BufferSize
    std::unique_ptr<AbstractExecutor> outer_;    // 左儿子节点（需要join的表)）
    std::unique_ptr<AbstractExecutor> inner_;   // 右儿子节点（需要join的表）
    size_t len_;                                // join后获得的每条记录的长度
    size_t outer_len_;                          //外表的记录长度
    std::vector<ColMeta> cols_;                 // join后获得的记录的字段
    std::unique_ptr<RmRecord> merged_rec; // 合并后的rec
    std::vector<Condition> fed_conds_;          // join条件
    std::map<TabCol, Value> prev_feed_dict_;
    std::unique_ptr<RmRecord> buffer_inner_;
    char* buffer_join;
    bool is_end_; //State 
    int buffer_join_ptr;
    int buffer_stored_cnt;


    public:
    BlockNestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right, std::vector<Condition> conds)
        : outer_(std::move(left)), inner_(std::move(right))
    {
        outer_len_ = outer_->tupleLen();
        len_ = outer_->tupleLen() + inner_->tupleLen();
        cols_ = outer_->cols();
        auto inner_cols = inner_->cols();
        for (auto &col : inner_cols) {
            col.offset += outer_->tupleLen();
        }

        cols_.insert(cols_.end(), inner_cols.begin(), inner_cols.end());
        // isend = false;
        fed_conds_ = std::move(conds);
        buffer_join = new char[BUFFER_JOIN_SIZE];
        is_end_ = false;
        buffer_join_ptr = 0;
        init_hash_cols();
    }

    //合并rec
    std::unique_ptr<RmRecord> merge_record(std::unique_ptr<RmRecord>& outer_record, std::unique_ptr<RmRecord>& inner_record){
        std::unique_ptr<RmRecord> ret = std::make_unique<RmRecord>(this->len_);
        //赋值
        memcpy(ret->data, outer_record->data, outer_record->size);
        memcpy(ret->data + outer_record->size, inner_record->data, inner_record->size);
        return ret;
    }

    ~BlockNestedLoopJoinExecutor(){
        //释放空间
        delete[] buffer_join;
    }

    bool is_end() const { 
        return this->is_end_;
    }

    /**
     * 预取outer_的rec, 直到buff满或者outer全部取完
     * return: prefetch是否成功.
    */
    bool prefetch(){
        //将外表的数据存入BUFFER中
        const int max_stored_recnum = BUFFER_JOIN_SIZE / outer_len_;
        int now_strored_recnum = 0;
        if(outer_->is_end()){
            return false;
        }
        while(true){
            //取一条record进来
            if(outer_->is_end() || now_strored_recnum>= max_stored_recnum){
                break;
            }
            std::unique_ptr<RmRecord> rec = outer_->Next();
            //写入buffer_join
            memcpy(buffer_join + now_strored_recnum * outer_len_, rec->data, outer_len_);
            //维护元数据
            now_strored_recnum++;
            outer_->nextTuple();
        }
        this->buffer_stored_cnt = now_strored_recnum;
        return true;
    }

    void beginTuple() override {
        //先将外表的数据存入BUFFER中
        outer_->beginTuple();
        is_end_ = !prefetch();//预取
        //一直取inner_的record直到满足样例或者到达末尾
        inner_->beginTuple();
        if(inner_->is_end()){
            //一开始inner_到end,说明inner_为空
            this->is_end_ = true;
            return;
        }
        buffer_inner_ = inner_->Next();
        while(true){
            if(this->is_end()){
                break;
            }
            //判断终止条件
            assert(buffer_join_ptr>=0 && buffer_join_ptr < this->buffer_stored_cnt);
            std::unique_ptr<RmRecord> outer_buff_rec_ = std::make_unique<RmRecord>(outer_len_ , buffer_join + buffer_join_ptr * outer_len_);
            this->merged_rec = std::move(merge_record(outer_buff_rec_, buffer_inner_));
            if(eval_conds(this, this->fed_conds_, this->merged_rec) == true){
                break;
            }
            //转移
            buffer_join_ptr++;
            if(buffer_join_ptr >= this->buffer_stored_cnt){
                buffer_join_ptr = 0;
                inner_ ->nextTuple();
                if(inner_->is_end()){
                    //prefetch
                    if(prefetch()==false){
                        is_end_ = true;
                    }
                    else{
                        inner_ -> beginTuple();
                        buffer_inner_ = std::move(inner_->Next());
                    }
                }
                else{
                    //取出rec,更新
                    buffer_inner_ = std::move(inner_ ->Next());
                }
            }
            else{
                continue;
            }
        }
    }

    void nextTuple() override {
        while(true){
            //判断终止条件
            if(this->is_end()){
                break;
            }
            assert(!inner_->is_end());
            //先转移再判断
            buffer_join_ptr++;
            if(buffer_join_ptr >= this->buffer_stored_cnt){
                buffer_join_ptr = 0;
                inner_ ->nextTuple();
                if(inner_->is_end()){
                    //prefetch
                    if(prefetch()==false){
                        is_end_ = true;
                    }
                    else{
                        inner_ -> beginTuple();
                        buffer_inner_ = std::move(inner_->Next());
                    }
                }
                else{
                    //取出rec,更新
                    buffer_inner_ = std::move(inner_ ->Next());
                }
            }
            else{
                
            }

            //判断
            assert(buffer_join_ptr>=0 && buffer_join_ptr < this->buffer_stored_cnt);
            if(this->is_end()){
                break;
            }
            else{
                std::unique_ptr<RmRecord> outer_buff_rec_ = std::make_unique<RmRecord>(outer_len_ , buffer_join + buffer_join_ptr * outer_len_);
                this->merged_rec = std::move(merge_record(outer_buff_rec_, buffer_inner_));
                if(eval_conds(this, this->fed_conds_, this->merged_rec) == true){
                    break;
                }
            }
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        return std::move(merged_rec);
    }


    Rid &rid() override { return _abstract_rid; }
    
    const std::vector<ColMeta> &cols() const {
        return this->cols_;
    };

    size_t tupleLen() const { 
        return this->len_; 
    }

    std::string getType() { return "BlockNestedJoinExecutor"; }

    ColMeta get_col_offset(const TabCol &target) override{ 
        for(auto iter = this->cols_.begin(); iter!=this->cols_.end(); iter++){
            if(iter->tab_name == target.tab_name && iter->name == target.col_name){
                return *iter;
            }
        }
        return ColMeta();
    };



};