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

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <vector>
#include "parser/ast.h"

#include "parser/parser.h"

typedef enum PlanTag{
    T_Invalid = 1,
    T_Help,
    T_ShowTable,
    T_ShowIndex,
    T_DescTable,
    T_CreateTable,
    T_DropTable,
    T_CreateIndex,
    T_DropIndex,
    T_Insert,
    T_Update,
    T_Delete,
    T_select,
    T_Transaction_begin,
    T_Transaction_commit,
    T_Transaction_abort,
    T_Transaction_rollback,
    T_SeqScan,
    T_IndexScan,
    T_IndexRangeScan,
    T_NestLoop,
    T_Sort,
    T_Projection,
    T_Projection_Nocopy,
    T_Aggregation,
    T_LoadData,
    T_IoEnable
} PlanTag;

// 查询执行计划
class Plan
{
public:
    PlanTag tag;
    virtual ~Plan() = default;
};

class ScanPlan : public Plan
{   
    public:
        std::string tab_name_;                     
        std::vector<ColMeta> cols_;                
        std::vector<Condition> conds_;             
        size_t len_;                               
        std::vector<Condition> fed_conds_;
        std::vector<std::string> index_col_names_;
    public:
        ScanPlan(PlanTag tag, SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, const std::vector<std::string> &index_col_names)
            :tab_name_(std::move(tab_name)), 
            cols_(sm_manager->db_.get_table(tab_name_).cols), 
            conds_(std::move(conds)), 
            len_(cols_.back().offset + cols_.back().len), 
            fed_conds_(conds_), 
            index_col_names_(index_col_names)
        {
            Plan::tag = tag;
        
        }
        ~ScanPlan(){}
};

class JoinPlan : public Plan
{
    public:
        JoinPlan(PlanTag tag, std::shared_ptr<Plan> left, std::shared_ptr<Plan> right, std::vector<Condition> conds)
        :left_(std::move(left)), right_(std::move(right)), conds_(std::move(conds)),type(INNER_JOIN) {
            Plan::tag = tag;
        }
        ~JoinPlan(){}
        // 左节点
        std::shared_ptr<Plan> left_;
        // 右节点
        std::shared_ptr<Plan> right_;
        // 连接条件
        std::vector<Condition> conds_;
        // future TODO: 后续可以支持的连接类型
        JoinType type;
        
};

class ProjectionPlan : public Plan
{
    public:
        ProjectionPlan(PlanTag tag, std::shared_ptr<Plan> subplan, std::vector<TabCol> sel_cols)
        :subplan_(std::move(subplan)), sel_cols_(std::move(sel_cols)){
            Plan::tag = tag;
        }
        ~ProjectionPlan(){}
        std::shared_ptr<Plan> subplan_;
        std::vector<TabCol> sel_cols_;
};

class AggregationPlan : public Plan
{
    public:
        AggregationPlan(PlanTag tag, std::shared_ptr<Plan> subplan, std::vector<AgreType> agre_types, std::vector<TabCol> agre_cols, std::vector<TabCol> target_cols)
        :subplan_(std::move(subplan)), agre_types_(std::move(agre_types)), agre_cols_(std::move(agre_cols)), target_cols_(std::move(target_cols)){
            Plan::tag = tag;
        }
        ~AggregationPlan(){}
        std::shared_ptr<Plan> subplan_;
        std::vector<TabCol> sel_cols_;
        std::vector<AgreType> agre_types_;
        std::vector<TabCol> agre_cols_;
        std::vector<TabCol> target_cols_;
};

class SortPlan : public Plan
{
    public:
        SortPlan(PlanTag tag, std::shared_ptr<Plan> subplan, std::vector<TabCol> order_cols, const std::vector<bool>& is_descs, int limit_cnt)
        : subplan_(std::move(subplan)), order_cols_(std::move(order_cols)), is_descs_(is_descs), limit_cnt_(limit_cnt){
            Plan::tag = tag;
        }
        ~SortPlan(){}
        std::shared_ptr<Plan> subplan_;
        std::vector<TabCol> order_cols_;
        std::vector<bool> is_descs_;
        int limit_cnt_;
        
};

// dml语句，包括insert; delete; update; select语句　
class DMLPlan : public Plan
{
    public:
        DMLPlan(PlanTag tag, std::shared_ptr<Plan> subplan,std::string tab_name,
                std::vector<Value> values, std::vector<Condition> conds,
                std::vector<SetClause> set_clauses)
        : subplan_(std::move(subplan)), tab_name_(std::move(tab_name)),values_(std::move(values)), conds_(std::move(conds)), set_clauses_(std::move(set_clauses)){
            Plan::tag = tag;
        }
        ~DMLPlan(){}
        std::shared_ptr<Plan> subplan_;
        std::string tab_name_;
        std::vector<Value> values_;
        std::vector<Condition> conds_;
        std::vector<SetClause> set_clauses_;
};

// ddl语句, 包括create/drop table; create/drop index;
class DDLPlan : public Plan
{
    public:
        std::string tab_name_;
        std::vector<std::string> tab_col_names_;
        std::vector<ColDef> cols_;
        DDLPlan(PlanTag tag, std::string tab_name, std::vector<std::string> col_names, std::vector<ColDef> cols)
            :tab_name_(std::move(tab_name)), 
            tab_col_names_(std::move(col_names)),
            cols_(std::move(cols))
        {
            Plan::tag = tag;
        }
        ~DDLPlan(){}
};

// help; show tables; desc tables; begin; abort; commit; rollback语句对应的plan
class OtherPlan : public Plan
{
    public:
        OtherPlan(PlanTag tag, std::string tab_name)
        : tab_name_(std::move(tab_name)) {
            Plan::tag = tag;
            // tab_name_ = std::move(tab_name);            
        }
        OtherPlan(PlanTag tag, std::string tab_name, std::string file_name)
        : tab_name_(std::move(tab_name)), file_name_(std::move(file_name)){
            Plan::tag = tag;
        }
        OtherPlan(PlanTag tag, bool io_enabled_)
        {
            Plan::tag = tag;
            io_enable_ = std::move(io_enabled_);           
        }

        ~OtherPlan(){}
        std::string tab_name_;
        std::string file_name_;
        bool io_enable_;
};

class plannerInfo{
    public:
    std::shared_ptr<ast::SelectStmt> parse;
    std::vector<Condition> where_conds;
    std::vector<TabCol> sel_cols;
    std::shared_ptr<Plan> plan;
    std::vector<std::shared_ptr<Plan>> table_scan_executors;
    std::vector<SetClause> set_clauses;
    plannerInfo(std::shared_ptr<ast::SelectStmt> parse_):parse(std::move(parse_)){}

};
