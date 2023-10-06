/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "analyze.h"

/**
 * @description: 分析器，进行语义分析和查询重写，需要检查不符合语义规定的部分
 * @param {shared_ptr<ast::TreeNode>} parse parser生成的结果集
 * @return {shared_ptr<Query>} Query 
 */
std::shared_ptr<Query> Analyze::do_analyze(std::shared_ptr<ast::TreeNode> parse)
{
    std::shared_ptr<Query> query = std::make_shared<Query>();
    if (auto x = std::dynamic_pointer_cast<ast::SelectStmt>(parse))
    {
        // 处理表名
        query->tables = std::move(x->tabs);
        /** TODO: 检查表是否存在 */
        for(auto &table_name:query->tables) {
            if(sm_manager_->fhs_.find(table_name) == sm_manager_->fhs_.end()) {
                throw TableNotFoundError(table_name);
            }
        }

        std::vector<ColMeta> all_cols;
        get_all_cols(query->tables, all_cols);
        // 处理target list，再target list中添加上表名，例如 a.id
        // 如果没有聚集函数
        if(x->agre_cols.size() == 0) {
            for (auto &sv_sel_col : x->cols) {
                TabCol sel_col = {.tab_name = sv_sel_col->tab_name, .col_name = sv_sel_col->col_name};
                query->cols.emplace_back(sel_col);
            }
        }
        else {
            for(auto &agre_col : x->agre_cols) {
                if(agre_col->col->col_name.empty() && agre_col->agre_type == ast::SvAgreType::TYPE_COUNT) {
                    query->agre_types.emplace_back(AgreType::AGRE_TYPE_COUNT_ALL);
                }
                else {
                    query->agre_types.emplace_back(convert_sv_agretype(agre_col->agre_type));
                }
                TabCol agre_col_ = {.tab_name = agre_col->col->tab_name, .col_name = agre_col->agre_name};
                TabCol target_col = {.tab_name = agre_col->col->tab_name, .col_name = agre_col->col->col_name};
                query->agre_cols.emplace_back(agre_col_);
                if(agre_col->col->col_name.empty()) {
                    query->target_cols.emplace_back(target_col);
                }
                else {
                    query->target_cols.emplace_back(check_column(all_cols, target_col));
                }
            }
        }


        //处理order list, target list中添加上表名，例如 a.id
        if(x->order!=nullptr){
            for (auto &sv_order_col : x->order->cols){
                TabCol order_col = {.tab_name = sv_order_col->col->tab_name, .col_name = sv_order_col->col->col_name};
                query->order_cols.emplace_back(order_col);
            }
        }

        if (query->cols.empty()) {
            // select all columns
            for (auto &col : all_cols) {
                TabCol sel_col = {.tab_name = col.tab_name, .col_name = col.name};
                query->cols.emplace_back(sel_col);
            }
        } else {
            // infer table name from column name
            for (auto &sel_col : query->cols) {
                sel_col = check_column(all_cols, sel_col);  // 列元数据校验
            }
        }
        // infer table name from column name
        for (auto &order_col: query->order_cols){
            order_col = check_column(all_cols, order_col);
        }
        //处理where条件
        get_clause(x->conds, query->conds);
        check_clause(query->tables, query->conds);
    } else if (auto x = std::dynamic_pointer_cast<ast::UpdateStmt>(parse)) {
        /** TODO: */

        //处理update的set 值
        for(auto &set_clause_val: x->set_clauses) {
            SetClause tmp;
            tmp.lhs.col_name = set_clause_val->col_name;
            tmp.lhs.tab_name = x->tab_name;
            tmp.rhs = convert_sv_value(set_clause_val->val);
            tmp.set_op = convert_setop(set_clause_val->setop);
            // tmp.rhs.init_raw();
            query->set_clauses.emplace_back(tmp);
        }

        //处理where条件
        get_clause(x->conds, query->conds);
        check_clause({x->tab_name}, query->conds);
    } else if (auto x = std::dynamic_pointer_cast<ast::DeleteStmt>(parse)) {
        //处理where条件
        get_clause(x->conds, query->conds);
        check_clause({x->tab_name}, query->conds);
    } else if (auto x = std::dynamic_pointer_cast<ast::InsertStmt>(parse)) {
        // 处理insert 的values值
        for (auto &sv_val : x->vals) {
            query->values.emplace_back(convert_sv_value(sv_val));
        }
    } else {
        // do nothing
    }
    query->parse = std::move(parse);
    return query;
}


TabCol Analyze::check_column(const std::vector<ColMeta> &all_cols, TabCol target) {
    if (target.tab_name.empty()) {
        // Table name not specified, infer table name from column name
        std::string tab_name;
        for (auto &col : all_cols) {
            if (col.name == target.col_name) {
                if (!tab_name.empty()) {
                    throw AmbiguousColumnError(target.col_name);
                }
                tab_name = col.tab_name;
            }
        }
        if (tab_name.empty()) {
            throw ColumnNotFoundError(target.col_name);
        }
        target.tab_name = tab_name;
    } else {
        /** TODO: Make sure target column exists */

        int find_col=0;
        for(auto &col : all_cols) {
            if(target.tab_name == col.tab_name && col.name == target.col_name) {
                find_col = 1;
                break;
            }
        }

        if(find_col==0) {
            throw ColumnNotFoundError(target.col_name);
        }
    }
    return target;
}

void Analyze::get_all_cols(const std::vector<std::string> &tab_names, std::vector<ColMeta> &all_cols) {
    for (auto &sel_tab_name : tab_names) {
        // 这里db_不能写成get_db(), 注意要传指针
        const auto &sel_tab_cols = sm_manager_->db_.get_table(sel_tab_name).cols;
        all_cols.insert(all_cols.end(), sel_tab_cols.begin(), sel_tab_cols.end());
    }
}

void Analyze::get_clause(const std::vector<std::shared_ptr<ast::BinaryExpr>> &sv_conds, std::vector<Condition> &conds) {
    conds.clear();
    for (auto &expr : sv_conds) {
        Condition cond;
        cond.lhs_col = {.tab_name = expr->lhs->tab_name, .col_name = expr->lhs->col_name};
        cond.op = convert_sv_comp_op(expr->op);
        if (auto rhs_val = std::dynamic_pointer_cast<ast::Value>(expr->rhs)) {
            cond.is_rhs_val = true;
            cond.rhs_val = convert_sv_value(rhs_val);
        } else if (auto rhs_col = std::dynamic_pointer_cast<ast::Col>(expr->rhs)) {
            cond.is_rhs_val = false;
            cond.rhs_col = {.tab_name = rhs_col->tab_name, .col_name = rhs_col->col_name};
        }
        conds.emplace_back(cond);
    }
}

void Analyze::check_clause(const std::vector<std::string> &tab_names, std::vector<Condition> &conds) {
    // auto all_cols = get_all_cols(tab_names);
    std::vector<ColMeta> all_cols;
    get_all_cols(tab_names, all_cols);
    // Get raw values in where clause
    for (auto &cond : conds) {
        // Infer table name from column name
        cond.lhs_col = check_column(all_cols, cond.lhs_col);
        if (!cond.is_rhs_val) {
            cond.rhs_col = check_column(all_cols, cond.rhs_col);
        }
        TabMeta &lhs_tab = sm_manager_->db_.get_table(cond.lhs_col.tab_name);
        auto lhs_col = lhs_tab.get_col(cond.lhs_col.col_name);
        ColType lhs_type = lhs_col->type;
        ColType rhs_type;
        if (cond.is_rhs_val) {
            cond.rhs_val.init_raw(lhs_col->len);
            rhs_type = cond.rhs_val.type;
        } else {
            TabMeta &rhs_tab = sm_manager_->db_.get_table(cond.rhs_col.tab_name);
            auto rhs_col = rhs_tab.get_col(cond.rhs_col.col_name);
            rhs_type = rhs_col->type;
        }
        if (lhs_type != rhs_type) {
            if(cond.is_rhs_val && lhs_type== TYPE_DATETIME){
                //处理DATETIME类型不等
                //convert
                cond.rhs_val.type = TYPE_DATETIME;
                cond.rhs_val.datetime_val = cond.rhs_val.str_val; 
            }
            else{
                throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(rhs_type));
            }
        }
    }
}


Value Analyze::convert_sv_value(const std::shared_ptr<ast::Value> &sv_val) {
    Value val;
    if (auto int_lit = std::dynamic_pointer_cast<ast::IntLit>(sv_val)) {
        val.set_int(int_lit->val);
    } else if(auto bigint_lit = std::dynamic_pointer_cast<ast::BigintLit>(sv_val)) {
        val.set_bigint(bigint_lit->val);
    } else if (auto float_lit = std::dynamic_pointer_cast<ast::FloatLit>(sv_val)) {
        val.set_float(float_lit->val);
    } else if (auto str_lit = std::dynamic_pointer_cast<ast::StringLit>(sv_val)) {
        val.set_str(str_lit->val);
    } else {
        throw InternalError("Unexpected sv value type");
    }
    return val;
}

CompOp Analyze::convert_sv_comp_op(ast::SvCompOp op) {
    std::map<ast::SvCompOp, CompOp> m = {
        {ast::SV_OP_EQ, OP_EQ}, {ast::SV_OP_NE, OP_NE}, {ast::SV_OP_LT, OP_LT},
        {ast::SV_OP_GT, OP_GT}, {ast::SV_OP_LE, OP_LE}, {ast::SV_OP_GE, OP_GE},
    };
    return m.at(op);
}

AgreType Analyze::convert_sv_agretype(ast::SvAgreType type) {
    std::map<ast::SvAgreType, AgreType> m = {
        {ast::SvAgreType::TYPE_MAX, AGRE_TYPE_MAX}, {ast::SvAgreType::TYPE_MIN, AGRE_TYPE_MIN},
        {ast::SvAgreType::TYPE_SUM, AGRE_TYPE_SUM}, {ast::SvAgreType::TYPE_COUNT, AGRE_TYPE_COUNT}
    };
    return m.at(type);
}

SetOp Analyze::convert_setop(ast::SetOperation setop){
    if(setop == ast::SetOperation::ASSIGN){
        return SetOp::OP_ASSIGN;
    }
    else if(setop == ast::SetOperation::PLUS){
        return SetOp::OP_PLUS;
    }
    else if(setop == ast::SetOperation::MINUS){
        return SetOp::OP_MINUS;
    }
    assert(0);
    return SetOp::OP_INVALID;
}