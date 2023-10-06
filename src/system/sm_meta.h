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

#include <algorithm>
#include <iostream>
#include <map>
#include <string>
#include <vector>
#include <set>

#include "errors.h"
#include "sm_defs.h"
#include "common/common.h"

/* 字段元数据 */
struct ColMeta {
    std::string tab_name;   // 字段所属表名称
    std::string name;       // 字段名称
    ColType type;           // 字段类型
    int len;                // 字段长度
    int offset;             // 字段位于记录中的偏移量
    bool index;             /** unused */

    friend std::ostream &operator<<(std::ostream &os, const ColMeta &col) {
        // ColMeta中有各个基本类型的变量，然后调用重载的这些变量的操作符<<（具体实现逻辑在defs.h）
        return os << col.tab_name << ' ' << col.name << ' ' << col.type << ' ' << col.len << ' ' << col.offset << ' '
                  << col.index;
    }

    friend std::istream &operator>>(std::istream &is, ColMeta &col) {
        return is >> col.tab_name >> col.name >> col.type >> col.len >> col.offset >> col.index;
    }
};

/* 索引元数据 */
struct IndexMeta {
    std::string tab_name;           // 索引所属表名称
    size_t col_tot_len;                // 索引字段长度总和
    size_t col_num;                    // 索引字段数量
    std::vector<ColMeta> cols;      // 索引包含的字段

    friend std::ostream &operator<<(std::ostream &os, const IndexMeta &index) {
        os << index.tab_name << " " << index.col_tot_len << " " << index.col_num;
        for(auto& col: index.cols) {
            os << "\n" << col;
        }
        return os;
    }

    friend std::istream &operator>>(std::istream &is, IndexMeta &index) {
        is >> index.tab_name >> index.col_tot_len >> index.col_num;
        for(size_t i = 0; i < index.col_num; ++i) {
            ColMeta col;
            is >> col;
            index.cols.emplace_back(col);
        }
        return is;
    }
};

/* 表元数据 */
struct TabMeta {
public:
    std::string name;                   // 表名称
    std::vector<ColMeta> cols;          // 表包含的字段
    std::vector<IndexMeta> indexes;     // 表上建立的索引
    std::unordered_map<std::string, int> cols_hash; //表字段hash

    TabMeta(){}

    void init_hash(){
        //建立hash:
        for(auto iter = cols.begin(); iter!= cols.end(); ++iter){
            cols_hash[iter->name] = iter - cols.begin();
        }
    }

    TabMeta(const TabMeta &other) 
        :name(other.name),
        cols(other.cols.begin(), other.cols.end()),
        indexes(other.indexes.begin(), other.indexes.end())
    {
        init_hash();
    }

    /* 判断当前表中是否存在名为col_name的字段 */
    bool is_col(const std::string &col_name) const {
        auto pos = std::find_if(cols.begin(), cols.end(), [&](const ColMeta &col) { return col.name == col_name; });
        return pos != cols.end();
    }

    /* 判断当前表上是否建有指定索引，索引包含的字段为col_names */
    bool is_index(const std::vector<std::string>& col_names) const {
        for(auto& index: indexes) {
            if((size_t)index.col_num == col_names.size()) {
                size_t i = 0;
                for(; i < (size_t)index.col_num; ++i) {
                    if(index.cols[i].name.compare(col_names[i]) != 0)
                        break;
                }
                if(i == index.col_num) return true;
            }
        }

        return false;
    }

    //仅判断eq_conds
    bool is_leftmost_match(const std::vector<Condition>& all_conds, std::vector<int>& vis_mask, std::map<int,int>& permutation, std::vector<std::string>& index_col_names){
        index_col_names.clear();

        //特殊情况
        if(all_conds.empty()){
            return false;
        }

        std::map<std::string, int> eq_map;
        std::map<std::string, int> lg_map;//! eq || neq

        for(auto iter = all_conds.begin(); iter != all_conds.end(); iter++){
            if(iter->op == OP_EQ){
                eq_map[iter->lhs_col.col_name] = iter - all_conds.begin();
            }
            else if(iter->op != OP_NE){
                lg_map[iter->lhs_col.col_name] = iter - all_conds.begin();
            }
            else{
                //NE Do Nothing
            }
        }

        int max_len = 0;
        int max_len_ind = -1;

        for(auto iter = this->indexes.begin(); iter!= this->indexes.end(); iter++){
            //枚举索引
            int tmp_len = 0;
            for(auto sub_it = iter->cols.begin(); sub_it!= iter->cols.end(); sub_it++){
                //枚举字段
                if(eq_map.count(sub_it->name) !=0){
                    tmp_len ++;
                    continue;
                }
                else{
                    if(lg_map.count(sub_it->name)!=0){
                        tmp_len ++;
                        break;
                    }
                    else{
                        break;
                    }
                }
            }
            if(tmp_len > max_len){
                max_len = tmp_len;
                max_len_ind = iter - this->indexes.begin();
            }
        }

        //取出最大值
        if(max_len == 0){
            return false;
        }
        else{
            auto ind_meta = this->indexes.at(max_len_ind);
            //取index_name
            for(auto iter = ind_meta.cols.begin(); iter != ind_meta.cols.end(); iter++){
                index_col_names.emplace_back(iter->name);
            }
            //取置换
            for(auto iter = ind_meta.cols.begin(); iter != ind_meta.cols.end(); iter++){
                if(eq_map.count(iter->name)!=0){
                    permutation[iter - ind_meta.cols.begin()] = eq_map[iter->name];
                    vis_mask[eq_map[iter->name]] = 1;
                }
                else{
                    if(lg_map.count(iter->name)!=0){
                        permutation[iter - ind_meta.cols.begin()] = lg_map[iter->name];
                        vis_mask[lg_map[iter->name]] = 1;
                        break;
                    }
                    else{
                        break;
                    }
                }
            }
        }

        return true;
    }

    //允许忽略第一个字段的前提下进行最左匹配
    bool is_leftmost_range_match(const std::vector<Condition>& all_conds, std::vector<int>& vis_mask, std::map<int,int>& permutation, std::vector<std::string>& index_col_names){
        index_col_names.clear();

        //特殊情况
        if(all_conds.empty()){
            return false;
        }

        std::map<std::string, int> eq_map;
        std::map<std::string, int> lg_map;//! eq || neq

        //生成eq_map和lg_map
        for(auto iter = all_conds.begin(); iter != all_conds.end(); iter++){
            if(iter->op == OP_EQ){
                eq_map[iter->lhs_col.col_name] = iter - all_conds.begin();
            }
            else if(iter->op != OP_NE){
                lg_map[iter->lhs_col.col_name] = iter - all_conds.begin();
            }
            else{
                //NE Do Nothing
            }
        }

        int max_len = 1;
        int max_len_ind = -1;

        //枚举索引
        for(auto iter = this->indexes.begin(); iter!= this->indexes.end(); iter++){
            if(iter->cols.begin()->type != ColType::TYPE_INT){
                continue;
            }
            int tmp_len = 1;//可忽略第一个字段
            for(auto sub_it = iter->cols.begin() + 1; sub_it!= iter->cols.end(); sub_it++){
                //枚举字段
                if(eq_map.count(sub_it->name) !=0){
                    tmp_len ++;
                    continue;
                }
                else{
                    if(lg_map.count(sub_it->name)!=0){
                        tmp_len ++;
                        break;
                    }
                    else{
                        break;
                    }
                }
            }
            if(tmp_len > max_len){
                max_len = tmp_len;
                max_len_ind = iter - this->indexes.begin();
            }
        }

        //取出最大值
        if(max_len == 1){
            return false;
        }
        else{
            auto ind_meta = this->indexes.at(max_len_ind);
            //取index_name
            for(auto iter = ind_meta.cols.begin(); iter != ind_meta.cols.end(); iter++){
                index_col_names.emplace_back(iter->name);
            }
            //取置换
            auto begin_it = ind_meta.cols.begin() + 1;
            for(auto iter = begin_it; iter != ind_meta.cols.end(); iter++){
                if(eq_map.count(iter->name)!=0){
                    permutation[iter - begin_it] = eq_map[iter->name];
                    vis_mask[eq_map[iter->name]] = 1;
                }
                else{
                    if(lg_map.count(iter->name)!=0){
                        permutation[iter - begin_it] = lg_map[iter->name];
                        vis_mask[lg_map[iter->name]] = 1;
                        break;
                    }
                    else{
                        break;
                    }
                }
            }
        }

        return true;
    }

    int find_index_pos(const std::vector<std::string>& col_names) const {
        for(int j = 0; j < (int)indexes.size(); j++) {
            auto& index = indexes[j];
            if(index.col_num == col_names.size()) {
                size_t i = 0;
                for(; i < index.col_num; ++i) {
                    if(index.cols[i].name.compare(col_names[i]) != 0)
                        break;
                }
                if(i == (size_t)index.col_num) return j;
            }
        }

        return -1;
    }

    /* 根据字段名称集合获取索引元数据 */
    std::vector<IndexMeta>::iterator get_index_meta(const std::vector<std::string>& col_names) {
        for(auto index = indexes.begin(); index != indexes.end(); ++index) {
            if(index->col_num != col_names.size()) continue;
            auto& index_cols = (*index).cols;
            size_t i = 0;
            for(; i < col_names.size(); ++i) {
                if(index_cols[i].name.compare(col_names[i]) != 0) 
                    break;
            }
            if(i == col_names.size()) return index;
        }
        throw IndexNotFoundError(name, col_names);
    }

    /* 根据字段名称获取字段元数据 */
    // std::vector<ColMeta>::iterator get_col(const std::string &col_name) {
    //     auto pos = std::find_if(cols.begin(), cols.end(), [&](const ColMeta &col) { return col.name == col_name; });
    //     if (pos == cols.end()) {
    //         throw ColumnNotFoundError(col_name);
    //     }
    //     return pos;
    // }
    std::vector<ColMeta>::iterator get_col(const std::string& col_name){
        return cols.begin() + cols_hash.at(col_name);
    }

    friend std::ostream &operator<<(std::ostream &os, const TabMeta &tab) {
        os << tab.name << '\n' << tab.cols.size() << '\n';
        for (auto &col : tab.cols) {
            os << col << '\n';  // col是ColMeta类型，然后调用重载的ColMeta的操作符<<
        }
        os << tab.indexes.size() << "\n";
        for (auto &index : tab.indexes) {
            os << index << "\n";
        }
        return os;
    }

    friend std::istream &operator>>(std::istream &is, TabMeta &tab) {
        size_t n;
        is >> tab.name >> n;
        for (size_t i = 0; i < n; i++) {
            ColMeta col;
            is >> col;
            tab.cols.emplace_back(col);
        }
        is >> n;
        for(size_t i = 0; i < n; ++i) {
            IndexMeta index;
            is >> index;
            tab.indexes.emplace_back(index);
        }
        return is;
    }
};

// 注意重载了操作符 << 和 >>，这需要更底层同样重载TabMeta、ColMeta的操作符 << 和 >>
/* 数据库元数据 */
class DbMeta {
    friend class SmManager;

   private:
    std::string name_;                      // 数据库名称

   public:
    std::map<std::string, TabMeta> tabs_;   // 数据库中包含的表
    // DbMeta(std::string name) : name_(name) {}

    /* 判断数据库中是否存在指定名称的表 */
    bool is_table(const std::string &tab_name) const { return tabs_.find(tab_name) != tabs_.end(); }

    void SetTabMeta(const std::string &tab_name, const TabMeta &meta) {
        tabs_[tab_name] = meta;
    }

    /* 获取指定名称表的元数据 */
    TabMeta &get_table(const std::string &tab_name) {
        auto pos = tabs_.find(tab_name);
        if (pos == tabs_.end()) {
            throw TableNotFoundError(tab_name);
        }

        return pos->second;
    }

    // 重载操作符 <<
    friend std::ostream &operator<<(std::ostream &os, const DbMeta &db_meta) {
        os << db_meta.name_ << '\n' << db_meta.tabs_.size() << '\n';
        for (auto &entry : db_meta.tabs_) {
            os << entry.second << '\n';
        }
        return os;
    }

    friend std::istream &operator>>(std::istream &is, DbMeta &db_meta) {
        size_t n;
        is >> db_meta.name_ >> n;
        for (size_t i = 0; i < n; i++) {
            TabMeta tab;
            is >> tab;
            db_meta.tabs_[tab.name] = tab;
        }
        return is;
    }
};
